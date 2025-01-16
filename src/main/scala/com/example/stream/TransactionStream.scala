package com.example.stream

import cats.Applicative
import cats.data.{EitherT, OptionT}
import cats.effect.{IO, Ref, Resource}
import cats.effect.kernel.Async
import cats.effect.std.{AtomicCell, MapRef, Mutex, Queue}
import fs2.Stream
import org.typelevel.log4cats.Logger
import cats.syntax.all._
import com.example.model.{OrderRow, TransactionRow}
import com.example.persistence.PreparedQueries
import com.example.synchronization.PerKeySynchronizer
import skunk._

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

// All SQL queries inside the Queries object are correct and should not be changed
final class TransactionStream[F[_]](
  synchronizer: PerKeySynchronizer[F],
  operationTimer: FiniteDuration,
  orders: Queue[F, OrderRow],
  session: Resource[F, Session[F]],
  transactionCounter: Ref[F, Int], // updated if long IO succeeds
  stateManager: StateManager[F],   // utility for state management
  maxConcurrent: Int
)(implicit F: Async[F], logger: Logger[F]) {

  def stream: Stream[F, Unit] = {
    Stream
      .fromQueueUnterminated(orders)
      .parEvalMap(maxConcurrent)(processUpdateWithSynchronisation)
  }

  def drainOrdersQueue: F[Unit] = {
    Stream
      .eval(orders.tryTake)
      .repeat
      .unNoneTerminate
      .parEvalMap(maxConcurrent)(processUpdateWithSynchronisation)
      .compile
      .drain
  }

  private def processUpdateWithSynchronisation(updatedOrder: OrderRow) =
    synchronizer.synchronize(updatedOrder.orderId, processUpdate(updatedOrder))

  // Application should shut down on error,
  // If performLongRunningOperation fails, we don't want to insert/update the records
  // Transactions always have positive amount
  // Order is executed if total == filled
  private def processUpdate(updatedOrder: OrderRow): F[Unit] = {
    PreparedQueries(session)
      .use { queries =>
        def considerReEnqueueingUpdatedOrder =
          Applicative[F].unlessA(updatedOrder.createdAt.isBefore(Instant.now.minusSeconds(5)))(
            orders.offer(updatedOrder)
          )
        def processTransaction(state: OrderRow, transaction: TransactionRow) = {
          // parameters for order update
          val params = updatedOrder.filled *: state.orderId *: EmptyTuple

          val tx =
            // update order with params
            queries.updateOrder.execute(params) *>
              // insert the transaction
              queries.insertTransaction.execute(transaction)

          Async[F].uncancelable(_ =>
            performLongRunningOperation(
              transaction
            ).value.void
              .redeemWith(th => logger.error(th)(s"Got error when performing long running IO!"), _ => tx.void)
          )
          // CAUTION: as we are currently executing a database transaction after a successful
          // performLongRunningOperation, there can be a situation when performLongRunningOperation succeeds but
          // database transaction don't
        }
        for {
          // Get current known order state
          maybeState <- OptionT(stateManager.getOrderState(updatedOrder, queries))
                          .flatTapNone(considerReEnqueueingUpdatedOrder)
                          .value
          transaction =
            maybeState.flatMap(state => TransactionRow.fromOrderUpdate(state = state, updated = updatedOrder))
          _ <- maybeState.zip(transaction).fold(logger.info(s"Processing an update did not result in transaction.")) {
                 case (state, transaction) =>
                   processTransaction(state, transaction)
               }

        } yield ()
      }
  }

  // represents some long running IO that can fail
  private def performLongRunningOperation(transaction: TransactionRow): EitherT[F, Throwable, Unit] = {
    EitherT.liftF[F, Throwable, Unit](
      F.sleep(operationTimer) *>
        stateManager.getSwitch.flatMap {
          case false =>
            transactionCounter
              .updateAndGet(_ + 1)
              .flatMap(count =>
                logger.info(
                  s"Updated counter to $count by transaction with amount ${transaction.amount} for order ${transaction.orderId}!"
                )
              )
          case true => F.raiseError(throw new Exception("Long running IO failed!"))
        }
    )
  }

  // helper methods for testing
  def publish(update: OrderRow): F[Unit]                                          = orders.offer(update)
  def getCounter: F[Int]                                                          = transactionCounter.get
  def setSwitch(value: Boolean): F[Unit]                                          = stateManager.setSwitch(value)
  def addNewOrder(order: OrderRow, insert: PreparedCommand[F, OrderRow]): F[Unit] = stateManager.add(order, insert)
  // helper methods for testing
}

object TransactionStream {

  private def gracefulShutdown[F[_]: Async: Logger](transactionStream: TransactionStream[F]) = Resource.onFinalize[F](
    Logger[F].info(s"Trying to drain orders queue on completion") *> transactionStream.drainOrdersQueue
  )

  def apply[F[_]: Async: Logger](
    operationTimer: FiniteDuration,
    session: Resource[F, Session[F]],
    maxConcurrent: Int
  ): Resource[F, TransactionStream[F]] = {
    val components = for {
      counter      <- Ref.of(0)
      queue        <- Queue.unbounded[F, OrderRow]
      stateManager <- StateManager.apply
      synchronizer <- PerKeySynchronizer.instance[F]
    } yield (counter, queue, stateManager, synchronizer)

    for {
      (counter, queue, stateManager, synchronizer) <- Resource.eval(components)
      transactionStream = new TransactionStream[F](
                            synchronizer,
                            operationTimer,
                            queue,
                            session,
                            counter,
                            stateManager,
                            maxConcurrent
                          )
      _ <- gracefulShutdown(transactionStream)
    } yield transactionStream
  }
}
