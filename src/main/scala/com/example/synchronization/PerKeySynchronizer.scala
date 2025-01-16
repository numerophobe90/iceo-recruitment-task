package com.example.synchronization
import cats.Applicative
import cats.effect.kernel.Async
import cats.effect.std.{AtomicCell, Mutex}
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps

class PerKeySynchronizer[F[_]: Async](cell: AtomicCell[F, Map[String, Mutex[F]]]) {
  def synchronize[T](key: String, eff: F[T]): F[T] = {
    val acquireCorrespondingMutex = cell.evalModify(map =>
      map
        .get(key) match {
        case Some(mutex) => Applicative[F].pure(map -> mutex)
        case None =>
          Mutex[F].map(mutex => map.updated(key, mutex) -> mutex) // memleak, mutexes are not removed from the map
      }
    )
    for {
      mutex  <- acquireCorrespondingMutex
      result <- mutex.lock.surround(eff)
    } yield result
  }
}

object PerKeySynchronizer {
  def instance[F[_]: Async]: F[PerKeySynchronizer[F]] =
    AtomicCell[F].of(Map.empty[String, Mutex[F]]).map(new PerKeySynchronizer[F](_))
}
