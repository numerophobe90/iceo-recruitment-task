package com.example.stream

import cats.Functor
import cats.effect.Async
import cats.effect.std.Queue
import cats.implicits.{toFunctorOps, toTraverseOps}
import fs2.Stream

trait HasPartitionKey[T] {
  def partitionKey(t: T): String
}

trait Partitioner[F[_], T] {
  def offerToPartition(t: T): F[Unit]
  def partitionsStream: fs2.Stream[F, fs2.Stream[F, T]]
}

class DefaultPartitioner[F[_]: Functor, T](queues: Vector[Queue[F, T]], hashFn: String => Int)(implicit
  hasKey: HasPartitionKey[T]
) extends Partitioner[F, T] {
  private val nrOfPartitions = queues.size
  override def offerToPartition(t: T): F[Unit] = {
    queues(hashFn(hasKey.partitionKey(t)) % nrOfPartitions).offer(t)
  }

  override def partitionsStream: Stream[F, Stream[F, T]] = Stream.emits(queues).map(Stream.fromQueueUnterminated(_))
}

object DefaultPartitioner {
  type PartitionKeyHashFn = String => Int
  val defaultHashFn: PartitionKeyHashFn = s => math.abs(s.hashCode)
  def make[F[_]: Async, T](maxConcurrent: Int, hashFn: PartitionKeyHashFn = defaultHashFn)(implicit
    hasKey: HasPartitionKey[T]
  ): F[DefaultPartitioner[F, T]] =
    (1 to maxConcurrent).toList
      .traverse(_ => Queue.unbounded[F, T])
      .map(partitionQueues => new DefaultPartitioner(partitionQueues.toVector, hashFn))
}
