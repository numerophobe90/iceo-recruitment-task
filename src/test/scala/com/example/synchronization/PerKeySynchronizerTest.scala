package com.example.synchronization

import cats.effect.std.{AtomicCell, Mutex}
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.implicits.catsSyntaxParallelTraverse1
import cats.instances.list.catsStdInstancesForList
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class PerKeySynchronizerTest extends AnyWordSpec with Matchers {

  "KeySynchronizer" should {

    val sut = PerKeySynchronizer.instance[IO].unsafeRunSync()

    "synchronize currently executed effects for the same key" in {
      val counter: Ref[IO, Int] = Ref.unsafe(0)
      val effect =
        counter.get.flatMap(n => counter.set(n + 1)) // race condition since ref.get and ref.set are not synchronised

      (for {
        _ <- (1 to 1024).toList.parTraverse(_ => sut.synchronize("key", effect)) // execute 1024 effects concurrently
        result <- counter.get
      } yield result shouldBe 1024).unsafeRunSync()
    }

    "allow not synchronised execution of effects for different keys" in {
      val counter: Ref[IO, Int] = Ref.unsafe(0)
      val effect                = IO.sleep(100.millis) *> counter.update(_ + 1)

      (for {
        _      <- (1 to 1024).toList.parTraverse(n => sut.synchronize(n.toString, effect)).timeout(200.millis)
        result <- counter.get
      } yield result shouldBe 1024).unsafeRunSync()
    }
  }
}
