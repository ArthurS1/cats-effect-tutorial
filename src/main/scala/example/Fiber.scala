package example

import cats._
import cats.syntax.all._
import cats.effect._
import cats.effect.std._
import cats.effect.syntax.all._
import cats.effect.unsafe.IORuntime
import scala.collection.immutable.{Queue => StdQueue}
import scala.concurrent.duration.{Duration, SECONDS}

/*
 * This exercise attempts to connects producers of values to consumers
 * concurrently. So basically it goes like this :
 *  - A producer produces an int and enqueues on a shared queue
 *  - A consumer consumes the int and does nothing with it except showing and
 *    message when some amount of ints have been produced.
 */

object Fibers extends IOApp {

  /*
   * Here the bounded queue is a functional abstraction on top of the standard
   * Scala immutable queue. What bounded means is that it allows at most
   * <capacity> elements in the queue. When this threshold is met it will
   * defer the new writes in offerers and reads into takers.
   * This should balance the queue so elements are not produced more than they
   * are consumed.
   */
  final case class BoundedQueue[F[_], A](
      queue: StdQueue[A],
      capacity: Int,
      takers: StdQueue[Deferred[F, A]],
      offerers: StdQueue[(A, Deferred[F, Unit])]
  )

  object BoundedQueue {
    def empty[F[_], A](): BoundedQueue[F, A] =
      BoundedQueue(
        queue = StdQueue.empty[A],
        capacity = 100,
        takers = StdQueue.empty[Deferred[F, A]],
        offerers = StdQueue.empty[(A, Deferred[F, Unit])]
      )
  }

  def run(args: List[String]): IO[ExitCode] =
    for {
      stateR <- Ref.of[IO, BoundedQueue[IO, Int]](BoundedQueue.empty)
      counterR <- Ref.of[IO, Int](0)
      parallelism = 17 // Since I have 16 cores it makes sense ?
      producers = List.range(1, parallelism).map(producer(_, stateR, counterR))
      consumers = List.range(1, parallelism).map(consumer(_, stateR))
      watchers = List(queueWatcher(stateR))
      res <-
        /*
         * We create producers fibers and consumer fibers that we run in
         * parallel.
         */
        (producers ++ consumers ++ watchers).parSequence
          .as(ExitCode.Success)
          .handleErrorWith { t =>
            Console[IO]
              .errorln(s"Error caught: ${t.getMessage()}")
              .as(ExitCode.Error)
          }
    } yield res

  def queueWatcher[F[_]: Temporal: Console](
      queueR: Ref[F, BoundedQueue[F, Int]]
  ): F[Unit] =
    for {
      queue <- queueR.get
      _ <- Temporal[F].sleep(Duration(1, SECONDS))
      _ <- Console[F].println(s"\n{Watcher} queue state is : ${queue.queue.toString()}\n ")
      _ <- queueWatcher(queueR)
    } yield ()

  def producer[F[_]: Async: Console](
      id: Int,
      queueR: Ref[F, BoundedQueue[F, Int]],
      counterR: Ref[F, Int] // The counter is shared amongst multiple producers
  ): F[Unit] = {
    // TODO : Should this be a def or a val ?
    def offer(i: Int): F[Unit] =
      Deferred[F, Unit].flatMap[Unit] { offerer =>
        queueR.modify {
          case BoundedQueue(queue, capacity, takers, offerers)
              if takers.nonEmpty =>
            val (taker, rest) = takers.dequeue
            BoundedQueue(queue, capacity, rest, offerers) -> taker
              .complete(i)
              .void

          case BoundedQueue(queue, capacity, takers, offerers)
              if queue.size >= capacity =>
            BoundedQueue(
              queue,
              capacity,
              takers,
              offerers.enqueue(i -> offerer)
            ) -> offerer.get

          case BoundedQueue(queue, capacity, takers, offerers) =>
            BoundedQueue(
              queue.enqueue(i),
              capacity,
              takers,
              offerers
            ) -> Applicative[F].unit
        }.flatten
      }

    for {
      i <- counterR.getAndUpdate(_ + 1) // Shared counter is incremented
      _ <- offer(i)
      _ <-
        if (i % 10000 === 0)
          Console[F].println(s"{Producer $id} has reached $i items")
        else Sync[F].unit
      _ <- producer(id, queueR, counterR)
    } yield ()
  }

  def consumer[F[_]: Async: Console](
      id: Int,
      bQueueR: Ref[F, BoundedQueue[F, Int]]
  ): F[Unit] = {
    val take: F[Int] =
      for {
        taker <- Deferred[F, Int]
        offerer <- Deferred[F, Unit]
        result <- bQueueR
          .modify[F[Int]](bQueue =>
            bQueue match {
              case BoundedQueue(queue, capacity, takers, offerers)
                  if queue.nonEmpty =>
                if (offerers.nonEmpty) {
                  val (elem, queueRest) = queue.dequeue
                  val ((i, deferred), rest) = offerers.dequeue
                  BoundedQueue(
                    queueRest.enqueue(i),
                    capacity,
                    takers,
                    rest
                  ) -> deferred
                    .complete()
                    .as(elem)
                } else {
                  val (i, rest) = queue.dequeue
                  BoundedQueue(rest, capacity, takers, offerers) -> Applicative[
                    F
                  ]
                    .pure(i)
                }
              case BoundedQueue(queue, capacity, takers, offerers) =>
                if (offerers.nonEmpty) {
                  val ((i, offerer), rest) = offerers.dequeue
                  BoundedQueue(queue, capacity, takers, rest) -> offerer
                    .complete()
                    .as(i)
                } else {
                  BoundedQueue(
                    queue,
                    capacity,
                    takers.enqueue(taker),
                    offerers
                  ) -> taker.get
                }
            }
          )
          .flatten
      } yield result

    for {
      i <- take
      _ <-
        if (i % 10000 === 0)
          Console[F].println(s"{Consumer $id} has reached $i items")
        else Async[F].unit
      _ <- consumer(id, bQueueR)
    } yield ()
  }

}
