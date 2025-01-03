package example

import cats._
import cats.syntax.all._
import cats.effect._
import cats.effect.std._
import cats.effect.syntax.all._
import scala.collection.immutable
import java.io.{
  File,
  InputStream,
  OutputStream,
  FileInputStream,
  FileOutputStream
}
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit
import java.io.IOException

object Transfer extends IOApp {

  def run(args: List[String]): IO[ExitCode] =
    args match {
      case origin :: destination :: "--buffer_size" :: bufSize :: _ =>
        copy[IO](new File(origin), new File(destination), bufSize.toInt) >> IO
          .pure(
            ExitCode.Success
          )
      case origin :: destination :: _ if origin === destination =>
        IO.blocking(
          Console[IO].errorln(
            "Destination and origin files cannot be the same."
          )
        ) >> IO.pure(
          ExitCode.Error
        )
      case origin :: destination :: _ =>
        copy[IO](new File(origin), new File(destination), 4096) >> IO.pure(
          ExitCode.Success
        )
      case _ => IO.pure(ExitCode.Error)
    }

  def copy[F[_]: Async: Console](
      origin: File,
      destination: File,
      bufSize: Int
  ): F[Long] =
    inputOutputStream(origin, destination).use(
      (t: Tuple2[InputStream, OutputStream]) => transfer(t._1, t._2, bufSize)
    )

  def transfer[F[_]: Async](
      inStream: InputStream,
      outStream: OutputStream,
      bufSize: Int
  ): F[Long] =
    transmit(inStream, outStream, new Array(bufSize), 0)

  def transmit[F[_]](
      inStream: InputStream,
      outStream: OutputStream,
      buffer: Array[Byte],
      acc: Long
  )(implicit F: Async[F]): F[Long] =
    for {
      bytesRead <- F.blocking(inStream.read(buffer))
      // Adds a second of delay for each transfer
      _ <- F.sleep(new FiniteDuration(1, TimeUnit.SECONDS))
      result <-
        if (bytesRead > -1)
          F.blocking(outStream.write(buffer)) >> transmit(
            inStream,
            outStream,
            buffer,
            acc + bytesRead
          )
        else
          F.pure(acc)
    } yield result

  def inputOutputStream[F[_]: Sync : Console](
      in: File,
      out: File
  ): Resource[F, (InputStream, OutputStream)] =
    for {
      inStream <- inputStream(in)
      outStream <- outputStream(out)
    } yield (inStream, outStream)

  def inputStream[F[_]: Console](f: File)(implicit
      F: Sync[F]
  ): Resource[F, InputStream] =
    Resource.make(
      F.blocking(new FileInputStream(f))
    )((inputStream: FileInputStream) =>
      F.blocking(inputStream.close())
        .handleErrorWith((err: Throwable) =>
          Console[F].errorln(s"Error: InputStream ${err.getMessage()}")
        )
        >> Console[F].println("Closing input stream")
    )

  def outputStream[F[_]: Console](f: File)(implicit
      F: Sync[F]
  ): Resource[F, OutputStream] =
    Resource.make(
      F.blocking(new FileOutputStream(f))
    )((ouputStream: FileOutputStream) =>
      F.blocking(ouputStream.close())
        .handleErrorWith((err: Throwable) =>
          F.blocking(
            Console[F].println(s"Error: OutputStream ${err.getMessage()}")
          )
        )
        >> Console[F].println("Closing input stream"))

}

