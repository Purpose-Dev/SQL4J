package sql4j.memory.wal

import zio.{UIO, ZIO, ZLayer}

import java.util.concurrent.atomic.AtomicLong

trait WALService:
		def append(bytes: Array[Byte]): UIO[Long]

		def lastLsn(): UIO[Long]

object WALService:
		def append(bytes: Array[Byte]): ZIO[WALService, Nothing, Long] = ZIO.serviceWithZIO[WALService](_.append(bytes))

		def lastLsn(): ZIO[WALService, Nothing, Long] = ZIO.serviceWithZIO[WALService](_.lastLsn())

		val inMemory: ZLayer[Any, Nothing, WALService] = ZLayer.succeed(new WALService {
				private val seq = AtomicLong(0L)

				override def append(bytes: Array[Byte]): UIO[Long] = ZIO.succeed(seq.incrementAndGet())

				override def lastLsn(): UIO[Long] = ZIO.succeed(seq.get())
		})