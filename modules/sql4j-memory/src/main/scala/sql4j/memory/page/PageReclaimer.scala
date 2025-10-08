package sql4j.memory.page

import sql4j.memory.MemoryPool
import sql4j.memory.off_heap.PageLayout
import zio.{durationLong, Ref, UIO, ZIO, ZLayer}

// @formatter:off
final case class PageReclaimerConfig(
		scanIntervalMs: Long = 500L,
		maxPagesPerScan: Int = 100
)
// @formatter:on

trait PageReclaimer:
		def start: UIO[Unit]

		def stop: UIO[Unit]

object PageReclaimer:
		def live(pm: PageManager, pool: MemoryPool, cfg: PageReclaimerConfig = PageReclaimerConfig()): ZLayer[Any, Nothing, PageReclaimer] =
				ZLayer.scoped {
						for {
								running <- Ref.make(false)
								fiber <- (reclaimerLoop(pm, pool, running, cfg)).forever.forkScoped
						} yield new PageReclaimer:
								override def start: UIO[Unit] = running.set(true)

								override def stop: UIO[Unit] = running.set(false)
				}

		private def reclaimerLoop(pm: PageManager, pool: MemoryPool, running: Ref[Boolean], cfg: PageReclaimerConfig): ZIO[Any, Nothing, Unit] =
				for {
						r <- running.get
						_ <- if !r then
								ZIO.yieldNow
						else {
								val candidates = pm.snapshotEntries().take(cfg.maxPagesPerScan)
								ZIO.foreachDiscard(candidates) { entry =>
										val header = PageHeader(entry.buffer)
										val nEntries = header.getNEntries
										val freePtr = header.getFreeSpacePointer

										if nEntries == 0 && freePtr == PageLayout.PageSize && entry.meta.pinnedCount() == 0 then
												ZIO.attempt {
														pm.free(entry.id)
												}.catchAll(_ => ZIO.unit)
										else
												ZIO.unit
								}
						} *> ZIO.sleep(cfg.scanIntervalMs.millis)
				} yield ()
