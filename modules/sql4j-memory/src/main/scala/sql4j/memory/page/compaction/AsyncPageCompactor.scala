package sql4j.memory.page.compaction

import sql4j.core.PageId
import sql4j.memory.page.PageManager
import zio.{durationLong, Queue, Ref, Scope, Task, UIO, ZIO, ZLayer}

// @formatter:off
final case class AsyncCompactorConfig(
		maxBytesPerIteration: Int = 4 * 1024,
		pollIntervalMs: Long = 50L,
		maxConcurrentCompaction: Int = 1																 
)
// @formatter:on

trait AsyncPageCompactor:
		def requestCompaction(pageId: PageId): UIO[Unit]

		def compactNow(pageId: PageId): Task[FragmentationStats]

		def shutdown: UIO[Unit]

object AsyncPageCompactor:
		def live(pageManager: PageManager, cfg: AsyncCompactorConfig = AsyncCompactorConfig()): ZLayer[Any, Nothing, AsyncPageCompactor] =
				ZLayer.scoped {
						for {
								q <- Queue.unbounded[PageId]
								ref <- Ref.make(true)
								worker <- (workerLoop(pageManager, q, ref, cfg).forever.forkScoped)
						} yield new AsyncPageCompactor:
								override def requestCompaction(pageId: PageId): UIO[Unit] = q.offer(pageId).unit

								override def compactNow(pageId: PageId): Task[FragmentationStats] =
										ZIO.attemptBlocking {
												val entry = pageManager.currentEntry(pageId)
													.getOrElse(throw new NoSuchElementException(s"Page $pageId not found"))
												PageCompactor.compact(entry)
										}

								override def shutdown: UIO[Unit] = ref.set(false) *> q.shutdown
				}

		private def workerLoop(pm: PageManager, q: Queue[PageId], running: Ref[Boolean], cfg: AsyncCompactorConfig): ZIO[Scope, Nothing, Unit] =
				def processOnce(id: PageId): UIO[Unit] =
						ZIO.logDebug(s"AsyncCompactor: compacting pageId=$id") *>
							ZIO.attempt {
									pm.currentEntry(id).foreach(PageCompactor.compact)
							}.catchAll(e => ZIO.logDebug(s"Compaction failed for $id: ${e.getMessage}")).ignore

				def loop: ZIO[Scope, Nothing, Unit] =
						running.get.flatMap {
								case false => ZIO.unit
								case true =>
										q.take.flatMap(processOnce) *> ZIO.sleep(cfg.pollIntervalMs.millis) *> loop
						}

				loop