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
						ZIO.logDebug(s"AsyncCompactor: starting compaction pageId=$id") *>
							ZIO.succeed(pm.currentEntry(id)).flatMap {
									case None => ZIO.unit
									case Some(entry) =>
											def loop: UIO[Unit] =
													ZIO.attempt(PageCompactor.compactStep(entry, cfg.maxBytesPerIteration)).foldZIO(
															e => ZIO.logDebug(s"Compaction step failed for $id: ${e.getMessage}").unit,
															{ case (_, done) =>
																	if done then
																			ZIO.unit
																	else
																			ZIO.yieldNow *> loop
															}
													)

											loop *> ZIO.logDebug(s"AsyncCompactor: finished compaction pageId=$id")
							}

				def loop: ZIO[Scope, Nothing, Unit] =
						running.get.flatMap {
								case false => ZIO.unit
								case true =>
										q.poll.flatMap {
												case Some(pageId) => processOnce(pageId)
												case None => ZIO.unit
										} *> ZIO.sleep(cfg.pollIntervalMs.millis) *> loop
						}

				loop