package sql4j.memory.page.compaction

import sql4j.memory.MemoryPool
import sql4j.memory.page.{PageEntry, PageHeader, PageManager, SlotDirectory}
import zio.*
import zio.test.*

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

object AsyncPageCompactorSpec extends ZIOSpecDefault:
		override def spec: Spec[TestEnvironment & Scope, Any] =
				suite("AsyncPageCompactorSpec")(
						test("async compaction reduces holes to zero and preserves data") {
								for
										// setup pool and page
										pool <- ZIO.succeed(MemoryPool(1))
										manager <- ZIO.succeed(PageManager(pool))
										entry <- ZIO.succeed(manager.newPage())
										buf = entry.buffer
										header = PageHeader(buf)
										_ <- ZIO.succeed(header.init())

										// fill with stub records
										n = 16
										slotIds <- ZIO.succeed {
												(0 until n).map { i =>
														val s = ("R" + i.toString).getBytes(StandardCharsets.UTF_8)
														val freePtr = header.getFreeSpacePointer
														val newPtr = freePtr - s.length
														buf.position(newPtr)
														buf.put(s)
														val slot = SlotDirectory.allocSlot(buf, newPtr, s.length)
														header.setFreeSpacePointer(newPtr)
														slot
												}.toArray
										}

										// remove even slots to create fragmentation
										_ <- ZIO.succeed {
												(0 until n by 2).foreach { i =>
														SlotDirectory.removeSlot(buf, slotIds(i))
												}
										}

										statsBefore <- ZIO.attempt(FragmentationStats.analyze(entry))
										_ <- ZIO.logInfo(
												s"""Before compaction: holes=${statsBefore.holes}, 
													 fragmentation=${statsBefore.fragmentationRatio}"""
										)

										expected <- ZIO.succeed {
												(1 until n by 2).map { i =>
														val s = ("R" + i.toString).getBytes(StandardCharsets.UTF_8)
														slotIds(i) -> new String(s, StandardCharsets.UTF_8)
												}.toMap
										}

										// Create the compactor with faster polling for testing
										cfg = AsyncCompactorConfig(maxBytesPerIteration = 64, pollIntervalMs = 50)

										// Use ZIO.scoped properly and keep the compactor alive for the test
										result <- ZIO.scoped {
												AsyncPageCompactor.live(manager, cfg).build.flatMap { env =>
														val comp = env.get[AsyncPageCompactor]
														for
																_ <- comp.requestCompaction(entry.id)
																_ <- ZIO.logInfo(s"Compaction requested for page ${entry.id}")

																_ <- ZIO.sleep(100.millis)
																_ <- waitForCompactionLive(entry, maxWaitMs = 5000)

																// verify content
																allOk <- ZIO.foreach(expected.toList) { case (slotId, str) =>
																		ZIO.attempt {
																				val (off, len) = SlotDirectory.readSlot(buf, slotId).get
																				val arr = new Array[Byte](len)
																				buf.position(off)
																				buf.get(arr)
																				new String(arr, StandardCharsets.UTF_8) == str
																		}
																}.map(_.forall(identity))

																statsAfter <- ZIO.attempt(FragmentationStats.analyze(entry))
																_ <- ZIO.logInfo(
																		s"""After compaction: holes=${statsAfter.holes},
																		fragmentation=${statsAfter.fragmentationRatio}"""
																)
														yield (allOk, statsAfter.holes, statsBefore.holes > 0)

												}
										}

										(allOk, holes, hadFragmentation) = result
								yield assertTrue(hadFragmentation && allOk && holes == 0)
						} @@ TestAspect.withLiveClock,

						test("synchronous compaction works") {
								for
										// setup pool and page
										pool <- ZIO.succeed(new MemoryPool(1))
										manager <- ZIO.succeed(new PageManager(pool))
										entry <- ZIO.succeed(manager.newPage())
										buf = entry.buffer
										header = PageHeader(buf)
										_ <- ZIO.succeed(header.init())

										// fill with dummy records
										n = 16
										slotIds <- ZIO.succeed {
												(0 until n).map { i =>
														val s = ("R" + i.toString).getBytes(StandardCharsets.UTF_8)
														val freePtr = header.getFreeSpacePointer
														val newPtr = freePtr - s.length
														buf.position(newPtr)
														buf.put(s)
														val slot = SlotDirectory.allocSlot(buf, newPtr, s.length)
														header.setFreeSpacePointer(newPtr)
														slot
												}.toArray
										}

										// remove even slots
										_ <- ZIO.succeed {
												(0 until n by 2).foreach { i =>
														SlotDirectory.removeSlot(buf, slotIds(i))
												}
										}

										expected <- ZIO.succeed {
												(1 until n by 2).map { i =>
														val s = ("R" + i.toString).getBytes(StandardCharsets.UTF_8)
														slotIds(i) -> new String(s, StandardCharsets.UTF_8)
												}.toMap
										}

										// Use synchronous compaction
										cfg = AsyncCompactorConfig()
										result <- ZIO.scoped {
												AsyncPageCompactor.live(manager, cfg).build.flatMap { env =>
														val comp = env.get[AsyncPageCompactor]
														for
																stats <- comp.compactNow(entry.id)

																allOk <- ZIO.foreach(expected.toList) { case (slotId, str) =>
																		ZIO.attempt {
																				val (off, len) = SlotDirectory.readSlot(buf, slotId).get
																				val arr = new Array[Byte](len)
																				buf.position(off)
																				buf.get(arr)
																				new String(arr, StandardCharsets.UTF_8) == str
																		}
																}.map(_.forall(identity))
														yield (allOk, stats.holes)
												}
										}

										(allOk, holes) = result
								yield assertTrue(allOk && holes == 0)
						}
				)

		private def waitForCompactionLive(entry: PageEntry, maxWaitMs: Long): ZIO[Any, Throwable, Unit] =
				def checkWithTimeout(deadline: Long, attempt: Int): ZIO[Any, Throwable, Unit] =
						for
								now <- Clock.currentTime(TimeUnit.MILLISECONDS)
								_ <- if now > deadline then
										ZIO.attempt(FragmentationStats.analyze(entry)).flatMap { finalStats =>
												ZIO.fail(new IllegalStateException(
														s"Compaction did not finish within ${maxWaitMs}ms after $attempt attempts. " +
															s"Final state: holes=${finalStats.holes}, fragmentation=${finalStats.fragmentationRatio}"
												))
										}
								else
										ZIO.attempt(FragmentationStats.analyze(entry)).flatMap { stats =>
												if stats.holes == 0 then
														ZIO.logInfo(s"Compaction completed after $attempt attempts")
												else
														ZIO.logDebug(s"Attempt $attempt: holes=${stats.holes}, sleeping...") *>
															ZIO.sleep(50.millis) *>
															checkWithTimeout(deadline, attempt + 1)
										}
						yield ()

				Clock.currentTime(TimeUnit.MILLISECONDS)
					.flatMap(now => checkWithTimeout(now + maxWaitMs, 1))
