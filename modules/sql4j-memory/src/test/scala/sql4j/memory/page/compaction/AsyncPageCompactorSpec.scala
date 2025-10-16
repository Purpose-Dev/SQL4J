package sql4j.memory.page.compaction

import sql4j.memory.MemoryPool
import sql4j.memory.page.{PageEntry, PageHeader, PageManager, SlotDirectory}
import zio._
import zio.test._

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

object AsyncPageCompactorSpec extends ZIOSpecDefault:
		override def spec: Spec[TestEnvironment & Scope, Any] =
				suite("AsyncPageCompactorSpec")(
						test("diagnose compaction issue") {
								for
										pool <- ZIO.succeed(new MemoryPool(1))
										manager <- ZIO.succeed(new PageManager(pool))
										entry <- ZIO.succeed(manager.newPage())
										buf = entry.buffer
										header = PageHeader(buf)
										_ <- ZIO.succeed(header.init())

										// Create just 4 records to match the 4 holes
										n = 4
										slotIds <- ZIO.succeed {
												(0 until n).map { i =>
														val s = s"REC$i".getBytes(StandardCharsets.UTF_8)
														val freePtr = header.getFreeSpacePointer
														val newPtr = freePtr - s.length
														buf.position(newPtr)
														buf.put(s)
														val slot = SlotDirectory.allocSlot(buf, newPtr, s.length)
														header.setFreeSpacePointer(newPtr)
														slot
												}.toArray
										}

										_ <- ZIO.logInfo(s"Initial freePtr: ${header.getFreeSpacePointer}")

										// Remove slots 0 and 2
										_ <- ZIO.succeed {
												SlotDirectory.removeSlot(buf, slotIds(0))
												SlotDirectory.removeSlot(buf, slotIds(2))
										}

										_ <- ZIO.logInfo(s"After removal freePtr: ${header.getFreeSpacePointer}")

										// Log slot directory state
										_ <- ZIO.succeed {
												(0 until 4).foreach { i =>
														SlotDirectory.readSlot(buf, i) match {
																case Some((off, len)) => println(s"Slot $i: offset=$off, length=$len")
																case None => println(s"Slot $i: EMPTY")
														}
												}
										}

										statsBefore <- ZIO.attempt(FragmentationStats.analyze(entry))
										_ <- ZIO.logInfo(s"Before compact: holes=${statsBefore.holes}, frag=${statsBefore.fragmentationRatio}")

										// Try compact
										statsAfter <- ZIO.attempt(PageCompactor.compact(entry))
										_ <- ZIO.logInfo(s"After compact: holes=${statsAfter.holes}, frag=${statsAfter.fragmentationRatio}")
										_ <- ZIO.logInfo(s"After compact freePtr: ${header.getFreeSpacePointer}")

										// Log slot directory state after compaction
										_ <- ZIO.succeed {
												(0 until 4).foreach { i =>
														SlotDirectory.readSlot(buf, i) match {
																case Some((off, len)) => println(s"After Slot $i: offset=$off, length=$len")
																case None => println(s"After Slot $i: EMPTY")
														}
												}
										}

								yield assertTrue(statsAfter.holes == 0)
						},

						test("compactStep makes progress incrementally") {
								for
										pool <- ZIO.succeed(new MemoryPool(1))
										manager <- ZIO.succeed(new PageManager(pool))
										entry <- ZIO.succeed(manager.newPage())
										buf = entry.buffer
										header = PageHeader(buf)
										_ <- ZIO.succeed(header.init())

										// Create 8 records
										n = 8
										slotIds <- ZIO.succeed {
												(0 until n).map { i =>
														val s = ("Record" + i.toString).getBytes(StandardCharsets.UTF_8)
														val freePtr = header.getFreeSpacePointer
														val newPtr = freePtr - s.length
														buf.position(newPtr)
														buf.put(s)
														val slot = SlotDirectory.allocSlot(buf, newPtr, s.length)
														header.setFreeSpacePointer(newPtr)
														slot
												}.toArray
										}

										// Remove every other slot to create fragmentation
										_ <- ZIO.succeed {
												(0 until n by 2).foreach { i =>
														SlotDirectory.removeSlot(buf, slotIds(i))
												}
										}

										statsBefore <- ZIO.attempt(FragmentationStats.analyze(entry))
										_ <- ZIO.logInfo(s"Before: holes=${statsBefore.holes}")

										// Manually call compactStep with a small budget
										step1 <- ZIO.attempt(PageCompactor.compactStep(entry, budgetBytes = 20))
										(processed1, done1) = step1
										stats1 <- ZIO.attempt(FragmentationStats.analyze(entry))
										_ <- ZIO.logInfo(s"Step 1: processed=$processed1, done=$done1, holes=${stats1.holes}")

										// Call again if not done
										step2 <- ZIO.attempt(PageCompactor.compactStep(entry, budgetBytes = 20))
										(processed2, done2) = step2
										stats2 <- ZIO.attempt(FragmentationStats.analyze(entry))
										_ <- ZIO.logInfo(s"Step 2: processed=$processed2, done=$done2, holes=${stats2.holes}")

										// Call again if not done
										step3 <- ZIO.attempt(PageCompactor.compactStep(entry, budgetBytes = 20))
										(processed3, done3) = step3
										stats3 <- ZIO.attempt(FragmentationStats.analyze(entry))
										_ <- ZIO.logInfo(s"Step 3: processed=$processed3, done=$done3, holes=${stats3.holes}")

										// Keep calling until done
										finalStats <- callUntilDone(entry, maxSteps = 10)
										_ <- ZIO.logInfo(s"Final: holes=${finalStats.holes}")

								yield assertTrue(
										statsBefore.holes > 0 &&
											finalStats.holes == 0
								)
						},

						test("full compact works") {
								for
										pool <- ZIO.succeed(new MemoryPool(1))
										manager <- ZIO.succeed(new PageManager(pool))
										entry <- ZIO.succeed(manager.newPage())
										buf = entry.buffer
										header = PageHeader(buf)
										_ <- ZIO.succeed(header.init())

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

										// Use full compact
										stats <- ZIO.attempt(PageCompactor.compact(entry))

										allOk <- ZIO.foreach(expected.toList) { case (slotId, str) =>
												ZIO.attempt {
														SlotDirectory.readSlot(buf, slotId) match {
																case Some((off, len)) =>
																		val arr = new Array[Byte](len)
																		buf.position(off)
																		buf.get(arr)
																		val actual = new String(arr, StandardCharsets.UTF_8)
																		val matches = actual == str
																		if !matches then
																				println(s"FULL COMPACT MISMATCH slot $slotId: expected '$str', got '$actual'")
																		matches
																case None =>
																		println(s"FULL COMPACT MISSING slot $slotId: expected '$str'")
																		false
														}
												}
										}.map(_.forall(identity))

										_ <- ZIO.logInfo(s"Full compact: holes=${stats.holes}, allOk=$allOk")
								yield assertTrue(allOk && stats.holes == 0)
						},

						test("async compaction reduces holes to zero and preserves data") {
								for
										pool <- ZIO.succeed(new MemoryPool(1))
										manager <- ZIO.succeed(new PageManager(pool))
										entry <- ZIO.succeed(manager.newPage())
										buf = entry.buffer
										header = PageHeader(buf)
										_ <- ZIO.succeed(header.init())

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

										_ <- ZIO.succeed {
												(0 until n by 2).foreach { i =>
														SlotDirectory.removeSlot(buf, slotIds(i))
												}
										}

										statsBefore <- ZIO.attempt(FragmentationStats.analyze(entry))
										_ <- ZIO.logInfo(s"Before async: holes=${statsBefore.holes}")

										expected <- ZIO.succeed {
												(1 until n by 2).map { i =>
														val s = ("R" + i.toString).getBytes(StandardCharsets.UTF_8)
														slotIds(i) -> new String(s, StandardCharsets.UTF_8)
												}.toMap
										}

										cfg = AsyncCompactorConfig(maxBytesPerIteration = 64, pollIntervalMs = 75)

										result <- ZIO.scoped {
												AsyncPageCompactor.live(manager, cfg).build.flatMap { env =>
														val comp = env.get[AsyncPageCompactor]
														for
																_ <- comp.requestCompaction(entry.id)
																_ <- ZIO.logInfo(s"Compaction requested for page ${entry.id}")
																_ <- ZIO.sleep(100.millis)
																_ <- waitForCompactionLive(entry, maxWaitMs = 5000)

																allOk <- ZIO.foreach(expected.toList) { case (slotId, str) =>
																		ZIO.attempt {
																				SlotDirectory.readSlot(buf, slotId) match {
																						case Some((off, len)) =>
																								val arr = new Array[Byte](len)
																								buf.position(off)
																								buf.get(arr)
																								val actual = new String(arr, StandardCharsets.UTF_8)
																								val matches = actual == str
																								if !matches then
																										println(s"MISMATCH slot $slotId: expected '$str', got '$actual'")
																								matches
																						case None =>
																								println(s"MISSING slot $slotId: expected '$str'")
																								false
																				}
																		}
																}.map(_.forall(identity))

																statsAfter <- ZIO.attempt(FragmentationStats.analyze(entry))
																_ <- ZIO.logInfo(s"After async: holes=${statsAfter.holes}")

														yield (allOk, statsAfter.holes, statsBefore.holes > 0)
												}
										}

										(allOk, holes, hadFragmentation) = result
								yield assertTrue(
										hadFragmentation,
										allOk,
										holes == 0
								) && assertCompletes
						} @@ TestAspect.withLiveClock
				)

		private def callUntilDone(entry: PageEntry, maxSteps: Int): ZIO[Any, Throwable, FragmentationStats] =
				if maxSteps <= 0 then
						ZIO.attempt(FragmentationStats.analyze(entry))
				else
						ZIO.attempt(PageCompactor.compactStep(entry, budgetBytes = 50)).flatMap { case (_, done) =>
								if done then
										ZIO.attempt(FragmentationStats.analyze(entry))
								else
										callUntilDone(entry, maxSteps - 1)
						}

		private def waitForCompactionLive(entry: PageEntry, maxWaitMs: Long): ZIO[Any, Throwable, Unit] =
				def checkWithTimeout(deadline: Long, attempt: Int): ZIO[Any, Throwable, Unit] =
						for
								now <- Clock.currentTime(TimeUnit.MILLISECONDS)
								_ <- if now > deadline then
										ZIO.attempt(FragmentationStats.analyze(entry)).flatMap { finalStats =>
												ZIO.fail(new IllegalStateException(
														s"Compaction did not finish within ${maxWaitMs}ms after $attempt attempts. " +
															s"Final state: holes=${finalStats.holes}, used=${finalStats.usedSpace}, free=${finalStats.freeSpace}, ratio=${finalStats.fragmentationRatio}"
												))
										}
								else
										ZIO.attempt(FragmentationStats.analyze(entry)).flatMap { stats =>
												val done = stats.holes == 0 && stats.freeSpace == stats.usedSpace
												if done then
														ZIO.logInfo(s"Compaction completed after $attempt attempts: holes=0, used=${stats.usedSpace}, free=${stats.freeSpace}")
												else
														ZIO.logDebug(s"Attempt $attempt: holes=${stats.holes}, used=${stats.usedSpace}, free=${stats.freeSpace}") *>
															ZIO.sleep(50.millis) *>
															checkWithTimeout(deadline, attempt + 1)
										}
						yield ()

				Clock.currentTime(TimeUnit.MILLISECONDS)
					.flatMap(now => checkWithTimeout(now + maxWaitMs, 1))
