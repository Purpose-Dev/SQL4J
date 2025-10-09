package sql4j.memory.page.compaction

import sql4j.memory.MemoryPool
import sql4j.memory.off_heap.PageLayout
import sql4j.memory.page.{PageHeader, PageManager, SlotDirectory}
import zio.*
import zio.test.*

import java.nio.charset.StandardCharsets

object AsyncPageCompactorSpec extends ZIOSpecDefault:
		override def spec: Spec[TestEnvironment & Scope, Any] =
				suite("AsyncPageCompactorSpec")(
						test("async compaction reduces holes to zero and preserves data") {
								val pool = MemoryPool(1)
								val manager = PageManager(pool)
								val entry = manager.newPage()
								val buf = entry.buffer
								val header = PageHeader(buf)
								header.init()

								// write some records and remove some to create holes
								val n = 16
								val slotIds = (0 until n).map { i =>
										val s = ("R" + i.toString).getBytes(StandardCharsets.UTF_8)
										val freePtr = header.getFreeSpacePointer
										val newPtr = freePtr - s.length
										buf.position(newPtr)
										buf.put(s)
										val slot = SlotDirectory.allocSlot(buf, newPtr, s.length)
										header.setFreeSpacePointer(newPtr)
										slot
								}.toArray

								// remove even slots to create fragmentation
								(0 until n by 2).foreach { i =>
										SlotDirectory.removeSlot(buf, slotIds(i))
								}

								// keep expected data for odd slots
								val expected = (1 until n by 2).map { i =>
										val s = ("R" + i.toString).getBytes(StandardCharsets.UTF_8)
										slotIds(i) -> new String(s, StandardCharsets.UTF_8)
								}.toMap

								val cfg = AsyncCompactorConfig(maxBytesPerIteration = 64, pollIntervalMs = 2)

								ZIO.scoped {
										for
												comp <- AsyncPageCompactor.live(manager, cfg).build.map(_.get[AsyncPageCompactor])
												_ <- comp.requestCompaction(entry.id)

												isDone <- (
													TestClock.adjust(10.millis)
														*> ZIO.succeed(FragmentationStats.analyze(entry).holes == 0)
													)
													.repeat(Schedule.doWhileEquals(false) && Schedule.recurs(100))
													.flatMap { isCompacted =>
															if (isCompacted)
																	ZIO.unit
															else
																	ZIO.fail(new IllegalStateException("Compaction did not finish in 100 iterations."))
													}

												// verify data
												_ <- ZIO.foreachDiscard(expected) { case (slotId, str) =>
														val (off, len) = SlotDirectory.readSlot(buf, slotId).get
														val arr = new Array[Byte](len)
														buf.position(off)
														buf.get(arr)
														assertTrue(new String(arr, StandardCharsets.UTF_8) == str)
												}

												// verify freePtr is set to compacted position
												statsAfter = FragmentationStats.analyze(entry)
												_ <- ZIO.succeed(assertTrue(statsAfter.holes == 0))
										yield assertTrue(true)
								}
						}
				)