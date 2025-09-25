package sql4j.memory

import sql4j.memory.off_heap.PageLayout
import sql4j.memory.page.SlotDirectory
import zio.Scope
import zio.test._

import java.nio.ByteBuffer

object SlotDirectorySpec extends ZIOSpecDefault:
		override def spec: Spec[TestEnvironment & Scope, Any] = suite("SlotDirectorySpec")(
				test("alloc/read/remove slot simple flow") {
						val buf = ByteBuffer.allocateDirect(PageLayout.PageSize)
						// prepare header: set slot count initially 0
						buf.putInt(PageLayout.HEADER_INDEX_NENTRIES, 0)

						// allocate three slots
						val s0 = SlotDirectory.allocSlot(buf, offset = PageLayout.HeaderBytes, length = 16)
						val s1 = SlotDirectory.allocSlot(buf, offset = PageLayout.HeaderBytes + 32, length = 24)
						val s2 = SlotDirectory.allocSlot(buf, offset = PageLayout.HeaderBytes + 100, length = 8)

						val check0 = SlotDirectory.readSlot(buf, s0) match
								case Some((o, l)) => assertTrue(o == PageLayout.HeaderBytes, l == 16)
								case None => assertTrue(false)
						val check1 = SlotDirectory.readSlot(buf, s1) match
								case Some((o, l)) => assertTrue(o == PageLayout.HeaderBytes + 32, l == 24)
								case None => assertTrue(false)

						// remove middle
						SlotDirectory.removeSlot(buf, s1)
						val checkRemoved = assertTrue(SlotDirectory.readSlot(buf, s1).isEmpty)
						val checkReuse = {
								val s3 = SlotDirectory.allocSlot(buf, offset = PageLayout.HeaderBytes + 200, length = 4)
								assertTrue(s3 == s1)
						}

						assertTrue(s0 == 0, s1 == 1, s2 == 2) &&
							check0 &&
							check1 &&
							checkRemoved &&
							checkReuse
				},
				test("foreachLiveSlot iterates only live slots") {
						val buf = ByteBuffer.allocateDirect(PageLayout.PageSize)
						buf.putInt(PageLayout.HEADER_INDEX_NENTRIES, 0)
						val a = SlotDirectory.allocSlot(buf, PageLayout.HeaderBytes + 0, 4)
						val b = SlotDirectory.allocSlot(buf, PageLayout.HeaderBytes + 8, 4)
						SlotDirectory.removeSlot(buf, a)

						var collected = scala.collection.mutable.ArrayBuffer.empty[(Int, Int, Int)]
						SlotDirectory.foreachLiveSlot(buf) { (idx, off, len) =>
								collected += ((idx, off, len)); true
						}
						assertTrue(collected.size == 1, collected.head._1 == b)
				}
		)
