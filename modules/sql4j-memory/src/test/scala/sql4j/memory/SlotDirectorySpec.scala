package sql4j.memory

import sql4j.memory.off_heap.PageLayout
import sql4j.memory.page.SlotDirectory
import zio.Scope
import zio.test.*
import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer

object SlotDirectorySpec extends ZIOSpecDefault:
		override def spec: Spec[TestEnvironment & Scope, Any] = suite("SlotDirectorySpec")(
				test("alloc/read/remove slot simple flow") {
						val buf = ByteBuffer.allocateDirect(PageLayout.PageSize)
						buf.putInt(PageLayout.HEADER_INDEX_NENTRIES, 0)

						val s0 = SlotDirectory.allocSlot(buf, PageLayout.HeaderBytes, 16)
						val s1 = SlotDirectory.allocSlot(buf, PageLayout.HeaderBytes + 32, 24)
						val s2 = SlotDirectory.allocSlot(buf, PageLayout.HeaderBytes + 100, 8)

						val check0 = SlotDirectory.readSlot(buf, s0) match
								case Some((off, len)) => assertTrue(off == PageLayout.HeaderBytes, len == 16)
								case None => assertTrue(false)
						val check1 = SlotDirectory.readSlot(buf, s1) match
								case Some((off, len)) => assertTrue(off == PageLayout.HeaderBytes + 32, len == 24)
								case None => assertTrue(false)

						val check2 = SlotDirectory.readSlot(buf, s2) match
								case Some((off, len)) => assertTrue(off == PageLayout.HeaderBytes + 100, len == 8)
								case None => assertTrue(false)

						SlotDirectory.removeSlot(buf, s1)
						val checkRemoved = assertTrue(SlotDirectory.readSlot(buf, s1).isEmpty)
						val s3 = SlotDirectory.allocSlot(buf, PageLayout.HeaderBytes + 200, 4)
						val checkReuse = assertTrue(s3 == s1)

						assertTrue(s0 == 0, s1 == 1, s2 == 2) &&
							check0 && check1 && check2 &&
							checkRemoved && checkReuse
				},
				test("foreachLiveSlot iterates only live slots") {
						val buf = ByteBuffer.allocateDirect(PageLayout.PageSize)
						buf.putInt(PageLayout.HEADER_INDEX_NENTRIES, 0)

						val a = SlotDirectory.allocSlot(buf, PageLayout.HeaderBytes, 4)
						val b = SlotDirectory.allocSlot(buf, PageLayout.HeaderBytes + 8, 4)
						SlotDirectory.removeSlot(buf, a)

						val collected = ArrayBuffer.empty[(Int, Int, Int)]
						SlotDirectory.foreachLiveSlot(buf) { (idx, off, len) =>
								collected += ((idx, off, len)); true
						}
						assertTrue(collected.size == 1, collected.head._1 == b)
				}
		)
