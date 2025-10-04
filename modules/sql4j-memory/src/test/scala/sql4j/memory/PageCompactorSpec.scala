package sql4j.memory

import sql4j.memory.page.{PageHeader, PageManager, SlotDirectory}
import sql4j.memory.page.compaction.{FragmentationStats, PageCompactor}
import zio.test.{assertTrue, Spec, TestEnvironment, ZIOSpecDefault}
import zio.Scope

import java.nio.charset.StandardCharsets

object PageCompactorSpec extends ZIOSpecDefault:
		override def spec: Spec[TestEnvironment & Scope, Any] =
				suite("PageCompactorSpec")(
						test("compact remove holes and preserves data") {
								val pool = MemoryPool(1)
								val page = PageManager(pool).newPage()

								val buffer = page.buffer
								val header = PageHeader(buffer)
								header.init()

								def writeRecord(data: Array[Byte]): Int =
										val freePtr = header.getFreeSpacePointer
										val newFreePtr = freePtr - data.length
										buffer.position(newFreePtr)
										buffer.put(data)
										val slotId = SlotDirectory.allocSlot(buffer, newFreePtr, data.length)
										header.setFreeSpacePointer(newFreePtr)
										slotId

								val recA = "AAA".getBytes(StandardCharsets.UTF_8)
								val recB = "BBBB".getBytes(StandardCharsets.UTF_8)
								val recC = "CCCCC".getBytes(StandardCharsets.UTF_8)

								val slotA = writeRecord(recA)
								val slotB = writeRecord(recB)
								val slotC = writeRecord(recC)

								// Remove middle record (slotB)
								assertTrue(SlotDirectory.removeSlot(buffer, slotB))

								val statsBefore = FragmentationStats.analyze(page)
								assertTrue(statsBefore.holes >= 1)

								val statsAfter = PageCompactor.compact(page)
								assertTrue(statsAfter.holes == 0)

								val aSlot = SlotDirectory.readSlot(buffer, slotA).get
								val cSlot = SlotDirectory.readSlot(buffer, slotC).get

								buffer.position(aSlot._1)
								val aBytes = new Array[Byte](aSlot._2)
								buffer.get(aBytes)

								buffer.position(cSlot._1)
								val cBytes = new Array[Byte](cSlot._2)
								buffer.get(cBytes)

								assertTrue(new String(aBytes, StandardCharsets.UTF_8).contentEquals("AAA"))
								assertTrue(new String(cBytes, StandardCharsets.UTF_8).contentEquals("CCCCC"))
						}
				)