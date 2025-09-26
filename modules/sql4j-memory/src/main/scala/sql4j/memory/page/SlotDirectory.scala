package sql4j.memory.page

import sql4j.memory.off_heap.PageLayout
import java.nio.ByteBuffer
import scala.util.boundary

object SlotDirectory:
		private final val SLOT_BYTES: Int = 8
		private final val SLOT_OFFSET_OFFSET: Int = 0
		private final val SLOT_LENGTH_OFFSET: Int = 4
		private final val UNUSED_SENTINEL: Int = -1

		private inline def slotTableBase(): Int = PageLayout.HEADER_END

		private inline def maxSlots(): Int = (PageLayout.PageSize - slotTableBase()) / SLOT_BYTES

		private inline def slotEntryByteOffset(slotIndex: Int): Int = slotTableBase() + slotIndex * SLOT_BYTES

		private def slotCount(buf: ByteBuffer): Int =
				buf.getInt(PageLayout.HEADER_INDEX_NENTRIES).max(0).min(maxSlots())

		private def setSlotCount(buf: ByteBuffer, n: Int): Unit =
				require(n >= 0 && n <= maxSlots(), s"Invalid slot count: $n")
				buf.putInt(PageLayout.HEADER_INDEX_NENTRIES, n)

		private def writeSlot(buf: ByteBuffer, slotIndex: Int, offset: Int, length: Int): Unit =
				require(slotIndex >= 0 && slotIndex < maxSlots(), s"Invalid slot index: $slotIndex")
				val pos = slotEntryByteOffset(slotIndex)
				buf.putInt(pos + SLOT_OFFSET_OFFSET, offset)
				buf.putInt(pos + SLOT_LENGTH_OFFSET, length)

		private def findFreeSlot(buf: ByteBuffer): Int =
				val n = slotCount(buf)
				var i = 0
				while i < n do
						val pos = slotEntryByteOffset(i)
						if buf.getInt(pos + SLOT_OFFSET_OFFSET) == UNUSED_SENTINEL then
								return i
						i += 1
				-1

		def readSlot(buf: ByteBuffer, slotIndex: Int): Option[(Int, Int)] =
				val n = slotCount(buf)
				if slotIndex < 0 || slotIndex >= n then
						None
				else
						val pos = slotEntryByteOffset(slotIndex)
						val offset = buf.getInt(pos + SLOT_OFFSET_OFFSET)
						if offset == UNUSED_SENTINEL then
								None
						else
								Some((offset, buf.getInt(pos + SLOT_LENGTH_OFFSET)))

		def allocSlot(buf: ByteBuffer, offset: Int, length: Int): Int =
				findFreeSlot(buf) match
						case freeIndex if freeIndex >= 0 =>
								writeSlot(buf, freeIndex, offset, length)
								freeIndex
						case _ =>
								val currentCount = slotCount(buf)
								require(currentCount < maxSlots(), s"No more slots available (max: ${maxSlots()})")
								writeSlot(buf, currentCount, offset, length)
								setSlotCount(buf, currentCount + 1)
								currentCount

		def removeSlot(buf: ByteBuffer, slotIndex: Int): Boolean =
				val n = slotCount(buf)
				if slotIndex >= 0 && slotIndex < n then
						val pos = slotEntryByteOffset(slotIndex)
						buf.putInt(pos + SLOT_OFFSET_OFFSET, UNUSED_SENTINEL)
						buf.putInt(pos + SLOT_LENGTH_OFFSET, 0)
						true
				else
						false

		def foreachLiveSlot(buf: ByteBuffer)(f: (Int, Int, Int) => Boolean): Unit =
				val n = slotCount(buf)
				boundary:
						for i <- 0 until n do
								val pos = slotEntryByteOffset(i)
								val offset = buf.getInt(pos + SLOT_OFFSET_OFFSET)
								if offset != UNUSED_SENTINEL then
										val length = buf.getInt(pos + SLOT_LENGTH_OFFSET)
										if !f(i, offset, length) then
												boundary.break()
