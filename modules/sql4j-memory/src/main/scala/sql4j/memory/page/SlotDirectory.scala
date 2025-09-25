package sql4j.memory.page

import sql4j.memory.off_heap.PageLayout

import java.nio.ByteBuffer

/** SlotDirectory: manages fixed-size slot entries inside the page's index area.
 *
 * Layout assumptions:
 *  - Slot table begins at PageLayout.HEADER_INDEX_OFFSET (byte offset)
 *  - Each slot entry is 8 bytes: [offset:int][length:int]
 *  - Header field PageLayout.HEADER_INDEX_NENTRIES holds the current number of slot entries (int).
 *
 * Usage:
 *  - Caller must pin the page before mutating.
 *  - Methods operate on the provided ByteBuffer that must represent the page (position 0...PageSize-1).
 *
 * NOTE: This implementation is intentionally simple and low-level.
 * For multi-writer safety, coordinate via page-level CAS/lock in PageHeader (not provided here).
 */
object SlotDirectory:
		final val SLOT_BYTES: Int = 8
		final val SLOT_OFFSET_OFFSET: Int = 0
		final val SLOT_LENGTH_OFFSET: Int = 4
		final val UNUSED_SENTINEL: Int = -1


		/**
		 * @return the byte offset in buffer for slot `slotIndex`
		 */
		inline def slotEntryByteOffset(slotIndex: Int): Int =
				PageLayout.HEADER_INDEX_OFFSET + (slotIndex * SLOT_BYTES)

		/** Read current number of slot entries from header. */
		def slotCount(buf: ByteBuffer): Int =
				buf.getInt(PageLayout.HEADER_INDEX_NENTRIES)

		def setSlotCount(buf: ByteBuffer, n: Int): Unit =
				buf.putInt(PageLayout.HEADER_INDEX_NENTRIES, n)

		/**
		 * Ensure slot table capacity for at least `capacity` entries.
		 * This will not move data; caller must guarantee there is physical space in the page
		 * (i.e., index area + payload area do not overlap).
		 * Here we only update header count when expanding.
		 *
		 * @param buf
		 * @param capacity
		 */
		def ensureCapacity(buf: ByteBuffer, capacity: Int): Unit =
				val current = slotCount(buf)
				if capacity > current then
						setSlotCount(buf, capacity)
						// Initialize new slots to UNUSED_SENTINEL
						var i = current
						while i < capacity do
								val pos = slotEntryByteOffset(i)
								buf.putInt(pos + SLOT_OFFSET_OFFSET, UNUSED_SENTINEL)
								buf.putInt(pos + SLOT_LENGTH_OFFSET, 0)
								i += 1

		def readSlot(buf: ByteBuffer, slotIndex: Int): Option[(Int, Int)] =
				val pos = slotEntryByteOffset(slotIndex)
				val offset = buf.getInt(pos + SLOT_LENGTH_OFFSET)
				if offset eq UNUSED_SENTINEL then
						None
				else
						val length = buf.getInt(pos + SLOT_LENGTH_OFFSET)
						Some((offset, length))

		def writeSlot(buf: ByteBuffer, slotIndex: Int, offset: Int, length: Int): Unit =
				val pos = slotEntryByteOffset(slotIndex)
				buf.putInt(pos + SLOT_OFFSET_OFFSET, offset)
				buf.putInt(pos + SLOT_LENGTH_OFFSET, length)

		def findFreeSlot(buf: ByteBuffer): Int =
				val n = slotCount(buf)
				var i = 0
				while i < n do
						val pos = slotEntryByteOffset(i)
						if buf.getInt() == UNUSED_SENTINEL then
								return i
						i += 1
				-1

		def allocSlot(buf: ByteBuffer, offset: Int, length: Int): Int =
				val free = findFreeSlot(buf)
				if free >= 0 then
						writeSlot(buf, free, offset, length)
						free
				else
						val current = slotCount(buf)
						writeSlot(buf, current, offset, length)
						setSlotCount(buf, current + 1)
						current

		def removeSlot(buf: ByteBuffer, slotIndex: Int): Unit =
				val pos = slotEntryByteOffset(slotIndex)
				buf.putInt(pos + SLOT_OFFSET_OFFSET, UNUSED_SENTINEL)
				buf.putInt(pos + SLOT_LENGTH_OFFSET, 0)

		def foreachLiveSlot(buf: ByteBuffer)(f: (Int, Int, Int) => Boolean): Unit =
				val n = slotCount(buf)
				var i = 0
				while i < n do
						val pos = slotEntryByteOffset(i)
						val off = buf.getInt(pos + SLOT_OFFSET_OFFSET)
						if off != UNUSED_SENTINEL then
								val len = buf.getInt(pos + SLOT_LENGTH_OFFSET)
								val shouldContinue = f(i, off, len)
								if !shouldContinue then
										return
						i += 1