package sql4j.memory.page

import sql4j.memory.off_heap.PageLayout
import sql4j.memory.off_heap.PageLayout.MetaField
import sql4j.memory.off_heap.VarHandleHelpers

import java.nio.ByteBuffer

/**
 * PageHeader: helpers to read/write header fields inside a page ByteBuffer slice.
 *
 * Important: ByteBuffer slice passed here must be positioned at the start of page (0...PageSize-1).
 * VarHandleHelpers provides safe "AtByteOffset" wrappers that accept byte offsets.
 */
object PageHeader:
		private inline def assertPageCapacity(byteBuffer: ByteBuffer): Unit =
				if byteBuffer.capacity() < PageLayout.PageSize then
						throw new IllegalArgumentException(s"page buffer capacity ${byteBuffer.capacity()} < ${PageLayout.PageSize}")

final class PageHeader(private val buffer: ByteBuffer):
		PageHeader.assertPageCapacity(buffer)

		def init(): Unit =
				buffer.putInt(PageLayout.HEADER_INDEX_NENTRIES, 0)
				buffer.putInt(PageLayout.HEADER_FREE_POINTER_OFFSET, PageLayout.PageSize)

		// Helpers to write/read primitives at byte offsets (for simple fields)
		private inline def getIntAtByteOffset(offset: Int): Int =
				buffer.getInt(offset)

		private inline def putIntAtByteOffset(offset: Int, v: Int): Unit =
				buffer.putInt(offset, v)

		private inline def getLongAtByteOffset(offset: Int): Long =
				buffer.getLong(offset)

		private inline def putLongAtByteOffset(offset: Int, v: Long): Unit =
				buffer.putLong(offset, v)

		// Slot table start/end
		private inline def slotTableStart: Int = PageLayout.HEADER_END

		inline def slotTableEnd: Int = slotTableStart + getNEntries * PageLayout.SLOT_BYTES

		// pageId
		def setPageId(id: Long): Unit = putLongAtByteOffset(PageLayout.HEADER_PAGE_ID_OFFSET, id)

		def getPageId: Long = getLongAtByteOffset(PageLayout.HEADER_PAGE_ID_OFFSET)

		// segmentId
		def setSegmentId(id: Long): Unit = putLongAtByteOffset(PageLayout.HEADER_SEGMENT_ID_OFFSET, id)

		def getSegmentId: Long = getLongAtByteOffset(PageLayout.HEADER_SEGMENT_ID_OFFSET)

		// MetaAtomic stored as a long at HEADER_META_OFFSET; use safe wrappers that accept byte offset
		private inline def metaOffset: Int = PageLayout.HEADER_META_OFFSET

		def getMetaAtomicVolatile: Long = VarHandleHelpers.getVolatileLongAtByteOffset(buffer, metaOffset)

		private def compareAndSetMetaAtomic(expected: Long, update: Long): Boolean =
				VarHandleHelpers.compareAndSetLongAtByteOffset(buffer, metaOffset, expected, update)

		private inline def metaAddDelta(delta: Long): Long =
				VarHandleHelpers.getAndAddLongAtByteOffset(buffer, metaOffset, delta)

		def getPinnedCountFromMeta(meta: Long): Int =
				((meta & MetaField.PINNED_MASK) >>> MetaField.PINNED_SHIFT).toInt

		def tryPin(): Boolean =
				val maxPinned = (1L << MetaField.PINNED_BITS) - 1L

				@annotation.tailrec
				def loop(): Boolean =
						val cur = getMetaAtomicVolatile
						val pinned = getPinnedCountFromMeta(cur)
						if pinned >= maxPinned then
								false
						else
								val next = cur + (1L << MetaField.PINNED_SHIFT)
								if compareAndSetMetaAtomic(cur, next) then
										true
								else
										loop()

				loop()

		def unpin(): Unit =
				@annotation.tailrec
				def loop(): Unit =
						val current = getMetaAtomicVolatile
						val pinned = getPinnedCountFromMeta(current)
						if pinned <= 0 then
								throw new IllegalStateException("unpin underflow")
						val next = current - (1L << MetaField.PINNED_SHIFT)
						if !compareAndSetMetaAtomic(current, next) then
								loop()

				loop()

		// flags helpers (stored in metaAtomic)
		def setFlag(flagMask: Long): Unit =
				@annotation.tailrec
				def loop(): Unit =
						val current = getMetaAtomicVolatile
						val flagsPart = (current & MetaField.FLAGS_MASK) >>> MetaField.FLAGS_SHIFT
						val next = (current & ~MetaField.FLAGS_MASK) | (((flagsPart | flagMask) << MetaField.FLAGS_SHIFT)
							& MetaField.FLAGS_MASK)
						if !compareAndSetMetaAtomic(current, next) then
								loop()

				loop()

		def clearFlag(flagMask: Long): Unit =
				@annotation.tailrec
				def loop(): Unit =
						val current = getMetaAtomicVolatile
						val flagsPart = (current & MetaField.FLAGS_MASK) >>> MetaField.FLAGS_SHIFT
						val newFlags = flagsPart & (~flagMask)
						val next = (current & ~MetaField.FLAGS_MASK) | ((newFlags << MetaField.FLAGS_SHIFT) & MetaField.FLAGS_MASK)
						if !compareAndSetMetaAtomic(current, next) then
								loop()

				loop()

		def hasFlag(flagMask: Long): Boolean =
				val current = getMetaAtomicVolatile
				val flagsPart = (current & MetaField.FLAGS_MASK) >>> MetaField.FLAGS_SHIFT
				(flagsPart & flagMask) != 0L

		// LSN Field (long) at HEADER_LSN_OFFSET
		private inline def lsnOffset: Int = PageLayout.HEADER_LSN_OFFSET

		def setLsn(lsn: Long): Unit = VarHandleHelpers.setVolatileLongAtByteOffset(buffer, lsnOffset, lsn)

		def getLsn: Long = VarHandleHelpers.getVolatileLongAtByteOffset(buffer, lsnOffset)

		def getFreeSpacePointer: Int =
				buffer.getInt(PageLayout.HEADER_FREE_POINTER_OFFSET)

		def setFreeSpacePointer(ptr: Int): Unit =
				buffer.putInt(PageLayout.HEADER_FREE_POINTER_OFFSET, ptr)

		def getNEntries: Int =
				buffer.getInt(PageLayout.HEADER_INDEX_NENTRIES)

		def incrementNEntries(): Unit =
				val n: Int = getNEntries + 1
				buffer.putInt(PageLayout.HEADER_INDEX_NENTRIES, n)

		def canFit(size: Int): Boolean =
				getFreeSpacePointer - size >= slotTableEnd

		// Validation / Helpers (use in tests or debug)
		def validateLayout(): Unit =
				PageHeader.assertPageCapacity(buffer)
				val nEntries = getNEntries
				val maxSlots = (PageLayout.PageSize - PageLayout.HEADER_END) / PageLayout.SLOT_BYTES
				require(nEntries >= 0 && nEntries <= maxSlots, s"invalid slot count: '$nEntries' (maxSlots=$maxSlots)")
				val freePtr = getFreeSpacePointer
				require(freePtr >= PageLayout.HEADER_END && freePtr <= PageLayout.PageSize, s"invalid free pointer: '$freePtr'")
				val end = slotTableEnd
				require(end <= PageLayout.PageSize, s"slot table overflows page: end=$end capacity=${PageLayout.PageSize}")

		private inline def safePosition(pos: Int): Unit =
				if pos < 0 || pos > buffer.capacity() then
						throw new IllegalArgumentException(s"invalid buffer position '$pos' capacity=${buffer.capacity()}")
				buffer.position(pos)