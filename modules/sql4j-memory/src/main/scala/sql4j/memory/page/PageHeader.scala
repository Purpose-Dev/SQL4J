package sql4j.memory.page

import sql4j.memory.off_heap.*
import sql4j.memory.off_heap.PageLayout.MetaField
import sql4j.memory.off_heap.VarHandleHelpers.*

import java.nio.ByteBuffer

/**
 * PageHeader: helpers to read/write header fields inside a page ByteBuffer slice.
 *
 * Important: ByteBuffer slice passed here must be positioned at the start of page (0...PageSize-1).
 * VarHandleHelpers uses element indexes (int/long indices) not byte offsets.
 */
//noinspection ScalaWeakerAccess
object PageHeader:
		inline def assertPageCapacity(byteBuffer: ByteBuffer): Unit =
				if byteBuffer.capacity() < PageLayout.PageSize then
						throw new IllegalArgumentException(s"page buffer capacity ${byteBuffer.capacity()} < ${PageLayout.PageSize}")

// TODO; add buffer alignment after assert of page capacity
final class PageHeader(private val buffer: ByteBuffer):
		PageHeader.assertPageCapacity(buffer)

		// Helpers to write/read primitives at byte offsets (for simple fields)
		private inline def getIntAtByteOffset(offset: Int): Int =
				buffer.getInt(offset)

		private inline def putIntAtByteOffset(offset: Int, v: Int): Unit =
				buffer.putInt(offset, v)

		private inline def getLongAtByteOffset(offset: Int): Long =
				buffer.getLong(offset)

		private inline def putLongAtByteOffset(offset: Int, v: Long): Unit =
				buffer.putLong(offset, v)

		// pageId
		def setPageId(id: Long): Unit = putLongAtByteOffset(PageLayout.HEADER_PAGE_ID_OFFSET, id)

		def getPageId: Long = getLongAtByteOffset(PageLayout.HEADER_PAGE_ID_OFFSET)

		// segmentId
		def setSegmentId(id: Long): Unit = putLongAtByteOffset(PageLayout.HEADER_SEGMENT_ID_OFFSET, id)

		def getSegmentId: Long = getLongAtByteOffset(PageLayout.HEADER_SEGMENT_ID_OFFSET)

		// MetaAtomic stored as a long at HEADER_META_OFFSET; we want CAS/pin increments on that field.
		// Use VarHandleHelpers.LONG_VH operating on long-array view; convert byte-offset -> long-element index:
		private inline def longIndexForMeta(): Int = PageLayout.HEADER_META_OFFSET / java.lang.Long.BYTES

		def getMetaAtomicVolatile: Long =
				// element index must be Int
				getVolatileLong(buffer, longIndexForMeta())

		private def compareAndSetMetaAtomic(expected: Long, update: Long): Boolean =
				compareAndSetLong(buffer, longIndexForMeta(), expected, update)

		private inline def metaAddDelta(delta: Long): Long =
				getAndAddLong(buffer, longIndexForMeta(), delta)

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
						val next = (current & ~MetaField.FLAGS_MASK) | (((flagsPart | flagMask)
							<< MetaField.FLAGS_SHIFT) & MetaField.FLAGS_MASK)
						if !compareAndSetMetaAtomic(current, next) then
								loop()

				loop()

		def clearFlag(flagMask: Long): Unit =
				@annotation.tailrec
				def loop(): Unit =
						val current = getMetaAtomicVolatile
						val flagsPart = (current & MetaField.FLAGS_MASK) >>> MetaField.FLAGS_SHIFT
						val newFlags = flagsPart & (~flagMask)
						val next = (current & ~MetaField.FLAGS_MASK)
							| ((newFlags << MetaField.FLAGS_SHIFT) & MetaField.FLAGS_MASK)
						if !compareAndSetMetaAtomic(current, next) then
								loop()

				loop()

		def hasFlag(flagMask: Long): Boolean =
				val current = getMetaAtomicVolatile
				val flagsPart = (current & MetaField.FLAGS_MASK) >>> MetaField.FLAGS_SHIFT
				(flagsPart & flagMask) != 0L

		// LSN Field (long) at HEADER_LSN_OFFSET
		private inline def longIndexForLsn(): Int = PageLayout.HEADER_LSN_OFFSET / java.lang.Long.BYTES

		def setLsn(lsn: Long): Unit = setVolatileLong(buffer, longIndexForLsn(), lsn)

		def getLsn: Long = getVolatileLong(buffer, longIndexForLsn())