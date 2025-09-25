package sql4j.memory.page

import sql4j.memory.off_heap.PageLayout
import sql4j.memory.off_heap.PageLayout.MetaField
import sql4j.memory.off_heap.VarHandleHelpers.*

import java.nio.ByteBuffer

/**
 * PageHeader: helpers to read/write header fields inside a page ByteBuffer slice.
 *
 * Important: ByteBuffer slice passed here must be positioned at the start of page (0..PageSize-1).
 * VarHandleHelpers uses element indexes (int/long indices) not byte offsets.
 */
//noinspection ScalaWeakerAccess
object PageHeader:
		inline def assertPageCapacity(byteBuffer: ByteBuffer): Unit =
				if byteBuffer.capacity() < PageLayout.PageSize then
						throw new IllegalArgumentException(s"page buffer capacity ${byteBuffer.capacity()} < ${PageLayout.PageSize}")

final class PageHeader(private val buffer: ByteBuffer):
		PageHeader.assertPageCapacity(buffer)

		// Helpers to write primitives at header byte offsets
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
		// We'll use VarHandleHelpers.LONG_VH operating on long-array view; convert byte-offset -> long index:
		private inline def longIndexForMeta(): Int = PageLayout.HEADER_META_OFFSET / java.lang.Long.BYTES

		def getMetaAtomicVolatile: Long =
				// use VarHandle getVolatile (index relative to long elements)
				getVolatileLong(buffer, longIndexForMeta())

		def compareAndSetMetaAtomic(expected: Long, update: Long): Boolean =
				compareAndSetLong(buffer, longIndexForMeta(), expected, update)

		def getPinnedCountFromMeta(meta: Long): Int =
				((meta & MetaField.PINNED_MASK) >>> MetaField.PINNED_SHIFT).toInt

		def tryPin(): Boolean =
				// spin CAS increment on pinned field
				while true do
						val current = getMetaAtomicVolatile
						val pinned = getPinnedCountFromMeta(current)
						if pinned >= (1 << MetaField.PINNED_SHIFT - 1) then
								return false
						val next = current + (1L << MetaField.PINNED_SHIFT)
						if compareAndSetMetaAtomic(current, next) then
								return true
				false

		def unpin(): Unit =
				@annotation.tailrec
				def loop(): Unit =
						val current = getMetaAtomicVolatile
						val pinned = getPinnedCountFromMeta(current)
						if pinned >= (1L << MetaField.PINNED_SHIFT) then
								throw IllegalStateException("unpin underflow")
						val next = current - (1L << MetaField.PINNED_SHIFT)
						if !compareAndSetMetaAtomic(current, next) then
								loop()

				loop()

