package sql4j.memory.page

import java.util.concurrent.atomic.AtomicLong

object PageMeta:
		// bit layout sizes
		private val PIN_BITS = 20
		private val REF_BITS = 12
		private val FLAGS_BITS = 8
		private val VER_BITS = 24

		private val PIN_SHIFT = 0
		private val REF_SHIFT = PIN_BITS
		private val FLAGS_SHIFT = PIN_BITS + REF_SHIFT
		private val VER_SHIFT = PIN_BITS + REF_BITS + FLAGS_BITS

		private val PIN_MASK: Long = ((1L << PIN_BITS) - 1L) << PIN_SHIFT
		private val REF_MASK: Long = ((1L << REF_BITS) - 1L) << REF_SHIFT
		private val FLAGS_MASK: Long = ((1L << FLAGS_SHIFT) - 1L) << FLAGS_SHIFT
		private val VER_MASK: Long = ((1L << VER_BITS) - 1L) << VER_SHIFT

		private val PIN_MAX = (1 << PIN_BITS) - 1
		private val VER_MAX = (1 << VER_BITS) - 1

		// Some flags (can be extended in future)
		val FLAG_DIRTY: Long = 1L << FLAGS_SHIFT // flag bit 0
		val FLAG_MAPPED: Long = 1L << (FLAGS_SHIFT + 1)
		val FLAG_COMPACTING: Long = 1L << (FLAGS_SHIFT + 2)
		val FLAG_SEALED: Long = 1L << (FLAGS_SHIFT + 3)

		def apply(): PageMeta = new PageMeta(new AtomicLong(0L))

		def fromRaw(v: Long): PageMeta = new PageMeta(new AtomicLong(v))

final class PageMeta(private val meta: AtomicLong):

		import PageMeta._

		def getRaw: Long = meta.get()

		def pinnedCount(): Int = (meta.get() & PIN_MASK).toInt

		def refCount(): Int = ((meta.get() & REF_MASK) >>> REF_SHIFT).toInt

		def flags(): Int = ((meta.get() & FLAGS_MASK) >>> FLAGS_SHIFT).toInt

		def version(): Int = ((meta.get() & VER_MASK) >>> VER_SHIFT).toInt

		def tryPin(): Boolean =
				while true do
						val cur = meta.get()
						val pinned = (cur & PIN_MASK).toInt
						val flags = ((cur & FLAGS_MASK) >>> FLAGS_SHIFT).toInt
						if (flags & FLAG_SEALED) != 0 || pinned == PIN_MAX then
								return false
						val next = cur + 1L
						if meta.compareAndSet(cur, next) then
								return true
				false

		def tryUnpin(): Boolean =
				while true do
						val cur = meta.get()
						val pinned = (cur & PIN_MASK).toInt
						if pinned == 0 then
								return false
						val next = cur - 1L
						if meta.compareAndSet(cur, next) then
								return true
				false

		def setFlag(flagMask: Long): Unit =
				while true do
						val cur = meta.get()
						val next = cur | flagMask
						if meta.compareAndSet(cur, next) then
								return

		def clearFlag(flagMask: Long): Unit =
				while true do
						val cur = meta.get()
						val next = cur & ~flagMask
						if meta.compareAndSet(cur, next) then
								return

		def bumpVersion(): Int =
				while true do
						val cur = meta.get()
						val ver = ((cur & VER_MASK) >>> VER_SHIFT).toInt
						val nextVer = (ver + 1) & VER_MAX
						val next = (cur & ~VER_MASK) | ((nextVer.toLong << VER_SHIFT) & VER_MASK)
						if meta.compareAndSet(cur, next) then
								return nextVer
				-1
