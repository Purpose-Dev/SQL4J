package sql4j.memory.page

import java.util.concurrent.atomic.AtomicLong

object PageMeta:
		// bit layout sizes
		private val PIN_BITS = 20
		private val REF_BITS = 12
		private val FLAGS_BITS = 8
		private val VER_BITS = 24

		private val PIN_SHIFT = 0
		private val REF_SHIFT = PIN_BITS + PIN_BITS
		private val FLAGS_SHIFT = REF_SHIFT + REF_BITS
		private val VER_SHIFT = FLAGS_SHIFT + FLAGS_BITS

		private val PIN_MASK: Long = ((1L << PIN_BITS) - 1L) << PIN_SHIFT
		private val REF_MASK: Long = ((1L << REF_BITS) - 1L) << REF_SHIFT
		private val FLAGS_MASK: Long = ((1L << FLAGS_BITS) - 1L) << FLAGS_SHIFT
		private val VER_MASK: Long = ((1L << VER_BITS) - 1L) << VER_SHIFT

		private val PIN_MAX = (1 << PIN_BITS) - 1
		private val VER_MAX = (1 << VER_BITS) - 1

		val R_FLAG_DIRTY: Int = 1 << 0
		val R_FLAG_MAPPED: Int = 1 << 1
		val R_FLAG_COMPACTING: Int = 1 << 2
		val R_FLAG_SEALED: Int = 1 << 3

		// Some flags (can be extended in future)
		val FLAG_DIRTY: Long = R_FLAG_DIRTY.toLong << FLAGS_SHIFT
		val FLAG_MAPPED: Long = R_FLAG_MAPPED.toLong << FLAGS_SHIFT
		val FLAG_COMPACTING: Long = R_FLAG_COMPACTING.toLong << FLAGS_SHIFT
		val FLAG_SEALED: Long = R_FLAG_SEALED.toLong << FLAGS_SHIFT

		def apply(): PageMeta = new PageMeta(new AtomicLong(0L))

		def fromRaw(v: Long): PageMeta = new PageMeta(new AtomicLong(v))

final class PageMeta(private val meta: AtomicLong):

		import PageMeta._

		def getRaw: Long = meta.get()

		def pinnedCount(): Int = ((meta.get() & PIN_MASK) >>> PIN_SHIFT).toInt

		def refCount(): Int = ((meta.get() & REF_MASK) >>> REF_SHIFT).toInt

		def flags(): Int = ((meta.get() & FLAGS_MASK) >>> FLAGS_SHIFT).toInt

		def version(): Int = ((meta.get() & VER_MASK) >>> VER_SHIFT).toInt

		def tryPin(): Boolean =
				@annotation.tailrec
				def loop(): Boolean =
						val cur = meta.get()
						val pinned = ((cur & PIN_MASK) >>> PIN_SHIFT).toInt
						val flagsPart = ((cur & FLAGS_MASK) >>> FLAGS_SHIFT).toInt
						if (flagsPart & R_FLAG_SEALED) != 0 || pinned == PIN_MAX then
								false
						else
								val next = cur + (1L << PIN_SHIFT)
								if meta.compareAndSet(cur, next) then
										true
								else
										loop()

				loop()

		def tryUnpin(): Boolean =
				@annotation.tailrec
				def loop(): Boolean =
						val cur = meta.get()
						val pinned = (cur & PIN_MASK >>> PIN_SHIFT).toInt
						if pinned == 0 then
								false
						else
								val next = cur - (1L << PIN_SHIFT)
								if meta.compareAndSet(cur, next) then
										true
								else
										loop()

				loop()

		def setFlag(flagMask: Long): Unit =
				@annotation.tailrec
				def loop(): Unit =
						val cur = meta.get()
						val next = cur | flagMask
						if meta.compareAndSet(cur, next) then
								()
						else
								loop()

				loop()

		def clearFlag(flagMask: Long): Unit =
				def loop(): Unit =
						val cur = meta.get()
						val next = cur & ~flagMask
						if meta.compareAndSet(cur, next) then
								()
						else
								loop()

				loop()

		def bumpVersion(): Int =
				@annotation.tailrec
				def loop(): Int =
						val cur = meta.get()
						val ver = ((cur & VER_MASK) >>> VER_SHIFT).toInt
						val nextVer = (ver + 1) & VER_MAX
						val next = (cur & ~VER_MASK) | ((nextVer.toLong << VER_SHIFT) & VER_MASK)
						if meta.compareAndSet(cur, next) then
								nextVer
						else
								loop()

				loop()
