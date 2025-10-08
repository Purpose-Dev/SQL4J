package sql4j.memory.page

import java.util.concurrent.atomic.AtomicLong

/**
 * Encapsulates atomic metadata for an off-heap page, including reference counts,
 * pin counters, state flags, and versioning information.
 *
 * == Overview ==
 * `PageMeta` stores a 64-bit bitfield that encodes mutable state for a page.
 * All updates are performed atomically using CAS operations on a single
 * `AtomicLong`. This enables lock-free coordination between concurrent readers
 * and writers in the page cache and memory manager.
 *
 * The bit layout is as follows (the least significant bit (LSB) first):
 * {{{
 * +------------+--------+-----------+-----------+
 * | version(24)| flags(8)| ref(12)  | pinned(20)|
 * +------------+--------+-----------+-----------+
 *  63         40 39    32 31       20 19        0
 * }}}
 *
 * == Field Semantics ==
 *  - `pinnedCount`: Number of concurrent pins (page in use by readers or writers).
 *  - `refCount`: Reference count for the owning segment or cache handle.
 *  - `flags`: Compact bitfield describing transient page states.
 *  - `version`: Monotonic counter used for conflict detection or LSN tagging.
 *
 * == Atomic Operations ==
 * All mutation methods (`tryPin`, `tryUnpin`, `setFlag`, `clearFlag`, `bumpVersion`)
 * use compare-and-set (CAS) retries until success, ensuring atomicity under contention.
 *
 * @note `PageMeta` is a lightweight mutable object and **not thread-safe for compound sequences**.
 *       Each method provides its own atomicity guarantees.
 */
object PageMeta:

		/** Number of bits used for pin counter (20 bits). */
		private val PIN_BITS = 20

		/** Number of bits used for reference count (12 bits). */
		private val REF_BITS = 12

		/** Number of bits used for flag field (8 bits). */
		private val FLAGS_BITS = 8

		/** Number of bits used for version counter (24 bits). */
		private val VER_BITS = 24

		// Bit offsets (LSB ordering)
		private val PIN_SHIFT = 0
		private val REF_SHIFT = PIN_BITS
		private val FLAGS_SHIFT = REF_SHIFT + REF_BITS
		private val VER_SHIFT = FLAGS_SHIFT + FLAGS_BITS

		private val PIN_MASK: Long = ((1L << PIN_BITS) - 1L) << PIN_SHIFT
		private val REF_MASK: Long = ((1L << REF_BITS) - 1L) << REF_SHIFT
		private val FLAGS_MASK: Long = ((1L << FLAGS_BITS) - 1L) << FLAGS_SHIFT
		private val VER_MASK: Long = ((1L << VER_BITS) - 1L) << VER_SHIFT

		private val PIN_MAX = (1 << PIN_BITS) - 1
		private val VER_MAX = (1 << VER_BITS) - 1

		// Runtime flags

		/** Page contains unflushed modifications. */
		val R_FLAG_DIRTY: Int = 1 << 0

		/** Page is mapped in a memory region (off-heap). */
		val R_FLAG_MAPPED: Int = 1 << 1

		/** Page is currently being compacted. */
		val R_FLAG_COMPACTING: Int = 1 << 2

		/** Page is sealed and cannot accept further writes. */
		val R_FLAG_SEALED: Int = 1 << 3

		// Some flags (can be extended in future)
		val FLAG_DIRTY: Long = R_FLAG_DIRTY.toLong << FLAGS_SHIFT
		val FLAG_MAPPED: Long = R_FLAG_MAPPED.toLong << FLAGS_SHIFT
		val FLAG_COMPACTING: Long = R_FLAG_COMPACTING.toLong << FLAGS_SHIFT
		val FLAG_SEALED: Long = R_FLAG_SEALED.toLong << FLAGS_SHIFT

		// Constructors

		/** Creates a new `PageMeta` instance with all counters and flags cleared. */
		def apply(): PageMeta = new PageMeta(new AtomicLong(0L))

		/** Creates a `PageMeta` from a raw 64-bit encoded value. */
		def fromRaw(v: Long): PageMeta = new PageMeta(new AtomicLong(v))

/**
 * Mutable atomic metadata associated with a single off-heap page.
 *
 * @param meta underlying atomic 64-bit word containing encoded counters and flags
 * @see [[PageMeta]] for a bit layout documentation.
 */
final class PageMeta(private val meta: AtomicLong):

		import PageMeta._

		/** Returns the raw 64-bit metadata value. */
		def getRaw: Long = meta.get()

		/** Returns the current pinned count (number of active holders). */
		def pinnedCount(): Int = ((meta.get() & PIN_MASK) >>> PIN_SHIFT).toInt

		/** Returns the current reference count. */
		def refCount(): Int = ((meta.get() & REF_MASK) >>> REF_SHIFT).toInt

		/** Returns the current flags bitfield as an integer. */
		def flags(): Int = ((meta.get() & FLAGS_MASK) >>> FLAGS_SHIFT).toInt

		/** Returns the current version counter. */
		def version(): Int = ((meta.get() & VER_MASK) >>> VER_SHIFT).toInt

		// Atomic mutation operations

		/**
		 * Attempts to increment the pin counter atomically.
		 *
		 * Fails if the page is sealed (`R_FLAG_SEALED`) or if the pin counter
		 * has reached its maximum representable value (`PIN_MAX`).
		 *
		 * @return true if pin successfully acquired, false otherwise.
		 */
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

		/**
		 * Attempts to decrement the pin counter atomically.
		 *
		 * Fails if the current pinned count is already zero.
		 *
		 * @return true if unpin succeeded, false otherwise.
		 */
		def tryUnpin(): Boolean =
				@annotation.tailrec
				def loop(): Boolean =
						val cur = meta.get()
						val pinned = ((cur & PIN_MASK) >>> PIN_SHIFT).toInt
						if pinned == 0 then
								false
						else
								val next = cur - (1L << PIN_SHIFT)
								if meta.compareAndSet(cur, next) then
										true
								else
										loop()

				loop()

		/**
		 * Sets the specified flag bits atomically.
		 *
		 * @param flagMask pre-shifted mask (use constants from [[PageMeta]])
		 */
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

		/**
		 * Clears the specified flag bits atomically.
		 *
		 * @param flagMask pre-shifted mask (use constants from [[PageMeta]])
		 */
		def clearFlag(flagMask: Long): Unit =
				@annotation.tailrec
				def loop(): Unit =
						val cur = meta.get()
						val next = cur & ~flagMask
						if meta.compareAndSet(cur, next) then
								()
						else
								loop()

				loop()

		/**
		 * Atomically increments the 24-bit version counter, wrapping on overflow.
		 *
		 * @return the new version value after increment.
		 */
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
