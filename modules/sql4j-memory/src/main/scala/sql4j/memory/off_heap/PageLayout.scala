package sql4j.memory.off_heap

/**
 * Defines the binary layout and metadata structure of an off-heap memory page.
 *
 * == Overview ==
 * A page is a fixed-size memory block used by the SQL4J storage engine for off-heap data management.
 * Each page begins with a 64-byte header, followed by data and index areas. All numeric fields are stored using
 * the platform's native byte order.
 *
 * == Default Parameters ==
 *  - `PageSize`: 64 * 1024 = 65,536 bytes (64KiB)
 *  - `PageHeader`: 64 bytes
 *
 * == Header Layout (byte offsets) ==
 * {{{
 * 0x00  HEADER_PAGE_ID_OFFSET        (8 bytes)   Page identifier
 * 0x08  HEADER_SEGMENT_ID_OFFSET     (8 bytes)   Segment identifier
 * 0x10  HEADER_META_OFFSET           (8 bytes)   Meta bitfield (AtomicLong)
 * 0x18  HEADER_LSN_OFFSET            (8 bytes)   Log sequence number
 * 0x20  HEADER_INDEX_OFFSET          (4 bytes)   Offset to index area
 * 0x24  HEADER_INDEX_NENTRIES        (4 bytes)   Number of index entries
 * 0x28  HEADER_CHECKSUM_OFFSET       (4 bytes)   Page checksum
 * 0x2C  HEADER_FREE_POINTER_OFFSET   (4 bytes)   Free pointer
 * 0x30  HEADER_RESERVED              (16 bytes)  Reserved / padding
 * ---
 * Total: 64 bytes
 * }}}
 *
 * == Meta Bitfield Layout (64 bits) ==
 * {{{
 * Bits  0..19 : pinnedCount
 * Bits  20.31 : refCount
 * Bits  32..39 : flags
 * Bits  40..63 : version/epoch
 * }}}
 *
 * @note The header size is fixed at 64 bytes to ensure alignment with cache lines.
 * @note VarHandle accessors always operate using native byte order.
 */
object PageLayout:
		/** Default page size in bytes (64 KiB). */
		final val PageSize: Int = 64 * 1024

		/** Header size in bytes (fixed at 64). */
		final val HeaderBytes: Int = 64

		/** Byte offset of the 64-bit page identifier field. */
		final val HEADER_PAGE_ID_OFFSET = 0x00

		/** Byte offset of the 64-bit segment identifier field. */
		final val HEADER_SEGMENT_ID_OFFSET = 0x08

		/** Byte offset of the 64-bit meta-atomic bitfield. */
		final val HEADER_META_OFFSET = 0x10

		/** Byte offset of the 64-bit log sequence number. */
		final val HEADER_LSN_OFFSET = 0x18

		/** Byte offset of the 32-bit index area offset field. */
		final val HEADER_INDEX_OFFSET = 0x20

		/** Byte offset of the 32-bit index entry count field. */
		final val HEADER_INDEX_NENTRIES = 0x24

		/** Byte offset of the 32-bit checksum field. */
		final val HEADER_CHECKSUM_OFFSET = 0x28

		/** Byte offset of the 32-bit free pointer offset. */
		final val HEADER_FREE_POINTER_OFFSET = 0x2C

		/** End of header structure (64 bytes). */
		final val HEADER_END = HeaderBytes

		require(
				HEADER_END == HeaderBytes,
				s"Page header size mismatch: expected $HeaderBytes, got $HEADER_END"
		)

		/** Slot size in bytes (8 bytes per index entry). */
		final val SLOT_BYTES: Int = 8

		/**
		 * Defines bitfield masks and shifts for the 64-bit <code>metaAtomic</code> header field.
		 *
		 * <p>The bitfield encodes counters, flags, and versioning metadata.
		 * Fields are packed starting from the least significant bits (LSB)
		 * </p>
		 *
		 * {{{
		 * +-------------+----------+-----------+------------+
		 * | version(24) | flags(8) |  ref(12)  | pinned(20) |
		 * +-------------+----------+-----------+------------+
		 * | 63       40 | 39    32 | 31     20 | 19       0 |
		 * +-------------+----------+-----------+------------+
		 * }}}
		 *
		 * @note Used internally by [[sql4j.memory.page.PageOps]] for atomic page state updates.
		 */
		object MetaField:
				/** A bit offset of the pinned count field (bits 0–19). */
				final val PINNED_SHIFT = 0

				/** Number of bits allocated for pinned count. */
				final val PINNED_BITS = 20

				/** Bit mask for pinned count. */
				final val PINNED_MASK: Long = ((1L << PINNED_BITS) - 1L) << PINNED_SHIFT

				/** A bit offset of the reference count field (bits 20–31). */
				final val REF_SHIFT = PINNED_SHIFT + PINNED_BITS

				/** Number of bits allocated for reference count. */
				final val REF_BITS = 12

				/** Bit mask for reference count. */
				final val REF_MASK: Long = ((1L << REF_BITS) - 1L) << REF_SHIFT

				/** A bit offset of the flags field (bits 32-39). */
				final val FLAGS_SHIFT = REF_SHIFT + REF_BITS

				/** Number of bits allocated for flags. */
				final val FLAGS_BITS = 8

				/** Bit mask for flags. */
				final val FLAGS_MASK: Long = ((1L << FLAGS_BITS) - 1L) << FLAGS_SHIFT

				/** A bit offset of the version field (bits 40-63). */
				final val VER_SHIFT = FLAGS_SHIFT + FLAGS_BITS

				/** Number of bits allocated for version. */
				final val VER_BITS = 24

				/** Bit mask for version field. */
				final val VER_MASK: Long = ((1L << VER_BITS) - 1L) << VER_SHIFT

				/** Indicates that the page is dirty (modified since last flush). */
				final val FLAG_DIRTY = 0x1

				/** Indicates that the page is sealed (no further writes allowed). */
				final val FLAG_SEALED = 0x2

				/** Indicates that the page is undergoing compaction. */
				final val FLAG_COMPACTING = 0x4
