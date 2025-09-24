package sql4j.memory.off_heap

/**
 *
 * Default:
 * PageSize = 64 * 1024 (64KiB)
 *
 * PageHeader (64 bytes) - relative offsets based on page (little/big endian depends on
 * platform, but VarHandle uses native order).
 *
 * Offsets (in bytes):
 * 	- HEADER_PAGE_ID_OFFSET    = 0x00  (8 bytes)
 * 	- HEADER_SEGMENT_ID_OFFSET = 0x08  (8 bytes)
 * 	- HEADER_META_OFFSET       = 0x10  (8 bytes)  // metaAtomic (bitfield, AtomicLong)
 * 	- HEADER_LSN_OFFSET        = 0x18  (8 bytes)
 * 	- HEADER_INDEX_OFFSET      = 0x20  (4 bytes)  // slot/index area offset (uint32)
 * 	- HEADER_INDEX_NENTRIES    = 0x24  (4 bytes)
 * 	- HEADER_CHECKSUM_OFFSET   = 0x28  (4 bytes)
 * 	- HEADER_RESERVED          = 0x2C  (36 bytes) // padding -> total header 64 bytes
 *
 * @note keep header size = 64 for alignment on cache lines.
 */
object PageLayout:
		final val PageSize: Int = 64 * 1024

		final val HeaderBytes: Int = 64

		final val HEADER_PAGE_ID_OFFSET = 0x00
		final val HEADER_SEGMENT_ID_OFFSET = 0x08
		final val HEADER_META_OFFSET = 0x10
		final val HEADER_LSN_OFFSET = 0x18
		final val HEADER_INDEX_OFFSET = 0x20
		final val HEADER_INDEX_NENTRIES = 0x24
		final val HEADER_CHECKSUM_OFFSET = 0x28
		// reserved until 0x40 (64)
		final val HEADER_END = HeaderBytes

		// metaAtomic bitfield layout (64 bits)
		object MetaField:
				// pinnedCount: bits 0..19 (20 bits)
				final val PINNED_SHIFT = 0
				final val PINNED_BITS = 20
				final val PINNED_MASK: Long = ((1L << PINNED_BITS) - 1L) << PINNED_SHIFT

				// refCount: bits 20..31 (12 bits)
				final val REF_SHIFT = PINNED_SHIFT + PINNED_BITS
				final val REF_BITS = 12
				final val REF_MASK: Long = ((1L << REF_BITS) - 1L) << REF_SHIFT

				// flags: bits 32..39 (8 bits)
				final val FLAGS_SHIFT = REF_SHIFT + REF_BITS
				final val FLAGS_BITS = 8
				final val FLAGS_MASK: Long = ((1L << FLAGS_BITS) - 1L) << FLAGS_SHIFT

				// version/epoch: bits 40..63 (24 bits)
				final val VER_SHIFT = FLAGS_SHIFT + FLAGS_BITS
				final val VER_BITS = 24
				final val VER_MASK: Long = ((1L << VER_BITS) - 1L) << VER_SHIFT

				// helper flags
				final val FLAG_DIRTY = 0x1
				final val FLAG_SEALED = 0x2
				final val FLAG_COMPACTING = 0x4
