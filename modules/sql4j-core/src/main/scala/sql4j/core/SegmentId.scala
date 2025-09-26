package sql4j.core

opaque type SegmentId = Long

object SegmentId:
		def apply(id: Long): SegmentId = id

		extension (s: SegmentId) def value: Long = s
