package sql4j.memory

// @formatter:off
final case class MemoryPoolMetrics(
		totalPages: Int,
		freePages: Int,
		allocatedPages: Int,
		pageSizeBytes: Int
)
// @formatter:on
