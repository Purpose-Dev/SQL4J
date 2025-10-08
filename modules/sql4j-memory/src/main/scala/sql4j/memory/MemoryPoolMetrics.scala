package sql4j.memory

/**
 *
 * @param totalPages
 * @param freePages
 * @param allocatedPages
 * @param pageSizeBytes
 */
// @formatter:off
final case class MemoryPoolMetrics(
		totalPages: Int,
		freePages: Int,
		allocatedPages: Int,
		pageSizeBytes: Int,
		avgFragmentation: Double = 0.0,
		maxFragmentation: Double = 0.0
):
		// @formatter:on

		/** Total of memory managed by the pool (bytes). */
		def totalBytes: Long = totalPages.toLong * pageSizeBytes

		/** Currently allocated memory (bytes). */
		def usedBytes: Long = allocatedPages.toLong * pageSizeBytes

		/** Free memory (bytes). */
		def freeBytes: Long = freePages.toLong * pageSizeBytes

		/** Pool usage ratio ∈ [0.0, 1.0]. */
		def usageRatio: Double =
				if totalPages == 0 then
						0.0
				else
						allocatedPages.toDouble / totalPages.toDouble

		override def toString: String =
				f"MemoryPoolMetrics(total=$totalPages, used=$allocatedPages, free=$freePages," +
					f"usage=${usageRatio * 100}%.3f%%, avgFrag=${avgFragmentation * 100}%.2f%%)"
