package sql4j.memory.page.metrics

// @formatter:off
case class PageManagerMetrics(
			 currentPages: Int,
			 cacheHits: Long,
			 cacheMisses: Long,
			 evictions: Long,
			 pinnedPages: Int,
			 freePages: Int,
			 totalPages: Int,
			 avgFragmentation: Double = 0.0D,
			 maxFragmentation: Double = 0.0D
)
// @formatter:on
