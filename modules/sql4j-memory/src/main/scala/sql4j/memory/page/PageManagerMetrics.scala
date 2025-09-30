package sql4j.memory.page

// @formatter:off
case class PageManagerMetrics(
			 currentPages: Int,
			 cacheHits: Long,
			 cacheMisses: Long,
			 evictions: Long,
			 pinnedPages: Int,
			 freePages: Int,
			 totalPages: Int
)
// @formatter:on