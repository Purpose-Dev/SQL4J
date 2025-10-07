package sql4j.memory.page.metrics

/**
 * Metrics representing a single page and the PageManager aggregate view.</br>
 * Fragmentation is expressed as a ratio ∈ [0.0, 1.0].
 */
// @formatter:off
final case class PageMetrics(
		pageId: Long,
		usedBytes: Int,
		freeBytes: Int,
		nEntries: Int,
		fragmentationRatio: Double
)
// @formatter:on
