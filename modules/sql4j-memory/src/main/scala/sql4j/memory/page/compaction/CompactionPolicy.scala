package sql4j.memory.page.compaction

/**
 * Determines when to run compaction on a page.
 */
trait CompactionPolicy:
		def shouldCompact(stats: FragmentationStats): Boolean

/**
 * Never compacts
 */
object NoCompaction extends CompactionPolicy:
		override def shouldCompact(stats: FragmentationStats): Boolean = false

/**
 * Still compact (useful for testing or stress)
 */
object AggressiveCompaction extends CompactionPolicy:
		override def shouldCompact(stats: FragmentationStats): Boolean = true

/**
 * Compact if the fragmentation ratio exceeds the threshold
 *
 * @param threshold threshold limit
 */
final case class ThresholdCompaction(threshold: Double) extends CompactionPolicy:
		override def shouldCompact(stats: FragmentationStats): Boolean =
				stats.fragmentationRatio >= threshold