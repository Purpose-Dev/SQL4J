package sql4j.memory.page.compaction

/**
 * Represents the fragmentation status of a memory page.
 *
 * @param totalSpace total space on the page (bytes)
 * @param usedSpace  space actually used by tuples (bytes)
 * @param freeSpace  total free space (bytes)
 * @param holes      number of “holes” due to deletions
 */
// @formatter:off
case class FragmentationStats(
		totalSpace: Int,
		usedSpace: Int,
		freeSpace: Int,
		holes: Int
):
		// @formatter:on
		def fragmentationRatio: Double =
				if totalSpace == 0 then
						0.0
				else
						holes.toDouble / (holes + usedSpace.toDouble)

		def utilizationRatio: Double =
				if totalSpace == 0 then
						0.0
				else
						usedSpace.toDouble / totalSpace

object FragmentationStats {
		def empty: FragmentationStats = FragmentationStats(0, 0, 0, 0)

		// todo: Add a method for quickly calculating fragmentation stats from SlotDirectory + PageLayout
}