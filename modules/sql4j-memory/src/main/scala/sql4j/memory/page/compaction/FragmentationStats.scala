package sql4j.memory.page.compaction

import sql4j.memory.off_heap.PageLayout
import sql4j.memory.page.{PageEntry, PageHeader, SlotDirectory}

import java.nio.ByteBuffer
import scala.collection.mutable

/**
 * Represents the fragmentation status of a memory page.
 *
 * @param totalSpace total payload region (PageSize - HEADER_END)
 * @param usedSpace  sum of live tuple lengths
 * @param freeSpace  contiguous free bytes reported by header (PageSize - freePtr)
 * @param holes      number of gaps between live tuples (exclude header->first if it's contiguous usage)
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
				val denom = usedSpace + freeSpace
				if denom == 0 then
						0.0
				else
						freeSpace.toDouble / (denom.toDouble)

		def utilizationRatio: Double =
				if totalSpace == 0 then
						0.0
				else
						usedSpace.toDouble / totalSpace.toDouble

object FragmentationStats:
		def empty: FragmentationStats = FragmentationStats(0, 0, 0, 0)

		def analyze(page: PageEntry): FragmentationStats =
				val buffer: ByteBuffer = page.buffer
				val pageSize = PageLayout.PageSize
				val headerEnd = PageLayout.HEADER_END

				val header = PageHeader(buffer)
				val freePtr = header.getFreeSpacePointer

				val totalPayload = pageSize - headerEnd

				val tuples = mutable.ArrayBuffer.empty[(Int, Int)]
				SlotDirectory.foreachLiveSlot(buffer) { (slotId, offset, length) =>
						if offset >= headerEnd && offset + length <= pageSize && length > 0 then
								tuples += ((offset, length))
						true
				}

				val used = tuples.map(_._2).sum
				val headerReportedFree =
						if freePtr >= headerEnd && freePtr <= pageSize then
								pageSize - freePtr
						else
								0

				val sorted = tuples.toList.sortBy(_._1)
				var holes = 0

				if sorted.nonEmpty then
						// Count gaps between consecutive tuples
						var i = 0
						while i < sorted.length - 1 do
								val (currentOffset, currentLen) = sorted(i)
								val currentEnd = currentOffset + currentLen
								val (nextOffset, _) = sorted(i + 1)

								// If there's a gap between this tuple and the next, count it as a hole
								if nextOffset > currentEnd then
										holes += 1
								i += 1

				FragmentationStats(
						totalSpace = totalPayload,
						usedSpace = used,
						freeSpace = headerReportedFree,
						holes = holes
				)
