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
						val start = Math.max(offset, headerEnd)
						val end = Math.min(offset + length, pageSize)
						if end > start then
								tuples += ((start, end - start))
						true
				}

				val used = tuples.foldLeft(0)((acc, t) => acc + t._2)
				val headerReportedFree =
						if freePtr <= pageSize then
								pageSize - freePtr
						else
								0

				val sorted = tuples.toList.sortBy(_._1)
				var holes = 0
				var cursor = headerEnd
				if sorted.nonEmpty then
						for ((s, len) <- sorted) do
								if s > cursor then
										holes += 1
										cursor = Math.max(cursor, s + len)
				else
						holes = 0

				FragmentationStats(
						totalSpace = totalPayload,
						usedSpace = used,
						freeSpace = headerReportedFree,
						holes = holes
				)
