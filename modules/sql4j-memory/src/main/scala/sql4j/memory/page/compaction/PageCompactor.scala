package sql4j.memory.page.compaction

import sql4j.memory.off_heap.PageLayout
import sql4j.memory.page.{PageEntry, PageHeader, SlotDirectory}

import scala.collection.mutable

/**
 * PageCompactor: compacts tuples inside a single page to eliminate holes.
 *
 * Algorithm (safe, simple):
 *  - scan SlotDirectory for live tuples
 *  - sort tuples by offset ascending
 *  - copy each tuple into a contiguous region starting at HEADER_END
 *    (copy via temporary byte[] to avoid overlapping copy issues)
 *  - update slot entries with new offsets
 *  - set header free pointer appropriately (PageSize - free space used)
 *
 * This is intentionally conservative (safe), not the most CPU-optimal variant.
 */
object PageCompactor:

		def compact(page: PageEntry): FragmentationStats =
				val buffer = page.buffer
				val header = PageHeader(buffer)
				val pageSize = PageLayout.PageSize
				val headerEnd = PageLayout.HEADER_END

				val tuples = mutable.ArrayBuffer.empty[(Int, Int, Int)]
				SlotDirectory.foreachLiveSlot(buffer) { (slotId, offset, length) =>
						if offset >= 0 && length > 0 && offset + length <= pageSize then
								tuples += ((slotId, offset, length))
						true
				}

				// sort by ascending offset (stable)
				val sorted = tuples.sortBy(_._2)

				var writePos = headerEnd
				var tmp = Array[Byte]()

				sorted.foreach { case (slotId, offset, length) =>
						if tmp.length < length then
								tmp = new Array[Byte](length)

								// read source
								buffer.position(offset)
								buffer.get(tmp, 0, length)

								// write destination
								buffer.position(writePos)
								buffer.put(tmp, 0, length)

								SlotDirectory.writeSlot(buffer, slotId, writePos, length)
								writePos += length
				}

				header.setFreeSpacePointer(writePos)
				FragmentationStats.analyze(page)


		def compactIfNeeded(page: PageEntry, policy: CompactionPolicy): FragmentationStats =
				val stats = FragmentationStats.analyze(page)
				if policy.shouldCompact(stats) then
						compact(page)
				else
						stats
