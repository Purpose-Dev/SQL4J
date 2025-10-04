package sql4j.memory.page.compaction

import sql4j.memory.off_heap.PageLayout
import sql4j.memory.page.{PageEntry, PageHeader, SlotDirectory}

import java.nio.ByteBuffer
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
				val buffer: ByteBuffer = page.buffer
				val header = PageHeader(buffer)
				val pageSize = PageLayout.PageSize
				val headerEnd = PageLayout.HEADER_END

				// gather live tuples as (slotId, offset, length)
				val tuples = mutable.ArrayBuffer.empty[(Int, Int, Int)]
				SlotDirectory.foreachLiveSlot(buffer) { (slotId, offset, length) =>
						if offset >= 0 && length > 0 && offset + length <= pageSize then
								tuples += ((slotId, offset, length))
						true
				}

				// nothing to compact
				if tuples.isEmpty then
						return FragmentationStats.analyze(page)

				// sort by original offset ascending to keep relative order
				val sorted = tuples.sortBy(_._2)

				// compute total used bytes
				val totalUsed = sorted.foldLeft(0)((acc, t) => acc + t._3)

				// compute where packed data must start so that data occupies top of page
				val writeStart = pageSize - totalUsed
				var writePos = writeStart

				// temporary buffer for copy (reuse and grow as needed)
				var tmp = Array[Byte]()

				// copy each tuple into its new location and update slot
				sorted.foreach { case (slotId, oldOff, length) =>
						if tmp.length < length then tmp = new Array[Byte](length)

						// read source bytes
						buffer.position(oldOff)
						buffer.get(tmp, 0, length)

						// write destination bytes
						buffer.position(writePos)
						buffer.put(tmp, 0, length)

						// update slot to point to new offset
						SlotDirectory.writeSlot(buffer, slotId, writePos, length)
						writePos += length
				}

				// set free pointer so future inserts follow the descending convention
				header.setFreeSpacePointer(writeStart)

				// sanity check: free pointer shouldn't collide with slot table
				if writeStart - header.slotTableEnd <= 0 then
						// This should not happen in normal usage; throw so tests detect it early.
						throw new IllegalStateException(s"Compaction produced freePtr=$writeStart which collides with slot table end=${header.slotTableEnd}")

				// return new fragmentation stats
				FragmentationStats.analyze(page)

		def compactIfNeeded(page: PageEntry, policy: CompactionPolicy): FragmentationStats =
				val stats = FragmentationStats.analyze(page)
				if policy.shouldCompact(stats) then
						compact(page)
				else
						stats
