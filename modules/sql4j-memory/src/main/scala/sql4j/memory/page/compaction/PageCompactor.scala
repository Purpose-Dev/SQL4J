package sql4j.memory.page.compaction

import sql4j.memory.off_heap.PageLayout
import sql4j.memory.page.{PageEntry, PageHeader, SlotDirectory}

import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer

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
		// Alias for clarity: (slotId, offset, length)
		private type LiveTuple = (Int, Int, Int)

		private def computeCompactionPlan(buffer: ByteBuffer): Option[(ArrayBuffer[LiveTuple], Array[Int], Int)] =
				val pageSize = PageLayout.PageSize

				val tuples = ArrayBuffer.empty[LiveTuple]
				SlotDirectory.foreachLiveSlot(buffer) { (slotId, offset, length) =>
						if offset >= 0 && length > 0 && offset + length <= pageSize then
								tuples += ((slotId, offset, length))
						true
				}

				if tuples.isEmpty then
						None
				else
						val sorted = tuples.sortBy(_._2)
						val lengths = sorted.map(_._3).toArray

						val totalUsed = lengths.sum
						val writeStart = pageSize - totalUsed

						val targets = new Array[Int](sorted.size)
						var acc = writeStart
						var i = 0
						while i < lengths.length do
								targets(i) = acc
								acc += lengths(i)
								i += 1

						Some((sorted, targets, writeStart))

		private def setHeaderAndCheckSanity(header: PageHeader, writeStart: Int): Unit =
				header.setFreeSpacePointer(writeStart)
				if writeStart - header.slotTableEnd <= 0 then
						throw new IllegalStateException(
								s"Compaction produced freePtr=$writeStart which collides with slot table end=${header.slotTableEnd}"
						)

		def compact(page: PageEntry): FragmentationStats =
				val buffer: ByteBuffer = page.buffer
				val header = PageHeader(buffer)

				computeCompactionPlan(buffer) match
						case None => FragmentationStats.analyze(page)
						case Some((sorted, _, writeStart)) =>
								var tmp = Array[Byte]()
								sorted.zipWithIndex.foreach { case ((slotId, oldOff, length), idx) =>
										//noinspection DuplicatedCode
										if tmp.length < length then
												tmp = new Array[Byte](length)

										buffer.position(oldOff)
										buffer.get(tmp, 0, length)

										val newOff = writeStart + sorted.take(idx).map(_._3).sum
										buffer.position(newOff)
										buffer.put(tmp, 0, length)

										SlotDirectory.writeSlot(buffer, slotId, newOff, length)
								}

								setHeaderAndCheckSanity(header, writeStart)
								FragmentationStats.analyze(page)

		def compactIfNeeded(page: PageEntry, policy: CompactionPolicy): FragmentationStats =
				val stats = FragmentationStats.analyze(page)
				if policy.shouldCompact(stats) then
						compact(page)
				else
						stats

		/**
		 * Incremental compaction step with a work budget in bytes.
		 * This recomputes the target packed layout each time and moves tuples whose current
		 * offset does not match the target, up to the provided budget. It is stateless and safe
		 * to call repeatedly; progress is derived from current slot offsets.
		 *
		 * @return (processedBytes, done)
		 */
		def compactStep(page: PageEntry, budgetBytes: Int): (Int, Boolean) =
				require(budgetBytes > 0, s"budgetBytes must be > 0, was $budgetBytes")
				val buffer: ByteBuffer = page.buffer
				val header = PageHeader(buffer)

				computeCompactionPlan(buffer) match
						case None => (0, true)
						case Some((sorted, targets, writeStart)) =>
								var processed = 0
								var idx = 0
								var tmp = Array[Byte]()

								var startIdx = 0
								while startIdx < sorted.length && sorted(startIdx)._2 == targets(startIdx) do
										startIdx += 1

								idx = startIdx

								while idx < sorted.length && processed + sorted(idx)._3 <= budgetBytes do
										val (slotId, currentOffset, length) = sorted(idx)
										val target = targets(idx)

										if currentOffset != target then
												//noinspection DuplicatedCode
												if tmp.length < length then
														tmp = new Array[Byte](length)
												buffer.position(currentOffset)
												buffer.get(tmp, 0, length)
												buffer.position(target)
												buffer.put(tmp, 0, length)
												SlotDirectory.writeSlot(buffer, slotId, target, length)
										end if
										processed += length
										idx += 1
								end while

								val isDone = startIdx == sorted.length

								if isDone then
										setHeaderAndCheckSanity(header, writeStart)

								(processed, isDone)