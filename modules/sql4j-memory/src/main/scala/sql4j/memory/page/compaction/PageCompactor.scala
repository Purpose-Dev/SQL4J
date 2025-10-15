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
						case None =>
								header.setFreeSpacePointer(PageLayout.PageSize)
								FragmentationStats.analyze(page)
						case Some((sorted, targets, writeStart)) =>
								val allData = ArrayBuffer[Byte]()

								sorted.foreach { case (slotId, oldOff, length) =>
										//noinspection DuplicatedCode
										val data = new Array[Byte](length)
										buffer.position(oldOff)
										buffer.get(data)
										allData.appendAll(data)
								}

								// Now write it all back contiguously
								val dataArray = allData.toArray
								buffer.position(writeStart)
								buffer.put(dataArray)

								// Update all slot pointers
								var offset = writeStart
								sorted.indices.foreach { idx =>
										val (slotId, _, length) = sorted(idx)
										SlotDirectory.writeSlot(buffer, slotId, offset, length)
										offset += length
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
		 *
		 * Strategy: Since all tuples are moving DOWN in the page (toward the end),
		 * we can safely process them in order from the lowest offset to the highest.
		 * This ensures we never overwrite data we haven't moved yet.
		 *
		 * The algorithm is stateless - it recomputes the plan each time and resumes
		 * from where it left off based on which tuples are already at their targets.
		 *
		 * @param page        the page to compact
		 * @param budgetBytes maximum bytes of tuple data to move in this call
		 * @return (bytesProcessed, isDone)
		 */
		def compactStep(page: PageEntry, budgetBytes: Int): (Int, Boolean) =
				require(budgetBytes > 0, s"budgetBytes must be > 0, was $budgetBytes")
				val buffer: ByteBuffer = page.buffer
				val header = PageHeader(buffer)

				computeCompactionPlan(buffer) match
						case None =>
								header.setFreeSpacePointer(PageLayout.PageSize)
								(0, true)
						case Some((sorted, targets, writeStart)) =>
								// Find tuples that need moving and process them in order
								val needsMoving = sorted.indices.filter(idx => sorted(idx)._2 != targets(idx))

								if needsMoving.isEmpty then
										setHeaderAndCheckSanity(header, writeStart)
										(0, true)
								else
										// Allocate buffer for the largest tuple we might move
										val maxLength = needsMoving.map(idx => sorted(idx)._3).max
										val tmp = new Array[Byte](maxLength)

										// Process tuples within a budget
										val (processed, moved) = needsMoving.foldLeft((0, 0)) {
												case (acc@(totalProcessed, totalMoved), idx) =>
														val (slotId, currentOff, length) = sorted(idx)
														val targetOff = targets(idx)

														if totalProcessed + length <= budgetBytes then
																// Move this tuple
																buffer.position(currentOff)
																buffer.get(tmp, 0, length)
																buffer.position(targetOff)
																buffer.put(tmp, 0, length)
																SlotDirectory.writeSlot(buffer, slotId, targetOff, length)

																(totalProcessed + length, totalMoved + 1)
														else
																acc // Budget exhausted, stop processing
										}

										val isDone = moved >= needsMoving.length

										if isDone then
												setHeaderAndCheckSanity(header, writeStart)

										(processed, isDone)