package sql4j.memory.page

import sql4j.core.DbError
import sql4j.memory.off_heap.PageLayout

import java.nio.ByteBuffer
import scala.collection.mutable

object PageOps:

		// @formatter:off
		final case class PageMetrics(
				pageSize: Int,
				headerEnd: Int,
				freePtr: Int,
				payloadUsedBytes: Int, // 	bytes between freePtr...PageSize
				liveBytes: Int, // sum(len of live slots)
				reclaimableBytes: Int, // payloadUsedBytes - liveBytes
				liveSlots: Int,
				largestContiguousFree: Int, // largest contiguous free region inside payload+holes (bytes)
				fragmentationRatio: Double // reclaimable / (reclaimable + largestContiguousFree)
		)

		// @formatter:on
		private def compactPage(buf: ByteBuffer, header: PageHeader): Unit =
				val n = header.getNEntries
				if n == 0 then
						header.setFreeSpacePointer(PageLayout.PageSize)
						return

				val liveSlots = (0 until n).flatMap { slotId =>
						SlotDirectory.readSlot(buf, slotId).map {
								case (offset, len) => (slotId, offset, len)
						}
				}

				val sortedSlots = liveSlots.sortBy(-_._2)

				var freePtr = PageLayout.PageSize
				var currentBlockStart = -1
				var currentBlockLen = 0
				var currentSlots = Vector.empty[(Int, Int, Int)]

				def flushBlock(): Unit =
						if currentBlockStart >= 0 then
								val tmp = new Array[Byte](currentBlockLen)
								buf.position(currentBlockStart)
								buf.get(tmp)
								buf.position(freePtr - currentBlockLen)
								buf.put(tmp)

								var offsetPtr = freePtr - currentBlockLen
								currentSlots.foreach { case (slotId, _, len) =>
										SlotDirectory.writeSlot(buf, slotId, offsetPtr, len)
										offsetPtr += len
								}
								freePtr -= currentBlockLen
								currentBlockStart = -1
								currentBlockLen = 0
								currentSlots = Vector.empty

				sortedSlots.foreach { case (slotId, offset, len) =>
						if currentBlockStart == -1 then
								currentBlockStart = offset
								currentBlockLen = len
								currentSlots = Vector((slotId, offset, len))
						else if offset + len == currentBlockStart then
								currentBlockStart = offset
								currentBlockLen += len
								currentSlots :+= ((slotId, offset, len))
						else
								flushBlock()
								currentBlockStart = offset
								currentBlockLen = len
								currentSlots = Vector((slotId, offset, len))
				}
				flushBlock()

				assert(freePtr >= PageLayout.HEADER_END, s"freePtr=$freePtr < HEADER_END after compaction!")
				header.setFreeSpacePointer(freePtr)

		def insertRecordWithCompaction(buf: ByteBuffer, header: PageHeader, data: Array[Byte]): Int =
				try
						insertRecord(buf, header, data)
				catch
						case _: DbError.PageFullError =>
								// Compaction
								compactPage(buf, header)
								// Retry insertion
								try
										insertRecord(buf, header, data)
								catch
										case _: DbError.PageFullError =>
												throw DbError.PageFullError(data.length)

		def insertRecord(buf: ByteBuffer, header: PageHeader, data: Array[Byte]): Int =
				val required = data.length
				if !header.canFit(required) then
						throw DbError.PageFullError(required)

				val newPtr = header.getFreeSpacePointer - required
				buf.position(newPtr)
				buf.put(data)

				// Allocate slot
				val slotId = SlotDirectory.allocSlot(buf, newPtr, required)
				header.setFreeSpacePointer(newPtr)

				// Increment NEntries (Note: only if a new slot)
				if slotId >= header.getNEntries then
						header.incrementNEntries()
				slotId

		def readRecord(buf: ByteBuffer, slotId: Int): Array[Byte] =
				SlotDirectory.readSlot(buf, slotId) match
						case Some((offset, len)) =>
								val dst = new Array[Byte](len)
								buf.position(offset)
								buf.get(dst, 0, len)
								dst
						case None =>
								throw DbError.RecordNotFound(slotId)

		def deleteRecord(buf: ByteBuffer, slotId: Int): Unit =
				if !SlotDirectory.removeSlot(buf, slotId) then
						throw DbError.SlotNotFoundError(slotId)

		def computeMetrics(buf: ByteBuffer, header: PageHeader): PageMetrics =
				val pageSize = PageLayout.PageSize
				val headerEnd = PageLayout.HEADER_END
				val freePtr = header.getFreeSpacePointer

				val payloadUsed = if freePtr <= pageSize then pageSize - freePtr else 0

				val liveSlots = mutable.ArrayBuffer.empty[(Int, Int)]
				var liveCount = 0

				SlotDirectory.foreachLiveSlot(buf) { (slotId, offset, length) =>
						liveSlots.addOne((offset, length))
						liveCount += 1
						true
				}

				val liveBytes = liveSlots.foldLeft(0)((acc, t) => acc + t._2)

				// compute the largest contiguous free region inside payload area (including holes)
				// Approach:
				//     - consider payload region as [headerEnd, pageSize]
				//     - build list of live ranges sorted ascending by offset
				//     - compute gaps between headerEnd and first range, between ranges, and between last range and pageSize
				val ranges = liveSlots.toList.sortBy(_._1).map { case (off, len) => (off, off + len) }
				val regions = mutable.ArrayBuffer.empty[(Int, Int)] // (start, end)

				ranges.foreach { case (s, e) =>
						val rs = math.max(s, headerEnd)
						val re = math.min(e, pageSize)
						if re > rs then
								regions.addOne((rs, re))
				}

				var largestGap = 0
				var cursor = headerEnd
				if regions.isEmpty then
						largestGap = math.max(largestGap, pageSize - headerEnd)
				else
						for ((s, e) <- regions) do
								if s > cursor then
										val gap = s - cursor
										if gap > largestGap then
												largestGap = gap
								cursor = math.max(cursor, e)
						// final gap after last used range
						if cursor < pageSize then
								val tailGap = pageSize - cursor
								if tailGap > largestGap then
										largestGap = tailGap

				val reclaimable = math.max(0, payloadUsed - liveBytes)
				val fragDenom = reclaimable + largestGap
				val fragRatio =
						if fragDenom == 0 then
								0.0
						else
								reclaimable.toDouble / fragDenom.toDouble

				PageMetrics(
						pageSize = pageSize,
						headerEnd = headerEnd,
						freePtr = freePtr,
						payloadUsedBytes = payloadUsed,
						liveBytes = liveBytes,
						reclaimableBytes = reclaimable,
						liveSlots = liveCount,
						largestContiguousFree = largestGap,
						fragmentationRatio = fragRatio
				)