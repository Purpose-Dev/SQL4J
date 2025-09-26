package sql4j.memory.page

import sql4j.core.DbError
import sql4j.memory.off_heap.PageLayout

import java.nio.ByteBuffer

object PageOps:

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