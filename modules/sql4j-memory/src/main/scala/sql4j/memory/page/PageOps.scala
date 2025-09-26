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

				var freePtr = PageLayout.PageSize
				liveSlots.sortBy(-_._2).foreach { case (slotId, oldOffset, len) =>
						freePtr -= len
						if oldOffset != freePtr then
								val tmp = new Array[Byte](len)
								buf.position(oldOffset)
								buf.get(tmp)
								buf.position(freePtr)
								buf.put(tmp)
								SlotDirectory.writeSlot(buf, slotId, freePtr, len)
				}

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