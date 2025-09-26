package sql4j.memory.page

import sql4j.core.DbError
import sql4j.memory.off_heap.PageLayout

import java.nio.ByteBuffer

object PageOps:

		def insertRecord(buf: ByteBuffer, header: PageHeader, data: Array[Byte]): Int =
				val freePtr = header.getFreeSpacePointer
				val required = data.length
				val newPtr = freePtr - required

				if newPtr <= PageLayout.HEADER_END then
						throw DbError.PageFullError(required)

				// Write payload
				buf.position(newPtr)
				buf.put(data)

				// Allocate slot
				val slotId = SlotDirectory.allocSlot(buf, newPtr, required)

				// Update header
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
								throw DbError.RecordNotFound(s"Slot '$slotId' was not found or deleted.")

		def deleteRecord(buf: ByteBuffer, slotId: Int): Unit =
				if !SlotDirectory.removeSlot(buf, slotId) then
						throw DbError.SlotNotFoundError(slotId)