package sql4j.memory.page.utils

import sql4j.memory.off_heap.PageLayout
import sql4j.memory.page.{PageEntry, PageHeader, SlotDirectory}

import java.nio.charset.StandardCharsets

object PageDumper:
		def dump(entry: PageEntry): String =
				val buffer = entry.buffer
				val header = PageHeader(buffer)
				val stringBuilder = StringBuilder()

				stringBuilder.append(s"Page ${entry.id.value} - meta=${entry.meta.getRaw}\n")
				stringBuilder.append(s"  pageSize=${PageLayout.PageSize}, freePtr=${header.getFreeSpacePointer}")
				stringBuilder.append(s", nEntries=${header.getNEntries}\n")

				SlotDirectory.foreachLiveSlot(buffer) { (slotId, offset, length) =>
						stringBuilder.append(s"  slot[$slotId] -> offset=$offset, length=$length, data='")
						//noinspection DuplicatedCode
						val bytes = new Array[Byte](length)
						buffer.position(offset)
						buffer.get(bytes)
						stringBuilder.append(new String(bytes, StandardCharsets.UTF_8))
						stringBuilder.append("'\n")
						true
				}

				stringBuilder.toString()
