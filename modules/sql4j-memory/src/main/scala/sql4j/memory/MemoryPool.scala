package sql4j.memory

import sql4j.memory.off_heap.PageLayout

import java.nio.ByteBuffer
import scala.collection.mutable

class MemoryPool(val totalPages: Int):
		private val freePages = mutable.Stack[ByteBuffer]()
		private val allPages = Array.fill(totalPages)(ByteBuffer.allocateDirect(PageLayout.PageSize))

		allPages.foreach(freePages.push)

		def allocatePage(): ByteBuffer =
				if freePages.isEmpty then
						throw new RuntimeException("MemoryPool exhausted")
				else
						freePages.pop()

		def releasePage(buf: ByteBuffer): Unit =
				buf.clear()
				freePages.push(buf)

		def availablePages: Int = freePages.size