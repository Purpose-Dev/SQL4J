package sql4j.memory

import sql4j.memory.page.{Page, PageId}

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

trait MemoryPool:
		def allocatePage(): Page
		def freePage(pageId: PageId): Unit

class DefaultMemoryPool(totalSize: Long, pageSize: Int) extends MemoryPool:
		require(totalSize > 0, "totalSize must be > 0")
		require(pageSize > 0 && (totalSize % pageSize == 0), "pageSize must divide totalSize")

		private val capacity = totalSize.toInt
		private val buffer: ByteBuffer = ByteBuffer.allocateDirect(capacity)
		private val numPages: Int = capacity / pageSize
		private val freeQueue = new ConcurrentLinkedQueue[Long]()
		private val nextAlloc = new AtomicLong(0L)

		// lazy fill (prefill free queue)
		(0 until numPages).foreach(i => freeQueue.add(i.toLong))

		override def allocatePage(): Page =
				val pid = freeQueue.poll()
				if pid eq null then
						throw new OutOfMemoryError("MemoryPool: no free pages")
				val offset: Int = (pid.toInt) * pageSize
				val dup = buffer.duplicate()
				// Position/limit for slice
				dup.position(offset)
				dup.limit(offset + pageSize)
				val slice = dup.slice()
				new Page(PageId(pid), slice)

		override def freePage(pageId: PageId): Unit =
				freeQueue.add(pageId.value)


object MemoryPool:
		def make(totalSize: Long, pageSize: Int): MemoryPool = DefaultMemoryPool(totalSize, pageSize)