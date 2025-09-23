package sql4j.memory

import sql4j.memory.page.{Page, PageId}

import java.nio.ByteBuffer

trait OffHeapBuffer:
		def byteBuffer: ByteBuffer

		def capacity: Int

		def asPage(id: PageId): Page = new Page(id, byteBuffer)