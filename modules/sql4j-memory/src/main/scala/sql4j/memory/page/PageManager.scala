package sql4j.memory.page

import sql4j.memory.MemoryPool

import java.nio.ByteBuffer
import scala.collection.mutable

class PageManager(pool: MemoryPool):
		private val pages = mutable.ListBuffer.empty[(ByteBuffer, PageHeader)]

		def newPage(): (ByteBuffer, PageHeader) =
				val buf = pool.allocatePage()
				val header = PageHeader(buf)
				header.init()
				pages.append((buf, header))
				(buf, header)

		def findPage(required: Int): Option[(ByteBuffer, PageHeader)] =
				pages.find {
						case (_, header) => header.canFit(required)
				}

		def releasePage(buf: ByteBuffer): Unit =
				pages.indexWhere(_._1 eq buf) match
						case -1 => ()
						case i =>
								pages.remove(i)
								pool.releasePage(buf)