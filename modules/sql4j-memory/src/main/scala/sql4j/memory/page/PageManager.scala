package sql4j.memory.page

import sql4j.core.PageId
import sql4j.memory.MemoryPool

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

class PageManager(pool: MemoryPool):
		private val idGenerator = AtomicLong(1L)
		private val directory = ConcurrentHashMap[PageId, PageEntry]()

		private def get(pageId: PageId): Option[PageEntry] =
				Option(directory.get(pageId))

		def newPage(): PageEntry =
				val buf = pool.allocatePage()
				PageHeader(buf).init()
				val id = PageId(idGenerator.getAndIncrement())
				val pm = PageMeta()
				val entry = PageEntry(id, buf, pm)
				directory.put(id, entry)
				entry

		def pin(pageId: PageId): Option[ByteBuffer] =
				get(pageId) match
						case Some(entry) =>
								if entry.tryPin() then
										Some(entry.buffer)
								else
										None
						case None => None

		def unpin(pageId: PageId): Unit =
				get(pageId).foreach(_.tryUnpin())

		def free(pageId: PageId): Boolean =
				get(pageId) match
						case None => false
						case Some(entry) =>
								if entry.meta.pinnedCount() == 0 then
										val removed = directory.remove(pageId)
										if removed ne null then
												pool.releasePage(entry.buffer)
												true
										else
												false
								else
										false

		def foreachEntry(f: PageEntry => Unit): Unit =
				val it = directory.values().iterator()
				while it.hasNext do
						f(it.next())

		def currentCount(): Int = directory.size()