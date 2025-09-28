package sql4j.memory.page

import sql4j.core.PageId
import sql4j.memory.MemoryPool

import scala.jdk.CollectionConverters._

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

class PageManager(pool: MemoryPool):
		private val idGenerator = AtomicLong(1L)
		private val directory = ConcurrentHashMap[PageId, PageEntry]()

		private inline def get(pageId: PageId): Option[PageEntry] = Option(directory.get(pageId))

		def currentEntry(pageId: PageId): Option[PageEntry] = get(pageId)

		def newPage(): PageEntry =
				val buf = pool.allocatePage()
				PageHeader(buf).init()
				val id = PageId(idGenerator.getAndIncrement())
				val entry = PageEntry(id, buf)
				directory.put(id, entry)
				entry

		def compareAndSwap(pageId: PageId, expected: PageEntry, update: PageEntry): Boolean =
				if update.id != pageId then
						throw IllegalArgumentException("update.id must match pageId")
				directory.replace(pageId, expected, update)

		def tryPin(pageId: PageId): Boolean =
				get(pageId) match
						case Some(e) => e.tryPin()
						case None => false

		def tryUnpin(pageId: PageId): Boolean =
				get(pageId) match
						case Some(e) => e.tryUnpin()
						case None => false

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

		def snapshotEntries(): List[PageEntry] =
				directory.values().asScala.toList

		def currentCount(): Int = directory.size()