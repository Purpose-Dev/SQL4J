package sql4j.memory.page

import sql4j.core.PageId
import sql4j.memory.MemoryPool

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

class PageManager(pool: MemoryPool, capacity: Int = 128):
		private val idGenerator = AtomicLong(1L)
		private val pages = mutable.Map.empty[PageId, PageEntry]
		private val lru = mutable.LinkedHashMap.empty[PageId, PageEntry]

		private inline def get(pageId: PageId): Option[PageEntry] = pages.get(pageId)

		private inline def touch(id: PageId, entry: PageEntry): Unit =
				lru.remove(id)
				lru.put(id, entry)
				if lru.size > capacity then
						evictOne()

		def currentEntry(pageId: PageId): Option[PageEntry] = get(pageId)

		def newPage(): PageEntry =
				val buf = pool.allocatePage()
				PageHeader(buf).init()
				val id = PageId(idGenerator.getAndIncrement())
				val entry = PageEntry(id, buf)
				pages.put(id, entry)
				touch(id, entry)
				entry

		def tryPin(pageId: PageId): Boolean =
				get(pageId) match
						case Some(e) => e.tryPin()
						case None => false

		def tryUnpin(pageId: PageId): Boolean =
				get(pageId) match
						case Some(e) => e.tryUnpin()
						case None => false

		def pin(id: PageId): Boolean =
				get(id) match
						case Some(entry) if entry.meta.tryPin() =>
								touch(id, entry)
								true
						case _ => false

		def unpin(id: PageId): Boolean =
				pages.get(id).exists(_.meta.tryUnpin())

		def compareAndSwap(pageId: PageId, expected: PageEntry, update: PageEntry): Boolean =
				if update.id != pageId then
						throw IllegalArgumentException("update.id must match pageId")

				get(pageId) match
						case Some(current) if current eq expected =>
								pages.update(pageId, update)
								touch(pageId, update)
								true
						case _ => false

		def free(pageId: PageId): Boolean =
				get(pageId) match
						case Some(entry) if entry.meta.pinnedCount() == 0 =>
								pages.remove(pageId)
								lru.remove(pageId)
								pool.releasePage(entry.buffer)
								true
						case _ => false

		def evictOne(): Option[PageId] =
				val victim = lru.iterator.find { case (_, e) => e.meta.pinnedCount() == 0 }
				victim match
						case Some((id, entry)) =>
								pages.remove(id)
								lru.remove(id)
								pool.releasePage(entry.buffer)
								Some(id)
						case None => None

		def snapshotEntries(): List[PageEntry] = pages.values.toList

		def currentCount(): Int = pages.size