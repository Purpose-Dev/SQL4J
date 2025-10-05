package sql4j.memory.page

import sql4j.core.PageId
import sql4j.memory.MemoryPool

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

class PageManager(pool: MemoryPool, capacity: Int = 128):
		private val idGenerator = AtomicLong(1L)
		private val pages = mutable.Map.empty[PageId, PageEntry]
		private val lru = mutable.LinkedHashMap.empty[PageId, PageEntry]

		private val hits = AtomicLong(0L)
		private val misses = AtomicLong(0L)
		private val evictions = AtomicLong(0L)

		private inline def evictOne(): Option[PageId] =
				val victim = lru.iterator.find { case (_, e) => e.meta.pinnedCount() == 0 }
				victim match
						case Some((id, entry)) =>
								pages.remove(id)
								lru.remove(id)
								pool.releasePage(entry.buffer)
								evictions.incrementAndGet()
								Some(id)
						case None => None

		private inline def touch(id: PageId, entry: PageEntry): Unit =
				lru.remove(id)
				lru.put(id, entry)
				while lru.size > capacity do
						evictOne()

		private inline def lookupForAccess(id: PageId): Option[PageEntry] =
				pages.get(id) match
						case some@Some(entry) =>
								hits.incrementAndGet()
								touch(id, entry)
								some
						case None =>
								misses.incrementAndGet()
								None

		private inline def lookupInternal(id: PageId): Option[PageEntry] = pages.get(id)

		def currentEntry(pageId: PageId): Option[PageEntry] = lookupForAccess(pageId)

		def newPage(): PageEntry =
				val buf = pool.allocatePage()
				PageHeader(buf).init()
				val id = PageId(idGenerator.getAndIncrement())
				val entry = PageEntry(id, buf)
				pages.put(id, entry)
				touch(id, entry)
				entry

		def tryPin(pageId: PageId): Boolean =
				lookupForAccess(pageId) match
						case Some(entry) => entry.meta.tryPin()
						case None => false

		def tryUnpin(pageId: PageId): Boolean =
				lookupForAccess(pageId) match
						case Some(e) => e.tryUnpin()
						case None => false

		def pin(id: PageId): Boolean = tryPin(id)

		def unpin(id: PageId): Boolean = tryUnpin(id)

		def compareAndSwap(pageId: PageId, expected: PageEntry, update: PageEntry): Boolean =
				if update.id != pageId then
						throw IllegalArgumentException("update.id must match pageId")

				lookupInternal(pageId) match
						case Some(current) if current eq expected =>
								pages.update(pageId, update)
								touch(pageId, update)
								true
						case _ => false

		def free(pageId: PageId): Boolean =
				lookupInternal(pageId) match
						case Some(entry) if entry.meta.pinnedCount() == 0 =>
								pages.remove(pageId)
								lru.remove(pageId)
								pool.releasePage(entry.buffer)
								true
						case _ => false

		def snapshotEntries(): List[PageEntry] = pages.values.toList

		def currentCount(): Int = pages.size

		def metrics(): PageManagerMetrics =
				PageManagerMetrics(
						currentPages = pages.size,
						cacheHits = hits.get(),
						cacheMisses = misses.get(),
						evictions = evictions.get(),
						pinnedPages = pages.values.count(_.meta.pinnedCount() > 0),
						freePages = pool.availablePages,
						totalPages = pool.totalPages
				)

		def metricsWithFragmentation(): (PageManagerMetrics, Double, Double) =
				val frags = pages.values.map { entry =>
						val m = PageOps.computeMetrics(entry.buffer, PageHeader(entry.buffer))
						m.fragmentationRatio
				}.toList
				val averageFragmentation = if frags.isEmpty then 0.0D else frags.sum / frags.size
				val maxFragmentation = if frags.isEmpty then 0.0D else frags.max

				(metrics(), averageFragmentation, maxFragmentation)