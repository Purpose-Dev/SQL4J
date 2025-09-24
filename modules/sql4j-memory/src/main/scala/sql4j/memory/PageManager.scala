package sql4j.memory

import sql4j.memory.page.{Page, PageId}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.jdk.CollectionConverters._

class PageManager(pool: MemoryPool):
		private final class Meta:
				val pinned = AtomicInteger(0)
				val dirty = AtomicInteger(0)
				val lsnSinceDirty = AtomicLong(0L)

		private val metaMap = ConcurrentHashMap[Long, Meta]()

		// Allocated pages tracked here
		private val pageMap = ConcurrentHashMap[Long, Page]()

		def allocate(): Page =
				val page = pool.allocatePage()
				pageMap.put(page.id.value, page)
				metaMap.put(page.id.value, Meta())
				page

		def free(pageId: PageId): Unit =
				val meta = metaMap.get(pageId.value)
				if meta != null && meta.pinned.get() == 0 then
						// remove mappings and return to pool
						pageMap.remove(pageId.value)
						metaMap.remove(pageId.value)
						pool.freePage(pageId)
				else
						throw new IllegalStateException(s"Page ${pageId.value} is pinned or missing; cannot free")

		def pin(pageId: PageId): Page =
				val meta = metaMap.get(pageId.value)
				if meta eq null then
						throw new NoSuchElementException(s"Page ${pageId.value} not found")
				meta.pinned.incrementAndGet()
				val page = pageMap.get(pageId.value)
				if page eq null then
						meta.pinned.decrementAndGet()
						throw new NoSuchElementException(s"Page ${pageId.value} missing in pageMap")
				page

		def unpin(pageId: PageId): Unit =
				val meta = metaMap.get(pageId.value)
				if meta == null then
						throw new NoSuchElementException(s"Page ${pageId.value} not found")
				val newv = meta.pinned.decrementAndGet()
				if newv < 0 then
						// shouldn't happen: repair
						meta.pinned.incrementAndGet()
						throw new IllegalStateException(s"Page ${pageId.value} unpinned too many times")

		def markDirty(pageId: PageId, lsn: Long): Unit =
				val meta = metaMap.get(pageId.value)
				if meta == null then
						throw new NoSuchElementException(s"Page ${pageId.value} not found")
				meta.dirty.set(1)
				meta.lsnSinceDirty.set(lsn)

		def isDirty(pageId: PageId): Boolean =
				val meta = metaMap.get(pageId.value)
				meta != null && meta.dirty.get() == 1

		def getPinnedCount(pageId: PageId): Int =
				val meta = metaMap.get(pageId.value);
				if meta == null then 0 else meta.pinned.get()

		def listAllocatedPages(): List[PageId] = {
				pageMap.keySet.asScala.map(k => PageId(k)).toList
		}
