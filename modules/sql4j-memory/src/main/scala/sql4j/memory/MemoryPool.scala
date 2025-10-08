package sql4j.memory

import sql4j.memory.off_heap.PageLayout
import sql4j.memory.page.PageManager

import java.nio.ByteBuffer
import scala.collection.mutable

/**
 * MemoryPool manages a pool of off-heap pages.
 * Provides simple allocation/release semantics and exposes usage metrics.
 */
class MemoryPool(val totalPages: Int):
		private val freePages = mutable.Stack[ByteBuffer]()
		private val allPages = Array.fill(totalPages)(ByteBuffer.allocateDirect(PageLayout.PageSize))

		allPages.foreach(freePages.push)

		/** Allocate a new page or throw if the pool is exhausted. */
		def allocatePage(): ByteBuffer =
				if freePages.isEmpty then
						throw new RuntimeException("MemoryPool exhausted")
				else
						freePages.pop()

		/** Release a previously allocated page back into the pool. */
		def releasePage(buf: ByteBuffer): Unit =
				buf.clear()
				freePages.push(buf)

		/** Number of pages currently available for allocation. */
		def availablePages: Int = freePages.size

		/** Compute high-level metrics for this memory pool. */
		def metrics(): MemoryPoolMetrics =
				MemoryPoolMetrics(
						totalPages = totalPages,
						freePages = availablePages,
						allocatedPages = totalPages - availablePages,
						pageSizeBytes = PageLayout.PageSize,
				)

		/**
		 * Return pool metrics enriched with fragmentation stats obtained from a PageManager.
		 * Non-breaking: this is an overload that accepts a PageManager to compute fragmentation.
		 */
		def metricsWith(manager: PageManager): MemoryPoolMetrics =
				val managerMetrics = manager.metrics()
				MemoryPoolMetrics(
						totalPages = totalPages,
						freePages = availablePages,
						allocatedPages = totalPages - availablePages,
						pageSizeBytes = PageLayout.PageSize,
						avgFragmentation = managerMetrics.avgFragmentation,
						maxFragmentation = managerMetrics.maxFragmentation
				)
