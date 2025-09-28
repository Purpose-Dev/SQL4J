package sql4j.memory.page

import sql4j.core.PageId

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

// @formatter:off
final class PageEntry(
		val id: PageId,
		val buffer: ByteBuffer,
		val meta: PageMeta,
		val lsnSinceDirty: AtomicLong
):
		// @formatter:on
		override def toString: String =
				s"PageEntry(${id.value}, ver=${meta.version()}, pinned=${meta.pinnedCount()}, lsn=${lsnSinceDirty.get()}"

		def tryPin(): Boolean = meta.tryPin()

		def tryUnpin(): Boolean = meta.tryUnpin()

		def setFlag(f: Long): Unit = meta.setFlag(f)

		def clearFlag(f: Long): Unit = meta.clearFlag(f)

		def version(): Int = meta.version()

		def rawMeta(): Long = meta.getRaw

object PageEntry:
		def apply(id: PageId, buf: ByteBuffer): PageEntry = new PageEntry(id, buf, PageMeta(), new AtomicLong(0L))
