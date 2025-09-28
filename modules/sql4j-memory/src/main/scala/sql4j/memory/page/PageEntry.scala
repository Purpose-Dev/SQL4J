package sql4j.memory.page

import sql4j.core.PageId

import java.nio.ByteBuffer

final class PageEntry(val id: PageId, val buffer: ByteBuffer, val meta: PageMeta):
		// convenience
		def tryPin(): Boolean = meta.tryPin()

		def tryUnpin(): Boolean = meta.tryUnpin()

		def setFlag(f: Long): Unit = meta.setFlag(f)

		def clearFlag(f: Long): Unit = meta.clearFlag(f)

		def version(): Int = meta.version()

		def rawMeta(): Long = meta.getRaw