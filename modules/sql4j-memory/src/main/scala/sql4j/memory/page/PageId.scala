package sql4j.memory.page

opaque type PageId = Long

object PageId:
		def apply(v: Long): PageId = v

		extension (p: PageId) def value: Long = p
