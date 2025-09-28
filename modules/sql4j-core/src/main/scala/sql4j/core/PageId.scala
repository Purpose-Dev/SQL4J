package sql4j.core

opaque type PageId = Long

object PageId {
		def apply(id: Long): PageId = id

		extension (p: PageId) def value: Long = p
}
