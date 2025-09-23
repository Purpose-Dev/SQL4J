package sql4j.core

opaque type RowId = Long

object RowId:
		def apply(id: Long): RowId = id

		extension (r: RowId) def value: Long = r
