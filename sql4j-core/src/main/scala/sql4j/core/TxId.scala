package sql4j.core

opaque type TxId = Long

object TxId:
		def apply(id: Long): TxId = id

		extension (r: TxId) def value: Long = r
