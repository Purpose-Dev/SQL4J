package sql4j.core

sealed trait DbError extends Throwable:
		def message: String

		override def getMessage: String = message

object DbError:
		case class UnsupportedOperationError(message: String) extends DbError:
				override def getMessage: String = s"Operation not supported: $message."

		case class RowNotFound(rowId: RowId) extends DbError:
				override def message: String = s"Row with ID: '$rowId' not found."

		case class DuplicateRowFound(rowId: RowId) extends DbError:
				override def message: String = s"Row with ID: '$rowId' has duplicate."
