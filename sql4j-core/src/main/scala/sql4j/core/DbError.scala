package sql4j.core

sealed trait DbError extends Throwable:
		def msg: String

		override def getMessage: String = msg

object DbError:
		case class NotFound(msg: String) extends DbError

		case class DuplicateKey(msg: String) extends DbError

		case class TransactionFailed(msg: String) extends DbError
