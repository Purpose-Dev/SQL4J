package sql4j.core

import scala.reflect.ClassTag

case class RowData(values: Map[String, Any]):
		def get[T: ClassTag](column: String): Option[T] =
				values.get(column).collect { case v: T => v }
