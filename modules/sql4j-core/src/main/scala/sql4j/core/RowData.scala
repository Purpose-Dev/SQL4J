package sql4j.core

import scala.reflect.ClassTag

case class RowData(values: Map[String, Any]):
		def get[T: ClassTag](column: String): Option[T] =
				values.get(column).collect { case v: T => v }

object RowData:
		def apply(origin: Map[String, Any]): RowData =
				val copy = origin.foldLeft(Map.empty[String, Any]) {
						case (acc, (k, v)) => acc + (k -> v)
				}
				new RowData(copy)