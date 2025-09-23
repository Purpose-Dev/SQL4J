package sql4j.core

import zio.Scope
import zio.test.{Spec, TestEnvironment, ZIOSpecDefault, assertTrue}

object CoreSpec extends ZIOSpecDefault:

		def spec: Spec[TestEnvironment & Scope, Any] = suite("CoreSpec")(
				suite("RowId")(
						test("should wrap and unwrap correctly") {
								val rowId = RowId(42)
								assertTrue(rowId.value == 42)
						},
						test("two RowIds with same value should be equal") {
								val r1 = RowId(100)
								val r2 = RowId(100)
								assertTrue(r1 == r2)
						}
				),
				suite("TxId")(
						test("should wrap and unwrap correctly") {
								val txId = TxId(99)
								assertTrue(txId.value == 99)
						},
						test("different TxIds should not be equal") {
								val t1 = TxId(1)
								val t2 = TxId(2)
								assertTrue(t1 != t2)
						}
				),
				suite("ColumnType")(
						test("enum should contain primitive types") {
								assertTrue(
										ColumnType.values.contains(ColumnType.ByteType),
										ColumnType.values.contains(ColumnType.ShortType),
										ColumnType.values.contains(ColumnType.IntType),
										ColumnType.values.contains(ColumnType.LongType),
										ColumnType.values.contains(ColumnType.FloatType),
										ColumnType.values.contains(ColumnType.DoubleType),
										ColumnType.values.contains(ColumnType.BooleanType),
										ColumnType.values.contains(ColumnType.CharType),
										ColumnType.values.contains(ColumnType.StringType),
										ColumnType.values.contains(ColumnType.BigDecimalType),
										ColumnType.values.contains(ColumnType.BigIntType),
										ColumnType.values.contains(ColumnType.BinaryType)
								)
						}
				),
				suite("RowData")(
						test("should retrieve a typed column value") {
								val row = RowData(Map("id" -> 1, "name" -> "Alice"))
								val id = row.get[Int]("id")
								val name = row.get[String]("name")
								assertTrue(id.contains(1), name.contains("Alice"))
						},
						test("should return None for missing column") {
								val row = RowData(Map("id" -> 1))
								assertTrue(row.get[String]("name").isEmpty)
						},
						test("should handle wrong type safely") {
								val row = RowData(Map("id" -> "oops"))
								val id = row.get[Int]("id")
								assertTrue(id.isEmpty)
						},
						suite("DbError")(
								test("NotFound should be throwable") {
										val err: Throwable = DbError.NotFound("missing row")
										assertTrue(err.getMessage.contains("missing row"))
								},
								test("DuplicateKey should be throwable") {
										val err: Throwable = DbError.DuplicateKey("duplicate key error")
										assertTrue(err.getMessage.contains("duplicate"))
								},
								test("TransactionFailed should be throwable") {
										val err: Throwable = DbError.TransactionFailed("tx aborted")
										assertTrue(err.getMessage.contains("aborted"))
								}
						)
				)
		)
