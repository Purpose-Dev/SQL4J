package sql4j.core

import zio.{durationInt, Scope, ZIO}
import zio.test.{assertTrue, Spec, TestAspect, TestEnvironment, ZIOSpecDefault}

object CoreSpec extends ZIOSpecDefault:

		def spec: Spec[TestEnvironment & Scope, Any] = suite("SQL4J Core Specification")(
				rowIdSpec,
				txIdSpec,
				columnTypeSpec,
				rowDataSpec,
				dbErrorSpec
		) @@ TestAspect.timeout(30.seconds)

		private val rowIdSpec = suite("RowId")(
				test("should create RowId with positive value") {
						val rowId = RowId(42L)
						assertTrue(rowId.value == 42L)
				},
				test("should create RowId with zero value") {
						val rowId = RowId(0L)
						assertTrue(rowId.value == 0L)
				},
				test("should create RowId with negative value") {
						val rowId = RowId(-2L)
						assertTrue(rowId.value == -2L)
				},
				test("should create RowId with Long.MaxValue value") {
						val rowId = RowId(Long.MaxValue)
						assertTrue(rowId.value == Long.MaxValue)
				},
				test("should create RowId with Long.MinValue value") {
						val rowId = RowId(Long.MinValue)
						assertTrue(rowId.value == Long.MinValue)
				},
				test("two RowIds with same value should be equal") {
						val rowId1 = RowId(100L)
						val rowId2 = RowId(100L)
						assertTrue(rowId1 == rowId2 && rowId1.hashCode == rowId2.hashCode)
				},
				test("two RowIds with different values should not be equal") {
						val rowId1 = RowId(100L)
						val rowId2 = RowId(104L)
						assertTrue(rowId1 != rowId2)
				},
				test("RowId should have meaningful toString") {
						val rowId = RowId(123L)
						assertTrue(rowId.toString.contains("123"))
				},
		)

		private val txIdSpec = suite("TxId")(
				test("should create TxId with positive value") {
						val txId = TxId(99L)
						assertTrue(txId.value == 99L)
				},
				test("should create TxId with zero value") {
						val txId = TxId(0L)
						assertTrue(txId.value == 0L)
				},
				test("should create TxId with maximum Long value") {
						val txId = TxId(Long.MaxValue)
						assertTrue(txId.value == Long.MaxValue)
				},
				test("different TxIds should not be equal") {
						val t1 = TxId(1L)
						val t2 = TxId(2L)
						assertTrue(t1 != t2)
				},
				test("same TxIds should be equal") {
						val t1 = TxId(42L)
						val t2 = TxId(42L)
						assertTrue(t1 == t2 && t1.hashCode == t2.hashCode)
				},
				test("TxId should have meaningful toString") {
						val txId = TxId(456L)
						assertTrue(txId.toString.contains("456"))
				}
		)

		private val segmentIdSpec = suite("SegmentId")(
				test("should create SegmentId with positive value") {
						val segmentId = SegmentId(1L)
						assertTrue(segmentId.value == 1L)
				},
				test("should create SegmentId with zero value") {
						val segmentId = SegmentId(0L)
						assertTrue(segmentId.value == 0L)
				},
				test("different SegmentIds should not be equal") {
						val s1 = SegmentId(10L)
						val s2 = SegmentId(20L)
						assertTrue(s1 != s2)
				},
				test("same SegmentIds should be equal") {
						val s1 = SegmentId(42L)
						val s2 = SegmentId(42L)
						assertTrue(s1 == s2 && s1.hashCode == s2.hashCode)
				}
		)

		private val columnTypeSpec = suite("ColumnType")(
				test("should contain all primitive types") {
						val allTypes = ColumnType.values.toSet
						assertTrue(
								allTypes.contains(ColumnType.ByteType) &&
									allTypes.contains(ColumnType.ShortType) &&
									allTypes.contains(ColumnType.IntType) &&
									allTypes.contains(ColumnType.LongType) &&
									allTypes.contains(ColumnType.FloatType) &&
									allTypes.contains(ColumnType.DoubleType) &&
									allTypes.contains(ColumnType.BooleanType) &&
									allTypes.contains(ColumnType.CharType) &&
									allTypes.contains(ColumnType.StringType) &&
									allTypes.contains(ColumnType.BigDecimalType) &&
									allTypes.contains(ColumnType.BigIntType) &&
									allTypes.contains(ColumnType.BinaryType)
						)
				},
				test("should have exactly 12 types") {
						assertTrue(ColumnType.values.length == 12)
				},
				test("enum values should be distinct") {
						val allTypes = ColumnType.values
						assertTrue(allTypes.distinct.length == allTypes.length)
				},
		)

		private val rowDataSpec = suite("RowData")(
				test("should retrieve typed column values correctly") {
						val row = RowData(Map(
								"id" -> 1,
								"name" -> "Alice",
								"age" -> 30,
								"salary" -> 50000.50,
								"active" -> true
						))

						assertTrue(
								row.get[Int]("id").contains(1) &&
									row.get[String]("name").contains("Alice") &&
									row.get[Int]("age").contains(30) &&
									row.get[Double]("salary").contains(50000.50) &&
									row.get[Boolean]("active").contains(true)
						)
				},
				test("should return None for missing columns") {
						val row = RowData(Map("id" -> 1))
						assertTrue(
								row.get[String]("name").isEmpty &&
									row.get[Int]("age").isEmpty &&
									row.get[Double]("salary").isEmpty
						)
				},
				test("should handle wrong type casting safely") {
						val row = RowData(Map(
								"id" -> "not_a_number",
								"name" -> 123,
								"active" -> "maybe"
						))

						assertTrue(
								row.get[Int]("id").isEmpty &&
									row.get[String]("name").isEmpty &&
									row.get[Boolean]("active").isEmpty
						)
				},
				test("should handle null values") {
						val row = RowData(Map(
								"id" -> null,
								"name" -> "Alice"
						))

						assertTrue(
								row.get[Int]("id").isEmpty &&
									row.get[String]("name").contains("Alice")
						)
				},
				test("should handle empty data map") {
						val row = RowData(Map.empty)
						assertTrue(
								row.get[Int]("any_column").isEmpty &&
									row.values.isEmpty
						)
				},
				test("should support complex types") {
						val row = RowData(Map(
								"decimal" -> BigDecimal("123.456"),
								"bigint" -> BigInt("999999999999999999"),
								"bytes" -> Array[Byte](1, 2, 3, 4)
						))

						assertTrue(
								row.get[BigDecimal]("decimal").contains(BigDecimal("123.456")) &&
									row.get[BigInt]("bigint").contains(BigInt("999999999999999999")) &&
									row.get[Array[Byte]]("bytes").isDefined
						)
				},
				test("should preserve original data immutably") {
						val originalData = Map("id" -> 1, "name" -> "Alice")
						val row = RowData(originalData)

						assertTrue(
								row.values == originalData &&
									row.values.ne(originalData)
						)
				}
		)

		private val dbErrorSpec = suite("DbError")(
				suite("UnsupportedOperationError")(
						test("should be a proper Throwable") {
								val error = DbError.UnsupportedOperationError("SELECT WHERE")
								assertTrue(
										error.isInstanceOf[Throwable] &&
											error.isInstanceOf[DbError] &&
											error.message == "SELECT WHERE"
								)
						},
						test("should format getMessage correctly") {
								val error = DbError.UnsupportedOperationError("MERGE INTO")
								assertTrue(error.getMessage == "Operation not supported: 'MERGE INTO''")
						},
						test("should handle empty message") {
								val error = DbError.UnsupportedOperationError("")
								assertTrue(error.getMessage == "Operation not supported: '''")
						}
				),
				suite("RowNotFound")(
						test("should be a proper Throwable") {
								val rowId = RowId(123L)
								val error = DbError.RowNotFound(rowId)
								assertTrue(
										error.isInstanceOf[Throwable] &&
											error.isInstanceOf[DbError] &&
											error.message == "Row with ID: '123' not found"
								)
						},
						test("should handle zero rowId") {
								val error = DbError.RowNotFound(RowId(0L))
								assertTrue(error.message == "Row with ID: '0' not found")
						},
						test("should handle negative rowId") {
								val error = DbError.RowNotFound(RowId(-1L))
								assertTrue(error.message == "Row with ID: '-1' not found")
						}
				),
				suite("DuplicateRowFound")(
						test("should be a proper Throwable") {
								val rowId = RowId(456L)
								val error = DbError.DuplicateRowFound(rowId)
								assertTrue(
										error.isInstanceOf[Throwable] &&
											error.message == "Row with ID: '456' has duplicate"
								)
						}
				),
				suite("PageFullError")(
						test("should indicate space requirements") {
								val error = DbError.PageFullError(1024)
								assertTrue(
										error.message == "Not enough space in page for '1024' bytes" &&
											error.isInstanceOf[DbError]
								)
						},
						test("should handle zero bytes needed") {
								val error = DbError.PageFullError(0)
								assertTrue(error.message.contains("0"))
						},
						test("should handle large byte requirements") {
								val error = DbError.PageFullError(Int.MaxValue)
								assertTrue(error.message.contains(Int.MaxValue.toString))
						}
				),
				suite("SlotNotFoundError")(
						test("should indicate missing slot") {
								val error = DbError.SlotNotFoundError(42)
								assertTrue(
										error.message == "Slot with slotId: '42' not found" &&
											error.isInstanceOf[DbError]
								)
						},
						test("should handle negative slot IDs") {
								val error = DbError.SlotNotFoundError(-1)
								assertTrue(error.message.contains("-1"))
						}
				),
				suite("RecordNotFound")(
						test("should indicate deleted or missing record") {
								val error = DbError.RecordNotFound(99)
								assertTrue(
										error.message == "Slot '99' was not found or deleted" &&
											error.isInstanceOf[DbError]
								)
						}
				),
				suite("InvalidPageStateError")(
						test("should preserve custom error message") {
								val customMessage = "Page header is corrupted"
								val error = DbError.InvalidPageStateError(customMessage)
								assertTrue(
										error.message == customMessage &&
											error.getMessage == customMessage
								)
						},
						test("should handle empty message") {
								val error = DbError.InvalidPageStateError("")
								assertTrue(error.message == "")
						}
				),
				suite("PageAllocationError")(
						test("should format allocation error message") {
								val error = DbError.PageAllocationError("Disk full")
								assertTrue(
										error.message == "Disk full" &&
											error.getMessage == "Failed to allocate page: Disk full"
								)
						}
				),
				suite("SegmentNotFound")(
						test("should indicate missing segment") {
								val segmentId = SegmentId(789L)
								val error = DbError.SegmentNotFound(segmentId)
								assertTrue(
										error.message == "Segment not found in page '789'" &&
											error.isInstanceOf[DbError]
								)
						}
				),
				test("all errors should extend DbError") {
						val errors = List(
								DbError.UnsupportedOperationError("test"),
								DbError.RowNotFound(RowId(1L)),
								DbError.DuplicateRowFound(RowId(1L)),
								DbError.PageFullError(100),
								DbError.SlotNotFoundError(1),
								DbError.RecordNotFound(1),
								DbError.InvalidPageStateError("test"),
								DbError.PageAllocationError("test"),
								DbError.SegmentNotFound(SegmentId(1L))
						)

						assertTrue(errors.forall(_.isInstanceOf[DbError]))
				},
				test("all errors should be throwable") {
						val errors = List(
								DbError.UnsupportedOperationError("test"),
								DbError.RowNotFound(RowId(1L)),
								DbError.DuplicateRowFound(RowId(1L)),
								DbError.PageFullError(100),
								DbError.SlotNotFoundError(1),
								DbError.RecordNotFound(1),
								DbError.InvalidPageStateError("test"),
								DbError.PageAllocationError("test"),
								DbError.SegmentNotFound(SegmentId(1L))
						)

						assertTrue(errors.forall(_.isInstanceOf[Throwable]))
				},
				test("error pattern matching should be exhaustive") {
						val error: DbError = DbError.RowNotFound(RowId(1L))
						val result = error match {
								case DbError.UnsupportedOperationError(_) => "unsupported"
								case DbError.RowNotFound(_) => "row_not_found"
								case DbError.DuplicateRowFound(_) => "duplicate"
								case DbError.PageFullError(_) => "page_full"
								case DbError.SlotNotFoundError(_) => "slot_not_found"
								case DbError.RecordNotFound(_) => "record_not_found"
								case DbError.InvalidPageStateError(_) => "invalid_page"
								case DbError.PageAllocationError(_) => "allocation_error"
								case DbError.SegmentNotFound(_) => "segment_not_found"
						}

						assertTrue(result == "row_not_found")
				}
		)

		private val integrationSpec = suite("Integration Tests")(
				test("should handle complex data scenarios") {
						val complexRow = RowData(Map(
								"user_id" -> RowId(100L),
								"transaction_id" -> TxId(200L),
								"segment_id" -> SegmentId(300L),
								"metadata" -> Map("key" -> "value"),
								"scores" -> List(1.0, 2.0, 3.0),
								"created_at" -> java.time.Instant.now(),
								"is_active" -> true
						))

						assertTrue(
								complexRow.get[RowId]("user_id").contains(RowId(100L)) &&
									complexRow.get[TxId]("transaction_id").contains(TxId(200L)) &&
									complexRow.get[SegmentId]("segment_id").contains(SegmentId(300L)) &&
									complexRow.get[Boolean]("is_active").contains(true)
						)
				},
				test("should handle concurrent error creation") {
						ZIO.collectAllPar(
								(1 to 100).map { i =>
										ZIO.succeed(DbError.RowNotFound(RowId(i.toLong)))
								}
						).map { errors =>
								assertTrue(
										errors.length == 100 &&
											errors.forall(_.isInstanceOf[DbError.RowNotFound]) &&
											errors.map(_.asInstanceOf[DbError.RowNotFound].rowId.value).toSet.size == 100
								)
						}
				},
				test("should maintain type safety under heavy load") {
						val largeDataSet = (1 to 1000).map { i =>
								s"col_$i" -> (i % 10 match {
										case 0 => i.toString
										case 1 => i
										case 2 => i.toLong
										case 3 => i.toDouble
										case 4 => i.toFloat
										case 5 => (i % 2) == 0
										case 6 => i.toByte
										case 7 => i.toShort
										case 8 => BigInt(i)
										case 9 => BigDecimal(i)
								})
						}.toMap
						val row = RowData(largeDataSet)

						assertTrue(
								row.get[String]("col_1000").contains("1000") &&
									row.get[Int]("col_1001").contains(1001) &&
									row.get[Long]("col_1002").contains(1002L) &&
									row.get[Boolean]("col_1004").contains(false)
						)
				}
		)