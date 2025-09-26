package sql4j.memory

import sql4j.core.DbError
import sql4j.memory.off_heap.PageLayout
import sql4j.memory.page.{PageHeader, PageOps}
import zio.Scope
import zio.test.*

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

object PageOpsSpec extends ZIOSpecDefault:
		override def spec: Spec[TestEnvironment & Scope, Any] = suite("PageOpsSpec")(
				test("insert/read/delete record simple flow") {
						val buf = ByteBuffer.allocateDirect(PageLayout.PageSize)
						val header = PageHeader(buf)
						header.init()

						val data = "hello-world".getBytes(StandardCharsets.UTF_8)

						// Insert record
						val slotId = PageOps.insertRecord(buf, header, data)

						// Read record
						val read = PageOps.readRecord(buf, slotId)
						assertTrue(new String(read, StandardCharsets.UTF_8) == "hello-world")

						// Delete record
						PageOps.deleteRecord(buf, slotId)

						// Reading again should throw
						val thrown = try
								PageOps.readRecord(buf, slotId)
								None
						catch
								case recNotFoundEx: DbError.RecordNotFound => Some(recNotFoundEx)

						assertTrue(thrown.nonEmpty)
				},
				test("insert too large record should throw PageFullError") {
						val buf = ByteBuffer.allocateDirect(PageLayout.PageSize)
						val header = PageHeader(buf)
						header.init()

						val tooBig = new Array[Byte](PageLayout.PageSize)
						val thrown = try
								PageOps.insertRecord(buf, header, tooBig)
								None
						catch
								case pageFullErr: DbError.PageFullError => Some(pageFullErr)

						assertTrue(thrown.nonEmpty)
				},
				test("delete non-existing slot should throw SlotNotFoundError") {
						val buf = ByteBuffer.allocateDirect(PageLayout.PageSize)
						val header = PageHeader(buf)
						header.init()

						val thrown = try
								PageOps.deleteRecord(buf, slotId = 42)
								None
						catch
								case slotNotFoundE: DbError.SlotNotFoundError => Some(slotNotFoundE)

						assertTrue(thrown.nonEmpty)
				}
		)
