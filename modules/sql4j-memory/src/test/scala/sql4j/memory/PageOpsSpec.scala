package sql4j.memory

import sql4j.core.DbError
import sql4j.memory.off_heap.PageLayout
import sql4j.memory.page.{PageHeader, PageOps}
import zio.{Exit, Scope, ZIO}
import zio.test._
import zio.test.Assertion._
import zio.test.assertZIO

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.util.Random

object PageOpsSpec extends ZIOSpecDefault:
		override def spec: Spec[TestEnvironment & Scope, Any] = suite("PageOpsSpec")(
				test("insert/read/delete record simple flow") {
						val buf = ByteBuffer.allocateDirect(PageLayout.PageSize)
						val header = PageHeader(buf)
						header.init()

						val data = "hello-world".getBytes(StandardCharsets.UTF_8)

						val slotId = PageOps.insertRecord(buf, header, data)
						val read = PageOps.readRecord(buf, slotId)

						assertTrue(new String(read, StandardCharsets.UTF_8) == "hello-world") &&
							assertTrue(header.getNEntries == 1)

						PageOps.deleteRecord(buf, slotId)

						val effect = ZIO.attempt(PageOps.readRecord(buf, slotId))
						assertZIO(effect.exit)(fails(isSubtype[DbError.RecordNotFound](anything)))
				},
				test("insert too large record should throw PageFullError") {
						val buf = ByteBuffer.allocateDirect(PageLayout.PageSize)
						val header = PageHeader(buf)
						header.init()

						val tooBig = new Array[Byte](PageLayout.PageSize)

						val effect = ZIO.attempt(PageOps.insertRecord(buf, header, tooBig))
						assertZIO(effect.exit)(fails(isSubtype[DbError.PageFullError](anything)))
				},
				test("delete non-existing slot should throw SlotNotFoundError") {
						val buf = ByteBuffer.allocateDirect(PageLayout.PageSize)
						val header = PageHeader(buf)
						header.init()

						val effect = ZIO.attempt(PageOps.deleteRecord(buf, 42))
						assertZIO(effect.exit)(fails(isSubtype[DbError.SlotNotFoundError](anything)))
				},
				test("multiple insertions update header and freeSpacePointer correctly") {
						val buf = ByteBuffer.allocateDirect(PageLayout.PageSize)
						val header = PageHeader(buf)
						header.init()

						val d1 = "alpha".getBytes(StandardCharsets.UTF_8)
						val d2 = "beta".getBytes(StandardCharsets.UTF_8)
						val d3 = "gamma-delta".getBytes(StandardCharsets.UTF_8)

						val slot1 = PageOps.insertRecord(buf, header, d1)
						val slot2 = PageOps.insertRecord(buf, header, d2)
						val slot3 = PageOps.insertRecord(buf, header, d3)

						val r1 = new String(PageOps.readRecord(buf, slot1), StandardCharsets.UTF_8)
						val r2 = new String(PageOps.readRecord(buf, slot2), StandardCharsets.UTF_8)
						val r3 = new String(PageOps.readRecord(buf, slot3), StandardCharsets.UTF_8)

						assertTrue(r1 == "alpha", r2 == "beta", r3 == "gamma-delta") &&
							assertTrue(header.getNEntries == 3) &&
							assertTrue(header.getFreeSpacePointer < PageLayout.PageSize)
				},
				test("reuse freed slot after delete") {
						val buf = ByteBuffer.allocateDirect(PageLayout.PageSize)
						val header = PageHeader(buf)
						header.init()

						val d1 = "first".getBytes(StandardCharsets.UTF_8)
						val d2 = "second".getBytes(StandardCharsets.UTF_8)

						val slot1 = PageOps.insertRecord(buf, header, d1)
						val slot2 = PageOps.insertRecord(buf, header, d2)

						// delete slot1
						PageOps.deleteRecord(buf, slot1)

						// new insert should reuse slot1
						val d3 = "third".getBytes(StandardCharsets.UTF_8)
						val slot3 = PageOps.insertRecord(buf, header, d3)

						val r3 = new String(PageOps.readRecord(buf, slot3), StandardCharsets.UTF_8)
						assertTrue(slot3 == slot1) &&
							assertTrue(r3 == "third")
				},
				test("stress: insert/delete many records in random order") {
						val buf = ByteBuffer.allocateDirect(PageLayout.PageSize)
						val header = PageHeader(buf)
						header.init()

						val rnd = new Random(42) // seed for reproducibility
						var live = Map.empty[Int, String]
						val iterations = 500

						for {
								_ <- ZIO.foreachDiscard(0 until iterations) { _ =>
										val action = rnd.nextInt(3) // 0 = insert, 1 = delete, 2 = read
										if action == 0 || live.isEmpty then
												// insert
												val payload = Array.fill[Byte](rnd.nextInt(50) + 1)(rnd.nextInt(126).toByte)
												val str = new String(payload.map(b => if b < 32 then 65 else b), StandardCharsets.UTF_8)
												val effect = ZIO.attempt(PageOps.insertRecord(buf, header, str.getBytes(StandardCharsets.UTF_8)))
												effect.exit.flatMap {
														case Exit.Success(slot) =>
																live = live.updated(slot, str)
																ZIO.unit
														case Exit.Failure(cause) =>
																// acceptable if page is full
																assertZIO(ZIO.succeed(cause.squash))(isSubtype[DbError.PageFullError](anything)).unit
												}
										else if action == 1 then
												// delete random live
												val (slot, _) = live.iterator.drop(rnd.nextInt(live.size)).next()
												PageOps.deleteRecord(buf, slot)
												live = live - slot
												ZIO.unit
										else
												// read random live
												val (slot, expected) = live.iterator.drop(rnd.nextInt(live.size)).next()
												val read = new String(PageOps.readRecord(buf, slot), StandardCharsets.UTF_8)
												assertTrue(read == expected)
								}
						} yield assertTrue(true)
				},
				test("insert with automatic compaction works under fragmentation") {
						val buf = ByteBuffer.allocateDirect(PageLayout.PageSize)
						val header = PageHeader(buf)
						header.init()

						val rnd = new Random(42)
						var live = Map.empty[Int, String]
						val iterations = 300

						for {
								_ <- ZIO.foreachDiscard(0 until iterations) { _ =>
										val action = if live.isEmpty then 0 else rnd.nextInt(3)

										if action == 0 then
												val payloadSize = rnd.nextInt(100) + 1
												val payload = Array.fill[Byte](payloadSize)(rnd.nextInt(126).toByte)
												val str = new String(payload.map(b => if b < 32 then 65 else b), StandardCharsets.UTF_8)

												ZIO.attempt {
														val slot = PageOps.insertRecordWithCompaction(buf, header, str.getBytes)
														live = live.updated(slot, str)
														assertTrue(true)
												}.catchAll {
														case e: DbError.PageFullError => ZIO.succeed(assertTrue(true))
														case other => ZIO.fail(other)
												}
										else if action == 1 then
												val (slot, _) = live.iterator.drop(rnd.nextInt(live.size)).next()
												PageOps.deleteRecord(buf, slot)
												live = live - slot
												ZIO.succeed(assertTrue(true))
										else
												val (slot, expected) = live.iterator.drop(rnd.nextInt(live.size)).next()
												val read = new String(PageOps.readRecord(buf, slot), StandardCharsets.UTF_8)
												ZIO.succeed(assertTrue(read == expected))
								}
						} yield assertTrue(true)
				},
				test("insert triggers compaction when fragmented") {
						val buf = ByteBuffer.allocateDirect(PageLayout.PageSize)
						val header = PageHeader(buf)
						header.init()

						val slots = (1 to 5).map(_ => PageOps.insertRecord(buf, header, Array.fill(10)(0.toByte)))

						PageOps.deleteRecord(buf, slots(1))
						PageOps.deleteRecord(buf, slots(3))

						val bigRecord = Array.fill(25)(1.toByte)
						val slotNew = PageOps.insertRecordWithCompaction(buf, header, bigRecord)

						val liveSlots = Seq(slots(0), slots(2), slots(4), slotNew)
						liveSlots.foreach { s =>
								val data = PageOps.readRecord(buf, s)
								assert(data.nonEmpty)
						}
						assertTrue(header.getFreeSpacePointer >= PageLayout.HEADER_END)
				}
		)