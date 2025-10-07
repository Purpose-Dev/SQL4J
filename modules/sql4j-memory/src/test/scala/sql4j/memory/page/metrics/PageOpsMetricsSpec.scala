package sql4j.memory.page.metrics

import sql4j.memory.off_heap.PageLayout
import sql4j.memory.page.{PageHeader, PageOps}
import sql4j.memory.MemoryPool
import zio.test.{assertTrue, Spec, TestEnvironment, ZIOSpecDefault}
import zio.Scope

object PageOpsMetricsSpec extends ZIOSpecDefault:
		override def spec: Spec[TestEnvironment & Scope, Any] =
				suite("PageOpsMetricsSpec (v2)")(
						test("empty page reports full free bytes and zero fragmentation") {
								val pool = new MemoryPool(1)
								val buf = pool.allocatePage()
								val header = PageHeader(buf)
								header.init()

								val metrics = PageOps.computeMetrics(buf, header)

								assertTrue(
										metrics.usedBytes == 0,
										metrics.nEntries == 0,
										metrics.freeBytes == (PageLayout.PageSize - PageLayout.HEADER_END),
										metrics.fragmentationRatio == 0.0
								)
						},
						test("single insertion increases usedBytes and decreases freeBytes") {
								val pool = new MemoryPool(1)
								val buf = pool.allocatePage()
								val header = PageHeader(buf)
								header.init()

								val data = Array.fill(8)(42.toByte)
								val _ = PageOps.insertRecord(buf, header, data)

								val metrics = PageOps.computeMetrics(buf, header)

								assertTrue(
										metrics.nEntries == 1,
										metrics.usedBytes > 0,
										metrics.freeBytes < (PageLayout.PageSize - PageLayout.HEADER_END),
										metrics.fragmentationRatio >= 0.0 && metrics.fragmentationRatio < 1.0
								)
						},
						test("fragmentation increases after deleting a record") {
								val pool = new MemoryPool(1)
								val buf = pool.allocatePage()
								val header = PageHeader(buf)
								header.init()

								val small = Array.fill(4)(0.toByte)
								val medium = Array.fill(16)(0.toByte)
								val large = Array.fill(32)(0.toByte)

								val s1 = PageOps.insertRecord(buf, header, small)
								val s2 = PageOps.insertRecord(buf, header, medium)
								val s3 = PageOps.insertRecord(buf, header, large)

								// delete middle record to create a hole
								PageOps.deleteRecord(buf, s2)

								val metrics = PageOps.computeMetrics(buf, header)

								assertTrue(
										metrics.nEntries == 3, // slots still allocated, but one may be tombstoned
										metrics.usedBytes > 0,
										metrics.fragmentationRatio > 0.0
								)
						},
						test("compaction restores lower fragmentation") {
								val pool = new MemoryPool(1)
								val buf = pool.allocatePage()
								val header = PageHeader(buf)
								header.init()

								val a = Array.fill(8)(0.toByte)
								val b = Array.fill(16)(0.toByte)
								val c = Array.fill(24)(0.toByte)

								val s1 = PageOps.insertRecord(buf, header, a)
								val s2 = PageOps.insertRecord(buf, header, b)
								val s3 = PageOps.insertRecord(buf, header, c)

								PageOps.deleteRecord(buf, s2)

								val before = PageOps.computeMetrics(buf, header)
								PageOps.insertRecordWithCompaction(buf, header, Array.fill(4)(1.toByte))
								val after = PageOps.computeMetrics(buf, header)

								assertTrue(
										before.fragmentationRatio >= 0.0,
										after.fragmentationRatio <= before.fragmentationRatio
								)
						}
				)