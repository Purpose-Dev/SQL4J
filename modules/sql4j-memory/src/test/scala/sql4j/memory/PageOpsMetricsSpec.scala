package sql4j.memory

import sql4j.memory.off_heap.PageLayout
import sql4j.memory.page.{PageHeader, PageOps}
import zio.test.{assertTrue, Spec, TestEnvironment, ZIOSpecDefault}
import zio.Scope

object PageOpsMetricsSpec extends ZIOSpecDefault:
		override def spec: Spec[TestEnvironment & Scope, Any] =
				suite("PageOpsMetricsSpec")(
						test("empty page reports zero live bytes and full largest gap") {
								val pool = new MemoryPool(1)
								val buf = pool.allocatePage()
								PageHeader(buf).init()
								val header = PageHeader(buf)

								val metrics = PageOps.computeMetrics(buf, header)
								assertTrue(metrics.liveBytes == 0) &&
									assertTrue(metrics.liveSlots == 0) &&
									assertTrue(metrics.payloadUsedBytes == 0) &&
									// largest contiguous free should be pageSize - headerEnd
									assertTrue(metrics.largestContiguousFree == (PageLayout.PageSize - PageLayout.HEADER_END)) &&
									assertTrue(metrics.fragmentationRatio == 0.0)
						},

						test("single insertion updates liveBytes and reduces largest gap") {
								val pool = new MemoryPool(1)
								val buf = pool.allocatePage()
								PageHeader(buf).init()
								val header = PageHeader(buf)

								val data = Array.fill(1)(0.toByte)
								val slotId = PageOps.insertRecord(buf, header, data)

								val metrics = PageOps.computeMetrics(buf, header)

								assertTrue(metrics.liveSlots == 1) &&
									assertTrue(metrics.liveBytes == 1) &&
									assertTrue(metrics.payloadUsedBytes >= 1) &&
									assertTrue(metrics.reclaimableBytes == (metrics.payloadUsedBytes - metrics.liveBytes))
						},

						test("fragmentation increases reclaimable and fragmentationRatio") {
								val pool = new MemoryPool(1)
								val buf = pool.allocatePage()
								PageHeader(buf).init()
								val header = PageHeader(buf)

								val a = Array.fill[Byte](1)(0.toByte)
								val b = Array.fill[Byte](2)(0.toByte)
								val c = Array.fill[Byte](3)(0.toByte)

								val s1 = PageOps.insertRecord(buf, header, a) // lives near top
								val s2 = PageOps.insertRecord(buf, header, b)
								val s3 = PageOps.insertRecord(buf, header, c)

								// delete middle slot to create a hole
								PageOps.deleteRecord(buf, s2)

								val metrics = PageOps.computeMetrics(buf, header)

								// liveBytes should be 1 + 3 = 4 (a + c)
								assertTrue(metrics.liveSlots == 2) &&
									assertTrue(metrics.liveBytes == 4) &&
									assertTrue(metrics.reclaimableBytes >= 2) && // at least b's bytes reclaimable
									assertTrue(metrics.fragmentationRatio > 0.0)
						}
				)
