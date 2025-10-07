package sql4j.memory

import sql4j.memory.off_heap.PageLayout
import zio.test.{assertTrue, Spec, TestEnvironment, ZIOSpecDefault}
import zio.Scope

object MemoryPoolMetricsSpec extends ZIOSpecDefault:
		override def spec: Spec[TestEnvironment & Scope, Any] =
				suite("MemoryPoolMetricsSpec")(
						test("metrics reflect allocation and releases") {
								val pool = new MemoryPool(totalPages = 8)

								val before = pool.metrics()
								assertTrue(before.freePages == 8, before.allocatedPages == 0)

								val buffers = (1 to 3).map(_ => pool.allocatePage())
								val mid = pool.metrics()

								assertTrue(mid.freePages == 5, mid.allocatedPages == 3)

								pool.releasePage(buffers.head)
								pool.releasePage(buffers(1))
								val after = pool.metrics()

								assertTrue(after.freePages == 7, after.allocatedPages == 1)
						},
						test("page size in metrics matches PageLayout constant") {
								val pool = new MemoryPool(totalPages = 2)
								val metrics = pool.metrics()
								assertTrue(metrics.pageSizeBytes == PageLayout.PageSize)
						}
				)
