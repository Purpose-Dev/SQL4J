package sql4j.memory

import sql4j.memory.off_heap.PageLayout
import sql4j.memory.page.PageManager
import zio.test.{assertTrue, Spec, TestEnvironment, ZIOSpecDefault}
import zio.Scope

object MemoryPoolMetricsSpec extends ZIOSpecDefault:
		override def spec: Spec[TestEnvironment & Scope, Any] =
				suite("MemoryPoolMetricsSpec")(
						test("metrics reflect allocations and releases") {
								val pool = new MemoryPool(4)
								val before = pool.metrics()
								val p1 = pool.allocatePage()
								val mid = pool.metrics()
								pool.releasePage(p1)
								val after = pool.metrics()

								assertTrue(
										before.freePages == 4,
										mid.freePages == 3,
										after.freePages == 4,
										mid.allocatedPages == 1,
										after.allocatedPages == 0
								)
						},
						test("metricsWith(manager) reports same fragmentation as manager.metrics()") {
								val pool = new MemoryPool(4)
								val manager = new PageManager(pool)
								val pm = manager.metrics()
								val poolMetrics = pool.metricsWith(manager)
								assertTrue(
										math.abs(poolMetrics.avgFragmentation - pm.avgFragmentation) < 1e-9,
										math.abs(poolMetrics.maxFragmentation - pm.maxFragmentation) < 1e-9
								)
						}
				)
