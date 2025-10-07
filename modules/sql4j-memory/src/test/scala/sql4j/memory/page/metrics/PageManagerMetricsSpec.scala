package sql4j.memory.page.metrics

import sql4j.core.PageId
import sql4j.memory.page.PageManager
import sql4j.memory.MemoryPool
import zio.test.{assertTrue, Spec, TestEnvironment, ZIOSpecDefault}
import zio.Scope

object PageManagerMetricsSpec extends ZIOSpecDefault:
		override def spec: Spec[TestEnvironment & Scope, Any] =
				suite("PageManagerMetricsSpec")(
						test("metrics track pinned pages and fragmentation fields") {
								val pool = new MemoryPool(8)
								val manager = new PageManager(pool)

								// allocate 3 pages
								val p1 = manager.newPage()
								val p2 = manager.newPage()
								val p3 = manager.newPage()

								// pin one
								assertTrue(manager.pin(p1.id))

								val m1 = manager.metrics()
								assertTrue(
										m1.currentPages == 3,
										m1.pinnedPages == 1,
										m1.totalPages == pool.totalPages
								)

								// Unpin and free one
								manager.unpin(p1.id)
								manager.free(p1.id)

								val m2 = manager.metrics()
								assertTrue(
										m2.currentPages == 2,
										m2.pinnedPages == 0
								)
						},
						test("fragmentation metrics remain between 0.0 and 1.0") {
								val pool = new MemoryPool(4)
								val manager = new PageManager(pool)

								manager.newPage()
								manager.newPage()

								val m = manager.metrics()
								assertTrue(
										m.avgFragmentation >= 0.0,
										m.avgFragmentation <= 1.0,
										m.maxFragmentation >= 0.0,
										m.maxFragmentation <= 1.0
								)
						}
				)
				