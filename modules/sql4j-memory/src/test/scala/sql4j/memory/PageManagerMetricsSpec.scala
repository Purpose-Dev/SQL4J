package sql4j.memory

import sql4j.core.PageId
import sql4j.memory.page.PageManager
import zio.test.{assertTrue, Spec, TestEnvironment, ZIOSpecDefault}
import zio.Scope

object PageManagerMetricsSpec extends ZIOSpecDefault:
		override def spec: Spec[TestEnvironment & Scope, Any] =
				suite("PageManagerMetricsSpec")(
						test("metrics should track hits and misses") {
								val pool = new MemoryPool(8)
								val pm = new PageManager(pool)

								val p1 = pm.newPage()
								val p2 = pm.newPage()
								val nonExisting = PageId(9999L)

								val r1 = pm.currentEntry(p1.id)
								val r2 = pm.currentEntry(p2.id)
								val r3 = pm.currentEntry(nonExisting)

								val m = pm.metrics()

								assertTrue(r1.isDefined) &&
									assertTrue(r2.isDefined) &&
									assertTrue(r3.isEmpty) &&
									assertTrue(m.cacheHits >= 2) &&
									assertTrue(m.cacheMisses >= 1) &&
									assertTrue(m.currentPages == 2)
						},
						test("metrics should reflect evictions and pinned pages") {
								val pool = new MemoryPool(4)
								val pm = new PageManager(pool, capacity = 2)

								val p1 = pm.newPage()
								val p2 = pm.newPage()
								val pinResult = pm.pin(p2.id)

								val p3 = pm.newPage() // this should evict p1 (unpinned)
								val m = pm.metrics()

								assertTrue(pinResult) &&
									assertTrue(m.evictions >= 1) &&
									assertTrue(m.pinnedPages >= 1) &&
									assertTrue(m.currentPages == 2) &&
									assertTrue(m.totalPages == 4) &&
									assertTrue(m.freePages == (4 - 2))
						}
				)