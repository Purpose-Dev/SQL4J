package sql4j.memory

import sql4j.memory.page.PageMeta
import zio.*
import zio.test.*
import zio.test.Assertion.*

object PageMetaSpec extends ZIOSpecDefault:
		override def spec: Spec[TestEnvironment & Scope, Any] = suite("PageMetaSpec")(
				test("single-thread pin/unpin works") {
						val pm = PageMeta()
						assertTrue(pm.pinnedCount() == 0)
							&& assertTrue(pm.tryPin())
							&& assertTrue(pm.pinnedCount() == 1)
							&& assertTrue(pm.tryUnpin())
							&& assertTrue(pm.pinnedCount() == 0)
				},
				test("double-unpin returns false and pinned remains 0") {
						val pm = PageMeta()
						assertTrue(!pm.tryUnpin())
							&& assertTrue(pm.pinnedCount() == 0)
				},
				test("sealed prevents new pins") {
						val pm = PageMeta()
						pm.setFlag(PageMeta.FLAG_SEALED)
						assertTrue(!pm.tryPin())
				},
				test("concurrent pin/unpin stress") {
						val pm = PageMeta()
						val nFibers = 500
						val opsPerFiber = 250
						val worker = ZIO.foreachPar(1 to nFibers)(_ =>
								ZIO.foreach(1 to opsPerFiber)(_ =>
										ZIO.succeed {
												val ok = pm.tryPin()
												if ok then
														pm.tryUnpin()
												ok
										}
								)
						)
						val program = worker.map(_.forall(_.forall(identity)))
						for {
								r <- program
						} yield assert(r)(equalTo(true))
				}
		)