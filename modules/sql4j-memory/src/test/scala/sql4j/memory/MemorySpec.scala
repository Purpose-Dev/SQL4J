package sql4j.memory

import zio.test.*
import zio.{Scope, ZIO}
import sql4j.memory.page.Page

object MemorySpec extends ZIOSpecDefault:

		override def spec: Spec[TestEnvironment & Scope, Any] = suite("MemorySpec")(
				test("allocate and write/read primitives on a page") {
						val pool = MemoryPool.make(totalSize = 1024 * 1024, pageSize = 4096)
						val page = pool.allocatePage()
						try
								page.putInt(0, 0xDEADBEEF)
								val got = page.getInt(0)
								assertTrue(got == 0xDEADBEEF)
						finally
								pool.freePage(page.id)
				},

				test("page manager pin/unpin and free works") {
						val pool = MemoryPool.make(totalSize = 1024 * 1024, pageSize = 4096)
						val mgr = PageManager(pool)
						val page = mgr.allocate()
						val id = page.id
						// pin twice concurrently simulated
						val p1 = mgr.pin(id)
						val p2 = mgr.pin(id)
						assertTrue(mgr.getPinnedCount(id) == 2) *> ZIO.succeed(mgr.unpin(id)) *> ZIO.succeed(mgr.unpin(id)) *>
							ZIO.attempt(mgr.free(id)).either.map(_.isRight).map(assertTrue(_))
				},

				test("concurrent allocations stress") {
						val pool = MemoryPool.make(totalSize = 4 * 1024 * 1024, pageSize = 4096) // ~1024 pages
						val mgr = PageManager(pool)
						val allocateN = 500
						val fibers: ZIO[Any, Throwable, List[Page]] =
								ZIO.foreachPar((0 until allocateN).toList)(_ => ZIO.attempt(mgr.allocate()))
						fibers.flatMap(pages => ZIO.foreach(pages)(p => ZIO.attempt(mgr.free(p.id)).orDie)).map(_ => assertTrue(true))
				}
		)
