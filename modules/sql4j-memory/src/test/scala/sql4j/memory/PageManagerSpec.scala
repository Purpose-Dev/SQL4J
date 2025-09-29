package sql4j.memory

import sql4j.memory.page.{PageEntry, PageManager}
import zio.test.{assertTrue, Spec, TestEnvironment, ZIOSpecDefault}
import zio.Scope

object PageManagerSpec extends ZIOSpecDefault:
		override def spec: Spec[TestEnvironment & Scope, Any] =
				suite("PageManagerSpec")(
						test("newPage should allocate and register page") {
								val pool = new MemoryPool(4096)
								val pm = new PageManager(pool)

								val p1 = pm.newPage()
								val p2 = pm.newPage()

								assertTrue(p1.id != p2.id) &&
									assertTrue(pm.currentCount() == 2) &&
									assertTrue(pm.currentEntry(p1.id).contains(p1))
						},
						test("pin/unpin should update ref count and eviction eligibility") {
								val pool = new MemoryPool(4096)
								val pm = new PageManager(pool)
								val p = pm.newPage()

								assertTrue(pm.pin(p.id)) &&
									assertTrue(!pm.free(p.id)) && // cannot free while pinned
									assertTrue(pm.unpin(p.id)) &&
									assertTrue(pm.free(p.id)) // now can free
						},
						test("compareAndSwap should replace entry only if expected matches") {
								val pool = new MemoryPool(4096)
								val pm = new PageManager(pool)

								val p1 = pm.newPage()
								val p2 = PageEntry(p1.id, pool.allocatePage())

								val ok = pm.compareAndSwap(p1.id, p1, p2)
								val wrong = pm.compareAndSwap(p1.id, p1, p2) // p1 is no longer current

								assertTrue(ok) &&
									assertTrue(!wrong) &&
									assertTrue(pm.currentEntry(p1.id).contains(p2))
						},
						test("evictOne should evict least recently used unpinned page") {
								val pool = new MemoryPool(4096)
								val pm = new PageManager(pool, capacity = 2)

								val p1 = pm.newPage()
								val p2 = pm.newPage()
								val p3 = pm.newPage() // triggers eviction

								assertTrue(pm.currentCount() == 2) &&
									assertTrue(pm.currentEntry(p1.id).isEmpty) // p1 evicted
						},
						test("CAS swap fails if expected doesn't match current") {
								val pool = new MemoryPool(4)
								val mgr = PageManager(pool)

								val e1 = mgr.newPage()
								val id = e1.id
								val e2 = PageEntry(id, pool.allocatePage())

								// install e2 by successful replace
								val swapped1 = mgr.compareAndSwap(id, e1, e2)
								assertTrue(swapped1)

								// a writer using stale expected (e1) should now fail
								val e3 = PageEntry(id, pool.allocatePage())
								val swapped2 = mgr.compareAndSwap(id, e1, e3)
								assertTrue(!swapped2)
						}
				)
