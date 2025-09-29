package sql4j.memory

import sql4j.memory.page.{PageEntry, PageManager, PageMeta}
import zio.Scope
import zio.test.*

object PageMetaSpec extends ZIOSpecDefault:
		override def spec: Spec[TestEnvironment & Scope, Any] = suite("PageManagerCASSwapSpec")(
				test("pin fails when page sealed") {
						val pool = new MemoryPool(2)
						val mgr = PageManager(pool)
						val entry = mgr.newPage()
						entry.meta.setFlag(PageMeta.FLAG_SEALED)
						val pinned = mgr.tryPin(entry.id)
						assertTrue(!pinned)
				},
				test("CAS swap keeps old visible until success and then new visible") {
						val pool = new MemoryPool(4)
						val mgr = PageManager(pool)

						val entry = mgr.newPage()
						val id = entry.id

						assertTrue(mgr.tryUnpin(id))
						val originalSnapshot = mgr.currentEntry(id)
						assertTrue(originalSnapshot.isDefined)

						// writer creates new buffer (scratch) and new entry
						val scratchBuf = pool.allocatePage()
						val newEntry = PageEntry(id, scratchBuf)

						// simulate WAL append -> we would set lsn; skip for now
						val swapped = mgr.compareAndSwap(id, originalSnapshot.get, newEntry)
						assertTrue(swapped)

						assertTrue(originalSnapshot.get.tryUnpin())

						val current = mgr.currentEntry(id)
						assertTrue(current.isDefined && (current.get eq newEntry))
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
