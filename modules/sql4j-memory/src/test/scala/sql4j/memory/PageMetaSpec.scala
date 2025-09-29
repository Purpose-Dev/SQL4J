package sql4j.memory

import sql4j.memory.page.{PageEntry, PageManager, PageMeta}
import zio.{Scope, ZIO}
import zio.test.*

object PageMetaSpec extends ZIOSpecDefault:
		override def spec: Spec[TestEnvironment & Scope, Any] =
				suite("PageManagerCASSwapSpec")(
						test("single-thread pin/unpin works") {
								val meta = PageMeta()
								assertTrue(meta.pinnedCount() == 0) &&
									assertTrue(meta.tryPin()) &&
									assertTrue(meta.pinnedCount() == 1) &&
									assertTrue(meta.tryUnpin()) &&
									assertTrue(meta.pinnedCount() == 0)
						},
						test("sealed prevents new pins") {
								val meta = PageMeta()
								meta.setFlag(PageMeta.FLAG_SEALED)
								val pinned = meta.tryPin()
								assertTrue(!pinned && meta.pinnedCount() == 0)
						},
						test("double-unpin returns false and leaves pinned at 0") {
								val meta = PageMeta()
								assertTrue(meta.tryPin())
								assertTrue(meta.tryUnpin())
								// second unpin should fail
								assertTrue(!meta.tryUnpin() && meta.pinnedCount() == 0)
						},
						test("concurrent pin/unpin stress") {
								val meta = PageMeta()
								val pins = ZIO.foreachParDiscard(1 to 1000)(_ => ZIO.attempt(meta.tryPin()))
								val unpins = ZIO.foreachParDiscard(1 to 1000)(_ => ZIO.attempt(meta.tryUnpin()))

								for {
										_ <- pins
										_ <- unpins
								} yield assertTrue(meta.pinnedCount() >= 0)
						},
						test("setFlag and clearFlag toggles correctly") {
								val meta = PageMeta()
								meta.setFlag(PageMeta.FLAG_DIRTY)
								val isSet = (meta.flags() & PageMeta.R_FLAG_DIRTY) != 0

								meta.clearFlag(PageMeta.FLAG_DIRTY)

								val isCleared = (meta.flags() & PageMeta.R_FLAG_DIRTY) == 0
								assertTrue(isCleared)
						},
						test("bumpVersion increments version atomically") {
								val meta = PageMeta()
								val v1 = meta.bumpVersion()
								val v2 = meta.bumpVersion()
								val expectedMaskedVersion = (v1 + 1) & ((1 << 24) - 1)
								assertTrue(v2 == expectedMaskedVersion)
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
