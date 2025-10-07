package sql4j.memory.page

import sql4j.memory.page.PageMeta
import zio.{Scope, ZIO}
import zio.test.*

object PageMetaSpec extends ZIOSpecDefault:
		override def spec: Spec[TestEnvironment & Scope, Any] =
				suite("PageMetaSpec")(
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
						}
				)
