package sql4j.memory

import sql4j.memory.off_heap.{OffHeapBuffer, PageLayout}
import sql4j.memory.page.PageHeader
import zio.Scope
import zio.test.*

import java.nio.ByteBuffer

object OffHeapBufferSpec extends ZIOSpecDefault:
		def spec: Spec[TestEnvironment & Scope, Any] = suite("OffHeapBufferSpec")(
				test("allocate direct buffer wrap and basic ops") {
						val buf = ByteBuffer.allocateDirect(PageLayout.PageSize)
						val ob = OffHeapBuffer.wrap(buf)
						assertTrue(ob.capacity == PageLayout.PageSize) &&
							assertTrue(ob.byteBuffer.capacity() == PageLayout.PageSize)
				},
				test("page header set/get and pin/unpin atomic") {
						val buf = ByteBuffer.allocateDirect(PageLayout.PageSize)
						val header = PageHeader(buf)
						header.setPageId(1234L)
						header.setSegmentId(42L)
						assertTrue(header.getPageId == 1234L, header.getSegmentId == 42L)
						// pinned operations
						assertTrue(header.getPinnedCountFromMeta(header.getMetaAtomicVolatile) == 0)
						val pinned = header.tryPin()
						assertTrue(pinned)
						assertTrue(header.getPinnedCountFromMeta(header.getMetaAtomicVolatile) == 1)
						header.unpin()
						assertTrue(header.getPinnedCountFromMeta(header.getMetaAtomicVolatile) == 0)
						// lsn
						header.setLsn(9999L)
						assertTrue(header.getLsn == 9999L)
				}
		)
