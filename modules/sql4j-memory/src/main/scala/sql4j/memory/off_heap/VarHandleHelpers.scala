package sql4j.memory.off_heap

import java.lang.invoke.{MethodHandles, VarHandle}
import java.nio.{ByteBuffer, ByteOrder}

/**
 * Helpers for VarHandle on ByteBuffer views.
 *
 * - INT_VH: view for Array[Int], native order
 * - LONG_VH: view for Array[Long], native order
 *
 * These VarHandles enable atomic operations such as getAndAdd/compareAndSet directly on ByteBuffer.
 *
 * IMPORTANT: VarHandle produced by byteBufferViewVarHandle expects an index of type Int
 * representing the element index (not a byte offset). For longs the index counts 8-byte words.
 */
object VarHandleHelpers:
		private val MH = MethodHandles.lookup()

		private val INT_VH: VarHandle =
				MethodHandles.byteBufferViewVarHandle(classOf[Array[Int]], ByteOrder.nativeOrder())

		private val LONG_VH: VarHandle =
				MethodHandles.byteBufferViewVarHandle(classOf[Array[Long]], ByteOrder.nativeOrder())

		inline def getVolatileInt(bb: ByteBuffer, idx: Int): Int =
				INT_VH.getVolatile(bb, idx).asInstanceOf[Int]

		inline def getAndAddInt(bb: ByteBuffer, idx: Int, delta: Int): Int =
				INT_VH.getAndAdd(bb, idx, delta).asInstanceOf[Int]

		inline def compareAndSetInt(bb: ByteBuffer, idx: Int, expected: Int, newVal: Int): Boolean =
				INT_VH.compareAndSet(bb, idx, expected, newVal)

		// ---- LONG helpers: index MUST be Int (element index), not a byte offset
		inline def getVolatileLong(bb: ByteBuffer, byteOffset: Int): Long =
				LONG_VH.getVolatile(bb, byteOffset).asInstanceOf[Long]

		inline def setVolatileLong(bb: ByteBuffer, byteOffset: Int, value: Long): Unit =
				LONG_VH.setVolatile(bb, byteOffset, value)

		inline def getAndAddLong(bb: ByteBuffer, byteOffset: Int, delta: Long): Long =
				LONG_VH.getAndAdd(bb, byteOffset, delta).asInstanceOf[Long]

		inline def compareAndSetLong(bb: ByteBuffer, byteOffset: Int, expected: Long, newVal: Long): Boolean =
				LONG_VH.compareAndSet(bb, byteOffset, expected, newVal)
