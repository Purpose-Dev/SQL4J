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
 */
object VarHandleHelpers:
		private val MH = MethodHandles.lookup()

		val INT_VH: VarHandle =
				MethodHandles.byteBufferViewVarHandle(classOf[Array[Int]], ByteOrder.nativeOrder)

		val LONG_VH: VarHandle =
				MethodHandles.byteBufferViewVarHandle(classOf[Array[Long]], ByteOrder.nativeOrder)

		inline def getVolatileInt(bb: ByteBuffer, idx: Int): Int =
				INT_VH.getVolatile(bb, idx).asInstanceOf[Int]

		inline def getAndAddInt(bb: ByteBuffer, idx: Int, delta: Int): Int =
				INT_VH.getAndAdd(bb, idx, delta).asInstanceOf[Int]

		inline def compareAndSetInt(bb: ByteBuffer, idx: Int, expected: Int, newVal: Int): Boolean =
				INT_VH.compareAndSet(bb, idx, expected, newVal)

		inline def getVolatileLong(bb: ByteBuffer, idx: Long): Long =
				LONG_VH.getVolatile(bb, idx).asInstanceOf[Long]

		inline def setVolatileLong(bb: ByteBuffer, idx: Long, value: Long): Unit =
				LONG_VH.setVolatile(bb, idx, value)

		inline def getAndAddLong(bb: ByteBuffer, idx: Long, delta: Long): Long =
				LONG_VH.getAndAdd(bb, idx, delta).asInstanceOf[Long]

		inline def compareAndSetLong(bb: ByteBuffer, idx: Long, expected: Long, newVal: Long): Boolean =
				LONG_VH.compareAndSet(bb, idx, expected, newVal)

