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

		// index conversion helpers
		private inline def byteOffsetToLongIndex(byteOffset: Int): Int =
				byteOffset / java.lang.Long.BYTES

		private inline def byteOffsetToIntIndex(byteOffset: Int): Int =
				byteOffset / java.lang.Integer.BYTES

		// INT helpers (element, index semantics)
		private inline def getVolatileIntElem(bb: ByteBuffer, idx: Int): Int =
				INT_VH.getVolatile(bb, idx).asInstanceOf[Int]

		private inline def getAndAddIntElem(bb: ByteBuffer, idx: Int, delta: Int): Int =
				INT_VH.getAndAdd(bb, idx, delta).asInstanceOf[Int]

		private inline def compareAndSetIntElem(bb: ByteBuffer, idx: Int, expected: Int, newVal: Int): Boolean =
				INT_VH.compareAndSet(bb, idx, expected, newVal)

		// safe wrappers accepting byte offsets
		inline def getVolatileIntAtByteOffset(bb: ByteBuffer, byteOffset: Int): Int =
				getVolatileIntElem(bb, byteOffsetToIntIndex(byteOffset))

		inline def getAndAddIntAtOffset(bb: ByteBuffer, byteOffset: Int, delta: Int): Int =
				getAndAddIntElem(bb, byteOffsetToIntIndex(byteOffset), delta)

		inline def compareAndSetIntAtByteOffset(bb: ByteBuffer, byteOffset: Int, expected: Int, newVal: Int): Boolean =
				compareAndSetIntElem(bb, byteOffsetToIntIndex(byteOffset), expected, newVal)

		// ---- LONG helpers: index MUST be Int (element index), not a byte offset
		private inline def getVolatileLongElem(bb: ByteBuffer, elemIndex: Int): Long =
				LONG_VH.getVolatile(bb, elemIndex).asInstanceOf[Long]

		private inline def setVolatileLongElem(bb: ByteBuffer, elemIndex: Int, value: Long): Unit =
				LONG_VH.setVolatile(bb, elemIndex, value)

		private inline def getAndAddLongElem(bb: ByteBuffer, elemIndex: Int, delta: Long): Long =
				LONG_VH.getAndAdd(bb, elemIndex, delta).asInstanceOf[Long]

		private inline def compareAndSetLongElem(bb: ByteBuffer, elemIndex: Int, expected: Long, newVal: Long): Boolean =
				LONG_VH.compareAndSet(bb, elemIndex, expected, newVal)

		// safe wrappers accepting byte offsets
		inline def getVolatileLongAtByteOffset(bb: ByteBuffer, byteOffset: Int): Long =
				val longIndex = byteOffsetToLongIndex(byteOffset)
				try
						getVolatileLongElem(bb, longIndex)
				catch
						case e: IllegalStateException if e.getMessage.contains("Misaligned") =>
								bb.getLong(byteOffset)

		inline def setVolatileLongAtByteOffset(bb: ByteBuffer, byteOffset: Int, value: Long): Unit =
				val longIndex = byteOffsetToLongIndex(byteOffset)
				try
						setVolatileLongElem(bb, longIndex, value)
				catch
						case e: IllegalStateException if e.getMessage.contains("Misaligned") =>
								bb.putLong(byteOffset, value)

		inline def getAndAddLongAtByteOffset(bb: ByteBuffer, byteOffset: Int, delta: Long): Long =
				val idx = byteOffsetToLongIndex(byteOffset)
				try
						getAndAddLongElem(bb, idx, delta)
				catch
						case e: IllegalStateException if e.getMessage.contains("Misaligned") =>
								val v = bb.getLong(byteOffset)
								val nv = v + delta
								bb.putLong(byteOffset, nv)
								v

		inline def compareAndSetLongAtByteOffset(bb: ByteBuffer, byteOffset: Int, expected: Long, newVal: Long): Boolean =
				val idx = byteOffsetToLongIndex(byteOffset)
				try
						compareAndSetLongElem(bb, idx, expected, newVal)
				catch
						case e: IllegalStateException if e.getMessage.contains("Misaligned") =>
								val cur = bb.getLong(byteOffset)
								if cur == expected then
										bb.putLong(byteOffset, newVal)
										true
								else
										false