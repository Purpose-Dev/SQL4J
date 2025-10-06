package sql4j.memory.off_heap

import java.lang.invoke.{MethodHandles, VarHandle}
import java.nio.{ByteBuffer, ByteOrder}

/**
 * Provides low-level utilities for atomic and volatile access to [[ByteBuffer]] content using [[VarHandle]].
 *
 * <p>This object exposes native-order views for both `Int` and `Long` elements, and defines conversion
 * and safe accessors operating on byte offsets.</p>
 *
 * == Overview ==
 * The `byteBufferViewVarHandler` API intercepts the index as an *element index*, not as a byte offset.
 * For long views, each element index represents an 8-byte word.
 *
 * This utility provides:
 * 	- direct element-index operations (fast, but require correct alignment)
 * 	- safe byte-offset wrappers performing index conversion
 *
 * On architectures where misaligned access is not supported, operations gracefully fall back
 * to standard <code>ByteBuffer.getLong / putLong</code> semantics.
 *
 * == Usage ==
 * {{{
 * val buffer = ByteBuffer.allocateDirect(64).order(ByteOrder.nativeOrder())
 * VarHandleHelpers.setVolatileLongAtByteOffset(buffer, 8, 42L)
 * val v = VarHandleHelpers.getVolatileLongAtByteOffset(buffer, 8)
 * assert(v == 42L)
 * }}}
 *
 * @note The API assumes native byte order for all operations.
 */
object VarHandleHelpers:

		/** VarHandler for native-order 32-bit integer view of a [[ByteBuffer]]. */
		private val INT_VH: VarHandle =
				MethodHandles.byteBufferViewVarHandle(classOf[Array[Int]], ByteOrder.nativeOrder())

		/** VarHandle for native-order 64-bit integer view of a [[ByteBuffer]]. */
		private val LONG_VH: VarHandle =
				MethodHandles.byteBufferViewVarHandle(classOf[Array[Long]], ByteOrder.nativeOrder())

		// Index conversion

		/** Converts a byte offset to a 64-bit element index. */
		private inline def byteOffsetToLongIndex(byteOffset: Int): Int =
				byteOffset / java.lang.Long.BYTES

		/** Converts a byte offset to a 32-bit element index. */
		private inline def byteOffsetToIntIndex(byteOffset: Int): Int =
				byteOffset / java.lang.Integer.BYTES

		// INT operations (element-index based)

		/**
		 * Returns the volatile 32-bit integer value at the specified element index.
		 *
		 * @param bb  target buffer
		 * @param idx element index (not a byte offset)
		 * @return current value
		 */
		private inline def getVolatileIntElem(bb: ByteBuffer, idx: Int): Int =
				INT_VH.getVolatile(bb, idx).asInstanceOf[Int]

		/**
		 * Atomically adds `delta` to the integer at the specified element index.
		 *
		 * @param bb    target buffer
		 * @param idx   element index
		 * @param delta value to add
		 * @return previous value before addition
		 */
		private inline def getAndAddIntElem(bb: ByteBuffer, idx: Int, delta: Int): Int =
				INT_VH.getAndAdd(bb, idx, delta).asInstanceOf[Int]

		/**
		 * Atomically compares and sets the integer value at the specified element index.
		 *
		 * @param bb       target buffer
		 * @param idx      element index
		 * @param expected expected value
		 * @param newVal   value to set if comparison succeeds
		 * @return `true` if successful, `false` otherwise
		 */
		private inline def compareAndSetIntElem(bb: ByteBuffer, idx: Int, expected: Int, newVal: Int): Boolean =
				INT_VH.compareAndSet(bb, idx, expected, newVal)

		// Int operation (byte-offset based)

		/**
		 * Returns the volatile integer value at the specified byte offset.
		 *
		 * @param bb         target buffer
		 * @param byteOffset byte offset within buffer
		 * @return current value
		 */
		inline def getVolatileIntAtByteOffset(bb: ByteBuffer, byteOffset: Int): Int =
				getVolatileIntElem(bb, byteOffsetToIntIndex(byteOffset))

		/**
		 * Atomically adds `delta` to the integer value at the specified byte offset.
		 *
		 * @param bb         target buffer
		 * @param byteOffset byte offset (multiple of 4)
		 * @param delta      value to add
		 * @return previous value before addition
		 */
		inline def getAndAddIntAtOffset(bb: ByteBuffer, byteOffset: Int, delta: Int): Int =
				getAndAddIntElem(bb, byteOffsetToIntIndex(byteOffset), delta)

		/**
		 * Atomically compares and sets the integer value at the specified byte offset.
		 *
		 * @param bb         target buffer
		 * @param byteOffset byte offset
		 * @param expected   expected value
		 * @param newVal     value to set if comparison succeeds
		 * @return `true` if successful, `false` otherwise
		 */
		inline def compareAndSetIntAtByteOffset(bb: ByteBuffer, byteOffset: Int, expected: Int, newVal: Int): Boolean =
				compareAndSetIntElem(bb, byteOffsetToIntIndex(byteOffset), expected, newVal)

		// Long operations (element-index based)

		/**
		 * Returns the volatile 64-bit integer value at the specified element index.
		 *
		 * @param bb        target buffer
		 * @param elemIndex element index (not a byte offset)
		 * @return current value
		 */
		private inline def getVolatileLongElem(bb: ByteBuffer, elemIndex: Int): Long =
				LONG_VH.getVolatile(bb, elemIndex).asInstanceOf[Long]

		/**
		 * Sets the volatile 64-bit integer value at the specified element index.
		 *
		 * @param bb        target buffer
		 * @param elemIndex element index
		 * @param value     new value
		 */
		private inline def setVolatileLongElem(bb: ByteBuffer, elemIndex: Int, value: Long): Unit =
				LONG_VH.setVolatile(bb, elemIndex, value)

		/**
		 * Atomically adds `delta` to the 64-bit integer at the specified element index.
		 *
		 * @param bb        target buffer
		 * @param elemIndex element index
		 * @param delta     value to add
		 * @return previous value before addition
		 */
		private inline def getAndAddLongElem(bb: ByteBuffer, elemIndex: Int, delta: Long): Long =
				LONG_VH.getAndAdd(bb, elemIndex, delta).asInstanceOf[Long]

		/**
		 * Atomically compares and sets the 64-bit integer at the specified element index.
		 *
		 * @param bb        target buffer
		 * @param elemIndex element index
		 * @param expected  expected value
		 * @param newVal    value to set if comparison succeeds
		 * @return `true` if successful, `false` otherwise
		 */
		private inline def compareAndSetLongElem(bb: ByteBuffer, elemIndex: Int, expected: Long, newVal: Long): Boolean =
				LONG_VH.compareAndSet(bb, elemIndex, expected, newVal)

		// Long operations (byte-offset based, misalignment-safe)

		/**
		 * Returns the volatile 64-bit integer value at the specified byte offset.</br>
		 * Falls back to standard <code>ByteBuffer.getLong</code> if misaligned.
		 *
		 * @param bb         target buffer
		 * @param byteOffset byte offset
		 * @return current value
		 * @throws IllegalStateException if the buffer is inaccessible
		 */
		inline def getVolatileLongAtByteOffset(bb: ByteBuffer, byteOffset: Int): Long =
				val longIndex = byteOffsetToLongIndex(byteOffset)
				try
						getVolatileLongElem(bb, longIndex)
				catch
						case e: IllegalStateException if e.getMessage.contains("Misaligned") =>
								bb.getLong(byteOffset)

		/**
		 * Sets the volatile 64-bit integer value at the specified byte offset.</br>
		 * Falls back to <code>ByteBuffer.putLong</code> if misaligned.
		 *
		 * @param bb         target buffer
		 * @param byteOffset byte offset
		 * @param value      new value
		 */
		inline def setVolatileLongAtByteOffset(bb: ByteBuffer, byteOffset: Int, value: Long): Unit =
				val longIndex = byteOffsetToLongIndex(byteOffset)
				try
						setVolatileLongElem(bb, longIndex, value)
				catch
						case e: IllegalStateException if e.getMessage.contains("Misaligned") =>
								bb.putLong(byteOffset, value)

		/**
		 * Atomically adds `delta` to the 64-bit integer value at the specified byte offset.</br>
		 * Falls back to non-atomic update if misaligned.
		 *
		 * @param bb         target buffer
		 * @param byteOffset byte offset
		 * @param delta      value to add
		 * @return previous value before addition
		 */
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

		/**
		 * Atomically compares and sets the 64-bit integer value at the specified byte offset.</br>
		 * Falls back to non-atomic update if misaligned.
		 *
		 * @param bb         target buffer
		 * @param byteOffset byte offset
		 * @param expected   expected value
		 * @param newVal     new value to set if comparison succeeds
		 * @return `true` if successful, `false` otherwise
		 */
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