package sql4j.memory.page

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

final class Page(val id: PageId, val buffer: ByteBuffer):
		inline def capacity: Int = buffer.capacity()

		inline def limit: Int = buffer.limit()

		inline def position: Int = buffer.position()

		inline def clear(): Unit = buffer.clear()

		def putInt(offset: Int, v: Int): Unit = buffer.putInt(offset, v)

		def getInt(offset: Int): Int = buffer.getInt(offset)

		def putLong(offset: Int, v: Long): Unit = buffer.putLong(offset, v)

		def getLong(offset: Int): Long = buffer.getLong(offset)

		def putByte(offset: Int, v: Byte): Unit = buffer.put(offset, v)

		def getByte(offset: Int): Byte = buffer.get(offset)

		def putBytes(offset: Int, src: Array[Byte]): Unit =
				val oldPos = buffer.position()
				buffer.position(offset)
				buffer.put(src)
				buffer.position(oldPos)

		def getBytes(offset: Int, len: Int, dst: Array[Byte]): Array[Byte] =
				val oldPos = buffer.position()
				buffer.position(offset)
				buffer.get(dst, 0, len)
				buffer.position(oldPos)
				dst

		def putString(offset: Int, str: String): Unit =
				val arr = str.getBytes(StandardCharsets.UTF_8)
				putBytes(offset, arr)

		def getString(offset: Int, len: Int): String =
				val arr = new Array[Byte](len)
				getBytes(offset, len, arr)
				new String(arr, StandardCharsets.UTF_8)
		