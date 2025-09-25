package sql4j.memory.off_heap

import zio.{UIO, ZIO}

import java.lang.reflect.Method
import java.nio.ByteBuffer
import scala.util.control.NonFatal


/** Minimal abstraction of an off-heap buffer.
 *
 * @note :
 *       <ul>
 *       <li>release() attempts to call the internal “cleaner” of DirectByteBuffer by reflection</li>
 *       <li>if available (JDK 8+). If the call fails, the GC is left to handle the release.</li>
 *       <li>For complete control, you can replace it with Unsafe.allocateMemory / freeMemory.</li>
 *       </ul>
 */
trait OffHeapBuffer:
		/** byteBuffer: view (slice) on native memory (DirectByteBuffer). */
		def byteBuffer: ByteBuffer

		/** capacity: usable capacity in bytes. */
		def capacity: Int

		/** addressOpt: Optional, native address if it can be extracted (platform-dependent). Can be None. */
		def addressOpt: Option[Long]

		/** release(): attempts to free native memory (best-effort). Always safe to call. */
		def release(): UIO[Unit]

object OffHeapBuffer:
		def wrap(byteBuffer: ByteBuffer): OffHeapBuffer = DefaultOffHeapBuffer(byteBuffer)

private final class DefaultOffHeapBuffer(private val origin: ByteBuffer) extends OffHeapBuffer:
		require(origin != null, "ByteBuffer must not be null")

		private val alignedBuffer: ByteBuffer =
				try
						val clazz = origin.getClass
						val addressMethod = clazz.getMethod("address")
						addressMethod.setAccessible(true)
						val baseAddress = addressMethod.invoke(origin).asInstanceOf[Long]

						val align = 8L
						val mod = (baseAddress % align).toInt
						if mod eq 0 then
								// already aligned - use original buffer (duplicate to avoid mutating position/limit)
								val dup = origin.duplicate()
								dup.clear()
								dup
						else
								val pad = (align - mod).toInt
								// create a duplicate, advance position by pad and slice
								val dup = origin.duplicate()
								// ensure capacity/padding fits
								if dup.capacity() <= pad then
										// impossible, fallback to unaligned (will likely fail VarHandle)
										dup.clear()
										dup
								else
										dup.position(pad)
										dup.limit(dup.capacity())
										dup.slice()
				catch
						case NonFatal(_) =>
								// reflection failed — best-effort fallback: use original duplicate.
								val dup = origin.duplicate()
								dup.clear()
								dup

		override val byteBuffer: ByteBuffer = alignedBuffer

		override val capacity: Int = origin.capacity()

		// try to extract native address via reflection (best-effort)
		override lazy val addressOpt: Option[Long] =
				try
						// Works on some JVMs: sun.nio.ch.DirectBuffer#getAddress or jdk.internal.ref.* access
						val originClazz = origin.getClass
						val addressMethod = originClazz.getMethod("address")
						val address = addressMethod.invoke(origin).asInstanceOf[Long]
						Some(addressMethod.invoke(origin).asInstanceOf[Long] + (origin.position() - alignedBuffer.position()))
				catch
						case _: Throwable => None

		/** release memory (best-effort). */
		override def release(): UIO[Unit] =
				// try common trick: call cleaner if present
				ZIO.succeed {
						try
								val clazz = origin.getClass
								try
										// For many JVMs: DirectByteBuffer has a 'cleaner' method (JDK8) or a 'cleaner' field
										val getCleaner: Method = clazz.getMethod("cleaner")
										getCleaner.setAccessible(true)
										val cleaner = getCleaner.invoke(origin)
										if cleaner ne null then
												val cleanMethod = cleaner.getClass.getMethod("clean")
												cleanMethod.setAccessible(true)
												cleanMethod.invoke(cleaner)
								catch
										// fallback: try invoking sun.misc.Cleaner via Unsafe (not portable) -> ignore
										case _: NoSuchMethodException => ()
						catch
								case NonFatal(_) => () // swallow best-effort
				}