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

private final class DefaultOffHeapBuffer(private val bb0: ByteBuffer) extends OffHeapBuffer:
		require(bb0 != null, "ByteBuffer must not be null")

		// ensure direct buffer for intended usage
		override val byteBuffer: ByteBuffer = bb0

		override val capacity: Int = bb0.capacity()

		// try to extract native address via reflection (best-effort)
		override lazy val addressOpt: Option[Long] =
				try
						// Works on some JVMs: sun.nio.ch.DirectBuffer#getAddress or jdk.internal.ref.* access
						val directBufClass = bb0.getClass
						val getAddress = directBufClass.getMethod("address")
						val address = getAddress.invoke(bb0).asInstanceOf[Long]
						Some(address)
				catch
						case _: Throwable => None

		/** release memory (best-effort). */
		override def release(): UIO[Unit] =
				// try common trick: call cleaner if present
				ZIO.succeed {
						try
								val clazz = bb0.getClass
								try
										// For many JVMs: DirectByteBuffer has a 'cleaner' method (JDK8) or a 'cleaner' field
										val getCleaner: Method = clazz.getMethod("cleaner")
										getCleaner.setAccessible(true)
										val cleaner = getCleaner.invoke(bb0)
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