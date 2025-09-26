package sql4j.core

/**
 * Base sealed trait for all domain-specific errors within SQL4J.
 * Using a sealed trait provides exhaustiveness checks for pattern matching on errors,
 * which significantly enhances type safety and helps prevent unhandled error states
 * in a production-grade system. All errors extend Throwable, making them compatible
 * with Java's exception mechanism and standard logging/monitoring tools that expect Throwable.
 *
 * Each error type should be as specific as possible to allow for fine-grained error handling
 * and reporting.
 */
sealed trait DbError extends Throwable:
		/** The error message describing what went wrong */
		def message: String

		override def getMessage: String = message

object DbError:
		/**
		 * Represents an operation that is not yet implemented or not supported by the current
		 * configuration or version of the database.
		 *
		 * This is useful for development and to clearly indicate unsupported features robustly.
		 *
		 * @param message A descriptive message explaining what operation is unsupported
		 */
		case class UnsupportedOperationError(message: String) extends DbError:
				override def getMessage: String = s"Operation not supported: $message"

		/**
		 * Error indicating that a requested row could not be found based on its logical ID.
		 *
		 * This is a common and expected error in database operations when querying for
		 * specific records that may have been deleted or never existed.
		 *
		 * @param rowId The logical ID of the row that was not found
		 */
		case class RowNotFound(rowId: RowId) extends DbError:
				override def message: String = s"Row with ID: '$rowId' not found"

		/**
		 * Error indicating that a duplicate row was found for a given logical ID.
		 *
		 * This is useful for detecting and handling data integrity issues where
		 * unique constraints are violated or data corruption has occurred.
		 *
		 * @param rowId The logical ID of the row that has duplicates
		 */
		case class DuplicateRowFound(rowId: RowId) extends DbError:
				override def message: String = s"Row with ID: '$rowId' has duplicate"

		/**
		 * Error indicating that a page does not have sufficient space to accommodate
		 * the requested data.
		 *
		 * This occurs during insert or update operations when the storage page
		 * cannot fit the additional or modified data.
		 *
		 * @param needed The number of bytes that were needed but not available
		 */
		case class PageFullError(needed: Int) extends DbError:
				override def message: String = s"Not enough space in page for '$needed' bytes"

		/**
		 * Error indicating that a slot with the specified ID could not be found.
		 *
		 * Slots are internal storage units within pages, and this error occurs
		 * when attempting to access a non-existent slot.
		 *
		 * @param slotId The ID of the slot that was not found
		 */
		case class SlotNotFoundError(slotId: Int) extends DbError:
				override def message: String = s"Slot with slotId: '$slotId' not found"

		/**
		 * Error indicating that a record in the specified slot was not found or has been deleted.
		 *
		 * This differs from SlotNotFoundError in that the slot exists but contains
		 * no valid record data, possibly due to deletion or corruption.
		 *
		 * @param slotId The ID of the slot that contains no valid record
		 */
		case class RecordNotFound(slotId: Int) extends DbError:
				override def message: String = s"Slot '$slotId' was not found or deleted"

		/**
		 * Error indicating that a page is in an invalid or corrupted state.
		 *
		 * This can occur due to data corruption, concurrent modification issues,
		 * or internal consistency violations within the page structure.
		 *
		 * @param message A descriptive message explaining the invalid state
		 */
		case class InvalidPageStateError(message: String) extends DbError

		/**
		 * Error indicating that a page could not be allocated in the storage system.
		 *
		 * This typically occurs when the storage medium is full, there are I/O issues,
		 * or the page allocation mechanism encounters an internal error.
		 *
		 * @param message A descriptive message explaining why allocation failed
		 */
		case class PageAllocationError(message: String) extends DbError:
				override def getMessage: String = s"Failed to allocate page: $message"

		/**
		 * Error indicating that a segment with the specified ID could not be found.
		 *
		 * Segments are logical divisions within the database storage, and this error
		 * occurs when attempting to access a non-existent or invalid segment.
		 *
		 * @param segmentId The ID of the segment that was not found
		 */
		case class SegmentNotFound(segmentId: SegmentId) extends DbError:
				override def message: String = s"Segment not found in page '${segmentId.value}'"