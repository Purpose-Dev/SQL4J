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
		def message: String

		override def getMessage: String = message

object DbError:
		/**
		 * Represents an operation that is not yet implemented or not supported by the current
		 * configuration or version of the database. This is useful for development and
		 * to clearly indicate unsupported features robustly.
		 *
		 * @param message A descriptive message explaining what operation is unsupported.
		 */
		case class UnsupportedOperationError(message: String) extends DbError:
				override def getMessage: String = s"Operation not supported: $message."

		/**
		 * Error indicating that a requested row could not be found based on its logical ID.
		 * This is a common and expected error in database operations.
		 *
		 * @param rowId The logical ID of the row that was not found.
		 */
		case class RowNotFound(rowId: RowId) extends DbError:
				override def message: String = s"Row with ID: '$rowId' not found."

		/**
		 * Error indicating that a duplicate row was found for a given logical ID.
		 * This is useful for detecting and handling data integrity issues.
		 *
		 * @param rowId The logical ID of the row that has duplicates.
		 */
		case class DuplicateRowFound(rowId: RowId) extends DbError:
				override def message: String = s"Row with ID: '$rowId' has duplicate."

		case class PageFullError(needed: Int) extends DbError:
				override def message: String = s"Not enough space in page for $needed bytes."

		case class SlotNotFoundError(slotId: Int) extends DbError:
				override def message: String = s"Slot with slotId: '$slotId' not found."

		case class RecordNotFound(message: String) extends DbError

		case class InvalidPageStateError(message: String) extends DbError

		case class PageAllocationError(message: String) extends DbError:
				override def getMessage: String = s"Failed to allocate page: '$message'."

		case class SegmentNotFound(message: String, segmentId: SegmentId) extends DbError:
				override def getMessage: String = s"Segment not found in page '${segmentId.value}'."