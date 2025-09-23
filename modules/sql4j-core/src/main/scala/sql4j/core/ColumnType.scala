package sql4j.core

/**
 * Represents the various types of columns that can be used in SQL-like operations.
 * This enumeration defines a set of supported data types for columns.
 */
enum ColumnType:
		/** Represents a column with a single byte value. */
		case ByteType
		/** Represents a column with a 16-bit signed integer value. */
		case ShortType
		/** Represents a column with a 32-bit signed integer value. */
		case IntType
		/** Represents a column with a 64-bit signed integer value. */
		case LongType
		/** Represents a column with a 32-bit floating-point value. */
		case FloatType
		/** Represents a column with a 64-bit floating-point value. */
		case DoubleType
		/** Represents a column with a boolean value (true or false). */
		case BooleanType
		/** Represents a column with a single character value. */
		case CharType
		/** Represents a column with a string value. */
		case StringType
		/** Represents a column with a BigDecimal value for high-precision arithmetic. */
		case BigDecimalType
		/** Represents a column with a BigInt value for large integer computations. */
		case BigIntType
		/** Represents a column with binary data (e.g., byte arrays). */
		case BinaryType