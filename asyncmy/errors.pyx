from .constants.ER import *
from .structs import H


cdef class MySQLError(Exception):
    """Exception related to operation with MySQL."""

cdef class Warning(MySQLError):
    """Exception raised for important warnings like data truncations
    while inserting, etc."""

cdef class Error(MySQLError):
    """Exception that is the base class of all other error exceptions
    (not Warning)."""

cdef class InterfaceError(Error):
    """Exception raised for errors that are related to the database
    interface rather than the database itself."""

cdef class DatabaseError(Error):
    """Exception raised for errors that are related to the
    database."""

cdef class DataError(DatabaseError):
    """Exception raised for errors that are due to problems with the
    processed data like division by zero, numeric value out of range,
    etc."""

cdef class OperationalError(DatabaseError):
    """Exception raised for errors that are related to the database's
    operation and not necessarily under the control of the programmer,
    e.g. an unexpected disconnect occurs, the data source name is not
    found, a transaction could not be processed, a memory allocation
    error occurred during processing, etc."""

cdef class IntegrityError(DatabaseError):
    """Exception raised when the relational integrity of the database
    is affected, e.g. a foreign key check fails, duplicate key,
    etc."""

cdef class InternalError(DatabaseError):
    """Exception raised when the database encounters an internal
    error, e.g. the cursor is not valid anymore, the transaction is
    out of sync, etc."""

cdef class ProgrammingError(DatabaseError):
    """Exception raised for programming errors, e.g. table not found
    or already exists, syntax error in the SQL statement, wrong number
    of parameters specified, etc."""

cdef class NotSupportedError(DatabaseError):
    """Exception raised in case a method or database API was used
    which is not supported by the database, e.g. requesting a
    .rollback() on a connection that does not support transaction or
    has transactions turned off."""

cdef dict error_map = {}

cdef _map_error(exc, list errors):
    for error in errors:
        error_map[error] = exc

_map_error(
    ProgrammingError, [
        DB_CREATE_EXISTS,
        SYNTAX_ERROR,
        PARSE_ERROR,
        NO_SUCH_TABLE,
        WRONG_DB_NAME,
        WRONG_TABLE_NAME,
        FIELD_SPECIFIED_TWICE,
        INVALID_GROUP_FUNC_USE,
        UNSUPPORTED_EXTENSION,
        TABLE_MUST_HAVE_COLUMNS,
        CANT_DO_THIS_DURING_AN_TRANSACTION,
        WRONG_DB_NAME,
        WRONG_COLUMN_NAME,
    ]
)
_map_error(
    DataError,
    [
        WARN_DATA_TRUNCATED,
        WARN_NULL_TO_NOTNULL,
        WARN_DATA_OUT_OF_RANGE,
        NO_DEFAULT,
        PRIMARY_CANT_HAVE_NULL,
        DATA_TOO_LONG,
        DATETIME_FUNCTION_OVERFLOW,
        TRUNCATED_WRONG_VALUE_FOR_FIELD,
        ILLEGAL_VALUE_FOR_TYPE,
    ]
)
_map_error(
    IntegrityError,
    [
        DUP_ENTRY,
        NO_REFERENCED_ROW,
        NO_REFERENCED_ROW_2,
        ROW_IS_REFERENCED,
        ROW_IS_REFERENCED_2,
        CANNOT_ADD_FOREIGN,
        BAD_NULL_ERROR,
    ]
)
_map_error(
    NotSupportedError,
    [
        WARNING_NOT_COMPLETE_ROLLBACK,
        NOT_SUPPORTED_YET,
        FEATURE_DISABLED,
        UNKNOWN_STORAGE_ENGINE,
    ]
)
_map_error(
    OperationalError,
    [
        DBACCESS_DENIED_ERROR,
        ACCESS_DENIED_ERROR,
        CON_COUNT_ERROR,
        TABLEACCESS_DENIED_ERROR,
        COLUMNACCESS_DENIED_ERROR,
        CONSTRAINT_FAILED,
        LOCK_DEADLOCK,
    ]
)

cpdef raise_mysql_exception(bytes data):
    errno = H.unpack(data[1:3])[0]
    if data[3] == ord("#"):
        err_val = data[9:].decode("utf-8", "replace")
    else:
        err_val = data[3:].decode("utf-8", "replace")
    error_class = error_map.get(errno)
    if error_class is None:
        error_class = InternalError if errno < 1000 else OperationalError
    raise error_class(errno, err_val)
