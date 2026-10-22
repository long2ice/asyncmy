# cython: freethreading_compatible=True
# Declares the module safe to import without re-enabling the GIL on
# free-threaded CPython. Module-level state (encoders/decoders, error_map,
# charset tables, escape table) is built during import and read-only after;
# per-connection state lives on the instances. It does NOT make a single
# Connection/Cursor safe to share between threads.
import re
import time
from decimal import Decimal

from cpython cimport datetime
from cpython.bytes cimport PyBytes_AS_STRING, PyBytes_GET_SIZE

from .constants.FIELD_TYPE import *
from .errors import ProgrammingError

datetime.import_datetime()


cdef inline int _p2(const unsigned char *s) noexcept:
    """Parse exactly two ASCII digits; -1 if not digits."""
    if s[0] < 48 or s[0] > 57 or s[1] < 48 or s[1] > 57:
        return -1
    return (s[0] - 48) * 10 + (s[1] - 48)

cdef inline int _p4(const unsigned char *s) noexcept:
    """Parse exactly four ASCII digits; -1 if not digits."""
    cdef int i, v = 0
    for i in range(4):
        if s[i] < 48 or s[i] > 57:
            return -1
        v = v * 10 + (s[i] - 48)
    return v

cdef inline int _pfrac(const unsigned char *s, Py_ssize_t n) noexcept:
    """Parse up to six fractional-second digits, right-padded to microseconds."""
    cdef:
        int v = 0
        Py_ssize_t i = 0
    while i < n and i < 6:
        if s[i] < 48 or s[i] > 57:
            return -1
        v = v * 10 + (s[i] - 48)
        i += 1
    while i < 6:
        v *= 10
        i += 1
    return v

cdef inline int _days_in_month(int year, int month) noexcept:
    if month == 2:
        if (year % 4 == 0 and year % 100 != 0) or year % 400 == 0:
            return 29
        return 28
    if month == 4 or month == 6 or month == 9 or month == 11:
        return 30
    return 31


cpdef escape_item(val, str charset, mapping: dict = None):
    if mapping is None:
        mapping = encoders
    encoder = mapping.get(type(val))

    # Fallback to default when no encoder found
    if not encoder:
        try:
            encoder = mapping[str]
        except KeyError:
            raise TypeError("no default type converter defined")

    if encoder is escape_sequence:
        val = encoder(val, charset, mapping)
    else:
        val = encoder(val, mapping)
    return val

cpdef str escape_sequence(tuple val, str charset, mapping: dict = None):
    n = []
    for item in val:
        quoted = escape_item(item, charset, mapping)
        n.append(quoted)
    return "(" + ",".join(n) + ")"

cpdef str escape_set(set val, str charset, mapping: dict = None):
    return ",".join([escape_item(x, charset, mapping) for x in val])

cpdef str escape_bool(int value, mapping: dict = None):
    return str(int(value))

cpdef str escape_int(value, mapping: dict = None):
    # Accept arbitrary Python ints: a C integer parameter would overflow on
    # values outside [LLONG_MIN, LLONG_MAX], e.g. unsigned BIGINT 2**64-1 (#35).
    return str(value)

cpdef str escape_float(double value, mapping: dict = None):
    s = repr(value)
    if s in ("inf", "nan"):
        raise ProgrammingError("%s can not be used with MySQL" % s)
    if "e" not in s:
        s += "e0"
    return s

cdef list _escape_table = [chr(x) for x in range(128)]
_escape_table[0] = "\\0"
_escape_table[ord("\\")] = "\\\\"
_escape_table[ord("\n")] = "\\n"
_escape_table[ord("\r")] = "\\r"
_escape_table[ord("\032")] = "\\Z"
_escape_table[ord('"')] = '\\"'
_escape_table[ord("'")] = "\\'"

cdef inline bint _str_needs_escape(str s):
    cdef Py_UCS4 ch
    for ch in s:
        if ch == 0 or ch == 10 or ch == 13 or ch == 26 or ch == 34 or ch == 39 or ch == 92:
            return True
    return False

cdef inline bint _bytes_need_escape(bytes value):
    cdef:
        const unsigned char *p = <const unsigned char *> PyBytes_AS_STRING(value)
        Py_ssize_t i, n = PyBytes_GET_SIZE(value)
        unsigned char c
    for i in range(n):
        c = p[i]
        if c == 0 or c == 10 or c == 13 or c == 26 or c == 34 or c == 39 or c == 92:
            return True
    return False

cpdef str escape_string(value, mapping: dict = None):
    """
    escapes *value* without adding quote.

    Value should be unicode
    """
    # Fast path: most strings contain nothing that needs escaping.
    if type(value) is str and not _str_needs_escape(<str> value):
        return <str> value
    return value.translate(_escape_table)

cpdef str escape_bytes_prefixed(bytes value, mapping: dict = None):
    if not _bytes_need_escape(value):
        return "_binary'%s'" % value.decode("ascii", "surrogateescape")
    return "_binary'%s'" % value.decode("ascii", "surrogateescape").translate(_escape_table)

cpdef str escape_bytes(bytes value, mapping: dict = None):
    if not _bytes_need_escape(value):
        return "'%s'" % value.decode("ascii", "surrogateescape")
    return "'%s'" % value.decode("ascii", "surrogateescape").translate(_escape_table)

cpdef str escape_str(value, mapping: dict = None):
    return "'%s'" % escape_string(str(value), mapping)

cpdef str escape_None(value, mapping: dict = None):
    return "NULL"

cpdef str escape_timedelta(obj: datetime.timedelta, mapping: dict = None):
    sign = ""
    if obj.days < 0:
        sign = "-"
        obj = -obj
    seconds = int(obj.seconds) % 60
    minutes = int(obj.seconds // 60) % 60
    hours = int(obj.seconds // 3600) % 24 + int(obj.days) * 24
    if obj.microseconds:
        fmt = "'{0}{1:02d}:{2:02d}:{3:02d}.{4:06d}'"
    else:
        fmt = "'{0}{1:02d}:{2:02d}:{3:02d}'"
    return fmt.format(sign, hours, minutes, seconds, obj.microseconds)

cpdef str escape_time(obj, mapping: dict = None):
    if obj.microsecond:
        fmt = "'{0.hour:02}:{0.minute:02}:{0.second:02}.{0.microsecond:06}'"
    else:
        fmt = "'{0.hour:02}:{0.minute:02}:{0.second:02}'"
    return fmt.format(obj)

cpdef str escape_datetime(datetime.datetime obj, mapping: dict = None):
    if obj.microsecond:
        fmt = "'{0.year:04}-{0.month:02}-{0.day:02} {0.hour:02}:{0.minute:02}:{0.second:02}.{0.microsecond:06}'"
    else:
        fmt = "'{0.year:04}-{0.month:02}-{0.day:02} {0.hour:02}:{0.minute:02}:{0.second:02}'"
    return fmt.format(obj)

cpdef str escape_date(datetime.date obj, mapping: dict = None):
    fmt = "'{0.year:04}-{0.month:02}-{0.day:02}'"
    return fmt.format(obj)

cpdef str escape_struct_time(obj: time.struct_time, mapping: dict = None):
    return escape_datetime(datetime.datetime(*obj[:6]))

cpdef str decimal2literal(o, d):
    return format(o, "f")

cpdef int _convert_second_fraction(s):
    if not s:
        return 0
    # Pad zeros to ensure the fraction length in microseconds
    s = s.ljust(6, "0")
    return int(s[:6])

DATETIME_RE = re.compile(
    r"(\d{1,4})-(\d{1,2})-(\d{1,2})[T ](\d{1,2}):(\d{1,2}):(\d{1,2})(?:.(\d{1,6}))?"
)

cpdef object convert_datetime(object obj):
    """Returns a DATETIME or TIMESTAMP column value as a datetime object:

      >>> convert_datetime('2007-02-25 23:06:20')
      datetime.datetime(2007, 2, 25, 23, 6, 20)
      >>> convert_datetime('2007-02-25T23:06:20')
      datetime.datetime(2007, 2, 25, 23, 6, 20)

    Illegal values are returned as str:

      >>> convert_datetime('2007-02-31T23:06:20')
      '2007-02-31T23:06:20'
      >>> convert_datetime('0000-00-00 00:00:00')
      '0000-00-00 00:00:00'

    """
    cdef:
        const unsigned char *s
        Py_ssize_t n
        int b_year, b_month, b_day, b_hour, b_minute, b_second, b_usec

    if type(obj) is bytes:
        # Fast path: parse "YYYY-MM-DD HH:MM:SS[.ffffff]" straight from bytes,
        # skipping the intermediate str allocation.
        s = <const unsigned char *> PyBytes_AS_STRING(<bytes> obj)
        n = PyBytes_GET_SIZE(<bytes> obj)
        if (
            n >= 19
            and s[4] == 45 and s[7] == 45  # '-'
            and (s[10] == 32 or s[10] == 84)  # ' ' or 'T'
            and s[13] == 58 and s[16] == 58  # ':'
        ):
            b_year = _p4(s)
            b_month = _p2(s + 5)
            b_day = _p2(s + 8)
            b_hour = _p2(s + 11)
            b_minute = _p2(s + 14)
            b_second = _p2(s + 17)
            if n > 20 and s[19] == 46:  # '.'
                b_usec = _pfrac(s + 20, n - 20)
            elif n == 19:
                b_usec = 0
            else:
                b_usec = -1
            if (
                b_year >= 1
                and 1 <= b_month <= 12
                and 1 <= b_day <= _days_in_month(b_year, b_month)
                and 0 <= b_hour < 24
                and 0 <= b_minute < 60
                and 0 <= b_second < 60
                and b_usec >= 0
            ):
                return datetime.datetime_new(
                    b_year, b_month, b_day, b_hour, b_minute, b_second, b_usec, None
                )
        obj = (<bytes> obj).decode("ascii")
    elif isinstance(obj, bytearray):
        obj = obj.decode("ascii")

    # Fast path: Use string slicing for standard MySQL datetime format
    # Format: "YYYY-MM-DD HH:MM:SS" (19 chars) or "YYYY-MM-DD HH:MM:SS.ffffff" (26 chars)
    cdef int obj_len = len(obj)
    if obj_len >= 19:
        try:
            # Extract components using string slicing (faster than regex)
            # "2007-02-25 23:06:20.123456"
            #  0123456789012345678901234
            year = int(obj[0:4])
            month = int(obj[5:7])
            day = int(obj[8:10])
            hour = int(obj[11:13])
            minute = int(obj[14:16])
            second = int(obj[17:19])

            # Check for microseconds
            if obj_len > 20 and obj[19] == '.':
                microsecond = _convert_second_fraction(obj[20:])
            else:
                microsecond = 0

            return datetime.datetime(year, month, day, hour, minute, second, microsecond)
        except (ValueError, IndexError):
            pass

    # Fallback to regex for non-standard formats
    m = DATETIME_RE.match(obj)
    if not m:
        return convert_date(obj)

    try:
        groups = list(m.groups())
        groups[-1] = _convert_second_fraction(groups[-1])
        return datetime.datetime(*[int(x) for x in groups])
    except ValueError:
        return convert_date(obj)

TIMEDELTA_RE = re.compile(r"(-)?(\d{1,3}):(\d{1,2}):(\d{1,2})(?:.(\d{1,6}))?")

cpdef object convert_timedelta(object obj):
    """Returns a TIME column as a timedelta object:

      >>> convert_timedelta('25:06:17')
      datetime.timedelta(1, 3977)
      >>> convert_timedelta('-25:06:17')
      datetime.timedelta(-2, 83177)

    Illegal values are returned as str:

      >>> convert_timedelta('random crap')
      'random crap'

    Note that MySQL always returns TIME columns as (+|-)HH:MM:SS, but
    can accept values as (+|-)DD HH:MM:SS. The latter format will not
    be parsed correctly by this function.
    """
    cdef:
        const unsigned char *s
        Py_ssize_t n, idx, k, mm_pos
        bint neg = False
        int b_hours, b_minutes, b_seconds, b_usec

    if type(obj) is bytes:
        # Fast path: parse "[-]H{1,3}:MM:SS[.ffffff]" straight from bytes.
        s = <const unsigned char *> PyBytes_AS_STRING(<bytes> obj)
        n = PyBytes_GET_SIZE(<bytes> obj)
        idx = 0
        if n > 0 and s[0] == 45:  # '-'
            neg = True
            idx = 1
        b_hours = 0
        k = 0
        while idx + k < n and k < 3 and 48 <= s[idx + k] <= 57:
            b_hours = b_hours * 10 + (s[idx + k] - 48)
            k += 1
        if k > 0 and idx + k + 6 <= n and s[idx + k] == 58 and s[idx + k + 3] == 58:  # ':'
            mm_pos = idx + k + 1
            b_minutes = _p2(s + mm_pos)
            b_seconds = _p2(s + mm_pos + 3)
            if n > mm_pos + 6 and s[mm_pos + 5] == 46:  # '.'
                b_usec = _pfrac(s + mm_pos + 6, n - (mm_pos + 6))
            elif n == mm_pos + 5:
                b_usec = 0
            else:
                b_usec = -1
            if 0 <= b_minutes < 60 and 0 <= b_seconds < 60 and b_usec >= 0:
                if neg:
                    return datetime.timedelta_new(
                        0, -(b_hours * 3600 + b_minutes * 60 + b_seconds), -b_usec
                    )
                return datetime.timedelta_new(
                    0, b_hours * 3600 + b_minutes * 60 + b_seconds, b_usec
                )
        obj = (<bytes> obj).decode("ascii")
    elif isinstance(obj, bytearray):
        obj = obj.decode("ascii")

    m = TIMEDELTA_RE.match(obj)
    if not m:
        return obj

    try:
        groups = list(m.groups())
        groups[-1] = _convert_second_fraction(groups[-1])
        negate = -1 if groups[0] else 1
        hours, minutes, seconds, microseconds = groups[1:]

        tdelta = (
                datetime.timedelta(
                    hours=int(hours),
                    minutes=int(minutes),
                    seconds=int(seconds),
                    microseconds=int(microseconds),
                )
                * negate
        )
        return tdelta
    except ValueError:
        return obj

TIME_RE = re.compile(r"(\d{1,2}):(\d{1,2}):(\d{1,2})(?:.(\d{1,6}))?")

cpdef object convert_time(object obj):
    """Returns a TIME column as a time object:

      >>> convert_time('15:06:17')
      datetime.time(15, 6, 17)

    Illegal values are returned as str:

      >>> convert_time('-25:06:17')
      '-25:06:17'
      >>> convert_time('random crap')
      'random crap'

    Note that MySQL always returns TIME columns as (+|-)HH:MM:SS, but
    can accept values as (+|-)DD HH:MM:SS. The latter format will not
    be parsed correctly by this function.

    Also note that MySQL's TIME column corresponds more closely to
    Python's timedelta and not time. However if you want TIME columns
    to be treated as time-of-day and not a time offset, then you can
    use set this function as the converter for TIME.
    """
    cdef:
        const unsigned char *s
        Py_ssize_t n
        int b_hour, b_minute, b_second, b_usec

    if type(obj) is bytes:
        # Fast path: parse "HH:MM:SS[.ffffff]" straight from bytes.
        s = <const unsigned char *> PyBytes_AS_STRING(<bytes> obj)
        n = PyBytes_GET_SIZE(<bytes> obj)
        if n >= 8 and s[2] == 58 and s[5] == 58:  # ':'
            b_hour = _p2(s)
            b_minute = _p2(s + 3)
            b_second = _p2(s + 6)
            if n > 9 and s[8] == 46:  # '.'
                b_usec = _pfrac(s + 9, n - 9)
            elif n == 8:
                b_usec = 0
            else:
                b_usec = -1
            if (
                0 <= b_hour < 24
                and 0 <= b_minute < 60
                and 0 <= b_second < 60
                and b_usec >= 0
            ):
                return datetime.time_new(b_hour, b_minute, b_second, b_usec, None)
        obj = (<bytes> obj).decode("ascii")
    elif isinstance(obj, bytearray):
        obj = obj.decode("ascii")

    # Fast path: Use string slicing for standard MySQL time format
    # Format: "HH:MM:SS" (8 chars) or "HH:MM:SS.ffffff" (15 chars)
    cdef int obj_len = len(obj)
    if obj_len >= 8 and obj[0] != '-':
        try:
            # "15:06:17.123456"
            #  01234567
            hour = int(obj[0:2])
            minute = int(obj[3:5])
            second = int(obj[6:8])

            if obj_len > 9 and obj[8] == '.':
                microsecond = _convert_second_fraction(obj[9:])
            else:
                microsecond = 0

            return datetime.time(hour, minute, second, microsecond)
        except (ValueError, IndexError):
            pass

    # Fallback to regex for non-standard formats
    m = TIME_RE.match(obj)
    if not m:
        return obj

    try:
        groups = list(m.groups())
        groups[-1] = _convert_second_fraction(groups[-1])
        hours, minutes, seconds, microseconds = groups
        return datetime.time(
            hour=int(hours),
            minute=int(minutes),
            second=int(seconds),
            microsecond=int(microseconds),
        )
    except ValueError:
        return obj

cpdef object convert_date(obj):
    """Returns a DATE column as a date object:

      >>> convert_date('2007-02-26')
      datetime.date(2007, 2, 26)

    Illegal values are returned as str:

      >>> convert_date('2007-02-31')
      '2007-02-31'
      >>> convert_date('0000-00-00')
      '0000-00-00'

    """
    cdef:
        const unsigned char *s
        int b_year, b_month, b_day

    if type(obj) is bytes:
        # Fast path: parse "YYYY-MM-DD" straight from bytes.
        s = <const unsigned char *> PyBytes_AS_STRING(<bytes> obj)
        if PyBytes_GET_SIZE(<bytes> obj) == 10 and s[4] == 45 and s[7] == 45:  # '-'
            b_year = _p4(s)
            b_month = _p2(s + 5)
            b_day = _p2(s + 8)
            if (
                b_year >= 1
                and 1 <= b_month <= 12
                and 1 <= b_day <= _days_in_month(b_year, b_month)
            ):
                return datetime.date_new(b_year, b_month, b_day)
        obj = (<bytes> obj).decode("ascii")
    elif isinstance(obj, bytearray):
        obj = obj.decode("ascii")

    # Fast path: Use string slicing for standard MySQL date format
    # Format: "YYYY-MM-DD" (10 chars)
    if len(obj) == 10:
        try:
            # "2007-02-26"
            #  0123456789
            year = int(obj[0:4])
            month = int(obj[5:7])
            day = int(obj[8:10])
            return datetime.date(year, month, day)
        except (ValueError, IndexError):
            pass

    # Fallback to split for non-standard formats
    try:
        return datetime.date(*[int(x) for x in obj.split("-", 2)])
    except ValueError:
        return obj

cpdef through(x):
    return x

# def convert_bit(b):
#    b = "\x00" * (8 - len(b)) + b # pad w/ zeroes
#    return struct.unpack(">Q", b)[0]
#
#     the snippet above is right, but MySQLdb doesn't process bits,
#     so we shouldn't either
convert_bit = through

cdef dict encoders = {
    bool: escape_bool,
    int: escape_int,
    float: escape_float,
    str: escape_str,
    bytes: escape_bytes,
    tuple: escape_sequence,
    list: escape_sequence,
    set: escape_sequence,
    frozenset: escape_sequence,
    type(None): escape_None,
    datetime.date: escape_date,
    datetime.datetime: escape_datetime,
    datetime.timedelta: escape_timedelta,
    datetime.time: escape_time,
    time.struct_time: escape_struct_time,
    Decimal: decimal2literal,
}

cdef dict decoders = {
    BIT: convert_bit,
    TINY: int,
    SHORT: int,
    LONG: int,
    FLOAT: float,
    DOUBLE: float,
    LONGLONG: int,
    INT24: int,
    YEAR: int,
    TIMESTAMP: convert_datetime,
    DATETIME: convert_datetime,
    TIME: convert_timedelta,
    DATE: convert_date,
    BLOB: through,
    TINY_BLOB: through,
    MEDIUM_BLOB: through,
    LONG_BLOB: through,
    STRING: through,
    VAR_STRING: through,
    VARCHAR: through,
    DECIMAL: Decimal,
    NEWDECIMAL: Decimal,
}

# for MySQLdb compatibility
conversions = encoders.copy()
conversions.update(decoders)
