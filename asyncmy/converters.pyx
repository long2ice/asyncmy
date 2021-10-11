import datetime
import re
import time
from decimal import Decimal

from .constants.FIELD_TYPE import *
from .errors import ProgrammingError


cpdef escape_item(val, str charset, mapping=None):
    if mapping is None:
        mapping = encoders
    encoder = mapping.get(type(val))

    # Fallback to default when no encoder found
    if not encoder:
        try:
            encoder = mapping[str]
        except KeyError:
            raise TypeError("no default type converter defined")

    if encoder in (escape_dict, escape_sequence):
        val = encoder(val, charset, mapping)
    else:
        val = encoder(val, mapping)
    return val

cpdef escape_dict(dict val, str charset, mapping=None):
    n = {}
    for k, v in val.items():
        quoted = escape_item(v, charset, mapping)
        n[k] = quoted
    return n

cpdef escape_sequence(tuple val, str charset, mapping=None):
    n = []
    for item in val:
        quoted = escape_item(item, charset, mapping)
        n.append(quoted)
    return "(" + ",".join(n) + ")"

cpdef escape_set(set val, str charset, mapping=None):
    return ",".join([escape_item(x, charset, mapping) for x in val])

cpdef escape_bool(int value, mapping=None):
    return str(int(value))

cpdef escape_int(long value, mapping=None):
    return str(value)

def escape_float(value, mapping=None):
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

cpdef str escape_string(str value, mapping=None):
    """
    escapes *value* without adding quote.

    Value should be unicode
    """
    return value.translate(_escape_table)

cpdef str escape_bytes_prefixed(bytes value, mapping=None):
    return "_binary'%s'" % value.decode(b"ascii", "surrogateescape").translate(_escape_table)

cpdef str escape_bytes(bytes value, mapping=None):
    return "'%s'" % value.decode(b"ascii", "surrogateescape").translate(_escape_table)

cpdef str escape_str(str value, mapping=None):
    return "'%s'" % escape_string(str(value), mapping)

cpdef str escape_None(value, mapping=None):
    return "NULL"

def escape_timedelta(obj, mapping=None):
    seconds = int(obj.seconds) % 60
    minutes = int(obj.seconds // 60) % 60
    hours = int(obj.seconds // 3600) % 24 + int(obj.days) * 24
    if obj.microseconds:
        fmt = "'{0:02d}:{1:02d}:{2:02d}.{3:06d}'"
    else:
        fmt = "'{0:02d}:{1:02d}:{2:02d}'"
    return fmt.format(hours, minutes, seconds, obj.microseconds)

cpdef str escape_time(obj, mapping=None):
    if obj.microsecond:
        fmt = "'{0.hour:02}:{0.minute:02}:{0.second:02}.{0.microsecond:06}'"
    else:
        fmt = "'{0.hour:02}:{0.minute:02}:{0.second:02}'"
    return fmt.format(obj)

cpdef str  escape_datetime(obj, mapping=None):
    if obj.microsecond:
        fmt = "'{0.year:04}-{0.month:02}-{0.day:02} {0.hour:02}:{0.minute:02}:{0.second:02}.{0.microsecond:06}'"
    else:
        fmt = "'{0.year:04}-{0.month:02}-{0.day:02} {0.hour:02}:{0.minute:02}:{0.second:02}'"
    return fmt.format(obj)

cpdef str  escape_date(obj, mapping=None):
    fmt = "'{0.year:04}-{0.month:02}-{0.day:02}'"
    return fmt.format(obj)

cpdef str  escape_struct_time(obj, mapping=None):
    return escape_datetime(datetime.datetime(*obj[:6]))

cpdef str  decimal2literal(o, d):
    return format(o, "f")

cpdef int  _convert_second_fraction(s):
    if not s:
        return 0
    # Pad zeros to ensure the fraction length in microseconds
    s = s.ljust(6, "0")
    return int(s[:6])

DATETIME_RE = re.compile(
    r"(\d{1,4})-(\d{1,2})-(\d{1,2})[T ](\d{1,2}):(\d{1,2}):(\d{1,2})(?:.(\d{1,6}))?"
)

cpdef convert_datetime(obj):
    """Returns a DATETIME or TIMESTAMP column value as a datetime object:

      >>> convert_datetime('2007-02-25 23:06:20')
      datetime.datetime(2007, 2, 25, 23, 6, 20)
      >>> convert_datetime('2007-02-25T23:06:20')
      datetime.datetime(2007, 2, 25, 23, 6, 20)

    Illegal values are returned as None:

      >>> convert_datetime('2007-02-31T23:06:20') is None
      True
      >>> convert_datetime('0000-00-00 00:00:00') is None
      True

    """
    if isinstance(obj, (bytes, bytearray)):
        obj = obj.decode("ascii")

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

def convert_timedelta(obj):
    """Returns a TIME column as a timedelta object:

      >>> convert_timedelta('25:06:17')
      datetime.timedelta(1, 3977)
      >>> convert_timedelta('-25:06:17')
      datetime.timedelta(-2, 83177)

    Illegal values are returned as None:

      >>> convert_timedelta('random crap') is None
      True

    Note that MySQL always returns TIME columns as (+|-)HH:MM:SS, but
    can accept values as (+|-)DD HH:MM:SS. The latter format will not
    be parsed correctly by this function.
    """
    if isinstance(obj, (bytes, bytearray)):
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

def convert_time(obj):
    """Returns a TIME column as a time object:

      >>> convert_time('15:06:17')
      datetime.time(15, 6, 17)

    Illegal values are returned as None:

      >>> convert_time('-25:06:17') is None
      True
      >>> convert_time('random crap') is None
      True

    Note that MySQL always returns TIME columns as (+|-)HH:MM:SS, but
    can accept values as (+|-)DD HH:MM:SS. The latter format will not
    be parsed correctly by this function.

    Also note that MySQL's TIME column corresponds more closely to
    Python's timedelta and not time. However if you want TIME columns
    to be treated as time-of-day and not a time offset, then you can
    use set this function as the converter for TIME.
    """
    if isinstance(obj, (bytes, bytearray)):
        obj = obj.decode("ascii")

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

cpdef convert_date(obj):
    """Returns a DATE column as a date object:

      >>> convert_date('2007-02-26')
      datetime.date(2007, 2, 26)

    Illegal values are returned as None:

      >>> convert_date('2007-02-31') is None
      True
      >>> convert_date('0000-00-00') is None
      True

    """
    if isinstance(obj, (bytes, bytearray)):
        obj = obj.decode("ascii")
    try:
        return datetime.date(*[int(x) for x in obj.split("-", 2)])
    except ValueError:
        return obj

def through(x):
    return x

# def convert_bit(b):
#    b = "\x00" * (8 - len(b)) + b # pad w/ zeroes
#    return struct.unpack(">Q", b)[0]
#
#     the snippet above is right, but MySQLdb doesn't process bits,
#     so we shouldn't either
convert_bit = through

cpdef dict encoders = {
    bool: escape_bool,
    int: escape_int,
    float: escape_float,
    str: escape_str,
    bytes: escape_bytes,
    tuple: escape_sequence,
    list: escape_sequence,
    set: escape_sequence,
    frozenset: escape_sequence,
    dict: escape_dict,
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
