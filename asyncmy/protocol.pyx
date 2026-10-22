# cython: boundscheck=False, wraparound=False, cdivision=True, initializedcheck=False
# cython: freethreading_compatible=True
# Declares the module safe to import without re-enabling the GIL on
# free-threaded CPython. Module-level state (encoders/decoders, error_map,
# charset tables, escape table) is built during import and read-only after;
# per-connection state lives on the instances. It does NOT make a single
# Connection/Cursor safe to share between threads.

from cpython cimport datetime
from cpython.bytearray cimport PyByteArray_AS_STRING, PyByteArray_GET_SIZE
from cpython.bytes cimport (PyBytes_AS_STRING, PyBytes_FromStringAndSize,
                            PyBytes_GET_SIZE)
from cpython.ref cimport Py_INCREF
from cpython.long cimport PyLong_AsLongLongAndOverflow, PyLong_AsUnsignedLongLong
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from cpython.unicode cimport PyUnicode_DecodeASCII, PyUnicode_DecodeUTF8
from libc.string cimport memchr, memcmp, memcpy

from decimal import Decimal

from .constants.FIELD_TYPE import VAR_STRING
from .constants.SERVER_STATUS import SERVER_MORE_RESULTS_EXISTS

include "charset.pxd"
from . import errors, structs

datetime.import_datetime()

# Length-coded integer markers
# NULL_COLUMN = 251, UNSIGNED_CHAR_COLUMN = 251, UNSIGNED_SHORT_COLUMN = 252,
# UNSIGNED_INT24_COLUMN = 253, UNSIGNED_INT64_COLUMN = 254


cdef object _parse_text_int(const unsigned char *p, Py_ssize_t length):
    """Parse canonical integer fields without allocating intermediate bytes."""
    cdef:
        unsigned long long value = 0
        unsigned int digit
        Py_ssize_t i = 0
        bint negative = False
        object boxed

    if 0 < length <= 20:
        if p[0] == 45:  # '-'
            negative = True
            i = 1
        elif p[0] == 43:  # '+'
            i = 1
        if i < length:
            while i < length:
                digit = <unsigned int> p[i] - 48
                if (
                    digit > 9
                    or value > 1844674407370955161ULL
                    or (value == 1844674407370955161ULL and digit > 5)
                ):
                    break
                value = value * 10 + digit
                i += 1
            if i == length:
                if not negative:
                    return value
                if value <= 9223372036854775807ULL:
                    return -<long long> value
                boxed = value
                return -boxed

    # Preserve int() semantics for larger values and noncanonical inputs
    # (whitespace, underscores, invalid digits, ...).
    return int(PyBytes_FromStringAndSize(<const char *> p, length))


cdef tuple _parse_row(const unsigned char *p, Py_ssize_t size, tuple converters):
    """Parse one text-protocol row directly from raw memory.

    ``converters`` holds one ``(code, encoding, converter)`` tuple per column:
    code 0 -> bytes, 1 -> utf8 decode, 2 -> ascii decode, 3 -> decode(encoding).
    """
    cdef:
        Py_ssize_t n = len(converters)
        Py_ssize_t pos = 0
        Py_ssize_t i, length
        unsigned int c
        int code
        tuple row = PyTuple_New(n)
        tuple conv
        object value, converter

    for i in range(n):
        if pos >= size:
            raise errors.InternalError("Truncated row packet")
        c = p[pos]
        pos += 1
        if c == 251:  # NULL
            Py_INCREF(None)
            PyTuple_SET_ITEM(row, i, None)
            continue
        if c < 251:
            length = <Py_ssize_t> c
        elif c == 252:
            length = <Py_ssize_t> (p[pos] | (p[pos + 1] << 8))
            pos += 2
        elif c == 253:
            length = <Py_ssize_t> (p[pos] | (p[pos + 1] << 8) | (p[pos + 2] << 16))
            pos += 3
        elif c == 254:
            length = <Py_ssize_t> (
                <unsigned long long> p[pos]
                | (<unsigned long long> p[pos + 1] << 8)
                | (<unsigned long long> p[pos + 2] << 16)
                | (<unsigned long long> p[pos + 3] << 24)
                | (<unsigned long long> p[pos + 4] << 32)
                | (<unsigned long long> p[pos + 5] << 40)
                | (<unsigned long long> p[pos + 6] << 48)
                | (<unsigned long long> p[pos + 7] << 56)
            )
            pos += 8
        else:
            raise errors.InternalError("Invalid length encoded integer in row")
        if pos + length > size:
            raise errors.InternalError("Truncated row packet")

        conv = <tuple> converters[i]
        code = <int> conv[0]
        converter = conv[2]
        if code == 0 and converter is int:
            value = _parse_text_int(p + pos, length)
            pos += length
            Py_INCREF(value)
            PyTuple_SET_ITEM(row, i, value)
            continue
        if code == 0:
            value = PyBytes_FromStringAndSize(<const char *> (p + pos), length)
        elif code == 1:
            value = PyUnicode_DecodeUTF8(<const char *> (p + pos), length, NULL)
        elif code == 2:
            value = PyUnicode_DecodeASCII(<const char *> (p + pos), length, NULL)
        else:
            value = PyBytes_FromStringAndSize(<const char *> (p + pos), length).decode(<str> conv[1])
        pos += length

        if converter is not None:
            value = converter(value)
        Py_INCREF(value)
        PyTuple_SET_ITEM(row, i, value)
    return row


def parse_rows_from_buffer(bytearray buf, Py_ssize_t pos, Py_ssize_t buf_len,
                           tuple converters, unsigned int seq_id, list rows,
                           bint deprecate_eof=False):
    """Parse as many complete row packets as available in ``buf[pos:buf_len]``.

    Stops (without consuming) at the first packet that is incomplete, has a
    wrong sequence id, is an ERROR/EOF packet, is empty, or spans multiple
    wire packets (16MB). Parsed rows are appended to ``rows``.

    ``buf_len`` is the number of valid bytes (the bytearray's capacity may be
    larger when it is used as a receive buffer).

    Returns ``(new_pos, new_seq_id)``.
    """
    cdef:
        const unsigned char *base = <const unsigned char *> PyByteArray_AS_STRING(buf)
        Py_ssize_t payload_len
        unsigned int first

    while buf_len - pos >= 4:
        payload_len = <Py_ssize_t> (base[pos] | (base[pos + 1] << 8) | (base[pos + 2] << 16))
        if base[pos + 3] != seq_id:
            break  # sequence mismatch: let read_packet() raise the proper error
        if payload_len == 0 or payload_len == 0xFFFFFF:
            break  # empty or multi-packet payload: slow path
        if buf_len - pos - 4 < payload_len:
            break  # incomplete packet
        first = base[pos + 4]
        if first == 0xFF:
            break  # error packet
        if first == 0xFE and (deprecate_eof or payload_len < 9):
            break  # EOF packet / DEPRECATE_EOF terminating OK packet
        rows.append(_parse_row(base + pos + 4, payload_len, converters))
        pos += 4 + payload_len
        seq_id = (seq_id + 1) & 0xFF
    return pos, seq_id


cdef tuple _parse_binary_row(const unsigned char *p, Py_ssize_t size, tuple colspecs):
    """Parse one binary-protocol row from raw memory.

    ``p`` points at the packet payload: [0x00 header][NULL bitmap][values...].
    ``colspecs`` holds one ``(type_code, is_unsigned, code, encoding, converter)``
    tuple per column, where code/encoding/converter follow the text-row rules
    and apply only to length-encoded (string-ish) values.

    Assumes a little-endian host (as does the rest of the wire parsing).
    """
    cdef:
        Py_ssize_t n = len(colspecs)
        Py_ssize_t bitmap_len = (n + 9) >> 3
        Py_ssize_t pos = 1 + bitmap_len
        const unsigned char *bitmap = p + 1
        Py_ssize_t i, length
        unsigned int c, u32
        unsigned long long u64
        int type_code, code, L
        int year, month, day, hour, minute, second
        long usec, days, secs
        float fv
        double dv
        tuple row = PyTuple_New(n)
        tuple spec
        object value, converter

    if size < pos:
        raise errors.InternalError("Truncated binary row packet")

    for i in range(n):
        if bitmap[(i + 2) >> 3] & (1 << ((i + 2) & 7)):
            Py_INCREF(None)
            PyTuple_SET_ITEM(row, i, None)
            continue
        spec = <tuple> colspecs[i]
        type_code = <int> spec[0]

        if type_code == 3 or type_code == 9:  # LONG, INT24 (both 4 bytes)
            if pos + 4 > size:
                raise errors.InternalError("Truncated binary row packet")
            u32 = p[pos] | (p[pos + 1] << 8) | (p[pos + 2] << 16) | (<unsigned int> p[pos + 3] << 24)
            value = u32 if <bint> spec[1] else <int> u32
            pos += 4
        elif type_code == 8:  # LONGLONG
            if pos + 8 > size:
                raise errors.InternalError("Truncated binary row packet")
            u64 = (
                <unsigned long long> p[pos]
                | (<unsigned long long> p[pos + 1] << 8)
                | (<unsigned long long> p[pos + 2] << 16)
                | (<unsigned long long> p[pos + 3] << 24)
                | (<unsigned long long> p[pos + 4] << 32)
                | (<unsigned long long> p[pos + 5] << 40)
                | (<unsigned long long> p[pos + 6] << 48)
                | (<unsigned long long> p[pos + 7] << 56)
            )
            value = u64 if <bint> spec[1] else <long long> u64
            pos += 8
        elif type_code == 1:  # TINY
            value = <unsigned int> p[pos] if <bint> spec[1] else <int> (<signed char> p[pos])
            pos += 1
        elif type_code == 2 or type_code == 13:  # SHORT, YEAR
            c = p[pos] | (p[pos + 1] << 8)
            value = c if (<bint> spec[1] or type_code == 13) else <int> (<short> c)
            pos += 2
        elif type_code == 5:  # DOUBLE
            if pos + 8 > size:
                raise errors.InternalError("Truncated binary row packet")
            memcpy(&dv, p + pos, 8)
            value = dv
            pos += 8
        elif type_code == 4:  # FLOAT
            memcpy(&fv, p + pos, 4)
            value = <double> fv
            pos += 4
        elif type_code == 7 or type_code == 12:  # TIMESTAMP, DATETIME
            L = p[pos]
            pos += 1
            if L == 0:
                value = None
            else:
                if pos + L > size:
                    raise errors.InternalError("Truncated binary row packet")
                year = p[pos] | (p[pos + 1] << 8)
                month = p[pos + 2]
                day = p[pos + 3]
                if L >= 7:
                    hour = p[pos + 4]
                    minute = p[pos + 5]
                    second = p[pos + 6]
                else:
                    hour = minute = second = 0
                if L >= 11:
                    usec = (
                        p[pos + 7] | (p[pos + 8] << 8)
                        | (p[pos + 9] << 16) | (<long> p[pos + 10] << 24)
                    )
                else:
                    usec = 0
                if year >= 1 and 1 <= month <= 12 and 1 <= day <= 31:
                    value = datetime.datetime_new(year, month, day, hour, minute, second, usec, None)
                else:
                    value = None
                pos += L
        elif type_code == 10 or type_code == 14:  # DATE, NEWDATE
            L = p[pos]
            pos += 1
            if L == 0:
                value = None
            else:
                if pos + L > size:
                    raise errors.InternalError("Truncated binary row packet")
                year = p[pos] | (p[pos + 1] << 8)
                month = p[pos + 2]
                day = p[pos + 3]
                if year >= 1 and 1 <= month <= 12 and 1 <= day <= 31:
                    value = datetime.date_new(year, month, day)
                else:
                    value = None
                pos += L
        elif type_code == 11:  # TIME
            L = p[pos]
            pos += 1
            if L == 0:
                value = datetime.timedelta_new(0, 0, 0)
            else:
                if pos + L > size:
                    raise errors.InternalError("Truncated binary row packet")
                days = (
                    p[pos + 1] | (p[pos + 2] << 8)
                    | (p[pos + 3] << 16) | (<long> p[pos + 4] << 24)
                )
                secs = p[pos + 5] * 3600 + p[pos + 6] * 60 + p[pos + 7]
                if L >= 12:
                    usec = (
                        p[pos + 8] | (p[pos + 9] << 8)
                        | (p[pos + 10] << 16) | (<long> p[pos + 11] << 24)
                    )
                else:
                    usec = 0
                if p[pos]:  # negative
                    value = datetime.timedelta_new(-days, -secs, -usec)
                else:
                    value = datetime.timedelta_new(days, secs, usec)
                pos += L
        else:
            # Length-encoded value: strings, blobs, decimals, JSON, BIT, ...
            if pos >= size:
                raise errors.InternalError("Truncated binary row packet")
            c = p[pos]
            pos += 1
            if c < 251:
                length = <Py_ssize_t> c
            elif c == 252:
                length = <Py_ssize_t> (p[pos] | (p[pos + 1] << 8))
                pos += 2
            elif c == 253:
                length = <Py_ssize_t> (p[pos] | (p[pos + 1] << 8) | (p[pos + 2] << 16))
                pos += 3
            elif c == 254:
                length = <Py_ssize_t> (
                    <unsigned long long> p[pos]
                    | (<unsigned long long> p[pos + 1] << 8)
                    | (<unsigned long long> p[pos + 2] << 16)
                    | (<unsigned long long> p[pos + 3] << 24)
                    | (<unsigned long long> p[pos + 4] << 32)
                    | (<unsigned long long> p[pos + 5] << 40)
                    | (<unsigned long long> p[pos + 6] << 48)
                    | (<unsigned long long> p[pos + 7] << 56)
                )
                pos += 8
            else:
                raise errors.InternalError("Invalid length encoded integer in binary row")
            if pos + length > size:
                raise errors.InternalError("Truncated binary row packet")
            code = <int> spec[2]
            if code == 0:
                value = PyBytes_FromStringAndSize(<const char *> (p + pos), length)
            elif code == 1:
                value = PyUnicode_DecodeUTF8(<const char *> (p + pos), length, NULL)
            elif code == 2:
                value = PyUnicode_DecodeASCII(<const char *> (p + pos), length, NULL)
            else:
                value = PyBytes_FromStringAndSize(<const char *> (p + pos), length).decode(<str> spec[3])
            pos += length
            converter = spec[4]
            if converter is not None:
                value = converter(value)

        Py_INCREF(value)
        PyTuple_SET_ITEM(row, i, value)
    return row


def parse_binary_rows_from_buffer(bytearray buf, Py_ssize_t pos, Py_ssize_t buf_len,
                                  tuple colspecs, unsigned int seq_id, list rows,
                                  bint deprecate_eof=False):
    """Binary-protocol counterpart of parse_rows_from_buffer."""
    cdef:
        const unsigned char *base = <const unsigned char *> PyByteArray_AS_STRING(buf)
        Py_ssize_t payload_len
        unsigned int first

    while buf_len - pos >= 4:
        payload_len = <Py_ssize_t> (base[pos] | (base[pos + 1] << 8) | (base[pos + 2] << 16))
        if base[pos + 3] != seq_id:
            break
        if payload_len == 0 or payload_len == 0xFFFFFF:
            break
        if buf_len - pos - 4 < payload_len:
            break
        first = base[pos + 4]
        if first == 0xFF:
            break  # error packet
        if first == 0xFE and (deprecate_eof or payload_len < 9):
            break  # EOF packet / DEPRECATE_EOF terminating OK packet
        rows.append(_parse_binary_row(base + pos + 4, payload_len, colspecs))
        pos += 4 + payload_len
        seq_id = (seq_id + 1) & 0xFF
    return pos, seq_id


def skip_packets_from_buffer(bytearray buf, Py_ssize_t pos, Py_ssize_t buf_len,
                             unsigned int seq_id, Py_ssize_t count):
    """Skip up to ``count`` complete packets without materializing them.

    Stops early at an incomplete packet, a sequence mismatch, a jumbo packet
    or an error packet (which the caller reads via read_packet to raise).

    Returns ``(new_pos, new_seq_id, skipped)``.
    """
    cdef:
        const unsigned char *base = <const unsigned char *> PyByteArray_AS_STRING(buf)
        Py_ssize_t payload_len
        Py_ssize_t skipped = 0

    while skipped < count and buf_len - pos >= 4:
        payload_len = <Py_ssize_t> (base[pos] | (base[pos + 1] << 8) | (base[pos + 2] << 16))
        if base[pos + 3] != seq_id:
            break
        if payload_len == 0xFFFFFF:
            break
        if buf_len - pos - 4 < payload_len:
            break
        if payload_len and base[pos + 4] == 0xFF:
            break  # error packet: let read_packet() raise properly
        pos += 4 + payload_len
        seq_id = (seq_id + 1) & 0xFF
        skipped += 1
    return pos, seq_id, skipped


cpdef object _parse_cached_binary_result(
        bytearray buf, Py_ssize_t pos, Py_ssize_t buf_len, unsigned int seq_id,
        Py_ssize_t metadata_packets, tuple colspecs, bint deprecate_eof, tuple column_packets):
    """Read a complete cached-metadata result already in the receive buffer.

    Return None without consuming or decoding anything if another network read
    or the generic packet reader is needed. Scan framing before decoding rows
    so an incomplete large result does not get converted twice.
    """
    cdef:
        const unsigned char *base = <const unsigned char *> PyByteArray_AS_STRING(buf)
        Py_ssize_t length, row_start, eof_start, next_pos, column = 0, row_count = 0, index
        unsigned int first, status, warnings
        bytes expected
        tuple rows, row

    if pos < 0 or buf_len < pos or buf_len > PyByteArray_GET_SIZE(buf) or metadata_packets < 0:
        return None
    if column_packets is None or len(column_packets) != len(colspecs):
        return None
    while metadata_packets:
        if buf_len - pos < 4:
            return None
        length = base[pos] | (base[pos + 1] << 8) | (base[pos + 2] << 16)
        if base[pos + 3] != seq_id or length == 0xFFFFFF or buf_len - pos - 4 < length:
            return None
        if length and base[pos + 4] == 0xFF:
            return None
        if column < len(column_packets):
            expected = column_packets[column]
            if length != PyBytes_GET_SIZE(expected) or memcmp(
                base + pos + 4, PyBytes_AS_STRING(expected), length
            ):
                return None  # Reprepare/parameter types can change metadata.
            column += 1
        pos += length + 4
        seq_id = (seq_id + 1) & 0xFF
        metadata_packets -= 1

    row_start = pos
    while True:
        if buf_len - pos < 4:
            return None
        length = base[pos] | (base[pos + 1] << 8) | (base[pos + 2] << 16)
        if (
            base[pos + 3] != seq_id or length == 0 or length == 0xFFFFFF
            or buf_len - pos - 4 < length
        ):
            return None
        first = base[pos + 4]
        if first == 0xFE and (deprecate_eof or length < 9):
            if deprecate_eof:
                # Unusual length-encoded affected/insert counts use the generic
                # OK reader. SELECT terminators normally carry zero for both.
                if length < 7 or base[pos + 5] != 0 or base[pos + 6] != 0:
                    return None
                status = base[pos + 7] | (base[pos + 8] << 8)
                warnings = base[pos + 9] | (base[pos + 10] << 8)
            else:
                if length < 5:
                    return None
                warnings = base[pos + 5] | (base[pos + 6] << 8)
                status = base[pos + 7] | (base[pos + 8] << 8)
            eof_start = pos
            next_pos = pos + length + 4
            seq_id = (seq_id + 1) & 0xFF
            break
        if first != 0:
            return None  # errors and unexpected packets need the packet reader
        pos += length + 4
        row_count += 1
        seq_id = (seq_id + 1) & 0xFF

    rows = PyTuple_New(row_count)
    pos = row_start
    for index in range(row_count):
        length = base[pos] | (base[pos + 1] << 8) | (base[pos + 2] << 16)
        row = _parse_binary_row(base + pos + 4, length, colspecs)
        Py_INCREF(row)
        PyTuple_SET_ITEM(rows, index, row)
        pos += length + 4
    return (
        next_pos, seq_id, rows, status if deprecate_eof else 0,
        warnings, status & SERVER_MORE_RESULTS_EXISTS,
    )


cdef object _parse_cached_binary_response(
        bytearray buf, Py_ssize_t pos, Py_ssize_t buf_len, unsigned int seq_id,
        Py_ssize_t field_count, tuple colspecs, bint deprecate_eof, tuple column_packets):
    cdef const unsigned char *base = <const unsigned char *> PyByteArray_AS_STRING(buf)
    if pos < 0 or buf_len > PyByteArray_GET_SIZE(buf) or buf_len - pos < 5:
        return None
    # Larger length-encoded column counts and OK/error responses use the
    # generic reader. This path handles ordinary SELECT column-count packets.
    if (
        base[pos] != 1 or base[pos + 1] or base[pos + 2]
        or base[pos + 3] != seq_id or field_count < 1 or field_count >= 251
        or base[pos + 4] != field_count
    ):
        return None
    return _parse_cached_binary_result(
        buf, pos + 5, buf_len, (seq_id + 1) & 0xFF,
        field_count + (0 if deprecate_eof else 1), colspecs, deprecate_eof, column_packets,
    )


cdef inline void _write_lenenc(bytearray out, Py_ssize_t n):
    if n < 251:
        out.append(n)
    elif n < (1 << 16):
        out.append(0xFC)
        out.append(n & 0xFF)
        out.append((n >> 8) & 0xFF)
    elif n < (1 << 24):
        out.append(0xFD)
        out.append(n & 0xFF)
        out.append((n >> 8) & 0xFF)
        out.append((n >> 16) & 0xFF)
    else:
        out.append(0xFE)
        out.append(n & 0xFF)
        out.append((n >> 8) & 0xFF)
        out.append((n >> 16) & 0xFF)
        out.append((n >> 24) & 0xFF)
        out.append((n >> 32) & 0xFF)
        out.append((n >> 40) & 0xFF)
        out.append((n >> 48) & 0xFF)
        out.append((n >> 56) & 0xFF)


cdef inline void _write_u32(bytearray out, unsigned long v):
    out.append(v & 0xFF)
    out.append((v >> 8) & 0xFF)
    out.append((v >> 16) & 0xFF)
    out.append((v >> 24) & 0xFF)


cdef inline void _write_u64(bytearray out, unsigned long long v):
    out.append(v & 0xFF)
    out.append((v >> 8) & 0xFF)
    out.append((v >> 16) & 0xFF)
    out.append((v >> 24) & 0xFF)
    out.append((v >> 32) & 0xFF)
    out.append((v >> 40) & 0xFF)
    out.append((v >> 48) & 0xFF)
    out.append((v >> 56) & 0xFF)


cdef int _bulk_param_type(object v) except -1:
    """MYSQL_TYPE code used for a value in COM_STMT_BULK_EXECUTE."""
    if isinstance(v, bool):
        return 1  # TINY
    if isinstance(v, int):
        return 8  # LONGLONG
    if isinstance(v, float):
        return 5  # DOUBLE
    if isinstance(v, str):
        return 253  # VAR_STRING
    if isinstance(v, (bytes, bytearray)):
        return 252  # BLOB
    if datetime.PyDateTime_Check(v):
        return 12  # DATETIME
    if datetime.PyDate_Check(v):
        return 10  # DATE
    if datetime.PyDelta_Check(v) or datetime.PyTime_Check(v):
        return 11  # TIME
    if isinstance(v, Decimal):
        return 246  # NEWDECIMAL
    return 253  # stringified fallback


cdef _append_binary_value(bytearray values, object v, str encoding):
    """Append one non-NULL value in binary wire format (shared with execute)."""
    cdef:
        long long i64v
        unsigned long long u64v
        double dv
        long days, secs
        long usec
        unsigned char tmp[8]
        bytes encoded
    if isinstance(v, bool):
        values.append(1 if v else 0)
    elif isinstance(v, int):
        if -9223372036854775808 <= v <= 9223372036854775807:
            i64v = v
            _write_u64(values, <unsigned long long> i64v)
        elif 0 < v <= 18446744073709551615:
            u64v = v
            _write_u64(values, u64v)
        else:
            raise ValueError("int parameter out of 64-bit range for MySQL: %r" % (v,))
    elif isinstance(v, float):
        dv = v
        memcpy(tmp, &dv, 8)
        values += tmp[:8]
    elif isinstance(v, str):
        encoded = (<str> v).encode(encoding)
        _write_lenenc(values, PyBytes_GET_SIZE(encoded))
        values += encoded
    elif isinstance(v, (bytes, bytearray)):
        encoded = bytes(v)
        _write_lenenc(values, PyBytes_GET_SIZE(encoded))
        values += encoded
    elif datetime.PyDateTime_Check(v):
        values.append(11)
        values.append(datetime.PyDateTime_GET_YEAR(v) & 0xFF)
        values.append((datetime.PyDateTime_GET_YEAR(v) >> 8) & 0xFF)
        values.append(datetime.PyDateTime_GET_MONTH(v))
        values.append(datetime.PyDateTime_GET_DAY(v))
        values.append(datetime.PyDateTime_DATE_GET_HOUR(v))
        values.append(datetime.PyDateTime_DATE_GET_MINUTE(v))
        values.append(datetime.PyDateTime_DATE_GET_SECOND(v))
        _write_u32(values, datetime.PyDateTime_DATE_GET_MICROSECOND(v))
    elif datetime.PyDate_Check(v):
        values.append(4)
        values.append(datetime.PyDateTime_GET_YEAR(v) & 0xFF)
        values.append((datetime.PyDateTime_GET_YEAR(v) >> 8) & 0xFF)
        values.append(datetime.PyDateTime_GET_MONTH(v))
        values.append(datetime.PyDateTime_GET_DAY(v))
    elif datetime.PyDelta_Check(v):
        days = datetime.PyDateTime_DELTA_GET_DAYS(v)
        secs = datetime.PyDateTime_DELTA_GET_SECONDS(v)
        usec = datetime.PyDateTime_DELTA_GET_MICROSECONDS(v)
        values.append(12)
        if days < 0:
            if secs or usec:
                days = -days - 1
                secs = 86400 - secs
                if usec:
                    secs -= 1
                    usec = 1000000 - usec
            else:
                days = -days
            values.append(1)
        else:
            values.append(0)
        _write_u32(values, days)
        values.append(secs // 3600)
        values.append((secs % 3600) // 60)
        values.append(secs % 60)
        _write_u32(values, usec)
    elif datetime.PyTime_Check(v):
        values.append(12)
        values.append(0)
        _write_u32(values, 0)
        values.append(datetime.PyDateTime_TIME_GET_HOUR(v))
        values.append(datetime.PyDateTime_TIME_GET_MINUTE(v))
        values.append(datetime.PyDateTime_TIME_GET_SECOND(v))
        _write_u32(values, datetime.PyDateTime_TIME_GET_MICROSECOND(v))
    elif isinstance(v, Decimal):
        encoded = format(v, "f").encode("ascii")
        _write_lenenc(values, PyBytes_GET_SIZE(encoded))
        values += encoded
    else:
        encoded = str(v).encode(encoding)
        _write_lenenc(values, PyBytes_GET_SIZE(encoded))
        values += encoded


def pack_bulk_rows(rows, Py_ssize_t nparams, str encoding):
    """Serialize rows for MariaDB COM_STMT_BULK_EXECUTE.

    Returns ``(types_bytes, rows_bytes)`` or None when the rows are not
    bulk-compatible (a column is NULL in every row, or mixes value types).
    Each value is prefixed with an indicator byte (0 = value, 1 = NULL).
    """
    cdef:
        bytearray types = bytearray()
        bytearray values = bytearray()
        list col_types = [-1] * nparams
        Py_ssize_t i
        int t
        object row, v

    for row in rows:
        if len(row) != nparams:
            return None
        for i in range(nparams):
            v = row[i]
            if v is None:
                continue
            t = _bulk_param_type(v)
            if <int> col_types[i] == -1:
                col_types[i] = t
            elif <int> col_types[i] != t:
                return None  # heterogeneous column: fall back
    for i in range(nparams):
        t = <int> col_types[i]
        if t == -1:
            return None  # all-NULL column: server needs a concrete type
        types.append(t)
        types.append(0)

    for row in rows:
        for i in range(nparams):
            v = row[i]
            if v is None:
                values.append(1)  # STMT_INDICATOR_NULL
            else:
                values.append(0)  # STMT_INDICATOR_NONE
                _append_binary_value(values, v, encoding)
    return bytes(types), bytes(values)


cdef inline Py_ssize_t _scalar_param_size(object value):
    if value is None:
        return 4
    if type(value) is bool:
        return 5
    if type(value) is int or type(value) is float:
        return 12
    return 0


cdef inline void _pack_scalar_param(unsigned char* out, object value) except *:
    cdef long long signed_value
    cdef unsigned long long unsigned_value
    cdef double real_value
    cdef int index, overflow
    out[0] = 1 if value is None else 0
    out[1] = 1
    out[3] = 0
    if value is None:
        out[2] = 6
    elif type(value) is bool:
        out[2] = 1
        out[4] = 1 if value else 0
    elif type(value) is int:
        out[2] = 8
        signed_value = PyLong_AsLongLongAndOverflow(value, &overflow)
        if not overflow:
            unsigned_value = <unsigned long long>signed_value
        elif overflow > 0:
            try:
                unsigned_value = PyLong_AsUnsignedLongLong(value)
            except OverflowError:
                raise ValueError("int parameter out of 64-bit range for MySQL: %r" % (value,)) from None
            out[3] = 0x80
        else:
            raise ValueError("int parameter out of 64-bit range for MySQL: %r" % (value,))
        for index in range(8):
            out[4 + index] = <unsigned char>(unsigned_value >> (8 * index))
    else:
        out[2] = 5
        real_value = value
        memcpy(out + 4, &real_value, 8)


cdef bytes _pack_execute_packet(bytes header, tuple args, str encoding, bint* is_packet):
    cdef Py_ssize_t size = _scalar_param_size(args[0]) if len(args) == 1 else 0
    cdef bytes packet, params
    cdef unsigned char* out
    is_packet[0] = True
    if size:
        packet = PyBytes_FromStringAndSize(NULL, 14 + size)
        out = <unsigned char*>PyBytes_AS_STRING(packet)
        _pack_scalar_param(out + 14, args[0])
    else:
        params = pack_binary_params(args, encoding) if args else b""
        size = len(params)
        if size + 9 >= 8191:
            # Let the command writer split large payloads without encoding
            # their parameters a second time.
            is_packet[0] = False
            return header + params
        packet = PyBytes_FromStringAndSize(NULL, 14 + size)
        out = <unsigned char*>PyBytes_AS_STRING(packet)
        memcpy(out + 14, PyBytes_AS_STRING(params), size)
    out[0] = <unsigned char>(size + 10)
    out[1] = <unsigned char>((size + 10) >> 8)
    out[2] = 0
    out[3] = 0
    out[4] = 0x17
    memcpy(out + 5, PyBytes_AS_STRING(header), 9)
    return packet


cpdef bytes pack_binary_params(tuple args, str encoding):
    """Serialize COM_STMT_EXECUTE parameters.

    Returns null_bitmap + new_params_bound_flag(1) + types + values.
    """
    cdef:
        Py_ssize_t n = len(args)
        bytearray bitmap, types, values
        Py_ssize_t i
        object v
        long long i64v
        unsigned long long u64v
        double dv
        long days, secs
        long usec
        unsigned char tmp[8]
        bytes encoded
        bytes packed
        unsigned char *out

    if n == 1:
        i = _scalar_param_size(args[0])
        if i:
            packed = PyBytes_FromStringAndSize(NULL, i)
            _pack_scalar_param(<unsigned char*>PyBytes_AS_STRING(packed), args[0])
            return packed

    bitmap = bytearray((n + 7) >> 3)
    types = bytearray()
    values = bytearray()

    for i in range(n):
        v = args[i]
        if v is None:
            bitmap[i >> 3] |= 1 << (i & 7)
            types.append(6)  # MYSQL_TYPE_NULL
            types.append(0)
        elif isinstance(v, bool):
            types.append(1)  # MYSQL_TYPE_TINY
            types.append(0)
            values.append(1 if v else 0)
        elif isinstance(v, int):
            if -9223372036854775808 <= v <= 9223372036854775807:
                types.append(8)  # MYSQL_TYPE_LONGLONG
                types.append(0)
                i64v = v
                _write_u64(values, <unsigned long long> i64v)
            elif 0 < v <= 18446744073709551615:
                types.append(8)
                types.append(0x80)  # unsigned flag
                u64v = v
                _write_u64(values, u64v)
            else:
                raise ValueError("int parameter out of 64-bit range for MySQL: %r" % (v,))
        elif isinstance(v, float):
            types.append(5)  # MYSQL_TYPE_DOUBLE
            types.append(0)
            dv = v
            memcpy(tmp, &dv, 8)
            values += tmp[:8]
        elif isinstance(v, str):
            types.append(253)  # MYSQL_TYPE_VAR_STRING
            types.append(0)
            encoded = (<str> v).encode(encoding)
            _write_lenenc(values, PyBytes_GET_SIZE(encoded))
            values += encoded
        elif isinstance(v, (bytes, bytearray)):
            types.append(252)  # MYSQL_TYPE_BLOB
            types.append(0)
            encoded = bytes(v)
            _write_lenenc(values, PyBytes_GET_SIZE(encoded))
            values += encoded
        elif datetime.PyDateTime_Check(v):
            types.append(12)  # MYSQL_TYPE_DATETIME
            types.append(0)
            values.append(11)
            values.append(datetime.PyDateTime_GET_YEAR(v) & 0xFF)
            values.append((datetime.PyDateTime_GET_YEAR(v) >> 8) & 0xFF)
            values.append(datetime.PyDateTime_GET_MONTH(v))
            values.append(datetime.PyDateTime_GET_DAY(v))
            values.append(datetime.PyDateTime_DATE_GET_HOUR(v))
            values.append(datetime.PyDateTime_DATE_GET_MINUTE(v))
            values.append(datetime.PyDateTime_DATE_GET_SECOND(v))
            _write_u32(values, datetime.PyDateTime_DATE_GET_MICROSECOND(v))
        elif datetime.PyDate_Check(v):
            types.append(10)  # MYSQL_TYPE_DATE
            types.append(0)
            values.append(4)
            values.append(datetime.PyDateTime_GET_YEAR(v) & 0xFF)
            values.append((datetime.PyDateTime_GET_YEAR(v) >> 8) & 0xFF)
            values.append(datetime.PyDateTime_GET_MONTH(v))
            values.append(datetime.PyDateTime_GET_DAY(v))
        elif datetime.PyDelta_Check(v):
            types.append(11)  # MYSQL_TYPE_TIME
            types.append(0)
            days = datetime.PyDateTime_DELTA_GET_DAYS(v)
            secs = datetime.PyDateTime_DELTA_GET_SECONDS(v)
            usec = datetime.PyDateTime_DELTA_GET_MICROSECONDS(v)
            values.append(12)
            if days < 0:
                # normalize to positive components with a sign byte
                if secs or usec:
                    days = -days - 1
                    secs = 86400 - secs
                    if usec:
                        secs -= 1
                        usec = 1000000 - usec
                else:
                    days = -days
                values.append(1)
            else:
                values.append(0)
            _write_u32(values, days)
            values.append(secs // 3600)
            values.append((secs % 3600) // 60)
            values.append(secs % 60)
            _write_u32(values, usec)
        elif datetime.PyTime_Check(v):
            types.append(11)  # MYSQL_TYPE_TIME
            types.append(0)
            values.append(12)
            values.append(0)
            _write_u32(values, 0)
            values.append(datetime.PyDateTime_TIME_GET_HOUR(v))
            values.append(datetime.PyDateTime_TIME_GET_MINUTE(v))
            values.append(datetime.PyDateTime_TIME_GET_SECOND(v))
            _write_u32(values, datetime.PyDateTime_TIME_GET_MICROSECOND(v))
        elif isinstance(v, Decimal):
            types.append(246)  # MYSQL_TYPE_NEWDECIMAL
            types.append(0)
            encoded = format(v, "f").encode("ascii")
            _write_lenenc(values, PyBytes_GET_SIZE(encoded))
            values += encoded
        else:
            # Fallback: stringify, mirroring the text protocol's default encoder
            types.append(253)
            types.append(0)
            encoded = str(v).encode(encoding)
            _write_lenenc(values, PyBytes_GET_SIZE(encoded))
            values += encoded

    return bytes(bitmap) + b"\x01" + bytes(types) + bytes(values)


cdef class MysqlPacket:
    """
    Representation of a MySQL response packet.
    Provides an interface for reading/parsing the packet results.
    """
    cdef:
        bytes _data
        const unsigned char *_ptr
        Py_ssize_t _size
        Py_ssize_t _position

    def __init__(self, bytes data, str encoding):
        self._position = 0
        self._data = data
        self._ptr = <const unsigned char *> PyBytes_AS_STRING(data)
        self._size = PyBytes_GET_SIZE(data)

    cpdef bytes get_all_data(self):
        return self._data

    cdef inline bytes _read_fast(self, Py_ssize_t size):
        """Fast internal read without bounds checking."""
        cdef Py_ssize_t pos = self._position
        self._position = pos + size
        return PyBytes_FromStringAndSize(<const char *> (self._ptr + pos), size)

    cpdef bytes read(self, Py_ssize_t size):
        """
        Read the first 'size' bytes in packet and advance cursor past them.
        """
        cdef Py_ssize_t pos = self._position
        if pos + size > self._size:
            error = (
                    "Result length not requested length:\n"
                    "Expected=%s.  Actual=%s.  Position: %s.  Data Length: %s"
                    % (size, self._size - pos, pos, self._size)
            )
            raise AssertionError(error)
        self._position = pos + size
        return PyBytes_FromStringAndSize(<const char *> (self._ptr + pos), size)

    cpdef bytes read_all(self):
        """Read all remaining data in the packet.

        (Subsequent read() will return errors.)
        """
        cdef bytes result = PyBytes_FromStringAndSize(
            <const char *> (self._ptr + self._position), self._size - self._position
        )
        self._position = 0
        return result

    cpdef advance(self, Py_ssize_t length):
        """
        Advance the cursor in data buffer 'length' bytes.
        """
        cdef Py_ssize_t new_position = self._position + length
        if new_position < 0 or new_position > self._size:
            raise Exception(
                "Invalid advance amount (%s) for cursor.  "
                "Position=%s" % (length, new_position)
            )
        self._position = new_position

    cpdef rewind(self, Py_ssize_t position=0):
        """
        Set the position of the data buffer cursor to 'position'.
        """
        if position < 0 or position > self._size:
            raise Exception("Invalid position to rewind cursor to: %s." % position)
        self._position = position

    cpdef bytes get_bytes(self, Py_ssize_t position, Py_ssize_t length=1):
        """
        Get 'length' bytes starting at 'position'.

        Position is start of payload (first four packet header bytes are not
        included) starting at index '0'.

        No error checking is done.  If requesting outside end of buffer
        an empty string (or string shorter than 'length') may be returned!
        """
        return self._data[position: (position + length)]

    cpdef unsigned int read_uint8(self):
        cdef unsigned int result = self._ptr[self._position]
        self._position += 1
        return result

    cpdef unsigned int read_uint16(self):
        cdef const unsigned char *p = self._ptr + self._position
        self._position += 2
        return p[0] | (p[1] << 8)

    cpdef unsigned int read_uint24(self):
        cdef const unsigned char *p = self._ptr + self._position
        self._position += 3
        return p[0] | (p[1] << 8) | (p[2] << 16)

    cpdef unsigned int read_uint32(self):
        cdef const unsigned char *p = self._ptr + self._position
        self._position += 4
        return p[0] | (p[1] << 8) | (p[2] << 16) | (<unsigned int> p[3] << 24)

    cpdef unsigned long long read_uint64(self):
        cdef const unsigned char *p = self._ptr + self._position
        self._position += 8
        return (
            <unsigned long long> p[0]
            | (<unsigned long long> p[1] << 8)
            | (<unsigned long long> p[2] << 16)
            | (<unsigned long long> p[3] << 24)
            | (<unsigned long long> p[4] << 32)
            | (<unsigned long long> p[5] << 40)
            | (<unsigned long long> p[6] << 48)
            | (<unsigned long long> p[7] << 56)
        )

    cpdef bytes read_string(self):
        cdef:
            const char *start = <const char *> (self._ptr + self._position)
            Py_ssize_t remaining = self._size - self._position
            const char *end
        end = <const char *> memchr(start, 0, remaining)
        if end == NULL:
            return None
        cdef bytes result = PyBytes_FromStringAndSize(start, end - start)
        self._position += (end - start) + 1
        return result

    cpdef read_length_encoded_integer(self):
        """
        Read a 'Length Coded Binary' number from the data buffer.

        Length coded numbers can be anywhere from 1 to 9 bytes depending
        on the value of the first byte.
        """
        cdef unsigned int c = self._ptr[self._position]
        self._position += 1
        if c == 251:  # NULL_COLUMN
            return None
        if c < 251:
            return c
        elif c == 252:
            return self.read_uint16()
        elif c == 253:
            return self.read_uint24()
        elif c == 254:
            return self.read_uint64()

    cpdef read_length_coded_string(self):
        """
        Read a 'Length Coded String' from the data buffer.

        A 'Length Coded String' consists first of a length coded
        (unsigned, positive) integer represented in 1-9 bytes followed by
        that many bytes of binary data.  (For example "cat" would be "3cat".)
        """
        cdef:
            unsigned int c
            Py_ssize_t length

        c = self._ptr[self._position]
        self._position += 1

        if c == 251:  # NULL
            return None
        if c < 251:
            length = <Py_ssize_t> c
        elif c == 252:
            length = <Py_ssize_t> self.read_uint16()
        elif c == 253:
            length = <Py_ssize_t> self.read_uint24()
        elif c == 254:
            length = <Py_ssize_t> self.read_uint64()
        else:
            return None
        return self._read_fast(length)

    cpdef tuple read_row(self, tuple converters):
        """Parse the whole packet as one text-protocol result row."""
        cdef tuple row = _parse_row(
            self._ptr + self._position, self._size - self._position, converters
        )
        self._position = self._size
        return row

    cpdef tuple read_binary_row(self, tuple colspecs):
        """Parse the whole packet as one binary-protocol result row."""
        cdef tuple row = _parse_binary_row(self._ptr, self._size, colspecs)
        self._position = self._size
        return row

    cpdef tuple read_struct(self, str fmt):
        s = getattr(structs, fmt[1:])
        result = s.unpack_from(self._data, self._position)
        self._position += s.size
        return tuple(result)

    cpdef int is_ok_packet(self):
        # https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
        return self._size >= 7 and self._ptr[0] == 0

    cpdef int is_eof_packet(self):
        # http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-EOF_Packet
        # Caution: \xFE may be LengthEncodedInteger.
        # If \xFE is LengthEncodedInteger header, 8bytes followed.
        return self._size < 9 and self._size > 0 and self._ptr[0] == 0xFE

    cpdef int is_auth_switch_request(self):
        # http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
        return self._size > 0 and self._ptr[0] == 0xFE

    cpdef int is_extra_auth_data(self):
        # https://dev.mysql.com/doc/internals/en/successful-authentication.html
        return self._size > 0 and self._ptr[0] == 1

    cpdef int is_resultset_packet(self):
        return self._size > 0 and 1 <= self._ptr[0] <= 250

    cpdef int is_load_local_packet(self):
        return self._size > 0 and self._ptr[0] == 0xFB

    cpdef int is_error_packet(self):
        return self._size > 0 and self._ptr[0] == 0xFF

    def check_error(self):
        if self.is_error_packet():
            self.raise_for_error()

    cpdef raise_for_error(self):
        errors.raise_mysql_exception(self._data)

cdef class FieldDescriptorPacket(MysqlPacket):
    """
    A MysqlPacket that represents a specific column's metadata in the result.

    Parsing is automatically done and the results are exported via public
    attributes on the class such as: db, table_name, name, length, type_code.
    """
    cdef:
        bytes catalog, db
        public str table_name, org_table, name, org_name
        public long long charsetnr, length, type_code, flags, scale

    def __init__(self, bytes data, str encoding):
        super(FieldDescriptorPacket, self).__init__(data, encoding)
        self._parse_field_descriptor(encoding)

    cdef _parse_field_descriptor(self, str encoding):
        """
        Parse the 'Field Descriptor' (Metadata) packet.

        This is compatible with MySQL 4.1+ (not compatible with MySQL 4.0).
        """
        self.catalog = self.read_length_coded_string()
        self.db = self.read_length_coded_string()
        self.table_name = self.read_length_coded_string().decode(encoding)
        self.org_table = self.read_length_coded_string().decode(encoding)
        self.name = self.read_length_coded_string().decode(encoding)
        self.org_name = self.read_length_coded_string().decode(encoding)
        # layout: filler(1) charsetnr(2) length(4) type_code(1) flags(2) scale(1) filler(2)
        self._position += 1
        self.charsetnr = self.read_uint16()
        self.length = self.read_uint32()
        self.type_code = self.read_uint8()
        self.flags = self.read_uint16()
        self.scale = self.read_uint8()
        self._position += 2

    cpdef description(self):
        """Provides a 7-item tuple compatible with the Python PEP249 DB Spec."""
        cdef int column_length = self.get_column_length()
        return (
            self.name,
            self.type_code,
            None,  # TODO: display_length; should this be self.length?
            column_length,  # 'internal_size'
            column_length,  # 'precision'  # TODO: why!?!?
            self.scale,
            self.flags % 2 == 0,
        )

    cdef int get_column_length(self):
        if self.type_code == VAR_STRING:
            mb_len = MB_LENGTH.get(self.charsetnr, 1)
            return self.length // mb_len
        return self.length

    def __str__(self):
        return "%s %r.%r.%r, type=%s, flags=%x" % (
            self.__class__,
            self.db,
            self.table_name,
            self.name,
            self.type_code,
            self.flags,
        )

cdef class OKPacketWrapper:
    """
    OK Packet Wrapper. It uses an existing packet object, and wraps
    around it, exposing useful variables while still providing access
    to the original packet objects variables and methods.
    """
    cdef:
        MysqlPacket packet
        public int server_status, warning_count, has_next
        public bytes message
        public unsigned long long affected_rows, insert_id

    def __init__(self, MysqlPacket from_packet):
        if not from_packet.is_ok_packet():
            raise ValueError(
                "Cannot create "
                + str(self.__class__.__name__)
                + " object from invalid packet type"
            )

        self.packet = from_packet
        self.packet.advance(1)

        self.affected_rows = self.packet.read_length_encoded_integer()
        self.insert_id = self.packet.read_length_encoded_integer()
        self.server_status = self.packet.read_uint16()
        self.warning_count = self.packet.read_uint16()
        self.message = self.packet.read_all()
        self.has_next = self.server_status & SERVER_MORE_RESULTS_EXISTS

    def __getattr__(self, key):
        return getattr(self.packet, key)

cdef class EOFPacketWrapper:
    """
    EOF Packet Wrapper. It uses an existing packet object, and wraps
    around it, exposing useful variables while still providing access
    to the original packet objects variables and methods.
    """
    cdef:
        MysqlPacket packet
        public int server_status, warning_count, has_next

    def __init__(self, MysqlPacket from_packet):
        if not from_packet.is_eof_packet():
            raise ValueError(
                f"Cannot create '{self.__class__}' object from invalid packet type"
            )

        self.packet = from_packet
        self.packet.advance(1)
        self.warning_count = self.packet.read_uint16()
        self.server_status = self.packet.read_uint16()
        self.has_next = self.server_status & SERVER_MORE_RESULTS_EXISTS

    def __getattr__(self, key):
        return getattr(self.packet, key)

cdef class LoadLocalPacketWrapper:
    """
    Load Local Packet Wrapper. It uses an existing packet object, and wraps
    around it, exposing useful variables while still providing access
    to the original packet objects variables and methods.
    """
    cdef:
        MysqlPacket packet
        public bytes filename

    def __init__(self, MysqlPacket from_packet):
        if not from_packet.is_load_local_packet():
            raise ValueError(
                f"Cannot create '{self.__class__}' object from invalid packet type"
            )

        self.packet = from_packet
        self.filename = self.packet.get_all_data()[1:]

    def __getattr__(self, key):
        return getattr(self.packet, key)


cdef extern from *:
    """
    #if !defined(_WIN32)
    #include <sys/socket.h>
    static Py_ssize_t asyncmy_send(int fd, const char *data, Py_ssize_t size) {
    #ifdef MSG_NOSIGNAL
        return send(fd, data, size, MSG_NOSIGNAL);
    #else
        return send(fd, data, size, 0);
    #endif
    }
    #else
    static Py_ssize_t asyncmy_send(int fd, const char *data, Py_ssize_t size) {return 0;}
    #endif
    """
    Py_ssize_t asyncmy_send(int fd, const char* data, Py_ssize_t size)

cpdef object _send_scalar_execute(bytes header, tuple args, int fd):
    """Send a small scalar packet from the stack on a nonblocking plain socket.

    None means sent; False means unsupported; bytes are the unsent suffix for
    the transport's ordinary buffering/error handling. Called only while that
    transport has no queued writes, preserving byte order after partial sends.
    """
    cdef unsigned char data[26]
    cdef Py_ssize_t size, sent
    if header is None or len(header) != 9:
        raise ValueError("Expected a 9-byte prepared execution header")
    if args is None:
        raise TypeError("Expected a parameter tuple")
    if len(args) != 1:
        return False
    size = _scalar_param_size(args[0])
    if not size:
        return False
    _pack_scalar_param(data + 14, args[0])
    data[0] = <unsigned char>(size + 10)
    data[1] = data[2] = data[3] = 0
    data[4] = 0x17
    memcpy(data + 5, PyBytes_AS_STRING(header), 9)
    sent = asyncmy_send(fd, <const char*>data, size + 14)
    if sent == size + 14:
        return None
    if sent < 0:
        sent = 0
    return PyBytes_FromStringAndSize(<const char*>data + sent, size + 14 - sent)
