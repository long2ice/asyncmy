cpdef object _parse_cached_binary_result(
    bytearray buf, Py_ssize_t pos, Py_ssize_t buf_len, unsigned int seq_id,
    Py_ssize_t metadata_packets, tuple colspecs, bint deprecate_eof, tuple column_packets
)

cdef object _parse_cached_binary_response(
    bytearray buf, Py_ssize_t pos, Py_ssize_t buf_len, unsigned int seq_id,
    Py_ssize_t field_count, tuple colspecs, bint deprecate_eof, tuple column_packets
)

cdef object _parse_text_response_from_buffer(
    bytearray buf, Py_ssize_t pos, Py_ssize_t buf_len, unsigned int seq_id,
    bint deprecate_eof, tuple last_meta, dict meta_cache, bint as_dicts=*
)

cpdef object _parse_rows_from_buffer_as_dicts(
    bytearray buf, Py_ssize_t pos, Py_ssize_t buf_len,
    tuple converters, list fields, unsigned int seq_id, list rows, bint deprecate_eof=*
)

cdef dict _parse_row_as_dict(
    const unsigned char *p, Py_ssize_t size, tuple converters, list fields
)

cpdef bytes pack_binary_params(tuple args, str encoding)

cdef bytes _pack_execute_packet(bytes header, tuple args, str encoding, bint* is_packet)

cpdef object _send_scalar_execute(bytes header, tuple args, int fd)

cpdef object _send_query_packet(object sql, str encoding, int fd)

cpdef object _format_executemany_batches(
    bytes prefix, str values_tpl, bytes postfix, object args,
    Py_ssize_t max_stmt_length, str encoding, bint no_backslash_escapes
)
