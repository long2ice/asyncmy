cpdef object _parse_cached_binary_result(
    bytearray buf, Py_ssize_t pos, Py_ssize_t buf_len, unsigned int seq_id,
    Py_ssize_t metadata_packets, tuple colspecs, bint deprecate_eof, tuple column_packets
)

cdef object _parse_cached_binary_response(
    bytearray buf, Py_ssize_t pos, Py_ssize_t buf_len, unsigned int seq_id,
    Py_ssize_t field_count, tuple colspecs, bint deprecate_eof, tuple column_packets
)

cpdef bytes pack_binary_params(tuple args, str encoding)

cdef bytes _pack_execute_packet(bytes header, tuple args, str encoding, bint* is_packet)

cpdef object _send_scalar_execute(bytes header, tuple args, int fd)
