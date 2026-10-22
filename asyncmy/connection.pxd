cdef class _ProtocolState:
    # Keep the receive state directly accessible from Cython packet readers.
    # The Python protocol subclass still implements BufferedProtocol and can
    # carry transport-specific state and overridden callbacks.
    cdef public bytearray buffer
    cdef public Py_ssize_t length, pos
    cdef public bint eof
    cdef public object exc, _read_waiter


cdef class _ConnectionState:
    cdef:
        dict __dict__
        object __weakref__
        public object _loop, _transport, _read_timeout, _affected_rows
        public object _query_callback, _cursor_cls
        public double _last_usage
        public _ProtocolState _proto
        public MySQLResult _result
        public str _encoding
        public dict _stmt_cache
        public Py_ssize_t _stmt_cache_size
        public unsigned int _next_seq_id
        public int server_status, _echo
        public bint _connected, _deprecate_eof


cdef class MySQLResult:
    cdef:
        public _ConnectionState connection
        public bytes message
        public int warning_count, field_count, server_status, unbuffered_active, has_next
        public object affected_rows
        public list fields, converters
        public unsigned long long insert_id
        public tuple rows, description
        tuple _row_converters
        tuple _binary_colspecs
        list _pending_rows
        Py_ssize_t _pending_idx
        bint _deprecate_eof
    cpdef list _take_pending_rows(self, limit=*)
    cdef _read_row_from_packet(self, packet)
