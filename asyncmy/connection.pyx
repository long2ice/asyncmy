# cython: freethreading_compatible=True
# Declares the module safe to import without re-enabling the GIL on
# free-threaded CPython. Module-level state (encoders/decoders, error_map,
# charset tables, escape table) is built during import and read-only after;
# per-connection state lives on the instances. It does NOT make a single
# Connection/Cursor safe to share between threads.
# Python implementation of the MySQL client-server protocol
# http://dev.mysql.com/doc/internals/en/client-server-protocol.html
# Error codes:
# https://dev.mysql.com/doc/refman/5.5/en/error-handling.html
import asyncio
import errno
import inspect
import os
import socket
import sys
import warnings
from typing import Optional, Type

from cpython.bytearray cimport PyByteArray_AS_STRING
from cpython.bytes cimport PyBytes_FromStringAndSize

from asyncmy import auth, converters, errors
from asyncmy.charset import charset_by_id, charset_by_name
from asyncmy.cursors import Cursor
from asyncmy.optionfile import Parser
from asyncmy.protocol import (EOFPacketWrapper, FieldDescriptorPacket,
                              LoadLocalPacketWrapper, MysqlPacket,
                              OKPacketWrapper, pack_binary_params,
                              pack_bulk_rows, parse_binary_rows_from_buffer,
                              parse_rows_from_buffer, skip_packets_from_buffer)

from .constants.CLIENT import (CAPABILITIES, CONNECT_ATTRS, CONNECT_WITH_DB,
                               DEPRECATE_EOF, LOCAL_FILES, MULTI_RESULTS,
                               MULTI_STATEMENTS, PLUGIN_AUTH,
                               PLUGIN_AUTH_LENENC_CLIENT_DATA,
                               SECURE_CONNECTION, SSL)
from .constants.COMMAND import (COM_INIT_DB, COM_PING, COM_PROCESS_KILL,
                                COM_QUERY, COM_QUIT, COM_STMT_CLOSE,
                                COM_STMT_EXECUTE, COM_STMT_PREPARE)
from .constants.CR import (CR_COMMANDS_OUT_OF_SYNC, CR_CONN_HOST_ERROR,
                           CR_SERVER_GONE_ERROR, CR_SERVER_LOST)
from .constants.ER import FILE_NOT_FOUND
from .constants.FIELD_TYPE import (BIT, BLOB, GEOMETRY, JSON, LONG_BLOB,
                                   MEDIUM_BLOB, STRING, TINY_BLOB, VAR_STRING,
                                   VARCHAR)
from .constants.SERVER_STATUS import (SERVER_MORE_RESULTS_EXISTS,
                                      SERVER_STATUS_AUTOCOMMIT,
                                      SERVER_STATUS_IN_TRANS,
                                      SERVER_STATUS_NO_BACKSLASH_ESCAPES)
from .contexts import _ConnectionContextManager
from .structs import B_, BHHB, IIB, B, H, I, Q, i, iB, iIB23s
from .version import __VERSION__

try:
    import ssl

    SSL_ENABLED = True
except ImportError:
    ssl = None
    SSL_ENABLED = False

try:
    import getpass

    DEFAULT_USER = getpass.getuser()
    del getpass
except (ImportError, KeyError, OSError):
    # When there's no entry in OS database for a current user:
    # KeyError is raised in Python 3.12 and below.
    # OSError is raised in Python 3.13+
    DEFAULT_USER = None

cdef set TEXT_TYPES = {
    BIT,
    BLOB,
    LONG_BLOB,
    MEDIUM_BLOB,
    STRING,
    TINY_BLOB,
    VAR_STRING,
    VARCHAR,
    GEOMETRY,
}

cdef str DEFAULT_CHARSET = "utf8mb4"

cdef int MAX_PACKET_LEN = 2 ** 24 - 1

# Initial size of the protocol receive buffer.
cdef int READ_CHUNK_SIZE = 2 ** 18

# MariaDB-specific protocol extensions
cdef int COM_STMT_BULK_EXECUTE = 0xFA
cdef unsigned int MARIADB_CLIENT_STMT_BULK_OPERATIONS = 1 << 2  # extended-caps dword
cdef unsigned int CLIENT_MYSQL = 1  # bit 0: cleared to signal MariaDB awareness

# Decoders that accept raw bytes input, allowing us to skip the ascii decode
# step entirely for their columns.
cdef set _BYTES_SAFE_DECODERS = {
    int,
    float,
    converters.convert_datetime,
    converters.convert_timedelta,
    converters.convert_time,
    converters.convert_date,
}

cdef inline bytes _take_bytes(bytearray buf, Py_ssize_t pos, Py_ssize_t n):
    """Copy buf[pos:pos+n] into a fresh bytes object with a single allocation."""
    return PyBytes_FromStringAndSize(PyByteArray_AS_STRING(buf) + pos, n)

cdef _pyformat_to_qmark(str query):
    """Convert pyformat ``%s`` placeholders to native ``?`` markers.

    Returns ``(converted_sql, param_count)`` or None when the query uses
    constructs the binary path does not support (named ``%(name)s``
    placeholders or a stray ``%``).
    """
    cdef:
        list out = []
        Py_ssize_t i = 0, j
        int nparams = 0
        str nxt
    while True:
        j = query.find("%", i)
        if j == -1:
            out.append(query[i:])
            break
        out.append(query[i:j])
        nxt = query[j + 1: j + 2]
        if nxt == "s":
            out.append("?")
            nparams += 1
            i = j + 2
        elif nxt == "%":
            out.append("%")
            i = j + 2
        else:
            return None
    return "".join(out), nparams

# https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger
cdef _lenenc_int(int i):
    if i < 0:
        raise ValueError(
            "Encoding %d is less than 0 - no representation in LengthEncodedInteger" % i
        )
    elif i < 0xFB:
        return bytes([i])
    elif i < (1 << 16):
        return b"\xfc" + H.pack(i)
    elif i < (1 << 24):
        return b"\xfd" + I.pack(i)[:3]
    elif i < (1 << 64):
        return b"\xfe" + Q.pack(i)
    else:
        raise ValueError(
            "Encoding %x is larger than %x - no representation in LengthEncodedInteger"
            % (i, (1 << 64))
        )


class _MySQLProtocol(asyncio.BufferedProtocol):
    """Receive-side transport protocol.

    Incoming bytes land directly in ``buffer`` (the parse buffer) via the
    zero-copy BufferedProtocol interface — no StreamReader, no intermediate
    chunk objects, no second copy. ``buffer[pos:length]`` is the unconsumed
    region; consumers advance ``pos`` and re-read all three attributes after
    every await (the buffer may be compacted or reallocated while waiting).
    """

    def __init__(self, loop):
        self._loop = loop
        self.transport = None
        self.buffer = bytearray(READ_CHUNK_SIZE)
        self.length = 0  # valid bytes in buffer
        self.pos = 0  # consumed bytes
        self.eof = False
        self.exc = None
        self._read_waiter = None
        self._drain_waiter = None
        self._write_paused = False
        self._closed_waiter = loop.create_future()

    # -- BufferedProtocol interface --

    def connection_made(self, transport):
        self.transport = transport

    def get_buffer(self, sizehint):
        buffer = self.buffer
        length = self.length
        pos = self.pos
        if pos:
            # compact the consumed prefix
            if length > pos:
                buffer[0: length - pos] = buffer[pos:length]
            length -= pos
            self.length = length
            self.pos = 0
        if length == 0 and len(buffer) > (READ_CHUNK_SIZE << 2):
            # shrink an oversized buffer left over from a huge result set
            self.buffer = buffer = bytearray(READ_CHUNK_SIZE)
        elif len(buffer) - length < 4096:
            # grow geometrically so huge packets stay O(n)
            grow = len(buffer)
            if sizehint > 0 and sizehint > grow:
                grow = sizehint
            buffer.extend(bytes(grow))
        return memoryview(buffer)[length:]

    def buffer_updated(self, nbytes):
        self.length += nbytes
        waiter = self._read_waiter
        if waiter is not None:
            self._read_waiter = None
            if not waiter.done():
                waiter.set_result(None)

    def eof_received(self):
        self.eof = True
        waiter = self._read_waiter
        if waiter is not None:
            self._read_waiter = None
            if not waiter.done():
                waiter.set_result(None)
        return False

    def connection_lost(self, exc):
        self.eof = True
        if exc is not None:
            self.exc = exc
        self.transport = None
        for waiter in (self._read_waiter, self._drain_waiter):
            if waiter is not None and not waiter.done():
                waiter.set_result(None)
        self._read_waiter = None
        self._drain_waiter = None
        if not self._closed_waiter.done():
            self._closed_waiter.set_result(None)

    def pause_writing(self):
        self._write_paused = True

    def resume_writing(self):
        self._write_paused = False
        waiter = self._drain_waiter
        if waiter is not None:
            self._drain_waiter = None
            if not waiter.done():
                waiter.set_result(None)

    # -- consumer helpers --

    async def wait_for_data(self, need):
        """Suspend until more bytes arrive (or EOF / connection loss)."""
        # wait_for() may schedule this coroutine after the transport callback.
        # Recheck the read predicate before registering a waiter so that data,
        # EOF, or errors delivered in that gap cannot lose their notification.
        if self.length - self.pos >= need or self.eof or self.exc is not None:
            return
        waiter = self._loop.create_future()
        self._read_waiter = waiter
        try:
            await waiter
        finally:
            if self._read_waiter is waiter:
                self._read_waiter = None

    async def drain(self):
        if self._write_paused and self.transport is not None:
            waiter = self._loop.create_future()
            self._drain_waiter = waiter
            await waiter

    async def wait_closed(self):
        await self._closed_waiter


class Connection:
    """
    Representation of a socket with a mysql server.

    The proper way to get an instance of this class is to call
    connect().

    Establish a connection to the MySQL database. Accepts several
    arguments:

    :param host: Host where the database server is located.
    :param user: Username to log in as.
    :param password: Password to use.
    :param password_creator:
        Callable returning the password to authenticate with, consulted before
        every connection attempt — including the ones a pool makes on its own
        when it recycles or reconnects, which is the point: short-lived
        credentials such as AWS RDS IAM tokens expire while pooled connections
        outlive them, and nothing else gives you a hook at that moment.
        May be a plain function or return an awaitable. Must return ``str`` or
        ``bytes``. Takes precedence over ``password`` when both are given.
    :param database: Database to use, None to not use a particular one.
    :param port: MySQL port to use, default is usually OK. (default: 3306)
    :param unix_socket: Use a unix socket rather than TCP/IP.
    :param sock:
        An already-connected socket to speak MySQL over, instead of connecting
        to ``host``/``port`` ourselves. Combine it with ``ssl`` to hand the
        driver a connection whose TLS is set up by the caller rather than
        negotiated in-protocol — what connectors that front the server with a
        TLS proxy (e.g. Cloud SQL) need. The socket is used by a single
        connection and is closed with it, so a pool needs a fresh socket per
        connection; reconnecting one raises. Keep-alive and TCP_NODELAY are
        left to the caller.
    :param read_timeout: The timeout for reading from the connection in seconds (default: None - no timeout)
    :param charset: Charset to use.
    :param sql_mode: Default SQL_MODE to use.
    :param read_default_file:
        Specifies  my.cnf file to read these parameters from under the [client] section.
    :param conv:
        Conversion dictionary to use instead of the default one.
        This is used to provide custom marshalling and unmarshalling of types.
        See converters.
    :param use_unicode:
        Whether or not to default to unicode strings.
        This option defaults to true.
    :param client_flag: Custom flags to send to MySQL. Find potential values in constants.
    :param cursor_cls: Custom cursor class to use.
    :param query_callback:
        Called as ``callback(cursor, query, elapsed_ms)`` after every statement,
        with the duration as a float in milliseconds. Use it to route statement
        logging wherever you want — a different level than ``echo``'s INFO, a
        slow-query log, a tracing span. ``executemany`` and ``callproc`` report
        once for the whole call rather than per row. Independent of ``echo``:
        setting both logs and calls back.
    :param init_command: Initial SQL statement to run when connection is established.
    :param connect_timeout: The timeout for connecting to the database in seconds.
        (default: 10, min: 1, max: 31536000)
    :param ssl: Optional dict of arguments similar to mysql_ssl_set()'s parameters or SSL Context to force SSL
    :param read_default_group: Group to read from in the configuration file.
    :param autocommit: Autocommit mode. None means use server default. (default: False)
    :param local_infile: Boolean to enable the use of LOAD DATA LOCAL  (default: False)
    :param max_allowed_packet: Max size of packet sent to server in bytes. (default: 16MB)
        Only used to limit size of "LOAD LOCAL INFILE" data packet smaller than default (16KB).
    :param auth_plugin_map: A dict of plugin names to a class that processes that plugin.
        The class will take the Connection object as the argument to the constructor.
        The class needs an authenticate method taking an authentication packet as
        an argument.  For the dialog plugin, a prompt(echo, prompt) method can be used
        (if no authenticate method) for returning a string from the user. (experimental)
    :param server_public_key: SHA256 authentication plugin public key value. (default: None)
    :param binary_prefix: Add _binary prefix on bytes and bytearray. (default: False)
    :param db: **DEPRECATED** Alias for database.

    See `Connection <https://www.python.org/dev/peps/pep-0249/#connection-objects>`_ in the
    specification.
    """

    def __init__(
            self,
            *,
            user=None,  # The first four arguments is based on DB-API 2.0 recommendation.
            password="",
            password_creator=None,
            host=None,
            database=None,
            unix_socket=None,
            sock=None,
            port=0,
            charset="",
            sql_mode=None,
            read_default_file=None,
            conv=None,
            use_unicode=True,
            client_flag=0,
            cursor_cls=Cursor,
            init_command=None,
            connect_timeout=10,
            read_default_group=None,
            autocommit=False,
            local_infile=False,
            max_allowed_packet=16 * 1024 * 1024,
            auth_plugin_map=None,
            read_timeout=None,
            binary_prefix=False,
            program_name=None,
            server_public_key=None,
            echo=False,
            query_callback=None,
            ssl=None,
            stmt_cache_size=0,
            db=None,  # deprecated
    ):
        self._loop = asyncio.get_event_loop()
        self._last_usage = self._loop.time()
        if db is not None and database is None:
            # We will raise warining in 2022 or later.
            # See https://github.com/PyMySQL/PyMySQL/issues/939
            # warnings.warn("'db' is deprecated, use 'database'", DeprecationWarning, 3)
            database = db
        self._local_infile = bool(local_infile)
        if self._local_infile:
            client_flag |= LOCAL_FILES

        if read_default_group and not read_default_file:
            if sys.platform.startswith("win"):
                read_default_file = "c:\\my.ini"
            else:
                read_default_file = "/etc/my.cnf"

        if read_default_file:
            if not read_default_group:
                read_default_group = "client"

            cfg = Parser()
            cfg.read(os.path.expanduser(read_default_file))

            def _config(key, arg):
                if arg:
                    return arg
                try:
                    return cfg.get(read_default_group, key)
                except Exception:
                    return arg

            user = _config("user", user)
            password = _config("password", password)
            host = _config("host", host)
            database = _config("database", database)
            unix_socket = _config("socket", unix_socket)
            port = int(_config("port", port))
            charset = _config("default-character-set", charset)
            if not ssl:
                ssl = {}
            if isinstance(ssl, dict):
                for key in ["ca", "capath", "cert", "key", "cipher"]:
                    value = _config("ssl-" + key, ssl.get(key))
                    if value:
                        ssl[key] = value
        self._ssl_context = None
        if ssl:
            if not SSL_ENABLED:
                raise NotImplementedError("SSL module not found")
            if sock is None:
                # With a caller-supplied socket the TLS handshake happens
                # before any MySQL byte is sent, so the server must not be
                # asked for an in-protocol upgrade — advertising CLIENT_SSL
                # and then not sending the SSL request packet is a
                # `1043 Bad handshake`.
                client_flag |= SSL
            self._ssl_context = self._create_ssl_ctx(ssl)

        self._echo = echo
        self._query_callback = query_callback
        self._last_usage = self._loop.time()

        self._host = host or "localhost"
        self._port = port or 3306
        if type(self._port) is not int:
            raise ValueError("port should be of type int")
        self._user = user or DEFAULT_USER
        self._password = password or b""
        self._password_creator = password_creator
        if isinstance(self._password, str):
            self._password = self._password.encode("latin1")
        self._db = database
        self._unix_socket = unix_socket
        self._sock = sock
        self._sock_consumed = False
        self._tls_established = False
        if not (0 < connect_timeout <= 31536000):
            raise ValueError("connect_timeout should be >0 and <=31536000")
        self._connect_timeout = connect_timeout or None
        if read_timeout is not None and read_timeout <= 0:
            raise ValueError("read_timeout should be > 0")
        self._read_timeout = read_timeout
        self._secure = False
        self._charset = charset or DEFAULT_CHARSET
        self._use_unicode = use_unicode
        self._encoding = charset_by_name(self._charset).encoding

        client_flag |= CAPABILITIES
        client_flag |= MULTI_STATEMENTS
        if self._db:
            client_flag |= CONNECT_WITH_DB

        self._client_flag = client_flag

        self._cursor_cls = cursor_cls

        self._result = None
        self._affected_rows = 0
        self.host_info = "Not connected"

        # specified autocommit mode. None means use server default.
        self.autocommit_mode = autocommit

        if conv is None:
            conv = converters.conversions

        # Need for MySQLdb compatibility.
        self._encoders = {k: v for (k, v) in conv.items() if type(k) is not int}
        self._decoders = {k: v for (k, v) in conv.items() if type(k) is int}
        self._sql_mode = sql_mode
        self._init_command = init_command
        self._max_allowed_packet = max_allowed_packet
        self._auth_plugin_map = auth_plugin_map or {}
        self._binary_prefix = binary_prefix
        self._server_public_key = server_public_key

        self._connect_attrs = {
            "_client_name": "asyncmy",
            "_pid": str(os.getpid()),
            "_client_version": __VERSION__,
        }

        if program_name:
            self._connect_attrs["program_name"] = program_name

        self._connected = False
        self._proto: Optional[_MySQLProtocol] = None
        self._transport = None
        self._close_reason = None
        self._deprecate_eof = False  # negotiated during the handshake
        self._mariadb_ext_caps = 0  # MariaDB extended capabilities
        self._bulk_supported = False  # MariaDB COM_STMT_BULK_EXECUTE

        # Transparent server-side prepared statement cache (binary protocol).
        # 0 disables it; cursor.execute() then always uses the text protocol.
        self._stmt_cache_size = int(stmt_cache_size)
        self._stmt_cache = {}  # insertion-ordered; LRU via re-insertion
        self._unpreparable = set()

        self._auth_plugin_name = ""

    def _create_ssl_ctx(self, sslp):
        if isinstance(sslp, ssl.SSLContext):
            return sslp
        elif sslp is True:
            # `ssl=True` means "use TLS with default settings" (the form DSN
            # parsers produce for `?ssl=True`). Previously this returned None
            # while SSL stayed advertised in the client flags, so the
            # connection silently fell back to plaintext (#90).
            sslp = {}
        elif not isinstance(sslp, dict):
            raise ValueError(
                "ssl argument must be True, a dict of ssl options, "
                "or an ssl.SSLContext, got %r" % (type(sslp).__name__,)
            )
        ca = sslp.get("ca")
        capath = sslp.get("capath")
        hasnoca = ca is None and capath is None
        ctx = ssl.create_default_context(cafile=ca, capath=capath)
        ctx.check_hostname = not hasnoca and sslp.get("check_hostname", True)
        verify_mode_value = sslp.get("verify_mode")
        if verify_mode_value is None:
            ctx.verify_mode = ssl.CERT_NONE if hasnoca else ssl.CERT_REQUIRED
        elif isinstance(verify_mode_value, bool):
            ctx.verify_mode = ssl.CERT_REQUIRED if verify_mode_value else ssl.CERT_NONE
        else:
            if isinstance(verify_mode_value, str):
                verify_mode_value = verify_mode_value.lower()
            if verify_mode_value in ("none", "0", "false", "no"):
                ctx.verify_mode = ssl.CERT_NONE
            elif verify_mode_value == "optional":
                ctx.verify_mode = ssl.CERT_OPTIONAL
            elif verify_mode_value in ("required", "1", "true", "yes"):
                ctx.verify_mode = ssl.CERT_REQUIRED
            else:
                ctx.verify_mode = ssl.CERT_NONE if hasnoca else ssl.CERT_REQUIRED
        if "cert" in sslp:
            ctx.load_cert_chain(sslp["cert"], keyfile=sslp.get("key"))
        if "cipher" in sslp:
            ctx.set_ciphers(sslp["cipher"])
        ctx.options |= ssl.OP_NO_SSLv2
        ctx.options |= ssl.OP_NO_SSLv3
        return ctx


    def close(self):
        """Close socket connection"""
        if self._transport is not None:
            self._transport.close()
        self._transport = None

    @property
    def _stream_broken(self):
        """True when the underlying stream can no longer be used."""
        proto = self._proto
        return proto is None or proto.eof or proto.exc is not None

    def _close_on_cancel(self):
        """Close the connection after a cancelled read left it desynced."""
        self.close()
        self._close_reason = "Cancelled during execution"
        self._connected = False

    @property
    def connected(self):
        """Return True if the connection is open."""
        return self._connected

    @property
    def loop(self):
        return self._loop

    @property
    def last_usage(self):
        """Return time() when connection was used."""
        return self._last_usage

    async def ensure_closed(self):
        """Send QUIT message and close connection."""
        if self._connected and self._transport is not None:
            send_data = i.pack(1) + B.pack(COM_QUIT)
            self._write_bytes(send_data)
            await self._proto.drain()
            self._transport.close()
            await self._proto.wait_closed()
        self.close()
        self._connected = False

    async def autocommit(self, value):
        self.autocommit_mode = bool(value)
        current = self.get_autocommit()
        if value != current:
            await self._send_autocommit_mode()

    def get_autocommit(self):
        return bool(self.server_status & SERVER_STATUS_AUTOCOMMIT)

    async def _read_ok_packet(self):
        pkt = await self.read_packet()
        if not pkt.is_ok_packet():
            raise errors.OperationalError(CR_COMMANDS_OUT_OF_SYNC, "Command Out of Sync")
        ok = OKPacketWrapper(pkt)
        self.server_status = ok.server_status
        return ok

    async def _send_autocommit_mode(self):
        """Set whether or not to commit after every execute()."""
        await self._execute_command(
            COM_QUERY, "SET AUTOCOMMIT = %s" % self.escape(self.autocommit_mode)
        )
        await self._read_ok_packet()

    async def begin(self):
        """Begin transaction."""
        await self._execute_command(COM_QUERY, "BEGIN")
        await self._read_ok_packet()

    async def commit(self):
        """
        Commit changes to stable storage.

        See `Connection.commit() <https://www.python.org/dev/peps/pep-0249/#commit>`_
        in the specification.
        """
        await self._execute_command(COM_QUERY, "COMMIT")
        await self._read_ok_packet()

    async def rollback(self):
        """
        Roll back the current transaction.

        See `Connection.rollback() <https://www.python.org/dev/peps/pep-0249/#rollback>`_
        in the specification.
        """
        await self._execute_command(COM_QUERY, "ROLLBACK")
        await self._read_ok_packet()

    async def show_warnings(self):
        """Send the "SHOW WARNINGS" SQL """
        await self._execute_command(COM_QUERY, "SHOW WARNINGS")
        result = MySQLResult(self)
        await result.read()
        return result.rows

    async def select_db(self, db):
        """
        Set current db.

        :param db: The name of the db.
        """
        await self._execute_command(COM_INIT_DB, db)
        await self._read_ok_packet()

    def _set_keep_alive(self):
        raw_sock = self._transport.get_extra_info('socket', default=None)
        if raw_sock is None:
            raise RuntimeError("Transport does not expose socket instance")
        raw_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    def _set_nodelay(self, value):
        flag = int(bool(value))
        raw_sock = self._transport.get_extra_info('socket', default=None)
        if raw_sock is None:
            raise RuntimeError("Transport does not expose socket instance")
        raw_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, flag)

    def escape(self, obj, mapping=None):
        """Escape whatever value is passed.

        Non-standard, for internal use; do not use this in your applications.
        """
        if isinstance(obj, str):
            return "'" + self.escape_string(obj) + "'"
        if isinstance(obj, (bytes, bytearray)):
            return converters.escape_bytes_prefixed(obj)
        return converters.escape_item(obj, self._charset, mapping=mapping)

    def literal(self, obj):
        """Alias for escape().

        Non-standard, for internal use; do not use this in your applications.
        """
        return self.escape(obj, self._encoders)

    def escape_string(self, s):
        if self.server_status & SERVER_STATUS_NO_BACKSLASH_ESCAPES:
            return s.replace("'", "''")
        return converters.escape_string(s)

    def _quote_bytes(self, bytes s):
        if self.server_status & SERVER_STATUS_NO_BACKSLASH_ESCAPES:
            return "'%s'" % (s.replace(b"'", b"''").decode("ascii", "surrogateescape"),)
        return converters.escape_bytes(s)

    def cursor(self, cursor: Optional[Type[Cursor]] = None):
        """
        Create a new cursor to execute queries with.

        :param cursor: The type of cursor to create. None means use Cursor.
        :type cursor: :py:class:`Cursor`, :py:class:`SSCursor`, :py:class:`DictCursor`, or :py:class:`SSDictCursor`.
        """
        self._last_usage = self._loop.time()
        if cursor:
            return cursor(self, echo=self._echo, query_callback=self._query_callback)
        return self._cursor_cls(self, echo=self._echo, query_callback=self._query_callback)

    # The following methods are INTERNAL USE ONLY (called from Cursor)
    async def query(self, sql, unbuffered=False):
        if isinstance(sql, str):
            sql = sql.encode(self._encoding, "surrogateescape")
        await self._execute_command(COM_QUERY, sql)
        await self._read_query_result(unbuffered=unbuffered)
        return self._affected_rows

    async def next_result(self, unbuffered=False):
        await self._read_query_result(unbuffered=unbuffered)
        return self._affected_rows

    async def prepare(self, query):
        """Create a server-side prepared statement (binary protocol).

        Returns a :class:`PreparedStatement`. Executing it skips client-side
        escaping entirely and reads results in the binary protocol, which is
        significantly faster for repeated (point) queries.

        :param query: SQL with ``?`` placeholders (native MySQL syntax).
        """
        if isinstance(query, str):
            query = query.encode(self._encoding)
        await self._execute_command(COM_STMT_PREPARE, query)
        # COM_STMT_PREPARE response:
        # https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html
        pkt = await self.read_packet()
        pkt.read_uint8()  # status: 0x00
        statement_id = pkt.read_uint32()
        num_columns = pkt.read_uint16()
        num_params = pkt.read_uint16()
        for _ in range(num_params):
            await self.read_packet()  # parameter definitions (unused)
        if num_params and not self._deprecate_eof:
            await self.read_packet()  # EOF
        for _ in range(num_columns):
            await self.read_packet()  # column definitions (re-read on execute)
        if num_columns and not self._deprecate_eof:
            await self.read_packet()  # EOF
        return PreparedStatement(self, statement_id, num_params)

    async def _acquire_cached_statement(self, query, nargs):
        """Return a cached PreparedStatement for a pyformat query, or None.

        None means: fall back to the text protocol (unsupported placeholder
        style, parameter count mismatch, or the server refused to prepare).
        """
        cache = self._stmt_cache
        stmt = cache.get(query)
        if stmt is not None:
            # LRU: move to the most-recently-used end
            del cache[query]
            cache[query] = stmt
            if stmt.parameter_count != nargs:
                return None
            return stmt
        if query in self._unpreparable:
            return None
        converted = _pyformat_to_qmark(query)
        if converted is None or converted[1] != nargs:
            return None
        try:
            stmt = await self.prepare(converted[0])
        except errors.Error:
            # Not preparable (multi-statement, some SHOW variants, ...) or a
            # genuine SQL error: let the text protocol produce the canonical
            # behavior, and stop re-trying known-bad statements.
            if len(self._unpreparable) < 128:
                self._unpreparable.add(query)
            return None
        if stmt.parameter_count != nargs:
            await stmt.close()
            return None
        cache[query] = stmt
        if len(cache) > self._stmt_cache_size:
            oldest_query = next(iter(cache))
            oldest = cache.pop(oldest_query)
            try:
                await oldest.close()
            except Exception:
                pass
        return stmt

    def affected_rows(self):
        return self._affected_rows

    async def kill(self, thread_id):
        arg = I.pack(thread_id)
        await self._execute_command(COM_PROCESS_KILL, arg)
        return await self._read_ok_packet()

    async def ping(self, reconnect=True):
        """
        Check if the server is alive.

        :param reconnect: If the connection is closed, reconnect.
        :type reconnect: boolean

        :raise Error: If the connection is closed and reconnect=False.
        """
        if not self._connected:
            if reconnect:
                await self.connect()
                reconnect = False
            else:
                raise errors.Error("Already closed")
        try:
            await self._execute_command(COM_PING, "")
            await self._read_ok_packet()
        except Exception:
            if reconnect:
                self.close()
                self._connected = False
                await self.connect()
                await self.ping(False)
            else:
                raise

    async def set_charset(self, charset):
        # Make sure charset is supported.
        encoding = charset_by_name(charset)._encoding

        await self._execute_command(COM_QUERY, "SET NAMES %s" % self.escape(charset))
        await self.read_packet()
        self._charset = charset
        self._encoding = encoding

    async def _refresh_password(self):
        """Ask password_creator for a fresh credential before authenticating."""
        password = self._password_creator()
        # Not iscoroutine: a creator wrapping an async client may hand back a
        # Task or any awaitable, and those must be awaited too.
        if inspect.isawaitable(password):
            password = await password
        if isinstance(password, str):
            password = password.encode("latin1")
        elif isinstance(password, (bytes, bytearray)):
            password = bytes(password)
        else:
            raise ValueError(
                "password_creator must return str or bytes, got %s"
                % type(password).__name__
            )
        self._password = password

    async def connect(self):
        if self._connected:
            return self._proto, self._transport
        try:
            self._close_reason = None
            # statement ids do not survive reconnects
            self._stmt_cache.clear()
            self._deprecate_eof = False
            self._mariadb_ext_caps = 0
            self._bulk_supported = False
            loop = self._loop
            self._tls_established = False

            if self._password_creator is not None:
                await self._refresh_password()

            if self._sock is not None:
                if self._sock_consumed:
                    raise errors.OperationalError(
                        CR_SERVER_LOST,
                        "Cannot reconnect a connection built from a user-supplied "
                        "socket; pass a fresh socket to connect(sock=...)",
                    )
                self._sock_consumed = True
                proto = _MySQLProtocol(loop)
                # With an ssl context the TLS handshake runs now, before any
                # MySQL bytes, instead of the in-protocol upgrade below: the
                # peer here is the caller's endpoint, not necessarily a server
                # that speaks MySQL's STARTTLS dance.
                self._transport, _ = await asyncio.wait_for(
                    loop.create_connection(
                        lambda: proto,
                        sock=self._sock,
                        ssl=self._ssl_context,
                        server_hostname=self._host if self._ssl_context else None,
                    ),
                    timeout=self._connect_timeout,
                )
                self._proto = proto
                self.host_info = "socket %s" % (self._sock,)
                if self._ssl_context is not None:
                    self._secure = True
                    self._tls_established = True
            elif self._unix_socket:
                proto = _MySQLProtocol(loop)
                self._transport, _ = await asyncio.wait_for(
                    loop.create_unix_connection(lambda: proto, self._unix_socket),
                    timeout=self._connect_timeout, )
                self._proto = proto
                self.host_info = "Localhost via UNIX socket"
                self._secure = True
            else:
                while True:
                    try:
                        proto = _MySQLProtocol(loop)
                        self._transport, _ = await asyncio.wait_for(loop.create_connection(
                            lambda: proto,
                            self._host,
                            self._port,
                        ), timeout=self._connect_timeout)
                        self._proto = proto
                        self._set_keep_alive()
                        break
                    except (OSError, IOError) as e:
                        if e.errno == errno.EINTR:
                            continue
                        raise
                self.host_info = "socket %s:%d" % (self._host, self._port)
            # A caller-supplied socket is left exactly as it was configured.
            if not self._unix_socket and self._sock is None:
                self._set_nodelay(True)
            self._next_seq_id = 0

            await self._get_server_information()
            await self._request_authentication()

            self._connected = True

            if self._sql_mode is not None:
                await self.query("SET sql_mode=%s" % (self._sql_mode,))

            if self._init_command is not None:
                await self.query(self._init_command)
                await self.commit()

            if self.autocommit_mode is not None:
                await self.autocommit(self.autocommit_mode)
        except BaseException as e:
            self.close()
            if isinstance(e, (OSError, IOError)):
                raise errors.OperationalError(
                    CR_CONN_HOST_ERROR, "Can't connect to MySQL server on %r (%s)" % (self._host, e)
                ) from e
            # If e is neither DatabaseError or IOError, It's a bug.
            # But raising AssertionError hides original error.
            # So just reraise it.
            raise e

    def write_packet(self, bytes payload):
        """
        Writes an entire "mysql packet" in its entirety to the network
        adding its length and sequence number.
        """
        # Internal note: when you build packet manually and calls _write_bytes()
        # directly, you should set self._next_seq_id properly.
        cdef Py_ssize_t length = len(payload)
        header = bytes(
            (length & 0xFF, (length >> 8) & 0xFF, (length >> 16) & 0xFF, self._next_seq_id)
        )
        if length < 8192:
            self._write_bytes(header + payload)
        else:
            # avoid copying large payloads just to prepend 4 header bytes
            self._write_bytes(header)
            self._write_bytes(payload)
        self._next_seq_id = (self._next_seq_id + 1) & 0xFF

    async def _fill_buffer(self, need):
        """Suspend until at least `need` unconsumed bytes are buffered."""
        proto = self._proto
        read_timeout = self._read_timeout
        while proto.length - proto.pos < need:
            if proto.exc is not None:
                raise errors.OperationalError(
                    CR_SERVER_LOST,
                    "Lost connection to MySQL server during query (%s)" % (proto.exc,),
                )
            if proto.eof:
                raise errors.OperationalError(
                    CR_SERVER_LOST, "Lost connection to MySQL server during query"
                )
            try:
                if read_timeout:
                    try:
                        await asyncio.wait_for(proto.wait_for_data(need), read_timeout)
                    except asyncio.TimeoutError:
                        await self.ensure_closed()
                        raise errors.OperationalError(
                            CR_SERVER_LOST,
                            "Lost connection to MySQL server during query (read timeout)",
                        )
                else:
                    await proto.wait_for_data(need)
            except asyncio.CancelledError:
                # Cancelled mid-read: the protocol stream is now desynced, so
                # the connection must not be reused (e.g. returned to a pool).
                self._close_on_cancel()
                raise
            except (IOError, OSError, asyncio.TimeoutError) as e:
                raise errors.OperationalError(
                    CR_SERVER_LOST,
                    "Lost connection to MySQL server during query (%s)" % (e,),
                )

    async def read_packet(self, packet_type=MysqlPacket):
        """
        Read an entire "mysql packet" in its entirety from the network
        and return a MysqlPacket type that represents the results.

        :raise OperationalError: If the connection to the MySQL server is lost.
        :raise InternalError: If the packet sequence number is wrong.
        """
        proto = self._proto
        buff = None
        while True:
            if proto.length - proto.pos < 4:
                await self._fill_buffer(4)
            buffer = proto.buffer
            pos = proto.pos
            bytes_to_read = buffer[pos] | (buffer[pos + 1] << 8) | (buffer[pos + 2] << 16)
            packet_number = buffer[pos + 3]
            pos += 4
            proto.pos = pos
            if packet_number != self._next_seq_id:
                if packet_number == 0:
                    # MariaDB sends error packet with seqno==0 when shutdown
                    raise errors.OperationalError(
                        CR_SERVER_LOST,
                        "Lost connection to MySQL server during query",
                    )
                raise errors.InternalError(
                    "Packet sequence number wrong - got %d expected %d"
                    % (packet_number, self._next_seq_id)
                )
            self._next_seq_id = (self._next_seq_id + 1) & 0xFF
            if proto.length - pos < bytes_to_read:
                await self._fill_buffer(bytes_to_read)
                # the buffer may have been compacted or reallocated while waiting
                buffer = proto.buffer
                pos = proto.pos
            recv_data = _take_bytes(buffer, pos, bytes_to_read)
            proto.pos = pos + bytes_to_read
            # Fast path: single packet (most common case ~99%)
            if bytes_to_read < MAX_PACKET_LEN and buff is None:
                break
            # Slow path: payload split across 16MB wire packets
            # https://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
            if buff is None:
                buff = []
            buff.append(recv_data)
            if bytes_to_read < MAX_PACKET_LEN:
                recv_data = b"".join(buff)
                break
        packet = packet_type(recv_data, encoding=self._encoding)
        if packet.is_error_packet():
            if self._result is not None and self._result.unbuffered_active is True:
                self._result.unbuffered_active = False
            packet.raise_for_error()
        return packet

    async def _read_bytes(self, num_bytes: int):
        # Kept for backwards compatibility; packet reads go through the
        # protocol's receive buffer (see _fill_buffer/read_packet).
        proto = self._proto
        if proto.length - proto.pos < num_bytes:
            await self._fill_buffer(num_bytes)
        pos = proto.pos
        data = _take_bytes(proto.buffer, pos, num_bytes)
        proto.pos = pos + num_bytes
        return data

    def _write_bytes(self, bytes data):
        transport = self._transport
        if transport is None or transport.is_closing():
            self.close()
            self._connected = False
            raise errors.OperationalError(CR_SERVER_GONE_ERROR, "MySQL server has gone away")
        try:
            transport.write(data)
        except (OSError, RuntimeError) as exc:
            # uvloop raises RuntimeError for a closed handle. Preserve unrelated
            # RuntimeErrors instead of masking application/programming errors.
            if isinstance(exc, RuntimeError) and not transport.is_closing():
                raise
            self.close()
            self._connected = False
            raise errors.OperationalError(
                CR_SERVER_GONE_ERROR, "MySQL server has gone away (%s)" % (exc,)
            ) from exc

    async def _read_query_result(self, unbuffered=False):
        self._result = None
        if unbuffered:
            try:
                result = MySQLResult(self)
                await result.init_unbuffered_query()
            except Exception:
                result.unbuffered_active = False
                result.connection = None
                raise
        else:
            result = MySQLResult(self)
            await result.read()
        self._result = result
        self._affected_rows = result.affected_rows
        if result.server_status != 0:
            self.server_status = result.server_status

    def insert_id(self):
        if self._result:
            return self._result.insert_id
        else:
            return 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.close()
        else:
            await self.ensure_closed()

    async def _execute_command(self, command, sql):
        """
        :raise InterfaceError: If the connection is closed.
        :raise ValueError: If no username was specified.
        """
        if not self._connected:
            raise errors.InterfaceError(0, self._close_reason or "Not connected")

        # If the last query was unbuffered, make sure it finishes before
        # sending new commands
        if self._result is not None:
            if self._result.unbuffered_active:
                warnings.warn("Previous unbuffered result was left incomplete")
                self._result._finish_unbuffered_query()
            while self._result.has_next:
                await self.next_result()
            self._result = None

        if isinstance(sql, str):
            sql = sql.encode(self._encoding)

        packet_size = min(MAX_PACKET_LEN, len(sql) + 1)  # +1 is for command

        # tiny optimization: build first packet manually instead of
        # calling self..write_packet()
        prelude = iB.pack(packet_size, command)
        chunk = sql[: packet_size - 1]
        if packet_size < 8192:
            self._write_bytes(prelude + chunk)
        else:
            # avoid copying a large SQL payload just to prepend the header
            self._write_bytes(prelude)
            self._write_bytes(chunk)
        self._next_seq_id = 1

        if packet_size < MAX_PACKET_LEN:
            return

        sql = sql[packet_size - 1:]
        while True:
            packet_size = min(MAX_PACKET_LEN, len(sql))
            self.write_packet(sql[:packet_size])
            sql = sql[packet_size:]
            if not sql and packet_size < MAX_PACKET_LEN:
                break

    async def _request_authentication(self):
        # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
        if int(self.server_version.split(".", 1)[0]) >= 5:
            self._client_flag |= MULTI_RESULTS

        if self.server_capabilities & DEPRECATE_EOF:
            # Result sets then omit the EOF between metadata and rows and end
            # with an OK packet (0xFE header) instead of an EOF packet.
            self._client_flag |= DEPRECATE_EOF
            self._deprecate_eof = True

        if self._user is None:
            raise ValueError("Did not specify a username")

        charset_id = charset_by_name(self._charset).id
        # _tls_established means the caller handed us a socket that is already
        # encrypted, so there is no in-protocol upgrade left to do.
        if self._ssl_context and not self._tls_established:
            # capablities, max packet, charset
            data = IIB.pack(self._client_flag, MAX_PACKET_LEN, charset_id)
            data += b'\x00' * (32 - len(data))

            self.write_packet(data)

            # Stop sending events to the protocol
            self._transport.pause_reading()

            # Get the raw socket from the transport
            raw_sock = self._transport.get_extra_info('socket', default=None)
            if raw_sock is None:
                raise RuntimeError("Transport does not expose socket instance")

            raw_sock = raw_sock.dup()
            self._transport.close()
            # MySQL expects TLS negotiation to happen in the middle of a
            # TCP connection not at start. Passing in a socket to
            # create_connection will cause it to negotiate TLS on an existing
            # connection not initiate a new one.
            proto = _MySQLProtocol(self._loop)
            self._transport, _ = await self._loop.create_connection(
                lambda: proto, sock=raw_sock, ssl=self._ssl_context,
                server_hostname=self._host,
            )
            self._proto = proto
            # The channel is encrypted from here on. caching_sha2_password
            # full auth sends the password in the clear over a secure channel;
            # without this it took the RSA branch instead and the server, which
            # expects cleartext on a TLS connection, replied 1045 (#117).
            self._secure = True
        if isinstance(self._user, str):
            self._user = self._user.encode(self._encoding)

        if self._mariadb_ext_caps & MARIADB_CLIENT_STMT_BULK_OPERATIONS:
            # MariaDB: clear CLIENT_MYSQL and advertise extended capabilities
            # in the last 4 bytes of the 23-byte filler.
            self._client_flag &= ~CLIENT_MYSQL
            self._bulk_supported = True
            data_init = (
                i.pack(self._client_flag)
                + I.pack(MAX_PACKET_LEN)
                + B_.pack(charset_id)
                + b"\x00" * 19
                + I.pack(MARIADB_CLIENT_STMT_BULK_OPERATIONS)
            )
        else:
            data_init = iIB23s.pack(self._client_flag, MAX_PACKET_LEN, charset_id, b"")
        data = data_init + self._user + b"\0"

        authresp = b""
        plugin_name = None

        if self._auth_plugin_name == "":
            plugin_name = b""
            authresp = auth.scramble_native_password(self._password, self.salt)
        elif self._auth_plugin_name == "mysql_native_password":
            plugin_name = b"mysql_native_password"
            authresp = auth.scramble_native_password(self._password, self.salt)
        elif self._auth_plugin_name == "caching_sha2_password":
            plugin_name = b"caching_sha2_password"
            if self._password:
                authresp = auth.scramble_caching_sha2(self._password, self.salt)
        elif self._auth_plugin_name == "sha256_password":
            plugin_name = b"sha256_password"
            if self._ssl_context and self.server_capabilities & SSL:
                authresp = self._password + b"\0"
            elif self._password:
                authresp = b"\1"  # request public key
            else:
                authresp = b"\0"  # empty password

        if self.server_capabilities & PLUGIN_AUTH_LENENC_CLIENT_DATA:
            data += _lenenc_int(len(authresp)) + authresp
        elif self.server_capabilities & SECURE_CONNECTION:
            data += B_.pack(len(authresp)) + authresp
        else:  # pragma: no cover - not testing against servers without secure auth (>=5.0)
            data += authresp + b"\0"

        if self._db and self.server_capabilities & CONNECT_WITH_DB:
            if isinstance(self._db, str):
                self._db = self._db.encode(self._encoding)
            data += self._db + b"\0"

        if self.server_capabilities & PLUGIN_AUTH:
            data += (plugin_name or b"") + b"\0"

        if self.server_capabilities & CONNECT_ATTRS:
            connect_attrs = b""
            for k, v in self._connect_attrs.items():
                k = k.encode("utf-8")
                connect_attrs += B_.pack(len(k)) + k
                v = v.encode("utf-8")
                connect_attrs += B_.pack(len(v)) + v
            data += B_.pack(len(connect_attrs)) + connect_attrs

        self.write_packet(data)
        auth_packet = await self.read_packet()

        # if authentication method isn't accepted the first byte
        # will have the octet 254
        if auth_packet.is_auth_switch_request():
            # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
            auth_packet.read_uint8()  # 0xfe packet identifier
            plugin_name = auth_packet.read_string()
            if self.server_capabilities & PLUGIN_AUTH and plugin_name is not None:
                auth_packet = await self._process_auth(plugin_name, auth_packet)
            else:
                # send legacy handshake
                data = auth.scramble_old_password(self._password, self.salt) + b"\0"
                self.write_packet(data)
                auth_packet = await self.read_packet()
        elif auth_packet.is_extra_auth_data():
            # https://dev.mysql.com/doc/internals/en/successful-authentication.html
            if self._auth_plugin_name == "caching_sha2_password":
                auth_packet = await auth.caching_sha2_password_auth(self, auth_packet)
            elif self._auth_plugin_name == "sha256_password":
                auth_packet = await auth.sha256_password_auth(self, auth_packet)
            else:
                raise errors.OperationalError(
                    "Received extra packet for auth method %r", self._auth_plugin_name
                )
        return auth_packet

    async def _process_auth(self, plugin_name, auth_packet):
        handler = self._get_auth_plugin_handler(plugin_name)
        if handler:
            try:
                return handler.authenticate(auth_packet)
            except AttributeError:
                if plugin_name != b"dialog":
                    raise errors.OperationalError(
                        2059,
                        "Authentication plugin '%s'"
                        " not loaded: - %r missing authenticate method"
                        % (plugin_name, type(handler)),
                    )
        if plugin_name == b"caching_sha2_password":
            return await auth.caching_sha2_password_auth(self, auth_packet)
        elif plugin_name == b"sha256_password":
            return await auth.sha256_password_auth(self, auth_packet)
        elif plugin_name == b"mysql_native_password":
            data = auth.scramble_native_password(self._password, auth_packet.read_all())
        elif plugin_name == b"client_ed25519":
            data = auth.ed25519_password(self._password, auth_packet.read_all())
        elif plugin_name == b"mysql_old_password":
            data = auth.scramble_old_password(self._password, auth_packet.read_all()) + b"\0"
        elif plugin_name == b"mysql_clear_password":
            # https://dev.mysql.com/doc/internals/en/clear-text-authentication.html
            data = self._password + b"\0"
        elif plugin_name == b"dialog":
            pkt = auth_packet
            while True:
                flag = pkt.read_uint8()
                echo = (flag & 0x06) == 0x02
                last = (flag & 0x01) == 0x01
                prompt = pkt.read_all()

                if prompt == b"Password: ":
                    self.write_packet(self._password + b"\0")
                elif handler:
                    resp = "no response - TypeError within plugin.prompt method"
                    try:
                        resp = handler.prompt(echo, prompt)
                        self.write_packet(resp + b"\0")
                    except AttributeError:
                        raise errors.OperationalError(
                            2059,
                            "Authentication plugin '%s'"
                            " not loaded: - %r missing prompt method" % (plugin_name, handler),
                        )
                    except TypeError:
                        raise errors.OperationalError(
                            2061,
                            "Authentication plugin '%s'"
                            " %r didn't respond with string. Returned '%r' to prompt %r"
                            % (plugin_name, handler, resp, prompt),
                        )
                else:
                    raise errors.OperationalError(
                        2059,
                        "Authentication plugin '%s' (%r) not configured" % (plugin_name, handler),
                    )
                pkt = await self.read_packet()
                pkt.check_error()
                if pkt.is_ok_packet() or last:
                    break
            return pkt
        else:
            raise errors.OperationalError(
                2059, "Authentication plugin '%s' not configured" % plugin_name
            )

        self.write_packet(data)
        pkt = await self.read_packet()
        pkt.check_error()
        return pkt

    def _get_auth_plugin_handler(self, plugin_name):
        plugin_class = self._auth_plugin_map.get(plugin_name)
        if not plugin_class and isinstance(plugin_name, bytes):
            plugin_class = self._auth_plugin_map.get(plugin_name.decode("ascii"))
        if plugin_class:
            try:
                handler = plugin_class(self)
            except TypeError:
                raise errors.OperationalError(
                    2059,
                    "Authentication plugin '%s'"
                    " not loaded: - %r cannot be constructed with connection object"
                    % (plugin_name, plugin_class),
                )
        else:
            handler = None
        return handler

    # _mysql support
    def thread_id(self):
        return self.server_thread_id[0]

    def character_set_name(self):
        return self._charset

    def get_host_info(self):
        return self.host_info

    def get_proto_info(self):
        return self.protocol_version

    def get_transaction_status(self):
        return bool(self.server_status & SERVER_STATUS_IN_TRANS)

    async def _get_server_information(self):
        i = 0
        packet = await self.read_packet()
        data = packet.get_all_data()

        self.protocol_version = data[i]
        i += 1

        server_end = data.find(b"\0", i)
        self.server_version = data[i:server_end].decode("latin1")
        i = server_end + 1

        self.server_thread_id = I.unpack(data[i: i + 4])
        i += 4

        self.salt = data[i: i + 8]
        i += 9  # 8 + 1(filler)

        self.server_capabilities = H.unpack(data[i: i + 2])[0]
        i += 2

        if len(data) >= i + 6:
            lang, stat, cap_h, salt_len = BHHB.unpack(data[i: i + 6])
            i += 6
            # TODO: deprecate server_language and server_charset.
            # mysqlclient-python doesn't provide it.
            self.server_language = lang
            try:
                self.server_charset = charset_by_id(lang).name
            except KeyError:
                # unknown collation
                self.server_charset = None

            self.server_status = stat
            self.server_capabilities |= cap_h << 16
            salt_len = max(12, salt_len - 9)

        # reserved: 6 filler bytes, then 4 bytes of MariaDB extended
        # capabilities (zero on MySQL servers)
        if "MariaDB" in self.server_version and len(data) >= i + 10:
            self._mariadb_ext_caps = I.unpack(data[i + 6: i + 10])[0]
        i += 10

        if len(data) >= i + salt_len:
            # salt_len includes auth_plugin_data_part_1 and filler
            self.salt += data[i: i + salt_len]
            i += salt_len

        i += 1
        # AUTH PLUGIN NAME may appear here.
        if self.server_capabilities & PLUGIN_AUTH and len(data) >= i:
            # Due to Bug#59453 the auth-plugin-name is missing the terminating
            # NUL-char in versions prior to 5.5.10 and 5.6.2.
            # ref: https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake
            # didn't use version checks as mariadb is corrected and reports
            # earlier than those two.
            server_end = data.find(b"\0", i)
            if server_end < 0:  # pragma: no cover - very specific upstream bug
                # not found \0 and last field so take it all
                self._auth_plugin_name = data[i:].decode("utf-8")
            else:
                self._auth_plugin_name = data[i:server_end].decode("utf-8")

    def get_server_info(self):
        return self.server_version

    Warning = errors.Warning
    Error = errors.Error
    InterfaceError = errors.InterfaceError
    DatabaseError = errors.DatabaseError
    DataError = errors.DataError
    OperationalError = errors.OperationalError
    IntegrityError = errors.IntegrityError
    InternalError = errors.InternalError
    ProgrammingError = errors.ProgrammingError
    NotSupportedError = errors.NotSupportedError


class PreparedStatement:
    """A server-side prepared statement (binary protocol).

    Created via :meth:`Connection.prepare`. Parameters are sent in binary form
    (no client-side escaping) and result rows are parsed from the binary
    protocol (no text parsing for numeric/temporal columns).

    Notes:
    - placeholders use native MySQL ``?`` syntax, not ``%s``;
    - custom ``conv`` decoders apply only to string-typed columns; numeric and
      temporal columns decode natively;
    - only the first result set is returned (extra sets are drained).
    """

    def __init__(self, connection, statement_id, parameter_count):
        self._connection = connection
        self._statement_id = statement_id
        self._parameter_count = parameter_count
        self._closed = False
        # Column metadata cached from the first execute; later executes skip
        # re-parsing the (identical) column definition packets.
        self._meta = None

    @property
    def parameter_count(self):
        return self._parameter_count

    async def execute(self, args=()):
        """Execute with ``args`` bound to the statement's ``?`` placeholders.

        Returns the result object: ``result.rows`` (tuple of row tuples, None
        for non-SELECT), ``result.affected_rows``, ``result.insert_id``,
        ``result.description``.
        """
        conn = self._connection
        if self._closed or conn is None:
            raise errors.ProgrammingError("Prepared statement is closed")
        if not isinstance(args, (tuple, list)):
            args = (args,)
        if len(args) != self._parameter_count:
            raise errors.ProgrammingError(
                "Expected %d parameters, got %d" % (self._parameter_count, len(args))
            )
        payload = I.pack(self._statement_id) + b"\x00" + I.pack(1)
        if self._parameter_count:
            payload += pack_binary_params(tuple(args), conn._encoding)
        await conn._execute_command(COM_STMT_EXECUTE, payload)
        result = MySQLResult(conn)
        await result.read_binary(self)
        has_next = result.has_next
        while has_next:  # drain extra result sets (e.g. from stored procedures)
            extra = MySQLResult(conn)
            await extra.read_binary()
            has_next = extra.has_next
        result.has_next = False
        conn._result = result
        conn._affected_rows = result.affected_rows
        if result.server_status != 0:
            conn.server_status = result.server_status
        return result

    async def execute_bulk(self, rows):
        """MariaDB COM_STMT_BULK_EXECUTE: bind many rows in one round-trip.

        ``rows`` is a sequence of parameter tuples. Returns the result object
        (``affected_rows``, ``insert_id``). Only usable when the server
        advertises MARIADB_CLIENT_STMT_BULK_OPERATIONS.
        """
        conn = self._connection
        if self._closed or conn is None:
            raise errors.ProgrammingError("Prepared statement is closed")
        if not conn._bulk_supported:
            raise errors.NotSupportedError("Server does not support COM_STMT_BULK_EXECUTE")
        packed = pack_bulk_rows(rows, self._parameter_count, conn._encoding)
        if packed is None:
            # Raised before any I/O, so callers may safely fall back.
            raise ValueError(
                "Rows are not bulk-compatible (mixed types or all-NULL column)"
            )
        # stmt_id(4) + flags(2): 128 = SEND_TYPES_TO_SERVER
        payload = I.pack(self._statement_id) + b"\x80\x00" + packed[0] + packed[1]
        await conn._execute_command(COM_STMT_BULK_EXECUTE, payload)
        result = MySQLResult(conn)
        await result.read()  # single OK (or error) packet
        conn._result = result
        conn._affected_rows = result.affected_rows
        if result.server_status != 0:
            conn.server_status = result.server_status
        return result

    async def close(self):
        """Deallocate the statement on the server (no server response)."""
        if self._closed:
            return
        self._closed = True
        conn = self._connection
        self._connection = None
        if conn is not None and conn.connected:
            await conn._execute_command(COM_STMT_CLOSE, I.pack(self._statement_id))

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


cdef class MySQLResult:
    cdef:
        public connection
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

    def __init__(self, connection: Connection):
        self.connection = connection
        self._deprecate_eof = connection._deprecate_eof
        self.affected_rows = 0
        self.insert_id = 0
        self.server_status = 0
        self.warning_count = 0
        self.message = None
        self.field_count = 0
        self.description = None
        self.rows = None
        self.has_next = False
        self.unbuffered_active = False

    def __del__(self):
        if self.unbuffered_active:
            self._finish_unbuffered_query()

    async def read(self):
        try:
            first_packet = await self.connection.read_packet()

            if first_packet.is_ok_packet():
                self._read_ok_packet(first_packet)
            elif first_packet.is_load_local_packet():
                await self._read_load_local_packet(first_packet)
            else:
                await self._read_result_packet(first_packet)
        finally:
            self.connection = None

    async def read_binary(self, stmt=None):
        """Read a COM_STMT_EXECUTE response (binary protocol result set)."""
        try:
            first_packet = await self.connection.read_packet()
            if first_packet.is_ok_packet():
                self._read_ok_packet(first_packet)
            else:
                self.field_count = first_packet.read_length_encoded_integer()
                meta = stmt._meta if stmt is not None else None
                if meta is not None and meta[0] == self.field_count:
                    await self._consume_descriptions(meta)
                else:
                    await self._get_descriptions()
                    if stmt is not None:
                        stmt._meta = (
                            self.field_count,
                            self.fields,
                            self.converters,
                            self._row_converters,
                            self._binary_colspecs,
                            self.description,
                        )
                await self._read_binary_rowdata_packet()
        finally:
            self.connection = None

    async def _consume_descriptions(self, tuple meta):
        """Skip the column definition packets, reusing cached metadata."""
        # column definitions, plus a trailing EOF unless DEPRECATE_EOF is on
        cdef Py_ssize_t remaining = self.field_count + (0 if self._deprecate_eof else 1)
        conn = self.connection
        proto = conn._proto
        while remaining:
            new_pos, new_seq, skipped = skip_packets_from_buffer(
                proto.buffer, proto.pos, proto.length, conn._next_seq_id, remaining
            )
            proto.pos = new_pos
            conn._next_seq_id = new_seq
            remaining -= skipped
            if remaining:
                # incomplete or error packet: take the generic path for one
                await conn.read_packet()
                remaining -= 1
        self.fields = meta[1]
        self.converters = meta[2]
        self._row_converters = meta[3]
        self._binary_colspecs = meta[4]
        self.description = meta[5]

    async def _read_binary_rowdata_packet(self):
        cdef list rows = []
        cdef tuple specs = self._binary_colspecs
        conn = self.connection
        proto = conn._proto
        while True:
            new_pos, new_seq = parse_binary_rows_from_buffer(
                proto.buffer, proto.pos, proto.length, specs, conn._next_seq_id, rows,
                self._deprecate_eof,
            )
            proto.pos = new_pos
            conn._next_seq_id = new_seq
            packet = await conn.read_packet()
            if self._check_packet_is_eof(packet):
                self.connection = None
                break
            rows.append(packet.read_binary_row(specs))

        self.affected_rows = len(rows)
        self.rows = tuple(rows)

    async def init_unbuffered_query(self):
        """
        :raise OperationalError: If the connection to the MySQL server is lost.
        :raise InternalError:
        """
        self.unbuffered_active = True
        first_packet = await self.connection.read_packet()

        if first_packet.is_ok_packet():
            self._read_ok_packet(first_packet)
            self.unbuffered_active = False
            self.connection = None
        elif first_packet.is_load_local_packet():
            await self._read_load_local_packet(first_packet)
            self.unbuffered_active = False
            self.connection = None
        else:
            self.field_count = first_packet.read_length_encoded_integer()
            await self._get_descriptions()

            # Apparently, MySQLdb picks this number because it's the maximum
            # value of a 64bit unsigned integer. Since we're emulating MySQLdb,
            # we set it to this instead of None, which would be preferred.
            self.affected_rows = 18446744073709551615

    def _read_ok_packet(self, first_packet):
        ok_packet = OKPacketWrapper(first_packet)
        self.affected_rows = ok_packet.affected_rows
        self.insert_id = ok_packet.insert_id
        self.server_status = ok_packet.server_status
        self.warning_count = ok_packet.warning_count
        self.message = ok_packet.message
        self.has_next = ok_packet.has_next

    async def _read_load_local_packet(self, first_packet):
        if not self.connection._local_infile:
            raise RuntimeError(
                "**WARN**: Received LOAD_LOCAL packet but local_infile option is false."
            )
        load_packet = LoadLocalPacketWrapper(first_packet)
        sender = LoadLocalFile(load_packet.filename, self.connection)
        try:
            await sender.send_data()
        except Exception:
            await self.connection.read_packet()  # skip ok packet
            raise

        ok_packet = await self.connection.read_packet()
        if not ok_packet.is_ok_packet():  # pragma: no cover - upstream induced protocol error
            raise errors.OperationalError(CR_COMMANDS_OUT_OF_SYNC, "Commands Out of Sync")
        self._read_ok_packet(ok_packet)

    def _check_packet_is_eof(self, packet):
        if self._deprecate_eof:
            # Result sets end with an OK packet carrying a 0xFE header.
            # A row packet can never start with 0xFE at payload < 16MB (that
            # first byte would be an 8-byte length-encoded integer marker,
            # implying a >=16MB packet).
            if not packet.is_auth_switch_request():  # first byte != 0xFE
                return False
            packet.advance(1)
            packet.read_length_encoded_integer()  # affected rows
            packet.read_length_encoded_integer()  # insert id
            server_status = packet.read_uint16()
            self.warning_count = packet.read_uint16()
            self.server_status = server_status
            self.has_next = server_status & SERVER_MORE_RESULTS_EXISTS
            return True
        if not packet.is_eof_packet():
            return False
        wp = EOFPacketWrapper(packet)
        self.warning_count = wp.warning_count
        self.has_next = wp.has_next
        return True

    async def _read_result_packet(self, first_packet):
        self.field_count = first_packet.read_length_encoded_integer()
        await self._get_descriptions()
        await self._read_rowdata_packet()

    async def _read_rowdata_packet_unbuffered(self):
        # Check if in an active query
        if not self.unbuffered_active:
            return
        cdef list pending = self._pending_rows

        # Serve rows already bulk-parsed from the receive buffer
        if pending is not None and self._pending_idx < len(pending):
            row = pending[self._pending_idx]
            self._pending_idx += 1
            self.affected_rows = 1
            self.rows = (row,)
            return row

        # Bulk-parse whatever complete row packets are sitting in the buffer
        conn = self.connection
        proto = conn._proto
        cdef list rows = []
        new_pos, new_seq = parse_rows_from_buffer(
            proto.buffer, proto.pos, proto.length, self._row_converters,
            conn._next_seq_id, rows, self._deprecate_eof,
        )
        proto.pos = new_pos
        conn._next_seq_id = new_seq
        if rows:
            self._pending_rows = rows
            self._pending_idx = 1
            row = rows[0]
            self.affected_rows = 1
            self.rows = (row,)
            return row
        self._pending_rows = None

        # Slow path: await the next packet (row, EOF, or error)
        packet = await conn.read_packet()
        if self._check_packet_is_eof(packet):
            self.unbuffered_active = False
            self.connection = None
            self.rows = None
            return

        row = self._read_row_from_packet(packet)
        self.affected_rows = 1
        self.rows = (row,)  # rows should tuple of row for MySQL-python compatibility.
        return row

    async def _finish_unbuffered_query(self):
        # After much reading on the MySQL protocol, it appears that there is,
        # in fact, no way to stop MySQL from sending all the data after
        # executing a query, so we just spin, and wait for an EOF packet.
        while self.unbuffered_active:
            packet = await self.connection.read_packet()
            if self._check_packet_is_eof(packet):
                self.unbuffered_active = False
                self.connection = None  # release reference to kill cyclic reference.

    async def _read_rowdata_packet(self):
        """Read a rowdata packet for each data row in the result set."""
        cdef list rows = []
        cdef tuple convs = self._row_converters
        conn = self.connection
        proto = conn._proto
        while True:
            # Bulk-parse every complete row packet already sitting in the
            # receive buffer without touching the event loop.
            new_pos, new_seq = parse_rows_from_buffer(
                proto.buffer, proto.pos, proto.length, convs, conn._next_seq_id, rows,
                self._deprecate_eof,
            )
            proto.pos = new_pos
            conn._next_seq_id = new_seq
            # Whatever stopped the bulk parser (incomplete data, EOF, error,
            # jumbo packet) goes through the generic packet reader.
            packet = await conn.read_packet()
            if self._check_packet_is_eof(packet):
                self.connection = None  # release reference to kill cyclic reference.
                break
            rows.append(packet.read_row(convs))

        self.affected_rows = len(rows)
        self.rows = tuple(rows)

    cdef _read_row_from_packet(self, packet: MysqlPacket):
        return packet.read_row(self._row_converters)

    async def _get_descriptions(self):
        """Read a column descriptor packet for each column in the result."""
        self.fields = []
        self.converters = []
        cdef list binary_specs = []
        use_unicode = self.connection._use_unicode
        conn_encoding = self.connection._encoding
        description = []

        for i in range(self.field_count):
            field = await self.connection.read_packet(FieldDescriptorPacket)
            self.fields.append(field)
            description.append(field.description())
            field_type = field.type_code
            if use_unicode:
                if field_type == JSON:
                    # When SELECT from JSON column: charset = binary
                    # When SELECT CAST(... AS JSON): charset = connection encoding
                    # This behavior is different from TEXT / BLOB.
                    # We should decode result by connection encoding regardless charsetnr.
                    # See https://github.com/PyMySQL/PyMySQL/issues/488
                    encoding = conn_encoding  # SELECT CAST(... AS JSON)
                elif field_type in TEXT_TYPES:
                    if field.charsetnr == 63:  # binary
                        # TEXTs with charset=binary means BINARY types.
                        encoding = None
                    else:
                        encoding = conn_encoding
                else:
                    # Integers, Dates and Times, and other basic data is encoded in ascii
                    encoding = "ascii"
            else:
                encoding = None
            converter = self.connection._decoders.get(field_type)
            if converter is converters.through:
                converter = None
            if encoding == "ascii" and converter in _BYTES_SAFE_DECODERS:
                # int()/float()/date-time parsers consume bytes directly;
                # skip the intermediate str allocation entirely.
                encoding = None
            if encoding is None:
                code = 0
            elif encoding == "utf8":
                code = 1
            elif encoding == "ascii":
                code = 2
            else:
                code = 3
            self.converters.append((code, encoding, converter))
            binary_specs.append(
                (field_type, bool(field.flags & 32), code, encoding, converter)  # 32: UNSIGNED
            )

        self._row_converters = tuple(self.converters)
        self._binary_colspecs = tuple(binary_specs)
        if not self._deprecate_eof:
            eof_packet = await self.connection.read_packet()
            assert eof_packet.is_eof_packet(), "Protocol error, expecting EOF"
        self.description = tuple(description)


class LoadLocalFile:
    def __init__(self, filename: str, connection: Connection):
        self.filename = filename
        self.connection = connection
        self._loop = connection.loop

    async def send_data(self):
        """
        Send data packets from the local file to the server
        """
        if not self.connection.connected:
            raise errors.InterfaceError(0, "")
        conn = self.connection

        try:
            with open(self.filename, "rb") as open_file:
                packet_size = min(conn._max_allowed_packet, 16 * 1024)  # 16KB is efficient enough
                while True:
                    chunk = open_file.read(packet_size)
                    if not chunk:
                        break
                    conn.write_packet(chunk)
                    await conn._proto.drain()
        except IOError:
            raise errors.OperationalError(FILE_NOT_FOUND, f"Can't find file '{self.filename}'")
        finally:
            # send the empty packet to signify we are done sending data
            conn.write_packet(b"")
            await conn._proto.drain()


def connect(user=None,
            password="",
            password_creator=None,
            host=None,
            database=None,
            unix_socket=None,
            sock=None,
            port=0,
            charset="",
            sql_mode=None,
            read_default_file=None,
            conv=None,
            use_unicode=True,
            client_flag=0,
            cursor_cls=Cursor,
            init_command=None,
            connect_timeout=10,
            read_default_group=None,
            autocommit=False,
            local_infile=False,
            max_allowed_packet=16 * 1024 * 1024,
            auth_plugin_map=None,
            read_timeout=None,
            binary_prefix=False,
            program_name=None,
            echo=False,
            query_callback=None,
            server_public_key=None,
            ssl=None,
            stmt_cache_size=0,
            db=None,  # deprecated
            ):
    coro = _connect(
        user=user,
        password=password,
        password_creator=password_creator,
        host=host,
        database=database,
        unix_socket=unix_socket,
        sock=sock,
        port=port,
        charset=charset,
        sql_mode=sql_mode,
        read_default_file=read_default_file,
        conv=conv,
        use_unicode=use_unicode,
        client_flag=client_flag,
        cursor_cls=cursor_cls,
        init_command=init_command,
        connect_timeout=connect_timeout,
        read_default_group=read_default_group,
        autocommit=autocommit,
        local_infile=local_infile,
        max_allowed_packet=max_allowed_packet,
        auth_plugin_map=auth_plugin_map,
        read_timeout=read_timeout,
        binary_prefix=binary_prefix,
        program_name=program_name,
        server_public_key=server_public_key,
        echo=echo,
        query_callback=query_callback,
        ssl=ssl,
        stmt_cache_size=stmt_cache_size,
        db=db,  # deprecated
    )
    return _ConnectionContextManager(coro)

async def _connect(
        **kwargs,
) -> Connection:
    conn = Connection(
        **kwargs,
    )
    await conn.connect()
    return conn
