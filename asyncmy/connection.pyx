# Python implementation of the MySQL client-server protocol
# http://dev.mysql.com/doc/internals/en/client-server-protocol.html
# Error codes:
# https://dev.mysql.com/doc/refman/5.5/en/error-handling.html
import asyncio
import errno
import os
import socket
import sys
import warnings
from asyncio import StreamReader, StreamWriter
from typing import Optional, Type

from asyncmy import auth, converters, errors
from asyncmy.charset import charset_by_id, charset_by_name
from asyncmy.cursors import Cursor
from asyncmy.optionfile import Parser
from asyncmy.protocol import (EOFPacketWrapper, FieldDescriptorPacket,
                              LoadLocalPacketWrapper, MysqlPacket,
                              OKPacketWrapper)

from .constants.CLIENT import (CAPABILITIES, CONNECT_ATTRS, CONNECT_WITH_DB,
                               LOCAL_FILES, MULTI_RESULTS, MULTI_STATEMENTS,
                               PLUGIN_AUTH, PLUGIN_AUTH_LENENC_CLIENT_DATA,
                               SECURE_CONNECTION, SSL)
from .constants.COMMAND import (COM_INIT_DB, COM_PING, COM_PROCESS_KILL,
                                COM_QUERY, COM_QUIT)
from .constants.CR import (CR_COMMANDS_OUT_OF_SYNC, CR_CONN_HOST_ERROR,
                           CR_SERVER_LOST)
from .constants.ER import FILE_NOT_FOUND
from .constants.FIELD_TYPE import (BIT, BLOB, GEOMETRY, JSON, LONG_BLOB,
                                   MEDIUM_BLOB, STRING, TINY_BLOB, VAR_STRING,
                                   VARCHAR)
from .constants.SERVER_STATUS import (SERVER_STATUS_AUTOCOMMIT,
                                      SERVER_STATUS_IN_TRANS,
                                      SERVER_STATUS_NO_BACKSLASH_ESCAPES)
from .contexts import _ConnectionContextManager
from .structs import B_, BHHB, HBB, IIB, B, H, I, Q, i, iB, iIB23s
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
except (ImportError, KeyError):
    # KeyError occurs when there's no entry in OS database for a current user.
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

cdef _pack_int24(int n):
    return I.pack(n)[:3]

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
    :param database: Database to use, None to not use a particular one.
    :param port: MySQL port to use, default is usually OK. (default: 3306)
    :param unix_socket: Use a unix socket rather than TCP/IP.
    :param read_timeout: The timeout for reading from the connection in seconds (default: None - no timeout)
    :param write_timeout: The timeout for writing to the connection in seconds (default: None - no timeout)
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
    :param init_command: Initial SQL statement to run when connection is established.
    :param connect_timeout: The timeout for connecting to the database in seconds.
        (default: 10, min: 1, max: 31536000)
    :param ssl: Optional SSL Context to force SSL
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
            host='localhost',
            database=None,
            unix_socket=None,
            port=3306,
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
            write_timeout=None,
            binary_prefix=False,
            program_name=None,
            server_public_key=None,
            echo=False,
            ssl=None,
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
        self._echo = echo
        self._last_usage = self._loop.time()
        self._ssl_context = ssl
        if ssl:
            client_flag |= SSL

        self._host = host
        self._port = port
        if type(self._port) is not int:
            raise ValueError("port should be of type int")
        self._user = user or DEFAULT_USER
        self._password = password or b""
        if isinstance(self._password, str):
            self._password = self._password.encode("latin1")
        self._db = database
        self._unix_socket = unix_socket
        if not (0 < connect_timeout <= 31536000):
            raise ValueError("connect_timeout should be >0 and <=31536000")
        self._connect_timeout = connect_timeout or None
        if read_timeout is not None and read_timeout <= 0:
            raise ValueError("read_timeout should be > 0")
        self._read_timeout = read_timeout
        if write_timeout is not None and write_timeout <= 0:
            raise ValueError("write_timeout should be > 0")
        self._write_timeout = write_timeout
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
        self._reader: Optional[StreamReader] = None
        self._writer: Optional[StreamWriter] = None

    def close(self):
        """Close socket connection"""
        if self._writer:
            self._writer.transport.close()
        self._writer = None
        self._reader = None

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
        """Close connection without QUIT message."""
        if self._connected:
            send_data = i.pack(1) + B.pack(COM_QUIT)
            self._write_bytes(send_data)
            await self._writer.drain()
            self._writer.close()
            await self._writer.wait_closed()
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
        transport = self._writer.transport
        transport.pause_reading()
        raw_sock = transport.get_extra_info('socket', default=None)
        if raw_sock is None:
            raise RuntimeError("Transport does not expose socket instance")
        raw_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        transport.resume_reading()

    def _set_nodelay(self, value):
        flag = int(bool(value))
        transport = self._writer.transport
        transport.pause_reading()
        raw_sock = transport.get_extra_info('socket', default=None)
        if raw_sock is None:
            raise RuntimeError("Transport does not expose socket instance")
        raw_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, flag)
        transport.resume_reading()

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

    def escape_string(self, str s):
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
            return cursor(self, echo=self._echo)
        return self._cursor_cls(self, echo=self._echo)

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

    async def connect(self):
        if self._connected:
            return self._reader, self._writer
        try:

            if self._unix_socket:
                self._reader, self._writer = await asyncio.wait_for(asyncio.open_unix_connection(self._unix_socket),
                                                                    timeout=self._connect_timeout, )
                self.host_info = "Localhost via UNIX socket"
                self._secure = True
            else:
                while True:
                    try:
                        self._reader, self._writer = await asyncio.wait_for(asyncio.open_connection(
                            self._host,
                            self._port,
                        ), timeout=self._connect_timeout)
                        self._set_keep_alive()
                        break
                    except (OSError, IOError) as e:
                        if e.errno == errno.EINTR:
                            continue
                        raise
                self.host_info = "socket %s:%d" % (self._host, self._port)
            if not self._unix_socket:
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
        data = _pack_int24(len(payload)) + B.pack(self._next_seq_id) + payload
        self._write_bytes(data)
        self._next_seq_id = (self._next_seq_id + 1) % 256

    async def read_packet(self, packet_type=MysqlPacket):
        """
        Read an entire "mysql packet" in its entirety from the network
        and return a MysqlPacket type that represents the results.

        :raise OperationalError: If the connection to the MySQL server is lost.
        :raise InternalError: If the packet sequence number is wrong.
        """
        buff = bytearray()
        while True:
            packet_header = await self._read_bytes(4)
            btrl, btrh, packet_number = HBB.unpack(packet_header)
            bytes_to_read = btrl + (btrh << 16)
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
            self._next_seq_id = (self._next_seq_id + 1) % 256
            recv_data = await self._read_bytes(bytes_to_read)
            buff.extend(recv_data)
            # https://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
            if bytes_to_read == 0xFFFFFF:
                continue
            if bytes_to_read < MAX_PACKET_LEN:
                break

        packet = packet_type(bytes(buff), encoding=self._encoding)
        if packet.is_error_packet():
            if self._result is not None and self._result.unbuffered_active is True:
                self._result.unbuffered_active = False
            packet.raise_for_error()
        return packet

    async def _read_bytes(self, num_bytes: int):
        try:
            data = await self._reader.readexactly(num_bytes)
        except (IOError, OSError) as e:
            raise errors.OperationalError(
                CR_SERVER_LOST,
                "Lost connection to MySQL server during query (%s)" % (e,),
            )
        except asyncio.IncompleteReadError as e:
            msg = "Lost connection to MySQL server during query"
            raise errors.OperationalError(CR_SERVER_LOST, msg) from e
        return data

    def _write_bytes(self, bytes data):
        self._writer.write(data)

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
            raise errors.InterfaceError(0, "Not connected")

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
        self._write_bytes(prelude + sql[: packet_size - 1])
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

        if self._user is None:
            raise ValueError("Did not specify a username")

        if self._ssl_context:
            # capablities, max packet, charset
            data = IIB.pack(self._client_flag, 16777216, 33)
            data += b'\x00' * (32 - len(data))

            self.write_packet(data)

            # Stop sending events to data_received
            self._writer.transport.pause_reading()

            # Get the raw socket from the transport
            raw_sock = self._writer.transport.get_extra_info('socket',
                                                             default=None)
            if raw_sock is None:
                raise RuntimeError("Transport does not expose socket instance")

            raw_sock = raw_sock.dup()
            self._writer.transport.close()
            # MySQL expects TLS negotiation to happen in the middle of a
            # TCP connection not at start. Passing in a socket to
            # open_connection will cause it to negotiate TLS on an existing
            # connection not initiate a new one.
            self._reader, self._writer = await asyncio.open_connection(
                sock=raw_sock, ssl=self._ssl_context,
                server_hostname=self._host,
            )
        charset_id = charset_by_name(self._charset).id
        if isinstance(self._user, str):
            self._user = self._user.encode(self._encoding)

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
            if self.ssl and self.server_capabilities & SSL:
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

        # reserved
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


cdef class MySQLResult:
    cdef:
        public connection
        public bytes message
        public int affected_rows, warning_count, field_count, server_status, unbuffered_active, has_next
        public list fields, converters
        public unsigned long insert_id
        public tuple rows, description

    def __init__(self, connection: Connection):
        self.connection = connection
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
        if not packet.is_eof_packet():
            return False
        # TODO: Support DEPRECATE_EOF
        # 1) Add DEPRECATE_EOF to CAPABILITIES
        # 2) Mask CAPABILITIES with server_capabilities
        # 3) if server_capabilities & DEPRECATE_EOF: use OKPacketWrapper instead of EOFPacketWrapper
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

        # EOF
        packet = await self.connection.read_packet()
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
        rows = []
        while True:
            packet = await self.connection.read_packet()
            if self._check_packet_is_eof(packet):
                self.connection = None  # release reference to kill cyclic reference.
                break
            rows.append(self._read_row_from_packet(packet))

        self.affected_rows = len(rows)
        self.rows = tuple(rows)

    cpdef _read_row_from_packet(self, packet: MysqlPacket):
        row = []
        for encoding, converter in self.converters:
            try:
                data = packet.read_length_coded_string()
            except IndexError:
                # No more columns in this row
                # See https://github.com/PyMySQL/PyMySQL/pull/434
                break
            if data is not None:
                if encoding is not None:
                    data = data.decode(encoding)
                if converter is not None:
                    data = converter(data)
            row.append(data)
        return tuple(row)

    async def _get_descriptions(self):
        """Read a column descriptor packet for each column in the result."""
        self.fields = []
        self.converters = []
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
            self.converters.append((encoding, converter))

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
                    await conn.write_packet(chunk)
        except IOError:
            raise errors.OperationalError(FILE_NOT_FOUND, f"Can't find file '{self.filename}'")
        finally:
            # send the empty packet to signify we are done sending data
            await conn.write_packet(b"")


def connect(user=None,
            password="",
            host='localhost',
            database=None,
            unix_socket=None,
            port=3306,
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
            write_timeout=None,
            binary_prefix=False,
            program_name=None,
            echo=False,
            server_public_key=None,
            ssl=None,
            db=None,  # deprecated
            ):
    coro = _connect(
        user=user,
        password=password,
        host=host,
        database=database,
        unix_socket=unix_socket,
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
        write_timeout=write_timeout,
        binary_prefix=binary_prefix,
        program_name=program_name,
        server_public_key=server_public_key,
        echo=echo,
        ssl=ssl,
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
