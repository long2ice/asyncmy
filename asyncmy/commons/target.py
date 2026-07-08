from __future__ import annotations

from urllib.parse import urlparse

from asysocks.unicomm.common.target import UniProto, UniTarget

DEFAULT_PORT = 3306

# extra (asyncmy specific) url query params, parsed on top of the ones
# UniTarget already understands (timeout, proxy*, ssl*, ...).
mysqltarget_url_params: dict = {}


class MySQLTarget(UniTarget):
    """Describes how to reach a MySQL/MariaDB server.

    This is a thin :class:`asysocks.unicomm.common.target.UniTarget` subclass
    so that the whole unicomm machinery (proxy chains, timeouts, source
    binding, ...) is available for free.

    MySQL negotiates TLS *after* a cleartext TCP connection is established
    (a STARTTLS-style upgrade), therefore the socket protocol is always
    ``CLIENT_TCP``. Whether TLS should be negotiated is tracked by the
    :attr:`ssl` flag / the presence of :attr:`ssl_ctx`.
    """

    def __init__(
        self,
        ip: str = None,
        port: int = DEFAULT_PORT,
        hostname: str = None,
        timeout: int = 10,
        proxies=None,
        protocol: UniProto = UniProto.CLIENT_TCP,
        dns: str = None,
        ssl: bool = False,
        ssl_ctx=None,
        domain: str = None,
        dc_ip: str = None,
    ):
        UniTarget.__init__(
            self,
            ip,
            port,
            protocol,
            timeout,
            hostname=hostname,
            ssl_ctx=ssl_ctx,
            proxies=proxies,
            domain=domain,
            dc_ip=dc_ip,
            dns=dns,
        )
        self.ssl = bool(ssl) or ssl_ctx is not None

    def is_ssl(self) -> bool:
        return self.ssl or self.protocol == UniProto.CLIENT_SSL_TCP

    def get_host(self) -> str:
        proto = "mysqls" if self.is_ssl() else "mysql"
        return "%s://%s:%s" % (proto, self.get_hostname_or_ip(), self.port)

    @staticmethod
    def from_url(connection_url: str) -> "MySQLTarget":
        url_e = urlparse(connection_url)
        if not url_e.scheme:
            raise Exception("Connection url must define a scheme (e.g. mysql://)")

        schemes = url_e.scheme.upper().split("+")
        base = schemes[0]
        if base in ("MYSQL", "MARIADB"):
            want_ssl = False
        elif base in ("MYSQLS", "MARIADBS", "MYSQL_SSL", "MARIADB_SSL"):
            want_ssl = True
        else:
            raise Exception("Unknown protocol '%s' in connection url" % base)

        # transport modifiers (e.g. mysql+ssl://, mysql+tcp://). Auth related
        # tags such as `+plain-password` are consumed by asyauth, not here.
        for modifier in schemes[1:]:
            token = modifier.replace("-", "_")
            if token in ("SSL", "TLS"):
                want_ssl = True

        port = url_e.port or DEFAULT_PORT

        # The actual socket is always plaintext TCP; TLS (if any) is applied
        # via a STARTTLS upgrade once the MySQL handshake starts.
        unitarget, _ = UniTarget.from_url(
            connection_url, UniProto.CLIENT_TCP, port, mysqltarget_url_params
        )

        return MySQLTarget(
            ip=unitarget.ip,
            port=unitarget.port,
            hostname=unitarget.hostname,
            timeout=unitarget.timeout,
            proxies=unitarget.proxies,
            protocol=UniProto.CLIENT_TCP,
            dns=unitarget.dns,
            ssl=want_ssl,
            ssl_ctx=unitarget.ssl_ctx,
            domain=unitarget.domain,
            dc_ip=unitarget.dc_ip,
        )

    def __str__(self) -> str:
        t = "==== MySQLTarget ====\r\n"
        for k in self.__dict__:
            t += "%s: %s\r\n" % (k, self.__dict__[k])
        return t
