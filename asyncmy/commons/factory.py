from __future__ import annotations

import copy
from urllib.parse import urlparse

from asyauth.common.credentials import UniCredential

from asyncmy.commons.target import MySQLTarget


class MySQLConnectionFactory:
    """Builds MySQL connections/pools from a single connection URL.

    Mirrors the ``asysocks``/``asyauth`` based factory pattern used by
    projects like ``aiosmb`` and ``msldap``: a URL is split into a
    :class:`~asyncmy.commons.target.MySQLTarget` (the *where*) and an
    :class:`asyauth.common.credentials.UniCredential` (the *who*), and the
    factory knows how to turn those into live connections.

    Example::

        factory = MySQLConnectionFactory.from_url(
            "mysql://root:secret@127.0.0.1:3306/mydb"
        )
        conn = await factory.get_connection()
        # or
        pool = await factory.create_pool(minsize=1, maxsize=10)

    Supported schemes: ``mysql``/``mariadb`` (plaintext) and
    ``mysqls``/``mariadbs`` (TLS via STARTTLS). Authentication can be tagged
    on the scheme (e.g. ``mysql+plain-password://``) but for MySQL the secret
    is simply carried as a password; the actual MySQL auth handshake
    (native/caching_sha2/sha256/ed25519) is negotiated with the server.
    """

    def __init__(
        self,
        credential: UniCredential = None,
        target: MySQLTarget = None,
        database: str = None,
    ):
        self.credential = credential
        self.target = target
        self.database = database

    @staticmethod
    def from_url(connection_url: str) -> "MySQLConnectionFactory":
        target = MySQLTarget.from_url(connection_url)
        credential = UniCredential.from_url(connection_url)

        url_e = urlparse(connection_url)
        database = None
        if url_e.path not in (None, "", "/"):
            database = url_e.path.lstrip("/") or None

        return MySQLConnectionFactory(credential, target, database)

    def get_target(self) -> MySQLTarget:
        return copy.deepcopy(self.target)

    def get_credential(self) -> UniCredential:
        return copy.deepcopy(self.credential)

    def get_connection(self, **kwargs):
        """Return a (not yet connected) :class:`asyncmy.Connection`."""
        from asyncmy.connection import Connection

        kwargs.setdefault("database", self.database)
        return Connection(
            target=self.get_target(),
            credential=self.get_credential(),
            **kwargs,
        )

    async def create_connection(self, **kwargs):
        """Return a connected :class:`asyncmy.Connection`."""
        conn = self.get_connection(**kwargs)
        await conn.connect()
        return conn

    async def create_pool(self, **kwargs):
        """Return a ready :class:`asyncmy.Pool`."""
        from asyncmy.pool import create_pool

        kwargs.setdefault("database", self.database)
        return await create_pool(
            target=self.get_target(),
            credential=self.get_credential(),
            **kwargs,
        )

    def __str__(self) -> str:
        return "MySQLConnectionFactory(target=%s, database=%s)" % (
            self.target,
            self.database,
        )
