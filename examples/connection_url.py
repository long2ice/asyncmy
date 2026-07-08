"""Connect using a connection URL via MySQLConnectionFactory.

asyncmy uses `unicomm` (from asysocks) as its only transport and `asyauth`
credential objects, so a whole connection can be described by a single URL::

    <scheme>://[user[:password]@]host[:port][/database][?options]

Schemes:
    mysql / mariadb    -> plaintext TCP (default port 3306)
    mysqls / mariadbs  -> TLS negotiated mid-handshake (STARTTLS style)

Useful query options (parsed by unicomm):
    timeout=10
    proxytype=socks5&proxyhost=127.0.0.1&proxyport=1080
    sslcert=... sslkey=... sslca=...

Run against the docker MySQL used by the tests::

    docker run -d --name asyncmy-test -e MYSQL_ROOT_PASSWORD=123456 \\
        -e MYSQL_DATABASE=test -p 3306:3306 mysql:8.0
"""

import asyncio

from asyncmy import MySQLConnectionFactory


async def basic_url():
    factory = MySQLConnectionFactory.from_url("mysql://root:123456@127.0.0.1:3306/test")
    conn = await factory.create_connection()
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT VERSION()")
        print("basic url ->", await cursor.fetchone())
    await conn.ensure_closed()


async def native_password_user():
    # A user created with:
    #   CREATE USER 'nativeuser'@'%' IDENTIFIED WITH mysql_native_password BY 'native123';
    # The `+plain-password` auth tag is optional; the secret is carried either way.
    factory = MySQLConnectionFactory.from_url(
        "mysql+plain-password://nativeuser:native123@127.0.0.1:3306/test"
    )
    conn = await factory.create_connection()
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT CURRENT_USER()")
        print("native password ->", await cursor.fetchone())
    await conn.ensure_closed()


async def tls_no_verify():
    # `mysqls://` negotiates TLS. With no ssl* params the default is a
    # no-verify context (handy for self-signed dev servers). To verify against
    # a CA pass `?sslca=/path/ca.pem`.
    factory = MySQLConnectionFactory.from_url("mysqls://root:123456@127.0.0.1:3306/test")
    conn = await factory.create_connection()
    async with conn.cursor() as cursor:
        await cursor.execute("SHOW STATUS LIKE 'Ssl_cipher'")
        print("tls ->", await cursor.fetchone())
    await conn.ensure_closed()


async def pool_from_url():
    factory = MySQLConnectionFactory.from_url("mysql://root:123456@127.0.0.1:3306/test")
    pool = await factory.create_pool(minsize=1, maxsize=5)
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1")
            print("pool ->", await cursor.fetchone())
    pool.close()
    await pool.wait_closed()


async def main():
    await basic_url()
    await native_password_user()
    await tls_no_verify()
    await pool_from_url()


if __name__ == "__main__":
    asyncio.run(main())
