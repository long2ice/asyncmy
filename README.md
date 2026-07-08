# asyncmy — Fast asyncio MySQL/MariaDB driver

[![PyPI](https://img.shields.io/pypi/v/asyncmy.svg)](https://pypi.org/pypi/asyncmy)
[![License](https://img.shields.io/github/license/long2ice/asyncmy)](https://github.com/long2ice/asyncmy)
[![CI](https://github.com/long2ice/asyncmy/actions/workflows/ci.yml/badge.svg)](https://github.com/long2ice/asyncmy/actions/workflows/ci.yml)
[![Release](https://github.com/long2ice/asyncmy/actions/workflows/pypi.yml/badge.svg)](https://github.com/long2ice/asyncmy/actions/workflows/pypi.yml)

`asyncmy` is a fast asyncio MySQL/MariaDB driver. It reuses most of [PyMySQL](https://github.com/PyMySQL/PyMySQL) and [aiomysql](https://github.com/aio-libs/aiomysql) while rewriting the core protocol in [Cython](https://cython.org/) for better performance.

## Features

- **API compatible** with [aiomysql](https://github.com/aio-libs/aiomysql)
- **Faster** via [Cython](https://cython.org/)-compiled core
- **Connection URLs & factory** built on [unicomm](https://github.com/skelsec/asysocks) + [asyauth](https://github.com/skelsec/asyauth), with built-in TLS and proxy (SOCKS/HTTP) support ([details](#connection-urls--factory))
- **MySQL replication protocol** with asyncio ([BinLogStream](https://github.com/long2ice/asyncmy/blob/dev/asyncmy/replication/binlogstream.py))
- **CI-tested** on MySQL and MariaDB ([workflow](https://github.com/long2ice/asyncmy/blob/dev/.github/workflows/ci.yml))

## Benchmark

asyncmy demonstrates excellent performance across realistic workloads:

| Test | asyncmy Rank | Performance |
| ---- | ------------ | ----------- |
| **Connection Pool** (2k queries) | 🏆 **#1/2** | ~10,500 qps (consistently 22-28% faster than aiomysql) |
| **Large Result Set** (50k rows) | #2/4 | ~0.090s (2x faster than aiomysql, close to mysqlclient) |
| **Concurrent Queries** (50 queries) | #1-2/2 | Comparable to aiomysql |
| **Batch Insert** (10k rows) | Variable | Results vary by run |

**Recent optimizations (v0.2.12)** delivered significant performance improvements:

- **Buffer Management**: Zero-copy fast path for single-packet reads
- **DateTime Parsing**: Fast string slicing replacing regex
- **Row Parsing**: Pre-allocated lists and C-level indexing in hot path
- **Protocol Parsing**: Inlined length-coded string reads with fast path for common cases

📊 **[View detailed benchmarks →](./benchmark/README.md)**

## Install

**Requirements:** Python ≥ 3.9

```bash
pip install asyncmy
```

### Windows

asyncmy uses Cython extensions; on Windows you need **Microsoft C++ Build Tools** to build them.

1. Download [Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/).
2. Open CMD as Administrator (recommended) and `cd` to the folder **where** the installer was downloaded.
3. Rename the installer (e.g. `vs_buildtools__XXXXXXXXX.XXXXXXXXXX.exe`) to `vs_buildtools.exe` for convenience.
4. Run (ensure ~5–6GB free disk space):

   ```bash
   vs_buildtools.exe --norestart --passive --downloadThenInstall --includeRecommended --add Microsoft.VisualStudio.Workload.NativeDesktop --add Microsoft.VisualStudio.Workload.VCTools --add Microsoft.VisualStudio.Workload.MSBuildTools
   ```

5. Wait for installation to complete, then restart your computer.
6. Install asyncmy:

   ```bash
   pip install asyncmy
   ```

You can uninstall the Build Tools afterward if desired.

## Usage

### `connect`

Use `asyncmy.connect()` for a single connection. For many concurrent connections, use a [connection pool](#pool).

```py
import asyncio
import os

from asyncmy import connect
from asyncmy.cursors import DictCursor


async def main():
    conn = await connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD", ""),
    )
    async with conn.cursor(cursor=DictCursor) as cursor:
        await cursor.execute("CREATE DATABASE IF NOT EXISTS test")
        await cursor.execute("""
            CREATE TABLE IF NOT EXISTS test.`asyncmy` (
                `id`       int PRIMARY KEY AUTO_INCREMENT,
                `decimal`  decimal(10, 2),
                `date`     date,
                `datetime` datetime,
                `float`    float,
                `string`   varchar(200),
                `tinyint`  tinyint
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
        """.strip())
    await conn.ensure_closed()


if __name__ == "__main__":
    asyncio.run(main())
```

### Pool

For multiple connections, use a connection pool. Pass the same kwargs as `connect()` (e.g. `host`, `user`, `password`).

```py
import asyncio
import asyncmy


async def main():
    pool = await asyncmy.create_pool(host="localhost", user="root", password="")
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1")
            ret = await cursor.fetchone()
            assert ret == (1,)
    pool.close()
    await pool.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
```

### Connection URLs & factory

asyncmy uses [unicomm](https://github.com/skelsec/asysocks) (from `asysocks`) as its **only transport** and [asyauth](https://github.com/skelsec/asyauth) credential objects. This means a whole connection — host, port, database, TLS, proxies and credentials — can be described by a single URL and built with `MySQLConnectionFactory`.

```py
import asyncio

from asyncmy import MySQLConnectionFactory


async def main():
    factory = MySQLConnectionFactory.from_url(
        "mysql://root:secret@127.0.0.1:3306/test"
    )

    # single connection
    conn = await factory.create_connection()
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT VERSION()")
        print(await cursor.fetchone())
    await conn.ensure_closed()

    # or a pool
    pool = await factory.create_pool(minsize=1, maxsize=10)
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1")
    pool.close()
    await pool.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
```

**URL format**

```
<scheme>://[user[:password]@]host[:port][/database][?options]
```

| Scheme | Transport |
| ------ | --------- |
| `mysql` / `mariadb` | plaintext TCP (default port `3306`) |
| `mysqls` / `mariadbs` | TLS, negotiated mid-handshake (STARTTLS style) |

Common query options (parsed by unicomm):

- `timeout=10` — connect timeout in seconds
- `proxytype=socks5&proxyhost=127.0.0.1&proxyport=1080` — tunnel through a SOCKS4/5 or HTTP proxy (chainable as `proxy1type`, `proxy2type`, …)
- `sslca=/path/ca.pem&sslcert=/path/client.pem&sslkey=/path/client.key` — TLS material for `mysqls://`

**TLS**

`mysqls://` upgrades the connection to TLS. With no `ssl*` params the default is a **no-verify** context (convenient for self-signed dev servers); pass `?sslca=/path/ca.pem` to verify against a CA.

```py
factory = MySQLConnectionFactory.from_url("mysqls://root:secret@db.example.com:3306/test")
conn = await factory.create_connection()
```

**Authentication**

The MySQL auth handshake (`mysql_native_password`, `caching_sha2_password`, `sha256_password`, `client_ed25519`) is negotiated with the server; the URL simply carries the secret. An explicit `+plain-password` tag is optional:

```py
# mysql_native_password user — the +plain-password tag is optional
factory = MySQLConnectionFactory.from_url(
    "mysql+plain-password://nativeuser:native123@127.0.0.1:3306/test"
)
```

The classic keyword API (`connect(host=..., user=..., password=...)`) keeps working unchanged; you can also pass a prebuilt `target=MySQLTarget(...)` and `credential=UniCredential(...)` to `connect()` / `Connection()` directly. See [`examples/connection_url.py`](./examples/connection_url.py) for a runnable script.

> **Note:** since unicomm is now the only transport (which is TCP based), UNIX-socket connections (`unix_socket=...`) are no longer supported.

## Replication

asyncmy supports the MySQL replication protocol (like [python-mysql-replication](https://github.com/noplay/python-mysql-replication)) over asyncio.

```py
import asyncio

from asyncmy import connect
from asyncmy.replication import BinLogStream


async def main():
    conn = await connect()
    ctl_conn = await connect()

    stream = BinLogStream(
        conn,
        ctl_conn,
        server_id=1,
        master_log_file="binlog.000172",
        master_log_position=2235312,
        resume_stream=True,
        blocking=True,
    )
    async for event in stream:
        print(event)
    await conn.ensure_closed()
    await ctl_conn.ensure_closed()


if __name__ == "__main__":
    asyncio.run(main())
```

## Acknowledgments

asyncmy builds on these projects:

- [PyMySQL](https://github.com/PyMySQL/PyMySQL) — pure Python MySQL client
- [aiomysql](https://github.com/aio-libs/aiomysql) — asyncio MySQL driver
- [python-mysql-replication](https://github.com/noplay/python-mysql-replication) — MySQL replication protocol (pure Python, on top of PyMySQL)

## License

[Apache-2.0](./LICENSE)
