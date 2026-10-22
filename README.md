# asyncmy — The fastest asyncio MySQL/MariaDB driver

[![PyPI](https://img.shields.io/pypi/v/asyncmy.svg)](https://pypi.org/pypi/asyncmy)
[![License](https://img.shields.io/github/license/long2ice/asyncmy)](https://github.com/long2ice/asyncmy)
[![CI](https://github.com/long2ice/asyncmy/actions/workflows/ci.yml/badge.svg)](https://github.com/long2ice/asyncmy/actions/workflows/ci.yml)
[![Release](https://github.com/long2ice/asyncmy/actions/workflows/pypi.yml/badge.svg)](https://github.com/long2ice/asyncmy/actions/workflows/pypi.yml)

`asyncmy` is the fastest asyncio MySQL/MariaDB driver for Python. It keeps the familiar [aiomysql](https://github.com/aio-libs/aiomysql) API while rewriting the entire protocol core in [Cython](https://cython.org/) — down to pointer-level packet parsing. See [our benchmarks](#benchmark) for Python-driver and Go/Rust comparisons, including their measured settings and results.

## Features

- 🚀 **Fast Python queries** — large-result fetching takes about half mysqlclient's time; pooled queries deliver 3.6× aiomysql's throughput in the [local benchmark](#benchmark)
- 🔌 **Drop-in aiomysql replacement** — same API, same cursors (`DictCursor`, `SSCursor`), same pool semantics
- 🧬 **Server-side prepared statements** (binary protocol) via `conn.prepare()` or cached regular Cursor calls — numeric and temporal values decode directly from their binary representation
- ⚡ **C-speed protocol core** — rows are parsed in bulk from the receive buffer in a single C loop, values decode straight from wire bytes via the CPython C-API
- 🏊 **Built-in connection pool** — `asyncmy.create_pool()`, with reusable connections and cached prepared statements
- 📡 **MySQL replication protocol** over asyncio ([BinLogStream](https://github.com/long2ice/asyncmy/blob/dev/asyncmy/replication/binlogstream.py))
- ✅ **CI-tested on MySQL and MariaDB** ([workflow](https://github.com/long2ice/asyncmy/blob/dev/.github/workflows/ci.yml))

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

### Prepared statements (binary protocol)

For repeated queries, server-side prepared statements skip client-side escaping
entirely and read results in MySQL's binary protocol — numeric and temporal
columns decode natively with no text parsing. Placeholders use native `?` syntax.

```py
stmt = await conn.prepare("SELECT id, name FROM users WHERE id = ?")
result = await stmt.execute((42,))
print(result.rows)           # tuple of row tuples
print(result.affected_rows)  # for INSERT/UPDATE/DELETE
await stmt.close()

# or as a context manager
async with await conn.prepare("SELECT ? + ?") as stmt:
    result = await stmt.execute((1, 2))
```

**Transparent mode:** pass `stmt_cache_size=N` to `connect()`/`create_pool()` and
regular `cursor.execute("... %s ...", args)` calls automatically run as cached
server-side prepared statements — no code changes needed (ORMs benefit too).
Queries the server can't prepare fall back to the text protocol silently.

```py
pool = await asyncmy.create_pool(
    host="localhost", user="root", password="", stmt_cache_size=128,
)
```

Note: with the binary protocol, `FLOAT` columns return the exact stored value
rather than the text protocol's decimal-rounded rendering, which is why this
is opt-in.

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

### Type checking

asyncmy ships `py.typed` and stubs for its compiled modules, so mypy and Pylance resolve the API
without extra configuration:

```py
conn = await asyncmy.connect(host="localhost", user="root")   # -> Connection
async with conn.cursor() as cur:
    rows = await cur.fetchall()                               # -> list[Any]
```

Contributors: `make stubs` regenerates the stubs after changing a `.pyx` signature. `make check`
runs `stubtest`, which compares every stub against the compiled module and fails on drift.

### Rotating credentials

Some credentials expire while a pooled connection outlives them — AWS RDS IAM auth tokens last 15
minutes, for instance. Pass `password_creator` instead of `password` and it is consulted before
every connection attempt, including the ones the pool makes on its own when it recycles or
reconnects:

```py
import boto3

client = boto3.client("rds")


def rds_auth_token():
    return client.generate_db_auth_token(
        DBHostname="mydb.cluster.amazonaws.com",
        Port=3306,
        DBUsername="dbuser",
        Region="us-east-1",
    )


pool = await asyncmy.create_pool(
    host="mydb.cluster.amazonaws.com",
    user="dbuser",
    password_creator=rds_auth_token,
)
```

The callable may be a plain function or return an awaitable, and must return `str` or `bytes`. If
both `password` and `password_creator` are given, the creator wins.

### Statement logging

`echo=True` logs every statement and its duration to the `asyncmy` logger at INFO level. Nothing
appears until logging is configured — `logging.basicConfig(level=logging.INFO)` at minimum, since
the root logger defaults to WARNING.

For anything beyond that, pass `query_callback`. It is called as `callback(cursor, query,
elapsed_ms)` after every successful statement, with the duration as a float in milliseconds:

```py
import logging

logger = logging.getLogger("myapp.sql")


def log_slow_queries(cursor, query, elapsed_ms):
    if elapsed_ms > 100:
        logger.warning("[%sms] %s", elapsed_ms, query)


conn = await asyncmy.connect(host="localhost", user="root", query_callback=log_slow_queries)
```

`executemany` and `callproc` report once for the whole call rather than once per row. The callback
is independent of `echo`: set both and you get the log line and the callback.

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

## Benchmark

Measured on **2026-10-22** against local MySQL over **TCP**. The Python-driver
suite and cross-language suite use different workloads and sampling methods;
each comparison below keeps those settings explicit. All times are in
**milliseconds**, lower is better.

### Python driver comparison

One warmup followed by **best of three runs**, using standard asyncio and
`autocommit=True`. The dataset contains 100,000 rows; the filtered large-result
query returns 33,334 of them.

| Workload | asyncmy | mysqlclient | PyMySQL | aiomysql |
| --- | ---: | ---: | ---: | ---: |
| Large result set, 33,334 mixed-type rows | **29** | 60 | 112 | 122 |
| 50 concurrent queries, one connection each | **6** | — | — | 8 |
| 2,000 pooled queries, 5–20 connections | **56** | — | — | 202 |
| Batch insert, 10,000 rows | **123** | 145 | 138 | **123** |

asyncmy leads on result fetching, concurrent queries and pooled queries. Large
result fetching takes about half mysqlclient's time; pooled queries deliver
about **3.6× aiomysql's throughput**. Batch insertion is effectively tied with
aiomysql in this run. Times are rounded to one millisecond; `—` means the
suite does not measure that combination.

The asyncmy column uses the text protocol. Its separate binary large-result
case took **30 ms** with `stmt_cache_size=16`. This schema's text and binary
results are close; the cross-language suite below also measures integer-heavy
and mixed-type scans separately.

Versions: asyncmy **0.2.17** (working tree), mysqlclient **2.3.0**, PyMySQL
**1.2.3**, aiomysql **0.3.2**. See the [Python benchmark guide](./benchmark/README.md#python-driver-suite)
for workloads and commands, and the [recorded run](./benchmark/python-results.json)
for the console-reported timings and settings.

### Cross-language comparison

Same table, queries and protocol mode, comparing asyncmy with
**go-sql-driver/mysql** and **mysql_async**. Times are the **median of 504
samples per runtime/case**, collected over **72 rounds** with balanced client
order. Every measured sample is retained.

| Workload | asyncmy asyncio | asyncmy uvloop | Go | Rust |
| --- | ---: | ---: | ---: | ---: |
| 50k rows × 8 integer columns, text | **12.94** | 13.95 | 19.12 | 18.01 |
| 50k rows × 8 integer columns, binary | 9.97 | 9.74 | 12.64 | **9.60** |
| 50k rows × 10 mixed columns, text | 36.71 | 41.86 | 33.73 | **33.41** |
| 50k rows × 10 mixed columns, binary | 23.56 | 23.55 | 25.49 | **20.43** |
| Pooled 20×200 point queries, text | 51.24 | 45.03 | 34.87 | **33.49** |
| Pooled 20×200 point queries, binary | 34.68 | **31.11** | 32.06 | 34.12 |

For **pooled binary point queries**, the ordinary Cursor API with uvloop,
`stmt_cache_size=32` and `autocommit=True` leads both native drivers:

| Compared with | Mean per-round throughput advantage | 95% confidence interval |
| --- | ---: | ---: |
| Go | **+2.79%** | +1.65% to +3.88% |
| Rust | **+9.37%** | +8.64% to +10.22% |

These percentages average per-round throughput ratios; they are calculated
separately from the aggregate median times above. Individual rounds overlap:
asyncmy wins 50/72 round medians against Go and 71/72 against Rust. Its average
advantage is positive in 11/12 balanced six-round blocks against Go and all
12 against Rust. The confidence intervals resample those complete blocks.

Both native drivers lead on text point queries, and Rust leads on mixed scans.
The results show where asyncmy leads and where further optimization remains.

### Environment and measurement details

| Component | Version / setting |
| --- | --- |
| Hardware / OS | Apple M4 Max / macOS 27.0.1 arm64 |
| MySQL | 26.10.0, TCP `127.0.0.1:3306` |
| CPython / Cython | 3.15.1 / 3.3.0 |
| uvloop | 0.23.0 |
| Go / go-sql-driver/mysql | 1.27.3 / 1.10.1 |
| Rust / mysql_async / Tokio | 1.99.1 / 0.37.1 / 1.54.1 |

The cross-language measurement uses:

- **Balanced sampling:** all six client-order permutations repeated twelve
  times, with one warmup and seven measured samples per case in each round.
  Clients run sequentially. Python uses one event-loop thread, Rust uses Tokio's
  current-thread runtime, and Go sets `GOMAXPROCS(1)`; helper threads can exist.
- **Pooled point queries:** ten prewarmed connections, 20 workers × 200 queries,
  autocommit enabled. Each query includes acquiring a connection, executing,
  fetching and releasing it. Binary statements are cached per connection;
  asyncmy uses the regular Cursor API with `stmt_cache_size=32`.
- **Complete scan results:** all 50,000 rows are fetched, materialized and
  retained. Binary scans use an explicitly prepared statement, with preparation
  outside timing. Temporal values include a duration for TIME in every language.
  Python constructs `Decimal` values; Go/Rust retain DECIMAL strings, so mixed
  materialization differs. Checksums are validated after timing; Python runs
  cyclic garbage collection before each measured sample.

The measured point-query settings are opt-in: asyncmy defaults to
`autocommit=False` and `stmt_cache_size=0`. Use transaction settings that match
your application. With uvloop installed, replace `asyncio.run(main())` with
`uvloop.run(main())` after importing `uvloop`.

### What makes asyncmy fast

- **Bulk packet parsing:** complete row packets in the receive buffer are parsed
  in one C loop, reducing packet objects and event-loop round-trips.
- **Direct value decoding:** integers, floats, strings and temporal values decode
  from wire bytes through Cython and the CPython C-API, avoiding intermediate
  strings and general-purpose parsing where possible.
- **Cached binary results:** prepared statements reuse per-connection state;
  complete responses can populate Cursor rows directly after metadata checks.
- **Cheaper pool operations:** healthy checkout avoids coroutine allocation,
  and coalesced notifications reduce waiter scheduling work.
- **Small-query fast paths:** scalar parameters avoid general packet-building
  overhead, while strings without special characters skip escape translation.

See the [full cross-language report](./benchmark/crosslang/README.md) for exact
queries, source hashes, compatibility checks, confidence-interval calculation
and reproduction commands. The [raw cross-language results](./benchmark/crosslang/results.json)
retain all samples and separate earlier toolchains and optimization stages.
The [benchmark overview](./benchmark/README.md) covers both suites.

## Acknowledgments

asyncmy builds on these projects:

- [PyMySQL](https://github.com/PyMySQL/PyMySQL) — pure Python MySQL client
- [aiomysql](https://github.com/aio-libs/aiomysql) — asyncio MySQL driver
- [python-mysql-replication](https://github.com/noplay/python-mysql-replication) — MySQL replication protocol (pure Python, on top of PyMySQL)

## License

[Apache-2.0](./LICENSE)
