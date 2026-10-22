import argparse
import asyncio
import datetime
import gc
import json
import statistics
import time
import asyncmy
import uvloop

KW = dict(host="127.0.0.1", port=3306, user="root", password="123456", db="test", autocommit=True)
QUERIES = {"integer": "SELECT a,b,a,b,a,b,a,b FROM test.bench", "mixed": "SELECT * FROM test.bench"}


async def measure(runtime, case, fn):
    await fn()
    times = []
    for _ in range(7):
        gc.collect()
        start = time.perf_counter()
        result = await fn()
        times.append((time.perf_counter() - start) * 1000)
        if case.startswith("scan"):
            assert len(result) == 50000
            assert sum(row[0 if "integer" in case else 1] for row in result) == 1249975000
        else:
            assert result == 0
        del result
    print(
        json.dumps(
            dict(
                runtime=runtime,
                case=case,
                median_ms=round(statistics.median(times), 3),
                samples_ms=[round(v, 3) for v in times],
            )
        ),
        flush=True,
    )


async def initialize_data():
    conn = await asyncmy.connect(**{key: value for key, value in KW.items() if key != "db"})
    try:
        async with conn.cursor() as cur:
            await cur.execute("CREATE DATABASE IF NOT EXISTS test")
            await cur.execute(
                "CREATE TABLE IF NOT EXISTS test.bench (id INT PRIMARY KEY AUTO_INCREMENT,"
                " a INT, b BIGINT, c DOUBLE, d DECIMAL(12,4), s1 VARCHAR(64),"
                " s2 VARCHAR(255), dt DATETIME(6), da DATE, tm TIME)"
            )
            await cur.execute("SELECT COUNT(*) FROM test.bench")
            count = (await cur.fetchone())[0]
            if count == 50000:
                return
            if count:
                raise RuntimeError("test.bench must be empty or contain the benchmark's 50000 rows")
            rows = [
                (
                    i + 1,
                    i,
                    i * 1000000007,
                    i * 0.31415,
                    "%d.%04d" % (i, i % 10000),
                    "user-name-%08d" % i,
                    "payload-" + "z" * (i % 120),
                    datetime.datetime(2021, 3, 4, 5, 6, i % 60, i % 1000000),
                    datetime.date(2000 + i % 25, 1 + i % 12, 1 + i % 28),
                    datetime.timedelta(hours=i % 120, minutes=i % 60, seconds=i % 60),
                )
                for i in range(50000)
            ]
            await cur.executemany(
                "INSERT INTO test.bench (id,a,b,c,d,s1,s2,dt,da,tm)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                rows,
            )
    finally:
        await conn.ensure_closed()


async def main(runtime):
    conn = await asyncmy.connect(**KW)
    try:
        for kind, query in QUERIES.items():
            for mode in ["text", "binary"]:
                stmt = await conn.prepare(query) if mode == "binary" else None

                async def scan():
                    if stmt:
                        rows = (await stmt.execute()).rows
                    else:
                        async with conn.cursor() as cur:
                            await cur.execute(query)
                            rows = await cur.fetchall()
                    assert len(rows) == 50000
                    return rows

                await measure(runtime, "scan_" + kind + "_" + mode, scan)
                if stmt:
                    await stmt.close()
    finally:
        await conn.ensure_closed()
    for mode in ["text", "binary"]:
        pool = await asyncmy.create_pool(
            minsize=10, maxsize=10, stmt_cache_size=32 if mode == "binary" else 0, **KW
        )

        async def worker():
            checksum = 0
            for _ in range(200):
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT a FROM test.bench WHERE id=%s", (1,))
                        checksum += (await cur.fetchone())[0]
            return checksum

        async def points():
            return sum(await asyncio.gather(*(worker() for _ in range(20))))

        try:
            await measure(runtime, "point_" + mode, points)
        finally:
            pool.close()
            await pool.wait_closed()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--loop", choices=["all", "asyncio", "uvloop"], default="all")
    parser.add_argument("--setup", action="store_true", help="Populate an empty test.bench table")
    args = parser.parse_args()
    if args.setup:
        asyncio.run(initialize_data())
    mode = args.loop
    if mode in ("all", "asyncio"):
        asyncio.run(main("asyncmy_asyncio"))
    if mode in ("all", "uvloop"):
        uvloop.run(main("asyncmy_uvloop"))
