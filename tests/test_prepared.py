import datetime
import weakref
from decimal import Decimal
from uuid import uuid4

import pytest

from asyncmy import errors
from asyncmy.connection import PreparedStatement


@pytest.mark.asyncio
async def test_prepared_statement_remains_extensible(connection, monkeypatch):
    original_execute = PreparedStatement.execute
    calls = []

    async def execute(self, args=()):
        calls.append(args)
        return await original_execute(self, args)

    monkeypatch.setattr(PreparedStatement, "execute", execute)
    stmt = await connection.prepare("SELECT ?")
    try:
        stmt.tag = "custom attribute"
        assert weakref.ref(stmt)() is stmt
        assert stmt.tag == "custom attribute"
        assert (await stmt.execute((42,))).rows == ((42,),)
        assert calls == [(42,)]
    finally:
        await stmt.close()


@pytest.mark.asyncio
async def test_prepared_type_roundtrip(connection):
    stmt = await connection.prepare("SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?")
    args = (
        None,
        True,
        -12345,
        2**63 - 1,
        2**64 - 1,
        3.25,
        "中文 with 'quotes' and \\backslash\0nul",
        b"\x00\x01\xff binary",
        datetime.datetime(2021, 3, 4, 5, 6, 7, 123456),
        datetime.date(2020, 2, 29),
        datetime.timedelta(hours=-25, minutes=-6, seconds=-17),
        Decimal("12345.6789"),
    )
    result = await stmt.execute(args)
    row = result.rows[0]
    assert row[0] is None
    assert row[1] == 1
    assert row[2] == -12345
    assert row[3] == 2**63 - 1
    assert row[4] == 2**64 - 1
    assert row[5] == 3.25
    assert row[6] == args[6]
    assert row[7] == args[7]
    assert str(row[8]).startswith("2021-03-04 05:06:07.123456")
    assert str(row[9]) == "2020-02-29"
    assert row[10] == datetime.timedelta(hours=-25, minutes=-6, seconds=-17)
    assert row[11] == Decimal("12345.6789")
    await stmt.close()


@pytest.mark.asyncio
async def test_prepared_table_roundtrip(connection):
    async with connection.cursor() as cur:
        await cur.execute("DROP TABLE IF EXISTS test.bin_prepared")
        await cur.execute(
            """CREATE TABLE test.bin_prepared (
            id INT PRIMARY KEY AUTO_INCREMENT,
            ti TINYINT, tiu TINYINT UNSIGNED, si SMALLINT, mi MEDIUMINT,
            i INT, bi BIGINT, biu BIGINT UNSIGNED, yr YEAR,
            f FLOAT, d DOUBLE, dec_ DECIMAL(20,6),
            s VARCHAR(100), b VARBINARY(100), t TEXT, bl BLOB,
            dt DATETIME(6), da DATE, tm TIME(6), js JSON
            )"""
        )
    try:
        ins = await connection.prepare(
            "INSERT INTO test.bin_prepared"
            " (ti,tiu,si,mi,i,bi,biu,yr,f,d,dec_,s,b,t,bl,dt,da,tm,js)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        )
        result = await ins.execute(
            (
                -128,
                255,
                -32768,
                8388607,
                -2147483648,
                -(2**63),
                2**64 - 1,
                2024,
                1.5,
                2.25,
                Decimal("999999999999.123456"),
                "varchar 值",
                b"\x00\xff",
                "text value",
                b"blob\x01",
                datetime.datetime(2021, 12, 31, 23, 59, 59, 999999),
                datetime.date(1999, 1, 1),
                datetime.timedelta(hours=100, minutes=30, seconds=15, microseconds=250000),
                '{"k": [1, 2, "v"]}',
            )
        )
        assert result.affected_rows == 1
        assert result.insert_id == 1
        # NULL bitmap covering all 19 parameters / 20 result columns
        result = await ins.execute((None,) * 19)
        assert result.insert_id == 2
        await ins.close()

        async with await connection.prepare("SELECT * FROM test.bin_prepared ORDER BY id") as sel:
            result = await sel.execute()
            cached = await sel.execute()
            assert cached.rows == result.rows
            result = cached
        r1, r2 = result.rows
        assert r1[1:9] == (-128, 255, -32768, 8388607, -2147483648, -(2**63), 2**64 - 1, 2024)
        assert r1[9] == 1.5 and r1[10] == 2.25
        assert r1[11] == Decimal("999999999999.123456")
        assert r1[12] == "varchar 值" and r1[13] == b"\x00\xff"
        assert r1[14] == "text value" and r1[15] == b"blob\x01"
        assert r1[16] == datetime.datetime(2021, 12, 31, 23, 59, 59, 999999)
        assert r1[17] == datetime.date(1999, 1, 1)
        assert r1[18] == datetime.timedelta(hours=100, minutes=30, seconds=15, microseconds=250000)
        assert all(v is None for v in r2[1:])
        assert result.description[1][0] == "ti"
    finally:
        async with connection.cursor() as cur:
            await cur.execute("DROP TABLE IF EXISTS test.bin_prepared")


@pytest.mark.asyncio
async def test_prepared_reuse_and_interleave(connection):
    stmt = await connection.prepare("SELECT ? + ?")
    for i in range(100):
        result = await stmt.execute((i, 1))
        assert result.rows[0][0] == i + 1
        # text protocol interleaved on the same connection
        if i % 10 == 0:
            async with connection.cursor() as cur:
                await cur.execute("SELECT 1")
                assert (await cur.fetchone()) == (1,)
    await stmt.close()


@pytest.mark.asyncio
async def test_prepared_errors(connection):
    with pytest.raises(errors.ProgrammingError):
        await connection.prepare("SELECT * FROM test.no_such_table_xyz")

    stmt = await connection.prepare("SELECT ?")
    with pytest.raises(errors.ProgrammingError):
        await stmt.execute((1, 2))  # parameter count mismatch
    await stmt.close()
    with pytest.raises(errors.ProgrammingError):
        await stmt.execute((1,))  # closed statement


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query,values",
    [
        ("SELECT ?", [1, 2**64 - 1, -1, 3.5]),
        ("SELECT ?", ["x", "longer string", "中文", None]),
        ("SELECT CAST(? AS BINARY)", [b"\xff", b"\x00\xfflonger"]),
        ("SELECT ?", [Decimal("1.25"), Decimal("12345.6789")]),
        ("SELECT ?", [None, 1, None, -1, 2**64 - 1]),
    ],
)
async def test_cached_prepared_metadata_tracks_parameter_type_changes(connection, query, values):
    async with await connection.prepare(query) as stmt:
        for value in values:
            result = await stmt.execute((value,))
            assert result.rows == ((value,),)
            assert (await stmt.execute((value,))).rows == result.rows


@pytest.mark.asyncio
async def test_cached_prepared_drains_multiple_result_sets(connection):
    name = "test.cached_results_" + uuid4().hex
    async with connection.cursor() as cur:
        await cur.execute(
            f"CREATE PROCEDURE {name}(IN p INT) BEGIN SELECT p AS value; SELECT p+1; END"
        )
    try:
        async with await connection.prepare(f"CALL {name}(?)") as stmt:
            for value in [1, 2, None]:
                result = await stmt.execute((value,))
                assert result.rows == ((value,),)
                assert not result.has_next
                async with connection.cursor() as cur:
                    await cur.execute("SELECT 99")
                    assert await cur.fetchone() == (99,)
    finally:
        async with connection.cursor() as cur:
            await cur.execute(f"DROP PROCEDURE {name}")
