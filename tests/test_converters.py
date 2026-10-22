import datetime

import pytest

from asyncmy.converters import convert_datetime, escape_item, escape_str, escape_timedelta


class CustomDate(datetime.date):
    pass


def test_escape_item():
    assert escape_item("\\\n\r\032\"'foobar\0", "utf-8") == "'\\\\\\n\\r\\Z\\\"\\'foobar\\0'"
    assert escape_item(datetime.date(2023, 6, 2), "utf-8") == "'2023-06-02'"
    assert escape_item(CustomDate(2023, 6, 2), "utf-8") == "'2023-06-02'"


def test_escape_str():
    assert escape_str("\\\n\r\032\"'foobar\0") == "'\\\\\\n\\r\\Z\\\"\\'foobar\\0'"

    # The encoder for the str type is a default encoder,
    # so it should accept values that are not strings as well.
    assert escape_str(datetime.date(2023, 6, 2)) == "'2023-06-02'"
    assert escape_str(CustomDate(2023, 6, 2)) == "'2023-06-02'"


def test_convert_datetime():
    assert convert_datetime("2023-06-02T23:06:20") == datetime.datetime(2023, 6, 2, 23, 6, 20)
    assert convert_datetime("2023-06-02 23:06:20") == datetime.datetime(2023, 6, 2, 23, 6, 20)

    # invalid datetime should be returned as str
    assert convert_datetime("0000-00-00 00:00:00") == "0000-00-00 00:00:00"


def test_escape_unsigned_big_int():
    # unsigned BIGINT max used to overflow the C `long long` parameter (#35)
    assert escape_item(2**64 - 1, "utf-8") == str(2**64 - 1)
    assert escape_item(-(2**63), "utf-8") == str(-(2**63))
    assert escape_item(0, "utf-8") == str(0)


@pytest.mark.parametrize(
    "value, expected",
    [
        (datetime.timedelta(), "'00:00:00'"),
        (datetime.timedelta(microseconds=1), "'00:00:00.000001'"),
        (datetime.timedelta(microseconds=-1), "'-00:00:00.000001'"),
        (datetime.timedelta(seconds=-1), "'-00:00:01'"),
        (datetime.timedelta(seconds=-62), "'-00:01:02'"),
        (datetime.timedelta(hours=-1), "'-01:00:00'"),
        (datetime.timedelta(hours=-1, microseconds=1), "'-00:59:59.999999'"),
        (datetime.timedelta(days=-1), "'-24:00:00'"),
        (
            datetime.timedelta(days=-2, seconds=3, microseconds=456789),
            "'-47:59:56.543211'",
        ),
        (datetime.timedelta(days=2, seconds=3, microseconds=456789), "'48:00:03.456789'"),
        (datetime.timedelta.min, "'-23999999976:00:00'"),
        (datetime.timedelta.max, "'23999999999:59:59.999999'"),
    ],
)
def test_escape_timedelta(value, expected):
    assert escape_timedelta(value) == expected
    assert escape_item(value, "utf-8") == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value",
    [
        datetime.timedelta(microseconds=-1),
        datetime.timedelta(seconds=-62),
        datetime.timedelta(hours=-1, microseconds=1),
        datetime.timedelta(days=-2, seconds=3, microseconds=456789),
        datetime.timedelta(days=2, seconds=3, microseconds=456789),
    ],
)
async def test_timedelta_binding_round_trip(connection, value):
    async with connection.cursor() as cursor:
        await cursor.execute("CREATE TEMPORARY TABLE test.timedelta_round_trip (duration TIME(6))")
        try:
            await cursor.execute("INSERT INTO test.timedelta_round_trip VALUES (%s)", (value,))
            await cursor.execute("SELECT duration FROM test.timedelta_round_trip")
            assert await cursor.fetchone() == (value,)
        finally:
            await cursor.execute("DROP TEMPORARY TABLE test.timedelta_round_trip")
