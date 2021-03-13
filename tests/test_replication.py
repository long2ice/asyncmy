import pytest

from asyncmy.replication.binlogstream import BinLogStream


@pytest.mark.asyncio
async def test_binlogstream(connection):
    stream = BinLogStream(connection, 1)
    await stream.connect()
    async for data in stream:
        print(data)
