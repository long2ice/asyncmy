import pytest

from asyncmy.connections import Connection


@pytest.mark.asyncio
async def test_connect():
    connection = Connection()
    await connection.connect()
