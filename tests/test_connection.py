import re

import pytest

from asyncmy.connections import Connection


@pytest.mark.asyncio
async def test_connect():
    connection = Connection(user="root", password="123456")
    await connection.connect()
    assert connection._connected
    assert re.match(
        r"\d\.\d\.\d",
        connection.get_server_info(),
    )
    assert connection.get_proto_info() == 10
    assert connection.get_host_info() != "Not Connected"
