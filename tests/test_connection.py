import re

import pytest

from asyncmy.connection import Connection
from conftest import connection_kwargs


@pytest.mark.asyncio
async def test_connect():
    connection = Connection(**connection_kwargs)
    await connection.connect()
    assert connection._connected
    assert re.match(
        r"\d\.\d\.\d",
        connection.get_server_info(),
    )
    assert connection.get_proto_info() == 10
    assert connection.get_host_info() != "Not Connected"
