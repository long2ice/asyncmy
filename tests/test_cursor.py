import time

import pytest

from asyncmy.connections import Connection


@pytest.mark.asyncio
async def test_query_10k():
    t = time.time()
    connection = Connection(user="root", password="123456")
    await connection.connect()
    with connection.cursor() as cursor:
        for _ in range(100000):
            await cursor.execute("SELECT 1,2,3,4,5")
            res = cursor.fetchall()
            assert len(res) == 1
            assert res[0] == (1, 2, 3, 4, 5)
    print(time.time() - t)
