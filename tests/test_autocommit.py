import pytest


@pytest.mark.asyncio
async def test_autocommit(connection):
    await connection.autocommit(True)
    cursor = connection.cursor()
    await cursor.execute("SELECT @@autocommit;")
    assert await cursor.fetchone() == (1,)
    await cursor.close()
    await connection.autocommit(False)
    cursor = connection.cursor()
    await cursor.execute("SELECT @@autocommit;")
    assert await cursor.fetchone() == (0,)
