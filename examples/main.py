import asyncio
import logging
import os

from asyncmy import connect

logging.basicConfig(level=logging.INFO)


async def main():
    conn = await connect(
        user="root",
        password=os.getenv("MYSQL_PASS", "123456"),
        database="test",
        echo=True,
    )
    async with conn.cursor() as cursor:
        await cursor.execute("select 1")
        ret = await cursor.fetchone()
        print(ret)


if __name__ == "__main__":
    asyncio.run(main())
