import asyncio

from asyncmy import connect
from asyncmy.replication import BinLogStream
from asyncmy.replication.row_events import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
from conftest import connection_kwargs


async def main():
    conn = await connect(**connection_kwargs)
    ctl_conn = await connect(**connection_kwargs)

    stream = BinLogStream(
        conn,
        ctl_conn,
        1,
        resume_stream=True,
        blocking=True,
        only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
    )
    async for event in stream:
        print(event.schema, event.table, event.rows)


if __name__ == "__main__":
    asyncio.run(main())
