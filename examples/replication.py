import asyncio

from asyncmy import connect
from asyncmy.replication import BinLogStream
from asyncmy.replication.row_events import WriteRowsEvent
from conftest import connection_kwargs


async def main():
    conn = await connect(**connection_kwargs)
    ctl_conn = await connect(**connection_kwargs)

    stream = BinLogStream(
        conn,
        ctl_conn,
        1,
        master_log_file="binlog.000020",
        master_log_position=405886343,
        resume_stream=True,
        blocking=True,
    )
    async for event in stream:
        if isinstance(event, WriteRowsEvent):
            print(event.schema, event.table, event.rows)


if __name__ == "__main__":
    asyncio.run(main())
