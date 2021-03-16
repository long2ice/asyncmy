import pytest

from asyncmy import connect
from asyncmy.replication import BinLogStream
from asyncmy.replication.row_events import WriteRowsEvent
from conftest import connection_kwargs


@pytest.mark.skip(reason="need test in local")
@pytest.mark.asyncio
async def test_binlogstream(connection):
    conn = await connect(**connection_kwargs)

    stream = BinLogStream(
        connection,
        conn,
        1,
        master_log_file="binlog.000172",
        resume_stream=True,
        blocking=True,
        master_log_position=2235312,
    )
    await stream.connect()
    async for event in stream:
        if isinstance(event, WriteRowsEvent):
            print(event.schema, event.table, event.rows)
