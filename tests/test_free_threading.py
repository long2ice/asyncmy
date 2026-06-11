import os
import subprocess
import sys
import sysconfig
import textwrap
from concurrent.futures import ThreadPoolExecutor

import pytest

from asyncmy.converters import convert_datetime, convert_timedelta, escape_item
from asyncmy.protocol import MysqlPacket


@pytest.mark.skipif(
    not sysconfig.get_config_var("Py_GIL_DISABLED"),
    reason="requires a free-threaded CPython build",
)
def test_imports_do_not_enable_gil_in_free_threaded_python():
    code = textwrap.dedent(
        """
        import sys
        import asyncmy.charset
        import asyncmy.connection
        import asyncmy.converters
        import asyncmy.cursors
        import asyncmy.errors
        import asyncmy.pool
        import asyncmy.protocol
        assert not sys._is_gil_enabled()
        """
    )
    env = os.environ.copy()
    env.pop("PYTHON_GIL", None)
    env["PYTHONNOUSERSITE"] = "1"
    subprocess.run([sys.executable, "-c", code], check=True, env=env)


@pytest.mark.skipif(
    not sysconfig.get_config_var("Py_GIL_DISABLED"),
    reason="requires a free-threaded CPython build",
)
def test_converter_and_protocol_paths_run_from_multiple_threads():
    assert not sys._is_gil_enabled()
    packet_data = b"\x03abc\x01x\xfc\x05\x00"

    def worker(iterations: int) -> int:
        total = 0
        for i in range(iterations):
            total += len(escape_item({"k": i, "v": "text"}, "utf8mb4")["v"])
            total += convert_datetime("2026-06-11 12:34:56").second
            total += convert_timedelta("12:34:56").seconds
            packet = MysqlPacket(packet_data, "utf8mb4")
            total += len(packet.read_length_coded_string())
            total += len(packet.read_length_coded_string())
            total += packet.read_length_encoded_integer()
        return total

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(worker, [1000] * 4))

    assert results == [results[0]] * 4
