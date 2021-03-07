# asyncmy - A fast asyncio MySQL driver

[![image](https://img.shields.io/pypi/v/asyncmy.svg?style=flat)](https://pypi.python.org/pypi/asyncmy)
[![image](https://img.shields.io/github/license/long2ice/asyncmy)](https://github.com/long2ice/asyncmy)
[![image](https://github.com/long2ice/asyncmy/workflows/pypi/badge.svg)](https://github.com/long2ice/asyncmy/actions?query=workflow:pypi)
[![image](https://github.com/long2ice/asyncmy/workflows/ci/badge.svg)](https://github.com/long2ice/asyncmy/actions?query=workflow:ci)

## Introduction

`asyncmy` is a fast asyncio MySQL driver, which reuse most of [pymysql](https://github.com/PyMySQL/PyMySQL) and rewrite core with [cython](https://cython.org/) to speedup.

## Performance

## Install

Just install from pypi:

```shell
> pip install asyncmy
```

## Usage

### Use `connect`

```py
from asyncmy import connect
import asyncio

async def run():
    conn = await connect()
    async with connection.cursor(cursor=DictCursor) as cursor:
        await cursor.execute("create database if not exists test")
        await cursor.execute(
            """CREATE TABLE if not exists test.asyncmy
    (
        `id`       int primary key auto_increment,
        `decimal`  decimal(10, 2),
        `date`     date,
        `datetime` datetime,
        `float`    float,
        `string`   varchar(200),
        `tinyint`  tinyint
    )"""
        )
if __name__ == '__main__':
    asyncio.run(run())
```

### Use `pool`

```py
from asyncmy import connect
import asyncio

async def run():
    pool = await asyncmy.create_pool()
    async with pool.acquire() as conn:
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT 1")
        ret = cursor.fetchone()
        assert ret == (1,)

if __name__ == '__main__':
    asyncio.run(run())
```

## ThanksTo

- [pymysql](https://github/pymysql/PyMySQL)，a pure python MySQL client.

## License

This project is licensed under the
[Apache-2.0](https://github.com/long2ice/aerich/blob/master/LICENSE) License.
