# ChangeLog

## 0.2

### 0.2.9

- Added support for SSL context creation via `ssl` parameter using a dictionary containing `mysql_ssl_set` parameters. (#64)
- Fix bug with fallback encoder in the `escape_item()` function. (#65)

### 0.2.8

- Fix sudden loss of float precision. (#56)
- Fix pool `echo` parameter not apply to create connection. (#62)
- Fix replication reconnect.

### 0.2.7

- Fix `No module named 'asyncmy.connection'`.

### 0.2.6

- Fix raise_mysql_exception (#28)
- Implement `read_timeout` and remove `write_timeout` parameters (#44)

### 0.2.5

- Revert `TIME` return `datetime.time` object. (#37)

### 0.2.4

- Fix `escape_string` for enum type. (#30)
- `TIME` return `datetime.time` object.

### 0.2.3

- Fix `escape_sequence`. (#20)
- Fix `connection.autocommit`. (#21)
- Fix `_clear_result`. (#22)

### 0.2.2

- Fix bug. (#18)
- Fix replication error.

### 0.2.1

- Fix `binlogstream` await. (#12)
- Remove `loop` argument. (#15)
- Fix `unix_socket` connect. (#17)

### 0.2.0

- Fix `cursor.close`.

## 0.1

### 0.1.9

- Force int `pool_recycle`.
- Fix `echo` option.
- Fix bug replication and now don't need to connect manual.

### 0.1.8

- Fix pool recycle. (#4)
- Fix async `fetchone`, `fetchall`, and `fetchmany`. (#7)

### 0.1.7

- Fix negative pk. (#2)

### 0.1.6

- Bug fix.

### 0.1.5

- Remove `byte2int` and `int2byte`.
- Fix warning for sql_mode.

### 0.1.4

- Add replication support.

### 0.1.3

- Fix pool.

### 0.1.2

- Fix build error.

### 0.1.1

- Fix build error.

### 0.1.0

- Release first version.
