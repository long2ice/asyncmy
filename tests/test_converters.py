import datetime

from asyncmy.converters import convert_datetime, escape_dict, escape_item, escape_str


class CustomDate(datetime.date):
    pass


def test_escape_item():
    assert escape_item("\\\n\r\032\"'foobar\0", "utf-8") == "'\\\\\\n\\r\\Z\\\"\\'foobar\\0'"
    assert escape_item(datetime.date(2023, 6, 2), "utf-8") == "'2023-06-02'"
    assert escape_item(CustomDate(2023, 6, 2), "utf-8") == "'2023-06-02'"


def test_escape_str():
    assert escape_str("\\\n\r\032\"'foobar\0") == "'\\\\\\n\\r\\Z\\\"\\'foobar\\0'"

    # The encoder for the str type is a default encoder,
    # so it should accept values that are not strings as well.
    assert escape_str(datetime.date(2023, 6, 2)) == "'2023-06-02'"
    assert escape_str(CustomDate(2023, 6, 2)) == "'2023-06-02'"


def test_convert_datetime():
    assert convert_datetime("2023-06-02T23:06:20") == datetime.datetime(2023, 6, 2, 23, 6, 20)
    assert convert_datetime("2023-06-02 23:06:20") == datetime.datetime(2023, 6, 2, 23, 6, 20)

    # invalid datetime should be returned as str
    assert convert_datetime("0000-00-00 00:00:00") == "0000-00-00 00:00:00"


def test_escape_dict_keys():
    """Test that dict keys are properly escaped (CVE-2025-65896).

    This test ensures that SQL injection via dict keys is prevented.
    Previously, only dict values were escaped, allowing attackers to
    inject arbitrary SQL via crafted dict keys.
    """
    # Test that keys with SQL injection characters are escaped
    malicious_key = "foo'; DROP TABLE users; --"
    result = escape_dict({malicious_key: "bar"}, "utf-8")
    # The key should be escaped, not contain raw SQL injection
    assert malicious_key not in result
    assert "foo\\'; DROP TABLE users; --" in result

    # Test escaping of various dangerous characters in keys
    result = escape_dict({"key'with\"quotes": "value"}, "utf-8")
    assert "key\\'with\\\"quotes" in result

    # Test backslash escaping in keys
    result = escape_dict({"key\\with\\backslash": "value"}, "utf-8")
    assert "key\\\\with\\\\backslash" in result

    # Test normal dict still works
    result = escape_dict({"name": "test", "id": 123}, "utf-8")
    assert result["name"] == "'test'"
    assert result["id"] == "123"
