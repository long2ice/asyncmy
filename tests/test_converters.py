import datetime

from asyncmy.converters import escape_item, escape_str


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
