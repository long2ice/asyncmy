from configparser import RawConfigParser


class Parser(RawConfigParser):
    def __init__(self, **kwargs):
        kwargs["allow_no_value"] = True
        super(Parser, self).__init__(**kwargs)

    def get(self, section, option, **kwargs):
        value = super(Parser, self).get(section, option)
        quotes = ["'", '"']
        for quote in quotes:
            if len(value) >= 2 and value[0] == value[-1] == quote:
                return value[1:-1]
        return value
