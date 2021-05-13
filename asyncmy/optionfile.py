import configparser


class Parser(configparser.RawConfigParser):
    def __init__(self, **kwargs):
        kwargs["allow_no_value"] = True
        configparser.RawConfigParser.__init__(self, **kwargs)

    def _remove_quotes(self, value):
        quotes = ["'", '"']
        for quote in quotes:
            if len(value) >= 2 and value[0] == value[-1] == quote:
                return value[1:-1]
        return value

    def get(self, section, option, **kwargs):
        value = configparser.RawConfigParser.get(self, section, option)
        return self._remove_quotes(value)
