from configparser import RawConfigParser


class Parser(RawConfigParser):
    def __init__(self, **kwargs):
        kwargs["allow_no_value"] = True
        super().__init__(**kwargs)

    def get(self, section, option, **kwargs):
        value = super().get(section, option)
        quotes = ("'", '"')
        if len(value) >= 2 and value[0] == value[-1] and value[0] in quotes:
            return value[1:-1]
        return value
