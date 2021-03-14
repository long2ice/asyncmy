class TableMetadataUnavailableError(Exception):
    """
    raise when table metadata is unavailable
    """


class BinLogNotEnabledError(Exception):
    """
    raise when binlog not enabled
    """


class StreamClosedError(Exception):
    """raise when stream is closed"""
