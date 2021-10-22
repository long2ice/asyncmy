"""Optional support for sqlalchemy.sql dynamic query generation."""
from asyncmy.sa.connection import SAConnection
from .engine import create_engine, Engine

from .exc import (Error, ArgumentError, InvalidRequestError,
                  NoSuchColumnError, ResourceClosedError)
from . import result


__all__ = ('create_engine', 'SAConnection', 'Error',
           'ArgumentError', 'InvalidRequestError', 'NoSuchColumnError',
           'ResourceClosedError', 'Engine', 'result')


(SAConnection, Error, ArgumentError, InvalidRequestError,
 NoSuchColumnError, ResourceClosedError, create_engine, Engine)
