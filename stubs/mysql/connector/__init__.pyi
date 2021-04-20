from .abstracts import MySQLConnectionAbstract as MySQLConnectionAbstract
from .connection import MySQLConnection as MySQLConnection
from .connection_cext import CMySQLConnection as CMySQLConnection
from .constants import (
    CharacterSet as CharacterSet,
    ClientFlag as ClientFlag,
    FieldFlag as FieldFlag,
    FieldType as FieldType,
    RefreshOption as RefreshOption,
)
from .dbapi import (
    BINARY as BINARY,
    Binary as Binary,
    DATETIME as DATETIME,
    Date as Date,
    DateFromTicks as DateFromTicks,
    NUMBER as NUMBER,
    ROWID as ROWID,
    STRING as STRING,
    Time as Time,
    TimeFromTicks as TimeFromTicks,
    Timestamp as Timestamp,
    TimestampFromTicks as TimestampFromTicks,
    apilevel as apilevel,
    paramstyle as paramstyle,
    threadsafety as threadsafety,
)
from .errors import (
    DataError as DataError,
    DatabaseError as DatabaseError,
    Error as Error,
    IntegrityError as IntegrityError,
    InterfaceError as InterfaceError,
    InternalError as InternalError,
    NotSupportedError as NotSupportedError,
    OperationalError as OperationalError,
    ProgrammingError as ProgrammingError,
    Warning as Warning,
    custom_error_exception as custom_error_exception,
)
from typing import Any

HAVE_CEXT: bool

def connect(*args: Any, **kwargs: Any) -> MySQLConnectionAbstract: ...

Connect = connect
