"""
Type hint aliases hub
"""

import typing

from datetime import datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type, Union

if hasattr(typing, "TypeAlias"):
    # pylint: disable=no-name-in-module
    from typing import TypeAlias  # type: ignore[attr-defined]
else:
    try:
        from typing_extensions import TypeAlias
    except ModuleNotFoundError:
        # pylint: disable=reimported
        from typing import Any as TypeAlias


if TYPE_CHECKING:
    from google.protobuf.message import Message as ProtoMessage

    # pylint: disable=redefined-builtin
    from .connection import Connection, Session, SocketStream
    from .crud import DatabaseObject, Schema
    from .dbdoc import DbDoc
    from .errors import (
        DatabaseError,
        DataError,
        Error,
        IntegrityError,
        InterfaceError,
        InternalError,
        NotSupportedError,
        OperationalError,
        PoolError,
        ProgrammingError,
        TimeoutError,
    )
    from .expr import ExprParser
    from .protobuf import Message as XdevMessage
    from .result import BaseResult, Column
    from .statement import Statement


StrOrBytes = Union[str, bytes]

BuildScalarTypes = Optional[Union[str, bytes, bool, int, float]]
BuildExprTypes = Union[
    "XdevMessage", "ExprParser", Dict[str, Any], "DbDoc", List, Tuple, BuildScalarTypes
]
ColumnType: TypeAlias = "Column"
ConnectionType: TypeAlias = "Connection"
DatabaseTargetType: TypeAlias = "DatabaseObject"
ErrorClassTypes = Union[
    Type["Error"],
    Type["InterfaceError"],
    Type["DatabaseError"],
    Type["InternalError"],
    Type["OperationalError"],
    Type["ProgrammingError"],
    Type["IntegrityError"],
    Type["DataError"],
    Type["NotSupportedError"],
    Type["PoolError"],
    Type["TimeoutError"],
]
ErrorTypes = Union[
    "Error",
    "InterfaceError",
    "DatabaseError",
    "InternalError",
    "OperationalError",
    "ProgrammingError",
    "IntegrityError",
    "DataError",
    "NotSupportedError",
    "PoolError",
    "TimeoutError",
]
EscapeTypes = Optional[Union[int, float, Decimal, StrOrBytes]]
FieldTypes = Optional[Union[int, float, str, bytes, Decimal, datetime, timedelta]]
MessageType: TypeAlias = "XdevMessage"
ProtobufMessageType: TypeAlias = "ProtoMessage"
ProtobufMessageCextType = Dict[str, Any]
ResultBaseType: TypeAlias = "BaseResult"
SchemaType: TypeAlias = "Schema"
SessionType: TypeAlias = "Session"
SocketType: TypeAlias = "SocketStream"
StatementType: TypeAlias = "Statement"
