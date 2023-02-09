"""
Type hint aliases hub
"""
import os
import typing

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from time import struct_time
from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

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
    from .custom_types import HexLiteral
    from .network import MySQLSocket


StrOrBytes = Union[str, bytes]
StrOrBytesPath = Union[StrOrBytes, os.PathLike]
SocketType: TypeAlias = "MySQLSocket"


""" Conversion """
ToPythonOutputTypes = Optional[
    Union[
        float,
        int,
        Decimal,
        StrOrBytes,
        date,
        timedelta,
        datetime,
        Set[str],
    ]
]
ToMysqlInputTypes = Optional[
    Union[
        int,
        float,
        Decimal,
        StrOrBytes,
        bool,
        datetime,
        date,
        time,
        struct_time,
        timedelta,
    ]
]
ToMysqlOutputTypes = Optional[Union[int, float, bytes, "HexLiteral"]]


""" Protocol """
HandShakeType = Dict[str, Optional[Union[int, StrOrBytes]]]
OkPacketType = Dict[str, Optional[Union[int, str]]]
EofPacketType = OkPacketType
StatsPacketType = Dict[str, Union[int, Decimal]]
SupportedMysqlBinaryProtocolTypes = Optional[
    Union[int, StrOrBytes, Decimal, float, datetime, date, timedelta, time]
]
QueryAttrType = List[
    # 2-Tuple: (str, attr_types)
    Tuple[str, SupportedMysqlBinaryProtocolTypes]
]
ParseValueFromBinaryResultPacketTypes = Optional[
    Union[
        int,
        float,
        Decimal,
        date,
        datetime,
        timedelta,
        str,
    ]
]
DescriptionType = Tuple[
    # Sometimes it can be represented as a 2-Tuple of the form:
    # Tuple[str, int],  # field name, field type,
    # but we will stick with the 9-Tuple format produced by
    # the protocol module.
    str,  # field name
    int,  # field type
    None,  # you can ignore it or take a look at protocol.parse_column()
    None,
    None,
    None,
    Union[bool, int],  # null ok
    int,  # field flags
    int,  # MySQL charset ID
]


""" Connection """
ConnAttrsType = Dict[str, Optional[Union[str, Tuple[str, str]]]]
ResultType = Mapping[
    str, Optional[Union[int, str, EofPacketType, List[DescriptionType]]]
]


""" Connection C-EXT """
CextEofPacketType = Dict[str, int]
CextResultType = Dict[str, Union[CextEofPacketType, List[DescriptionType]]]


""" Cursor """
ParamsSequenceType = Sequence[ToMysqlInputTypes]
ParamsDictType = Dict[str, ToMysqlInputTypes]
ParamsSequenceOrDictType = Union[ParamsDictType, ParamsSequenceType]
RowType = Tuple[ToPythonOutputTypes, ...]
WarningType = Tuple[str, int, str]
