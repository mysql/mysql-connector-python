# Copyright (c) 2016, 2022, Oracle and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0, as
# published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an
# additional permission to link the program and your derivative works
# with the separately licensed software that they have included with
# MySQL.
#
# Without limiting anything contained in the foregoing, this file,
# which is part of MySQL Connector/Python, is also subject to the
# Universal FOSS Exception, version 1.0, a copy of which can be found at
# http://oss.oracle.com/licenses/universal-foss-exception.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

"""Implementation of the Result classes."""

from __future__ import annotations

import decimal
import struct
import sys

from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from .charsets import MYSQL_CHARACTER_SETS
from .dbdoc import DbDoc
from .helpers import decode_from_bytes, deprecated
from .types import ConnectionType, FieldTypes


# pylint: disable=missing-class-docstring,missing-function-docstring
def from_protobuf(column: Column, payload: bytes) -> Any:
    if len(payload) == 0:
        return None

    if column.get_type() == ColumnType.STRING:
        return decode_from_bytes(payload[:-1])  # Strip trailing char

    try:
        return ColumnProtoType.converter_map[column.get_proto_type()](payload)
    except KeyError as err:
        sys.stderr.write(f"{err}")
        sys.stderr.write(f"{payload.encode('hex')}")  # type: ignore[attr-defined]
        return None


def bytes_from_protobuf(payload: bytes) -> bytes:
    # Strip trailing char
    return payload[:-1]


def float_from_protobuf(payload: bytes) -> float:
    assert len(payload) == 4
    return struct.unpack("<f", payload)[0]


def double_from_protobuf(payload: bytes) -> float:
    assert len(payload) == 8
    return struct.unpack("<d", payload)[0]


def varint_from_protobuf_stream(payload: bytes) -> Tuple[int, bytes]:
    if len(payload) == 0:
        raise ValueError("Payload is empty")

    cur = 0
    i = 0
    shift = 0

    for item in payload:
        char = item if isinstance(item, int) else ord(item)  # type: ignore[arg-type]
        eos = (char & 0x80) == 0
        cur_bits = char & 0x7F
        cur_bits <<= shift
        i |= cur_bits
        if eos:
            return i, payload[cur + 1 :]
        cur += 1
        shift += 7

    raise EOFError("Payload too short")


def varint_from_protobuf(payload: bytes) -> int:
    i, payload = varint_from_protobuf_stream(payload)
    if len(payload) != 0:
        raise ValueError("Payload too long")

    return i


def varsint_from_protobuf(payload: bytes) -> int:
    i, payload = varint_from_protobuf_stream(payload)
    if len(payload) != 0:
        raise ValueError("Payload too long")

    # Zigzag encoded, revert it
    if i & 0x1:
        i = ~i
        i = i >> 1
        i |= 1 << 63
    else:
        i = i >> 1

    return i


def set_from_protobuf(payload: bytes) -> List[bytes]:
    set_pb: List = []
    while True:
        try:
            field_len, payload = varint_from_protobuf_stream(payload)
            if len(payload) < field_len:
                if len(payload) == 0 and field_len == 1 and len(set_pb) == 0:
                    # Special case for empty set
                    return []
                raise ValueError("Invalid Set encoding")

            set_pb.append(payload[:field_len])
            payload = payload[field_len:]
            if len(payload) == 0:
                # Done
                break
        except ValueError:
            break
    return set_pb


def decimal_from_protobuf(payload: bytes) -> decimal.Decimal:
    digits = []
    sign = None
    scale = payload[0] if isinstance(payload[0], int) else ord(payload[0])  # type: ignore[arg-type]
    payload = payload[1:]

    for item in payload:
        char = item if isinstance(item, int) else ord(item)  # type: ignore[arg-type]
        high_bcd = (char & 0xF0) >> 4
        low_bcd = char & 0x0F
        if high_bcd < 0x0A:
            digits.append(high_bcd)
            if low_bcd < 0x0A:
                digits.append(low_bcd)
            elif low_bcd == 0x0C:
                sign = 0
                break
            elif low_bcd == 0x0D:
                sign = 1
                break
            else:
                raise ValueError("Invalid BCD")
        elif high_bcd == 0x0C:
            sign = 0
            assert low_bcd == 0x00
            break
        elif high_bcd == 0x0D:
            sign = 1
            assert low_bcd == 0x00
            break
        else:
            raise ValueError(f"Invalid BCD: {high_bcd}")

    return decimal.Decimal((sign, digits, -scale))


def datetime_from_protobuf(payload: bytes) -> datetime:
    # A sequence of varints
    hour = 0
    minutes = 0
    seconds = 0
    useconds = 0
    year, payload = varint_from_protobuf_stream(payload)
    month, payload = varint_from_protobuf_stream(payload)
    day, payload = varint_from_protobuf_stream(payload)

    try:
        hour, payload = varint_from_protobuf_stream(payload)
        minutes, payload = varint_from_protobuf_stream(payload)
        seconds, payload = varint_from_protobuf_stream(payload)
        useconds, payload = varint_from_protobuf_stream(payload)
    except ValueError:
        pass

    return datetime(year, month, day, hour, minutes, seconds, useconds)


def time_from_protobuf(payload: bytes) -> timedelta:
    # A sequence of varints
    hour = 0
    minutes = 0
    seconds = 0
    useconds = 0
    negate = payload[0] == 1
    payload = payload[1:]

    try:
        hour, payload = varint_from_protobuf_stream(payload)
        minutes, payload = varint_from_protobuf_stream(payload)
        seconds, payload = varint_from_protobuf_stream(payload)
        useconds, payload = varint_from_protobuf_stream(payload)
    except ValueError:
        pass

    if negate:
        # Negate the first non-zero value
        if hour:
            hour *= -1
        elif minutes:
            minutes *= -1
        elif seconds:
            seconds *= -1
        elif useconds:
            useconds *= -1

    return timedelta(
        hours=hour, minutes=minutes, seconds=seconds, microseconds=useconds
    )


class Collations:
    UTF8_GENERAL_CI = 33


class ColumnType:
    BIT = 1
    TINYINT = 2
    SMALLINT = 3
    MEDIUMINT = 4
    INT = 5
    BIGINT = 6
    REAL = 7
    FLOAT = 8
    DECIMAL = 9
    NUMERIC = 10
    DOUBLE = 11
    JSON = 12
    STRING = 13
    BYTES = 14
    TIME = 15
    DATE = 16
    DATETIME = 17
    TIMESTAMP = 18
    SET = 19
    ENUM = 20
    GEOMETRY = 21
    XML = 22
    YEAR = 23
    CHAR = 24
    VARCHAR = 25
    BINARY = 26
    VARBINARY = 27
    TINYBLOB = 28
    BLOB = 29
    MEDIUMBLOB = 30
    LONGBLOB = 31
    TINYTEXT = 32
    TEXT = 33
    MEDIUMTEXT = 34
    LONGTEXT = 35

    @classmethod
    def to_string(cls, needle: Any) -> Optional[str]:
        for key, value in vars(cls).items():
            if value == needle:
                return key
        return None

    @classmethod
    def from_string(cls, key: str) -> Any:
        return getattr(cls, key.upper(), None)

    @classmethod
    def is_char(cls, col_type: int) -> bool:
        return col_type in (
            cls.CHAR,
            cls.VARCHAR,
        )

    @classmethod
    def is_binary(cls, col_type: int) -> bool:
        return col_type in (
            cls.BINARY,
            cls.VARBINARY,
        )

    @classmethod
    def is_text(cls, col_type: int) -> bool:
        return col_type in (
            cls.TEXT,
            cls.TINYTEXT,
            cls.MEDIUMTEXT,
            cls.LONGTEXT,
        )

    @classmethod
    def is_decimals(cls, col_type: int) -> bool:
        return col_type in (
            cls.REAL,
            cls.DOUBLE,
            cls.FLOAT,
            cls.DECIMAL,
            cls.NUMERIC,
        )

    @classmethod
    def is_numeric(cls, col_type: int) -> bool:
        return col_type in (
            cls.BIT,
            cls.TINYINT,
            cls.SMALLINT,
            cls.MEDIUMINT,
            cls.INT,
            cls.BIGINT,
        )

    @classmethod
    def is_finite_set(cls, col_type: int) -> bool:
        return col_type in (
            cls.SET,
            cls.ENUM,
        )


class ColumnProtoType:
    SINT = 1
    UINT = 2
    DOUBLE = 5
    FLOAT = 6
    BYTES = 7
    TIME = 10
    DATETIME = 12
    SET = 15
    ENUM = 16
    BIT = 17
    DECIMAL = 18

    converter_map: Dict[int, Callable[[bytes], Any]] = {
        SINT: varsint_from_protobuf,
        UINT: varint_from_protobuf,
        BYTES: bytes_from_protobuf,
        DATETIME: datetime_from_protobuf,
        TIME: time_from_protobuf,
        FLOAT: float_from_protobuf,
        DOUBLE: double_from_protobuf,
        BIT: varint_from_protobuf,
        SET: set_from_protobuf,
        ENUM: bytes_from_protobuf,
        DECIMAL: decimal_from_protobuf,
    }


class Flags:
    def __init__(self, value: int) -> None:
        self._allowed_flags: Dict[str, int] = {}
        self._flag_names: Dict[int, str] = {}
        for key, val in self.__class__.__dict__.items():
            if key.startswith("__"):
                continue
            if isinstance(val, int):
                self._allowed_flags[key] = val
                self._flag_names[val] = key
        self._value: int = value

    def __str__(self) -> str:
        mask = 1
        flag_names = []
        value = self._value

        for _ in range(0, 63):
            mask <<= 1
            flag = value & mask
            if flag:
                # We matched something, find the name for it
                try:
                    flag_names.append(self._flag_names[flag])
                except KeyError:
                    sys.stderr.write(f"{self._flag_names}")
                    sys.stderr.write(f"{self.__class__.__dict__}")

        return ",".join(flag_names)

    @property
    def value(self) -> int:
        return self._value

    @value.setter
    def value(self, val: int) -> None:
        self._value = val


class ColumnFlags(Flags):
    NOT_NULL = 0x0010
    PRIMARY_KEY = 0x0020
    UNIQUE_KEY = 0x0040
    MULTIPLE_KEY = 0x0080
    AUTO_INCREMENT = 0x0100


class DatetimeColumnFlags(ColumnFlags):
    TIMESTAMP = 0x0001


class UIntColumnFlags(ColumnFlags):
    ZEROFILL = 0x0001


class DoubleColumnFlags(ColumnFlags):
    UNSIGNED = 0x0001


class FloatColumnFlags(ColumnFlags):
    UNSIGNED = 0x0001


class BytesColumnFlags(ColumnFlags):
    RIGHT_PAD = 0x0001


class BytesContentType(ColumnFlags):
    GEOMETRY = 0x0001
    JSON = 0x0002
    XML = 0x0003


# pylint: enable=missing-class-docstring,missing-function-docstring


class Column:
    """Represents meta data for a table column.

    Args:
        col_type (int): The column type.
        catalog (str): The catalog.
        schema (str): The schema name.
        table (str): The table name.
        original_table (str): The original table name.
        name (str): The column name.
        original_name (str): The original table name.
        length (int): The column length,
        collation (str): The collation name.
        fractional_digits (int): The fractional digits.
        flags (int): The flags.
        content_type (int): The content type.

    .. versionchanged:: 8.0.12
    """

    def __init__(
        self,
        col_type: int,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        original_table: Optional[str] = None,
        name: Optional[str] = None,
        original_name: Optional[str] = None,
        length: Optional[int] = None,
        collation: Optional[int] = None,
        fractional_digits: Optional[int] = None,
        flags: Optional[int] = None,
        content_type: Optional[int] = None,
    ) -> None:
        self._schema: str = decode_from_bytes(schema)
        self._name: str = decode_from_bytes(name)
        self._original_name: str = decode_from_bytes(original_name)
        self._table: str = decode_from_bytes(table)
        self._original_table: str = decode_from_bytes(original_table)
        self._proto_type: int = col_type
        self._col_type: Optional[int] = None
        self._catalog: Optional[str] = catalog
        self._length: Optional[int] = length
        self._collation: Optional[int] = collation
        self._fractional_digits: Optional[int] = fractional_digits
        self._flags: Optional[int] = flags
        self._content_type: Optional[int] = content_type
        self._number_signed: bool = False
        self._is_padded: Union[bool, int] = False
        self._is_binary: bool = False
        self._is_bytes: bool = False
        self._collation_name: Optional[str] = None
        self._character_set_name: Optional[str] = None
        self._zero_fill: Optional[int] = None

        if self._collation > 0:
            if self._collation >= len(MYSQL_CHARACTER_SETS):
                raise ValueError(f"No mapping found for collation {self._collation}")
            info = MYSQL_CHARACTER_SETS[self._collation]
            self._character_set_name = info[0]
            self._collation_name = info[1]
            self._is_binary = (
                "binary" in self._collation_name or "_bin" in self._collation_name
            )
        self._map_type()
        self._is_bytes = self._col_type in (
            ColumnType.GEOMETRY,
            ColumnType.JSON,
            ColumnType.XML,
            ColumnType.BYTES,
            ColumnType.STRING,
        )

    def __str__(self) -> str:
        return str(
            {
                "col_type": self._col_type,
                "schema": self._schema,
                "table": self._table,
                "flags": str(self._flags),
            }
        )

    def _map_bytes(self) -> None:
        """Map bytes."""
        if self._content_type == BytesContentType.GEOMETRY:
            self._col_type = ColumnType.GEOMETRY
        elif self._content_type == BytesContentType.JSON:
            self._col_type = ColumnType.JSON
        elif self._content_type == BytesContentType.XML:
            self._col_type = ColumnType.XML
        elif self._is_binary:
            self._col_type = ColumnType.BYTES
        else:
            self._col_type = ColumnType.STRING
        self._is_padded = self._flags & 1

    def _map_datetime(self) -> None:
        """Map datetime."""
        if self._length == 10:
            self._col_type = ColumnType.DATE
        elif self._flags & DatetimeColumnFlags.TIMESTAMP > 0:
            self._col_type = ColumnType.TIMESTAMP
        elif self._length >= 19:
            self._col_type = ColumnType.DATETIME
        else:
            raise ValueError("Datetime mapping scenario unhandled")

    def _map_int_type(self) -> None:
        """Map int type."""
        if self._length <= 4:
            self._col_type = ColumnType.TINYINT
        elif self._length <= 6:
            self._col_type = ColumnType.SMALLINT
        elif self._length <= 9:
            self._col_type = ColumnType.MEDIUMINT
        elif self._length <= 11:
            self._col_type = ColumnType.INT
        else:
            self._col_type = ColumnType.BIGINT
        self._number_signed = True

    def _map_uint_type(self) -> None:
        """Map uint type."""
        if self._length <= 3:
            self._col_type = ColumnType.TINYINT
        elif self._length <= 5:
            self._col_type = ColumnType.SMALLINT
        elif self._length <= 8:
            self._col_type = ColumnType.MEDIUMINT
        elif self._length <= 10:
            self._col_type = ColumnType.INT
        else:
            self._col_type = ColumnType.BIGINT
        self._zero_fill = self._flags & 1

    def _map_type(self) -> None:
        """Map type."""
        if self._proto_type == ColumnProtoType.SINT:
            self._map_int_type()
        elif self._proto_type == ColumnProtoType.UINT:
            self._map_uint_type()
        elif self._proto_type == ColumnProtoType.FLOAT:
            self._col_type = ColumnType.FLOAT
            self._is_number_signed = (self._flags & FloatColumnFlags.UNSIGNED) == 0
        elif self._proto_type == ColumnProtoType.DECIMAL:
            self._col_type = ColumnType.DECIMAL
            self._is_number_signed = (self._flags & FloatColumnFlags.UNSIGNED) == 0
        elif self._proto_type == ColumnProtoType.DOUBLE:
            self._col_type = ColumnType.DOUBLE
            self._is_number_signed = (self._flags & FloatColumnFlags.UNSIGNED) == 0
        elif self._proto_type == ColumnProtoType.BYTES:
            self._map_bytes()
        elif self._proto_type == ColumnProtoType.TIME:
            self._col_type = ColumnType.TIME
        elif self._proto_type == ColumnProtoType.DATETIME:
            self._map_datetime()
        elif self._proto_type == ColumnProtoType.SET:
            self._col_type = ColumnType.SET
        elif self._proto_type == ColumnProtoType.ENUM:
            self._col_type = ColumnType.ENUM
        elif self._proto_type == ColumnProtoType.BIT:
            self._col_type = ColumnType.BIT
        else:
            raise ValueError(f"Unknown column type {self._proto_type}")

    @property
    def schema_name(self) -> str:
        """str: The schema name.

        .. versionadded:: 8.0.12
        """
        return self._schema

    @property
    def table_name(self) -> str:
        """str: The table name.

        .. versionadded:: 8.0.12
        """
        return self._original_table or self._table

    @property
    def table_label(self) -> str:
        """str: The table label.

        .. versionadded:: 8.0.12
        """
        return self._table or self._original_table

    @property
    def column_name(self) -> str:
        """str: The column name.

        .. versionadded:: 8.0.12
        """
        return self._original_name or self._name

    @property
    def column_label(self) -> str:
        """str: The column label.

        .. versionadded:: 8.0.12
        """
        return self._name or self._original_name

    @property
    def type(self) -> int:
        """int: The column type.

        .. versionadded:: 8.0.12
        """
        return self._col_type

    @property
    def length(self) -> int:
        """int. The column length.

        .. versionadded:: 8.0.12
        """
        return self._length

    @property
    def fractional_digits(self) -> int:
        """int: The column fractional digits.

        .. versionadded:: 8.0.12
        """
        return self._fractional_digits

    @property
    def collation_name(self) -> str:
        """str: The collation name.

        .. versionadded:: 8.0.12
        """
        return self._collation_name

    @property
    def character_set_name(self) -> str:
        """str: The character set name.

        .. versionadded:: 8.0.12
        """
        return self._character_set_name

    def get_schema_name(self) -> str:
        """Returns the schema name.

        Returns:
            str: The schema name.
        """
        return self._schema

    def get_table_name(self) -> str:
        """Returns the table name.

        Returns:
            str: The table name.
        """
        return self._original_table or self._table

    def get_table_label(self) -> str:
        """Returns the table label.

        Returns:
            str: The table label.
        """
        return self._table or self._original_table

    def get_column_name(self) -> str:
        """Returns the column name.

        Returns:
            str: The column name.
        """
        return self._original_name or self._name

    def get_column_label(self) -> str:
        """Returns the column label.

        Returns:
            str: The column label.
        """
        return self._name or self._original_name

    def get_proto_type(self) -> int:
        """Returns the column proto type.

        Returns:
            int: The column proto type.
        """
        return self._proto_type

    def get_type(self) -> int:
        """Returns the column type.

        Returns:
            int: The column type.
        """
        return self._col_type

    def get_length(self) -> int:
        """Returns the column length.

        Returns:
            int: The column length.
        """
        return self._length

    def get_fractional_digits(self) -> int:
        """Returns the column fractional digits.

        Returns:
            int: The column fractional digits.
        """
        return self._fractional_digits

    def get_collation_name(self) -> str:
        """Returns the collation name.

        Returns:
            str: The collation name.
        """
        return self._collation_name

    def get_character_set_name(self) -> str:
        """Returns the character set name.

        Returns:
            str: The character set name.
        """
        return self._character_set_name

    def is_number_signed(self) -> bool:
        """Returns `True` if is a number signed.

        Returns:
            bool: Returns `True` if is a number signed.
        """
        return self._number_signed

    def is_padded(self) -> Union[bool, int]:
        """Returns `True` if is padded.

        Returns:
            bool: Returns `True` if is padded.
        """
        return self._is_padded

    def is_bytes(self) -> bool:
        """Returns `True` if is bytes.

        Returns:
            bool: Returns `True` if is bytes.
        """
        return self._is_bytes


class Row:
    """Represents a row element returned from a SELECT query.

    Args:
        resultset (mysqlx.SqlResult or mysqlx.RowResult): The result set.
        fields (`list`): The list of fields.
    """

    def __init__(
        self, resultset: Union[BufferingResult, RowResult], fields: Sequence[FieldTypes]
    ) -> None:
        self._fields: Sequence[FieldTypes] = fields
        self._resultset: Union[BufferingResult, RowResult] = resultset

    def __repr__(self) -> str:
        return repr(self._fields)

    def __getitem__(self, index: Union[int, str]) -> Any:
        """Returns the value of a column by name or index.

        .. versionchanged:: 8.0.12
        """
        int_index = self._resultset.index_of(index) if isinstance(index, str) else index
        if int_index == -1 and isinstance(index, str):
            raise ValueError(f"Column name '{index}' not found")
        if int_index >= len(self._fields) or int_index < 0:
            raise IndexError("Index out of range")
        return self._fields[int_index]

    @deprecated("8.0.12")
    def get_string(self, str_index: str) -> str:
        """Returns the value using the column name.

        Args:
            str_index (str): The column name.

        .. deprecated:: 8.0.12
        """
        int_index = self._resultset.index_of(str_index)
        if int_index >= len(self._fields):
            raise IndexError("Argument out of range")
        if int_index == -1:
            raise ValueError(f"Column name '{str_index}' not found")
        return str(self._fields[int_index])


class BaseResult:
    """Provides base functionality for result objects.

    Args:
        connection (mysqlx.connection.Connection): The Connection object.
    """

    def __init__(self, connection: ConnectionType) -> None:
        self._connection: ConnectionType = connection
        self._closed: bool = False
        self._rows_affected: int = 0
        self._generated_id: int = -1
        self._generated_ids: List[int] = []
        self._warnings: List[Dict[str, Union[int, str]]] = []

        if connection is None:
            self._protocol = None
        else:
            self._protocol = connection.protocol
            connection.fetch_active_result()

    def get_affected_items_count(self) -> int:
        """Returns the number of affected items for the last operation.

        Returns:
            int: The number of affected items.
        """
        return self._rows_affected

    def get_warnings(self) -> List[Dict[str, Union[int, str]]]:
        """Returns the warnings.

        Returns:
            `list`: The list of warnings.
        """
        return self._warnings

    def get_warnings_count(self) -> int:
        """Returns the number of warnings.

        Returns:
            int: The number of warnings.
        """
        return len(self._warnings)

    def set_closed(self, flag: bool) -> None:
        """Sets if resultset fetch is done."""
        self._closed = flag

    def append_warning(self, level: int, code: int, msg: str) -> None:
        """Append a warning.

        Args:
            level (int): The warning level.
            code (int): The warning code.
            msg (str): The warning message.
        """
        self._warnings.append({"level": level, "code": code, "msg": msg})

    def set_generated_ids(self, generated_ids: List[int]) -> None:
        """Sets the generated ids."""
        self._generated_ids = generated_ids

    def set_generated_insert_id(self, generated_id: int) -> None:
        """Sets the generated insert id."""
        self._generated_id = generated_id

    def set_rows_affected(self, total: int) -> None:
        """Sets the number of rows affected."""
        self._rows_affected = total


class Result(BaseResult):
    """Allows retrieving information about non query operations performed on
    the database.

    Args:
        connection (mysqlx.connection.Connection): The Connection object.
                                                   ids (`list`): A list of IDs.
    """

    def __init__(
        self,
        connection: Optional[ConnectionType] = None,
        ids: Optional[List[int]] = None,
    ) -> None:
        super().__init__(connection)
        self._ids: Optional[List[int]] = ids

        if connection is not None:
            self._connection.close_result(self)

    def get_autoincrement_value(self) -> int:
        """Returns the last insert id auto generated.

        Returns:
            int: The last insert id.
        """
        return self._generated_id

    @deprecated("8.0.12")
    def get_document_id(self) -> Optional[int]:
        """Returns ID of the last document inserted into a collection.

        .. deprecated:: 8.0.12
        """
        if self._ids is None or len(self._ids) == 0:
            return None
        return self._ids[0]

    @deprecated("8.0.12")
    def get_generated_insert_id(self) -> int:
        """Returns the generated insert id.

        .. deprecated:: 8.0.12
        """
        return self._generated_id

    def get_generated_ids(self) -> List[int]:
        """Returns the generated ids."""
        return self._generated_ids


class BufferingResult(BaseResult):
    """Provides base functionality for buffering result objects.

    Args:
        connection (mysqlx.connection.Connection): The Connection object.
                                                   ids (`list`): A list of IDs.
    """

    def __init__(self, connection: ConnectionType) -> None:
        super().__init__(connection)
        self._columns: List[Column] = []
        self._has_data: bool = False
        self._has_more_results: bool = False
        self._items: List[Union[Row, DbDoc]] = []
        self._page_size: int = 0
        self._position: int = -1
        self._init_result()

    def __getitem__(self, index: int) -> Union[Row, DbDoc]:
        return self._items[index]

    @property
    def count(self) -> int:
        """int: The total of items."""
        return len(self._items)

    def _init_result(self) -> None:
        """Initialize the result."""
        self._columns = self._connection.get_column_metadata(self)
        self._has_more_data = len(self._columns) > 0
        self._items = []
        self._page_size = 20
        self._position = -1
        self._connection.set_active_result(self if self._has_more_data else None)

    def _read_item(self, dumping: bool) -> Optional[Union[Row, DbDoc]]:
        """Read item.

        Args:
            dumping (bool): `True` for dumping.

        Returns:
            :class:`mysqlx.Row`: A `Row` object.
        """
        row = self._connection.read_row(self)
        if row is None:
            return None
        item = [None] * len(row["field"])
        if not dumping:
            for key in range(len(row["field"])):
                column = self._columns[key]
                item[key] = from_protobuf(column, row["field"][key])
        return Row(self, item)

    def _page_in_items(self) -> Union[bool, int]:
        """Reads the page items.

        Returns:
            int: Total items read.
        """
        if self._closed:
            return False

        count = 0
        for _ in range(self._page_size):
            item = self._read_item(False)
            if item is None:
                break
            self._items.append(item)
            count += 1
        return count

    def index_of(self, col_name: str) -> int:
        """Returns the index of the column.

        Returns:
            int: The index of the column.
        """
        index = 0
        for col in self._columns:
            if col.get_column_label() == col_name:
                return index
            index += 1
        return -1

    def fetch_one(self) -> Optional[Union[Row, DbDoc]]:
        """Fetch one item.

        Returns:
            :class:`mysqlx.Row` or :class:`mysqlx.DbDoc`: one result item.
        """
        if self._closed:
            return None

        return self._read_item(False)

    def fetch_all(self) -> List[Union[Row, DbDoc]]:
        """Fetch all items.

        Returns:
            `list`: The list of items of :class:`mysqlx.DbDoc` or
                    :class:`mysqlx.Row`.
        """
        while True:
            if not self._page_in_items():
                break
        return self._items

    def set_has_data(self, flag: bool) -> None:
        """Sets if result has data.

        Args:
            flag (bool): `True` if result has data.
        """
        self._has_data = flag

    def set_has_more_results(self, flag: bool) -> None:
        """Sets if has more results.

        Args:
            flag (bool): `True` if has more results.
        """
        self._has_more_results = flag


class RowResult(BufferingResult):
    """Allows traversing the Row objects returned by a Table.select operation.

    Args:
        connection (mysqlx.connection.Connection): The Connection object.
    """

    @property
    def columns(self) -> List[Column]:
        """`list`: The list of columns."""
        return self._columns

    def get_columns(self) -> List[Column]:
        """Returns the list of columns.

        Returns:
            `list`: The list of columns.

        .. versionadded:: 8.0.12
        """
        return self._columns


class SqlResult(RowResult):
    """Represents a result from a SQL statement.

    Args:
        connection (mysqlx.connection.Connection): The Connection object.
    """

    def get_autoincrement_value(self) -> int:
        """Returns the identifier for the last record inserted.

        Returns:
            str: The identifier of the last record inserted.
        """
        return self._generated_id

    def next_result(self) -> bool:
        """Process the next result.

        Returns:
            bool: Returns `True` if the fetch is done.
        """
        if self._closed:
            return False
        self._has_more_results = False
        self._init_result()
        return True

    def has_data(self) -> bool:
        """Returns True if result has data.

        Returns:
            bool: Returns `True` if result has data.

        .. versionadded:: 8.0.12
        """
        return self._has_data


class DocResult(BufferingResult):
    """Allows traversing the DbDoc objects returned by a Collection.find
    operation.

    Args:
        connection (mysqlx.connection.Connection): The Connection object.
    """

    def _read_item(self, dumping: bool) -> DbDoc:
        """Read item.

        Args:
            dumping (bool): `True` for dumping.

        Returns:
            :class:`mysqlx.DbDoc`: A `DbDoc` object.
        """
        row = super()._read_item(dumping)
        if row is None:
            return None
        return DbDoc(decode_from_bytes(row[0]))  # type: ignore[index]
