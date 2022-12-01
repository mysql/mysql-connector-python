# Copyright (c) 2017, 2022, Oracle and/or its affiliates.
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

# mypy: disable-error-code="attr-defined,union-attr"

"""Implementation of a helper class for MySQL X Protobuf messages."""

# pylint: disable=c-extension-no-member, no-member

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Type, Union

from ..types import MessageType, ProtobufMessageCextType, ProtobufMessageType

_SERVER_MESSAGES_TUPLES: Tuple[Tuple[str, str], ...] = (
    ("Mysqlx.ServerMessages.Type.OK", "Mysqlx.Ok"),
    ("Mysqlx.ServerMessages.Type.ERROR", "Mysqlx.Error"),
    (
        "Mysqlx.ServerMessages.Type.CONN_CAPABILITIES",
        "Mysqlx.Connection.Capabilities",
    ),
    (
        "Mysqlx.ServerMessages.Type.SESS_AUTHENTICATE_CONTINUE",
        "Mysqlx.Session.AuthenticateContinue",
    ),
    (
        "Mysqlx.ServerMessages.Type.SESS_AUTHENTICATE_OK",
        "Mysqlx.Session.AuthenticateOk",
    ),
    ("Mysqlx.ServerMessages.Type.NOTICE", "Mysqlx.Notice.Frame"),
    (
        "Mysqlx.ServerMessages.Type.RESULTSET_COLUMN_META_DATA",
        "Mysqlx.Resultset.ColumnMetaData",
    ),
    ("Mysqlx.ServerMessages.Type.RESULTSET_ROW", "Mysqlx.Resultset.Row"),
    (
        "Mysqlx.ServerMessages.Type.RESULTSET_FETCH_DONE",
        "Mysqlx.Resultset.FetchDone",
    ),
    (
        "Mysqlx.ServerMessages.Type.RESULTSET_FETCH_SUSPENDED",
        "Mysqlx.Resultset.FetchSuspended",
    ),
    (
        "Mysqlx.ServerMessages.Type.RESULTSET_FETCH_DONE_MORE_RESULTSETS",
        "Mysqlx.Resultset.FetchDoneMoreResultsets",
    ),
    (
        "Mysqlx.ServerMessages.Type.SQL_STMT_EXECUTE_OK",
        "Mysqlx.Sql.StmtExecuteOk",
    ),
    (
        "Mysqlx.ServerMessages.Type.RESULTSET_FETCH_DONE_MORE_OUT_PARAMS",
        "Mysqlx.Resultset.FetchDoneMoreOutParams",
    ),
    (
        "Mysqlx.ServerMessages.Type.COMPRESSION",
        "Mysqlx.Connection.Compression",
    ),
)

PROTOBUF_VERSION: Optional[str] = None
PROTOBUF_REPEATED_TYPES: List[
    Union[
        Type[List[Dict[str, Any]]],
        Type[RepeatedCompositeContainer],
        Type[RepeatedCompositeFieldContainer],
    ]
] = [list]

try:
    import _mysqlxpb

    SERVER_MESSAGES = {
        int(_mysqlxpb.enum_value(key)): val for key, val in _SERVER_MESSAGES_TUPLES
    }
    HAVE_MYSQLXPB_CEXT = True
except ImportError:
    HAVE_MYSQLXPB_CEXT = False

from ..helpers import BYTE_TYPES, NUMERIC_TYPES, encode_to_bytes

try:
    from google import protobuf
    from google.protobuf import (
        descriptor_database,
        descriptor_pb2,
        descriptor_pool,
        message_factory,
    )
    from google.protobuf.internal.containers import RepeatedCompositeFieldContainer

    try:
        from google.protobuf.pyext._message import RepeatedCompositeContainer

        PROTOBUF_REPEATED_TYPES.append(RepeatedCompositeContainer)
    except ImportError:
        pass

    PROTOBUF_REPEATED_TYPES.append(RepeatedCompositeFieldContainer)
    if hasattr(protobuf, "__version__"):
        # Only Protobuf versions >=3.0.0 provide `__version__`
        PROTOBUF_VERSION = protobuf.__version__

    from . import (
        mysqlx_connection_pb2,
        mysqlx_crud_pb2,
        mysqlx_cursor_pb2,
        mysqlx_datatypes_pb2,
        mysqlx_expect_pb2,
        mysqlx_expr_pb2,
        mysqlx_notice_pb2,
        mysqlx_pb2,
        mysqlx_prepare_pb2,
        mysqlx_resultset_pb2,
        mysqlx_session_pb2,
        mysqlx_sql_pb2,
    )

    # Dictionary with all messages descriptors
    _MESSAGES: Dict[str, int] = {}

    # Mysqlx
    for key, val in mysqlx_pb2.ClientMessages.Type.items():
        _MESSAGES[f"Mysqlx.ClientMessages.Type.{key}"] = val
    for key, val in mysqlx_pb2.ServerMessages.Type.items():
        _MESSAGES[f"Mysqlx.ServerMessages.Type.{key}"] = val
    for key, val in mysqlx_pb2.Error.Severity.items():
        _MESSAGES[f"Mysqlx.Error.Severity.{key}"] = val

    # Mysqlx.Crud
    for key, val in mysqlx_crud_pb2.DataModel.items():
        _MESSAGES[f"Mysqlx.Crud.DataModel.{key}"] = val
    for key, val in mysqlx_crud_pb2.Find.RowLock.items():
        _MESSAGES[f"Mysqlx.Crud.Find.RowLock.{key}"] = val
    for key, val in mysqlx_crud_pb2.Order.Direction.items():
        _MESSAGES[f"Mysqlx.Crud.Order.Direction.{key}"] = val
    for key, val in mysqlx_crud_pb2.UpdateOperation.UpdateType.items():
        _MESSAGES[f"Mysqlx.Crud.UpdateOperation.UpdateType.{key}"] = val

    # Mysqlx.Datatypes
    for key, val in mysqlx_datatypes_pb2.Scalar.Type.items():
        _MESSAGES[f"Mysqlx.Datatypes.Scalar.Type.{key}"] = val
    for key, val in mysqlx_datatypes_pb2.Any.Type.items():
        _MESSAGES[f"Mysqlx.Datatypes.Any.Type.{key}"] = val

    # Mysqlx.Expect
    for key, val in mysqlx_expect_pb2.Open.Condition.ConditionOperation.items():
        _MESSAGES[f"Mysqlx.Expect.Open.Condition.ConditionOperation.{key}"] = val
    for key, val in mysqlx_expect_pb2.Open.Condition.Key.items():
        _MESSAGES[f"Mysqlx.Expect.Open.Condition.Key.{key}"] = val
    for key, val in mysqlx_expect_pb2.Open.CtxOperation.items():
        _MESSAGES[f"Mysqlx.Expect.Open.CtxOperation.{key}"] = val

    # Mysqlx.Expr
    for key, val in mysqlx_expr_pb2.Expr.Type.items():
        _MESSAGES[f"Mysqlx.Expr.Expr.Type.{key}"] = val
    for key, val in mysqlx_expr_pb2.DocumentPathItem.Type.items():
        _MESSAGES[f"Mysqlx.Expr.DocumentPathItem.Type.{key}"] = val

    # Mysqlx.Notice
    for key, val in mysqlx_notice_pb2.Frame.Scope.items():
        _MESSAGES[f"Mysqlx.Notice.Frame.Scope.{key}"] = val
    for key, val in mysqlx_notice_pb2.Warning.Level.items():
        _MESSAGES[f"Mysqlx.Notice.Warning.Level.{key}"] = val
    for key, val in mysqlx_notice_pb2.SessionStateChanged.Parameter.items():
        _MESSAGES[f"Mysqlx.Notice.SessionStateChanged.Parameter.{key}"] = val

    # Mysql.Prepare
    for key, val in mysqlx_prepare_pb2.Prepare.OneOfMessage.Type.items():
        _MESSAGES[f"Mysqlx.Prepare.Prepare.OneOfMessage.Type.{key}"] = val

    # Mysql.Resultset
    for key, val in mysqlx_resultset_pb2.ColumnMetaData.FieldType.items():
        _MESSAGES[f"Mysqlx.Resultset.ColumnMetaData.FieldType.{key}"] = val

    # Add messages to the descriptor pool
    _DESCRIPTOR_DB = descriptor_database.DescriptorDatabase()
    _DESCRIPTOR_POOL = descriptor_pool.DescriptorPool(_DESCRIPTOR_DB)

    _DESCRIPTOR_DB.Add(
        descriptor_pb2.FileDescriptorProto.FromString(
            mysqlx_connection_pb2.DESCRIPTOR.serialized_pb
        )
    )
    _DESCRIPTOR_DB.Add(
        descriptor_pb2.FileDescriptorProto.FromString(
            mysqlx_crud_pb2.DESCRIPTOR.serialized_pb
        )
    )
    _DESCRIPTOR_DB.Add(
        descriptor_pb2.FileDescriptorProto.FromString(
            mysqlx_cursor_pb2.DESCRIPTOR.serialized_pb
        )
    )
    _DESCRIPTOR_DB.Add(
        descriptor_pb2.FileDescriptorProto.FromString(
            mysqlx_datatypes_pb2.DESCRIPTOR.serialized_pb
        )
    )
    _DESCRIPTOR_DB.Add(
        descriptor_pb2.FileDescriptorProto.FromString(
            mysqlx_expect_pb2.DESCRIPTOR.serialized_pb
        )
    )
    _DESCRIPTOR_DB.Add(
        descriptor_pb2.FileDescriptorProto.FromString(
            mysqlx_expr_pb2.DESCRIPTOR.serialized_pb
        )
    )
    _DESCRIPTOR_DB.Add(
        descriptor_pb2.FileDescriptorProto.FromString(
            mysqlx_notice_pb2.DESCRIPTOR.serialized_pb
        )
    )
    _DESCRIPTOR_DB.Add(
        descriptor_pb2.FileDescriptorProto.FromString(
            mysqlx_pb2.DESCRIPTOR.serialized_pb
        )
    )
    _DESCRIPTOR_DB.Add(
        descriptor_pb2.FileDescriptorProto.FromString(
            mysqlx_prepare_pb2.DESCRIPTOR.serialized_pb
        )
    )
    _DESCRIPTOR_DB.Add(
        descriptor_pb2.FileDescriptorProto.FromString(
            mysqlx_resultset_pb2.DESCRIPTOR.serialized_pb
        )
    )
    _DESCRIPTOR_DB.Add(
        descriptor_pb2.FileDescriptorProto.FromString(
            mysqlx_session_pb2.DESCRIPTOR.serialized_pb
        )
    )
    _DESCRIPTOR_DB.Add(
        descriptor_pb2.FileDescriptorProto.FromString(
            mysqlx_sql_pb2.DESCRIPTOR.serialized_pb
        )
    )

    SERVER_MESSAGES = {_MESSAGES[key]: val for key, val in _SERVER_MESSAGES_TUPLES}
    HAVE_PROTOBUF = True
    HAVE_PROTOBUF_ERROR = None

    class _mysqlxpb_pure:  # pylint: disable=invalid-name
        """This class implements the methods in pure Python used by the
        _mysqlxpb C++ extension."""

        factory: message_factory.MessageFactory = message_factory.MessageFactory()

        @staticmethod
        def new_message(name: str) -> ProtobufMessageType:
            """Create new Protobuf message.

            Args:
                name (str): Message type name.

            Returns:
                object: Protobuf message.
            """
            cls = _mysqlxpb_pure.factory.GetPrototype(
                _DESCRIPTOR_POOL.FindMessageTypeByName(name)
            )
            return cls()

        @staticmethod
        def enum_value(enum_key: str) -> int:
            """Return enum value.

            Args:
                enum_key (str): Enum key.

            Returns:
                int: enum value.
            """
            return _MESSAGES[enum_key]

        @staticmethod
        def serialize_message(msg: ProtobufMessageType) -> bytes:
            """Serialize message.

            Args:
                msg (object): Protobuf message.

            Returns:
                bytes: Serialized message bytes string.
            """
            return msg.SerializeToString()

        @staticmethod
        def serialize_partial_message(msg: ProtobufMessageType) -> bytes:
            """Serialize partial message.

            Args:
                msg (object): Protobuf message.

            Returns:
                bytes: Serialized partial message bytes string.
            """
            return msg.SerializePartialToString()

        @staticmethod
        def parse_message(msg_type_name: str, payload: bytes) -> ProtobufMessageType:
            """Serialize partial message.

            Args:
                msg_type_name (str): Message type name.
                payload (bytes): Payload.

            Returns:
                object: Message parsed from string.
            """
            msg = _mysqlxpb_pure.new_message(msg_type_name)
            msg.ParseFromString(payload)
            return msg

        @staticmethod
        def parse_server_message(msg_type: int, payload: bytes) -> ProtobufMessageType:
            """Parse server message message.

            Args:
                msg_type (int): Message type.
                payload (bytes): Payload.

            Returns:
                object: Server message parsed from string.
            """
            msg_type_name = SERVER_MESSAGES.get(msg_type)
            if not msg_type_name:
                raise ValueError(f"Unknown msg_type: {msg_type}")
            msg = _mysqlxpb_pure.new_message(msg_type_name)
            msg.ParseFromString(payload)
            return msg

except (ImportError, SyntaxError, TypeError) as err:
    HAVE_PROTOBUF = False
    HAVE_PROTOBUF_ERROR = (
        err if PROTOBUF_VERSION is not None else "Protobuf >=3.11.0 is required"
    )
    if not HAVE_MYSQLXPB_CEXT:
        raise ImportError(f"Protobuf is not available: {HAVE_PROTOBUF_ERROR}") from err

CRUD_PREPARE_MAPPING: Dict[str, Tuple[str, str]] = {
    "Mysqlx.ClientMessages.Type.CRUD_FIND": (
        "Mysqlx.Prepare.Prepare.OneOfMessage.Type.FIND",
        "find",
    ),
    "Mysqlx.ClientMessages.Type.CRUD_INSERT": (
        "Mysqlx.Prepare.Prepare.OneOfMessage.Type.INSERT",
        "insert",
    ),
    "Mysqlx.ClientMessages.Type.CRUD_UPDATE": (
        "Mysqlx.Prepare.Prepare.OneOfMessage.Type.UPDATE",
        "update",
    ),
    "Mysqlx.ClientMessages.Type.CRUD_DELETE": (
        "Mysqlx.Prepare.Prepare.OneOfMessage.Type.DELETE",
        "delete",
    ),
    "Mysqlx.ClientMessages.Type.SQL_STMT_EXECUTE": (
        "Mysqlx.Prepare.Prepare.OneOfMessage.Type.STMT",
        "stmt_execute",
    ),
}


class Protobuf:
    """Protobuf class acts as a container of the Protobuf message class.

    It allows the switch between the C extension and pure Python implementation
    message handlers, by patching the `mysqlxpb` class attribute.
    """

    mysqlxpb = _mysqlxpb if HAVE_MYSQLXPB_CEXT else _mysqlxpb_pure
    use_pure: bool = not HAVE_MYSQLXPB_CEXT

    @staticmethod
    def set_use_pure(use_pure: bool) -> None:
        """Sets whether to use the C extension or pure Python implementation.

        Args:
            use_pure (bool): `True` to use pure Python implementation.
        """
        if use_pure and not HAVE_PROTOBUF:
            raise ImportError(f"Protobuf is not available: {HAVE_PROTOBUF_ERROR}")
        if not use_pure and not HAVE_MYSQLXPB_CEXT:
            raise ImportError("MySQL X Protobuf C extension is not available")
        Protobuf.mysqlxpb = _mysqlxpb_pure if use_pure else _mysqlxpb
        Protobuf.use_pure = use_pure


class Message:
    """Helper class for interfacing with the MySQL X Protobuf extension.

    Args:
        msg_type_name (string): Protobuf type name.
        **kwargs: Arbitrary keyword arguments with values for the message.
    """

    def __init__(self, msg_type_name: Optional[str] = None, **kwargs: Any) -> None:
        # _msg is a protobuf message instance when use_pure=True,
        # else is a dictionary instance.
        self.__dict__["_msg"] = (
            Protobuf.mysqlxpb.new_message(msg_type_name) if msg_type_name else None
        )
        for name, value in kwargs.items():
            self.__setattr__(name, value)

    def __setattr__(self, name: str, value: Any) -> None:
        if Protobuf.use_pure:
            if isinstance(value, str):
                setattr(self._msg, name, encode_to_bytes(value))
            elif isinstance(value, (NUMERIC_TYPES, BYTE_TYPES)):
                setattr(self._msg, name, value)
            elif isinstance(value, list):
                getattr(self._msg, name).extend(value)
            elif isinstance(value, Message):
                getattr(self._msg, name).MergeFrom(value.get_message())
            else:
                getattr(self._msg, name).MergeFrom(value)
        else:
            if isinstance(value, str):
                self._msg[name] = encode_to_bytes(value)
            else:
                self._msg[name] = (
                    value.get_message() if isinstance(value, Message) else value
                )

    def __getattr__(self, name: str) -> Any:
        try:
            return (
                self._msg[name] if not Protobuf.use_pure else getattr(self._msg, name)
            )
        except KeyError:
            raise AttributeError from None

    def __setitem__(self, name: str, value: Any) -> None:
        self.__setattr__(name, value)

    def __getitem__(self, name: str) -> Any:
        return self.__getattr__(name)

    def get(self, name: str, default: Any = None) -> Any:
        """Returns the value of an element of the message dictionary.

        Args:
            name (string): Key name.
            default (object): The default value if the key does not exists.

        Returns:
            object: The value of the provided key name.
        """
        return (
            self.__dict__["_msg"].get(name, default)
            if not Protobuf.use_pure
            else getattr(self.__dict__["_msg"], name, default)
        )

    def set_message(
        self, msg: Union[ProtobufMessageType, ProtobufMessageCextType]
    ) -> None:
        """Sets the message.

        Args:
            msg (dict): Dictionary representing a message.
        """
        self.__dict__["_msg"] = msg

    def get_message(self) -> Union[ProtobufMessageType, ProtobufMessageCextType]:
        """Returns the dictionary representing a message containing parsed
        data.

        Returns:
            dict: The dictionary representing a message containing parsed data.
        """
        return self.__dict__["_msg"]

    def serialize_to_string(self) -> bytes:
        """Serializes a message to a string.

        Returns:
            str: A string representing a message containing parsed data.
        """
        return Protobuf.mysqlxpb.serialize_message(self._msg)

    def serialize_partial_to_string(self) -> bytes:
        """Serializes the protocol message to a binary string.

        This method is similar to serialize_to_string but doesn't check if the
        message is initialized.

        Returns:
            str: A string representation of the partial message.
        """
        return Protobuf.mysqlxpb.serialize_partial_message(self._msg)

    @property
    def type(self) -> str:
        """string: Message type name."""
        return (
            self._msg["_mysqlxpb_type_name"]
            if not Protobuf.use_pure
            else self._msg.DESCRIPTOR.full_name
        )

    @staticmethod
    def parse(
        msg_type_name: str, payload: bytes
    ) -> Union[ProtobufMessageType, ProtobufMessageCextType]:
        """Creates a new message, initialized with parsed data.

        Args:
            msg_type_name (string): Message type name.
            payload (string): Serialized message data.

        Returns:
            dict: The dictionary representing a message containing parsed data.

        .. versionadded:: 8.0.21
        """
        return Protobuf.mysqlxpb.parse_message(msg_type_name, payload)

    @staticmethod
    def byte_size(msg: MessageType) -> int:
        """Returns the size of the message in bytes.

        Args:
            msg (mysqlx.protobuf.Message): MySQL X Protobuf Message.

        Returns:
            int: Size of the message in bytes.

        .. versionadded:: 8.0.21
        """
        return (
            msg.ByteSize()
            if Protobuf.use_pure
            else len(encode_to_bytes(msg.serialize_to_string()))
        )

    @staticmethod
    def parse_from_server(
        msg_type: int, payload: bytes
    ) -> Union[ProtobufMessageType, ProtobufMessageCextType]:
        """Creates a new server-side message, initialized with parsed data.

        Args:
            msg_type (int): Message type.
            payload (string): Serialized message data.

        Returns:
            dict: The dictionary representing a message containing parsed data.
        """
        return Protobuf.mysqlxpb.parse_server_message(msg_type, payload)

    @classmethod
    def from_message(cls, msg_type_name: str, payload: bytes) -> MessageType:
        """Creates a new message, initialized with parsed data and returns a
        :class:`mysqlx.protobuf.Message` object.

        Args:
            msg_type_name (string): Message type name.
            payload (string): Serialized message data.

        Returns:
            mysqlx.protobuf.Message: The Message representing a message
                                     containing parsed data.
        """
        msg = cls()
        msg.set_message(Protobuf.mysqlxpb.parse_message(msg_type_name, payload))
        return msg

    @classmethod
    def from_server_message(cls, msg_type: int, payload: bytes) -> MessageType:
        """Creates a new server-side message, initialized with parsed data and
        returns a :class:`mysqlx.protobuf.Message` object.

        Args:
            msg_type (int): Message type.
            payload (string): Serialized message data.

        Returns:
            mysqlx.protobuf.Message: The Message representing a message
                                     containing parsed data.
        """
        msg = cls()
        msg.set_message(Protobuf.mysqlxpb.parse_server_message(msg_type, payload))
        return msg


def mysqlxpb_enum(name: str) -> int:
    """Returns the value of a MySQL X Protobuf enumerator.

    Args:
        name (string): MySQL X Protobuf numerator name.

    Returns:
        int: Value of the enumerator.
    """
    return Protobuf.mysqlxpb.enum_value(name)
