# Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.
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

"""This module contains the implementation of a helper class for MySQL X
Protobuf messages."""

from mysqlx.compat import PY3, NUMERIC_TYPES, STRING_TYPES, BYTE_TYPES
from mysqlx.helpers import encode_to_bytes


_SERVER_MESSAGES_TUPLES = (
    ("Mysqlx.ServerMessages.Type.OK", "Mysqlx.Ok"),
    ("Mysqlx.ServerMessages.Type.ERROR", "Mysqlx.Error"),
    ("Mysqlx.ServerMessages.Type.CONN_CAPABILITIES",
     "Mysqlx.Connection.Capabilities"),
    ("Mysqlx.ServerMessages.Type.SESS_AUTHENTICATE_CONTINUE",
     "Mysqlx.Session.AuthenticateContinue"),
    ("Mysqlx.ServerMessages.Type.SESS_AUTHENTICATE_OK",
     "Mysqlx.Session.AuthenticateOk"),
    ("Mysqlx.ServerMessages.Type.NOTICE", "Mysqlx.Notice.Frame"),
    ("Mysqlx.ServerMessages.Type.RESULTSET_COLUMN_META_DATA",
     "Mysqlx.Resultset.ColumnMetaData"),
    ("Mysqlx.ServerMessages.Type.RESULTSET_ROW",
     "Mysqlx.Resultset.Row"),
    ("Mysqlx.ServerMessages.Type.RESULTSET_FETCH_DONE",
     "Mysqlx.Resultset.FetchDone"),
    ("Mysqlx.ServerMessages.Type.RESULTSET_FETCH_SUSPENDED",
     "Mysqlx.Resultset.FetchSuspended"),
    ("Mysqlx.ServerMessages.Type.RESULTSET_FETCH_DONE_MORE_RESULTSETS",
     "Mysqlx.Resultset.FetchDoneMoreResultsets"),
    ("Mysqlx.ServerMessages.Type.SQL_STMT_EXECUTE_OK",
     "Mysqlx.Sql.StmtExecuteOk"),
    ("Mysqlx.ServerMessages.Type.RESULTSET_FETCH_DONE_MORE_OUT_PARAMS",
     "Mysqlx.Resultset.FetchDoneMoreOutParams"),
)


try:
    import _mysqlxpb

    HAVE_MYSQLXPB_CEXT = True
    SERVER_MESSAGES = dict([(int(_mysqlxpb.enum_value(key)), val)
                            for key, val in _SERVER_MESSAGES_TUPLES])
except ImportError:
    from google.protobuf import descriptor_database
    from google.protobuf import descriptor_pb2
    from google.protobuf import descriptor_pool
    from google.protobuf import message_factory

    from . import mysqlx_connection_pb2
    from . import mysqlx_crud_pb2
    from . import mysqlx_datatypes_pb2
    from . import mysqlx_expect_pb2
    from . import mysqlx_expr_pb2
    from . import mysqlx_notice_pb2
    from . import mysqlx_pb2
    from . import mysqlx_resultset_pb2
    from . import mysqlx_session_pb2
    from . import mysqlx_sql_pb2

    HAVE_MYSQLXPB_CEXT = False

    # Dictionary with all messages descriptors
    _MESSAGES = {}

    # Mysqlx
    for key, val in mysqlx_pb2.ClientMessages.Type.items():
        _MESSAGES["Mysqlx.ClientMessages.Type.{0}".format(key)] = val
    for key, val in mysqlx_pb2.ServerMessages.Type.items():
        _MESSAGES["Mysqlx.ServerMessages.Type.{0}".format(key)] = val
    for key, val in mysqlx_pb2.Error.Severity.items():
        _MESSAGES["Mysqlx.Error.Severity.{0}".format(key)] = val

    # Mysqlx.Crud
    for key, val in mysqlx_crud_pb2.DataModel.items():
        _MESSAGES["Mysqlx.Crud.DataModel.{0}".format(key)] = val
    for key, val in mysqlx_crud_pb2.Find.RowLock.items():
        _MESSAGES["Mysqlx.Crud.Find.RowLock.{0}".format(key)] = val
    for key, val in mysqlx_crud_pb2.Order.Direction.items():
        _MESSAGES["Mysqlx.Crud.Order.Direction.{0}".format(key)] = val
    for key, val in mysqlx_crud_pb2.UpdateOperation.UpdateType.items():
        _MESSAGES["Mysqlx.Crud.UpdateOperation.UpdateType.{0}"
                  "".format(key)] = val

    # Mysqlx.Datatypes
    for key, val in mysqlx_datatypes_pb2.Scalar.Type.items():
        _MESSAGES["Mysqlx.Datatypes.Scalar.Type.{0}".format(key)] = val
    for key, val in mysqlx_datatypes_pb2.Any.Type.items():
        _MESSAGES["Mysqlx.Datatypes.Any.Type.{0}".format(key)] = val

    # Mysqlx.Expect
    for key, val in \
        mysqlx_expect_pb2.Open.Condition.ConditionOperation.items():
        _MESSAGES["Mysqlx.Expect.Open.Condition.ConditionOperation.{0}"
                  "".format(key)] = val
    for key, val in mysqlx_expect_pb2.Open.CtxOperation.items():
        _MESSAGES["Mysqlx.Expect.Open.CtxOperation.{0}"
                  "".format(key)] = val

    # Mysqlx.Expr
    for key, val in mysqlx_expr_pb2.Expr.Type.items():
        _MESSAGES["Mysqlx.Expr.Expr.Type.{0}".format(key)] = val
    for key, val in mysqlx_expr_pb2.DocumentPathItem.Type.items():
        _MESSAGES["Mysqlx.Expr.DocumentPathItem.Type.{0}".format(key)] = val

    # Mysqlx.Notice
    for key, val in mysqlx_notice_pb2.Frame.Scope.items():
        _MESSAGES["Mysqlx.Notice.Frame.Scope.{0}".format(key)] = val
    for key, val in mysqlx_notice_pb2.Warning.Level.items():
        _MESSAGES["Mysqlx.Notice.Warning.Level.{0}".format(key)] = val
    for key, val in mysqlx_notice_pb2.SessionStateChanged.Parameter.items():
        _MESSAGES["Mysqlx.Notice.SessionStateChanged.Parameter.{0}"
                  "".format(key)] = val

    # Mysql.Resultset
    for key, val in mysqlx_resultset_pb2.ColumnMetaData.FieldType.items():
        _MESSAGES["Mysqlx.Resultset.ColumnMetaData.FieldType.{0}"
                  "".format(key)] = val

    SERVER_MESSAGES = dict(
        [(_MESSAGES[key], val) for key, val in _SERVER_MESSAGES_TUPLES]
    )

    # Add messages to the descriptor pool
    _DESCRIPTOR_DB = descriptor_database.DescriptorDatabase()
    _DESCRIPTOR_POOL = descriptor_pool.DescriptorPool(_DESCRIPTOR_DB)

    _DESCRIPTOR_DB.Add(descriptor_pb2.FileDescriptorProto.FromString(
        mysqlx_connection_pb2.DESCRIPTOR.serialized_pb))
    _DESCRIPTOR_DB.Add(descriptor_pb2.FileDescriptorProto.FromString(
        mysqlx_crud_pb2.DESCRIPTOR.serialized_pb))
    _DESCRIPTOR_DB.Add(descriptor_pb2.FileDescriptorProto.FromString(
        mysqlx_datatypes_pb2.DESCRIPTOR.serialized_pb))
    _DESCRIPTOR_DB.Add(descriptor_pb2.FileDescriptorProto.FromString(
        mysqlx_expect_pb2.DESCRIPTOR.serialized_pb))
    _DESCRIPTOR_DB.Add(descriptor_pb2.FileDescriptorProto.FromString(
        mysqlx_expr_pb2.DESCRIPTOR.serialized_pb))
    _DESCRIPTOR_DB.Add(descriptor_pb2.FileDescriptorProto.FromString(
        mysqlx_notice_pb2.DESCRIPTOR.serialized_pb))
    _DESCRIPTOR_DB.Add(descriptor_pb2.FileDescriptorProto.FromString(
        mysqlx_pb2.DESCRIPTOR.serialized_pb))
    _DESCRIPTOR_DB.Add(descriptor_pb2.FileDescriptorProto.FromString(
        mysqlx_resultset_pb2.DESCRIPTOR.serialized_pb))
    _DESCRIPTOR_DB.Add(descriptor_pb2.FileDescriptorProto.FromString(
        mysqlx_session_pb2.DESCRIPTOR.serialized_pb))
    _DESCRIPTOR_DB.Add(descriptor_pb2.FileDescriptorProto.FromString(
        mysqlx_sql_pb2.DESCRIPTOR.serialized_pb))


    class _mysqlxpb(object):
        """This class implements the methods in pure Python used by the
        _mysqlxpb C++ extension."""

        factory = message_factory.MessageFactory()

        @staticmethod
        def new_message(name):
            cls = _mysqlxpb.factory.GetPrototype(
                _DESCRIPTOR_POOL.FindMessageTypeByName(name))
            return cls()

        @staticmethod
        def enum_value(key):
            return _MESSAGES[key]

        @staticmethod
        def serialize_message(msg):
            return msg.SerializeToString()

        @staticmethod
        def parse_message(msg_type_name, payload):
            msg = _mysqlxpb.new_message(msg_type_name)
            msg.ParseFromString(payload)
            return msg

        @staticmethod
        def parse_server_message(msg_type, payload):
            msg_type_name = SERVER_MESSAGES.get(msg_type)
            if not msg_type_name:
                raise ValueError("Unknown msg_type: {0}".format(msg_type))
            msg = _mysqlxpb.new_message(msg_type_name)
            msg.ParseFromString(payload)
            return msg


def mysqlxpb_enum(name):
    """Returns the value of a MySQL X Protobuf enumerator.

    Args:
        name (string): MySQL X Protobuf numerator name.

    Returns:
        int: Value of the enumerator.
    """
    return _mysqlxpb.enum_value(name)


class Message(object):
    """Helper class for interfacing with the MySQL X Protobuf extension.

    Args:
        msg_type_name (string): Protobuf type name.
        **kwargs: Arbitrary keyword arguments with values for the message.
    """
    def __init__(self, msg_type_name=None, **kwargs):
        self.__dict__["_msg"] = _mysqlxpb.new_message(msg_type_name) \
            if msg_type_name else None
        for key, value in kwargs.items():
            self.__setattr__(key, value)

    def __setattr__(self, name, value):
        if HAVE_MYSQLXPB_CEXT:
            self._msg[name] = value.get_message() \
                if isinstance(value, Message) else value
        else:
            if PY3 and isinstance(value, STRING_TYPES):
                setattr(self._msg, name, encode_to_bytes(value))
            elif isinstance(value, (NUMERIC_TYPES, STRING_TYPES, BYTE_TYPES)):
                setattr(self._msg, name, value)
            elif isinstance(value, list):
                getattr(self._msg, name).extend(value)
            elif isinstance(value, Message):
                getattr(self._msg, name).MergeFrom(value.get_message())
            else:
                getattr(self._msg, name).MergeFrom(value)

    def __getattr__(self, name):
        try:
            return self._msg[name] if HAVE_MYSQLXPB_CEXT else \
                getattr(self._msg, name)
        except KeyError:
            raise AttributeError

    def __setitem__(self, name, value):
        self.__setattr__(name, value)

    def __getitem__(self, name):
        return self.__getattr__(name)

    def get(self, name, default=None):
        """Returns the value of an element of the message dictionary.

        Args:
            name (string): Key name.
            default (object): The default value if the key does not exists.

        Returns:
            object: The value of the provided key name.
        """
        return self.__dict__["_msg"].get(name, default) if HAVE_MYSQLXPB_CEXT \
            else getattr(self.__dict__["_msg"], name, default)

    def set_message(self, msg):
        """Sets the message.

        Args:
            msg (dict): Dictionary representing a message.
        """
        self.__dict__["_msg"] = msg

    def get_message(self):
        """Returns the dictionary representing a message containing parsed
        data.

        Returns:
            dict: The dictionary representing a message containing parsed data.
        """
        return self.__dict__["_msg"]

    def serialize_to_string(self):
        """Serializes a message to a string.

        Returns:
           string: A string representing a message containing parsed data.
        """
        return _mysqlxpb.serialize_message(self._msg)

    @property
    def type(self):
        """string: Message type name."""
        return self._msg["_mysqlxpb_type_name"] if HAVE_MYSQLXPB_CEXT else \
            self._msg.DESCRIPTOR.full_name

    @staticmethod
    def parse(msg_type_name, payload):
        """Creates a new message, initialized with parsed data.

        Args:
            msg_type_name (string): Message type name.
            payload (string): Serialized message data.

        Returns:
            dict: The dictionary representing a message containing parsed data.
        """
        return _mysqlxpb.parse_message(msg_type_name, payload)

    @staticmethod
    def parse_from_server(msg_type, payload):
        """Creates a new server-side message, initialized with parsed data.

        Args:
            msg_type (int): Message type.
            payload (string): Serialized message data.

        Returns:
            dict: The dictionary representing a message containing parsed data.
        """
        return _mysqlxpb.parse_server_message(msg_type, payload)

    @classmethod
    def from_message(cls, msg_type_name, payload):
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
        msg.set_message(_mysqlxpb.parse_message(msg_type_name, payload))
        return msg

    @classmethod
    def from_server_message(cls, msg_type, payload):
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
        msg.set_message(_mysqlxpb.parse_server_message(msg_type, payload))
        return msg
