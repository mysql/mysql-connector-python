# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.

# MySQL Connector/Python is licensed under the terms of the GPLv2
# <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most
# MySQL Connectors. There are special exceptions to the terms and
# conditions of the GPLv2 as it is applied to this software, see the
# FOSS License Exception
# <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA

"""This module contains the implementation of a helper class for MySQL X
Protobuf messages."""

try:
    import _mysqlxpb
except ImportError:
    raise RuntimeError("MySQL X Protobuf extension is not available")


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


SERVER_MESSAGES = dict([(int(_mysqlxpb.enum_value(key)), val)
                        for key, val in _SERVER_MESSAGES_TUPLES])

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
            if msg_type_name else {}
        for key, value in kwargs.items():
            self.__dict__["_msg"][key] = value.get_message() \
                if isinstance(value, Message) else value

    def __setattr__(self, name, value):
        self._msg[name] = value.get_message() \
            if isinstance(value, Message) else value

    def __getattr__(self, name):
        try:
            return self._msg[name]
        except KeyError:
            raise AttributeError

    def __setitem__(self, name, value):
        self._msg[name] = value.get_message() \
            if isinstance(value, Message) else value

    def __getitem__(self, name):
        try:
            return self._msg[name]
        except KeyError:
            raise AttributeError

    def get(self, name, default=None):
        """Returns the value of an element of the message dictionary.

        Args:
            name (string): Key name.
            default (object): The default value if the key does not exists.

        Returns:
            object: The value of the provided key name.
        """
        return self.__dict__["_msg"].get(name, default)

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
        return self._msg["_mysqlxpb_type_name"]

    @staticmethod
    def parse(msg_type, payload):
        """Creates a new message, initialized with parsed data.

        Args:
            msg_type (string): Message type name.
            payload (string): Serialized message data.

        Returns:
            dict: The dictionary representing a message containing parsed data.
        """
        return _mysqlxpb.parse_message(msg_type, payload)

    @staticmethod
    def parse_from_server(msg_type, payload):
        """Creates a new server-side message, initialized with parsed data.

        Args:
            msg_type (string): Message type name.
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
            msg_type (string): Message type name.
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
            msg_type (string): Message type name.
            payload (string): Serialized message data.

        Returns:
            mysqlx.protobuf.Message: The Message representing a message
                                     containing parsed data.
        """
        msg = cls()
        msg.set_message(_mysqlxpb.parse_server_message(msg_type, payload))
        return msg
