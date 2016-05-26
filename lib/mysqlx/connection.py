# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

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

"""Implementation of MySQLx servers enabled communication."""

import socket

from .protocol import Protocol, MessageReaderWriter
from .authentication import MySQL41AuthPlugin
from .result import Result, RowResult
from .crud import Schema
from .statement import SqlStatement

_DROP_DATABASE_QUERY = "DROP DATABASE IF EXISTS `{0}`"
_CREATE_DATABASE_QUERY = "CREATE DATABASE IF NOT EXISTS `{0}`"


class SocketStream(object):
    def __init__(self):
        self._socket = None

    def connect(self, host, port):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((host, port,))

    def read(self, count):
        buf = ""
        while count > 0:
            data = self._socket.recv(count)
            if data == "":
                raise RuntimeError("Unexpected connection close")
            buf += data
            count -= len(data)
        return buf

    def sendall(self, data):
        self._socket.sendall(data)


class Connection(object):
    def __init__(self, settings):
        self._host = settings.get("host", "localhost")
        self._port = settings.get("port", 33060)
        self._user = settings.get("user")
        self._password = settings.get("password")
        self.stream = SocketStream()
        self.reader_writer = None
        self.protocol = None

    def connect(self):
        self.stream.connect(self._host, self._port)
        self.reader_writer = MessageReaderWriter(self.stream)
        self.protocol = Protocol(self.reader_writer)
        self._handle_capabilities()
        self._authenticate()

    def _handle_capabilities(self):
        # TODO: To implement
        # caps = mysqlx_connection_pb2.CapabilitiesGet()
        # data = caps.SerializeToString()
        pass

    def _authenticate(self):
        plugin = MySQL41AuthPlugin(self._user, self._password)
        self.protocol.send_auth_start(plugin.auth_name())
        extra_data = self.protocol.read_auth_continue()
        self.protocol.send_auth_continue(
            plugin.build_authentication_response(extra_data))
        self.protocol.read_auth_ok()

    def send_sql(self, sql, *args):
        self.protocol.send_execute_statement("sql", sql, args)

    def send_doc_insert(self, statement):
        self.protocol.send_doc_insert(statement.schema.name,
                                  statement.target.name,
                                  statement._docs)
        return Result(self)

    def send_delete(self, statement, is_docs):
        # TODO: make sure statement is a FilterableStatement
        self.protocol.send_delete(statement.schema.name,
                                  statement.target.name, is_docs,
                                  statement.filter)
        return Result(self)

    def execute_nonquery(self, namespace, cmd, raise_on_fail=True, *args):
        self.protocol.send_execute_statement(namespace, cmd, args)
        return Result(self)

    def execute_sql_scalar(self, sql, *args):
        self.protocol.send_execute_statement("sql", sql, args)
        result = RowResult(self)
        result.fetch_all()
        if result.count == 0:
            raise Exception("No data found")
        return result[0][0]

    def get_row_result(self, cmd, *args):
        self.protocol.send_execute_statement("xplugin", cmd, args)
        return RowResult(self)


class BaseSession(object):
    def __init__(self, settings):
        self._settings = settings
        self._connection = Connection(self._settings)
        self._connection.connect()

    def get_schema(self, name):
        return Schema(self, name)

    def drop_schema(self, name):
        self._connection.execute_nonquery(
            "sql", _DROP_DATABASE_QUERY.format(name), True)

    def create_schema(self, name):
        self._connection.execute_nonquery(
            "sql", _CREATE_DATABASE_QUERY.format(name), True)
        return Schema(self, name)


class XSession(BaseSession):
    def __init__(self, settings):
        super(XSession, self).__init__(settings)


class NodeSession(BaseSession):
    def __init__(self, settings):
        super(NodeSession, self).__init__(settings)

    def sql(self, sql):
        return SqlStatement(self._connection, sql)
