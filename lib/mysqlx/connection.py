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

"""Implementation of communication for MySQL X servers."""

try:
    import ssl
    SSL_AVAILABLE = True
except:
    SSL_AVAILABLE = False

import sys
import socket

from functools import wraps

from .authentication import MySQL41AuthPlugin
from .errors import InterfaceError, OperationalError, ProgrammingError
from .crud import Schema
from .protocol import Protocol, MessageReaderWriter
from .result import Result, RowResult, DocResult
from .statement import SqlStatement, AddStatement


_DROP_DATABASE_QUERY = "DROP DATABASE IF EXISTS `{0}`"
_CREATE_DATABASE_QUERY = "CREATE DATABASE IF NOT EXISTS `{0}`"


class SocketStream(object):
    def __init__(self):
        self._socket = None
        self._is_ssl = False

    def connect(self, params):
        if isinstance(params, tuple):
            s_type = socket.AF_INET6 if ":" in params[0] else socket.AF_INET
        else:
            s_type = socket.AF_UNIX
        self._socket = socket.socket(s_type, socket.SOCK_STREAM)
        self._socket.connect(params)

    def read(self, count):
        if self._socket is None:
            raise OperationalError("MySQLx Connection not available")
        buf = []
        while count > 0:
            data = self._socket.recv(count)
            if data == b"":
                raise RuntimeError("Unexpected connection close")
            buf.append(data)
            count -= len(data)
        return b"".join(buf)

    def sendall(self, data):
        if self._socket is None:
            raise OperationalError("MySQLx Connection not available")
        self._socket.sendall(data)

    def close(self):
        if not self._socket:
            return

        self._socket.close()
        self._socket = None

    def set_ssl(self, ssl_opts={}):
        if not SSL_AVAILABLE:
            self.close()
            raise RuntimeError("Python installation has no SSL support.")

        context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        context.load_default_certs()
        if "ssl-ca" in ssl_opts:
            try:
                context.load_verify_locations(ssl_opts["ssl-ca"])
                context.verify_mode = ssl.CERT_REQUIRED
            except (IOError, ssl.SSLError):
                self.close()
                raise InterfaceError("Invalid CA certificate.")
        if "ssl-crl" in ssl_opts:
            try:
                context.load_verify_locations(ssl_opts["ssl-crl"])
                context.verify_flags = ssl.VERIFY_CRL_CHECK_CHAIN
            except (IOError, ssl.SSLError):
                self.close()
                raise InterfaceError("Invalid CRL.")
        if "ssl-cert" in ssl_opts:
            try:
                context.load_cert_chain(ssl_opts["ssl-cert"],
                    ssl_opts.get("ssl-key", None))
            except (IOError, ssl.SSLError):
                self.close()
                raise InterfaceError("Invalid Client Certificate/Key.")
        elif "ssl-key" in ssl_opts:
            self.close()
            raise InterfaceError("Client Certificate not provided.")

        self._socket = context.wrap_socket(self._socket)
        self._is_ssl = True


def catch_network_exception(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except (socket.error, RuntimeError):
            self.disconnect()
            raise InterfaceError("Cannot connect to host.")
    return wrapper


class Connection(object):
    def __init__(self, settings):
        self._user = settings.get("user")
        self._password = settings.get("password")
        self._schema = settings.get("schema")
        self._active_result = None
        self.settings = settings
        self.stream = SocketStream()
        self.reader_writer = None
        self.protocol = None

    def fetch_active_result(self):
        if self._active_result is not None:
            self._active_result.fetch_all()
            self._active_result = None

    def _connection_params(self):
        if "host" in self.settings:
            return self.settings["host"], self.settings.get("port", 33060)
        if "socket" in self.settings:
            return self.settings["socket"]
        return ("localhost", 33060,)

    def connect(self):
        self.stream.connect(self._connection_params())
        self.reader_writer = MessageReaderWriter(self.stream)
        self.protocol = Protocol(self.reader_writer)
        self._handle_capabilities()
        self._authenticate()

    def _handle_capabilities(self):
        data = self.protocol.get_capabilites().capabilities
        if not (data[0]["name"].lower() == "tls" if data else False):
            if self.settings.get("ssl-enable", False):
                self.close()
                raise OperationalError("SSL not enabled at server.")
            return

        if sys.version_info < (2, 7, 9):
            if self.settings.get("ssl-enable", False):
                self.close()
                raise RuntimeError("The support for SSL is not available for "
                    "this Python version.")
            return

        self.protocol.set_capabilities(tls=True)
        self.stream.set_ssl(self.settings)

    def _authenticate(self):
        plugin = MySQL41AuthPlugin(self._user, self._password)
        self.protocol.send_auth_start(plugin.auth_name())
        extra_data = self.protocol.read_auth_continue()
        self.protocol.send_auth_continue(
            plugin.build_authentication_response(extra_data))
        self.protocol.read_auth_ok()

    @catch_network_exception
    def send_sql(self, sql, *args):
        self.protocol.send_execute_statement("sql", sql, args)

    @catch_network_exception
    def send_insert(self, statement):
        self.protocol.send_insert(statement)
        ids = None
        if isinstance(statement, AddStatement):
            ids = statement._ids
        return Result(self, ids)

    @catch_network_exception
    def find(self, statement):
        self.protocol.send_find(statement)
        return DocResult(self) if statement._doc_based else RowResult(self)

    @catch_network_exception
    def delete(self, statement):
        self.protocol.send_delete(statement)
        return Result(self)

    @catch_network_exception
    def update(self, statement):
        self.protocol.send_update(statement)
        return Result(self)

    @catch_network_exception
    def execute_nonquery(self, namespace, cmd, raise_on_fail=True, *args):
        self.protocol.send_execute_statement(namespace, cmd, args)
        return Result(self)

    @catch_network_exception
    def execute_sql_scalar(self, sql, *args):
        self.protocol.send_execute_statement("sql", sql, args)
        result = RowResult(self)
        result.fetch_all()
        if result.count == 0:
            raise InterfaceError("No data found")
        return result[0][0]

    @catch_network_exception
    def get_row_result(self, cmd, *args):
        self.protocol.send_execute_statement("xplugin", cmd, args)
        return RowResult(self)

    @catch_network_exception
    def read_row(self, result):
        return self.protocol.read_row(result)

    @catch_network_exception
    def close_result(self, result):
        self.protocol.close_result(result)

    @catch_network_exception
    def get_column_metadata(self, result):
        return self.protocol.get_column_metadata(result)

    def is_open(self):
        return self.stream._socket is not None

    def disconnect(self):
        if not self.is_open():
            return
        self.stream.close()

    def close(self):
        if not self.is_open():
            return
        if self._active_result is not None:
            self._active_result.fetch_all()
        self.protocol.send_close()
        self.protocol.read_ok()
        self.stream.close()


class XConnection(Connection):
    def __init__(self, settings):
        super(XConnection, self).__init__(settings)
        self.dependent_connections = []
        self._routers = settings.pop("routers", [])

        if 'host' in settings and settings['host']:
            self._routers.append({
                'host': settings.pop('host'),
                'port': settings.pop('port', None)
            })

        self._cur_router = -1
        self._can_failover = True
        self._ensure_priorities()
        self._routers.sort(key=lambda x: x['priority'], reverse=True)

    def _ensure_priorities(self):
        priority_count = 0
        priority = 100

        for router in self._routers:
            pri = router.get('priority', None)
            if pri is None:
                priority_count += 1
                router["priority"] = priority
            elif pri > 100:
                raise ProgrammingError("The priorities must be between 0 and "
                    "100", 4007)
            priority -= 1

        if 0 < priority_count < len(self._routers):
            raise ProgrammingError("You must either assign no priority to any "
                "of the routers or give a priority for every router", 4000)

    def _connection_params(self):
        if not self._routers:
            self._can_failover = False
            return super(XConnection, self)._connection_params()

        # Reset routers status once all are tried
        if not self._can_failover or self._cur_router is -1:
            self._cur_router = -1
            self._can_failover = True
            for router in self._routers:
                router['available'] = True

        self._cur_router += 1
        host = self._routers[self._cur_router]["host"]
        port = self._routers[self._cur_router]["port"]

        if self._cur_router > 0:
            self._routers[self._cur_router-1]["available"] = False
        if self._cur_router >= len(self._routers) - 1:
            self._can_failover = False

        return (host, port,)

    def connect(self):
        # Loop and check
        error = None
        while self._can_failover:
            try:
                return super(XConnection, self).connect()
            except socket.error as err:
                error = err

        if len(self._routers) <= 1:
            raise InterfaceError("Cannot connect to host: {0}".format(error))
        raise InterfaceError("Failed to connect to any of the routers.", 4001)

    def bind_connection(self, connection):
        self.dependent_connections.append(connection)

    def close(self):
        while self.dependent_connections:
            self.dependent_connections.pop().close()
        super(XConnection, self).close()

    def disconnect(self):
        while self.dependent_connections:
            self.dependent_connections.pop().disconnect()
        super(XConnection, self).disconnect()


class NodeConnection(Connection):
    def __init__(self, settings):
        super(NodeConnection, self).__init__(settings)

    def connect(self):
        try:
            super(NodeConnection, self).connect()
        except socket.error as err:
            raise InterfaceError("Cannot connect to host: {0}".format(err))


class BaseSession(object):
    """Base functionality for Session classes through the X Protocol.

    This class encloses the core functionality to be made available on both
    the XSession and NodeSession classes, such functionality includes:

        - Accessing available schemas.
        - Schema management operations.
        - Enabling/disabling warning generation.
        - Retrieval of connection information.

    Args:
        settings (dict): Connection data used to connect to the database.
    """
    def __init__(self, settings):
        self._settings = settings

    def is_open(self):
        return self._connection.stream._socket is not None

    def get_schema(self, name):
        """Retrieves a Schema object from the current session by it's name.

        Args:
            name (string): The name of the Schema object to be retrieved.

        Returns:
            mysqlx.Schema: The Schema object with the given name.
        """
        return Schema(self, name)

    def get_default_schema(self):
        """Retrieves a Schema object from the current session by the schema
        name configured in the connection settings.

        Returns:
            mysqlx.Schema: The Schema object with the given name at connect
                           time.

        Raises:
            ProgrammingError: If default schema not provided.
        """
        if self._connection.settings.get("schema"):
            return Schema(self, self._connection.settings["schema"])
        raise ProgrammingError("Default schema not provided")

    def drop_schema(self, name):
        """Drops the schema with the specified name.

        Args:
            name (string): The name of the Schema object to be retrieved.
        """
        self._connection.execute_nonquery(
            "sql", _DROP_DATABASE_QUERY.format(name), True)

    def create_schema(self, name):
        """Creates a schema on the database and returns the corresponding
        object.

        Args:
            name (string): A string value indicating the schema name.
        """
        self._connection.execute_nonquery(
            "sql", _CREATE_DATABASE_QUERY.format(name), True)
        return Schema(self, name)

    def start_transaction(self):
        """Starts a transaction context on the server.
        """
        self._connection.execute_nonquery("sql", "START TRANSACTION", True)

    def commit(self):
        """Commits all the operations executed after a call to
        startTransaction().
        """
        self._connection.execute_nonquery("sql", "COMMIT", True)

    def rollback(self):
        """Discards all the operations executed after a call to
        startTransaction().
        """
        self._connection.execute_nonquery("sql", "ROLLBACK", True)

    def close(self):
        self._connection.close()


class XSession(BaseSession):
    """Enables interaction with a X Protocol enabled MySQL Product.

    The functionality includes:

    - Accessing available schemas.
    - Schema management operations.
    - Enabling/disabling warning generation.
    - Retrieval of connection information.

    Args:
        settings (dict): Connection data used to connect to the database.
    """
    def __init__(self, settings):
        super(XSession, self).__init__(settings)
        self._connection = XConnection(self._settings)
        self._connection.connect()

    def bind_to_default_shard(self):
        if not self.is_open():
            raise OperationalError("XSession is not connected to a farm.")

        nsess = NodeSession(self._settings)
        self._connection.bind_connection(nsess._connection)
        return nsess


class NodeSession(BaseSession):
    """Enables interaction with a X Protocol enabled MySQL Server.

    The functionality includes:

    - Accessing available schemas.
    - Schema management operations.
    - Enabling/disabling warning generation.
    - Retrieval of connection information.
    - Includes SQL Execution.

    Args:
        settings (dict): Connection data used to connect to the database.
    """
    def __init__(self, settings):
        super(NodeSession, self).__init__(settings)
        self._connection = NodeConnection(self._settings)
        self._connection.connect()

    def sql(self, sql):
        """Creates a :class:`mysqlx.SqlStatement` object to allow running the
        SQL statement on the target MySQL Server.
        """
        return SqlStatement(self._connection, sql)
