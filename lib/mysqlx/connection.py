# Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
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

"""Implementation of communication for MySQL X servers."""

try:
    import ssl
    SSL_AVAILABLE = True
except:
    SSL_AVAILABLE = False

import sys
import socket
import logging
import uuid
import platform
import os

from functools import wraps

from .authentication import (MySQL41AuthPlugin, PlainAuthPlugin,
                             Sha256MemoryAuthPlugin)
from .errors import InterfaceError, OperationalError, ProgrammingError
from .compat import PY3, STRING_TYPES, UNICODE_TYPES
from .crud import Schema
from .constants import SSLMode, Auth
from .helpers import get_item_or_attr
from .protocol import Protocol, MessageReaderWriter
from .result import Result, RowResult, DocResult
from .statement import SqlStatement, AddStatement, quote_identifier
from .protobuf import Protobuf


_DROP_DATABASE_QUERY = "DROP DATABASE IF EXISTS `{0}`"
_CREATE_DATABASE_QUERY = "CREATE DATABASE IF NOT EXISTS `{0}`"
_LOGGER = logging.getLogger("mysqlx")

class SocketStream(object):
    """Implements a socket stream."""
    def __init__(self):
        self._socket = None
        self._is_ssl = False
        self._is_socket = False
        self._host = None

    def connect(self, params):
        """Connects to a TCP service.

        Args:
            params (tuple): The connection parameters.

        Raises:
            :class:`mysqlx.InterfaceError`: If Unix socket is not supported.
        """
        try:
            self._socket = socket.create_connection(params)
            self._host = params[0]
        except ValueError:
            try:
                self._socket = socket.socket(socket.AF_UNIX)
                self._is_socket = True
                self._socket.connect(params)
            except AttributeError:
                raise InterfaceError("Unix socket unsupported")

    def read(self, count):
        """Receive data from the socket.

        Args:
            count (int): Buffer size.

        Returns:
            bytes: The data received.
        """
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
        """Send data to the socket.

        Args:
            data (bytes): The data to be sent.
        """
        if self._socket is None:
            raise OperationalError("MySQLx Connection not available")
        self._socket.sendall(data)

    def close(self):
        """Close the socket."""
        if not self._socket:
            return

        self._socket.close()
        self._socket = None

    def set_ssl(self, ssl_mode, ssl_ca, ssl_crl, ssl_cert, ssl_key):
        """Set SSL parameters.

        Args:
            ssl_mode (str): SSL mode.
            ssl_ca (str): The certification authority certificate.
            ssl_crl (str): The certification revocation lists.
            ssl_cert (str): The certificate.
            ssl_key (str): The certificate key.

        Raises:
            :class:`mysqlx.RuntimeError`: If Python installation has no SSL
                                          support.
            :class:`mysqlx.InterfaceError`: If the parameters are invalid.
        """
        if not SSL_AVAILABLE:
            self.close()
            raise RuntimeError("Python installation has no SSL support")

        context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        context.load_default_certs()

        if ssl_ca:
            try:
                context.load_verify_locations(ssl_ca)
                context.verify_mode = ssl.CERT_REQUIRED
            except (IOError, ssl.SSLError) as err:
                self.close()
                raise InterfaceError("Invalid CA Certificate: {}".format(err))

        if ssl_crl:
            try:
                context.load_verify_locations(ssl_crl)
                context.verify_flags = ssl.VERIFY_CRL_CHECK_LEAF
            except (IOError, ssl.SSLError) as err:
                self.close()
                raise InterfaceError("Invalid CRL: {}".format(err))

        if ssl_cert:
            try:
                context.load_cert_chain(ssl_cert, ssl_key)
            except (IOError, ssl.SSLError) as err:
                self.close()
                raise InterfaceError("Invalid Certificate/Key: {}".format(err))

        self._socket = context.wrap_socket(self._socket)
        if ssl_mode == SSLMode.VERIFY_IDENTITY:
            hostnames = []
            # Windows does not return loopback aliases on gethostbyaddr
            if os.name == 'nt' and (self._host == 'localhost' or \
               self._host == '127.0.0.1'):
                hostnames = ['localhost', '127.0.0.1']
            aliases = socket.gethostbyaddr(self._host)
            hostnames.extend([aliases[0]] + aliases[1])
            match_found = False
            errs = []
            for hostname in hostnames:
                try:
                    ssl.match_hostname(self._socket.getpeercert(), hostname)
                except ssl.CertificateError as err:
                    errs.append(err)
                else:
                    match_found = True
                    break
            if not match_found:
                self.close()
                raise InterfaceError("Unable to verify server identity: {}"
                                     "".format(", ".join(errs)))
        self._is_ssl = True

    def is_ssl(self):
        """Verifies if SSL is being used.

        Returns:
            bool: Returns `True` if SSL is being used.
        """
        return self._is_ssl

    def is_socket(self):
        """Verifies if socket connection is being used.

        Returns:
            bool: Returns `True` if socket connection is being used.
        """
        return self._is_socket

    def is_secure(self):
        """Verifies if connection is secure.

        Returns:
            bool: Returns `True` if connection is secure.
        """
        return self._is_ssl or self._is_socket

    def is_open(self):
        """Verifies if connection is open.

        Returns:
            bool: Returns `True` if connection is open.
        """
        return self._socket is not None


def catch_network_exception(func):
    """Decorator used to catch socket.error or RuntimeError.

    Raises:
        :class:`mysqlx.InterfaceError`: If `socket.Error` or `RuntimeError`
                                        is raised.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        """Wrapper function."""
        try:
            return func(self, *args, **kwargs)
        except (socket.error, RuntimeError):
            self.disconnect()
            raise InterfaceError("Cannot connect to host")
    return wrapper


class Connection(object):
    """Connection to a MySQL Server.

    Args:
        settings (dict): Dictionary with connection settings.
    """
    def __init__(self, settings):
        self.settings = settings
        self.stream = SocketStream()
        self.reader_writer = None
        self.protocol = None
        self._user = settings.get("user")
        self._password = settings.get("password")
        self._schema = settings.get("schema")
        self._active_result = None
        self._routers = settings.get("routers", [])

        if 'host' in settings and settings['host']:
            self._routers.append({
                'host': settings.get('host'),
                'port': settings.get('port', None)
            })

        self._cur_router = -1
        self._can_failover = True
        self._ensure_priorities()
        self._routers.sort(key=lambda x: x['priority'], reverse=True)

    def fetch_active_result(self):
        """Fetch active result."""
        if self._active_result is not None:
            self._active_result.fetch_all()
            self._active_result = None

    def set_active_result(self, result):
        """Set active result.

        Args:
            `Result`: It can be :class:`mysqlx.Result`,
                      :class:`mysqlx.BufferingResult`,
                      :class:`mysqlx.RowResult`, :class:`mysqlx.SqlResult` or
                      :class:`mysqlx.DocResult`.
        """
        self._active_result = result

    def _ensure_priorities(self):
        """Ensure priorities.

        Raises:
            :class:`mysqlx.ProgrammingError`: If priorities are invalid.
        """
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
                                   "of the routers or give a priority for "
                                   "every router", 4000)

    def _get_connection_params(self):
        """Returns the connection parameters.

        Returns:
            tuple: The connection parameters.
        """
        if not self._routers:
            self._can_failover = False
            if "host" in self.settings:
                return self.settings["host"], self.settings.get("port", 33060)
            if "socket" in self.settings:
                return self.settings["socket"]
            return ("localhost", 33060,)

        # Reset routers status once all are tried
        if not self._can_failover or self._cur_router == -1:
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
        """Attempt to connect to the MySQL server.

        Raises:
            :class:`mysqlx.InterfaceError`: If fails to connect to the MySQL
                                            server.
        """
        # Loop and check
        error = None
        while self._can_failover:
            try:
                self.stream.connect(self._get_connection_params())
                self.reader_writer = MessageReaderWriter(self.stream)
                self.protocol = Protocol(self.reader_writer)
                self._handle_capabilities()
                self._authenticate()
                return
            except socket.error as err:
                error = err

        if len(self._routers) <= 1:
            raise InterfaceError("Cannot connect to host: {0}".format(error))
        raise InterfaceError("Failed to connect to any of the routers", 4001)

    def _handle_capabilities(self):
        """Handle capabilities.

        Raises:
            :class:`mysqlx.OperationalError`: If SSL is not enabled at the
                                             server.
            :class:`mysqlx.RuntimeError`: If support for SSL is not available
                                          in Python.
        """
        if self.settings.get("ssl-mode") == SSLMode.DISABLED:
            return
        if self.stream.is_socket():
            if self.settings.get("ssl-mode"):
                _LOGGER.warning("SSL not required when using Unix socket.")
            return

        data = self.protocol.get_capabilites().capabilities
        if not (get_item_or_attr(data[0], "name").lower() == "tls"
                if data else False):
            self.close_connection()
            raise OperationalError("SSL not enabled at server")

        is_ol7 = False
        if platform.system() == "Linux":
            # pylint: disable=W1505
            distname, version, _ = platform.linux_distribution()
            try:
                is_ol7 = "Oracle Linux" in distname and \
                    version.split(".")[0] == "7"
            except IndexError:
                is_ol7 = False

        if sys.version_info < (2, 7, 9) and not is_ol7:
            self.close_connection()
            raise RuntimeError("The support for SSL is not available for "
                               "this Python version")

        self.protocol.set_capabilities(tls=True)
        self.stream.set_ssl(self.settings.get("ssl-mode", SSLMode.REQUIRED),
                            self.settings.get("ssl-ca"),
                            self.settings.get("ssl-crl"),
                            self.settings.get("ssl-cert"),
                            self.settings.get("ssl-key"))

    def _authenticate(self):
        """Authenticate with the MySQL server."""
        auth = self.settings.get("auth")
        if auth:
            if auth == Auth.PLAIN:
                self._authenticate_plain()
            elif auth == Auth.SHA256_MEMORY:
                self._authenticate_sha256_memory()
            elif auth == Auth.MYSQL41:
                self._authenticate_mysql41()
        elif self.stream.is_secure():
            # Use PLAIN if no auth provided and connection is secure
            self._authenticate_plain()
        else:
            # Use MYSQL41 if connection is not secure
            try:
                self._authenticate_mysql41()
            except InterfaceError:
                pass
            else:
                return
            # Try SHA256_MEMORY if MYSQL41 fails
            try:
                self._authenticate_sha256_memory()
            except InterfaceError:
                raise InterfaceError("Authentication failed using MYSQL41 and "
                                     "SHA256_MEMORY, check username and "
                                     "password or try a secure connection")

    def _authenticate_mysql41(self):
        """Authenticate with the MySQL server using `MySQL41AuthPlugin`."""
        plugin = MySQL41AuthPlugin(self._user, self._password)
        self.protocol.send_auth_start(plugin.auth_name())
        extra_data = self.protocol.read_auth_continue()
        self.protocol.send_auth_continue(plugin.auth_data(extra_data))
        self.protocol.read_auth_ok()

    def _authenticate_plain(self):
        """Authenticate with the MySQL server using `PlainAuthPlugin`."""
        if not self.stream.is_secure():
            raise InterfaceError("PLAIN authentication is not allowed via "
                                 "unencrypted connection")
        plugin = PlainAuthPlugin(self._user, self._password)
        self.protocol.send_auth_start(plugin.auth_name(),
                                      auth_data=plugin.auth_data())
        self.protocol.read_auth_ok()

    def _authenticate_sha256_memory(self):
        """Authenticate with the MySQL server using `Sha256MemoryAuthPlugin`."""
        plugin = Sha256MemoryAuthPlugin(self._user, self._password)
        self.protocol.send_auth_start(plugin.auth_name())
        extra_data = self.protocol.read_auth_continue()
        self.protocol.send_auth_continue(plugin.auth_data(extra_data))
        self.protocol.read_auth_ok()

    @catch_network_exception
    def send_sql(self, sql, *args):
        """Execute a SQL statement.

        Args:
            sql (str): The SQL statement.
            *args: Arbitrary arguments.

        Raises:
            :class:`mysqlx.ProgrammingError`: If the SQL statement is not a
                                              valid string.
        """
        if not isinstance(sql, STRING_TYPES):
            raise ProgrammingError("The SQL statement is not a valid string")
        elif not PY3 and isinstance(sql, UNICODE_TYPES):
            self.protocol.send_execute_statement(
                "sql", bytes(bytearray(sql, "utf-8")), args)
        else:
            self.protocol.send_execute_statement("sql", sql, args)

    @catch_network_exception
    def send_insert(self, statement):
        """Send an insert statement.

        Args:
            statement (`Statement`): It can be :class:`mysqlx.InsertStatement`
                                     or :class:`mysqlx.AddStatement`.

        Returns:
            :class:`mysqlx.Result`: A result object.
        """
        self.protocol.send_insert(statement)
        ids = None
        if isinstance(statement, AddStatement):
            ids = statement.ids
        return Result(self, ids)

    @catch_network_exception
    def find(self, statement):
        """Send an find statement.

        Args:
            statement (`Statement`): It can be :class:`mysqlx.ReadStatement`
                                     or :class:`mysqlx.FindStatement`.

        Returns:
            `Result`: It can be class:`mysqlx.DocResult` or
                      :class:`mysqlx.RowResult`.
        """
        self.protocol.send_find(statement)
        return DocResult(self) if statement.is_doc_based() else RowResult(self)

    @catch_network_exception
    def delete(self, statement):
        """Send an delete statement.

        Args:
            statement (`Statement`): It can be :class:`mysqlx.RemoveStatement`
                                     or :class:`mysqlx.DeleteStatement`.

        Returns:
            :class:`mysqlx.Result`: The result object.
        """
        self.protocol.send_delete(statement)
        return Result(self)

    @catch_network_exception
    def update(self, statement):
        """Send an delete statement.

        Args:
            statement (`Statement`): It can be :class:`mysqlx.ModifyStatement`
                                     or :class:`mysqlx.UpdateStatement`.

        Returns:
            :class:`mysqlx.Result`: The result object.
        """
        self.protocol.send_update(statement)
        return Result(self)

    @catch_network_exception
    def execute_nonquery(self, namespace, cmd, raise_on_fail, *args):
        """Execute a non query command.

        Args:
            namespace (str): The namespace.
            cmd (str): The command.
            raise_on_fail (bool): `True` to raise on fail.
            *args: Arbitrary arguments.

        Raises:
            :class:`mysqlx.OperationalError`: On errors.

        Returns:
            :class:`mysqlx.Result`: The result object.
        """
        try:
            self.protocol.send_execute_statement(namespace, cmd, args)
            return Result(self)
        except OperationalError:
            if raise_on_fail:
                raise

    @catch_network_exception
    def execute_sql_scalar(self, sql, *args):
        """Execute a SQL scalar.

        Args:
            sql (str): The SQL statement.
            *args: Arbitrary arguments.

        Raises:
            :class:`mysqlx.InterfaceError`: If no data found.

        Returns:
            :class:`mysqlx.Result`: The result.
        """
        self.protocol.send_execute_statement("sql", sql, args)
        result = RowResult(self)
        result.fetch_all()
        if result.count == 0:
            raise InterfaceError("No data found")
        return result[0][0]

    @catch_network_exception
    def get_row_result(self, cmd, *args):
        """Returns the row result.

        Args:
            cmd (str): The command.
            *args: Arbitrary arguments.

        Returns:
            :class:`mysqlx.RowResult`: The result object.
        """
        self.protocol.send_execute_statement("xplugin", cmd, args)
        return RowResult(self)

    @catch_network_exception
    def read_row(self, result):
        """Read row.

        Args:
            result (:class:`mysqlx.RowResult`): The result object.
        """
        return self.protocol.read_row(result)

    @catch_network_exception
    def close_result(self, result):
        """Close result.

        Args:
            result (:class:`mysqlx.Result`): The result object.
        """
        self.protocol.close_result(result)

    @catch_network_exception
    def get_column_metadata(self, result):
        """Get column metadata.

        Args:
            result (:class:`mysqlx.Result`): The result object.
        """
        return self.protocol.get_column_metadata(result)

    def is_open(self):
        """Check if connection is open.

        Returns:
            bool: `True` if connection is open.
        """
        return self.stream.is_open()

    def disconnect(self):
        """Disconnect from server."""
        if not self.is_open():
            return
        self.stream.close()

    def close_session(self):
        """Close a sucessfully authenticated session."""
        if not self.is_open():
            return
        if self._active_result is not None:
            self._active_result.fetch_all()
        self.protocol.send_close()
        self.protocol.read_ok()
        self.stream.close()

    def close_connection(self):
        """Announce to the server that the client wants to close the
        connection. Discards any session state of the server.
        """
        if not self.is_open():
            return
        if self._active_result is not None:
            self._active_result.fetch_all()
        self.protocol.send_connection_close()
        self.protocol.read_ok()
        self.stream.close()


class Session(object):
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
        self.use_pure = settings.get("use-pure", Protobuf.use_pure)
        self._settings = settings
        self._connection = Connection(self._settings)
        self._connection.connect()

    @property
    def use_pure(self):
        """bool: `True` to use pure Python Protobuf implementation.
        """
        return Protobuf.use_pure

    @use_pure.setter
    def use_pure(self, value):
        if not isinstance(value, bool):
            raise ProgrammingError("'use_pure' option should be True or False")
        Protobuf.set_use_pure(value)

    def is_open(self):
        """Returns `True` if the session is open.

        Returns:
            bool: Returns `True` if the session is open.
        """
        return self._connection.stream.is_open()

    def sql(self, sql):
        """Creates a :class:`mysqlx.SqlStatement` object to allow running the
        SQL statement on the target MySQL Server.
        """
        return SqlStatement(self._connection, sql)

    def get_connection(self):
        """Returns the underlying connection.

        Returns:
            mysqlx.connection.Connection: The connection object.
        """
        return self._connection

    def get_schemas(self):
        """Returns the list of schemas in the current session.

        Returns:
            `list`: The list of schemas in the current session.

        .. versionadded:: 8.0.12
        """
        result = self.sql("SHOW DATABASES").execute()
        return [row[0] for row in result.fetch_all()]

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
            :class:`mysqlx.ProgrammingError`: If default schema not provided.
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
        """Starts a transaction context on the server."""
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

    def set_savepoint(self, name=None):
        """Creates a transaction savepoint.

        If a name is not provided, one will be generated using the uuid.uuid1()
        function.

        Args:
            name (Optional[string]): The savepoint name.

        Returns:
            string: The savepoint name.
        """
        if name is None:
            name = "{0}".format(uuid.uuid1())
        elif not isinstance(name, STRING_TYPES) or len(name.strip()) == 0:
            raise ProgrammingError("Invalid SAVEPOINT name")
        self._connection.execute_nonquery("sql", "SAVEPOINT {0}"
                                          "".format(quote_identifier(name)),
                                          True)
        return name

    def rollback_to(self, name):
        """Rollback to a transaction savepoint with the given name.

        Args:
            name (string): The savepoint name.
        """
        if not isinstance(name, STRING_TYPES) or len(name.strip()) == 0:
            raise ProgrammingError("Invalid SAVEPOINT name")
        self._connection.execute_nonquery("sql", "ROLLBACK TO SAVEPOINT {0}"
                                          "".format(quote_identifier(name)),
                                          True)

    def release_savepoint(self, name):
        """Release a transaction savepoint with the given name.

        Args:
            name (string): The savepoint name.
        """
        if not isinstance(name, STRING_TYPES) or len(name.strip()) == 0:
            raise ProgrammingError("Invalid SAVEPOINT name")
        self._connection.execute_nonquery("sql", "RELEASE SAVEPOINT {0}"
                                          "".format(quote_identifier(name)),
                                          True)

    def close(self):
        """Closes the session."""
        self._connection.close_session()
