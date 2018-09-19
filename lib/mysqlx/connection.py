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
import re
import threading

from functools import wraps

from .authentication import (MySQL41AuthPlugin, PlainAuthPlugin,
                             Sha256MemoryAuthPlugin)
# pylint: disable=W0622
from .errors import (InterfaceError, OperationalError, PoolError,
                     ProgrammingError, TimeoutError)
from .compat import PY3, STRING_TYPES, UNICODE_TYPES, queue
from .crud import Schema
from .constants import SSLMode, Auth
from .helpers import get_item_or_attr
from .protocol import Protocol, MessageReaderWriter
from .result import Result, RowResult, DocResult
from .statement import SqlStatement, AddStatement, quote_identifier
from .protobuf import Protobuf


_CONNECT_TIMEOUT = 10000  # Default connect timeout in milliseconds
_DROP_DATABASE_QUERY = "DROP DATABASE IF EXISTS `{0}`"
_CREATE_DATABASE_QUERY = "CREATE DATABASE IF NOT EXISTS `{0}`"

_CNX_POOL_MAXSIZE = 99
_CNX_POOL_MAX_NAME_SIZE = 120
_CNX_POOL_NAME_REGEX = re.compile(r'[^a-zA-Z0-9._:\-*$#]')
_CNX_POOL_MAX_IDLE_TIME = 2147483
_CNX_POOL_QUEUE_TIMEOUT = 2147483

_LOGGER = logging.getLogger("mysqlx")


def generate_pool_name(**kwargs):
    """Generate a pool name.

    This function takes keyword arguments, usually the connection arguments and
    tries to generate a name for the pool.

    Args:
        **kwargs: Arbitrary keyword arguments with the connection arguments.

    Raises:
        PoolError: If the name can't be generated.

    Returns:
        str: The generated pool name.
    """
    parts = []
    for key in ("host", "port", "user", "database", "client_id"):
        try:
            parts.append(str(kwargs[key]))
        except KeyError:
            pass

    if not parts:
        raise PoolError("Failed generating pool name; specify pool_name")

    return "_".join(parts)


class SocketStream(object):
    """Implements a socket stream."""
    def __init__(self):
        self._socket = None
        self._is_ssl = False
        self._is_socket = False
        self._host = None

    def connect(self, params, connect_timeout=_CONNECT_TIMEOUT):
        """Connects to a TCP service.

        Args:
            params (tuple): The connection parameters.

        Raises:
            :class:`mysqlx.InterfaceError`: If Unix socket is not supported.
        """
        if connect_timeout is not None:
            connect_timeout = connect_timeout / 1000  # Convert to seconds
        try:
            self._socket = socket.create_connection(params, connect_timeout)
            self._host = params[0]
        except ValueError:
            try:
                self._socket = socket.socket(socket.AF_UNIX)
                self._socket.settimeout(connect_timeout)
                self._socket.connect(params)
                self._is_socket = True
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
        try:
            self._socket.shutdown(socket.SHUT_RDWR)
            self._socket.close()
        except socket.error:
            # On [Errno 107] Transport endpoint is not connected
            pass
        self._socket = None

    def __del__(self):
        self.close()

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

        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
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
        self._connect_timeout = settings.get("connect-timeout",
                                             _CONNECT_TIMEOUT)
        if self._connect_timeout == 0:
            # None is assigned if connect timeout is 0, which disables timeouts
            # on socket operations
            self._connect_timeout = None

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
            :class:`mysqlx.TimeoutError`: If connect timeout was exceeded.
        """
        # Loop and check
        error = None
        while self._can_failover:
            try:
                self.stream.connect(self._get_connection_params(),
                                    self._connect_timeout)
                self.reader_writer = MessageReaderWriter(self.stream)
                self.protocol = Protocol(self.reader_writer)
                self._handle_capabilities()
                self._authenticate()
                return
            except socket.error as err:
                error = err

        # Python 2.7 does not raise a socket.timeout exception when using
        # settimeout(), but it raises a socket.error with errno.EAGAIN (11)
        # or errno.EINPROGRESS (115) if connect-timeout value is too low
        if error is not None and (isinstance(error, socket.timeout) or
                                  (error.errno in (11, 115) and not PY3)):
            if len(self._routers) <= 1:
                raise TimeoutError("Connection attempt to the server was "
                                   "aborted. Timeout of {0} ms was exceeded"
                                   "".format(self._connect_timeout))
            raise TimeoutError("All server connection attempts were aborted. "
                               "Timeout of {0} ms was exceeded for each "
                               "selected server".format(self._connect_timeout))
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
        if self.protocol is None:
            raise OperationalError("MySQLx Connection not available")
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
        if self.protocol is None:
            raise OperationalError("MySQLx Connection not available")
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
        try:
            if self._active_result is not None:
                self._active_result.fetch_all()
                self.protocol.send_close()
                self.protocol.read_ok()
        except (InterfaceError, OperationalError) as err:
            _LOGGER.warning("Warning: An error occurred while attempting to "
                            "close the connection: {}".format(err))
        finally:
            # The remote connection with the server has been lost,
            # close the connection locally.
            self.stream.close()

    def reset_session(self):
        """Reset a sucessfully authenticated session."""
        if not self.is_open():
            return
        if self._active_result is not None:
            self._active_result.fetch_all()
        try:
            self.protocol.send_reset()
        except (InterfaceError, OperationalError) as err:
            _LOGGER.warning("Warning: An error occurred while attempting to "
                            "reset the session: {}".format(err))

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


class PooledConnection(Connection):
    """Class to hold :class:`Connection` instances in a pool.

    PooledConnection is used by :class:`ConnectionPool` to facilitate the
    connection to return to the pool once is not required, more specifically
    once the close_session() method is invoked. It works like a normal
    Connection except for methods like close() and sql().

    The close_session() method will add the connection back to the pool rather
    than disconnecting from the MySQL server.

    The sql() method is used to execute sql statements.

    Args:
        pool (ConnectionPool): The pool where this connection must return.

    .. versionadded:: 8.0.13
    """
    def __init__(self, pool):
        if not isinstance(pool, ConnectionPool):
            raise AttributeError("pool should be a ConnectionPool object")
        super(PooledConnection, self).__init__(pool.cnx_config)
        self.pool = pool
        self.host = pool.cnx_config["host"]
        self.port = pool.cnx_config["port"]

    def close_connection(self):
        """Closes the connection.

        This method closes the socket.
        """
        super(PooledConnection, self).close_session()

    def close_session(self):
        """Do not close, but add connection back to pool.

        The close_session() method does not close the connection with the
        MySQL server. The connection is added back to the pool so it
        can be reused.

        When the pool is configured to reset the session, the session
        state will be cleared by re-authenticating the user once the connection
        is get from the pool.
        """
        self.pool.add_connection(self)

    def reconnect(self):
        """Reconnect this connection.
        """
        if self._active_result is not None:
            self._active_result.fetch_all()
        self._authenticate()

    def reset(self):
        """Reset the connection.

        Resets the connection by re-authenticate.
        """
        self.reconnect()

    def sql(self, sql_statement):
        """Creates a :class:`mysqlx.SqlStatement` object to allow running the
        SQL statement on the target MySQL Server.

        Returns:
            :class:`mysqlx.SqlStatement`: The sql statement object.
        """
        return SqlStatement(self, sql_statement)


class ConnectionPool(queue.Queue):
    """This class represents a pool of connections.

    Initializes the Pool with the given name and settings.

    Args:
        name (str): The name of the pool, used to track a single pool per
                    combination of host and user.
        **kwargs:
            max_size (int): The maximun number of connections to hold in
                            the pool.
            reset_session (bool): If the connection should be reseted when
                                  is taken from the pool.
            max_idle_time (int): The maximum number of milliseconds to allow
                                 a connection to be idle in the queue before
                                 being closed. Zero value means infinite.
            queue_timeout (int): The maximum number of milliseconds a
                                 request will wait for a connection to
                                 become available. A zero value means
                                 infinite.
            priority (int): The router priority, to choose this pool over
                            other with lower priority.

    Raises:
        :class:`mysqlx.PoolError` on errors.

    .. versionadded:: 8.0.13
    """
    def __init__(self, name, **kwargs):
        self._set_pool_name(name)
        self._open_sessions = 0
        self._connections_openned = []
        self.pool_max_size = kwargs.get("max_size", 25)
        # Can't invoke super due to Queue not is a new-style class
        queue.Queue.__init__(self, self.pool_max_size)
        self.reset_session = kwargs.get("reset_session", True)
        self.max_idle_time = kwargs.get("max_idle_time", 25)
        self.settings = kwargs
        self.queue_timeout = kwargs.get("queue_timeout", 25)
        self._priority = kwargs.get('priority', 0)
        self.cnx_config = kwargs
        self.host = kwargs['host']
        self.port = kwargs['port']

    def _set_pool_name(self, pool_name):
        r"""Set the name of the pool.

        This method checks the validity and sets the name of the pool.

        Args:
            pool_name (str): The pool name.

        Raises:
            AttributeError: If the pool_name contains illegal characters
                            ([^a-zA-Z0-9._\-*$#]) or is longer than
                            connection._CNX_POOL_MAX_NAME_SIZE.
        """
        if _CNX_POOL_NAME_REGEX.search(pool_name):
            raise AttributeError(
                "Pool name '{0}' contains illegal characters".format(pool_name))
        if len(pool_name) > _CNX_POOL_MAX_NAME_SIZE:
            raise AttributeError(
                "Pool name '{0}' is too long".format(pool_name))
        self.name = pool_name

    @property
    def open_connections(self):
        """Returns the number of open connections that can return to this pool.
        """
        return len(self._connections_openned)

    def add_connection(self, cnx=None):
        """Adds a connection to this pool.

        This method instantiates a Connection using the configuration passed
        when initializing the ConnectionPool instance or using the set_config()
        method.
        If cnx is a Connection instance, it will be added to the queue.

        Args:
            cnx (PooledConnection): The connection object.

        Raises:
            PoolError: If no configuration is set, if no more connection can
                       be added (maximum reached) or if the connection can not
                       be instantiated.
        """
        if not self.cnx_config:
            raise PoolError("Connection configuration not available")

        if self.full():
            raise PoolError("Failed adding connection; queue is full")

        if not cnx:
            cnx = PooledConnection(self)
            # mysqlx_wait_timeout is only available on MySQL 8
            ver = cnx.sql('show variables like "version"'
                         ).execute().fetch_all()[0][1]
            if tuple([int(n) for n in ver.split("-")[0].split(".")]) > (8, 10):
                cnx.sql("set mysqlx_wait_timeout = {}".format(1)
                       ).execute()
            self._connections_openned.append(cnx)
        else:
            if not isinstance(cnx, PooledConnection):
                raise PoolError(
                    "Connection instance not subclass of PooledSession.")

        self.queue_connection(cnx)

    def queue_connection(self, cnx):
        """Put connection back in the queue:

        This method is putting a connection back in the queue.
        It will not acquire a lock as the methods using _queue_connection() will
        have it set.

        Args:
            PooledConnection: The connection object.

        Raises:
            PoolError: On errors.
        """
        if not isinstance(cnx, PooledConnection):
            raise PoolError(
                "Connection instance not subclass of PooledSession.")

        # Reset the connection
        if self.reset_session:
            cnx.reset_session()
        try:
            self.put(cnx, block=False)
        except queue.Full:
            PoolError("Failed adding connection; queue is full")

    def track_connection(self, connection):
        """Tracks connection in order of close it when client.close() is invoke.
        """
        self._connections_openned.append(connection)

    def __str__(self):
        return self.name

    def close(self):
        """Empty this ConnectionPool.
        """
        for cnx in self._connections_openned:
            cnx.close_connection()


class PoolsManager(object):
    """Manages a pool of connections for a host or hosts in routers.

    This class handles all the pools of Connections.

    .. versionadded:: 8.0.13
    """
    __instance = None
    __pools = {}

    def __new__(cls):
        if PoolsManager.__instance is None:
            PoolsManager.__instance = object.__new__(cls)
            PoolsManager.__pools = {}
        return PoolsManager.__instance

    def _pool_exists(self, client_id, pool_name):
        """Verifies if a pool exists with the given name.

        Args:
            client_id (str): The client id.
            pool_name (str): The name of the pool.

        Returns:
            bool: Returns `True` if the pool exists otherwise `False`.
        """
        pools = self.__pools.get(client_id, [])
        for pool in pools:
            if pool.name == pool_name:
                return True
        return False

    def _get_pools(self, settings):
        """Retrieves a list of pools that shares the given settings.

        Args:
            settings (dict): the configuration of the pool.

        Returns:
            list: A list of pools that shares the given settings.
        """
        available_pools = []
        pool_names = []
        connections_settings = self._get_connections_settings(settings)

        # Generate the names of the pools this settings can connect to
        for router_name, _ in connections_settings:
            pool_names.append(router_name)

        # Generate the names of the pools this settings can connect to
        for pool in self.__pools.get(settings.get("client_id", "No id"), []):
            if pool.name in pool_names:
                available_pools.append(pool)
        return available_pools

    def _get_connections_settings(self, settings):
        """Generates a list of separated connection settings for each host.

        Gets a list of connection settings for each host or router found in the
        given settings.

        Args:
            settings (dict): The configuration for the connections.

        Returns:
            list: A list of connections settings
        """
        pool_settings = settings.copy()
        routers = pool_settings.get("routers", [])
        connections_settings = []
        if "routers" in pool_settings:
            pool_settings.pop("routers")
        if "host" in pool_settings and "port" in pool_settings:
            routers.append({"priority": 0,
                            "host": pool_settings["host"],
                            "port": pool_settings["port"]})
        # Order routers
        routers.sort(key=lambda x: x["priority"], reverse=True)
        for router in routers:
            connection_settings = pool_settings.copy()
            connection_settings["host"] = router["host"]
            connection_settings["port"] = router["port"]
            connection_settings["priority"] = router["priority"]
            connections_settings.append(
                (generate_pool_name(**connection_settings),
                 connection_settings))
        return connections_settings

    def create_pool(self, cnx_settings):
        """Creates a `ConnectionPool` instance to hold the connections.

        Creates a `ConnectionPool` instance to hold the connections only if
        no other pool exists with the same configuration.

        Args:
            cnx_settings (dict): The configuration for the connections.
        """
        connections_settings = self._get_connections_settings(cnx_settings)

        # Subscribe client if it does not exists
        if cnx_settings.get("client_id", "No id") not in self.__pools:
            self.__pools[cnx_settings.get("client_id", "No id")] = []

        # Create a pool for each router
        for router_name, settings in connections_settings:
            if self._pool_exists(cnx_settings.get("client_id", "No id"),
                                 router_name):
                continue
            else:
                pool = self.__pools.get(cnx_settings.get("client_id", "No id"),
                                        [])
                pool.append(ConnectionPool(router_name, **settings))

    def get_connection(self, settings):
        """Get a connection from the pool.

        This method returns an `PooledConnection` instance which has a reference
        to the pool that created it, and can be used as a normal Connection.

        When the MySQL connection is not connected, a reconnect is attempted.

        Raises:
            :class:`PoolError`: On errors.

        Returns:
            PooledConnection: A pooled connection object.
        """
        pools = self._get_pools(settings)
        # Pools are stored by router priority
        num_pools = len(pools)
        for pool_number in range(num_pools):
            pool = pools[pool_number]
            try:
                # Check connections aviability in this pool
                if pool.qsize() > 0:
                    # We have connections in pool, try to return a working one
                    with threading.RLock():
                        try:
                            cnx = pool.get(block=True,
                                           timeout=pool.queue_timeout)
                        except queue.Empty:
                            raise PoolError(
                                "Failed getting connection; pool exhausted")
                        cnx.reset()
                        # mysqlx_wait_timeout is only available on MySQL 8
                        ver = cnx.sql('show variables like "version"'
                                     ).execute().fetch_all()[0][1]
                        if tuple([int(n) for n in
                                  ver.split("-")[0].split(".")]) > (8, 10):
                            cnx.sql("set mysqlx_wait_timeout = {}".format(1)
                                   ).execute()
                        return cnx
                elif pool.open_connections < pool.pool_max_size:
                    # No connections in pool, but we can open a new one
                    cnx = PooledConnection(pool)
                    pool.track_connection(cnx)
                    cnx.connect()
                    # mysqlx_wait_timeout is only available on MySQL 8
                    ver = cnx.sql('show variables like "version"'
                                 ).execute().fetch_all()[0][1]
                    if tuple([int(n) for n in
                              ver.split("-")[0].split(".")]) > (8, 10):
                        cnx.sql("set mysqlx_wait_timeout = {}".format(1)
                               ).execute()
                    return cnx
                else:
                    # Pool is exaust so the client needs to wait
                    with threading.RLock():
                        try:
                            cnx = pool.get(block=True,
                                           timeout=pool.queue_timeout)
                            cnx.reset()
                            # mysqlx_wait_timeout is only available on MySQL 8
                            ver = cnx.sql('show variables like "version"'
                                         ).execute().fetch_all()[0][1]
                            if tuple([int(n) for n in
                                      ver.split("-")[0].split(".")]) > (8, 10):
                                cnx.sql("set mysqlx_wait_timeout = {}".format(1)
                                       ).execute()
                            return cnx
                        except queue.Empty:
                            raise PoolError("pool max size has been reached")
            except (InterfaceError, TimeoutError):
                if pool_number == num_pools - 1:
                    raise
                else:
                    continue

    def close_pool(self, cnx_settings):
        """Closes the connections in the pools

        Returns:
            int: The number of closed pools
        """
        pools = self._get_pools(cnx_settings)
        for pool in pools:
            pool.close()
            # Remove the pool
            if cnx_settings.get("client_id", None) is not None:
                client_pools = self.__pools.get(cnx_settings.get("client_id"))
                if pool in client_pools:
                    client_pools.remove(pool)
        return len(pools)


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
        if "pooling" in settings and settings["pooling"]:
            # Create pool and retrieve a Connection instance
            PoolsManager().create_pool(settings)
            self._connection = PoolsManager().get_connection(settings)
            if self._connection is None:
                raise PoolError("connection could not be retrieve from pool. %s",
                                values=settings)
        else:
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
        # Set an unconnected connection
        self._connection = Connection(self._settings)

    def close_connections(self):
        """Closes all underliying connections as pooled connections"""
        self._connection.close_connection()


class Client(object):
    """Class defining a client, it stores a connection configuration.

       Args:
           connection_dict (dict): The connection information to connect to a
                                   MySQL server.
           options_dict (dict): The options to configure this client.

       .. versionadded:: 8.0.13
    """
    def __init__(self, connection_dict, options_dict=None):
        self.settings = connection_dict
        if options_dict is None:
            options_dict = {}

        self.sessions = []
        self.client_id = uuid.uuid4()

        self._set_pool_size(options_dict.get("max_size", 25))
        self._set_max_idle_time(options_dict.get("max_idle_time", 0))
        self._set_queue_timeout(options_dict.get("queue_timeout", 0))
        self._set_pool_enabled(options_dict.get("enabled", True))

        self.settings["pooling"] = self.pooling_enabled
        self.settings["max_size"] = self.max_size
        self.settings["client_id"] = self.client_id

    def _set_pool_size(self, pool_size):
        """Set the size of the pool.

        This method sets the size of the pool but it will not resize the pool.

        Args:
            pool_size (int): An integer equal or greater than 0 indicating
                             the pool size.

        Raises:
            :class:`AttributeError`: If the pool_size value is not an integer
                                     greater or equal to 0.
        """
        if isinstance(pool_size, bool) or not isinstance(pool_size, int) or \
           not pool_size > 0:
            raise AttributeError("Pool max_size value must be an integer "
                                 "greater than 0, the given value {} "
                                 "is not valid.".format(pool_size))

        self.max_size = _CNX_POOL_MAXSIZE if pool_size == 0 else pool_size

    def _set_max_idle_time(self, max_idle_time):
        """Set the max idle time.

        This method sets the max idle time.

        Args:
            max_idle_time (int): An integer equal or greater than 0 indicating
                                 the max idle time.

        Raises:
            :class:`AttributeError`: If the max_idle_time value is not an
                                     integer greater or equal to 0.
        """
        if isinstance(max_idle_time, bool) or \
           not isinstance(max_idle_time, int) or not max_idle_time > -1:
            raise AttributeError("Connection max_idle_time value must be an "
                                 "integer greater or equal to 0, the given "
                                 "value {} is not valid.".format(max_idle_time))

        self.max_idle_time = max_idle_time
        self.settings["max_idle_time"] = _CNX_POOL_MAX_IDLE_TIME \
            if max_idle_time == 0 else int(max_idle_time / 1000)

    def _set_pool_enabled(self, enabled):
        """Set if the pool is enabled.

        This method sets if the pool is enabled.

        Args:
            enabled (bool): True if to enabling the pool.

        Raises:
            :class:`AttributeError`: If the value of enabled is not a bool type.
        """
        if not isinstance(enabled, bool):
            raise AttributeError("The enabled value should be True or False.")
        self.pooling_enabled = enabled

    def _set_queue_timeout(self, queue_timeout):
        """Set the queue timeout.

        This method sets the queue timeout.

        Args:
            queue_timeout (int): An integer equal or greater than 0 indicating
                                 the queue timeout.

        Raises:
            :class:`AttributeError`: If the queue_timeout value is not an
                                     integer greater or equal to 0.
        """
        if isinstance(queue_timeout, bool) or \
           not isinstance(queue_timeout, int) or not queue_timeout > -1:
            raise AttributeError("Connection queue_timeout value must be an "
                                 "integer greater or equal to 0, the given "
                                 "value {} is not valid.".format(queue_timeout))

        self.queue_timeout = queue_timeout
        self.settings["queue_timeout"] = _CNX_POOL_QUEUE_TIMEOUT \
            if queue_timeout == 0 else int(queue_timeout / 1000)
        # To avoid a connection stall waiting for the server, if the
        # connect-timeout is not given, use the queue_timeout
        if not "connect-timeout" in self.settings:
            self.settings["connect-timeout"] = self.queue_timeout

    def get_session(self):
        """Creates a Session instance using the provided connection data.

        Returns:
            Session: Session object.
        """
        session = Session(self.settings)
        self.sessions.append(session)
        return session

    def close(self):
        """Closes the sessions opened by this client.
        """
        PoolsManager().close_pool(self.settings)
        for session in self.sessions:
            session.close_connections()
