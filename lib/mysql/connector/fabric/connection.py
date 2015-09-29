# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2013, 2015, Oracle and/or its affiliates. All rights reserved.

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

"""Implementing communication with MySQL Fabric"""

import sys
import datetime
import time
import uuid
from base64 import b16decode
from bisect import bisect
from hashlib import md5
import logging
import socket
import collections

# pylint: disable=F0401,E0611
try:
    from xmlrpclib import Fault, ServerProxy, Transport
    import urllib2
    from httplib import BadStatusLine
except ImportError:
    # Python v3
    from xmlrpc.client import Fault, ServerProxy, Transport
    import urllib.request as urllib2
    from http.client import BadStatusLine

if sys.version_info[0] == 2:
    try:
        from httplib import HTTPSConnection
    except ImportError:
        HAVE_SSL = False
    else:
        HAVE_SSL = True
else:
    try:
        from http.client import HTTPSConnection
    except ImportError:
        HAVE_SSL = False
    else:
        HAVE_SSL = True
# pylint: enable=F0401,E0611

import mysql.connector
from ..connection import MySQLConnection
from ..conversion import MySQLConverter
from ..pooling import MySQLConnectionPool
from ..errors import (
    Error, InterfaceError, NotSupportedError, MySQLFabricError, InternalError,
    DatabaseError
)
from ..cursor import (
    MySQLCursor, MySQLCursorBuffered,
    MySQLCursorRaw, MySQLCursorBufferedRaw
)
from .. import errorcode
from . import FabricMySQLServer, FabricShard
from .caching import FabricCache
from .balancing import WeightedRoundRobin
from .. import version
from ..catch23 import PY2, isunicode, UNICODE_TYPES

RESET_CACHE_ON_ERROR = (
    errorcode.CR_SERVER_LOST,
    errorcode.ER_OPTION_PREVENTS_STATEMENT,
)


# Errors to be reported to Fabric
REPORT_ERRORS = (
    errorcode.CR_SERVER_LOST,
    errorcode.CR_SERVER_GONE_ERROR,
    errorcode.CR_CONN_HOST_ERROR,
    errorcode.CR_CONNECTION_ERROR,
    errorcode.CR_IPSOCK_ERROR,
)
REPORT_ERRORS_EXTRA = []

DEFAULT_FABRIC_PROTOCOL = 'xmlrpc'

MYSQL_FABRIC_PORT = {
    'xmlrpc': 32274,
    'mysql': 32275
}

FABRICS = {}

# For attempting to connect with Fabric
_CNX_ATTEMPT_DELAY = 1
_CNX_ATTEMPT_MAX = 3

_GETCNX_ATTEMPT_DELAY = 1
_GETCNX_ATTEMPT_MAX = 3

MODE_READONLY = 1
MODE_WRITEONLY = 2
MODE_READWRITE = 3

STATUS_FAULTY = 0
STATUS_SPARE = 1
STATUS_SECONDARY = 2
STATUS_PRIMARY = 3

SCOPE_GLOBAL = 'GLOBAL'
SCOPE_LOCAL = 'LOCAL'

_SERVER_STATUS_FAULTY = 'FAULTY'

_CNX_PROPERTIES = {
    # name: ((valid_types), description, default)
    'group': ((str,), "Name of group of servers", None),
    'key': (tuple([int, str, datetime.datetime,
                   datetime.date] + list(UNICODE_TYPES)),
            "Sharding key", None),
    'tables': ((tuple, list), "List of tables in query", None),
    'mode': ((int,), "Read-Only, Write-Only or Read-Write", MODE_READWRITE),
    'shard': ((str,), "Identity of the shard for direct connection", None),
    'mapping': ((str,), "", None),
    'scope': ((str,), "GLOBAL for accessing Global Group, or LOCAL",
              SCOPE_LOCAL),
    'attempts': ((int,), "Attempts for getting connection",
                 _GETCNX_ATTEMPT_MAX),
    'attempt_delay': ((int,), "Seconds to wait between each attempt",
                      _GETCNX_ATTEMPT_DELAY),
}

_LOGGER = logging.getLogger('myconnpy-fabric')


class MySQLRPCProtocol(object):
    """Class using MySQL protocol to query Fabric.
    """
    def __init__(self, fabric, host, port, connect_attempts, connect_delay):
        self.converter = MySQLConverter()
        self.handler = FabricMySQLConnection(fabric, host, port,
                                             connect_attempts,
                                             connect_delay)
        self.handler.connect()

    def _process_params_dict(self, params):
        """Process query parameters given as dictionary"""
        try:
            res = []
            for key, value in list(params.items()):
                conv = value
                conv = self.converter.to_mysql(conv)
                conv = self.converter.escape(conv)
                conv = self.converter.quote(conv)
                res.append("{0}={1}".format(key, str(conv)))
        except Exception as err:
            raise mysql.connector.errors.ProgrammingError(
                "Failed processing pyformat-parameters; %s" % err)
        else:
            return res

    def _process_params(self, params):
        """Process query parameters."""
        try:
            res = params
            res = [self.converter.to_mysql(i) for i in res]
            res = [self.converter.escape(i) for i in res]
            res = [self.converter.quote(i) for i in res]
            res = [str(i) for i in res]
        except Exception as err:
            raise mysql.connector.errors.ProgrammingError(
                "Failed processing format-parameters; %s" % err)
        else:
            return tuple(res)

    def _execute_cmd(self, stmt, params=None):
        """Executes the given query

        Returns a list containing response from Fabric
        """
        if not params:
            params = ()
        cur = self.handler.connection.cursor(dictionary=True)
        results = []

        for res in cur.execute(stmt, params, multi=True):
            results.append(res.fetchall())

        return results

    def create_params(self, *args, **kwargs):
        """Process arguments to create query parameters.
        """
        params = []
        if args:
            args = self._process_params(args)
            params.extend(args)
        if kwargs:
            kwargs = self._process_params_dict(kwargs)
            params.extend(kwargs)

        params = ', '.join(params)
        return params

    def execute(self, group, command, *args, **kwargs):
        """Executes the given command with MySQL protocol

        Executes the given command with the given parameters.

        Returns an iterator to navigate to navigate through the result set
        returned by Fabric
        """
        params = self.create_params(*args, **kwargs)
        cmd = "CALL {0}.{1}({2})".format(group, command, params)

        fab_set = None
        try:
            data = self._execute_cmd(cmd)
            fab_set = FabricMySQLSet(data)
        except (Fault, socket.error, InterfaceError) as exc:
            msg = "Executing {group}.{command} failed: {error}".format(
                group=group, command=command, error=str(exc))
            raise InterfaceError(msg)

        return fab_set


class XMLRPCProtocol(object):
    """Class using XML-RPC protocol to query Fabric.
    """
    def __init__(self, fabric, host, port, connect_attempts, connect_delay):
        self.handler = FabricXMLRPCConnection(fabric, host, port,
                                              connect_attempts, connect_delay)
        self.handler.connect()

    def execute(self, group, command, *args, **kwargs):
        """Executes the given command with XML-RPC protocol

        Executes the given command with the given parameters

        Returns an iterator to navigate to navigate through the result set
        returned by Fabric
        """
        try:
            grp = getattr(self.handler.proxy, group)
            cmd = getattr(grp, command)
        except AttributeError as exc:
            raise ValueError("{group}.{command} not available ({err})".format(
                group=group, command=command, err=str(exc)))

        fab_set = None
        try:
            data = cmd(*args, **kwargs)
            fab_set = FabricSet(data)
        except (Fault, socket.error, InterfaceError) as exc:
            msg = "Executing {group}.{command} failed: {error}".format(
                group=group, command=command, error=str(exc))
            raise InterfaceError(msg)

        return fab_set


class FabricMySQLResponse(object):
    """Class used to parse a response got from Fabric with MySQL protocol.
    """
    def __init__(self, data):
        info = data[0][0]
        (fabric_uuid_str, ttl, error) = (info['fabric_uuid'], info['ttl'],
                                         info['message'])
        if error:
            raise InterfaceError(error)

        self.fabric_uuid_str = fabric_uuid_str
        self.ttl = ttl
        self.coded_rows = data[1]


class FabricMySQLSet(FabricMySQLResponse):
    """Iterator to navigate through the result set returned from Fabric
    with MySQL Protocol.
    """
    def __init__(self, data):
        """Initialize the FabricSet object.
        """
        super(FabricMySQLSet, self).__init__(data)
        self.__names = self.coded_rows[0].keys()
        self.__rows = self.coded_rows
        self.__result = collections.namedtuple('ResultSet', self.__names)

    def rowcount(self):
        """The number of rows in the result set.
        """
        return len(self.__rows)

    def rows(self):
        """Iterate over the rows of the result set.

        Each row is a named tuple.
        """
        for row in self.__rows:
            yield self.__result(**row)

    def row(self, index):
        """Indexing method for a row.

        Each row is a named tuple.
        """
        return self.__result(**self.__rows[index])


class FabricResponse(object):
    """Class used to parse a response got from Fabric.
    """

    SUPPORTED_VERSION = 1

    def __init__(self, data):
        """Initialize the FabricResponse object
        """
        (format_version, fabric_uuid_str, ttl, error, rows) = data
        if error:
            raise InterfaceError(error)
        if format_version != FabricResponse.SUPPORTED_VERSION:
            raise InterfaceError(
                "Supported protocol has version {sversion}. Got a response "
                "from MySQL Fabric with version {gversion}.".format(
                    sversion=FabricResponse.SUPPORTED_VERSION,
                    gversion=format_version)
            )
        self.format_version = format_version
        self.fabric_uuid_str = fabric_uuid_str
        self.ttl = ttl
        self.coded_rows = rows


class FabricSet(FabricResponse):
    """Iterator to navigate through the result set returned from Fabric
    """
    def __init__(self, data):
        """Initialize the FabricSet object.
        """
        super(FabricSet, self).__init__(data)
        assert len(self.coded_rows) == 1
        self.__names = self.coded_rows[0]['info']['names']
        self.__rows = self.coded_rows[0]['rows']
        assert all(len(self.__names) == len(row) for row in self.__rows) or \
               len(self.__rows) == 0
        self.__result = collections.namedtuple('ResultSet', self.__names)

    def rowcount(self):
        """The number of rows in the result set.
        """
        return len(self.__rows)

    def rows(self):
        """Iterate over the rows of the result set.

        Each row is a named tuple.
        """
        for row in self.__rows:
            yield self.__result(*row)

    def row(self, index):
        """Indexing method for a row.

        Each row is a named tuple.
        """
        return self.__result(*self.__rows[index])


def extra_failure_report(error_codes):
    """Add MySQL error to be reported to Fabric

    This function adds error_codes to the error list to be reported to
    Fabric. To reset the custom error reporting list, pass None or empty
    list.

    The error_codes argument can be either a MySQL error code defined in the
    errorcode module, or list of error codes.

    Raises AttributeError when code is not an int.
    """
    global REPORT_ERRORS_EXTRA  # pylint: disable=W0603

    if not error_codes:
        REPORT_ERRORS_EXTRA = []

    if not isinstance(error_codes, (list, tuple)):
        error_codes = [error_codes]

    for code in error_codes:
        if not isinstance(code, int) or not (code >= 1000 and code < 3000):
            raise AttributeError("Unknown or invalid error code.")
        REPORT_ERRORS_EXTRA.append(code)


def _fabric_xmlrpc_uri(host, port):
    """Create an XMLRPC URI for connecting to Fabric

    This method will create a URI using the host and TCP/IP
    port suitable for connecting to a MySQL Fabric instance.

    Returns a URI.
    """
    return 'http://{host}:{port}'.format(host=host, port=port)


def _fabric_server_uuid(host, port):
    """Create a UUID using host and port"""
    return uuid.uuid3(uuid.NAMESPACE_URL, _fabric_xmlrpc_uri(host, port))


def _validate_ssl_args(ssl_ca, ssl_key, ssl_cert):
    """Validate the SSL argument.

    Raises AttributeError is required argument is not set.

    Returns dict or None.
    """
    if not HAVE_SSL:
        raise InterfaceError("Python does not support SSL")
    if any([ssl_ca, ssl_key, ssl_cert]):
        if not ssl_ca:
            raise AttributeError("Missing ssl_ca argument.")
        if (ssl_key or ssl_cert) and not (ssl_key and ssl_cert):
            raise AttributeError(
                "ssl_key and ssl_cert need to be both "
                "specified, or neither."
            )
        return {
            'ca': ssl_ca,
            'key': ssl_key,
            'cert': ssl_cert,
        }

    return None


if HAVE_SSL:
    class FabricHTTPSHandler(urllib2.HTTPSHandler):

        """Class handling HTTPS connections"""

        def __init__(self, ssl_config):  #pylint: disable=E1002
            """Initialize"""
            if PY2:
                urllib2.HTTPSHandler.__init__(self)
            else:
                super().__init__()  # pylint: disable=W0104
            self._ssl_config = ssl_config

        def https_open(self, req):
            """Open HTTPS connection"""
            return self.do_open(self.get_https_connection, req)

        def get_https_connection(self, host, timeout=300):
            """Returns a HTTPSConnection"""
            return HTTPSConnection(
                host,
                key_file=self._ssl_config['key'],
                cert_file=self._ssl_config['cert']
            )


class FabricTransport(Transport):

    """Custom XMLRPC Transport for Fabric"""

    user_agent = 'MySQL Connector Python/{0}'.format(version.VERSION_TEXT)

    def __init__(self, username, password,  #pylint: disable=E1002
                 verbose=0, use_datetime=False, https_handler=None):
        """Initialize"""
        if PY2:
            Transport.__init__(self, use_datetime=False)
        else:
            super().__init__(use_datetime=False)
        self._username = username
        self._password = password
        self._use_datetime = use_datetime
        self.verbose = verbose
        self._username = username
        self._password = password

        self._handlers = []

        if self._username and self._password:
            self._passmgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
            self._auth_handler = urllib2.HTTPDigestAuthHandler(self._passmgr)
        else:
            self._auth_handler = None
            self._passmgr = None

        if https_handler:
            self._handlers.append(https_handler)
            self._scheme = 'https'
        else:
            self._scheme = 'http'

        if self._auth_handler:
            self._handlers.append(self._auth_handler)

    def request(self, host, handler, request_body, verbose=0):
        """Send XMLRPC request"""
        uri = '{scheme}://{host}{handler}'.format(scheme=self._scheme,
                                                  host=host, handler=handler)

        if self._passmgr:
            self._passmgr.add_password(None, uri, self._username,
                                       self._password)
        if self.verbose:
            _LOGGER.debug("FabricTransport: {0}".format(uri))

        opener = urllib2.build_opener(*self._handlers)

        headers = {
            'Content-Type': 'text/xml',
            'User-Agent': self.user_agent,
        }
        req = urllib2.Request(uri, request_body, headers=headers)

        try:
            return self.parse_response(opener.open(req))
        except (urllib2.URLError, urllib2.HTTPError) as exc:
            try:
                code = -1
                if exc.code == 400:
                    reason = 'Permission denied'
                    code = exc.code
                else:
                    reason = exc.reason
                msg = "{reason} ({code})".format(reason=reason, code=code)
            except AttributeError:
                if 'SSL' in str(exc):
                    msg = "SSL error"
                else:
                    msg = str(exc)
            raise InterfaceError("Connection with Fabric failed: " + msg)
        except BadStatusLine:
            raise InterfaceError("Connection with Fabric failed: check SSL")


class Fabric(object):

    """Class managing MySQL Fabric instances"""

    def __init__(self, host, username=None, password=None,
                 port=None,
                 connect_attempts=_CNX_ATTEMPT_MAX,
                 connect_delay=_CNX_ATTEMPT_DELAY,
                 report_errors=False,
                 ssl_ca=None, ssl_key=None, ssl_cert=None, user=None,
                 protocol=DEFAULT_FABRIC_PROTOCOL):
        """Initialize"""
        if protocol == 'xmlrpc':
            self._protocol_class = XMLRPCProtocol
        elif protocol == 'mysql':
            self._protocol_class = MySQLRPCProtocol
        else:
            raise InterfaceError(
                "Protocol not supported by MySQL Fabric,"
                " was '{}'".format(protocol))

        if not port:
            port = MYSQL_FABRIC_PORT[protocol]

        self._fabric_instances = {}
        self._fabric_uuid = None
        self._ttl = 1 * 60  # one minute by default
        self._version_token = None
        self._connect_attempts = connect_attempts
        self._connect_delay = connect_delay
        self._cache = FabricCache()
        self._group_balancers = {}
        self._init_host = host
        self._init_port = port
        self._ssl = _validate_ssl_args(ssl_ca, ssl_key, ssl_cert)
        self._report_errors = report_errors
        self._protocol = protocol
        if user and username:
            raise ValueError("can not specify both user and username")
        self._username = user or username
        self._password = password

    @property
    def username(self):
        """Return username used to authenticate with Fabric"""
        return self._username

    @property
    def password(self):
        """Return password used to authenticate with Fabric"""
        return self._password

    @property
    def ssl_config(self):
        """Return the SSL configuration"""
        return self._ssl

    def seed(self, host=None, port=None):
        """Get MySQL Fabric Instances

        This method uses host and port to connect to a MySQL Fabric server
        and get all the instances managing the same metadata.

        Raises InterfaceError on errors.
        """
        host = host or self._init_host
        port = port or self._init_port

        fabinst = self._protocol_class(self, host, port,
                                       connect_attempts=self._connect_attempts,
                                       connect_delay=self._connect_delay)

        fabric_uuid, fabric_version, ttl, fabrics = self.get_fabric_servers(
            fabinst)

        if not fabrics:
            # Raise, something went wrong.
            raise InterfaceError("Failed getting list of Fabric servers")

        if self._version_token == fabric_version:
            return

        _LOGGER.info(
            "Loading Fabric configuration version {version}".format(
                version=fabric_version))
        self._fabric_uuid = fabric_uuid
        self._version_token = fabric_version
        if ttl > 0:
            self._ttl = ttl

        # Update the Fabric servers
        for fabric in fabrics:
            inst = self._protocol_class(self, fabric['host'], fabric['port'],
                                        connect_attempts=self._connect_attempts,
                                        connect_delay=self._connect_delay)
            inst_uuid = inst.handler.uuid
            if inst_uuid not in self._fabric_instances:
                self._fabric_instances[inst_uuid] = inst
                _LOGGER.debug(
                    "Added new Fabric server {host}:{port}".format(
                        host=inst.handler.host, port=inst.handler.port))

    def reset_cache(self, group=None):
        """Reset cached information

        This method destroys all cached information.
        """
        if group:
            _LOGGER.debug("Resetting cache for group '{group}'".format(
                group=group))
            self.get_group_servers(group, use_cache=False)
        else:
            _LOGGER.debug("Resetting cache")
            self._cache = FabricCache()

    def get_instance(self):
        """Get a MySQL Fabric Instance

        This method will get the next available MySQL Fabric Instance.

        Raises InterfaceError when no instance is available or connected.
        """
        nxt = 0
        errmsg = "No MySQL Fabric instance available"
        if not self._fabric_instances:
            raise InterfaceError(errmsg + " (not seeded?)")
        if PY2:
            instance_list = self._fabric_instances.keys()
            inst = self._fabric_instances[instance_list[nxt]]
        else:
            inst = self._fabric_instances[list(self._fabric_instances)[nxt]]
        if not inst.handler.is_connected:
            inst.handler.connect()
        return inst

    def report_failure(self, server_uuid, errno):
        """Report failure to Fabric

        This method sets the status of a MySQL server identified by
        server_uuid.
        """
        if not self._report_errors:
            return

        errno = int(errno)
        current_host = socket.getfqdn()

        if errno in REPORT_ERRORS or errno in REPORT_ERRORS_EXTRA:
            _LOGGER.debug("Reporting error %d of server %s", errno,
                          server_uuid)
            inst = self.get_instance()
            try:
                data = inst.execute('threat', 'report_failure',
                                    server_uuid, current_host, errno)
                FabricResponse(data)
            except (Fault, socket.error) as exc:
                _LOGGER.debug("Failed reporting server to Fabric (%s)",
                              str(exc))
                # Not requiring further action

    def get_fabric_servers(self, fabric_cnx=None):
        """Get all MySQL Fabric instances

        This method looks up the other MySQL Fabric instances which uses
        the same metadata. The returned list contains dictionaries with
        connection information such ass host and port. For example:

        [
            {'host': 'fabric_prod_1.example.com', 'port': 32274 },
            {'host': 'fabric_prod_2.example.com', 'port': 32274 },
        ]

        Returns a list of dictionaries
        """
        inst = fabric_cnx or self.get_instance()
        result = []
        err_msg = "Looking up Fabric servers failed using {host}:{port}: {err}"
        try:
            fset = inst.execute('dump', 'fabric_nodes',
                                "protocol." + self._protocol)

            for row in fset.rows():
                result.append({'host': row.host, 'port': row.port})
        except (Fault, socket.error) as exc:
            msg = err_msg.format(err=str(exc), host=inst.handler.host,
                                 port=inst.handler.port)
            raise InterfaceError(msg)
        except (TypeError, AttributeError) as exc:
            msg = err_msg.format(
                err="No Fabric server available ({0})".format(exc),
                host=inst.handler.host, port=inst.handler.port)
            raise InterfaceError(msg)

        try:
            fabric_uuid = uuid.UUID(fset.fabric_uuid_str)
        except TypeError:
            fabric_uuid = uuid.uuid4()

        fabric_version = 0

        return fabric_uuid, fabric_version, fset.ttl, result

    def get_group_servers(self, group, use_cache=True):
        """Get all MySQL servers in a group

        This method returns information about all MySQL part of the
        given high-availability group. When use_cache is set to
        True, the cached information will be used.

        Raises InterfaceError on errors.

        Returns list of FabricMySQLServer objects.
        """
        # Get group information from cache
        if use_cache:
            entry = self._cache.group_search(group)
            if entry:
                # Cache group information
                return entry.servers

        inst = self.get_instance()
        result = []
        try:
            fset = inst.execute('dump', 'servers', self._version_token, group)
        except (Fault, socket.error) as exc:
            msg = ("Looking up MySQL servers failed for group "
                   "{group}: {error}").format(error=str(exc), group=group)
            raise InterfaceError(msg)

        weights = []
        for row in fset.rows():
            # We make sure, when using local groups, we skip the global group
            if row.group_id == group:
                mysqlserver = FabricMySQLServer(
                    row.server_uuid, row.group_id, row.host, row.port,
                    row.mode, row.status, row.weight
                )
                result.append(mysqlserver)
                if mysqlserver.status == STATUS_SECONDARY:
                    weights.append((mysqlserver.uuid, mysqlserver.weight))

        self._cache.cache_group(group, result)
        if weights:
            self._group_balancers[group] = WeightedRoundRobin(*weights)

        return result

    def get_group_server(self, group, mode=None, status=None):
        """Get a MySQL server from a group

        The method uses MySQL Fabric to get the correct MySQL server
        for the specified group. You can specify mode or status, but
        not both.

        The mode argument will decide whether the primary or a secondary
        server is returned. When no secondary server is available, the
        primary is returned.

        Status is used to force getting either a primary or a secondary.

        The returned tuple contains host, port and uuid.

        Raises InterfaceError on errors; ValueError when both mode
        and status are given.

        Returns a FabricMySQLServer object.
        """
        if mode and status:
            raise ValueError(
                "Either mode or status must be given, not both")

        errmsg = "No MySQL server available for group '{group}'"

        servers = self.get_group_servers(group, use_cache=True)
        if not servers:
            raise InterfaceError(errmsg.format(group=group))

        # Get the Master and return list (host, port, UUID)
        primary = None
        secondary = []
        for server in servers:
            if server.status == STATUS_SECONDARY:
                secondary.append(server)
            elif server.status == STATUS_PRIMARY:
                primary = server

        if mode in (MODE_WRITEONLY, MODE_READWRITE) or status == STATUS_PRIMARY:
            if not primary:
                self.reset_cache(group=group)
                raise InterfaceError((errmsg + ' {query}={value}').format(
                    query='status' if status else 'mode',
                    group=group,
                    value=status or mode))
            return primary

        # Return primary if no secondary is available
        if not secondary and primary:
            return primary
        elif group in self._group_balancers:
            next_secondary = self._group_balancers[group].get_next()[0]
            for mysqlserver in secondary:
                if next_secondary == mysqlserver.uuid:
                    return mysqlserver

        self.reset_cache(group=group)
        raise InterfaceError(errmsg.format(group=group, mode=mode))

    def get_sharding_information(self, tables=None, database=None):
        """Get and cache the sharding information for given tables

        This method is fetching sharding information from MySQL Fabric
        and caches the result. The tables argument must be sequence
        of sequences contain the name of the database and table. If no
        database is given, the value for the database argument will
        be used.

        Examples:
          tables = [('salary',), ('employees',)]
          get_sharding_information(tables, database='employees')

          tables = [('salary', 'employees'), ('employees', employees)]
          get_sharding_information(tables)

        Raises InterfaceError on errors; ValueError when something is wrong
        with the tables argument.
        """
        if not isinstance(tables, (list, tuple)):
            raise ValueError("tables should be a sequence")

        patterns = []
        for table in tables:
            if not isinstance(table, (list, tuple)) and not database:
                raise ValueError("No database specified for table {0}".format(
                    table))

            if isinstance(table, (list, tuple)):
                dbase = table[1]
                tbl = table[0]
            else:
                dbase = database
                tbl = table
            patterns.append("{0}.{1}".format(dbase, tbl))

        inst = self.get_instance()
        try:

            fset = inst.execute(
                'dump', 'sharding_information', self._version_token,
                ','.join(patterns)
            )
        except (Fault, socket.error) as exc:
            msg = "Looking up sharding information failed : {error}".format(
                error=str(exc))
            raise InterfaceError(msg)

        for row in fset.rows():
            self._cache.sharding_cache_table(
                FabricShard(row.schema_name, row.table_name, row.column_name,
                            row.lower_bound, row.shard_id, row.type_name,
                            row.group_id, row.global_group)
            )

    def get_shard_server(self, tables, key, scope=SCOPE_LOCAL, mode=None):
        """Get MySQL server information for a particular shard

        Raises DatabaseError when the table is unknown or when tables are not
        on the same shard. ValueError is raised when there is a problem
        with the methods arguments. InterfaceError is raised for other errors.
        """
        if not isinstance(tables, (list, tuple)):
            raise ValueError("tables should be a sequence")

        groups = []

        for dbobj in tables:
            try:
                database, table = dbobj.split('.')
            except ValueError:
                raise ValueError(
                    "tables should be given as <database>.<table>, "
                    "was {0}".format(dbobj))

            entry = self._cache.sharding_search(database, table)
            if not entry:
                self.get_sharding_information((table,), database)
                entry = self._cache.sharding_search(database, table)
                if not entry:
                    raise DatabaseError(
                        errno=errorcode.ER_BAD_TABLE_ERROR,
                        msg="Unknown table '{database}.{table}'".format(
                            database=database, table=table))

            if scope == 'GLOBAL':
                return self.get_group_server(entry.global_group, mode=mode)

            if entry.shard_type == 'RANGE':
                try:
                    range_key = int(key)
                except ValueError:
                    raise ValueError("Key must be an integer for RANGE")
                partitions = entry.keys
                index = partitions[bisect(partitions, range_key) - 1]
                partition = entry.partitioning[index]
            elif entry.shard_type == 'RANGE_DATETIME':
                if not isinstance(key, (datetime.date, datetime.datetime)):
                    raise ValueError(
                        "Key must be datetime.date or datetime.datetime for "
                        "RANGE_DATETIME")
                index = None
                for partkey in entry.keys_reversed:
                    if key >= partkey:
                        index = partkey
                        break
                try:
                    partition = entry.partitioning[index]
                except KeyError:
                    raise ValueError("Key invalid; was '{0}'".format(key))
            elif entry.shard_type == 'RANGE_STRING':
                if not isunicode(key):
                    raise ValueError("Key must be a unicode value")
                index = None
                for partkey in entry.keys_reversed:
                    if key >= partkey:
                        index = partkey
                        break
                try:
                    partition = entry.partitioning[index]
                except KeyError:
                    raise ValueError("Key invalid; was '{0}'".format(key))
            elif entry.shard_type == 'HASH':
                md5key = md5(str(key))
                index = entry.keys_reversed[-1]
                for partkey in entry.keys_reversed:
                    if md5key.digest() >= b16decode(partkey):
                        index = partkey
                        break
                partition = entry.partitioning[index]
            else:
                raise InterfaceError(
                    "Unsupported sharding type {0}".format(entry.shard_type))

            groups.append(partition['group'])
            if not all(group == groups[0] for group in groups):
                raise DatabaseError(
                    "Tables are located in different shards.")

        return self.get_group_server(groups[0], mode=mode)

    def execute(self, group, command, *args, **kwargs):
        """Execute a Fabric command from given group

        This method will execute the given Fabric command from the given group
        using the given arguments. It returns an instance of FabricSet.

        Raises ValueError when group.command is not valid and raises
        InterfaceError when an error occurs while executing.

        Returns FabricSet.
        """
        inst = self.get_instance()
        return inst.execute(group, command, *args, **kwargs)


class FabricConnection(object):
    """Base Class for a class holding a connection to a MySQL Fabric server
    """
    def __init__(self, fabric, host,
                 port=MYSQL_FABRIC_PORT[DEFAULT_FABRIC_PROTOCOL],
                 connect_attempts=_CNX_ATTEMPT_MAX,
                 connect_delay=_CNX_ATTEMPT_DELAY):
        """Initialize"""
        if not isinstance(fabric, Fabric):
            raise ValueError("fabric must be instance of class Fabric")
        self._fabric = fabric
        self._host = host
        self._port = port
        self._connect_attempts = connect_attempts
        self._connect_delay = connect_delay

    @property
    def host(self):
        """Returns server IP or name of current Fabric connection"""
        return self._host

    @property
    def port(self):
        """Returns TCP/IP port of current Fabric connection"""
        return self._port

    @property
    def uuid(self):
        """Returns UUID of the Fabric server we are connected with"""
        return _fabric_server_uuid(self._host, self._port)

    def connect(self):
        """Connect with MySQL Fabric"""
        pass

    @property
    def is_connected(self):
        """Check whether connection with Fabric is valid

        Return True if we can still interact with the Fabric server; False
        if Not.

        Returns True or False.
        """
        pass

    def __repr__(self):
        return "{class_}(host={host}, port={port})".format(
            class_=self.__class__,
            host=self._host,
            port=self._port,
        )


class FabricXMLRPCConnection(FabricConnection):

    """Class holding a connection to a MySQL Fabric server through XML-RPC"""

    def __init__(self, fabric, host, port=MYSQL_FABRIC_PORT['xmlrpc'],
                 connect_attempts=_CNX_ATTEMPT_MAX,
                 connect_delay=_CNX_ATTEMPT_DELAY):
        """Initialize"""
        super(FabricXMLRPCConnection, self).__init__(
            fabric, host, port, connect_attempts, connect_delay
        )
        self._proxy = None

    @property
    def proxy(self):
        """Returns the XMLRPC Proxy of current Fabric connection"""
        return self._proxy

    @property
    def uri(self):
        """Returns the XMLRPC URI for current Fabric connection"""
        return _fabric_xmlrpc_uri(self._host, self._port)

    def _xmlrpc_get_proxy(self):
        """Return the XMLRPC server proxy instance to MySQL Fabric

        This method tries to get a valid connection to a MySQL Fabric
        server.

        Returns a XMLRPC ServerProxy instance.
        """
        if self.is_connected:
            return self._proxy

        attempts = self._connect_attempts
        delay = self._connect_delay

        proxy = None
        counter = 0
        while counter != attempts:
            counter += 1
            try:
                if self._fabric.ssl_config:
                    if not HAVE_SSL:
                        raise InterfaceError("Python does not support SSL")
                    https_handler = FabricHTTPSHandler(self._fabric.ssl_config)
                else:
                    https_handler = None

                transport = FabricTransport(self._fabric.username,
                                            self._fabric.password,
                                            verbose=0,
                                            https_handler=https_handler)
                proxy = ServerProxy(self.uri, transport=transport, verbose=0)
                proxy._some_nonexisting_method()  # pylint: disable=W0212
            except Fault:
                # We are actually connected
                return proxy
            except socket.error as exc:
                if counter == attempts:
                    raise InterfaceError(
                        "Connection to MySQL Fabric failed ({0})".format(exc))
                _LOGGER.debug(
                    "Retrying {host}:{port}, attempts {counter}".format(
                        host=self.host, port=self.port, counter=counter))
            if delay > 0:
                time.sleep(delay)

    def connect(self):
        """Connect with MySQL Fabric"""
        self._proxy = self._xmlrpc_get_proxy()

    @property
    def is_connected(self):
        """Check whether connection with Fabric is valid

        Return True if we can still interact with the Fabric server; False
        if Not.

        Returns True or False.
        """
        try:
            self._proxy._some_nonexisting_method()  # pylint: disable=W0212
        except Fault:
            return True
        except (TypeError, AttributeError):
            return False
        else:
            return False


class FabricMySQLConnection(FabricConnection):
    """
    Class holding a connection to a MySQL Fabric server through MySQL protocol
    """
    def __init__(self, fabric, host, port=MYSQL_FABRIC_PORT['mysql'],
                 connect_attempts=_CNX_ATTEMPT_MAX,
                 connect_delay=_CNX_ATTEMPT_DELAY):
        """Initialize"""
        super(FabricMySQLConnection, self).__init__(
            fabric, host, port=port,
            connect_attempts=connect_attempts, connect_delay=connect_delay
        )
        self._connection = None

    @property
    def connection(self):
        """Returns the MySQL RPC Connection to Fabric"""
        return self._connection

    def _get_connection(self):
        """Return the connection instance to MySQL Fabric through MySQL RPC

        This method tries to get a valid connection to a MySQL Fabric
        server.

        Returns a MySQLConnection instance.
        """
        if self.is_connected:
            return self._connection

        attempts = self._connect_attempts
        delay = self._connect_delay

        counter = 0

        while counter != attempts:
            counter += 1
            try:
                dbconfig = {
                    'host': self._host,
                    'port': self._port,
                    'user': self._fabric.username,
                    'password': self._fabric.password
                }
                if self._fabric.ssl_config:
                    if not HAVE_SSL:
                        raise InterfaceError("Python does not support SSL")
                    dbconfig['ssl_key'] = self._fabric.ssl_config['key']
                    dbconfig['ssl_cert'] = self._fabric.ssl_config['cert']

                return MySQLConnection(**dbconfig)

            except AttributeError as exc:
                if counter == attempts:
                    raise InterfaceError(
                        "Connection to MySQL Fabric failed ({0})".format(exc))
                _LOGGER.debug(
                    "Retrying {host}:{port}, attempts {counter}".format(
                        host=self.host, port=self.port, counter=counter))
            if delay > 0:
                time.sleep(delay)

    def connect(self):
        """Connect with MySQL Fabric"""
        self._connection = self._get_connection()

    @property
    def is_connected(self):
        """Check whether connection with Fabric is valid

        Return True if we can still interact with the Fabric server; False
        if Not.

        Returns True or False.
        """
        try:
            return self._connection.is_connected()
        except AttributeError:
            return False


class MySQLFabricConnection(object):

    """Connection to a MySQL server through MySQL Fabric"""

    def __init__(self, **kwargs):
        """Initialize"""
        self._mysql_cnx = None
        self._fabric = None
        self._fabric_mysql_server = None
        self._mysql_config = None
        self._cnx_properties = {}
        self.reset_properties()

        # Validity of fabric-argument is checked in config()-method
        if 'fabric' not in kwargs:
            raise ValueError("Configuration parameters for Fabric missing")

        if kwargs:
            self.store_config(**kwargs)

    def __getattr__(self, attr):
        """Return the return value of the MySQLConnection instance"""
        if attr.startswith('cmd_'):
            raise NotSupportedError(
                "Calling {attr} is not supported for connections managed by "
                "MySQL Fabric.".format(attr=attr))
        return getattr(self._mysql_cnx, attr)

    @property
    def fabric_uuid(self):
        """Returns the Fabric UUID of the MySQL server"""
        if self._fabric_mysql_server:
            return self._fabric_mysql_server.uuid
        return None

    @property
    def properties(self):
        """Returns connection properties"""
        return self._cnx_properties

    def reset_cache(self, group=None):
        """Reset cache for this connection's group"""
        if not group and self._fabric_mysql_server:
            group = self._fabric_mysql_server.group
        self._fabric.reset_cache(group=group)

    def is_connected(self):
        """Check whether we are connected with the MySQL server

        Returns True or False
        """
        return self._mysql_cnx is not None

    def reset_properties(self):
        """Resets the connection properties

        This method can be called to reset the connection properties to
        their default values.
        """
        self._cnx_properties = {}
        for key, attr in _CNX_PROPERTIES.items():
            self._cnx_properties[key] = attr[2]

    def set_property(self, **properties):
        """Set one or more connection properties

        Arguments to the set_property() method will be used as properties.
        They are validated against the _CNX_PROPERTIES constant.

        Raise ValueError in case an invalid property is being set. TypeError
        is raised when the type of the value is not correct.

        To unset a property, set it to None.
        """
        try:
            self.close()
        except Error:
            # We tried, but it's OK when we fail.
            pass

        props = self._cnx_properties

        for name, value in properties.items():
            if name not in _CNX_PROPERTIES:
                raise ValueError(
                    "Invalid property connection {0}".format(name))
            elif value and not isinstance(value, _CNX_PROPERTIES[name][0]):
                valid_types_str = ' or '.join(
                    [atype.__name__ for atype in _CNX_PROPERTIES[name][0]])
                raise TypeError(
                    "{name} is not valid, excepted {typename}".format(
                        name=name, typename=valid_types_str))

            if (name == 'group' and value and
                    (props['key'] or props['tables'])):
                raise ValueError(
                    "'group' property can not be set when 'key' or "
                    "'tables' are set")
            elif name in ('key', 'tables') and value and props['group']:
                raise ValueError(
                    "'key' and 'tables' property can not be "
                    "set together with 'group'")
            elif name == 'scope' and value not in (SCOPE_LOCAL, SCOPE_GLOBAL):
                raise ValueError("Invalid value for 'scope'")
            elif name == 'mode' and value not in (
                    MODE_READWRITE, MODE_READONLY):
                raise ValueError("Invalid value for 'mode'")

            if value is None:
                # Set the default
                props[name] = _CNX_PROPERTIES[name][2]
            else:
                props[name] = value

    def _configure_fabric(self, config):
        """Configure the Fabric connection

        The config argument can be either a dictionary containing the
        necessary information to setup the connection. Or config can
        be an instance of Fabric.
        """
        if isinstance(config, Fabric):
            self._fabric = config
        else:
            required_keys = ['host']
            for required_key in required_keys:
                if required_key not in config:
                    raise ValueError(
                        "Missing configuration parameter '{parameter}' "
                        "for fabric".format(parameter=required_key))
            host = config['host']
            protocol = config.get('protocol', DEFAULT_FABRIC_PROTOCOL)
            try:
                port = config.get('port', MYSQL_FABRIC_PORT[protocol])
            except KeyError:
                raise InterfaceError(
                    "{0} protocol is not available".format(protocol))
            server_uuid = _fabric_server_uuid(host, port)
            try:
                self._fabric = FABRICS[server_uuid]
            except KeyError:
                _LOGGER.debug("New Fabric connection")
                self._fabric = Fabric(**config)
                self._fabric.seed()
                # Cache the new connection
                FABRICS[server_uuid] = self._fabric

    def store_config(self, **kwargs):
        """Store configuration of MySQL connections to use with Fabric

        The configuration found in the dictionary kwargs is used
        when instanciating a MySQLConnection object. The host and port
        entries are used to connect to MySQL Fabric.

        Raises ValueError when the Fabric configuration parameter
        is not correct or missing; AttributeError is raised when
        when a paramater is not valid.
        """
        config = kwargs.copy()

        # Configure the Fabric connection
        if 'fabric' in config:
            self._configure_fabric(config['fabric'])
            del config['fabric']

        if 'unix_socket' in config:
            _LOGGER.warning("MySQL Fabric does not use UNIX sockets.")
            config['unix_socket'] = None

        # Try to use the configuration
        test_config = config.copy()
        if 'pool_name' in test_config:
            del test_config['pool_name']
        if 'pool_size' in test_config:
            del test_config['pool_size']
        if 'pool_reset_session' in test_config:
            del test_config['pool_reset_session']
        try:
            pool = MySQLConnectionPool(pool_name=str(uuid.uuid4()))
            pool.set_config(**test_config)
        except AttributeError as err:
            raise AttributeError(
                "Connection configuration not valid: {0}".format(err))

        self._mysql_config = config

    def _connect(self):
        """Get a MySQL server based on properties and connect

        This method gets a MySQL server from MySQL Fabric using already
        properties set using the set_property() method. You can specify how
        many times and the delay between trying using attempts and
        attempt_delay.

        Raises ValueError when there are problems with arguments or
        properties; InterfaceError on connectivity errors.
        """
        if self.is_connected():
            return
        props = self._cnx_properties
        attempts = props['attempts']
        attempt_delay = props['attempt_delay']

        dbconfig = self._mysql_config.copy()
        counter = 0
        while counter != attempts:
            counter += 1
            try:
                group = None
                if props['tables']:
                    if props['scope'] == 'LOCAL' and not props['key']:
                        raise ValueError(
                            "Scope 'LOCAL' needs key property to be set")
                    mysqlserver = self._fabric.get_shard_server(
                        props['tables'], props['key'],
                        scope=props['scope'],
                        mode=props['mode'])
                elif props['group']:
                    group = props['group']
                    mysqlserver = self._fabric.get_group_server(
                        group, mode=props['mode'])
                else:
                    raise ValueError(
                        "Missing group or key and tables properties")
            except InterfaceError as exc:
                _LOGGER.debug(
                    "Trying to get MySQL server (attempt {0}; {1})".format(
                        counter, exc))
                if counter == attempts:
                    raise InterfaceError("Error getting connection: {0}".format(
                        exc))
                if attempt_delay > 0:
                    _LOGGER.debug("Waiting {0}".format(attempt_delay))
                    time.sleep(attempt_delay)
                continue

            # Make sure we do not change the stored configuration
            dbconfig['host'] = mysqlserver.host
            dbconfig['port'] = mysqlserver.port
            try:
                self._mysql_cnx = mysql.connector.connect(**dbconfig)
            except Error as exc:
                if counter == attempts:
                    self.reset_cache(mysqlserver.group)
                    self._fabric.report_failure(mysqlserver.uuid, exc.errno)
                    raise InterfaceError(
                        "Reported faulty server to Fabric ({0})".format(exc))
                if attempt_delay > 0:
                    time.sleep(attempt_delay)
                continue
            else:
                self._fabric_mysql_server = mysqlserver
                break

    def disconnect(self):
        """Close connection to MySQL server"""
        try:
            self.rollback()
            self._mysql_cnx.close()
        except AttributeError:
            pass  # There was no connection
        except Error:
            raise
        finally:
            self._mysql_cnx = None
            self._fabric_mysql_server = None
    close = disconnect

    def cursor(self, buffered=None, raw=None, prepared=None, cursor_class=None):
        """Instantiates and returns a cursor

        This method is similar to MySQLConnection.cursor() except that
        it checks whether the connection is available and raises
        an InterfaceError when not.

        cursor_class argument is not supported and will raise a
        NotSupportedError exception.

        Returns a MySQLCursor or subclass.
        """
        self._connect()
        if cursor_class:
            raise NotSupportedError(
                "Custom cursors not supported with MySQL Fabric")

        if prepared:
            raise NotSupportedError(
                "Prepared Statements are not supported with MySQL Fabric")

        if self._unread_result is True:
            raise InternalError("Unread result found.")

        buffered = buffered or self._buffered
        raw = raw or self._raw

        cursor_type = 0
        if buffered is True:
            cursor_type |= 1
        if raw is True:
            cursor_type |= 2

        types = (
            MySQLCursor,  # 0
            MySQLCursorBuffered,
            MySQLCursorRaw,
            MySQLCursorBufferedRaw,
        )
        return (types[cursor_type])(self)

    def handle_mysql_error(self, exc):
        """Handles MySQL errors

        This method takes a mysql.connector.errors.Error exception
        and checks the error code. Based on the value, it takes
        certain actions such as clearing the cache.
        """
        if exc.errno in RESET_CACHE_ON_ERROR:
            self.reset_cache()
            self.disconnect()
            raise MySQLFabricError(
                "Temporary error ({error}); "
                "retry transaction".format(error=str(exc)))

        raise exc

    def commit(self):
        """Commit current transaction

        Raises whatever MySQLConnection.commit() raises, but
        raises MySQLFabricError when MySQL returns error
        ER_OPTION_PREVENTS_STATEMENT.
        """
        try:
            self._mysql_cnx.commit()
        except Error as exc:
            self.handle_mysql_error(exc)

    def rollback(self):
        """Rollback current transaction

        Raises whatever MySQLConnection.rollback() raises, but
        raises MySQLFabricError when MySQL returns error
        ER_OPTION_PREVENTS_STATEMENT.
        """
        try:
            self._mysql_cnx.rollback()
        except Error as exc:
            self.handle_mysql_error(exc)

    def cmd_query(self, statement):
        """Send a statement to the MySQL server

        Raises whatever MySQLConnection.cmd_query() raises, but
        raises MySQLFabricError when MySQL returns error
        ER_OPTION_PREVENTS_STATEMENT.

        Returns a dictionary.
        """
        self._connect()
        try:
            return self._mysql_cnx.cmd_query(statement)
        except Error as exc:
            self.handle_mysql_error(exc)

    def cmd_query_iter(self, statements):
        """Send one or more statements to the MySQL server

        Raises whatever MySQLConnection.cmd_query_iter() raises, but
        raises MySQLFabricError when MySQL returns error
        ER_OPTION_PREVENTS_STATEMENT.

        Returns a dictionary.
        """
        self._connect()
        try:
            return self._mysql_cnx.cmd_query_iter(statements)
        except Error as exc:
            self.handle_mysql_error(exc)
