# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2016, 2017, Oracle and/or its affiliates. All rights reserved.

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

"""MySQL X DevAPI Python implementation"""

import re
from . import constants

from .config import (PersistenceHandler, SessionConfigManager, SessionConfig,
                     PasswordHandler)
from .compat import STRING_TYPES, urlparse, unquote, parse_qsl
from .connection import Session
from .constants import SSLMode, Auth
from .crud import Schema, Collection, Table, View
from .dbdoc import DbDoc
from .errors import (Error, Warning, InterfaceError, DatabaseError,
                     NotSupportedError, DataError, IntegrityError,
                     ProgrammingError, OperationalError, InternalError,
                     PoolError)
from .result import (ColumnMetaData, Row, Result, BufferingResult, RowResult,
                     SqlResult, ColumnType)
from .statement import (Statement, FilterableStatement, SqlStatement,
                        FindStatement, AddStatement, RemoveStatement,
                        ModifyStatement, SelectStatement, InsertStatement,
                        DeleteStatement, UpdateStatement,
                        CreateCollectionIndexStatement, CreateTableStatement,
                        CreateViewStatement, AlterViewStatement, ColumnDef,
                        GeneratedColumnDef, ForeignKeyDef, Expr,
                        ReadStatement, WriteStatement)

_SPLIT = re.compile(r',(?![^\(\)]*\))')
_PRIORITY = re.compile(r'^\(address=(.+),priority=(\d+)\)$', re.VERBOSE)
ssl_opts = ["ssl-cert", "ssl-ca", "ssl-key", "ssl-crl"]
sess_opts = ssl_opts + ["user", "password", "schema", "host", "port",
                        "routers", "socket", "ssl-mode", "auth"]

def _parse_address_list(path):
    """Parses a list of host, port pairs

    Args:
        path: String containing a list of routers or just router

    Returns:
        Returns a dict with parsed values of host, port and priority if
        specified.
    """
    path = path.replace(" ", "")
    array  = not("," not in path and path.count(":") > 1
                 and path.count("[") == 1) and \
             path.startswith("[") and path.endswith("]")

    routers = []
    address_list = _SPLIT.split(path[1:-1] if array else path)
    for address in address_list:
        router = {}

        match = _PRIORITY.match(address)
        if match:
            address = match.group(1)
            router["priority"] = int(match.group(2))

        match = urlparse("//{0}".format(address))
        if not match.hostname:
            raise InterfaceError("Invalid address: {0}".format(address))

        router.update(host=match.hostname, port=match.port)
        routers.append(router)

    return {"routers": routers} if array else routers[0]

def _parse_connection_uri(uri):
    """Parses the connection string and returns a dictionary with the
    connection settings.

    Args:
        uri: mysqlx URI scheme to connect to a MySQL server/farm.

    Returns:
        Returns a dict with parsed values of credentials and address of the
        MySQL server/farm.
    """
    settings = {"schema": ""}
    uri = "{0}{1}".format("" if uri.startswith("mysqlx://")
                          else "mysqlx://", uri)
    scheme, temp = uri.split("://", 1)
    userinfo, temp = temp.partition("@")[::2]
    host, query_str = temp.partition("?")[::2]

    pos = host.rfind("/")
    if host[pos:].find(")") is -1 and pos > 0:
        host, settings["schema"] = host.rsplit("/", 1)
    host = host.strip("()")

    if not host or not userinfo or ":" not in userinfo:
        raise InterfaceError("Malformed URI '{0}'".format(uri))
    settings["user"], settings["password"] = userinfo.split(":", 1)

    if host.startswith(("/", "..", ".")):
        settings["socket"] = unquote(host)
    elif host.startswith("\\."):
        raise InterfaceError("Windows Pipe is not supported.")
    else:
        settings.update(_parse_address_list(host))

    for key, val in parse_qsl(query_str, True):
        opt = key.lower()
        if opt in settings:
            raise InterfaceError("Duplicate option '{0}'.".format(key))
        if opt in ssl_opts:
            settings[opt] = unquote(val.strip("()"))
        else:
            settings[opt] = val.lower()
    return settings

def _validate_settings(settings):
    """Validates the settings to be passed to a Session object
    the port values are converted to int if specified or set to 33060
    otherwise. The priority values for each router is converted to int
    if specified.

    Args:
        settings: dict containing connection settings.
    """
    invalid_opts = set(settings.keys()).difference(sess_opts)
    if invalid_opts:
        raise ProgrammingError("Invalid options: {0}."
                               "".format(", ".join(invalid_opts)))

    if "routers" in settings:
        for router in settings["routers"]:
            _validate_hosts(router)
    elif "host" in settings:
        _validate_hosts(settings)

    if "ssl-mode" in settings:
        try:
            settings["ssl-mode"] = settings["ssl-mode"].lower()
            SSLMode.index(settings["ssl-mode"])
        except (AttributeError, ValueError):
            raise InterfaceError("Invalid SSL Mode '{0}'."
                                 "".format(settings["ssl-mode"]))
        if settings["ssl-mode"] == SSLMode.DISABLED and \
            any(key in settings for key in ssl_opts):
            raise InterfaceError("SSL options used with ssl-mode 'disabled'.")

    if "ssl-crl" in settings and not "ssl-ca" in settings:
        raise InterfaceError("CA Certificate not provided.")
    if "ssl-key" in settings and not "ssl-cert" in settings:
        raise InterfaceError("Client Certificate not provided.")

    if not "ssl-ca" in settings and settings.get("ssl-mode") \
        in [SSLMode.VERIFY_IDENTITY, SSLMode.VERIFY_CA]:
        raise InterfaceError("Cannot verify Server without CA.")
    if "ssl-ca" in settings and settings.get("ssl-mode") \
        not in [SSLMode.VERIFY_IDENTITY, SSLMode.VERIFY_CA]:
        raise InterfaceError("Must verify Server if CA is provided.")

    if "auth" in settings:
        try:
            settings["auth"] = settings["auth"].lower()
            Auth.index(settings["auth"])
        except (AttributeError, ValueError):
            raise InterfaceError("Invalid Auth '{0}'".format(settings["auth"]))


def _validate_hosts(settings):
    if "priority" in settings and settings["priority"]:
        try:
            settings["priority"] = int(settings["priority"])
        except NameError:
            raise InterfaceError("Invalid priority")

    if "port" in settings and settings["port"]:
        try:
            settings["port"] = int(settings["port"])
        except NameError:
            raise InterfaceError("Invalid port")
    elif "host" in settings:
        settings["port"] = 33060

def _get_connection_settings(*args, **kwargs):
    """Parses the connection string and returns a dictionary with the
    connection settings.

    Args:
        *args: Variable length argument list with the connection data used
               to connect to the database. It can be a dictionary or a
               connection string.
        **kwargs: Arbitrary keyword arguments with connection data used to
                  connect to the database.

    Returns:
        mysqlx.Session: Session object.
    """
    settings = {}
    if args:
        if isinstance(args[0], STRING_TYPES):
            settings = _parse_connection_uri(args[0])
        elif isinstance(args[0], dict):
            settings.update(args[0])
        elif isinstance(args[0], SessionConfig):
            settings.update(args[0].to_dict())
            settings.pop("appdata", None)

        if len(args) is 2:
            settings["password"] = args[1]
    elif kwargs:
        settings.update(kwargs)
        for key, val in settings.items():
            if "_" in key:
                settings[key.replace("_", "-")] = settings.pop(key)

    if "session_name" in settings:
        sess_config = sessions.get(settings.pop("session_name")).to_dict()
        settings = dict(sess_config, **settings)
        settings.pop("appdata", None)
    if "uri" in settings:
        settings = dict(_parse_connection_uri(settings.pop("uri")), **settings)

    if not settings:
        raise InterfaceError("Settings not provided")

    _validate_settings(settings)
    return settings

def get_session(*args, **kwargs):
    """Creates a Session instance using the provided connection data.

    Args:
        *args: Variable length argument list with the connection data used
               to connect to the database. It can be a dictionary or a
               connection string.
        **kwargs: Arbitrary keyword arguments with connection data used to
                  connect to the database.

    Returns:
        mysqlx.Session: Session object.
    """
    settings = _get_connection_settings(*args, **kwargs)
    return Session(settings)


sessions = SessionConfigManager()
sessions.set_persistence_handler(PersistenceHandler())

__all__ = [
    # mysqlx.connection
    "Session", "get_session",

    # mysqlx.sessions
    "sessions",

    # mysqlx.constants
    "constants",

    # mysqlx.crud
    "Schema", "Collection", "Table", "View",

    # mysqlx.errors
    "Error", "Warning", "InterfaceError", "DatabaseError", "NotSupportedError",
    "DataError", "IntegrityError", "ProgrammingError", "OperationalError",
    "InternalError", "PoolError",

    # mysqlx.result
    "ColumnMetaData", "Row", "Result", "BufferingResult", "RowResult",
    "SqlResult", "ColumnType",

    # mysqlx.statement
    "DbDoc", "Statement", "FilterableStatement", "SqlStatement",
    "FindStatement", "AddStatement", "RemoveStatement", "ModifyStatement",
    "SelectStatement", "InsertStatement", "DeleteStatement", "UpdateStatement",
    "CreateCollectionIndexStatement", "CreateTableStatement",
    "CreateViewStatement", "AlterViewStatement","ColumnDef",
    "GeneratedColumnDef", "ForeignKeyDef", "Expr",
]
