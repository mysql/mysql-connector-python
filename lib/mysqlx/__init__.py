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

"""MySQL X DevAPI Python implementation"""

import re

from . import constants

from .compat import STRING_TYPES, urlparse, unquote, parse_qsl
from .connection import XSession, NodeSession
from .crud import Schema, Collection, Table, View
from .dbdoc import DbDoc
from .errors import (Error, Warning, InterfaceError, DatabaseError,
                     NotSupportedError, DataError, IntegrityError,
                     ProgrammingError, OperationalError, InternalError)
from .result import (ColumnMetaData, Row, Result, BufferingResult, RowResult,
                     SqlResult, ColumnType)
from .statement import (Statement, FilterableStatement, SqlStatement,
                        FindStatement, AddStatement, RemoveStatement,
                        ModifyStatement, SelectStatement, InsertStatement,
                        DeleteStatement, UpdateStatement,
                        CreateCollectionIndexStatement,
                        DropCollectionIndexStatement, CreateViewStatement,
                        AlterViewStatement, ColumnDef,
                        GeneratedColumnDef, ForeignKeyDef, Expr)


def _parse_address_list(address_list):
    """Parses a list of host, port pairs

    Args:
        address_list: String containing a list of routers or just router

    Returns:
        Returns a dict with parsed values of host, port and priority if
        specified.
    """
    is_list  = re.compile(r'^\[(?![^,]*\]).*]$')
    hst_list = re.compile(r',(?![^\(\)]*\))')
    pri_addr = re.compile(r'^\(address\s*=\s*(?P<address>.+)\s*,\s*priority\s*=\s*(?P<priority>\d+)\)$')

    routers = []
    if is_list.match(address_list):
        address_list = address_list.strip("[]")
        address_list = hst_list.split(address_list)
    else:
        match = urlparse("//{0}".format(address_list))
        return {
            "host": match.hostname,
            "port": match.port
        }

    while address_list:
        router = {}
        address = address_list.pop(0).strip()
        match = pri_addr.match(address)
        if match:
            address = match.group("address").strip()
            router["priority"] = int(match.group("priority"))

        match = urlparse("//{0}".format(address))
        if not match.hostname:
            raise InterfaceError("Invalid address: {0}".format(address))

        router["host"] = match.hostname
        router["port"] = match.port
        routers.append(router)

    return { "routers": routers }

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

    for opt, val in dict(parse_qsl(query_str, True)).items():
        settings[opt] = unquote(val.strip("()")) or True
    return settings

def _validate_settings(settings):
    """Validates the settings to be passed to a Session object
    the port values are converted to int if specified or set to 33060
    otherwise. The priority values for each router is converted to int
    if specified.

    Args:
        settings: dict containing connection settings.
    """
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
        mysqlx.XSession: XSession object.
    """
    settings = {}
    if args:
        if isinstance(args[0], STRING_TYPES):
            settings = _parse_connection_uri(args[0])
        elif isinstance(args[0], dict):
            settings.update(args[0])
    elif kwargs:
        settings.update(kwargs)

    if not settings:
        raise InterfaceError("Settings not provided")

    if "routers" in settings:
        for router in settings.get("routers"):
            _validate_settings(router)
    else:
        _validate_settings(settings)

    return settings

def get_session(*args, **kwargs):
    """Creates a XSession instance using the provided connection data.

    Args:
        *args: Variable length argument list with the connection data used
               to connect to the database. It can be a dictionary or a
               connection string.
        **kwargs: Arbitrary keyword arguments with connection data used to
                  connect to the database.

    Returns:
        mysqlx.XSession: XSession object.
    """
    settings = _get_connection_settings(*args, **kwargs)
    return XSession(settings)


def get_node_session(*args, **kwargs):
    """Creates a NodeSession instance using the provided connection data.

    Args:
        *args: Variable length argument list with the connection data used
               to connect to the database. It can be a dictionary or a
               connection string.
        **kwargs: Arbitrary keyword arguments with connection data used to
                  connect to the database.

    Returns:
        mysqlx.XSession: XSession object.
    """
    settings = _get_connection_settings(*args, **kwargs)
    if "routers" in settings:
        raise InterfaceError("NodeSession expects only one pair of host and port")

    return NodeSession(settings)


__all__ = [
    # mysqlx.connection
    "XSession", "NodeSession", "get_session", "get_node_session",

    # mysqlx.constants
    "constants",

    # mysqlx.crud
    "Schema", "Collection", "Table", "View",

    # mysqlx.errors
    "Error", "Warning", "InterfaceError", "DatabaseError", "NotSupportedError",
    "DataError", "IntegrityError", "ProgrammingError", "OperationalError",
    "InternalError",

    # mysqlx.result
    "ColumnMetaData", "Row", "Result", "BufferingResult", "RowResult",
    "SqlResult", "ColumnType",

    # mysqlx.statement
    "DbDoc", "Statement", "FilterableStatement", "SqlStatement",
    "FindStatement", "AddStatement", "RemoveStatement", "ModifyStatement",
    "SelectStatement", "InsertStatement", "DeleteStatement", "UpdateStatement",
    "CreateCollectionIndexStatement", "DropCollectionIndexStatement",
    "CreateViewStatement", "AlterViewStatement",
    "ColumnDef", "GeneratedColumnDef", "ForeignKeyDef", "Expr",
]
