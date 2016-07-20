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

from urlparse import urlparse

from .connection import XSession, NodeSession
from .crud import Schema, Collection, Table
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
                        DropCollectionIndexStatement)
from .dbdoc import DbDoc


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
        if isinstance(args[0], (str, unicode,)):
            uri = "{0}{1}".format("" if args[0].startswith("mysqlx://")
                                  else "mysqlx://", args[0])
            parsed = urlparse(uri)
            if parsed.hostname is None or parsed.username is None \
               or parsed.password is None:
                raise InterfaceError("Malformed URI '{0}'".format(args[0]))
            settings = {
                "host": parsed.hostname,
                "port": parsed.port,
                "user": parsed.username,
                "password": parsed.password,
                "database": parsed.path.lstrip("/")
            }
        elif isinstance(args[0], dict):
            settings = args[0]
    elif kwargs:
        settings = kwargs

    if not settings:
        raise InterfaceError("Settings not provided")

    if "port" in settings and settings["port"]:
        try:
            settings["port"] = int(settings["port"])
        except NameError:
            raise InterfaceError("Invalid port")
    else:
        settings["port"] = 33060

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
    return NodeSession(settings)


__all__ = [
    # mysqlx.connection
    "XSession", "NodeSession", "get_session", "get_node_session",

    # mysqlx.crud
    "Schema", "Collection", "Table",

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
]
