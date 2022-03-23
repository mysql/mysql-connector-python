# Copyright (c) 2016, 2022, Oracle and/or its affiliates.
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

"""MySQL X DevAPI Python implementation."""

import logging

from .connection import Client, Session, get_client, get_session
from .constants import Auth, Compression, LockContention, SSLMode
from .crud import Collection, Schema, Table, View
from .dbdoc import DbDoc
from .errors import (  # pylint: disable=redefined-builtin
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    PoolError,
    ProgrammingError,
    TimeoutError,
)
from .expr import ExprParser as expr
from .result import (
    BufferingResult,
    Column,
    ColumnType,
    DocResult,
    Result,
    Row,
    RowResult,
    SqlResult,
)
from .statement import (
    AddStatement,
    CreateCollectionIndexStatement,
    DeleteStatement,
    Expr,
    FilterableStatement,
    FindStatement,
    InsertStatement,
    ModifyStatement,
    ReadStatement,
    RemoveStatement,
    SelectStatement,
    SqlStatement,
    Statement,
    UpdateStatement,
    WriteStatement,
)

logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    # mysqlx.connection
    "Client",
    "Session",
    "get_client",
    "get_session",
    "expr",
    # mysqlx.constants
    "Auth",
    "Compression",
    "LockContention",
    "SSLMode",
    # mysqlx.crud
    "Schema",
    "Collection",
    "Table",
    "View",
    # mysqlx.errors
    "Error",
    "InterfaceError",
    "DatabaseError",
    "NotSupportedError",
    "DataError",
    "IntegrityError",
    "ProgrammingError",
    "OperationalError",
    "InternalError",
    "PoolError",
    "TimeoutError",
    # mysqlx.result
    "Column",
    "Row",
    "Result",
    "BufferingResult",
    "RowResult",
    "SqlResult",
    "DocResult",
    "ColumnType",
    # mysqlx.statement
    "DbDoc",
    "Statement",
    "FilterableStatement",
    "SqlStatement",
    "FindStatement",
    "AddStatement",
    "RemoveStatement",
    "ModifyStatement",
    "SelectStatement",
    "InsertStatement",
    "DeleteStatement",
    "UpdateStatement",
    "ReadStatement",
    "WriteStatement",
    "CreateCollectionIndexStatement",
    "Expr",
]
