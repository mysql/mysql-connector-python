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

"""Implementation of the X protocol for MySQL servers."""

import struct

from .protobuf import mysqlx_pb2 as MySQLx
from .protobuf import mysqlx_session_pb2 as MySQLxSession
from .protobuf import mysqlx_sql_pb2 as MySQLxSQL
from .protobuf import mysqlx_notice_pb2 as MySQLxNotice
from .protobuf import mysqlx_datatypes_pb2 as MySQLxDatatypes
from .protobuf import mysqlx_resultset_pb2 as MySQLxResultset
from .protobuf import mysqlx_crud_pb2 as MySQLxCrud
from .protobuf import mysqlx_expr_pb2 as MySQLxExpr
from .protobuf import mysqlx_connection_pb2 as MySQLxConnection
from .result import ColumnMetaData
from .compat import STRING_TYPES, INT_TYPES
from .dbdoc import DbDoc
from .errors import InterfaceError, OperationalError, ProgrammingError
from .expr import (ExprParser, build_null_scalar, build_string_scalar,
                   build_bool_scalar, build_double_scalar, build_int_scalar)


_SERVER_MESSAGES = [
    (MySQLx.ServerMessages.SESS_AUTHENTICATE_CONTINUE,
     MySQLxSession.AuthenticateContinue),
    (MySQLx.ServerMessages.SESS_AUTHENTICATE_OK,
     MySQLxSession.AuthenticateOk),
    (MySQLx.ServerMessages.SQL_STMT_EXECUTE_OK, MySQLxSQL.StmtExecuteOk),
    (MySQLx.ServerMessages.ERROR, MySQLx.Error),
    (MySQLx.ServerMessages.NOTICE, MySQLxNotice.Frame),
    (MySQLx.ServerMessages.RESULTSET_COLUMN_META_DATA,
     MySQLxResultset.ColumnMetaData),
    (MySQLx.ServerMessages.RESULTSET_ROW, MySQLxResultset.Row),
    (MySQLx.ServerMessages.RESULTSET_FETCH_DONE, MySQLxResultset.FetchDone),
    (MySQLx.ServerMessages.RESULTSET_FETCH_DONE_MORE_RESULTSETS,
     MySQLxResultset.FetchDoneMoreResultsets),
    (MySQLx.ServerMessages.OK, MySQLx.Ok),
    (MySQLx.ServerMessages.CONN_CAPABILITIES, MySQLxConnection.Capabilities),
]


def encode_to_bytes(value, encoding="utf-8"):
    return value if isinstance(value, bytes) else value.encode(encoding)


class MessageReaderWriter(object):
    def __init__(self, socket_stream):
        self._stream = socket_stream
        self._msg = None

    def push_message(self, msg):
        if self._msg is not None:
            raise OperationalError("Message push slot is full")
        self._msg = msg

    def read_message(self):
        if self._msg is not None:
            m = self._msg
            self._msg = None
            return m
        return self._read_message()

    def _read_message(self):
        hdr = self._stream.read(5)
        msg_len, msg_type = struct.unpack("<LB", hdr)
        payload = self._stream.read(msg_len - 1)

        for msg_tuple in _SERVER_MESSAGES:
            if msg_tuple[0] == msg_type:
                msg = msg_tuple[1]()
                msg.ParseFromString(payload)
                return msg

        raise ValueError("Unknown msg_type: {0}".format(msg_type))

    def write_message(self, msg_id, msg):
        msg_str = msg.SerializeToString()
        header = struct.pack("<LB", len(msg_str) + 1, msg_id)
        self._stream.sendall(b"".join([header, msg_str]))


class Protocol(object):
    def __init__(self, reader_writer):
        self._reader = reader_writer
        self._writer = reader_writer
        self._message = None

    def get_capabilites(self):
        msg = MySQLxConnection.CapabilitiesGet()
        self._writer.write_message(
            MySQLx.ClientMessages.CON_CAPABILITIES_GET, msg)
        return self._reader.read_message()

    def set_capabilities(self, **kwargs):
        msg = MySQLxConnection.CapabilitiesSet()
        for key, value in kwargs.items():
            value = self._create_any(value)
            capability = MySQLxConnection.Capability(name=key, value=value)
            msg.capabilities.capabilities.extend([capability])

        self._writer.write_message(
            MySQLx.ClientMessages.CON_CAPABILITIES_SET, msg)

        return self.read_ok()

    def send_auth_start(self, method):
        msg = MySQLxSession.AuthenticateStart(mech_name=method)
        self._writer.write_message(
            MySQLx.ClientMessages.SESS_AUTHENTICATE_START, msg)

    def read_auth_continue(self):
        msg = self._reader.read_message()
        if not isinstance(msg, MySQLxSession.AuthenticateContinue):
            raise InterfaceError("Unexpected message encountered during "
                                 "authentication handshake")
        return msg.auth_data

    def send_auth_continue(self, data):
        msg = MySQLxSession.AuthenticateContinue(
            auth_data=encode_to_bytes(data))
        self._writer.write_message(
            MySQLx.ClientMessages.SESS_AUTHENTICATE_CONTINUE, msg)

    def read_auth_ok(self):
        while True:
            msg = self._reader.read_message()
            if isinstance(msg, MySQLxSession.AuthenticateOk):
                break
            if isinstance(msg, MySQLx.Error):
                raise InterfaceError(msg.msg)

    def get_binding_scalars(self, statement):
        count = len(statement._binding_map)
        scalars = count * [None]

        for binding in statement._bindings:
            name = binding["name"]
            if name not in statement._binding_map:
                raise ProgrammingError("Unable to find placeholder for "
                                       "parameter: {0}".format(name))
            pos = statement._binding_map[name]
            scalars[pos] = self.arg_object_to_scalar(binding["value"],
                                                     not statement._doc_based)
        return scalars

    def _apply_filter(self, message, statement):
        if statement._has_where:
            message.criteria.CopyFrom(statement._where_expr)
        if statement._has_bindings:
            message.args.extend(self.get_binding_scalars(statement))
        if statement._has_limit:
            message.limit.row_count = statement._limit_row_count
            message.limit.offset = statement._limit_offset
        if statement._has_sort:
            message.order.extend(statement._sort_expr)
        if statement._has_group_by:
            message.grouping.extend(statement._grouping)
        if statement._has_having:
            message.grouping_criteria.CopyFrom(statement._having)

    def send_find(self, stmt):
        find = MySQLxCrud.Find(
            data_model=(MySQLxCrud.DOCUMENT
                        if stmt._doc_based else MySQLxCrud.TABLE),
            collection=MySQLxCrud.Collection(name=stmt.target.name,
                                             schema=stmt.schema.name))
        if stmt._has_projection:
            find.projection.extend(stmt._projection_expr)
        self._apply_filter(find, stmt)
        self._writer.write_message(MySQLx.ClientMessages.CRUD_FIND, find)

    def send_update(self, statement):
        update = MySQLxCrud.Update(
            data_model=(MySQLxCrud.DOCUMENT
                        if statement._doc_based else MySQLxCrud.TABLE),
            collection=MySQLxCrud.Collection(name=statement.target.name,
                                             schema=statement.schema.name))
        self._apply_filter(update, statement)
        for update_op in statement._update_ops:
            opexpr = MySQLxCrud.UpdateOperation(
                operation=update_op.update_type, source=update_op.source)
            if update_op.value is not None:
                opexpr.value.CopyFrom(
                    self.arg_object_to_expr(
                        update_op.value, not statement._doc_based))
            update.operation.extend([opexpr])
        self._writer.write_message(MySQLx.ClientMessages.CRUD_UPDATE, update)

    def send_delete(self, stmt):
        delete = MySQLxCrud.Delete(
            data_model=(MySQLxCrud.DOCUMENT
                        if stmt._doc_based else MySQLxCrud.TABLE),
            collection=MySQLxCrud.Collection(name=stmt.target.name,
                                             schema=stmt.schema.name))
        self._apply_filter(delete, stmt)
        self._writer.write_message(MySQLx.ClientMessages.CRUD_DELETE, delete)

    def send_execute_statement(self, namespace, stmt, args):
        stmt = MySQLxSQL.StmtExecute(namespace=namespace,
                                     stmt=encode_to_bytes(stmt),
                                     compact_metadata=False)
        for arg in args:
            value = self._create_any(arg)
            stmt.args.extend([value])
        self._writer.write_message(MySQLx.ClientMessages.SQL_STMT_EXECUTE,
                                   stmt)

    def send_insert(self, statement):
        insert = MySQLxCrud.Insert(
            data_model=(MySQLxCrud.DOCUMENT
                        if statement._doc_based else MySQLxCrud.TABLE),
            collection=MySQLxCrud.Collection(name=statement.target.name,
                                             schema=statement.schema.name))
        if hasattr(statement, "_fields"):
            for field in statement._fields:
                insert.projection.extend([
                    ExprParser(field, not statement._doc_based)
                    .parse_table_insert_field()])
        for value in statement._values:
            row = MySQLxCrud.Insert.TypedRow()
            if isinstance(value, list):
                for val in value:
                    obj = self.arg_object_to_expr(
                        val, not statement._doc_based)
                    row.field.extend([obj])
            else:
                obj = self.arg_object_to_expr(value, not statement._doc_based)
                row.field.extend([obj])
            insert.row.extend([row])
        self._writer.write_message(MySQLx.ClientMessages.CRUD_INSERT, insert)

    def _create_any(self, arg):
        if isinstance(arg, STRING_TYPES):
            val = MySQLxDatatypes.Scalar.String(value=encode_to_bytes(arg))
            scalar = MySQLxDatatypes.Scalar(type=8, v_string=val)
            return MySQLxDatatypes.Any(type=1, scalar=scalar)
        elif isinstance(arg, bool):
            return MySQLxDatatypes.Any(type=1, scalar=build_bool_scalar(arg))
        elif isinstance(arg, INT_TYPES):
            return MySQLxDatatypes.Any(type=1, scalar=build_int_scalar(arg))
        return None

    def close_result(self, rs):
        msg = self._read_message(rs)
        if msg is not None:
            raise OperationalError("Expected to close the result")

    def read_row(self, rs):
        msg = self._read_message(rs)
        if msg is None:
            return None
        if isinstance(msg, MySQLxResultset.Row):
            return msg
        self._reader.push_message(msg)
        return None

    def _process_frame(self, msg, rs):
        if msg.type == 1:
            warningMsg = MySQLxNotice.Warning()
            warningMsg.ParseFromString(msg.payload)
            rs._warnings.append(Warning(warningMsg.level, warningMsg.code,
                                        warningMsg.msg))
        elif msg.type == 2:
            sessVarMsg = MySQLxNotice.SessionVariableChanged()
            sessVarMsg.ParseFromString(msg.payload)
        elif msg.type == 3:
            sessStateMsg = MySQLxNotice.SessionStateChanged()
            sessStateMsg.ParseFromString(msg.payload)
            if sessStateMsg.param == \
                    MySQLxNotice.SessionStateChanged.ROWS_AFFECTED:
                rs._rows_affected = sessStateMsg.value.v_unsigned_int
            elif sessStateMsg.param == \
                    MySQLxNotice.SessionStateChanged.GENERATED_INSERT_ID:
                rs._generated_id = sessStateMsg.value.v_unsigned_int

    def _read_message(self, rs):
        while True:
            msg = self._reader.read_message()
            if isinstance(msg, MySQLx.Error):
                raise OperationalError(msg.msg)
            elif isinstance(msg, MySQLxNotice.Frame):
                self._process_frame(msg, rs)
            elif isinstance(msg, MySQLxSQL.StmtExecuteOk):
                return None
            elif isinstance(msg, MySQLxResultset.FetchDone):
                rs._closed = True
            elif isinstance(msg, MySQLxResultset.FetchDoneMoreResultsets):
                rs._has_more_results = True
            else:
                break
        return msg

    def get_column_metadata(self, rs):
        columns = []
        while True:
            msg = self._read_message(rs)
            if msg is None:
                break
            if isinstance(msg, MySQLxResultset.Row):
                self._reader.push_message(msg)
                break
            if not isinstance(msg, MySQLxResultset.ColumnMetaData):
                raise InterfaceError("Unexpected msg type")
            col = ColumnMetaData(msg.type, msg.catalog, msg.schema, msg.table,
                                 msg.original_table, msg.name,
                                 msg.original_name, msg.length, msg.collation,
                                 msg.fractional_digits, msg.flags,
                                 msg.content_type)
            columns.append(col)
        return columns

    def arg_object_to_expr(self, value, allow_relational):
        if value is None:
            return MySQLxExpr.Expr(type=MySQLxExpr.Expr.LITERAL,
                                   literal=build_null_scalar())
        if isinstance(value, bool):
            return MySQLxExpr.Expr(type=MySQLxExpr.Expr.LITERAL,
                                   literal=build_bool_scalar(value))
        elif isinstance(value, INT_TYPES):
            return MySQLxExpr.Expr(type=MySQLxExpr.Expr.LITERAL,
                                   literal=build_int_scalar(value))
        elif isinstance(value, (float)):
            return MySQLxExpr.Expr(type=MySQLxExpr.Expr.LITERAL,
                                   literal=build_double_scalar(value))
        elif isinstance(value, STRING_TYPES):
            try:
                expression = ExprParser(value, allow_relational).expr()
                if expression.has_identifier():
                    return MySQLxExpr.Expr(type=MySQLxExpr.Expr.LITERAL,
                                           literal=build_string_scalar(value))
                return expression
            except:
                return MySQLxExpr.Expr(type=MySQLxExpr.Expr.LITERAL,
                                       literal=build_string_scalar(value))
        elif isinstance(value, DbDoc):
            return MySQLxExpr.Expr(type=MySQLxExpr.Expr.LITERAL,
                                   literal=build_string_scalar(str(value)))
        raise InterfaceError("Unsupported type: {0}".format(type(value)))

    def arg_object_to_scalar(self, value, allow_relational):
        return self.arg_object_to_expr(value, allow_relational).literal

    def read_ok(self):
        msg = self._reader.read_message()
        if isinstance(msg, MySQLx.Error):
            raise InterfaceError(msg.msg)

        if not isinstance(msg, MySQLx.Ok):
            raise InterfaceError("Unexpected message encountered")

    def send_close(self):
        msg = MySQLxSession.Close()
        self._writer.write_message(MySQLx.ClientMessages.SESS_CLOSE, msg)
