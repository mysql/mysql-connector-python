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

import struct

from .protobuf import mysqlx_pb2 as MySQLx
from .protobuf import mysqlx_session_pb2 as MySQLxSession
from .protobuf import mysqlx_sql_pb2 as MySQLxSQL
from .protobuf import mysqlx_notice_pb2 as MySQLxNotice
from .protobuf import mysqlx_datatypes_pb2 as MySQLxDatatypes
from .protobuf import mysqlx_resultset_pb2 as MySQLxResultset
from .protobuf import mysqlx_crud_pb2 as MySQLxCrud
from .protobuf import mysqlx_expr_pb2 as MySQLxExpr
from .result import *
from .statement import *
from .expr import *

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
]


class MessageReaderWriter(object):
    def __init__(self, socket_stream):
        self._stream = socket_stream
        self._msg = None

#    def peek_message(self):
 #       if self._msg == None:
  #          self._msg = self._read_message()
   #     return self._msg

    def push_message(self, msg):
        if not self._msg == None:
            raise Exception("message push slot is full")
        self._msg = msg

    def read_message(self):
        if not self._msg == None:
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

        raise Exception("Unknown msg_type: {0}".format(msg_type))

    def write_message(self, msg_id, msg):
        msg_str = msg.SerializeToString()
        header = struct.pack("<LB", len(msg_str) + 1, msg_id)
        self._stream.sendall("{0}{1}".format(header, msg_str))


class Protocol(object):
    def __init__(self, reader_writer):
        self._reader = reader_writer
        self._writer = reader_writer
        self._message = None

    def send_auth_start(self, method):
        msg = MySQLxSession.AuthenticateStart(mech_name=method)
        self._writer.write_message(
            MySQLx.ClientMessages.SESS_AUTHENTICATE_START, msg)

    def read_auth_continue(self):
        msg = self._reader.read_message()
        if not isinstance(msg, MySQLxSession.AuthenticateContinue):
            raise Exception("Unexpected message encountered during "
                            "authentication handshake")
        return msg.auth_data

    def send_auth_continue(self, data):
        msg = MySQLxSession.AuthenticateContinue(auth_data=data)
        self._writer.write_message(
            MySQLx.ClientMessages.SESS_AUTHENTICATE_CONTINUE, msg)

    def read_auth_ok(self):
        while True:
            msg = self._reader.read_message()
            if msg.__class__ is MySQLxSession.AuthenticateOk:
                break
            if msg.__class__ is MySQLx.Error:
                raise Exception(msg.msg)

    def send_insert(self, schema, target, is_docs, rows, cols):
        stmt = MySQLxCrud.Insert(
            datamodel=MySQLxCrud.DOCUMENT if is_docs else MySQLxCrud.TABLE,
            collection=MySQLxCrud.Collection(name=target, schema=schema))
        for row in rows:
            typed_row = MySQLxCrud.Insert.TypedRow()
            stmt.rows.extend(row)

    def send_delete(self, schema, target, is_docs, filter):
        stmt = MySQLxCrud.Delete(
            data_model=MySQLxCrud.DOCUMENT if is_docs else MySQLxCrud.TABLE,
            collection=MySQLxCrud.Collection(name=target, schema=schema),
            criteria=filter["expr"] or None)
        self._writer.write_message(MySQLx.ClientMessages.CRUD_DELETE, stmt)

    def send_execute_statement(self, namespace, stmt, args):
        stmt = MySQLxSQL.StmtExecute(namespace=namespace, stmt=stmt,
                                     compact_metadata=False)
        for arg in args:
            v = self._create_any(arg)
            stmt.args.extend([v])
        self._writer.write_message(MySQLx.ClientMessages.SQL_STMT_EXECUTE,
                                   stmt)

    def send_doc_insert(self, schema, target, docs):
        insert = MySQLxCrud.Insert(
            collection=MySQLxCrud.Collection(schema=schema, name=target),
            data_model=MySQLxCrud.DOCUMENT)
        for doc in docs:
            row = MySQLxCrud.Insert.TypedRow()
            o = self.arg_object_to_expr(doc, False)
            row.field.extend([o])
            insert.row.extend([row])

        self._writer.write_message(MySQLx.ClientMessages.CRUD_INSERT, insert)

    def _create_any(self, arg):
        if isinstance(arg, (str, unicode,)):
            val = MySQLxDatatypes.Scalar.String(value=arg)
            scalar = MySQLxDatatypes.Scalar(type=8, v_string=val)
            return MySQLxDatatypes.Any(type=1, scalar=scalar)
        return None

    def close_result(self, rs):
        msg = self._read_message(rs)
        if not msg == None:
            raise Exception("Expected to close the result")

    def read_row(self, rs):
        msg = self._read_message(rs)
        if msg == None:
            return None
        if isinstance(msg, MySQLxResultset.Row):
            return msg
        self._reader.push_message(msg)
        return None

    def _process_frame(self, msg, rs):
        if msg.type == 1:
            warningMsg = MySQLxNotice.Warning()
            warningMsg.ParseFromString(msg.payload)
            rs._warnings.append(Warning(warningMsg.level, warningMsg.code, warningMsg.msg))
        elif msg.type == 2:
            sessVarMsg = MySQLxNotice.SessionVariableChanged()
            sessVarMsg.ParseFromString(msg.payload)
        elif msg.type == 3:
            sessStateMsg = MySQLxNotice.SessionStateChanged()
            sessStateMsg.ParseFromString(msg.payload)
            if sessStateMsg.param == MySQLxNotice.SessionStateChanged.ROWS_AFFECTED:
                rs._rows_affected = sessStateMsg.value.v_unsigned_int

    def _read_message(self, rs):
        while True:
            msg = self._reader.read_message()
            if isinstance(msg, MySQLx.Error):
                raise Exception(msg.msg)
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
        while (True):
            msg = self._read_message(rs)
            if msg == None:
                break
            if isinstance(msg, MySQLxResultset.Row):
                self._reader.push_message(msg)
                break;
            if not isinstance(msg, MySQLxResultset.ColumnMetaData):
                raise Exception("Unexpected msg type")
            col = ColumnMetaData(msg.type, msg.catalog, msg.schema, msg.table,
                         msg.original_table, msg.name, msg.original_name,
                         msg.length, msg.collation, msg.fractional_digits,
                         msg.flags)
            columns.append(col)
        return columns

    def arg_object_to_expr(self, value, allow_relational):
        if value == None:
            return Expr.build_null_scalar()
        value_type = type(value)
        if value_type == type(True):
            return MySQLxExpr.Expr(type=MySQLxExpr.Expr.LITERAL, literal=ExprParser().build_bool_scalar(value))
        elif isinstance(value, (int, long)):
            return MySQLxExpr.Expr(type=MySQLxExpr.Expr.LITERAL, literal=ExprParser().build_int_scalar(value))
        elif isinstance(value, (float)):
            return MySQLxExpr.Expr(type=MySQLxExpr.Expr.LITERAL, literal=ExprParser().build_double_scalar(value))
        elif value_type == type(str):
            try:
                expression = ExprParser(value, allow_relational).expr()
                if expression.has_identifier():
                    return MySQLxExpr.Expr(type=MySQLxExpr.Expr.LITERAL, literal=ExprParser().build_string_scalar(value))
                return expression
            except Exception as e:
                return MySQLxExpr.Expr(type=MySQLxExpr.Expr.LITERAL, literal=ExprParser().build_string_scalar(value))
        elif isinstance(value, DbDoc):
            return MySQLxExpr.Expr(type=MySQLxExpr.Expr.LITERAL, literal=build_string_scalar(str(value)))
        raise Exception("Unsupported type")