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

"""Implementation of the X protocol for MySQL servers."""

import struct

from .compat import STRING_TYPES, INT_TYPES
from .dbdoc import DbDoc
from .errors import InterfaceError, OperationalError, ProgrammingError
from .expr import (ExprParser, build_null_scalar, build_string_scalar,
                   build_bool_scalar, build_double_scalar, build_int_scalar)
from .result import ColumnMetaData
from .protobuf import SERVER_MESSAGES, Message, mysqlxpb_enum


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
        if msg_type == 10:
            raise ProgrammingError("The connected server does not have the "
                                   "MySQL X protocol plugin enabled")
        payload = self._stream.read(msg_len - 1)
        msg_type_name = SERVER_MESSAGES.get(msg_type)
        if not msg_type_name:
            raise ValueError("Unknown msg_type: {0}".format(msg_type))
        msg = Message.from_server_message(msg_type, payload)
        return msg

    def write_message(self, msg_id, msg):
        msg_str = encode_to_bytes(msg.serialize_to_string())
        header = struct.pack("<LB", len(msg_str) + 1, msg_id)
        self._stream.sendall(b"".join([header, msg_str]))


class Protocol(object):
    def __init__(self, reader_writer):
        self._reader = reader_writer
        self._writer = reader_writer
        self._message = None

    def get_capabilites(self):
        msg = Message("Mysqlx.Connection.CapabilitiesGet")
        self._writer.write_message(
            mysqlxpb_enum("Mysqlx.ClientMessages.Type.CON_CAPABILITIES_GET"),
            msg)
        return self._reader.read_message()

    def set_capabilities(self, **kwargs):
        capabilities = Message("Mysqlx.Connection.Capabilities")
        for key, value in kwargs.items():
            capability = Message("Mysqlx.Connection.Capability")
            capability["name"] = key
            capability["value"] = self._create_any(value)
            capabilities["capabilities"].append(capability.get_message())
        msg = Message("Mysqlx.Connection.CapabilitiesSet")
        msg["capabilities"] = capabilities
        self._writer.write_message(
            mysqlxpb_enum("Mysqlx.ClientMessages.Type.CON_CAPABILITIES_SET"),
            msg)
        return self.read_ok()

    def send_auth_start(self, method):
        msg = Message("Mysqlx.Session.AuthenticateStart")
        msg["mech_name"] = method
        self._writer.write_message(mysqlxpb_enum(
            "Mysqlx.ClientMessages.Type.SESS_AUTHENTICATE_START"), msg)

    def read_auth_continue(self):
        msg = self._reader.read_message()
        if msg.type != "Mysqlx.Session.AuthenticateContinue":
            raise InterfaceError("Unexpected message encountered during "
                                 "authentication handshake")
        return msg["auth_data"]

    def send_auth_continue(self, data):
        msg = Message("Mysqlx.Session.AuthenticateContinue",
                      auth_data=data)
        self._writer.write_message(mysqlxpb_enum(
            "Mysqlx.ClientMessages.Type.SESS_AUTHENTICATE_CONTINUE"), msg)

    def read_auth_ok(self):
        while True:
            msg = self._reader.read_message()
            if msg.type == "Mysqlx.Session.AuthenticateOk":
                break
            if msg.type == "Mysqlx.Error":
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
            message["criteria"] = statement._where_expr
        if statement._has_bindings:
            message["args"].extend(self.get_binding_scalars(statement))
        if statement._has_limit:
            message["limit"] = Message("Mysqlx.Crud.Limit",
                                       row_count=statement._limit_row_count,
                                       offset=statement._limit_offset)
        if statement._has_sort:
            message["order"].extend(statement._sort_expr)
        if statement._has_group_by:
            message["grouping"].extend(statement._grouping)
        if statement._has_having:
            message["grouping_criteria"] = statement._having

    def send_find(self, stmt):
        data_model = mysqlxpb_enum("Mysqlx.Crud.DataModel.DOCUMENT"
                                   if stmt._doc_based else
                                   "Mysqlx.Crud.DataModel.TABLE")
        collection = Message("Mysqlx.Crud.Collection",
                             name=stmt.target.name,
                             schema=stmt.schema.name)
        msg = Message("Mysqlx.Crud.Find", data_model=data_model,
                      collection=collection)
        if stmt._has_projection:
            msg["projection"] = stmt._projection_expr
        self._apply_filter(msg, stmt)
        self._writer.write_message(
            mysqlxpb_enum("Mysqlx.ClientMessages.Type.CRUD_FIND"), msg)

    def send_update(self, stmt):
        data_model = mysqlxpb_enum("Mysqlx.Crud.DataModel.DOCUMENT"
                                   if stmt._doc_based else
                                   "Mysqlx.Crud.DataModel.TABLE")
        collection = Message("Mysqlx.Crud.Collection",
                             name=stmt.target.name,
                             schema=stmt.schema.name)
        msg = Message("Mysqlx.Crud.Update", data_model=data_model,
                      collection=collection)
        self._apply_filter(msg, stmt)
        for update_op in stmt._update_ops:
            operation = Message("Mysqlx.Crud.UpdateOperation")
            operation["operation"] = update_op.update_type
            operation["source"] = update_op.source
            if update_op.value is not None:
                operation["value"] = self.arg_object_to_expr(
                    update_op.value, not stmt._doc_based)
            msg["operation"].extend([operation.get_message()])

        self._writer.write_message(
            mysqlxpb_enum("Mysqlx.ClientMessages.Type.CRUD_UPDATE"), msg)

    def send_delete(self, stmt):
        data_model = mysqlxpb_enum("Mysqlx.Crud.DataModel.DOCUMENT"
                                   if stmt._doc_based else
                                   "Mysqlx.Crud.DataModel.TABLE")
        collection = Message("Mysqlx.Crud.Collection", name=stmt.target.name,
                             schema=stmt.schema.name)
        msg = Message("Mysqlx.Crud.Delete", data_model=data_model,
                      collection=collection)
        self._writer.write_message(
            mysqlxpb_enum("Mysqlx.ClientMessages.Type.CRUD_DELETE"), msg)

    def send_execute_statement(self, namespace, stmt, args):
        msg = Message("Mysqlx.Sql.StmtExecute", namespace=namespace, stmt=stmt,
                      compact_metadata=False)
        for arg in args:
            value = self._create_any(arg)
            msg["args"].extend([value.get_message()])
        self._writer.write_message(
            mysqlxpb_enum("Mysqlx.ClientMessages.Type.SQL_STMT_EXECUTE"), msg)

    def send_insert(self, stmt):
        data_model = mysqlxpb_enum("Mysqlx.Crud.DataModel.DOCUMENT"
                                   if stmt._doc_based else
                                   "Mysqlx.Crud.DataModel.TABLE")
        collection = Message("Mysqlx.Crud.Collection",
                             name=stmt.target.name,
                             schema=stmt.schema.name)
        msg = Message("Mysqlx.Crud.Insert", data_model=data_model,
                      collection=collection)

        if hasattr(stmt, "_fields"):
            for field in stmt._fields:
                expr = ExprParser(field, not stmt._doc_based) \
                    .parse_table_insert_field()
                msg["projection"].extend([expr.get_message()])

        for value in stmt._values:
            row = Message("Mysqlx.Crud.Insert.TypedRow")
            if isinstance(value, list):
                for val in value:
                    obj = self.arg_object_to_expr(val, not stmt._doc_based)
                    row["field"].extend([obj.get_message()])
            else:
                obj = self.arg_object_to_expr(value, not stmt._doc_based)
                row["field"].extend([obj.get_message()])
            msg["row"].extend([row.get_message()])

        self._writer.write_message(
            mysqlxpb_enum("Mysqlx.ClientMessages.Type.CRUD_INSERT"), msg)

    def _create_any(self, arg):
        if isinstance(arg, STRING_TYPES):
            value = Message("Mysqlx.Datatypes.Scalar.String", value=arg)
            scalar = Message("Mysqlx.Datatypes.Scalar", type=8, v_string=value)
            return Message("Mysqlx.Datatypes.Any", type=1, scalar=scalar)
        elif isinstance(arg, bool):
            return Message("Mysqlx.Datatypes.Any", type=1,
                           scalar=build_bool_scalar(arg))
        elif isinstance(arg, INT_TYPES):
            return Message("Mysqlx.Datatypes.Any", type=1,
                           scalar=build_int_scalar(arg))
        return None

    def close_result(self, rs):
        msg = self._read_message(rs)
        if msg is not None:
            raise OperationalError("Expected to close the result")

    def read_row(self, rs):
        msg = self._read_message(rs)
        if msg is None:
            return None
        if msg.type == "Mysqlx.Resultset.Row":
            return msg
        self._reader.push_message(msg)
        return None

    def _process_frame(self, msg, rs):
        if msg["type"] == 1:
            warn_msg = Message.from_message("Mysqlx.Notice.Warning",
                                            msg["payload"])
            rs._warnings.append(Warning(warn_msg.level, warn_msg.code,
                                        warn_msg.msg))
        elif msg["type"] == 2:
            Message.from_message("Mysqlx.Notice.SessionVariableChanged",
                                 msg["payload"])
        elif msg["type"] == 3:
            sess_state_msg = Message.from_message(
                "Mysqlx.Notice.SessionStateChanged", msg["payload"])
            if sess_state_msg["param"] == mysqlxpb_enum(
                    "Mysqlx.Notice.SessionStateChanged.Parameter."
                    "ROWS_AFFECTED"):
                rs._rows_affected = sess_state_msg["value"]["v_unsigned_int"]
            elif sess_state_msg["param"] == mysqlxpb_enum(
                    "Mysqlx.Notice.SessionStateChanged.Parameter."
                    "GENERATED_INSERT_ID"):
                rs._generated_id = sess_state_msg["value"]["v_unsigned_int"]

    def _read_message(self, rs):
        while True:
            msg = self._reader.read_message()
            if msg.type == "Mysqlx.Error":
                raise OperationalError(msg["msg"])
            elif msg.type == "Mysqlx.Notice.Frame":
                self._process_frame(msg, rs)
            elif msg.type == "Mysqlx.Sql.StmtExecuteOk":
                return None
            elif msg.type == "Mysqlx.Resultset.FetchDone":
                rs._closed = True
            elif msg.type == "Mysqlx.Resultset.FetchDoneMoreResultsets":
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
            if msg.type == "Mysqlx.Resultset.Row":
                self._reader.push_message(msg)
                break
            if msg.type != "Mysqlx.Resultset.ColumnMetaData":
                raise InterfaceError("Unexpected msg type")
            col = ColumnMetaData(msg["type"], msg["catalog"], msg["schema"],
                                 msg["table"], msg["original_table"],
                                 msg["name"], msg["original_name"],
                                 msg["length"], msg["collation"],
                                 msg["fractional_digits"], msg["flags"],
                                 msg.get("content_type"))
            columns.append(col)
        return columns

    def arg_object_to_expr(self, value, allow_relational):
        literal = None
        if value is None:
            literal = build_null_scalar()
        if isinstance(value, bool):
            literal = build_bool_scalar(value)
        elif isinstance(value, INT_TYPES):
            literal = build_int_scalar(value)
        elif isinstance(value, (float)):
            literal = build_double_scalar(value)
        elif isinstance(value, STRING_TYPES):
            try:
                expression = ExprParser(value, allow_relational).expr()
                if expression.has_identifier():
                    msg = Message("Mysqlx.Expr.Expr",
                                  literal=build_string_scalar(value))
                    return msg
                return expression.serialize_to_string()
            except:
                literal = build_string_scalar(value)
        elif isinstance(value, DbDoc):
            literal = build_string_scalar(str(value))
        if literal is None:
            raise InterfaceError("Unsupported type: {0}".format(type(value)))

        msg = Message("Mysqlx.Expr.Expr")
        msg["type"] = mysqlxpb_enum("Mysqlx.Expr.Expr.Type.LITERAL")
        msg["literal"] = literal
        return msg

    def arg_object_to_scalar(self, value, allow_relational):
        return self.arg_object_to_expr(value, allow_relational).literal

    def read_ok(self):
        msg = self._reader.read_message()
        if msg.type == "Mysqlx.Error":
            raise InterfaceError(msg["msg"])
        if msg.type != "Mysqlx.Ok":
            raise InterfaceError("Unexpected message encountered")

    def send_close(self):
        msg = Message("Mysqlx.Session.Close")
        self._writer.write_message(mysqlxpb_enum(
            "Mysqlx.ClientMessages.Type.SESS_CLOSE"), msg)
