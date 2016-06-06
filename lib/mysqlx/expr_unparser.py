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

from .protobuf.mysqlx_datatypes_pb2 import *
from .protobuf.mysqlx_expr_pb2 import *


def escape_literal(string):
    return string.replace('"', '""')


def scalar_to_string(s):
    if s.type == Scalar.V_SINT:
        return str(s.v_signed_int)
    elif s.type == Scalar.V_DOUBLE:
        return str(s.v_double)
    elif s.type == Scalar.V_BOOL:
        if s.v_bool:
            return "TRUE"
        else:
            return "FALSE"
    elif s.type == Scalar.V_STRING:
        return '"{0}"'.format(escape_literal(s.v_string.value))
    elif s.type == Scalar.V_NULL:
        return "NULL"
    else:
        raise ValueError("Unknown type tag: {0}".format(s.type))


def column_identifier_to_string(id):
    s = quote_identifier(id.name)
    if id.HasField("table_name"):
        s = "{0}.{1}".format(quote_identifier(id.table_name), s)
    if id.HasField("schema_name"):
        s = "{0}.{1}".format(quote_identifier(id.schema_name), s)
    # if id.HasField("document_path"):
    #     s = "{0}@{1}".format(s, id.document_path)
    return s


def function_call_to_string(fc):
    s = quote_identifier(fc.name.name) + "("
    if fc.name.HasField("schema_name"):
        s = quote_identifier(fc.name.schema_name) + "." + s
    for i in xrange(0, len(fc.param)):
        s = s + expr_to_string(fc.param[i])
        if i + 1 < len(fc.param):
            s = s + ", "
    return s + ")"


def operator_to_string(op):
    ps = op.param
    if op.name == "IN":
        s = expr_to_string(ps[0]) + " IN ("
        for i in xrange(1, len(ps)):
            s = s + expr_to_string(ps[i])
            if i + 1 < len(ps):
                s = s + ", "
        return s + ")"
    elif op.name == "INTERVAL":
        return ("INTERVAL {0} {1}"
                "".format(expr_to_string(ps[0]),
                          expr_to_string(ps[1]).replace('"', '')))
    elif op.name == "BETWEEN":
        return "{0} BETWEEN {1} AND {2}".format(expr_to_string(ps[0]),
                                                expr_to_string(ps[1]),
                                                expr_to_string(ps[2]))
    elif op.name == "LIKE" and len(ps) == 3:
        return "{0} LIKE {1} ESCAPE {2}".format(expr_to_string(ps[0]),
                                                expr_to_string(ps[1]),
                                                expr_to_string(ps[2]))
    elif len(ps) == 2:
        return "{0} {1} {2}".format(expr_to_string(ps[0]), op.name,
                                    expr_to_string(ps[1]))
    elif len(ps) == 1:
        if len(op.name) == 1:
            return "{0}{1}".format(op.name, expr_to_string(ps[0]))
        else:
            # something like NOT
            return "{0} ({1})".format(op.name, expr_to_string(ps[0]))
    else:
        raise ValueError("Unknown operator structure: {0}".format(op))


def quote_identifier(id):
    if "`" in id or '"' in id or "'" in id or "@" in id or "." in id:
        return "`{0}`".format(id.replace("`", "``"))
    else:
        return id


def expr_to_string(e):
    if e.type == Expr.LITERAL:
        return scalar_to_string(e.literal)
    elif e.type == Expr.IDENT:
        return column_identifier_to_string(e.identifier)
    elif e.type == Expr.FUNC_CALL:
        return function_call_to_string(e.function_call)
    elif e.type == Expr.OPERATOR:
        return operator_to_string(e.operator)
    elif e.type == Expr.VARIABLE:
        return "@{0}".format(quote_identifier(e.variable))
    else:
        raise ValueError("Unknown expression type: {0}".format(e.type))
