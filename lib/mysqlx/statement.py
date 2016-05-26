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

import json
import uuid

from .result import SqlResult
from .expr import ExprParser


class DbDoc(object):
    def __init__(self, value):
        # TODO: handle exceptions.  What happens if it doesn't load properly
        if isinstance(value, dict):
            self.__dict__ = value
        elif isinstance(value, basestring):
            self.__dict__ = json.loads(value)
        else:
            raise Exception("Unable to handle type: ".format(type(value)))

    def ensure_id(self):
        if "_id" not in self.__dict__:
            self.__dict__["_id"] = str(uuid.uuid4())

    def __str__(self):
        return json.dumps(self.__dict__)
        #return str(self.__dict__)


class Statement(object):
    def __init__(self, target, doc_based=True):
        self._target = target
        self._doc_based = doc_based
        self._connection = target._connection if target else None

    @property
    def target(self):
        return self._target

    @property
    def schema(self):
        return self._target.schema

    def execute(self):
        raise NotImplementedError


class FilterableStatement(Statement):
    def __init__(self, target, doc_based=True, condition=None):
        super(FilterableStatement, self).__init__(target=target,
                                                  doc_based=doc_based)
        self._filter = {}
        if condition is not None:
            self.where(condition)

    @property
    def filter(self):
        return self._filter

    def where(self, condition):
        self._filter["where"] = condition
        self._filter["expr"] = ExprParser(condition,
                                          not self._doc_based).expr()

    def limit(self, offset, limit):
        self._filter["has_limit"] = True
        self._filter["offset"] = offset
        self._filter["limit"] = limit

    def execute(self):
        pass


class SqlStatement(Statement):
    def __init__(self, connection, sql):
        super(SqlStatement, self).__init__(target=None, doc_based=False)
        self._connection = connection
        self._sql = sql

    def execute(self):
        self._connection.send_sql(self._sql)
        return SqlResult(self._connection)


class AddStatement(Statement):
    def __init__(self, collection):
        super(AddStatement, self).__init__(target=collection)
        self._docs = []

    def add(self, *values):
        for val in values:
            if isinstance(val, DbDoc):
                self._docs.append(val)
            else:
                self._docs.append(DbDoc(val))
        return self

    def execute(self):
        for doc in self._docs:
            doc.ensure_id()
        return self._connection.send_doc_insert(self)


class RemoveStatement(FilterableStatement):
    def __init__(self, collection):
        super(RemoveStatement, self).__init__(target=collection)

    def execute(self):
        return self._connection.send_delete(self, True)


class TableDeleteStatement(FilterableStatement):
    def __init__(self, table, condition=None):
        super(TableDeleteStatement, self).__init__(target=table,
                                                   condition=condition,
                                                   doc_based=False)

    def execute(self):
        return self._connection.send_delete(self, False)
