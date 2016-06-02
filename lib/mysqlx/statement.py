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



from .result import SqlResult
from .expr import ExprParser
from .dbdoc import DbDoc

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
        self._has_projection = False
        self._has_where = False
        self._has_limit = False
        self._has_sort = False
        self._has_group_by = False
        self._has_having = False
        if condition is not None:
            self.where(condition)

    def where(self, condition):
        self._has_where = True
        self._where = condition
        self._where_expr = ExprParser(condition, not self._doc_based).expr()
        return self

    def _projection(self, *fields):
        self._has_projection = True
        self._projection_expr = ExprParser(",".join(fields), not self._doc_based).parse_table_select_projection()
        return self

    def limit(self, row_count, offset=0):
        self._has_limit = True
        self._limit_offset = offset
        self._limit_row_count = row_count
        return self

    def sort(self, *sort_clauses):
        self._has_sort = True
        self._sort_expr = ExprParser(",".join(sort_clauses), not self._doc_based).parse_order_spec()
        return self

    def _group_by(self, *fields):
        self._has_group_by = True
        self._grouping = ExprParser(",".join(fields), not self._doc_based).parse_expr_list()

    def _having(self, condition):
        self._has_having = True
        self._having = ExprParser(condition, not self._doc_based).expr()

    def execute(self):
        raise Exception("This should never be called")


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

class FindStatement(FilterableStatement):
    def __init__(self, collection, condition=None):
        super(FindStatement, self).__init__(collection, True, condition)

    def fields(self, *fields):
        return self._projection(*fields)

    def group_by(self, *fields):
        self._group_by(*fields)
        return self

    def having(self, condition):
        self._having(condition)
        return self

    def execute(self):
        return self._connection.find(self)


class SelectStatement(FilterableStatement):
    def __init__(self, table, *fields):
        super(SelectStatement, self).__init__(table, False)
        self._projection(*fields)

    def group_by(self, *fields):
        self._group_by(*fields)
        return self

    def having(self, condition):
        self._having(condition)
        return self

    def execute(self):
        return self._connection.find(self)


class RemoveStatement(FilterableStatement):
    def __init__(self, collection):
        super(RemoveStatement, self).__init__(target=collection)

    def execute(self):
        return self._connection.delete(self)


class TableDeleteStatement(FilterableStatement):
    def __init__(self, table, condition=None):
        super(TableDeleteStatement, self).__init__(target=table,
                                                   condition=condition,
                                                   doc_based=False)

    def execute(self):
        return self._connection.delete(self)


class CreateCollectionIndexStatement(Statement):
    """A statement that creates an index on a collection.

    Args:
        collection (mysqlx.Collection): Collection.
        index_name (string): Index name.
        is_unique (bool): True if the index is unique.
    """
    def __init__(self, collection, index_name, is_unique):
        super(CreateCollectionIndexStatement, self).__init__(target=collection)
        self._index_name = index_name
        self._is_unique = is_unique
        self._fields = []

    def field(self, document_path, column_type, is_required):
        """Add the field specification to this index creation statement.

        Args:
            document_path (string): Document path.
            column_type (string): Column type.
            is_required (bool): True if the field is required.
        """
        self._fields.append((document_path, column_type, is_required,))
        return self

    def execute(self):
        """Execute the Statement.

        Returns:
            mysqlx.Result: Result object
        """
        fields = [item for sublist in self._fields for item in sublist]
        return self._connection.execute_nonquery(
            "xplugin", "create_collection_index", True,
            self._target.schema.name, self._target.name, self._index_name,
            self._is_unique, *fields)


class DropCollectionIndexStatement(Statement):
    """A statement that drops an index on a collection.

    Args:
        collection (mysqlx.Collection): Collection.
        index_name (string): Index name.
    """
    def __init__(self, collection, index_name):
        super(DropCollectionIndexStatement, self).__init__(target=collection)
        self._index_name = index_name

    def execute(self):
        """Execute the Statement.

        Returns:
            mysqlx.Result
        """
        return self._connection.execute_nonquery(
            "xplugin", "drop_collection_index", True,
            self._target.schema.name, self._target.name, self._index_name)
