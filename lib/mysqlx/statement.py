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


from .protobuf import mysqlx_crud_pb2 as MySQLxCrud
from .result import SqlResult
from .expr import ExprParser
from .dbdoc import DbDoc

class Statement(object):
    """Provides base functionality for statement objects.

    Args:
        target (object): The target database object, it can be
                         :class:`mysqlx.Collection` or :class:`mysqlx.Table`.
        doc_based (bool): `True` if it is document based.
    """
    def __init__(self, target, doc_based=True):
        self._target = target
        self._doc_based = doc_based
        self._connection = target._connection if target else None

    @property
    def target(self):
        """object: The database object target.
        """
        return self._target

    @property
    def schema(self):
        """:class:`mysqlx.Schema`: The Schema object.
        """
        return self._target.schema

    def execute(self):
        """Execute the statement.

        Raises:
           NotImplementedError: This method must be implemented.
        """
        raise NotImplementedError


class FilterableStatement(Statement):
    """A statement to be used with filterable statements.

    Args:
        target (object): The target database object, it can be
                         :class:`mysqlx.Collection` or :class:`mysqlx.Table`.
        doc_based (Optional[bool]): `True` if it is document based
                                    (default: `True`).
        condition (Optional[str]): Sets the search condition to filter
                                   documents or records.
    """
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
        """Sets the search condition to filter.

        Args:
            condition (str): Sets the search condition to filter documents or
                             records.
        """
        self._has_where = True
        self._where = condition
        self._where_expr = ExprParser(condition, not self._doc_based).expr()
        return self

    def _projection(self, *fields):
        self._has_projection = True
        self._projection_expr = ExprParser(",".join(fields), not self._doc_based).parse_table_select_projection()
        return self

    def limit(self, row_count, offset=0):
        """Sets the maximum number of records or documents to be returned.

        Args:
            row_count (int): The maximum number of records or documents.
            offset (Optional[int]) The number of records or documents to skip.
        """
        self._has_limit = True
        self._limit_offset = offset
        self._limit_row_count = row_count
        return self

    def sort(self, *sort_clauses):
        """Sets the sorting criteria.

        Args:
            *sort_clauses: The expression strings defining the sort criteria.
        """
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
        """Execute the statement.

        Raises:
           NotImplementedError: This method must be implemented.
        """
        raise Exception("This should never be called")


class SqlStatement(Statement):
    """A statement for SQL execution.

    Args:
        connection (mysqlx.connection.Connection): Connection object.
        sql (string): The sql statement to be executed.
    """
    def __init__(self, connection, sql):
        super(SqlStatement, self).__init__(target=None, doc_based=False)
        self._connection = connection
        self._sql = sql

    def execute(self):
        """Execute the statement.

        Returns:
            mysqlx.SqlResult: SqlResult object.
        """
        self._connection.send_sql(self._sql)
        return SqlResult(self._connection)


class AddStatement(Statement):
    """A statement for document addition on a collection.

    Args:
        collection (mysqlx.Collection): The Collection object.
    """
    def __init__(self, collection):
        super(AddStatement, self).__init__(target=collection)
        self._values = []

    """Adds a list of documents into a collection.

    Args:
        *values: The documents to be added into the collection.

    Returns:
        mysqlx.AddStatement: AddStatement object.
    """
    def add(self, *values):
        for val in values:
            if isinstance(val, DbDoc):
                self._values.append(val)
            else:
                self._values.append(DbDoc(val))
        return self

    def execute(self):
        """Execute the statement.

        Returns:
            mysqlx.Result: Result object.
        """
        for doc in self._values:
            doc.ensure_id()
        return self._connection.send_insert(self)

class UpdateSpec(object):
    def __init__(self, type, source, value=None):
        if type == MySQLxCrud.UpdateOperation.SET:
            self._table_set(source, value)
        else:
            self.update_type = type
            self.source = source
            if len(source) > 0 and source[0] == '$':
                self.source = source[1:]
            self.source = ExprParser(self.source, False).document_field().identifier
            self.value = value

    def _table_set(self, source, value):
        self.update_type = MySQLxCrud.UpdateOperation.SET
        self.source = ExprParser(source, True).parse_table_update_field()
        self.value = value

class ModifyStatement(FilterableStatement):
    """A statement for document update operations on a Collection.

    Args:
        collection (mysqlx.Collection): The Collection object.
        condition (Optional[str]): Sets the search condition to identify the
                                   documents to be updated.
    """
    def __init__(self, collection, condition=None):
        super(ModifyStatement, self).__init__(target=collection, condition=condition)
        self._update_ops = []

    def set(self, doc_path, value):
        """Sets or updates attributes on documents in a collection.

        Args:
            doc_path (string): The document path of the item to be set.
            value (string): The value to be set on the specified attribute.

        Returns:
            mysqlx.ModifyStatement: ModifyStatement object.
        """
        self._update_ops.append(UpdateSpec(MySQLxCrud.UpdateOperation.ITEM_SET, doc_path, value))
        return self

    def change(self, doc_path, value):
        """Add an update to the statement setting the field, if it exists at
        the document path, to the given value.

        Args:
            doc_path (string): The document path of the item to be set.
            value (object): The value to be set on the specified attribute.

        Returns:
            mysqlx.ModifyStatement: ModifyStatement object.
        """
        self._update_ops.append(UpdateSpec(MySQLxCrud.UpdateOperation.ITEM_REPLACE, doc_path, value))
        return self

    def unset(self, doc_path):
        """Removes attributes from documents in a collection.

        Args:
            doc_path (string): The document path of the attribute to be
                               removed.

        Returns:
            mysqlx.ModifyStatement: ModifyStatement object.
        """
        self._update_ops.append(UpdateSpec(MySQLxCrud.UpdateOperation.ITEM_REMOVE, doc_path))
        return self

    def array_insert(self, field, value):
        """Insert a value into the specified array in documents of a
        collection.

        Args:
            field (string): A document path that identifies the array attribute
                            and position where the value will be inserted.
            value (object): The value to be inserted.

        Returns:
            mysqlx.ModifyStatement: ModifyStatement object.
        """
        self._update_ops.append(UpdateSpec(MySQLxCrud.UpdateOperation.ARRAY_INSERT, field, value))
        return self

    def array_append(self, doc_path, value):
        """Inserts a value into a specific position in an array attribute in
        documents of a collection.

        Args:
            doc_path (string): A document path that identifies the array
                               attribute and position where the value will be
                               inserted.
            value (object): The value to be inserted.

        Returns:
            mysqlx.ModifyStatement: ModifyStatement object.
        """
        self._update_ops.append(UpdateSpec(MySQLxCrud.UpdateOperation.UpdateType.ARRAY_APPEND, doc_path, value))
        return self

    def execute(self):
        """Execute the statement.

        Returns:
            mysqlx.Result: Result object.
        """
        return self._connection.update(self)


class FindStatement(FilterableStatement):
    """A statement document selection on a Collection.

    Args:
        collection (mysqlx.Collection): The Collection object.
        condition (Optional[str]): An optional expression to identify the
                                   documents to be retrieved. If not specified
                                   all the documents will be included on the
                                   result unless a limit is set.
    """
    def __init__(self, collection, condition=None):
        super(FindStatement, self).__init__(collection, True, condition)

    def fields(self, *fields):
        """Sets a document field filter.

        Args:
            *fields: The string expressions identifying the fields to be
                     extracted.
        """
        return self._projection(*fields)

    def group_by(self, *fields):
        """Sets a grouping criteria for the resultset.

        Args:
            *fields: The string expressions identifying the grouping criteria.

        Returns:
            mysqlx.FindStatement: FindStatement object.
        """
        self._group_by(*fields)
        return self

    def having(self, condition):
        """Sets a condition for records to be considered in agregate function
        operations.

        Args:
            condition (string): A condition on the agregate functions used on
                                the grouping criteria.

        Returns:
            mysqlx.FindStatement: FindStatement object.
        """
        self._having(condition)
        return self

    def execute(self):
        """Execute the statement.

        Returns:
            mysqlx.DocResult: DocResult object.
        """
        return self._connection.find(self)


class SelectStatement(FilterableStatement):
    """A statement for record retrieval operations on a Table.

    Args:
        table (mysqlx.Table): The Table object.
        *fields: The fields to be retrieved.
    """
    def __init__(self, table, *fields):
        super(SelectStatement, self).__init__(table, False)
        self._projection(*fields)

    def group_by(self, *fields):
        """Sets a grouping criteria for the resultset.

        Args:
            *fields: The fields identifying the grouping criteria.

        Returns:
            mysqlx.SelectStatement: SelectStatement object.
        """
        self._group_by(*fields)
        return self

    def having(self, condition):
        """Sets a condition for records to be considered in agregate function
        operations.

        Args:
            condition (str): A condition on the agregate functions used on the
                             grouping criteria.

        Returns:
            mysqlx.SelectStatement: SelectStatement object.
        """
        self._having(condition)
        return self

    def execute(self):
        """Execute the statement.

        Returns:
            mysqlx.RowResult: RowResult object.
        """
        return self._connection.find(self)


class InsertStatement(Statement):
    """A statement for insert operations on Table.

    Args:
        table (mysqlx.Table): The Table object.
        *fields: The fields to be inserted.
    """
    def __init__(self, table, *fields):
        super(InsertStatement, self).__init__(target=table, doc_based=False)
        self._fields = fields
        self._values = []

    def values(self, *values):
        """Set the values to be inserted.

        Args:
            *values: The values of the columns to be inserted.

        Returns:
            mysqlx.InsertStatement: InsertStatement object.
        """
        self._values.append(list(values))
        return self

    def execute(self):
        """Execute the statement.

        Returns:
            mysqlx.Result: Result object.
        """
        return self._connection.send_insert(self)

class UpdateStatement(FilterableStatement):
    """A statement for record update operations on a Table.

    Args:
        table (mysqlx.Table): The Table object.
        *fields: The fields to be updated.
    """
    def __init__(self, table, *fields):
        super(UpdateStatement, self).__init__(target=table, doc_based=False)
        self._update_ops = []

    def set(self, field, value):
        """Updates the column value on records in a table.

        Args:
            field (string): The column name to be updated.
            value (object): The value to be set on the specified column.
        """
        self._update_ops.append(UpdateSpec(MySQLxCrud.UpdateOperation.SET, field, value))
        return self

    def execute(self):
        """Execute the statement.

        Returns:
            mysqlx.Result: Result object
        """
        return self._connection.update(self)


class RemoveStatement(FilterableStatement):
    """A statement for document removal from a collection.

    Args:
        collection (mysqlx.Collection): The Collection object.
    """
    def __init__(self, collection):
        super(RemoveStatement, self).__init__(target=collection)

    def execute(self):
        """Execute the statement.

        Returns:
            mysqlx.Result: Result object.
        """
        return self._connection.delete(self)


class DeleteStatement(FilterableStatement):
    """A statement that drops a table.

    Args:
        table (mysqlx.Table): The Table object.
        condition (Optional[str]): The string with the filter expression of
                                   the rows to be deleted.
    """
    def __init__(self, table, condition=None):
        super(DeleteStatement, self).__init__(target=table,
                                                   condition=condition,
                                                   doc_based=False)

    def execute(self):
        """Execute the statement.

        Returns:
            mysqlx.Result: Result object.
        """
        return self._connection.delete(self)


class CreateCollectionIndexStatement(Statement):
    """A statement that creates an index on a collection.

    Args:
        collection (mysqlx.Collection): Collection.
        index_name (string): Index name.
        is_unique (bool): `True` if the index is unique.
    """
    def __init__(self, collection, index_name, is_unique):
        super(CreateCollectionIndexStatement, self).__init__(target=collection)
        self._index_name = index_name
        self._is_unique = is_unique
        self._fields = []

    def field(self, document_path, column_type, is_required):
        """Add the field specification to this index creation statement.

        Args:
            document_path (string): The document path.
            column_type (string): The column type.
            is_required (bool): `True` if the field is required.
        """
        self._fields.append((document_path, column_type, is_required,))
        return self

    def execute(self):
        """Execute the statement.

        Returns:
            mysqlx.Result: Result object.
        """
        fields = [item for sublist in self._fields for item in sublist]
        return self._connection.execute_nonquery(
            "xplugin", "create_collection_index", True,
            self._target.schema.name, self._target.name, self._index_name,
            self._is_unique, *fields)


class DropCollectionIndexStatement(Statement):
    """A statement that drops an index on a collection.

    Args:
        collection (mysqlx.Collection): The Collection object.
        index_name (string): The index name.
    """
    def __init__(self, collection, index_name):
        super(DropCollectionIndexStatement, self).__init__(target=collection)
        self._index_name = index_name

    def execute(self):
        """Execute the statement.

        Returns:
            mysqlx.Result: Result object.
        """
        return self._connection.execute_nonquery(
            "xplugin", "drop_collection_index", True,
            self._target.schema.name, self._target.name, self._index_name)
