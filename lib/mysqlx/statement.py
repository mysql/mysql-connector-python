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

"""Implementation of Statements."""

import copy
import json
import re

from .errors import ProgrammingError
from .expr import ExprParser
from .compat import STRING_TYPES
from .constants import Algorithms, Securities
from .dbdoc import DbDoc
from .result import SqlResult, Result, ColumnType
from .protobuf import mysqlxpb_enum


class Expr(object):
    def __init__(self, expr):
        self.expr = expr


def flexible_params(*values):
    if len(values) == 1 and isinstance(values[0], (list, tuple,)):
        return values[0]
    return values


def is_quoted_identifier(identifier, sql_mode=""):
    """Check if the given identifier is quoted.

    Args:
        identifier (string): Identifier to check.
        sql_mode (Optional[string]): SQL mode.

    Returns:
        `True` if the identifier has backtick quotes, and False otherwise.
    """
    if "ANSI_QUOTES" in sql_mode:
        return ((identifier[0] == "`" and identifier[-1] == "`") or
                (identifier[0] == '"' and identifier[-1] == '"'))
    else:
        return identifier[0] == "`" and identifier[-1] == "`"


def quote_identifier(identifier, sql_mode=""):
    """Quote the given identifier with backticks, converting backticks (`) in
    the identifier name with the correct escape sequence (``) unless the
    identifier is quoted (") as in sql_mode set to ANSI_QUOTES.

    Args:
        identifier (string): Identifier to quote.
        sql_mode (Optional[string]): SQL mode.

    Returns:
        A string with the identifier quoted with backticks.
    """
    if is_quoted_identifier(identifier, sql_mode):
        return identifier
    if "ANSI_QUOTES" in sql_mode:
        return '"{0}"'.format(identifier.replace('"', '""'))
    else:
        return "`{0}`".format(identifier.replace("`", "``"))


def quote_multipart_identifier(identifiers, sql_mode=""):
    """Quote the given multi-part identifier with backticks.

    Args:
        identifiers (iterable): List of identifiers to quote.
        sql_mode (Optional[string]): SQL mode.

    Returns:
        A string with the multi-part identifier quoted with backticks.
    """
    return ".".join([quote_identifier(identifier, sql_mode)
                     for identifier in identifiers])


def parse_table_name(default_schema, table_name, sql_mode=""):
    quote = '"' if "ANSI_QUOTES" in sql_mode else "`"
    delimiter = ".{0}".format(quote) if quote in table_name else "."
    temp = table_name.split(delimiter, 1)
    return (default_schema if len(temp) is 1 else temp[0].strip(quote),
            temp[-1].strip(quote),)


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
        self._has_bindings = False
        self._binding_map = {}
        self._bindings = []
        if condition is not None:
            self.where(condition)

    def where(self, condition):
        """Sets the search condition to filter.

        Args:
            condition (str): Sets the search condition to filter documents or
                             records.

        Returns:
            mysqlx.FilterableStatement: FilterableStatement object.
        """
        self._has_where = True
        self._where = condition
        expr = ExprParser(condition, not self._doc_based)
        self._where_expr = expr.expr()
        self._binding_map = expr.placeholder_name_to_position
        return self

    def _projection(self, *fields):
        fields = flexible_params(*fields)
        self._has_projection = True
        self._projection_str = ",".join(fields)
        self._projection_expr = ExprParser(self._projection_str,
            not self._doc_based).parse_table_select_projection()
        return self

    def limit(self, row_count, offset=0):
        """Sets the maximum number of records or documents to be returned.

        Args:
            row_count (int): The maximum number of records or documents.
            offset (Optional[int]) The number of records or documents to skip.

        Returns:
            mysqlx.FilterableStatement: FilterableStatement object.
        """
        self._has_limit = True
        self._limit_offset = offset
        self._limit_row_count = row_count
        return self

    def sort(self, *sort_clauses):
        """Sets the sorting criteria.

        Args:
            *sort_clauses: The expression strings defining the sort criteria.

        Returns:
            mysqlx.FilterableStatement: FilterableStatement object.
        """
        sort_clauses = flexible_params(*sort_clauses)
        self._has_sort = True
        self._sort_str = ",".join(sort_clauses)
        self._sort_expr = ExprParser(self._sort_str,
                                     not self._doc_based).parse_order_spec()
        return self

    def _group_by(self, *fields):
        fields = flexible_params(*fields)
        self._has_group_by = True
        self._grouping_str = ",".join(fields)
        self._grouping = ExprParser(self._grouping_str,
                                    not self._doc_based).parse_expr_list()

    def _having(self, condition):
        self._has_having = True
        self._having = ExprParser(condition, not self._doc_based).expr()

    def bind(self, *args):
        """Binds a value to a specific placeholder.

        Args:
            *args: The name of the placeholder and the value to bind.
                   A :class:`mysqlx.DbDoc` object or a JSON string
                   representation can be used.

        Returns:
            mysqlx.FilterableStatement: FilterableStatement object.

        Raises:
            ProgrammingError: If the number of arguments is invalid.
        """
        self._has_bindings = True
        count = len(args)
        if count == 1:
            self._bind_single(args[0])
        elif count > 2:
            raise ProgrammingError("Invalid number of arguments to bind")
        else:
            self._bindings.append({"name": args[0], "value": args[1]})
        return self

    def _bind_single(self, object):
        if isinstance(object, DbDoc):
            self.bind(str(object))
        elif isinstance(object, STRING_TYPES):
            dict = json.loads(object)
            for key in dict.keys():
                self.bind(key, dict[key])

    def execute(self):
        """Execute the statement.

        Raises:
           NotImplementedError: This method must be implemented.
        """
        raise NotImplementedError


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
        self._ids = []

    def add(self, *values):
        """Adds a list of documents into a collection.

        Args:
            *values: The documents to be added into the collection.

        Returns:
            mysqlx.AddStatement: AddStatement object.
        """
        for val in flexible_params(*values):
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
        if len(self._values) == 0:
            return Result()

        for doc in self._values:
            self._ids.append(doc.ensure_id())

        return self._connection.send_insert(self)


class UpdateSpec(object):
    def __init__(self, update_type, source, value=None):
        if update_type == mysqlxpb_enum(
                "Mysqlx.Crud.UpdateOperation.UpdateType.SET"):
            self._table_set(source, value)
        else:
            self.update_type = update_type
            self.source = source
            if len(source) > 0 and source[0] == '$':
                self.source = source[1:]
            self.source = ExprParser(self.source,
                                     False).document_field().identifier
            self.value = value

    def _table_set(self, source, value):
        self.update_type = mysqlxpb_enum(
            "Mysqlx.Crud.UpdateOperation.UpdateType.SET")
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
        super(ModifyStatement, self).__init__(target=collection,
                                              condition=condition)
        self._update_ops = []

    def set(self, doc_path, value):
        """Sets or updates attributes on documents in a collection.

        Args:
            doc_path (string): The document path of the item to be set.
            value (string): The value to be set on the specified attribute.

        Returns:
            mysqlx.ModifyStatement: ModifyStatement object.
        """
        self._update_ops.append(UpdateSpec(mysqlxpb_enum(
            "Mysqlx.Crud.UpdateOperation.UpdateType.ITEM_SET"),
            doc_path, value))
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
        self._update_ops.append(UpdateSpec(mysqlxpb_enum(
            "Mysqlx.Crud.UpdateOperation.UpdateType.ITEM_REPLACE"),
            doc_path, value))
        return self

    def unset(self, *doc_paths):
        """Removes attributes from documents in a collection.

        Args:
            doc_path (string): The document path of the attribute to be
                               removed.

        Returns:
            mysqlx.ModifyStatement: ModifyStatement object.
        """
        self._update_ops.extend([
            UpdateSpec(mysqlxpb_enum(
                "Mysqlx.Crud.UpdateOperation.UpdateType.ITEM_REMOVE"), item)
            for item in flexible_params(*doc_paths)])
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
        self._update_ops.append(
            UpdateSpec(mysqlxpb_enum(
                "Mysqlx.Crud.UpdateOperation.UpdateType.ARRAY_INSERT"),
                field, value))
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
        self._update_ops.append(
            UpdateSpec(mysqlxpb_enum(
                "Mysqlx.Crud.UpdateOperation.UpdateType.ARRAY_APPEND"),
                doc_path, value))
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

        Returns:
            mysqlx.FindStatement: FindStatement object.
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

    def order_by(self, *clauses):
        """Sets the order by criteria.

        Args:
            *clauses: The expression strings defining the order by criteria.

        Returns:
            mysqlx.SelectStatement: SelectStatement object.
        """
        self.sort(*clauses)
        return self

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

    def get_sql(self):
        where = " WHERE {0}".format(self._where) if self._has_where else ""
        group_by = " GROUP BY {0}".format(self._grouping_str) if \
            self._has_group_by else ""
        having = " HAVING {0}".format(self._having) if self._has_having else ""
        order_by = " ORDER BY {0}".format(self._sort_str) if self._has_sort \
            else ""
        limit = " LIMIT {0} OFFSET {1}".format(self._limit_row_count,
            self._limit_offset) if self._has_limit else ""

        stmt = ("SELECT {select} FROM {schema}.{table}{where}{group}{having}"
                "{order}{limit}".format(select=self._projection_str or "*",
                schema=self.schema.name, table=self.target.name, limit=limit,
                where=where, group=group_by, having=having, order=order_by))

        return stmt

class InsertStatement(Statement):
    """A statement for insert operations on Table.

    Args:
        table (mysqlx.Table): The Table object.
        *fields: The fields to be inserted.
    """
    def __init__(self, table, *fields):
        super(InsertStatement, self).__init__(target=table, doc_based=False)
        self._fields = flexible_params(*fields)
        self._values = []

    def values(self, *values):
        """Set the values to be inserted.

        Args:
            *values: The values of the columns to be inserted.

        Returns:
            mysqlx.InsertStatement: InsertStatement object.
        """
        self._values.append(list(flexible_params(*values)))
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

        Returns:
            mysqlx.UpdateStatement: UpdateStatement object.
        """
        self._update_ops.append(
            UpdateSpec(mysqlxpb_enum(
                "Mysqlx.Crud.UpdateOperation.UpdateType.SET"), field, value))
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

        Returns:
            mysqlx.CreateCollectionIndexStatement: \
                                   CreateCollectionIndexStatement object.
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


class TableIndex(object):
    UNIQUE_INDEX = 1
    INDEX = 2
    def __init__(self, name, index_type, columns):
        self._name = name
        self._index_type = index_type
        self._columns = columns

    def get_sql(self):
        stmt = ""
        if self._index_type is TableIndex.UNIQUE_INDEX:
            stmt += "UNIQUE "
        stmt += "INDEX {0} ({1})"
        return stmt.format(self._name, ",".join(self._columns))


class CreateViewStatement(Statement):
    """A statement for creating views.

    Args:
        view (mysqlx.View): The View object.
        replace (Optional[bool]): `True` to add replace.
    """
    def __init__(self, view, replace=False):
        super(CreateViewStatement, self).__init__(target=view, doc_based=False)
        self._view = view
        self._schema = view.schema
        self._name = view.name
        self._replace = replace
        self._columns = []
        self._algorithm = Algorithms.UNDEFINED
        self._security = Securities.DEFINER
        self._definer = None
        self._defined_as = None
        self._check_option = None

    def columns(self, columns):
        """Sets the column names.

        Args:
            columns (list): The list of column names.

        Returns:
            mysqlx.CreateViewStatement: CreateViewStatement object.
        """
        self._columns = [quote_identifier(col) for col in columns]
        return self

    def algorithm(self, algorithm):
        """Sets the algorithm.

        Args:
            mysqlx.constants.ALGORITHMS: The algorithm.

        Returns:
            mysqlx.CreateViewStatement: CreateViewStatement object.
        """
        self._algorithm = algorithm
        return self

    def security(self, security):
        """Sets the SQL security mode.

        Args:
            mysqlx.constants.SECURITIES: The SQL security mode.

        Returns:
            mysqlx.CreateViewStatement: CreateViewStatement object.
        """
        self._security = security
        return self

    def definer(self, definer):
        """Sets the definer.

        Args:
            definer (string): The definer.

        Returns:
            mysqlx.CreateViewStatement: CreateViewStatement object.
        """
        self._definer = definer
        return self

    def defined_as(self, statement):
        """Sets the SelectStatement statement for describing the view.

        Args:
            mysqlx.SelectStatement: SelectStatement object.

        Returns:
            mysqlx.CreateViewStatement: CreateViewStatement object.
        """
        if not isinstance(statement, SelectStatement) and \
           not isinstance(statement, STRING_TYPES):
            raise ProgrammingError("The statement must be an instance of "
                                   "SelectStatement or a SQL string.")
        self._defined_as = copy.copy(statement)  # Prevent modifications
        return self

    def with_check_option(self, check_option):
        """Sets the check option.

        Args:
            mysqlx.constants.CHECK_OPTIONS: The check option.

        Returns:
            mysqlx.CreateViewStatement: CreateViewStatement object.
        """
        self._check_option = check_option
        return self

    def execute(self):
        """Execute the statement to create a view.

        Returns:
            mysqlx.View: View object.
        """
        replace = " OR REPLACE" if self._replace else ""
        definer = " DEFINER = {0}".format(self._definer) \
                  if self._definer else ""
        columns = " ({0})".format(", ".join(self._columns)) \
                  if self._columns else ""
        defined_as = self._defined_as.get_sql() \
                     if isinstance(self._defined_as, SelectStatement) \
                     else self._defined_as
        view_name = quote_multipart_identifier((self._schema.name, self._name))
        check_option = " WITH {0} CHECK OPTION".format(self._check_option) \
                       if self._check_option else ""
        sql = ("CREATE{replace} ALGORITHM = {algorithm}{definer} "
               "SQL SECURITY {security} VIEW {view_name}{columns} "
               "AS {defined_as}{check_option}"
               "".format(replace=replace, algorithm=self._algorithm,
                         definer=definer, security=self._security,
                         view_name=view_name, columns=columns,
                         defined_as=defined_as, check_option=check_option))

        self._connection.execute_nonquery("sql", sql)
        return self._view


class AlterViewStatement(CreateViewStatement):
    """A statement for alter views.

    Args:
        view (mysqlx.View): The View object.
    """
    def __init__(self, view):
        super(AlterViewStatement, self).__init__(view)

    def execute(self):
        """Execute the statement to alter a view.

        Returns:
            mysqlx.View: View object.
        """
        definer = " DEFINER = {0}".format(self._definer) \
                  if self._definer else ""
        columns = " ({0})".format(", ".join(self._columns)) \
                  if self._columns else ""
        defined_as = self._defined_as.get_sql() \
                     if isinstance(self._defined_as, SelectStatement) \
                     else self._defined_as
        view_name = quote_multipart_identifier((self._schema.name, self._name))
        check_option = " WITH {0} CHECK OPTION".format(self._check_option) \
                       if self._check_option else ""
        sql = ("ALTER ALGORITHM = {algorithm}{definer} "
               "SQL SECURITY {security} VIEW {view_name}{columns} "
               "AS {defined_as}{check_option}"
               "".format(algorithm=self._algorithm, definer=definer,
                         security=self._security, view_name=view_name,
                         columns=columns, defined_as=defined_as,
                         check_option=check_option))

        self._connection.execute_nonquery("sql", sql)
        return self._view

class CreateTableStatement(Statement):
    """A statement that creates a new table if it doesn't exist already.

    Args:
        collection (mysqlx.Schema): The Schema object.
        table_name (string): The name for the new table.
    """
    tbl_frmt = re.compile(r"(from\s+)([`\"].+[`\"]|[^\.]+)(\s|$)", re.IGNORECASE)
    def __init__(self, schema, table_name):
        super(CreateTableStatement, self).__init__(schema)
        self._charset = None
        self._collation = None
        self._comment = None
        self._as = None
        self._like = None
        self._temp = False
        self._columns = []
        self._f_keys = []
        self._indices = []
        self._p_keys = []
        self._u_indices = []
        self._auto_inc = 0
        self._name = table_name

        self._tbl_repl = r"\1{0}.\2\3".format(self.schema.get_name())

    @property
    def table_name(self):
        """string: The fully qualified name of the Table.
        """
        return quote_multipart_identifier(parse_table_name(
            self.schema.name, self._name))

    def _get_table_opts(self):
        options = []
        options.append("AUTO_INCREMENT = {inc}")
        if self._charset:
            options.append("DEFAULT CHARACTER SET = {charset}")
        if self._collation:
            options.append("DEFAULT COLLATE = {collation}")
        if self._comment:
            options.append("COMMENT = '{comment}'")

        table_opts = ",".join(options)
        return table_opts.format(inc=self._auto_inc, charset=self._charset,
            collation=self._collation, comment=self._comment)

    def _get_create_def(self):
        defs = []
        if self._p_keys:
            defs.append("PRIMARY KEY ({0})".format(",".join(self._p_keys)))
        for col in self._columns:
            defs.append(col.get_sql())
        for key in self._f_keys:
            defs.append(key.get_sql())
        for index in self._indices:
            defs.append(index.get_sql())
        for index in self._u_indices:
            defs.append(index.get_sql())

        return ",".join(defs)

    def like(self, table_name):
        """Create table with the definition of another existing Table.

        Args:
            table_name (string): Name of the source table.

        Returns:
            mysqlx.CreateTableStatement: CreateTableStatement object.
        """
        self._like = quote_multipart_identifier(
            parse_table_name(self.schema.name, table_name))
        return self

    def as_select(self, select):
        """Create the Table and fill it with values from a Select Statement.

        Args:
            select (object): Select Statement. Can be a string or an instance
                             of :class:`mysqlx.SelectStatement`.

        Returns:
            mysqlx.CreateTableStatement: CreateTableStatement object.
        """
        if isinstance(select, STRING_TYPES):
            self._as = CreateTableStatement.tbl_frmt.sub(self._tbl_repl, select)
        elif isinstance(select, SelectStatement):
            self._as = select.get_sql()
        return self

    def add_column(self, column_def):
        """Add a Column to the Table.

        Args:
            column_def (MySQLx.ColumnDef): Column Definition object.

        Returns:
            mysqlx.CreateTableStatement: CreateTableStatement object.
        """
        column_def.set_schema(self.schema.get_name())
        self._columns.append(column_def)
        return self

    def add_primary_key(self, *keys):
        """Add multiple Primary Keys to the Table.

        Args:
            *keys: Fields to be used as Primary Keys.

        Returns:
            mysqlx.CreateTableStatement: CreateTableStatement object.
        """
        keys = flexible_params(*keys)
        self._p_keys.extend(keys)
        return self

    def add_index(self, index_name, *cols):
        """Adds an Index to the Table.

        Args:
            index_name (string): Name of the Index.
            *cols: Fields to be used as an Index.

        Returns:
            mysqlx.CreateTableStatement: CreateTableStatement object.
        """
        self._indices.append(TableIndex(index_name, TableIndex.INDEX,
            flexible_params(*cols)))
        return self

    def add_unique_index(self, index_name, *cols):
        """Adds a Unique Index to the Table.

        Args:
            index_name (string): Name of the Unique Index.
            *cols: Fields to be used as a Unique Index.

        Returns:
            mysqlx.CreateTableStatement: CreateTableStatement object.
        """
        self._u_indices.append(TableIndex(index_name, TableIndex.UNIQUE_INDEX,
            flexible_params(*cols)))
        return self

    def add_foreign_key(self, name, key):
        """Adds a Foreign Key to the Table.

        Args:
            key (MySQLx.ForeignKeyDef): The Foreign Key Definition object.

        Returns:
            mysqlx.CreateTableStatement: CreateTableStatement object.
        """
        key.set_schema(self.schema.get_name())
        key.set_name(name)
        self._f_keys.append(key)
        return self

    def set_initial_auto_increment(self, inc):
        """Set the initial Auto Increment value for the table.

        Args:
            inc (int): The initial AUTO_INCREMENT value for the table.

        Returns:
            mysqlx.CreateTableStatement: CreateTableStatement object.
        """
        self._auto_inc = inc
        return self

    def set_default_charset(self, charset):
        """Sets the default Charset type for the Table.

        Args:
            charset (string): Charset type.

        Returns:
            mysqlx.CreateTableStatement: CreateTableStatement object.
        """
        self._charset = charset
        return self

    def set_default_collation(self, collation):
        """Sets the default Collation type for the Table.

        Args:
            collation (string): Collation type.

        Returns:
            mysqlx.CreateTableStatement: CreateTableStatement object.
        """
        self._collation = collation
        return self

    def set_comment(self, comment):
        """Add a comment to the Table.

        Args:
            comment (string): Comment to be added to the Table.

        Returns:
            mysqlx.CreateTableStatement: CreateTableStatement object.
        """
        self._comment = comment
        return self

    def temporary(self):
        """Set the Table to be Temporary.

        Returns:
            mysqlx.CreateTableStatement: CreateTableStatement object.
        """
        self._temp = True
        return self

    def execute(self):
        """Execute the statement.

        Returns:
            mysqlx.Table: Table object.
        """
        create = "CREATE {table_type} {name}".format(name=self.table_name,
            table_type="TEMPORARY TABLE" if self._temp else "TABLE")
        if self._like:
            stmt = "{create} LIKE {query}"
        else:
            stmt = "{create} ({create_def}) {table_opts} {query}"

        stmt = stmt.format(
            create=create,
            query=self._like or self._as or "",
            create_def=self._get_create_def(),
            table_opts=self._get_table_opts())

        self._connection.execute_nonquery("sql", stmt, False)
        return self.schema.get_table(self._name)


class ColumnDefBase(object):
    """A Base class defining the basic parameters required to define a column.

    Args:
        name (string): Name of the column.
        type (MySQLx.ColumnType): Type of the column.
        size (int): Size of the column.
    """
    def __init__(self, name, type, size):
        self._default_schema = None
        self._not_null = False
        self._p_key = False
        self._u_index = False
        self._name = name
        self._size = size
        self._comment = ""
        self._type = type

    def not_null(self):
        """Disable NULL values for this column.

        Returns:
            mysqlx.ColumnDefBase: ColumnDefBase object.
        """
        self._not_null = True
        return self

    def unique_index(self):
        """Set current column as a Unique Index.

        Returns:
            mysqlx.ColumnDefBase: ColumnDefBase object.
       """
        self._u_index = True
        return self

    def comment(self, comment):
        """Add a comment to the column.

        Args:
            comment (string): Comment to be added to the column.

        Returns:
            mysqlx.ColumnDefBase: ColumnDefBase object.
        """
        self._comment = comment
        return self

    def primary(self):
        """Sets the Column as a Primary Key.

        Returns:
            mysqlx.ColumnDefBase: ColumnDefBase object.
        """
        self._p_key = True
        return self

    def set_schema(self, schema):
        self._default_schema = schema


class ColumnDef(ColumnDefBase):
    """Class containing the complete definition of the Column.

    Args:
        name (string): Name of the column.
        type (MySQL.ColumnType): Type of the column.
        size (int): Size of the column.
    """
    def __init__(self, name, type, size=None):
        super(ColumnDef, self).__init__(name, type, size)
        self._ref = None
        self._default = None
        self._decimals = None
        self._ref_table = None

        self._binary = False
        self._auto_inc = False
        self._unsigned = False

        self._values = []
        self._ref_fields = []

        self._charset = None
        self._collation = None

    def _data_type(self):
        type_def = ""
        if self._size and (ColumnType.is_numeric(self._type) or \
            ColumnType.is_char(self._type) or ColumnType.is_binary(self._type)):
            type_def = "({0})".format(self._size)
        elif ColumnType.is_decimals(self._type) and self._size:
            type_def = "({0}, {1})".format(self._size, self._decimals or 0)
        elif ColumnType.is_finite_set(self._type):
            type_def = "({0})".format(",".join(self._values))

        if self._unsigned:
            type_def = "{0} UNSIGNED".format(type_def)
        if self._binary:
            type_def = "{0} BINARY".format(type_def)
        if self._charset:
            type_def = "{0} CHARACTER SET {1}".format(type_def, self._charset)
        if self._collation:
            type_def = "{0} COLLATE {1}".format(type_def, self._collation)

        return "{0} {1}".format(ColumnType.to_string(self._type), type_def)

    def _col_definition(self):
        null = " NOT NULL" if self._not_null else " NULL"
        auto_inc = " AUTO_INCREMENT" if self._auto_inc else ""
        default = " DEFAULT {default}" if self._default else ""
        comment = " COMMENT '{comment}'" if self._comment else ""

        defn = "{0}{1}{2}{3}{4}".format(self._data_type(), null, default,
            auto_inc, comment)

        if self._p_key:
            defn = "{0} PRIMARY KEY".format(defn)
        elif self._u_index:
            defn = "{0} UNIQUE KEY".format(defn)
        if self._ref_table and self._ref_fields:
            ref_table = quote_multipart_identifier(parse_table_name(
                self._default_schema, self._ref_table))
            defn = "{0} REFERENCES {1} ({2})".format(defn, ref_table,
                ",".join(self._ref_fields))

        return defn.format(default=self._default, comment=self._comment)

    def set_default(self, default_val):
        """Sets the default value of this Column.

        Args:
            default_val (object): The default value of the Column. Can be a
            string, number or :class`MySQLx.Expr`.

        Returns:
            mysqlx.ColumnDef: ColumnDef object.
        """
        if isinstance(default_val, Expr):
            self._default = default_val.expr
        elif default_val is None:
            self._default = "NULL"
        else:
            self._default = repr(default_val)

        return self

    def auto_increment(self):
        """Set the Column to Auto Increment.

        Returns:
            mysqlx.ColumnDef: ColumnDef object.
        """
        self._auto_inc = True
        return self

    def foreign_key(self, name, *refs):
        """Sets the Column as a Foreign Key.

        Args:
            name (string): Name of the referenced Table.
            *refs: Fields this Column references.

        Returns:
            mysqlx.ColumnDef: ColumnDef object.
        """
        self._ref_fields = flexible_params(*refs)
        self._ref_table = name
        return self

    def unsigned(self):
        """Set the Column as unsigned.

        Returns:
            mysqlx.ColumnDef: ColumnDef object.
        """
        self._unsigned = True
        return self

    def decimals(self, size):
        """Set the size of the decimal Column.

        Args:
            size (int): Size of the decimal.

        Returns:
            mysqlx.ColumnDef: ColumnDef object.
        """
        self._decimals = size
        return self

    def charset(self, charset):
        """Set the Charset type of the Column.

        Args:
            charset (string): Charset type.

        Returns:
            mysqlx.ColumnDef: ColumnDef object.
        """
        self._charset = charset
        return self

    def collation(self, collation):
        """Set the Collation type of the Column.

        Args:
            collation (string): Collation type.

        Returns:
            mysqlx.ColumnDef: ColumnDef object.
        """
        self._collation = collation
        return self

    def binary(self):
        """Set the current column to binary type.

        Returns:
            mysqlx.ColumnDef: ColumnDef object.
        """
        self._binary = True
        return self

    def values(self, *values):
        """Set the Enum/Set values.

        Args:
            *values: Values for Enum/Set type Column.

        Returns:
            mysqlx.ColumnDef: ColumnDef object.
        """
        self._values = map(repr, flexible_params(*values))
        return self

    def get_sql(self):
        return "{0} {1}".format(self._name, self._col_definition())


class GeneratedColumnDef(ColumnDef):
    """Class used to describe a Generated Column.

    Args:
        name: Name of the column.
        col_type: Type of the column.
        expr: The Expression used to generate the value of this column.
    """
    def __init__(self, name, col_type, expr):
        super(GeneratedColumnDef, self).__init__(name, col_type)
        assert isinstance(expr, Expr)
        self._stored = False
        self._expr = expr.expr

    def stored(self):
        """Set the Generated Column to be stored.

        Returns:
            mysqlx.GeneratedColumnDef: GeneratedColumnDef object.
        """
        self._stored = True
        return self

    def get_sql(self):
        return "{0} GENERATED ALWAYS AS ({1}){2}".format(
            super(GeneratedColumnDef, self).get_sql(),
            self._expr, " STORED" if self._stored else "")


class ForeignKeyDef(object):
    """Class describing a Foreign Key."""
    NO_ACTION = 1
    RESTRICT = 2
    CASCADE = 3
    SET_NULL = 4

    def __init__(self):
        self._fields = []
        self._f_fields = []
        self._name = None
        self._f_table = None
        self._default_schema = None
        self._update_action = self._action(ForeignKeyDef.NO_ACTION)
        self._delete_action = self._action(ForeignKeyDef.NO_ACTION)

    def _action(self, action):
        if action is ForeignKeyDef.RESTRICT:
            return "RESTRICT"
        elif action is ForeignKeyDef.CASCADE:
            return "CASCADE"
        elif action is ForeignKeyDef.SET_NULL:
            return "SET NULL"
        return "NO ACTION"

    def set_name(self, name):
        self._name = name

    def set_schema(self, schema):
        self._default_schema = schema

    def fields(self, *fields):
        """Add a list of fields in the parent table.

        Args:
            *fields: Fields in the given table which constitute the Foreign Key.

        Returns:
            mysqlx.ForeignKeyDef: ForeignKeyDef object.
        """
        self._fields = flexible_params(*fields)
        return self

    def refers_to(self, name, *refs):
        """Add the child table name and the fields.

        Args:
            name (string): Name of the referenced table.
            *refs: A list fields in the referenced table.

        Returns:
            mysqlx.ForeignKeyDef: ForeignKeyDef object.
        """
        self._f_fields = flexible_params(*refs)
        self._f_table = name
        return self

    def on_update(self, action):
        """Define the action on updating a Foreign Key.

        Args:
            action (int): Action to be performed on updating the reference.
                          Can be any of the following values:
                          1. ForeignKeyDef.NO_ACTION
                          2. ForeignKeyDef.RESTRICT
                          3. ForeignKeyDef.CASCADE
                          4. ForeignKeyDef.SET_NULL

        Returns:
            mysqlx.ForeignKeyDef: ForeignKeyDef object.
        """

        self._update_action = self._action(action)
        return self

    def on_delete(self, action):
        """Define the action on deleting a Foreign Key.

        Args:
            action (int): Action to be performed on updating the reference.
                          Can be any of the following values:
                          1. ForeignKeyDef.NO_ACTION
                          2. ForeignKeyDef.RESTRICT
                          3. ForeignKeyDef.CASCADE
                          4. ForeignKeyDef.SET_NULL

        Returns:
            mysqlx.ForeignKeyDef: ForeignKeyDef object.
        """
        self._delete_action = self._action(action)
        return self

    def get_sql(self):
        update = "ON UPDATE {0}".format(self._update_action)
        delete = "ON DELETE {0}".format(self._delete_action)
        key = "FOREIGN KEY {0}({1}) REFERENCES {2} ({3})".format(
            self._name, ",".join(self._fields), quote_multipart_identifier(
            parse_table_name(self._default_schema, self._f_table)),
            ",".join(self._f_fields))
        return "{0} {1} {2}".format(key, update, delete)
