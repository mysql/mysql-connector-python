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

from .statement import (FindStatement, AddStatement, RemoveStatement, ModifyStatement,
                        SelectStatement, InsertStatement, DeleteStatement, UpdateStatement,
                        CreateCollectionIndexStatement, DropCollectionIndexStatement)


_COUNT_TABLES_QUERY = ("SELECT COUNT(*) FROM information_schema.tables "
                       "WHERE table_schema = '{0}' AND table_name = '{1}'")
_COUNT_SCHEMAS_QUERY = ("SELECT COUNT(*) FROM information_schema.schemata "
                        "WHERE schema_name like '{0}'")
_COUNT_QUERY = "SELECT COUNT(*) FROM `{0}`.`{1}`"
_DROP_TABLE_QUERY = "DROP TABLE IF EXISTS `{0}`.`{1}`"


class DatabaseObject(object):
    def __init__(self, schema, name):
        self._schema = schema
        self._name = name
        self._connection = self._schema.get_session()._connection

    @property
    def schema(self):
        return self._schema

    @property
    def name(self):
        return self._name

    def get_schema(self):
        return self._schema

    def get_name(self):
        return self._name


class Schema(DatabaseObject):
    def __init__(self, session, name):
        self._session = session
        super(Schema, self).__init__(self, name)

    def exists_in_database(self):
        sql = _COUNT_SCHEMAS_QUERY.format(self._name)
        return self._connection.execute_sql_scalar(sql) == 1

    def get_session(self):
        return self._session

    def get_collections(self):
        rows = self._connection.get_row_result("list_objects", self._name)
        rows.fetch_all()
        collections = []
        for row in rows:
            if row.get_string("type") != "COLLECTION":
                continue
            collection = Collection(self, row.get_string("name"))
            collections.append(collection)
        return collections

    def get_tables(self):
        rows = self._connection.get_row_result("list_objects", self._name)
        rows.fetch_all()
        tables = []
        for row in rows:
            if row.get_string("type") != "TABLE":
                continue
            table = Table(self, row.get_string("name"))
            tables.append(table)
        return tables

    def get_table(self, name, check_existence=False):
        table = Table(self, name)
        if check_existence:
            if not table.exists_in_database():
                raise Exception("table does not exist")
        return table

    def get_collection(self, name, check_existence=False):
        collection = Collection(self, name)
        if check_existence:
            if not collection.exists_in_database():
                raise Exception("collection does not exist")
        return collection

    def drop_collection(self, name):
        self._connection.execute_nonquery(
            "sql", _DROP_TABLE_QUERY.format(self._name, name), False)

    def drop_table(self, name):
        self._connection.execute_nonquery(
            "sql", _DROP_TABLE_QUERY.format(self._name, name), False)

    def create_collection(self, name, reuse=False):
        collection = Collection(self, name)
        if not collection.exists_in_database():
            self._connection.execute_nonquery("xplugin", "create_collection",
                                              True, self._name, name)
        elif not reuse:
            raise Exception("Collection already exists")
        return collection


class Collection(DatabaseObject):
    def __init__(self, schema, name):
        super(Collection, self).__init__(schema, name)

    def exists_in_database(self):
        sql = _COUNT_TABLES_QUERY.format(self._schema.get_name(), self._name)
        return self._connection.execute_sql_scalar(sql) == 1

    def find(self, condition=None):
        return FindStatement(self, condition)

    def add(self, *values):
        return AddStatement(self).add(*values)

    def remove_one(self, id):
        return self.remove("_id = '{0}'".format(id))

    def remove(self, condition=None):
        rs = RemoveStatement(self)
        if not condition == None:
            rs.where(condition)
        return rs

    def modify(self, condition=None):
        return ModifyStatement(self, condition)

    def count(self):
        sql = _COUNT_QUERY.format(self._schema.name, self._name)
        return self._connection.execute_sql_scalar(sql)

    def create_index(self, index_name, is_unique):
        """Creates a collection index.

        Args:
            index_name (string): Index name.
            is_unique (bool): True if the index is unique.
        """
        return CreateCollectionIndexStatement(self, index_name, is_unique)

    def drop_index(self, index_name):
        """Drops a collection index.

        Args:
            index_name (string): Index name.
        """
        return DropCollectionIndexStatement(self, index_name)


class Table(DatabaseObject):
    def __init__(self, schema, name):
        super(Table, self).__init__(schema, name)

    def exists_in_database(self):
        sql = _COUNT_TABLES_QUERY.format(self._schema.name, self._name)
        return self._connection.execute_sql_scalar(sql) == 1

    def select(self, *fields):
        return SelectStatement(self, *fields)

    def insert(self, *fields):
        return InsertStatement(self, *fields)

    def update(self):
        return UpdateStatement(self)

    def delete(self, condition=None):
        return DeleteStatement(self, condition)

    def count(self):
        sql = _COUNT_QUERY.format(self._schema.name, self._name)
        return self._connection.execute_sql_scalar(sql)
