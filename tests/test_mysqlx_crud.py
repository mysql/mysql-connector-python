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

"""Unittests for mysqlx.crud
"""

import logging
import unittest

import tests

try:
    import mysqlx
except ImportError:
    MYSQLX_AVAILABLE = False
else:
    MYSQLX_AVAILABLE = True

LOGGER = logging.getLogger(tests.LOGGER_NAME)

_CREATE_TEST_TABLE_QUERY = "CREATE TABLE `{0}`.`{1}` (id INT)"
_INSERT_TEST_TABLE_QUERY = "INSERT INTO `{0}`.`{1}` VALUES ({2})"
_COUNT_TABLES_QUERY = ("SELECT COUNT(*) FROM information_schema.tables "
                       "WHERE table_schema = '{0}' AND table_name = '{1}'")


@unittest.skipIf(MYSQLX_AVAILABLE is False, "MySQLX not available")
@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 12), "XPlugin not compatible")
class MySQLxSchemaTests(tests.MySQLxTests):

    def setUp(self):
        self.connect_kwargs = tests.get_mysqlx_config()
        self.schema_name = self.connect_kwargs["database"]
        try:
            self.session = mysqlx.get_session(self.connect_kwargs)
        except Exception as err:  # TODO: Replace by a custom error
            self.fail("{0}".format(err))
        self.schema = self.session.create_schema(self.schema_name)

    def tearDown(self):
        self.session.drop_schema(self.schema_name)

    def test_get_session(self):
        session = self.schema.get_session()
        self.assertEqual(session, self.session)

    # TODO: Fix this test
    # def test_get_collections(self):
    #     collection_names = ["collection_{0}".format(idx) for idx in range(3)]
    #     for name in collection_names:
    #         self.schema.create_collection(name)
    #     collections = self.schema.get_collections()
    #     for collection in collections:
    #         self.assertTrue(collection.exists_in_database())
    #     for name in collection_names:
    #         self.schema.drop_collection(name)

    def test_create_collection(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name, True)
        self.assertEqual(collection.get_name(), collection_name)
        self.assertTrue(collection.exists_in_database())

        # reusing the existing collection should work
        collection = self.schema.create_collection(collection_name, True)
        self.assertEqual(collection.get_name(), collection_name)
        self.assertTrue(collection.exists_in_database())

        # should get exception if reuse is false and it already exists
        self.assertRaises(Exception, self.schema.create_collection, collection_name, False)

        self.schema.drop_collection(collection_name)

    def test_get_collection(self):
        collection_name = "collection_test"
        coll =  self.schema.get_collection(collection_name)
        self.assertFalse(coll.exists_in_database())
        coll = self.schema.create_collection(collection_name)
        self.assertTrue(coll.exists_in_database())

    def test_get_collections(self):
        coll = self.schema.get_collections()
        self.assertEqual(0, len(coll), "Should have returned 0 objects")
        self.schema.create_collection("coll1")
        self.schema.create_collection("coll2")
        self.schema.create_collection("coll3")
        coll = self.schema.get_collections()
        self.assertEqual(3, len(coll), "Should have returned 3 objects")
        self.assertEqual("coll1", coll[0].get_name())
        self.assertEqual("coll2", coll[1].get_name())
        self.assertEqual("coll3", coll[2].get_name())

    def test_get_tables(self):
        tables = self.schema.get_tables()
        self.assertEqual(0, len(tables), "Should have returned 0 objects")

        self.session.connection.execute_nonquery("sql", "CREATE TABLE {0}.table1(id INT)".format(self.schema_name), True)
        self.session.connection.execute_nonquery("sql", "CREATE TABLE {0}.table2(id INT)".format(self.schema_name), True)
        self.session.connection.execute_nonquery("sql", "CREATE TABLE {0}.table3(id INT)".format(self.schema_name), True)

        tables = self.schema.get_tables()
        self.assertEqual(3, len(tables), "Should have returned 3 objects")
        self.assertEqual("table1", tables[0].get_name())
        self.assertEqual("table2", tables[1].get_name())
        self.assertEqual("table3", tables[2].get_name())

    def test_drop_collection(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        self.schema.drop_collection(collection_name)
        self.assertFalse(collection.exists_in_database())

        # dropping an non-existing collection should succeed silently
        self.schema.drop_collection(collection_name)

    def test_drop_table(self):
        table_name = "table_test"
        # TODO: Replace the table creation by:
        #       schema.create_table(table_name)
        try:
            sql = _CREATE_TEST_TABLE_QUERY.format(self.schema_name, table_name)
            self.session.connection.execute_nonquery("sql", sql, True)
        except Exception as err:
            LOGGER.info("{0}".format(err))
        table = self.schema.get_table(table_name)
        self.schema.drop_table(table_name)
        self.assertFalse(table.exists_in_database())

        # dropping an non-existing table should succeed silently
        self.schema.drop_table(table_name)


@unittest.skipIf(MYSQLX_AVAILABLE is False, "MySQLX not available")
@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 12), "XPlugin not compatible")
class MySQLxCollectionTests(tests.MySQLxTests):

    def setUp(self):
        self.connect_kwargs = tests.get_mysqlx_config()
        self.schema_name = self.connect_kwargs["database"]
        try:
            self.session = mysqlx.get_session(self.connect_kwargs)
        except Exception as err:  # TODO: Replace by a custom error
            self.fail("{0}".format(err))
        self.schema = self.session.get_schema(self.schema_name)
        if not self.schema.exists_in_database():
            self.schema = self.session.create_schema(self.schema_name)

    def test_exists_in_database(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        self.assertTrue(collection.exists_in_database())
        self.schema.drop_collection(collection_name)

    def test_add(self):
        # TODO: To implement
        pass

    def test_remove(self):
        # TODO: To implement
        pass

    def test_count(self):
        # TODO: To implement
        pass


@unittest.skipIf(MYSQLX_AVAILABLE is False, "MySQLX not available")
@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 12), "XPlugin not compatible")
class MySQLxTableTests(tests.MySQLxTests):

    def setUp(self):
        self.connect_kwargs = tests.get_mysqlx_config()
        self.schema_name = self.connect_kwargs["database"]
        try:
            self.session = mysqlx.get_session(self.connect_kwargs)
        except Exception as err:  # TODO: Replace by a custom error
            self.fail("{0}".format(err))
        self.schema = self.session.get_schema(self.schema_name)
        if not self.schema.exists_in_database():
            self.schema = self.session.create_schema(self.schema_name)

    def test_exists_in_database(self):
        table_name = "table_test"
        try:
            sql = _CREATE_TEST_TABLE_QUERY.format(self.schema_name, table_name)
            self.session.connection.execute_nonquery("sql", sql, True)
        except Exception as err:
            LOGGER.info("{0}".format(err))
        table = self.schema.get_table(table_name)
        self.assertTrue(table.exists_in_database())
        self.schema.drop_table(table_name)

    def test_select(self):
        # TODO: To implement
        pass

    def test_insert(self):
        # TODO: To implement
        pass

    def test_update(self):
        # TODO: To implement
        pass

    def test_delete(self):
        table_name = "table_test"
        self.session.connection.execute_nonquery(
            "sql", _CREATE_TEST_TABLE_QUERY.format(self.schema_name,
                                                   table_name), True)
        self.session.connection.execute_nonquery(
            "sql", _INSERT_TEST_TABLE_QUERY.format(self.schema_name,
                                                   table_name, "1"), True)
        table = self.schema.get_table(table_name)
        self.assertEqual(table.count(), 1)
        table.delete("id = 1").execute()
        self.assertEqual(table.count(), 0)
        self.schema.drop_table(table_name)

    def test_count(self):
        table_name = "table_test"
        self.session.connection.execute_nonquery(
            "sql", _CREATE_TEST_TABLE_QUERY.format(self.schema_name,
                                                   table_name), True)
        self.session.connection.execute_nonquery(
            "sql", _INSERT_TEST_TABLE_QUERY.format(self.schema_name,
                                                   table_name, "1"), True)
        table = self.schema.get_table(table_name)
        self.assertEqual(table.count(), 1)
        self.schema.drop_table(table_name)
