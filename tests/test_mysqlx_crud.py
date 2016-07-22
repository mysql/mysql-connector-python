# -*- coding: utf-8 -*-
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
import mysqlx

LOGGER = logging.getLogger(tests.LOGGER_NAME)

_CREATE_TEST_TABLE_QUERY = "CREATE TABLE `{0}`.`{1}` (id INT)"
_INSERT_TEST_TABLE_QUERY = "INSERT INTO `{0}`.`{1}` VALUES ({2})"
_CREATE_TEST_VIEW_QUERY = ("CREATE VIEW `{0}`.`{1}` AS SELECT * "
                           "FROM `{2}`.`{3}`;")
_COUNT_TABLES_QUERY = ("SELECT COUNT(*) FROM information_schema.tables "
                       "WHERE table_schema = '{0}' AND table_name = '{1}'")


@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 12), "XPlugin not compatible")
class MySQLxSchemaTests(tests.MySQLxTests):

    def setUp(self):
        self.connect_kwargs = tests.get_mysqlx_config()
        self.schema_name = self.connect_kwargs["schema"]
        try:
            self.session = mysqlx.get_session(self.connect_kwargs)
            self.node_session = mysqlx.get_node_session(self.connect_kwargs)
        except mysqlx.Error as err:
            self.fail("{0}".format(err))
        self.schema = self.session.create_schema(self.schema_name)

    def tearDown(self):
        self.session.drop_schema(self.schema_name)

    def test_get_session(self):
        session = self.schema.get_session()
        self.assertEqual(session, self.session)
        self.assertTrue(self.schema.exists_in_database())
        bad_schema = self.session.get_schema("boo")
        self.assertFalse(bad_schema.exists_in_database())

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
        self.assertRaises(mysqlx.ProgrammingError,
                          self.schema.create_collection, collection_name,
                          False)

        self.schema.drop_collection(collection_name)

    def test_get_collection(self):
        collection_name = "collection_test"
        coll = self.schema.get_collection(collection_name)
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

        self.node_session.sql(_CREATE_TEST_TABLE_QUERY.format(
            self.schema_name, "table1")).execute()
        self.node_session.sql(_CREATE_TEST_TABLE_QUERY.format(
            self.schema_name, "table2")).execute()
        self.node_session.sql(_CREATE_TEST_TABLE_QUERY.format(
            self.schema_name, "table3")).execute()
        self.node_session.sql(_CREATE_TEST_VIEW_QUERY.format(
            self.schema_name, "view1",
            self.schema_name, "table1")).execute()
        tables = self.schema.get_tables()
        self.assertEqual(4, len(tables), "Should have returned 4 objects")
        self.assertEqual("table1", tables[0].get_name())
        self.assertEqual("table2", tables[1].get_name())
        self.assertEqual("table3", tables[2].get_name())
        self.assertEqual("view1", tables[3].get_name())

    def test_drop_collection(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        self.schema.drop_collection(collection_name)
        self.assertFalse(collection.exists_in_database())

        # dropping an non-existing collection should succeed silently
        self.schema.drop_collection(collection_name)

    def test_drop_table(self):
        table_name = "table_test"
        try:
            self.node_session.sql(
                _CREATE_TEST_TABLE_QUERY.format(self.schema_name, table_name)
            ).execute()
        except mysqlx.Error as err:
            LOGGER.info("{0}".format(err))
        table = self.schema.get_table(table_name)
        self.schema.drop_table(table_name)
        self.assertFalse(table.exists_in_database())

        # dropping an non-existing table should succeed silently
        self.schema.drop_table(table_name)


@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 12), "XPlugin not compatible")
class MySQLxCollectionTests(tests.MySQLxTests):

    def setUp(self):
        self.connect_kwargs = tests.get_mysqlx_config()
        self.schema_name = self.connect_kwargs["schema"]
        try:
            self.session = mysqlx.get_session(self.connect_kwargs)
            self.node_session = mysqlx.get_node_session(self.connect_kwargs)
        except mysqlx.Error as err:
            self.fail("{0}".format(err))
        self.schema = self.session.create_schema(self.schema_name)

    def tearDown(self):
        self.session.drop_schema(self.schema_name)

    def test_exists_in_database(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        self.assertTrue(collection.exists_in_database())
        self.schema.drop_collection(collection_name)

    def test_add(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        result = collection.add({"name": "Fred", "age": 21}).execute()
        self.assertEqual(result.get_affected_items_count(), 1)
        self.assertEqual(1, collection.count())

        # now add multiple dictionaries at once
        result = collection.add({"name": "Wilma", "age": 33},
                                {"name": "Barney", "age": 42}).execute()
        self.assertEqual(result.get_affected_items_count(), 2)
        self.assertEqual(3, collection.count())

        # now let's try adding strings
        result = collection.add('{"name": "Bambam", "age": 8}',
                                '{"name": "Pebbles", "age": 8}').execute()
        self.assertEqual(result.get_affected_items_count(), 2)
        self.assertEqual(5, collection.count())

    def test_get_document_ids(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        result = collection.add({"name": "Fred", "age": 21}).execute()
        self.assertTrue(result.get_document_id() is not None)

        result = collection.add(
            {"name": "Fred", "age": 21},
            {"name": "Barney", "age": 45}).execute()
        self.assertEqual(2, len(result.get_document_ids()))

    def test_remove(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add({"name": "Fred", "age": 21}).execute()
        self.assertEqual(1, collection.count())
        result = collection.remove("age == 21").execute()
        self.assertEqual(1, result.get_affected_items_count())
        self.assertEqual(0, collection.count())

    def test_find(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        result = collection.add(
            {"name": "Fred", "age": 21},
            {"name": "Barney", "age": 28},
            {"name": "Wilma", "age": 42},
            {"name": "Betty", "age": 67},
        ).execute()
        result = collection.find("$.age == 67").execute()
        docs = result.fetch_all()
        self.assertEqual(1, len(docs))
        self.assertEqual("Betty", docs[0]["name"])

        result = \
            collection.find("$.age > 28").sort("age DESC, name ASC").execute()
        docs = result.fetch_all()
        self.assertEqual(2, len(docs))
        self.assertEqual(67, docs[0]["age"])

        result = \
            collection.find().fields("age").sort("age DESC").limit(2).execute()
        docs = result.fetch_all()
        self.assertEqual(2, len(docs))
        self.assertEqual(42, docs[1]["age"])
        self.assertEqual(1, len(docs[1].keys()))

    def test_modify(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        result = collection.add(
            {"name": "Fred", "age": 21},
            {"name": "Barney", "age": 28},
            {"name": "Wilma", "age": 42},
            {"name": "Betty", "age": 67},
        ).execute()

        result = collection.modify("age < 67").set("young", True).execute()
        self.assertEqual(3, result.get_affected_items_count())
        doc = collection.find("name = 'Fred'").execute().fetch_all()[0]
        self.assertEqual(True, doc.young)

        result = \
            collection.modify("age == 28").change("young", False).execute()
        self.assertEqual(1, result.get_affected_items_count())
        docs = collection.find("young = True").execute().fetch_all()
        self.assertEqual(2, len(docs))

        result = collection.modify("young == True").unset("young").execute()
        self.assertEqual(2, result.get_affected_items_count())
        docs = collection.find("young = True").execute().fetch_all()
        self.assertEqual(0, len(docs))

    def test_results(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add(
            {"name": "Fred", "age": 21},
            {"name": "Barney", "age": 28},
            {"name": "Wilma", "age": 42},
            {"name": "Betty", "age": 67},
        ).execute()
        result1 = collection.find().execute()
        # now do another collection find.
        # the first one will have to be transparently buffered
        result2 = collection.find("age > 28").sort("age DESC").execute()
        docs2 = result2.fetch_all()
        self.assertEqual(2, len(docs2))
        self.assertEqual("Betty", docs2[0]["name"])

        docs1 = result1.fetch_all()
        self.assertEqual(4, len(docs1))

        result3 = collection.find("age > 28").sort("age DESC").execute()
        self.assertEqual("Betty", result3.fetch_one()["name"])
        self.assertEqual("Wilma", result3.fetch_one()["name"])
        self.assertEqual(None, result3.fetch_one())

    # def test_create_index(self):
    #     collection_name = "collection_test"
    #     collection = self.schema.create_collection(collection_name)
    #
    #     index_name = "age_idx"
    #     collection.create_index(index_name, True) \
    #         .field("$.age", "INT", False).execute()
    #
    #     show_indexes_sql = (
    #         "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'"
    #         "".format(self.schema_name, collection_name, index_name)
    #     )
    #
    #     result = self.node_session.sql(show_indexes_sql).execute()
    #     rows = result.fetch_all()
    #     self.assertEqual(1, len(rows))

    # def test_drop_index(self):
    #     collection_name = "collection_test"
    #     collection = self.schema.create_collection(collection_name)
    #
    #     index_name = "age_idx"
    #     collection.create_index(index_name, True) \
    #         .field("$.age", "INT", False).execute()
    #
    #     show_indexes_sql = (
    #         "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'"
    #         "".format(self.schema_name, collection_name, index_name)
    #     )
    #
    #     result = self.node_session.sql(show_indexes_sql).execute()
    #     rows = result.fetch_all()
    #     self.assertEqual(1, len(rows))
    #
    #     collection.drop_index(index_name).execute()
    #     result = self.node_session.sql(show_indexes_sql).execute()
    #     rows = result.fetch_all()
    #     self.assertEqual(0, len(rows))

    def test_parameter_binding(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        result = collection.add(
            {"name": "Fred", "age": 21},
            {"name": "Barney", "age": 28},
            {"name": "Wilma", "age": 42},
            {"name": "Betty", "age": 67},
        ).execute()
        result = collection.find("age == :age").bind("age", 67).execute()
        docs = result.fetch_all()
        self.assertEqual(1, len(docs))
        self.assertEqual("Betty", docs[0]["name"])

        result = collection.find("$.age = :age").bind('{"age": 42}') \
            .sort("age DESC, name ASC").execute()
        docs = result.fetch_all()
        self.assertEqual(1, len(docs))
        self.assertEqual("Wilma", docs[0]["name"])

    def test_unicode_parameter_binding(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        result = collection.add(
            {"name": u"José", "age": 21},
            {"name": u"João", "age": 28},
            {"name": u"Célia", "age": 42},
        ).execute()
        result = collection.find("name == :name").bind("name", u"José") \
                                                 .execute()
        docs = result.fetch_all()
        self.assertEqual(1, len(docs))
        self.assertEqual(u"José", docs[0]["name"])

        result = collection.find("$.name = :name").bind(u'{"name": "João"}') \
                                                  .execute()
        docs = result.fetch_all()
        self.assertEqual(1, len(docs))
        self.assertEqual(u"João", docs[0]["name"])

    def test_array_insert(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add(
            {"_id": 1, "name": "Fred", "cards": []},
            {"_id": 2, "name": "Barney", "cards": [1, 2, 4]},
            {"_id": 3, "name": "Wilma", "cards": []},
            {"_id": 4, "name": "Betty", "cards": []},
        ).execute()
        collection.modify("$._id == 2").array_insert("$.cards[2]", 3).execute()
        docs = collection.find("$._id == 2").execute().fetch_all()
        self.assertEqual([1, 2, 3, 4], docs[0]["cards"])

    def test_array_append(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add(
            {"_id": 1, "name": "Fred", "cards": []},
            {"_id": 2, "name": "Barney", "cards": [1, 2, 4]},
            {"_id": 3, "name": "Wilma", "cards": []},
            {"_id": 4, "name": "Betty", "cards": []},
        ).execute()
        collection.modify("$._id == 2").array_append("$.cards[1]", 3).execute()
        docs = collection.find("$._id == 2").execute().fetch_all()
        self.assertEqual([1, [2, 3], 4], docs[0]["cards"])


@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 12), "XPlugin not compatible")
class MySQLxTableTests(tests.MySQLxTests):

    def setUp(self):
        self.connect_kwargs = tests.get_mysqlx_config()
        self.schema_name = self.connect_kwargs["schema"]
        try:
            self.session = mysqlx.get_session(self.connect_kwargs)
            self.node_session = mysqlx.get_node_session(self.connect_kwargs)
        except mysqlx.Error as err:
            self.fail("{0}".format(err))
        self.schema = self.session.create_schema(self.schema_name)

    def tearDown(self):
        self.session.drop_schema(self.schema_name)

    def test_exists_in_database(self):
        table_name = "table_test"
        try:
            sql = _CREATE_TEST_TABLE_QUERY.format(self.schema_name, table_name)
            self.node_session.sql(sql).execute()
        except mysqlx.Error as err:
            LOGGER.info("{0}".format(err))
        table = self.schema.get_table(table_name)
        self.assertTrue(table.exists_in_database())
        self.schema.drop_table(table_name)

    def test_select(self):
        table_name = "{0}.test".format(self.schema_name)

        self.node_session.sql("CREATE TABLE {0}(age INT, name VARCHAR(50))"
                              "".format(table_name)).execute()
        self.node_session.sql("INSERT INTO {0} VALUES (21, 'Fred')"
                              "".format(table_name)).execute()
        self.node_session.sql("INSERT INTO {0} VALUES (28, 'Barney')"
                              "".format(table_name)).execute()
        self.node_session.sql("INSERT INTO {0} VALUES (42, 'Wilma')"
                              "".format(table_name)).execute()
        self.node_session.sql("INSERT INTO {0} VALUES (67, 'Betty')"
                              "".format(table_name)).execute()

        table = self.schema.get_table("test")
        result = table.select().sort("age DESC").execute()
        rows = result.fetch_all()
        self.assertEqual(4, len(rows))
        self.assertEqual(67, rows[0]["age"])

        result = table.select("age").where("age = 42").execute()
        self.assertEqual(1, len(result.columns))
        rows = result.fetch_all()
        self.assertEqual(1, len(rows))

    def test_having(self):
        table_name = "{0}.test".format(self.schema_name)

        self.node_session.sql("CREATE TABLE {0}(age INT, name VARCHAR(50), "
                              "gender CHAR(1))".format(table_name)).execute()
        self.node_session.sql("INSERT INTO {0} VALUES (21, 'Fred', 'M')"
                              "".format(table_name)).execute()
        self.node_session.sql("INSERT INTO {0} VALUES (28, 'Barney', 'M')"
                              "".format(table_name)).execute()
        self.node_session.sql("INSERT INTO {0} VALUES (42, 'Wilma', 'F')"
                              "".format(table_name)).execute()
        self.node_session.sql("INSERT INTO {0} VALUES (67, 'Betty', 'F')"
                              "".format(table_name)).execute()

        table = self.schema.get_table("test")
        result = table.select().group_by("gender").sort("age ASC").execute()
        rows = result.fetch_all()
        self.assertEqual(2, len(rows))
        self.assertEqual(21, rows[0]["age"])
        self.assertEqual(42, rows[1]["age"])

        result = table.select().group_by("gender").having("gender = 'F'") \
                                                  .sort("age ASC").execute()
        rows = result.fetch_all()
        self.assertEqual(1, len(rows))
        self.assertEqual(42, rows[0]["age"])

    def test_insert(self):
        table = self.schema.get_table("test")

        self.node_session.sql("CREATE TABLE {0}.test(age INT, name "
                              "VARCHAR(50), gender CHAR(1))"
                              "".format(self.schema_name)).execute()

        result = table.insert("age", "name") \
            .values(21, 'Fred') \
            .values(28, 'Barney') \
            .values(42, 'Wilma') \
            .values(67, 'Betty').execute()

        result = table.select().execute()
        rows = result.fetch_all()
        self.assertEqual(4, len(rows))

    def test_update(self):
        table = self.schema.get_table("test")

        self.node_session.sql("CREATE TABLE {0}.test(age INT, name "
                              "VARCHAR(50), gender CHAR(1))"
                              "".format(self.schema_name)).execute()

        result = table.insert("age", "name") \
            .values(21, 'Fred') \
            .values(28, 'Barney') \
            .values(42, 'Wilma') \
            .values(67, 'Betty').execute()

        result = table.update().set("age", 25).where("age == 21").execute()
        self.assertEqual(1, result.get_affected_items_count())

    def test_delete(self):
        table_name = "table_test"
        self.node_session.sql(_CREATE_TEST_TABLE_QUERY.format(
            self.schema_name, table_name)).execute()
        self.node_session.sql(_INSERT_TEST_TABLE_QUERY.format(
            self.schema_name, table_name, "1")).execute()
        table = self.schema.get_table(table_name)
        self.assertTrue(table.exists_in_database())
        self.assertEqual(table.count(), 1)
        table.delete("id = 1").execute()
        self.assertEqual(table.count(), 0)
        self.schema.drop_table(table_name)

    def test_count(self):
        table_name = "table_test"
        self.node_session.sql(_CREATE_TEST_TABLE_QUERY.format(
            self.schema_name, table_name)).execute()
        self.node_session.sql(_INSERT_TEST_TABLE_QUERY.format(
            self.schema_name, table_name, "1")).execute()
        table = self.schema.get_table(table_name)
        self.assertTrue(table.exists_in_database())
        self.assertEqual(table.count(), 1)
        self.schema.drop_table(table_name)

    def test_results(self):
        table_name = "{0}.test".format(self.schema_name)

        self.node_session.sql("CREATE TABLE {0}(age INT, name VARCHAR(50))"
                              "".format(table_name)).execute()
        self.node_session.sql("INSERT INTO {0} VALUES (21, 'Fred')"
                              "".format(table_name)).execute()
        self.node_session.sql("INSERT INTO {0} VALUES (28, 'Barney')"
                              "".format(table_name)).execute()

        table = self.schema.get_table("test")
        result = table.select().execute()

        self.assertEqual("Fred", result.fetch_one()["name"])
        self.assertEqual("Barney", result.fetch_one()["name"])
        self.assertEqual(None, result.fetch_one())

    def test_multiple_resultsets(self):
        self.node_session.sql("CREATE PROCEDURE {0}.spProc() BEGIN SELECT 1; "
                              "SELECT 2; SELECT 'a'; END"
                              "".format(self.schema_name)).execute()

        result = self.node_session.sql(" CALL {0}.spProc"
                                       "".format(self.schema_name)).execute()
        rows = result.fetch_all()
        self.assertEqual(1, len(rows))
        self.assertEqual(1, rows[0][0])
        self.assertEqual(True, result.next_result())
        rows = result.fetch_all()
        self.assertEqual(1, len(rows))
        self.assertEqual(2, rows[0][0])
        self.assertEqual(True, result.next_result())
        rows = result.fetch_all()
        self.assertEqual(1, len(rows))
        self.assertEqual("a", rows[0][0])
        self.assertEqual(False, result.next_result())

    def test_auto_inc_value(self):
        table_name = "{0}.test".format(self.schema_name)

        self.node_session.sql(
            "CREATE TABLE {0}(id INT KEY AUTO_INCREMENT, name VARCHAR(50))"
            "".format(table_name)).execute()
        result = self.node_session.sql("INSERT INTO {0} VALUES (NULL, 'Fred')"
                                       "".format(table_name)).execute()
        self.assertEqual(1, result.get_autoincrement_value())
        table = self.schema.get_table("test")
        result2 = table.insert("id", "name").values(None, "Boo").execute()
        self.assertEqual(2, result2.get_autoincrement_value())

    def test_column_metadata(self):
        table_name = "{0}.test".format(self.schema_name)

        self.node_session.sql(
            "CREATE TABLE {0}(age INT, name VARCHAR(50), pic VARBINARY(100), "
            "config JSON, created DATE, active BIT)"
            "".format(table_name)).execute()
        self.node_session.sql(
            "INSERT INTO {0} VALUES (21, 'Fred', NULL, NULL, '2008-07-26', 0)"
            "".format(table_name)).execute()
        self.node_session.sql(
            "INSERT INTO {0} VALUES (28, 'Barney', NULL, NULL, '2012-03-12'"
            ", 0)".format(table_name)).execute()
        self.node_session.sql(
            "INSERT INTO {0} VALUES (42, 'Wilma', NULL, NULL, '1975-11-11', 1)"
            "".format(table_name)).execute()
        self.node_session.sql(
            "INSERT INTO {0} VALUES (67, 'Betty', NULL, NULL, '2015-06-21', 0)"
            "".format(table_name)).execute()

        table = self.schema.get_table("test")
        result = table.select().execute()
        result.fetch_all()
        col = result.columns[0]
        self.assertEqual("age", col.get_column_name())
        self.assertEqual("test", col.get_table_name())
        self.assertEqual(mysqlx.ColumnType.INT, col.get_type())

        col = result.columns[1]
        self.assertEqual("name", col.get_column_name())
        self.assertEqual("test", col.get_table_name())
        self.assertEqual(mysqlx.ColumnType.STRING, col.get_type())

        col = result.columns[2]
        self.assertEqual("pic", col.get_column_name())
        self.assertEqual("test", col.get_table_name())
        self.assertEqual("binary", col.get_collation_name())
        self.assertEqual("binary", col.get_character_set_name())
        self.assertEqual(mysqlx.ColumnType.BYTES, col.get_type())

        col = result.columns[3]
        self.assertEqual("config", col.get_column_name())
        self.assertEqual("test", col.get_table_name())
        self.assertEqual(mysqlx.ColumnType.JSON, col.get_type())

        col = result.columns[5]
        self.assertEqual("active", col.get_column_name())
        self.assertEqual("test", col.get_table_name())
        self.assertEqual(mysqlx.ColumnType.BIT, col.get_type())

    def test_is_view(self):
        table_name = "table_test"
        view_name = "view_test"
        self.node_session.sql(_CREATE_TEST_TABLE_QUERY.format(
            self.schema_name, table_name)).execute()
        self.node_session.sql(_INSERT_TEST_TABLE_QUERY.format(
            self.schema_name, table_name, "1")).execute()
        table = self.schema.get_table(table_name)
        self.assertFalse(table.is_view())

        self.node_session.sql(_CREATE_TEST_VIEW_QUERY.format(
            self.schema_name, view_name,
            self.schema_name, table_name)).execute()
        view = self.schema.get_table(view_name)
        self.assertTrue(view.is_view())
