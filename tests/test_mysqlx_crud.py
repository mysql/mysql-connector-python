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
        self.schema = self.session.get_schema(self.schema_name)

    def tearDown(self):
        self.session.close()
        self.node_session.close()

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

        # should get exception if using an invalid name
        self.assertRaises(mysqlx.ProgrammingError,
                          self.schema.create_collection, "")
        self.assertRaises(mysqlx.ProgrammingError,
                          self.schema.create_collection, None)

        self.schema.drop_collection(collection_name)

    def test_create_view(self):
        table_name = "table_test"
        view_name = "view_test"
        self.node_session.sql(_CREATE_TEST_TABLE_QUERY.format(
            self.schema_name, table_name)).execute()
        self.node_session.sql(_INSERT_TEST_TABLE_QUERY.format(
            self.schema_name, table_name, "1")).execute()

        defined_as = "SELECT id FROM {0}.{1}".format(self.schema_name,
                                                     table_name)
        view = self.schema.create_view(view_name) \
                          .defined_as(defined_as) \
                          .execute()
        self.assertEqual(view.get_name(), view_name)
        self.assertTrue(view.exists_in_database())
        self.assertTrue(view.is_view())

        # should get exception if reuse is false and it already exists
        self.assertRaises(mysqlx.ProgrammingError,
                          self.schema.create_collection, view_name,
                          False)

        # using replacing the existing view should work
        view = self.schema.create_view(view_name, True) \
                          .defined_as(defined_as) \
                          .execute()

        self.schema.drop_table(view_name)

        # using a non-updatable view
        defined_as = ("SELECT COLUMN_TYPE, COLUMN_COMMENT FROM "
                      "INFORMATION_SCHEMA.columns WHERE TABLE_NAME='{0}' "
                      "AND COLUMN_NAME='id'".format(table_name))
        view = self.schema.create_view(view_name, True) \
                          .defined_as(defined_as) \
                          .execute()

        self.schema.drop_table(table_name)
        self.schema.drop_table(view_name)

    def test_alter_view(self):
        table_name = "table_test"
        view_name = "view_test"

        self.node_session.sql(_CREATE_TEST_TABLE_QUERY.format(
            self.schema_name, table_name)).execute()
        for i in range(10):
            self.node_session.sql(_INSERT_TEST_TABLE_QUERY.format(
                self.schema_name, table_name, "{0}".format(i + 1))
            ).execute()

        defined_as = "SELECT id FROM {0}.{1}".format(self.schema_name,
                                                     table_name)
        view = self.schema.create_view(view_name) \
                          .defined_as(defined_as) \
                          .execute()
        self.assertEqual(10, view.count())

        defined_as = ("SELECT id FROM {0}.{1} WHERE id > 5"
                      "".format(self.schema_name, table_name))
        view = self.schema.alter_view(view_name) \
                          .defined_as(defined_as) \
                          .execute()
        self.assertEqual(5, view.count())

        # using a non-updatable view
        defined_as = ("SELECT COLUMN_TYPE, COLUMN_COMMENT FROM "
                      "INFORMATION_SCHEMA.columns WHERE TABLE_NAME='{0}' "
                      "AND COLUMN_NAME='id'".format(table_name))
        view = self.schema.alter_view(view_name) \
                          .defined_as(defined_as) \
                          .execute()

        self.schema.drop_table(table_name)
        self.schema.drop_table(view_name)

    def test_get_collection(self):
        collection_name = "collection_test"
        coll = self.schema.get_collection(collection_name)
        self.assertFalse(coll.exists_in_database())
        coll = self.schema.create_collection(collection_name)
        self.assertTrue(coll.exists_in_database())

        self.schema.drop_collection(collection_name)

    def test_get_view(self):
        table_name = "table_test"
        view_name = "view_test"
        view = self.schema.get_view(view_name)
        self.assertFalse(view.exists_in_database())

        self.node_session.sql(_CREATE_TEST_TABLE_QUERY.format(
            self.schema_name, table_name)).execute()

        defined_as = "SELECT id FROM {0}.{1}".format(self.schema_name,
                                                     table_name)
        view = self.schema.create_view(view_name) \
                          .defined_as(defined_as) \
                          .execute()
        self.assertTrue(view.exists_in_database())

        # raise a ProgrammingError if the view does not exists
        self.assertRaises(mysqlx.ProgrammingError,
                          self.schema.get_view, "nonexistent",
                          check_existence=True)

        self.schema.drop_table(table_name)
        self.schema.drop_view(view_name)

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

        self.schema.drop_collection("coll1")
        self.schema.drop_collection("coll2")
        self.schema.drop_collection("coll3")

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

        self.schema.drop_table("table1")
        self.schema.drop_table("table2")
        self.schema.drop_table("table3")
        self.schema.drop_table("view1")

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

    def test_create_table(self):
        table_a = 'test_language'
        table_b = 'test_film'
        table_c = 'test_lang'
        table_d = 'test_lang_a'
        table_e = 'test_lang_b'

        # Simple create table
        language = self.schema.create_table(table_a) \
            .add_column(mysqlx.ColumnDef('language_id', mysqlx.ColumnType.INT) \
                .primary().auto_increment().unsigned().not_null()) \
            .set_initial_auto_increment(12) \
            .set_comment("Table Comment test").execute()

        self.assertTrue(language.exists_in_database())
        self.assertEqual(12,
            self.node_session.sql(
                'SELECT AUTO_INCREMENT '
                'FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = "{0}" AND '
                'TABLE_NAME = "{1}"'.format(self.schema.name, table_a)) \
            .execute().fetch_all()[0][0])
        self.assertEqual("Table Comment test",
            self.node_session.sql(
                'SELECT TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES '
                'WHERE TABLE_SCHEMA = "{0}" '
                'AND TABLE_NAME = "{1}"'.format(self.schema.name, table_a)) \
            .execute().fetch_all()[0][0])

        # Create table with index and foreign keys
        film = self.schema.create_table(table_b) \
            .add_column(mysqlx.ColumnDef('film_id', mysqlx.ColumnType.INT) \
                .primary().auto_increment().unsigned().not_null()) \
            .add_column(mysqlx.ColumnDef('title', mysqlx.ColumnType.TEXT, 255) \
                .not_null()) \
            .add_column(mysqlx.ColumnDef('native_title',
                mysqlx.ColumnType.VARCHAR, 255).charset("utf8mb4")
                .collation("utf8mb4_general_ci")) \
            .add_column(mysqlx.ColumnDef('description',
                mysqlx.ColumnType.TEXT).binary()) \
            .add_column(mysqlx.ColumnDef('release_year',
                mysqlx.ColumnType.YEAR, 4)) \
            .add_column(mysqlx.ColumnDef('language_id', mysqlx.ColumnType.INT) \
                .unsigned().not_null().foreign_key(table_a, 'language_id')) \
            .add_column(mysqlx.ColumnDef('original_language_id',
                mysqlx.ColumnType.INT).unsigned())\
            .add_column(mysqlx.ColumnDef('rental_duration',
                mysqlx.ColumnType.INT).unsigned().not_null().set_default(3)) \
            .add_column(mysqlx.ColumnDef('rental_rate',
                mysqlx.ColumnType.DECIMAL, 4).decimals(2).not_null() \
                .set_default(4.99).unique_index()) \
            .add_column(mysqlx.ColumnDef('length', mysqlx.ColumnType.INT) \
                .unsigned()) \
            .add_column(mysqlx.ColumnDef('replacement_cost',
                mysqlx.ColumnType.DECIMAL, 5).decimals(2).not_null(). \
                set_default(19.99)) \
            .add_column(mysqlx.ColumnDef('rating', mysqlx.ColumnType.ENUM) \
                .values(['G', 'PG', 'PG-13', 'R', 'NC-17']).set_default('G')) \
            .add_column(mysqlx.ColumnDef('special_features',
                mysqlx.ColumnType.SET).values(['Trailers', 'Commentaries',
                'Deleted Scenes', 'Behind the Scenes'])) \
            .add_column(mysqlx.ColumnDef('last_update',
                mysqlx.ColumnType.TIMESTAMP)) \
            .add_index('idx_title', ['title(255)']) \
            .add_foreign_key('fk_film_language', mysqlx.ForeignKeyDef() \
                .fields(['language_id']).refers_to(table_a,
                ['language_id'])) \
            .add_foreign_key('fk_film_language_original',
                mysqlx.ForeignKeyDef().fields(['original_language_id']) \
                .refers_to(table_a,['language_id']).on_update('Cascade')) \
            .set_default_charset('latin2') \
            .set_default_collation('latin2_general_ci') \
            .execute()

        self.assertTrue(film.exists_in_database())
        self.assertEqual(1, len(self.node_session.sql('SHOW INDEXES FROM '
            '{0}.{1} WHERE COLUMN_NAME = "{2}" AND NOT NON_UNIQUE'.format(
            self.schema.name, table_b, 'rental_rate')).execute().fetch_all()))

        res = self.node_session.sql('SELECT CHARACTER_SET_NAME, COLLATION_NAME '
                'FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = "{0}" '
                'AND COLUMN_NAME = "{1}"'.format(table_b, 'native_title')) \
            .execute().fetch_all()
        self.assertEqual("utf8mb4", res[0]['CHARACTER_SET_NAME'])
        self.assertEqual("utf8mb4_general_ci", res[0]['COLLATION_NAME'])

        res = self.node_session.sql('SELECT CHARACTER_SET_NAME, COLLATION_NAME '
                'FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = "{0}" '
                'AND COLUMN_NAME = "{1}"'.format(table_b, 'title')) \
            .execute().fetch_all()
        self.assertEqual("latin2", res[0]['CHARACTER_SET_NAME'])
        self.assertEqual("latin2_general_ci", res[0]['COLLATION_NAME'])

        # Create table like another table
        lang = self.schema.create_table(table_c).like(table_a).execute()
        self.assertTrue(lang.exists_in_database())

        # Create table as Select Statement
        lang_a = self.schema.create_table(table_d) \
            .add_column(mysqlx.ColumnDef('language_id', mysqlx.ColumnType.INT) \
                .not_null().primary().auto_increment().unsigned()) \
            .as_select("SELECT language_id from {0}".format(table_a)).execute()
        self.assertTrue(lang_a.exists_in_database())

        select = language.select('language_id').where('language_id > 100')
        lang_b = self.schema.create_table(table_e) \
            .add_column(mysqlx.ColumnDef('language_id', mysqlx.ColumnType.INT) \
                .not_null().primary().auto_increment().unsigned()) \
            .as_select(select).execute()
        self.assertTrue(lang_b.exists_in_database())

        self.schema.drop_table(table_b)
        self.schema.drop_table(table_a)
        self.schema.drop_table(table_c)
        self.schema.drop_table(table_d)
        self.schema.drop_table(table_e)


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
        self.schema = self.session.get_schema(self.schema_name)

    def tearDown(self):
        self.session.close()
        self.node_session.close()

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

        self.schema.drop_collection(collection_name)

    def test_get_document_ids(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        result = collection.add({"name": "Fred", "age": 21}).execute()
        self.assertTrue(result.get_document_id() is not None)

        result = collection.add(
            {"name": "Fred", "age": 21},
            {"name": "Barney", "age": 45}).execute()
        self.assertEqual(2, len(result.get_document_ids()))

        self.schema.drop_collection(collection_name)

    def test_remove(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add({"name": "Fred", "age": 21}).execute()
        self.assertEqual(1, collection.count())
        result = collection.remove("age == 21").execute()
        self.assertEqual(1, result.get_affected_items_count())
        self.assertEqual(0, collection.count())

        self.schema.drop_collection(collection_name)

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

        # test flexible params
        result = collection.find("$.age > 28")\
                           .sort(["age DESC", "name ASC"]).execute()
        docs = result.fetch_all()
        self.assertEqual(2, len(docs))
        self.assertEqual(67, docs[0]["age"])

        # test flexible params
        result = collection.find().fields(["age"])\
                           .sort("age DESC").limit(2).execute()
        docs = result.fetch_all()
        self.assertEqual(2, len(docs))
        self.assertEqual(42, docs[1]["age"])
        self.assertEqual(1, len(docs[1].keys()))

        self.schema.drop_collection(collection_name)

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

        # test flexible params
        result = collection.modify().unset(["young"]).execute()
        self.assertEqual(1, result.get_affected_items_count())

        self.schema.drop_collection(collection_name)

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

        self.schema.drop_collection(collection_name)

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

        self.schema.drop_collection(collection_name)

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

        self.schema.drop_collection(collection_name)

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

        self.schema.drop_collection(collection_name)

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

        self.schema.drop_collection(collection_name)


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
        self.schema = self.session.get_schema(self.schema_name)

    def tearDown(self):
        self.session.close()
        self.node_session.close()

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

        # test flexible params
        result = table.select(['age', 'name']).sort("age DESC").execute()
        rows = result.fetch_all()
        self.assertEqual(4, len(rows))

        self.schema.drop_table("test")

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

        # test flexible params
        result = table.select().group_by(["gender"]).sort(["name DESC", "age ASC"]).execute()
        rows = result.fetch_all()
        self.assertEqual(2, len(rows))
        self.assertEqual(42, rows[0]["age"])
        self.assertEqual(21, rows[1]["age"])

        self.schema.drop_table("test")

    def test_insert(self):
        self.node_session.sql("CREATE TABLE {0}.test(age INT, name "
                              "VARCHAR(50), gender CHAR(1))"
                              "".format(self.schema_name)).execute()
        table = self.schema.get_table("test")

        result = table.insert("age", "name") \
            .values(21, 'Fred') \
            .values(28, 'Barney') \
            .values(42, 'Wilma') \
            .values(67, 'Betty').execute()

        result = table.select().execute()
        rows = result.fetch_all()
        self.assertEqual(4, len(rows))

        # test flexible params
        result = table.insert(["age", "name"]) \
            .values([35, 'Eddard']) \
            .values(9, 'Arya').execute()

        result = table.select().execute()
        rows = result.fetch_all()
        self.assertEqual(6, len(rows))

        self.schema.drop_table("test")

    def test_update(self):
        self.node_session.sql("CREATE TABLE {0}.test(age INT, name "
                              "VARCHAR(50), gender CHAR(1))"
                              "".format(self.schema_name)).execute()
        table = self.schema.get_table("test")

        result = table.insert("age", "name") \
            .values(21, 'Fred') \
            .values(28, 'Barney') \
            .values(42, 'Wilma') \
            .values(67, 'Betty').execute()

        result = table.update().set("age", 25).where("age == 21").execute()
        self.assertEqual(1, result.get_affected_items_count())

        self.schema.drop_table("test")

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

        self.schema.drop_table("test")

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

        self.node_session.sql("DROP PROCEDURE IF EXISTS {0}.spProc".format(self.schema_name)).execute()

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

        self.schema.drop_table("test")

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

        self.schema.drop_table("test")

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

        self.schema.drop_table(table_name)
        self.schema.drop_table(view_name)


@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 12), "XPlugin not compatible")
class MySQLxViewTests(tests.MySQLxTests):

    def setUp(self):
        self.connect_kwargs = tests.get_mysqlx_config()
        self.schema_name = self.connect_kwargs["schema"]
        self.table_name = "table_test"
        self.view_name = "view_test"
        try:
            self.session = mysqlx.get_session(self.connect_kwargs)
            self.node_session = mysqlx.get_node_session(self.connect_kwargs)
        except mysqlx.Error as err:
            self.fail("{0}".format(err))
        self.schema = self.session.get_schema(self.schema_name)

    def tearDown(self):
        self.schema.drop_table(self.table_name)
        self.schema.drop_view(self.view_name)
        self.session.close()
        self.node_session.close()

    def test_exists_in_database(self):
        view = self.schema.get_view(self.view_name)
        self.assertFalse(view.exists_in_database())
        self.node_session.sql(_CREATE_TEST_TABLE_QUERY.format(
            self.schema_name, self.table_name)).execute()
        defined_as = "SELECT id FROM {0}.{1}".format(self.schema_name,
                                                     self.table_name)
        view = self.schema.create_view(self.view_name) \
                          .defined_as(defined_as) \
                          .execute()
        self.assertTrue(view.exists_in_database())

    def test_select(self):
        table_name = "{0}.{1}".format(self.schema_name, self.table_name)

        self.node_session.sql("CREATE TABLE {0} (age INT, name VARCHAR(50))"
                              "".format(table_name)).execute()
        self.node_session.sql("INSERT INTO {0} VALUES (21, 'Fred')"
                              "".format(table_name)).execute()
        self.node_session.sql("INSERT INTO {0} VALUES (28, 'Barney')"
                              "".format(table_name)).execute()
        self.node_session.sql("INSERT INTO {0} VALUES (42, 'Wilma')"
                              "".format(table_name)).execute()
        self.node_session.sql("INSERT INTO {0} VALUES (67, 'Betty')"
                              "".format(table_name)).execute()

        defined_as = "SELECT age, name FROM {0}".format(table_name)
        view = self.schema.create_view(self.view_name) \
                          .defined_as(defined_as) \
                          .execute()
        result = view.select().sort("age DESC").execute()
        rows = result.fetch_all()
        self.assertEqual(4, len(rows))
        self.assertEqual(67, rows[0]["age"])

        result = view.select("age").where("age = 42").execute()
        self.assertEqual(1, len(result.columns))
        rows = result.fetch_all()
        self.assertEqual(1, len(rows))

        # test flexible params
        result = view.select(['age', 'name']).sort("age DESC").execute()
        rows = result.fetch_all()
        self.assertEqual(4, len(rows))

    def test_having(self):
        table_name = "{0}.{1}".format(self.schema_name, self.table_name)

        self.node_session.sql("CREATE TABLE {0} (age INT, name VARCHAR(50), "
                              "gender CHAR(1))".format(table_name)).execute()
        self.node_session.sql("INSERT INTO {0} VALUES (21, 'Fred', 'M')"
                              "".format(table_name)).execute()
        self.node_session.sql("INSERT INTO {0} VALUES (28, 'Barney', 'M')"
                              "".format(table_name)).execute()
        self.node_session.sql("INSERT INTO {0} VALUES (42, 'Wilma', 'F')"
                              "".format(table_name)).execute()
        self.node_session.sql("INSERT INTO {0} VALUES (67, 'Betty', 'F')"
                              "".format(table_name)).execute()

        defined_as = "SELECT age, name, gender FROM {0}".format(table_name)
        view = self.schema.create_view(self.view_name) \
                          .defined_as(defined_as) \
                          .execute()
        result = view.select().group_by("gender").sort("age ASC").execute()
        rows = result.fetch_all()
        self.assertEqual(2, len(rows))
        self.assertEqual(21, rows[0]["age"])
        self.assertEqual(42, rows[1]["age"])

        result = view.select().group_by("gender").having("gender = 'F'") \
                                                 .sort("age ASC").execute()
        rows = result.fetch_all()
        self.assertEqual(1, len(rows))
        self.assertEqual(42, rows[0]["age"])

        # test flexible params
        result = view.select().group_by(["gender"]) \
                              .sort(["name DESC", "age ASC"]).execute()
        rows = result.fetch_all()
        self.assertEqual(2, len(rows))
        self.assertEqual(42, rows[0]["age"])
        self.assertEqual(21, rows[1]["age"])

    def test_insert(self):
        table_name = "{0}.{1}".format(self.schema_name, self.table_name)

        self.node_session.sql("CREATE TABLE {0} (age INT, name VARCHAR(50), "
                              "gender CHAR(1))".format(table_name)).execute()
        defined_as = "SELECT age, name, gender FROM {0}".format(table_name)
        view = self.schema.create_view(self.view_name) \
                          .defined_as(defined_as) \
                          .execute()

        result = view.insert("age", "name").values(21, 'Fred') \
                                           .values(28, 'Barney') \
                                           .values(42, 'Wilma') \
                                           .values(67, 'Betty').execute()
        result = view.select().execute()
        rows = result.fetch_all()
        self.assertEqual(4, len(rows))

        # test flexible params
        result = view.insert(["age", "name"]).values([35, 'Eddard']) \
                                             .values(9, 'Arya').execute()
        result = view.select().execute()
        rows = result.fetch_all()
        self.assertEqual(6, len(rows))

    def test_update(self):
        table_name = "{0}.{1}".format(self.schema_name, self.table_name)

        self.node_session.sql("CREATE TABLE {0} (age INT, name VARCHAR(50), "
                              "gender CHAR(1))".format(table_name)).execute()
        defined_as = ("SELECT age, name, gender FROM {0}".format(table_name))
        view = self.schema.create_view(self.view_name) \
                          .defined_as(defined_as) \
                          .execute()

        result = view.insert("age", "name").values(21, 'Fred') \
                                           .values(28, 'Barney') \
                                           .values(42, 'Wilma') \
                                           .values(67, 'Betty').execute()
        result = view.update().set("age", 25).where("age == 21").execute()
        self.assertEqual(1, result.get_affected_items_count())
        self.schema.drop_table("test")

    def test_delete(self):
        self.node_session.sql(_CREATE_TEST_TABLE_QUERY.format(
            self.schema_name, self.table_name)).execute()
        self.node_session.sql(_INSERT_TEST_TABLE_QUERY.format(
            self.schema_name, self.table_name, "1")).execute()

        defined_as = "SELECT id FROM {0}.{1}".format(self.schema_name,
                                                     self.table_name)
        view = self.schema.create_view(self.view_name) \
                          .defined_as(defined_as) \
                          .execute()
        self.assertEqual(view.count(), 1)
        view.delete("id = 1").execute()
        self.assertEqual(view.count(), 0)

    def test_count(self):
        self.node_session.sql(_CREATE_TEST_TABLE_QUERY.format(
            self.schema_name, self.table_name)).execute()
        self.node_session.sql(_INSERT_TEST_TABLE_QUERY.format(
            self.schema_name, self.table_name, "1")).execute()

        defined_as = "SELECT id FROM {0}.{1}".format(self.schema_name,
                                                     self.table_name)
        view = self.schema.create_view(self.view_name) \
                          .defined_as(defined_as) \
                          .execute()
        self.assertEqual(view.count(), 1)

    def test_results(self):
        table_name = "{0}.{1}".format(self.schema_name, self.table_name)

        self.node_session.sql("CREATE TABLE {0} (age INT, name VARCHAR(50))"
                              "".format(table_name)).execute()
        self.node_session.sql("INSERT INTO {0} VALUES (21, 'Fred')"
                              "".format(table_name)).execute()
        self.node_session.sql("INSERT INTO {0} VALUES (28, 'Barney')"
                              "".format(table_name)).execute()

        defined_as = "SELECT age, name FROM {0}".format(table_name)
        view = self.schema.create_view(self.view_name) \
                          .defined_as(defined_as) \
                          .execute()
        result = view.select().execute()

        self.assertEqual("Fred", result.fetch_one()["name"])
        self.assertEqual("Barney", result.fetch_one()["name"])
        self.assertEqual(None, result.fetch_one())

    def test_auto_inc_value(self):
        table_name = "{0}.{1}".format(self.schema_name, self.table_name)

        self.node_session.sql("CREATE TABLE {0} (id INT KEY AUTO_INCREMENT, "
                              "name VARCHAR(50))".format(table_name)).execute()
        result = self.node_session.sql("INSERT INTO {0} VALUES (NULL, 'Fred')"
                                       "".format(table_name)).execute()
        self.assertEqual(1, result.get_autoincrement_value())

        defined_as = "SELECT id, name FROM {0}".format(table_name)
        view = self.schema.create_view(self.view_name) \
                          .defined_as(defined_as) \
                          .execute()
        result2 = view.insert("id", "name").values(None, "Boo").execute()
        self.assertEqual(2, result2.get_autoincrement_value())

    def test_column_metadata(self):
        table_name = "{0}.{1}".format(self.schema_name, self.table_name)

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

        defined_as = ("SELECT age, name, pic, config, created, active FROM {0}"
                      "".format(table_name))
        view = self.schema.create_view(self.view_name) \
                          .defined_as(defined_as) \
                          .execute()

        result = view.select().execute()
        result.fetch_all()
        col = result.columns[0]
        self.assertEqual("age", col.get_column_name())
        self.assertEqual(self.view_name, col.get_table_name())
        self.assertEqual(mysqlx.ColumnType.INT, col.get_type())

        col = result.columns[1]
        self.assertEqual("name", col.get_column_name())
        self.assertEqual(self.view_name, col.get_table_name())
        self.assertEqual(mysqlx.ColumnType.STRING, col.get_type())

        col = result.columns[2]
        self.assertEqual("pic", col.get_column_name())
        self.assertEqual(self.view_name, col.get_table_name())
        self.assertEqual("binary", col.get_collation_name())
        self.assertEqual("binary", col.get_character_set_name())
        self.assertEqual(mysqlx.ColumnType.BYTES, col.get_type())

        col = result.columns[3]
        self.assertEqual("config", col.get_column_name())
        self.assertEqual(self.view_name, col.get_table_name())
        self.assertEqual(mysqlx.ColumnType.JSON, col.get_type())

        col = result.columns[5]
        self.assertEqual("active", col.get_column_name())
        self.assertEqual(self.view_name, col.get_table_name())
        self.assertEqual(mysqlx.ColumnType.BIT, col.get_type())
