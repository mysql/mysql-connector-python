# -*- coding: utf-8 -*-

# Copyright (c) 2016, 2022, Oracle and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0, as
# published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an
# additional permission to link the program and your derivative works
# with the separately licensed software that they have included with
# MySQL.
#
# Without limiting anything contained in the foregoing, this file,
# which is part of MySQL Connector/Python, is also subject to the
# Universal FOSS Exception, version 1.0, a copy of which can be found at
# http://oss.oracle.com/licenses/universal-foss-exception.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

"""Unittests for mysqlx.crud
"""

import datetime
import decimal
import gc
import json
import logging
import sys
import threading
import time
import unittest

import mysqlx
import tests

LOGGER = logging.getLogger(tests.LOGGER_NAME)
ARCH_64BIT = sys.maxsize > 2**32 and sys.platform != "win32"

_CREATE_TEST_TABLE_QUERY = "CREATE TABLE `{0}`.`{1}` (id INT)"
_INSERT_TEST_TABLE_QUERY = "INSERT INTO `{0}`.`{1}` VALUES ({2})"
_CREATE_TEST_VIEW_QUERY = "CREATE VIEW `{0}`.`{1}` AS SELECT * FROM `{2}`.`{3}`"
_CREATE_VIEW_QUERY = "CREATE VIEW `{0}`.`{1}` AS {2}"
_DROP_TABLE_QUERY = "DROP TABLE IF EXISTS `{0}`.`{1}`"
_DROP_VIEW_QUERY = "DROP VIEW IF EXISTS `{0}`.`{1}`"
_SHOW_INDEXES_QUERY = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'"
_PREP_STMT_QUERY = (
    "SELECT p.sql_text, p.count_execute "
    "FROM performance_schema.prepared_statements_instances AS p "
    "JOIN performance_schema.threads AS t ON p.owner_thread_id = t.thread_id "
    "AND t.processlist_id = @@pseudo_thread_id"
)


def create_view(schema, view_name, defined_as):
    query = _CREATE_VIEW_QUERY.format(schema.name, view_name, defined_as)
    schema.get_session().sql(query).execute()
    return schema.get_view(view_name, True)


def drop_table(schema, table_name):
    query = _DROP_TABLE_QUERY.format(schema.name, table_name)
    schema.get_session().sql(query).execute()


def drop_view(schema, view_name):
    query = _DROP_VIEW_QUERY.format(schema.name, view_name)
    schema.get_session().sql(query).execute()


@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 14), "XPlugin not compatible")
class MySQLxDbDocTests(tests.MySQLxTests):
    def setUp(self):
        self.connect_kwargs = tests.get_mysqlx_config()
        self.schema_name = self.connect_kwargs["schema"]
        self.collection_name = "collection_test"
        try:
            self.session = mysqlx.get_session(self.connect_kwargs)
        except mysqlx.Error as err:
            self.fail("{0}".format(err))
        self.schema = self.session.get_schema(self.schema_name)
        self.collection = self.schema.create_collection(self.collection_name)

    def tearDown(self):
        self.schema.drop_collection(self.collection_name)
        self.session.close()

    def test_dbdoc_creation(self):
        doc_1 = mysqlx.DbDoc({"_id": "1", "name": "Fred", "age": 21})
        self.collection.add(doc_1).execute()
        self.assertEqual(1, self.collection.count())

        # Don't allow _id assignment
        self.assertRaises(mysqlx.ProgrammingError, doc_1.__setitem__, "_id", "1")

        doc_2 = {"_id": "2", "name": "Wilma", "age": 33}
        self.collection.add(doc_2).execute()
        self.assertEqual(2, self.collection.count())

        # Copying a DbDoc
        doc_3 = self.collection.find().execute().fetch_one()
        doc_4 = doc_3.copy("new_id")
        self.assertEqual(doc_4["_id"], "new_id")
        self.assertNotEqual(doc_3, doc_4)

        # Copying a DbDoc without _id
        doc_5 = mysqlx.DbDoc({"name": "Fred", "age": 21})
        doc_6 = doc_5.copy()


@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 14), "XPlugin not compatible")
class MySQLxSchemaTests(tests.MySQLxTests):
    def setUp(self):
        self.connect_kwargs = tests.get_mysqlx_config()
        self.schema_name = self.connect_kwargs["schema"]
        try:
            self.session = mysqlx.get_session(self.connect_kwargs)
        except mysqlx.Error as err:
            self.fail("{0}".format(err))
        self.schema = self.session.get_schema(self.schema_name)

    def tearDown(self):
        self.session.close()

    def test_exists_in_database(self):
        # Test with special chars
        schema_name_1 = "myschema%"
        schema_name_2 = "myschema_"
        schema_1 = self.session.get_schema(schema_name_1)
        if schema_1.exists_in_database():
            self.session.drop_schema(schema_name_1)
        schema_2 = self.session.get_schema(schema_name_2)
        if schema_2.exists_in_database():
            self.session.drop_schema(schema_name_2)
        schema_1 = self.session.create_schema(schema_name_1)
        self.assertTrue(schema_1.exists_in_database())
        schema_2 = self.session.create_schema(schema_name_2)
        self.assertTrue(schema_2.exists_in_database())
        self.session.drop_schema(schema_name_1)
        self.session.drop_schema(schema_name_2)

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
        self.assertRaises(
            mysqlx.ProgrammingError,
            self.schema.create_collection,
            collection_name,
            False,
        )

        # should get exception if using an invalid name
        self.assertRaises(mysqlx.ProgrammingError, self.schema.create_collection, "")
        self.assertRaises(mysqlx.ProgrammingError, self.schema.create_collection, None)

        self.schema.drop_collection(collection_name)

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

        self.session.sql(
            _CREATE_TEST_TABLE_QUERY.format(self.schema_name, table_name)
        ).execute()

        defined_as = "SELECT id FROM {0}.{1}".format(self.schema_name, table_name)
        view = create_view(self.schema, view_name, defined_as)
        self.assertTrue(view.exists_in_database())

        # raise a ProgrammingError if the view does not exists
        self.assertRaises(
            mysqlx.ProgrammingError,
            self.schema.get_view,
            "nonexistent",
            check_existence=True,
        )

        drop_table(self.schema, table_name)
        drop_view(self.schema, view_name)

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

        self.session.sql(
            _CREATE_TEST_TABLE_QUERY.format(self.schema_name, "table1")
        ).execute()
        self.session.sql(
            _CREATE_TEST_TABLE_QUERY.format(self.schema_name, "table2")
        ).execute()
        self.session.sql(
            _CREATE_TEST_TABLE_QUERY.format(self.schema_name, "table3")
        ).execute()
        self.session.sql(
            _CREATE_TEST_VIEW_QUERY.format(
                self.schema_name, "view1", self.schema_name, "table1"
            )
        ).execute()
        tables = self.schema.get_tables()
        self.assertEqual(4, len(tables), "Should have returned 4 objects")
        self.assertEqual("table1", tables[0].get_name())
        self.assertEqual("table2", tables[1].get_name())
        self.assertEqual("table3", tables[2].get_name())
        self.assertEqual("view1", tables[3].get_name())

        drop_table(self.schema, "table1")
        drop_table(self.schema, "table2")
        drop_table(self.schema, "table3")
        drop_view(self.schema, "view1")

    def test_drop_collection(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        self.schema.drop_collection(collection_name)
        self.assertFalse(collection.exists_in_database())

        # dropping an non-existing collection should succeed silently
        self.schema.drop_collection(collection_name)

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 19), "Schema validation unavailable.")
    def test_schema_validation(self):
        collection_name = "collection_test"
        json_schema = {
            "id": "http://json-schema.org/geo",
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Longitude and Latitude Values",
            "description": "A geographical coordinate",
            "required": ["latitude", "longitude"],
            "type": "object",
            "properties": {
                "latitude": {"type": "number", "minimum": -90, "maximum": 90},
                "longitude": {
                    "type": "number",
                    "minimum": -180,
                    "maximum": 180,
                },
            },
        }
        json_schema_string = json.dumps(json_schema)

        # Invalid validation options cases
        invalid_options = [
            "",
            -1,
            {},
            {"foo": "bar"},
            {"level": None, "schema": None},
            {"level": "off", "schema": True},
            {"level": "off", "schema": None},
            {"level": "on", "schema": json_schema},
            {"level": True, "schema": json_schema},
        ]

        # Test Schema.create_collection() validation options
        for validation in invalid_options:
            self.assertRaises(
                mysqlx.ProgrammingError,
                self.schema.create_collection,
                collection_name,
                validation=validation,
            )

        # Invalid option in validation
        self.assertRaises(
            mysqlx.ProgrammingError,
            self.schema.create_collection,
            collection_name,
            validation={
                "level": "strict",
                "schema": json_schema,
                "invalid": "option",
            },
        )

        # Test using JSON schema as dict
        coll = self.schema.create_collection(
            collection_name,
            validation={"level": "strict", "schema": json_schema},
        )

        # The latitude and longitude should be numbers
        self.assertRaises(
            mysqlx.OperationalError,
            coll.add({"latitude": "41.14961", "longitude": "-8.61099"}).execute,
        )
        coll.add({"latitude": 41.14961, "longitude": -8.61099}).execute()
        self.assertEqual(1, coll.count())

        self.schema.drop_collection(collection_name)

        # Test JSON schema as string
        coll = self.schema.create_collection(
            collection_name,
            validation={"level": "strict", "schema": json_schema_string},
        )

        self.assertRaises(
            mysqlx.OperationalError,
            coll.add({"latitude": "41.14961", "longitude": "-8.61099"}).execute,
        )
        coll.add({"latitude": 41.14961, "longitude": -8.61099}).execute()
        self.assertEqual(1, coll.count())

        # Test Schema.modify_collection() validation options
        for validation in invalid_options:
            self.assertRaises(
                mysqlx.ProgrammingError,
                self.schema.modify_collection,
                collection_name,
                validation=validation,
            )

        # Test Schema.modify_collection()
        coll.modify("TRUE").set("location", "Porto/Portugal").execute()
        json_schema["properties"]["location"] = {"type": "string"}
        json_schema["required"].append("location")
        self.schema.modify_collection(
            collection_name,
            validation={"level": "strict", "schema": json_schema},
        )

        # The 'location' property is required
        self.assertRaises(
            mysqlx.OperationalError,
            coll.add({"latitude": 41.14961, "longitude": -8.61099}).execute,
        )
        coll.add(
            {
                "location": "Porto/Portugal",
                "latitude": 41.14961,
                "longitude": -8.61099,
            }
        ).execute()
        self.assertEqual(2, coll.count())

        # Test using only 'level' option in Schema.modify_collectioa()
        self.schema.modify_collection(collection_name, validation={"level": "off"})

        # Test using only 'schema' option in Schema.modify_collection()
        self.schema.modify_collection(
            collection_name, validation={"schema": json_schema}
        )

        # Test validation without any information in Schema.modify_collection()
        self.schema.modify_collection(
            collection_name, validation={"schema": json_schema}
        )

        # Drop the collection
        self.schema.drop_collection(collection_name)

    @unittest.skipIf(
        tests.MYSQL_VERSION >= (8, 0, 19), "Schema validation is available."
    )
    def test_unsupported_schema_validation(self):
        collection_name = "collection_test"
        json_schema = {
            "id": "http://json-schema.org/geo",
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Longitude and Latitude Values",
            "description": "A geographical coordinate",
            "required": ["latitude", "longitude"],
            "type": "object",
            "properties": {
                "latitude": {"type": "number", "minimum": -90, "maximum": 90},
                "longitude": {
                    "type": "number",
                    "minimum": -180,
                    "maximum": 180,
                },
            },
        }

        # Test creating a schema-less collection on server < 8.0.19
        coll = self.schema.create_collection(collection_name)
        self.schema.drop_collection(collection_name)

        # Test creating a collection with validation on server < 8.0.19
        self.assertRaises(
            mysqlx.NotSupportedError,
            self.schema.create_collection,
            collection_name,
            validation={"level": "strict", "schema": json_schema},
        )

        # Test modifying a collection with validation on server < 8.0.19
        self.assertRaises(
            mysqlx.NotSupportedError,
            self.schema.modify_collection,
            collection_name,
            validation={"level": "strict", "schema": json_schema},
        )


@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 14), "XPlugin not compatible")
class MySQLxCollectionTests(tests.MySQLxTests):
    def setUp(self):
        self.connect_kwargs = tests.get_mysqlx_config()
        self.schema_name = self.connect_kwargs["schema"]
        try:
            self.session = mysqlx.get_session(self.connect_kwargs)
        except mysqlx.Error as err:
            self.fail("{0}".format(err))
        self.schema = self.session.get_schema(self.schema_name)

    def tearDown(self):
        self.session.close()

    def test_exists_in_database(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        self.assertTrue(collection.exists_in_database())
        self.schema.drop_collection(collection_name)

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 3), "Row locks unavailable.")
    def test_lock_shared(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add({"_id": "1", "name": "Fred", "age": 21}).execute()

        waiting = threading.Event()

        lock_a = threading.Lock()
        lock_b = threading.Lock()

        errors = []

        def client_a(lock_a, lock_b, waiting):
            sess1 = mysqlx.get_session(self.connect_kwargs)
            schema = sess1.get_schema(self.schema_name)
            collection = schema.get_collection(collection_name)

            sess1.start_transaction()
            result = collection.find("name = 'Fred'").lock_shared().execute()
            lock_a.release()
            lock_b.acquire()
            time.sleep(2)
            if waiting.is_set():
                errors.append("S-S lock test failure.")
                sess1.commit()
                return
            sess1.commit()

            sess1.start_transaction()
            result = collection.find("name = 'Fred'").lock_shared().execute()
            lock_b.release()
            lock_a.acquire()
            time.sleep(2)
            if not waiting.is_set():
                errors.append("S-X lock test failure.")
                sess1.commit()
                return
            sess1.commit()

        def client_b(lock_a, lock_b, waiting):
            sess1 = mysqlx.get_session(self.connect_kwargs)
            schema = sess1.get_schema(self.schema_name)
            collection = schema.get_collection(collection_name)

            lock_a.acquire()
            sess1.start_transaction()
            waiting.set()
            lock_b.release()
            result = collection.find("name = 'Fred'").lock_shared().execute()
            waiting.clear()

            sess1.commit()

            lock_b.acquire()
            sess1.start_transaction()
            waiting.set()
            lock_a.release()
            result = collection.find("name = 'Fred'").lock_exclusive().execute()
            waiting.clear()
            sess1.commit()

        client1 = threading.Thread(
            target=client_a,
            args=(
                lock_a,
                lock_b,
                waiting,
            ),
        )
        client2 = threading.Thread(
            target=client_b,
            args=(
                lock_a,
                lock_b,
                waiting,
            ),
        )

        lock_a.acquire()
        lock_b.acquire()

        client1.start()
        client2.start()

        client1.join()
        client2.join()

        self.schema.drop_collection(collection_name)
        if errors:
            self.fail(errors[0])

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 3), "Row locks unavailable.")
    def test_lock_exclusive(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add({"_id": "1", "name": "Fred", "age": 21}).execute()
        event = threading.Event()

        pause = threading.Event()
        locking = threading.Event()
        waiting = threading.Event()

        errors = []

        def client_a(pause, locking, waiting):
            sess1 = mysqlx.get_session(self.connect_kwargs)
            schema = sess1.get_schema(self.schema_name)
            collection = schema.get_collection(collection_name)

            sess1.start_transaction()
            result = collection.find("name = 'Fred'").lock_exclusive().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if not waiting.is_set():
                sess1.commit()
                errors.append("X-X lock test failure.")
                return
            sess1.commit()

            pause.set()

            sess1.start_transaction()
            result = collection.find("name = 'Fred'").lock_exclusive().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if not waiting.is_set():
                errors.append("X-S lock test failure.")
                sess1.commit()
                return
            sess1.commit()

        def client_b(pause, locking, waiting):
            sess1 = mysqlx.get_session(self.connect_kwargs)
            schema = sess1.get_schema(self.schema_name)
            collection = schema.get_collection(collection_name)

            if not locking.wait(2):
                return
            sess1.start_transaction()

            waiting.set()
            result = collection.find("name = 'Fred'").lock_exclusive().execute()
            waiting.clear()

            sess1.commit()

            if not pause.wait(2):
                return

            if not locking.wait(2):
                return
            sess1.start_transaction()
            waiting.set()
            result = collection.find("name = 'Fred'").lock_shared().execute()
            waiting.clear()
            sess1.commit()

        client1 = threading.Thread(
            target=client_a,
            args=(
                pause,
                locking,
                waiting,
            ),
        )
        client2 = threading.Thread(
            target=client_b,
            args=(
                pause,
                locking,
                waiting,
            ),
        )

        client1.start()
        client2.start()

        client1.join()
        client2.join()

        self.schema.drop_collection(collection_name)
        if errors:
            self.fail(errors[0])

    @unittest.skipIf(
        tests.MYSQL_VERSION > (8, 0, 4),
        "id field creation on server must not be available.",
    )
    def test_add_old_versions(self):
        """Tests error message when adding documents without an ids on old
        servers"""
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)

        coll_add = collection.add({"name": "Fred", "age": 21})
        self.assertRaises(mysqlx.errors.OperationalError, coll_add.execute)

        # Providing _id for each document must allow his insertion
        persons = [
            {
                "_id": "12345678901234567890123456789012",
                "name": "Dyno dog dinosaur",
                "age": 33,
            },
            {
                "_id": "12345678901234567890123456789013",
                "name": "Puss saber-toothed cat",
                "age": 42,
            },
        ]

        result = collection.add(persons).execute()

        self.assertEqual(2, result.get_affected_items_count(), "documents not inserted")

        # Empty list is expected here since the server did not generate the ids
        self.assertEqual(
            [], result.get_generated_ids(), "_id from user was overwritten"
        )

        self.schema.drop_collection(collection_name)

    def _test_lock_contention(self, lock_type_1, lock_type_2, lock_contention):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add({"name": "Fred", "age": 21}).execute()

        locking = threading.Event()
        waiting = threading.Event()

        errors = []

        def thread_a(locking, waiting):
            session = mysqlx.get_session(self.connect_kwargs)
            schema = session.get_schema(self.schema_name)
            collection = schema.get_collection(collection_name)

            session.start_transaction()
            result = collection.find("name = 'Fred'")
            if lock_type_1 == "S":
                result.lock_shared().execute()
            else:
                result.lock_exclusive().execute()

            locking.set()
            time.sleep(2)
            locking.clear()

            if not waiting.is_set():
                errors.append(
                    "{0}-{0} lock test failure.".format(lock_type_1, lock_type_2)
                )
                session.commit()
                return

            session.commit()

        def thread_b(locking, waiting):
            session = mysqlx.get_session(self.connect_kwargs)
            schema = session.get_schema(self.schema_name)
            collection = schema.get_collection(collection_name)

            if not locking.wait(2):
                errors.append(
                    "{0}-{0} lock test failure.".format(lock_type_1, lock_type_2)
                )
                session.commit()
                return

            session.start_transaction()
            if lock_type_2 == "S":
                result = collection.find("name = 'Fred'").lock_shared(lock_contention)
            else:
                result = collection.find("name = 'Fred'").lock_exclusive(
                    lock_contention
                )

            if lock_contention == mysqlx.LockContention.NOWAIT and (
                lock_type_1 == "X" or lock_type_2 == "X"
            ):
                self.assertRaises(mysqlx.OperationalError, result.execute)
                session.rollback()

            waiting.set()
            time.sleep(4)

            session.start_transaction()
            result.execute()
            session.commit()
            waiting.clear()

        client1 = threading.Thread(
            target=thread_a,
            args=(
                locking,
                waiting,
            ),
        )
        client2 = threading.Thread(
            target=thread_b,
            args=(
                locking,
                waiting,
            ),
        )

        client1.start()
        client2.start()

        client1.join()
        client2.join()

        self.schema.drop_collection(collection_name)

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 5), "Lock contention unavailable.")
    def test_lock_shared_with_nowait(self):
        self._test_lock_contention("S", "S", mysqlx.LockContention.NOWAIT)
        self._test_lock_contention("S", "X", mysqlx.LockContention.NOWAIT)

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 5), "Lock contention unavailable.")
    def test_lock_exclusive_with_nowait(self):
        self._test_lock_contention("X", "X", mysqlx.LockContention.NOWAIT)
        self._test_lock_contention("X", "S", mysqlx.LockContention.NOWAIT)

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 5), "Lock contention unavailable.")
    def test_lock_shared_with_skip_locked(self):
        self._test_lock_contention("S", "S", mysqlx.LockContention.SKIP_LOCKED)
        self._test_lock_contention("S", "X", mysqlx.LockContention.SKIP_LOCKED)

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 5), "Lock contention unavailable.")
    def test_lock_exclusive_with_skip_locker(self):
        self._test_lock_contention("X", "X", mysqlx.LockContention.SKIP_LOCKED)
        self._test_lock_contention("X", "S", mysqlx.LockContention.SKIP_LOCKED)

    def test_add(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        result = collection.add({"_id": 1, "name": "Fred", "age": 21}).execute()
        self.assertEqual(result.get_affected_items_count(), 1)
        self.assertEqual(1, collection.count())

        # Adding multiple dictionaries at once
        result = collection.add(
            {"_id": 2, "name": "Wilma", "age": 33},
            {"_id": 3, "name": "Barney", "age": 42},
        ).execute()
        self.assertEqual(result.get_affected_items_count(), 2)
        self.assertEqual(3, collection.count())

        # Adding JSON strings
        result = collection.add(
            '{"_id": 4, "name": "Bambam", "age": 8}',
            '{"_id": 5, "name": "Pebbles", "age": 8}',
        ).execute()
        self.assertEqual(result.get_affected_items_count(), 2)
        self.assertEqual(5, collection.count())

        # All strings should be considered literal, for expressions
        # mysqlx.expr() function must be used
        collection.add(
            {
                "_id": "6",
                "status": "Approved",
                "email": "Fred (fred@example.com)",
            },
            {
                "_id": "7",
                "status": "Rejected\n(ORA:Pending)",
                "email": "Barney (barney@example.com)",
            },
        ).execute()
        result = collection.find().execute()
        self.assertEqual(7, len(result.fetch_all()))

        # test unicode
        result = collection.add({"_id": "8", "age": 1, "name": "😀"}).execute()
        self.assertEqual(result.get_affected_items_count(), 1)
        self.assertEqual(8, collection.count())

        if tests.MYSQL_VERSION > (8, 0, 4):
            # Following test are only possible on servers with id generetion.
            # Ensure _id is created at the server side
            persons = [
                {"name": "Wilma", "age": 33},
                {"name": "Barney", "age": 42},
            ]
            result = collection.add(persons).execute()
            for person in persons:
                # Ensure no '_id' field was added locally.
                self.assertFalse("_id" in person)

            self.assertEqual(
                2,
                result.get_affected_items_count(),
                "Not all documents were inserted",
            )

            # Allow _id given from the user and server side generation
            persons = [
                {
                    "_id": "12345678901234567890123456789012",
                    "name": "Dyno",
                    "desc": "dog dinosaur",
                },
                {
                    "_id": "12345678901234567890123456789013",
                    "name": "Puss",
                    "desc": "saber-toothed cat",
                },
                # following doc does not have id field and must be
                # generated at the server side
                {"name": "hoppy", "desc": "hoppy kangaroo/dinosaur"},
            ]

            result = collection.add(persons).execute()

            self.assertEqual(
                3,
                result.get_affected_items_count(),
                "Not all documents were inserted",
            )

            # Only 1 `_id` was generated, 2 were given by us.
            self.assertEqual(
                1,
                len(result.get_generated_ids()),
                "Unexpected number of _id were generated.",
            )

            result = collection.find().execute()
            for row in result.fetch_all():
                self.assertTrue(
                    hasattr(row, "_id"),
                    "`_id` field could not be found in doc",
                )

        self.schema.drop_collection(collection_name)

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 2), "CONT_IN operator unavailable")
    def test_cont_in_operator(self):
        collection_name = "{0}.test".format(self.schema_name)
        collection = self.schema.create_collection(collection_name)
        collection.add(
            {
                "_id": "a6f4b93e1a264a108393524f29546a8c",
                "title": "AFRICAN EGG",
                "description": "A Fast-Paced Documentary of a Pastry Chef And a "
                "Dentist who must Pursue a Forensic Psychologist in "
                "The Gulf of Mexico",
                "releaseyear": 2006,
                "language": "English",
                "duration": 130,
                "rating": "G",
                "genre": "Science fiction",
                "actors": [
                    {
                        "name": "MILLA PECK",
                        "country": "Mexico",
                        "birthdate": "12 Jan 1984",
                    },
                    {
                        "name": "VAL BOLGER",
                        "country": "Botswana",
                        "birthdate": "26 Jul 1975",
                    },
                    {
                        "name": "SCARLETT BENING",
                        "country": "Syria",
                        "birthdate": "16 Mar 1978",
                    },
                ],
                "additionalinfo": {
                    "director": "Sharice Legaspi",
                    "writers": [
                        "Rusty Couturier",
                        "Angelic Orduno",
                        "Carin Postell",
                    ],
                    "productioncompanies": ["Qvodrill", "Indigoholdings"],
                },
            }
        ).execute()

        if tests.MYSQL_VERSION >= (8, 0, 17):
            # To comply with the SQL standard, IN returns NULL not only if the
            # expression on the left hand side is NULL, but also if no match
            # is found in the list and one of the expressions in the list is NULL.
            not_found_without_null = False if tests.MYSQL_VERSION < (8, 0, 22) else None
            not_found_with_null = None
            # Value false match result changed
            value_false_match_everything = False
        else:
            not_found_without_null = None
            not_found_with_null = True
            value_false_match_everything = True

        test_cases = [
            ("(1+5) in (1, 2, 3, 4, 5)", False),
            ("(1>5) in (true, false)", True),
            ("('a'>'b') in (true, false)", True),
            ("(1>5) in [true, false]", None),
            ("(1+5) in [1, 2, 3, 4, 5]", None),
            ("('a'>'b') in [true, false]", None),
            (
                "true IN [(1>5), !(false), (true || false), (false && true)]",
                True,
            ),
            (
                "true IN ((1>5), !(false), (true || false), (false && true))",
                True,
            ),
            ("{ 'name' : 'MILLA PECK' } IN actors", True),
            (
                '{"field":true} IN ("mystring", 124, myvar, othervar.jsonobj)',
                not_found_without_null,
            ),
            (
                "actor.name IN ['a name', null, (1<5-4), myvar.jsonobj.name]",
                None,
            ),
            ("!false && true IN [true]", True),
            ("1-5/2*2 > 3-2/1*2 IN [true, false]", None),
            ("true IN [1-5/2*2 > 3-2/1*2]", False),
            (
                "'African Egg' IN ('African Egg', 1, true, NULL, [0,1,2], "
                "{ 'title' : 'Atomic Firefighter' })",
                True,
            ),
            (
                "1 IN ('African Egg', 1, true, NULL, [0,1,2], "
                "{ 'title' : 'Atomic Firefighter' })",
                True,
            ),
            (
                "true IN ('African Egg', 1, false, NULL, [0,1,2], "
                "{ 'title' : 'Atomic Firefighter' })",
                not_found_with_null,
            ),
            (
                "false IN ('African Egg', 1, true, NULL, [0,1,2], "
                "{ 'title' : 'Atomic Firefighter' })",
                not_found_with_null,
            ),
            (
                "false IN ('African Egg', 1, true, 'No null', [0,1,2], "
                "{ 'title' : 'Atomic Firefighter' })",
                value_false_match_everything,
            ),
            (
                "[0,1,2] IN ('African Egg', 1, true, NULL, [0,1,2], "
                "{ 'title' : 'Atomic Firefighter' })",
                True,
            ),
            (
                "{ 'title' : 'Atomic Firefighter' } IN ('African Egg', 1, true, "
                "NULL, [0,1,2], { 'title' : 'Atomic Firefighter' })",
                True,
            ),
            (
                "title IN ('African Egg', 'The Witcher', 'Jurassic Perk')",
                False,
            ),
            ("releaseyear IN (2006, 2010, 2017)", True),
            ("'African Egg' in movietitle", None),
            ("0 NOT IN [1,2,3]", True),
            ("1 NOT IN [1,2,3]", False),
            ("'' IN title", False),
            ("title IN ('', ' ')", False),
            ("title IN ['', ' ']", False),
            (
                '["Rusty Couturier", "Angelic Orduno", "Carin Postell"] IN '
                "additionalinfo.writers",
                True,
            ),
            (
                '{ "name" : "MILLA PECK", "country" : "Mexico", '
                '"birthdate": "12 Jan 1984"} IN actors',
                True,
            ),
            ("releaseyear IN [2006, 2007, 2008]", True),
            ("true IN title", False),
            ("false IN genre", False),
            ("'Sharice Legaspi' IN additionalinfo.director", True),
            ("'Mexico' IN actors[*].country", True),
            ("'Angelic Orduno' IN additionalinfo.writers", True),
        ]

        for test in test_cases:
            try:
                result = (
                    collection.find()
                    .fields("{0} as res".format(test[0]))
                    .execute()
                    .fetch_one()
                )
            except:
                self.assertEqual(None, test[1])
            else:
                self.assertEqual(
                    result["res"],
                    test[1],
                    "For test case {} result was {}".format(test, result),
                )
        self.schema.drop_collection(collection_name)

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 17), "OVERLAPS operator unavailable")
    def test_overlaps_operator(self):
        collection_name = "{0}.test".format(self.schema_name)
        collection = self.schema.create_collection(collection_name)
        collection.add(
            {
                "_id": "a6f4b93e1a264a108393524f29546a8c",
                "title": "AFRICAN EGG",
                "description": "A Fast-Paced Documentary of a Pastry Chef And a "
                "Dentist who must Pursue a Forensic Psychologist in "
                "The Gulf of Mexico",
                "releaseyear": 2006,
                "language": "English",
                "duration": 130,
                "rating": "G",
                "genre": "Science fiction",
                "actors": [
                    {
                        "name": "MILLA PECK",
                        "country": "Mexico",
                        "birthdate": "12 Jan 1984",
                    },
                    {
                        "name": "VAL BOLGER",
                        "country": "Botswana",
                        "birthdate": "26 Jul 1975",
                    },
                    {
                        "name": "SCARLETT BENING",
                        "country": "Syria",
                        "birthdate": "16 Mar 1978",
                    },
                ],
                "additionalinfo": {
                    "director": "Sharice Legaspi",
                    "writers": [
                        "Rusty Couturier",
                        "Angelic Orduno",
                        "Carin Postell",
                    ],
                    "productioncompanies": ["Qvodrill", "Indigoholdings"],
                },
            }
        ).execute()

        test_cases = [
            ("(1+5) overlaps (1, 2, 3, 4, 5)", None),
            ("(1>5) overlaps (true, false)", None),
            ("('a'>'b') overlaps (true, false)", None),
            ("(1>5) overlaps [true, false]", None),
            ("[1>5] overlaps [true, false]", True),
            ("[(1+5)] overlaps [1, 2, 3, 4, 5]", False),
            ("[(1+4)] overlaps [1, 2, 3, 4, 5]", True),
            ("('a'>'b') overlaps [true, false]", None),
            (
                "true overlaps [(1>5), !(false), (true || false), (false && true)]",
                True,
            ),
            (
                "true overlaps ((1>5), !(false), (true || false), (false && true))",
                None,
            ),
            ("{ 'name' : 'MILLA PECK' } overlaps actors", False),
            (
                '{"field":true} overlaps ("mystring", 124, myvar, othervar.jsonobj)',
                None,
            ),
            (
                "actor.name overlaps ['a name', null, (1<5-4), myvar.jsonobj.name]",
                None,
            ),
            ("!false && true overlaps [true]", True),
            ("1-5/2*2 > 3-2/1*2 overlaps [true, false]", None),
            ("true IN [1-5/2*2 > 3-2/1*2]", False),
            (
                "'African Egg' overlaps ('African Egg', 1, true, NULL, [0,1,2], "
                "{ 'title' : 'Atomic Firefighter' })",
                None,
            ),
            (
                "1 overlaps ('African Egg', 1, true, NULL, [0,1,2], "
                "{ 'title' : 'Atomic Firefighter' })",
                None,
            ),
            (
                "true overlaps ('African Egg', 1, false, NULL, [0,1,2], "
                "{ 'title' : 'Atomic Firefighter' })",
                None,
            ),
            (
                "false overlaps ('African Egg', 1, true, NULL, [0,1,2], "
                "{ 'title' : 'Atomic Firefighter' })",
                None,
            ),
            (
                "false overlaps ('African Egg', 1, true, 'No null', [0,1,2], "
                "{ 'title' : 'Atomic Firefighter' })",
                None,
            ),
            (
                "[0,1,2] overlaps ('African Egg', 1, true, NULL, [0,1,2], "
                "{ 'title' : 'Atomic Firefighter' })",
                None,
            ),
            (
                "{ 'title' : 'Atomic Firefighter' } overlaps ('African Egg', 1, true, "
                "NULL, [0,1,2], { 'title' : 'Atomic Firefighter' })",
                None,
            ),
            (
                "title overlaps ('African Egg', 'The Witcher', 'Jurassic Perk')",
                None,
            ),
            ("releaseyear overlaps (2006, 2010, 2017)", None),
            ("'African overlaps' in movietitle", None),
            ("0 NOT overlaps [1,2,3]", True),
            ("1 NOT overlaps [1,2,3]", False),
            ("[0] NOT overlaps [1,2,3]", True),
            ("[1] NOT overlaps [1,2,3]", False),
            ("[!false && true] OVERLAPS [true]", True),
            ("[!false AND true] OVERLAPS [true]", True),
            ("[!false & true] OVERLAPS [true]", False),
            ("'' IN title", False),
            ("title overlaps ('', ' ')", None),
            ("title overlaps ['', ' ']", False),
            (
                '["Rusty Couturier", "Angelic Orduno", "Carin Postell"] IN '
                "additionalinfo.writers",
                True,
            ),
            (
                '{ "name" : "MILLA PECK", "country" : "Mexico", '
                '"birthdate": "12 Jan 1984"} IN actors',
                True,
            ),
            ("releaseyear IN [2006, 2007, 2008]", True),
            ("true overlaps title", False),
            ("false overlaps genre", False),
            ("'Sharice Legaspi' overlaps additionalinfo.director", True),
            ("'Mexico' overlaps actors[*].country", True),
            ("'Angelic Orduno' overlaps additionalinfo.writers", True),
            ("[([1,2] overlaps [1,2])] overlaps [false] invalid [true]", None),
            ("[([1] overlaps [2])] overlaps [3] invalid [true] as res", None),
            ("[] []", None),
            ("[] TRUE as res", None),
        ]

        for test in test_cases:
            try:
                result = (
                    collection.find()
                    .fields("{0} as res".format(test[0]))
                    .execute()
                    .fetch_one()
                )
            except:
                self.assertEqual(
                    None,
                    test[1],
                    "For test case {} exeption was not expected.".format(test),
                )
            else:
                self.assertEqual(
                    result["res"],
                    test[1],
                    "For test case {} result was {}".format(test, result),
                )
        self.schema.drop_collection(collection_name)

    def test_ilri_expressions(self):
        collection_name = "{0}.test".format(self.schema_name)
        collection = self.schema.create_collection(collection_name)

        collection.add(
            {"_id": "1", "name": "Fred", "age": 21},
            {"_id": "2", "name": "Barney", "age": 28},
            {"_id": "3", "name": "Wilma", "age": 42},
            {"_id": "4", "name": "Betty", "age": 67},
        ).execute()

        # is
        result = collection.find("$.key is null").execute()
        self.assertEqual(4, len(result.fetch_all()))

        # is_not
        result = collection.find("$.key is not null").execute()
        self.assertEqual(0, len(result.fetch_all()))

        # regexp
        result = collection.find("$.name regexp 'F.*'").execute()
        self.assertEqual(1, len(result.fetch_all()))

        # not_regexp
        result = collection.find("$.name not regexp 'F.*'").execute()
        self.assertEqual(3, len(result.fetch_all()))

        # like
        result = collection.find("$.name like 'F%'").execute()
        self.assertEqual(1, len(result.fetch_all()))

        # not_like
        result = collection.find("$.name not like 'F%'").execute()
        self.assertEqual(3, len(result.fetch_all()))

        # in
        result = collection.find("$.age in (21, 28)").execute()
        self.assertEqual(2, len(result.fetch_all()))

        # not_in
        result = collection.find("$.age not in (21, 28)").execute()
        self.assertEqual(2, len(result.fetch_all()))

        # between
        result = collection.find("$.age between 20 and 29").execute()
        self.assertEqual(2, len(result.fetch_all()))

        # between_not
        result = collection.find("$.age not between 20 and 29").execute()
        self.assertEqual(2, len(result.fetch_all()))

        self.schema.drop_collection(collection_name)

    def test_unary_operators(self):
        collection_name = "{0}.test".format(self.schema_name)
        collection = self.schema.create_collection(collection_name)

        collection.add(
            {"_id": "1", "name": "Fred", "age": 21},
            {"_id": "2", "name": "Barney", "age": 28},
            {"_id": "3", "name": "Wilma", "age": 42},
            {"_id": "4", "name": "Betty", "age": 67},
        ).execute()

        # sign_plus
        result = (
            collection.find("$.age == 21").fields("+($.age * -1) as test").execute()
        )
        self.assertEqual(-21, result.fetch_all()[0]["test"])

        # sign_minus
        result = collection.find("$.age == 21").fields("-$.age as test").execute()
        self.assertEqual(-21, result.fetch_all()[0]["test"])

        # !
        result = (
            collection.find("$.age == 21").fields("! ($.age == 21) as test").execute()
        )
        self.assertFalse(result.fetch_all()[0]["test"])

        # not
        result = (
            collection.find("$.age == 21").fields("not ($.age == 21) as test").execute()
        )
        self.assertFalse(result.fetch_all()[0]["test"])

        # ~
        result = collection.find("$.age == 21").fields("5 & ~1 as test").execute()
        self.assertEqual(4, result.fetch_all()[0]["test"])

        self.schema.drop_collection(collection_name)

    def test_interval_expressions(self):
        collection_name = "{0}.test".format(self.schema_name)
        collection = self.schema.create_collection(collection_name)

        collection.add(
            {
                "_id": "1",
                "adate": "2000-01-01",
                "adatetime": "2000-01-01 12:00:01",
            }
        ).execute()

        result = (
            collection.find()
            .fields(
                "$.adatetime + interval 1000000 "
                "microsecond = '2000-01-01 12:00:02'"
                " as test"
            )
            .execute()
        )
        self.assertTrue(result.fetch_all()[0]["test"])

        result = (
            collection.find()
            .fields("$.adatetime + interval 1 second = '2000-01-01 12:00:02' as test")
            .execute()
        )
        self.assertTrue(result.fetch_all()[0]["test"])

        result = (
            collection.find()
            .fields("$.adatetime + interval 2 minute = '2000-01-01 12:02:01' as test")
            .execute()
        )
        self.assertTrue(result.fetch_all()[0]["test"])

        result = (
            collection.find()
            .fields("$.adatetime + interval 4 hour = '2000-01-01 16:00:01' as test")
            .execute()
        )
        self.assertTrue(result.fetch_all()[0]["test"])

        result = (
            collection.find()
            .fields("$.adate + interval 10 day = '2000-01-11' as test")
            .execute()
        )
        self.assertTrue(result.fetch_all()[0]["test"])

        result = (
            collection.find()
            .fields("$.adate + interval 2 week = '2000-01-15' as test")
            .execute()
        )
        self.assertTrue(result.fetch_all()[0]["test"])

        result = (
            collection.find()
            .fields("$.adate - interval 2 month = '1999-11-01' as test")
            .execute()
        )
        self.assertTrue(result.fetch_all()[0]["test"])

        result = (
            collection.find()
            .fields("$.adate + interval 2 quarter = '2000-07-01' as test")
            .execute()
        )
        self.assertTrue(result.fetch_all()[0]["test"])

        result = (
            collection.find()
            .fields("$.adate - interval 1 year = '1999-01-01' as test")
            .execute()
        )
        self.assertTrue(result.fetch_all()[0]["test"])

        result = (
            collection.find()
            .fields(
                "$.adatetime + interval '3.1000000' "
                "second_microsecond = '2000-01-01 "
                "12:00:05' as test"
            )
            .execute()
        )
        self.assertTrue(result.fetch_all()[0]["test"])

        result = (
            collection.find()
            .fields(
                "$.adatetime + interval '1:1.1' "
                "minute_microsecond = "
                "'2000-01-01 12:01:02.100000' "
                "as test"
            )
            .execute()
        )
        self.assertTrue(result.fetch_all()[0]["test"])

        result = (
            collection.find()
            .fields(
                "$.adatetime + interval "
                "'1:1' minute_second "
                "= '2000-01-01 12:01:02'"
                " as test"
            )
            .execute()
        )
        self.assertTrue(result.fetch_all()[0]["test"])

        result = (
            collection.find()
            .fields(
                "$.adatetime + interval '1:1:1.1' "
                "hour_microsecond = "
                "'2000-01-01 13:01:02.100000'"
                " as test"
            )
            .execute()
        )
        self.assertTrue(result.fetch_all()[0]["test"])

        result = (
            collection.find()
            .fields(
                "$.adatetime + interval '1:1:1' "
                "hour_second = '2000-01-01 13:01:02'"
                " as test"
            )
            .execute()
        )
        self.assertTrue(result.fetch_all()[0]["test"])

        result = (
            collection.find()
            .fields(
                "$.adatetime + interval '1:1' "
                "hour_minute = '2000-01-01 13:01:01'"
                " as test"
            )
            .execute()
        )
        self.assertTrue(result.fetch_all()[0]["test"])

        result = (
            collection.find()
            .fields(
                "$.adatetime + interval "
                "'2 3:4:5.600' day_microsecond = "
                "'2000-01-03 15:04:06.600000'"
                " as test"
            )
            .execute()
        )
        self.assertTrue(result.fetch_all()[0]["test"])

        result = (
            collection.find()
            .fields(
                "$.adatetime + interval '2 3:4:5' "
                "day_second = '2000-01-03 15:04:06' "
                "as test"
            )
            .execute()
        )
        self.assertTrue(result.fetch_all()[0]["test"])

        result = (
            collection.find()
            .fields(
                "$.adatetime + interval '2 3:4' "
                "day_minute = '2000-01-03 15:04:01' "
                "as test"
            )
            .execute()
        )
        self.assertTrue(result.fetch_all()[0]["test"])

        result = (
            collection.find()
            .fields(
                "$.adatetime + interval '2 3' "
                "day_hour = '2000-01-03 15:00:01' "
                "as test"
            )
            .execute()
        )
        self.assertTrue(result.fetch_all()[0]["test"])

        result = (
            collection.find()
            .fields("$.adate + interval '2-3' year_month = '2002-04-01' as test")
            .execute()
        )
        self.assertTrue(result.fetch_all()[0]["test"])

        self.schema.drop_collection(collection_name)

    def test_bitwise_operators(self):
        collection_name = "{0}.test".format(self.schema_name)
        collection = self.schema.create_collection(collection_name)

        result = collection.add(
            {"_id": "1", "name": "Fred", "age": 21},
            {"_id": "2", "name": "Barney", "age": 28},
            {"_id": "3", "name": "Wilma", "age": 42},
            {"_id": "4", "name": "Betty", "age": 67},
        ).execute()

        # &
        result = collection.find("$.age = 21").fields("$.age & 1 as test").execute()
        self.assertEqual(1, result.fetch_all()[0]["test"])

        # |
        result = collection.find("$.age == 21").fields("0 | 1 as test").execute()
        self.assertEqual(1, result.fetch_all()[0]["test"])

        # ^
        result = collection.find("$.age = 21").fields("$.age ^ 1 as test").execute()
        self.assertEqual(20, result.fetch_all()[0]["test"])

        # <<
        result = collection.find("$.age == 21").fields("1 << 2 as test").execute()
        self.assertEqual(4, result.fetch_all()[0]["test"])

        # >>
        result = collection.find("$.age == 21").fields("4 >> 2 as test").execute()
        self.assertEqual(1, result.fetch_all()[0]["test"])

        self.schema.drop_collection(collection_name)

    def test_numeric_operators(self):
        collection_name = "{0}.test".format(self.schema_name)
        collection = self.schema.create_collection(collection_name)

        collection.add(
            {"_id": "1", "name": "Fred", "age": 21},
            {"_id": "2", "name": "Barney", "age": 28},
            {"_id": "3", "name": "Wilma", "age": 42},
            {"_id": "4", "name": "Betty", "age": 67},
        ).execute()

        # =
        result = collection.find("$.age = 21").execute()
        self.assertEqual(1, len(result.fetch_all()))

        # ==
        result = collection.find("$.age == 21").execute()
        self.assertEqual(1, len(result.fetch_all()))

        # &&
        result = collection.find("$.age == 21 && $.name == 'Fred'").execute()
        self.assertEqual(1, len(result.fetch_all()))

        # and
        result = collection.find("$.age == 21 and $.name == 'Fred'").execute()
        self.assertEqual(1, len(result.fetch_all()))

        # or
        result = collection.find("$.age == 21 or $.age == 42").execute()
        self.assertEqual(2, len(result.fetch_all()))

        # ||
        result = collection.find("$.age == 21 || $.age == 42").execute()
        self.assertEqual(2, len(result.fetch_all()))

        # xor
        result = collection.find().fields("$.age xor 1 as test").execute()
        docs = result.fetch_all()
        self.assertTrue(all([i["test"] is False for i in docs]))

        # !=
        result = collection.find("$.age != 21").execute()
        self.assertEqual(3, len(result.fetch_all()))

        # <>
        result = collection.find("$.age <> 21").execute()
        self.assertEqual(3, len(result.fetch_all()))

        # >
        result = collection.find("$.age > 28").execute()
        self.assertEqual(2, len(result.fetch_all()))

        # >=
        result = collection.find("$.age >= 28").execute()
        self.assertEqual(3, len(result.fetch_all()))

        # <
        result = collection.find("$.age < 28").execute()
        self.assertEqual(1, len(result.fetch_all()))

        # <=
        result = collection.find("$.age <= 28").execute()
        self.assertEqual(2, len(result.fetch_all()))

        # +
        result = collection.find("$.age == 21").fields("$.age + 10 as test").execute()
        self.assertEqual(31, result.fetch_all()[0]["test"])

        # -
        result = collection.find("$.age == 21").fields("$.age - 10 as test").execute()
        self.assertEqual(11, result.fetch_all()[0]["test"])

        # *
        result = collection.find("$.age == 21").fields("$.age * 10 as test").execute()
        self.assertEqual(210, result.fetch_all()[0]["test"])

        # /
        result = collection.find("$.age == 21").fields("$.age / 7 as test").execute()
        self.assertEqual(3, result.fetch_all()[0]["test"])

        # div
        result = collection.find("$.age == 21").fields("$.age div 7 as test").execute()
        self.assertEqual(3, result.fetch_all()[0]["test"])

        # %
        result = collection.find("$.age == 21").fields("$.age % 7 as test").execute()
        self.assertEqual(0, result.fetch_all()[0]["test"])

        self.schema.drop_collection(collection_name)

    @unittest.skipIf(
        tests.MYSQL_VERSION < (8, 0, 5),
        "id field creation on server side is required.",
    )
    def test_get_generated_ids(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        result = collection.add({"name": "Fred", "age": 21}).execute()
        self.assertTrue(result.get_generated_ids() is not None)

        result = collection.add(
            {"name": "Fred", "age": 21}, {"name": "Barney", "age": 45}
        ).execute()
        self.assertEqual(2, len(result.get_generated_ids()))

        self.schema.drop_collection(collection_name)

    def test_remove(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add(
            {"_id": "1", "name": "Fred", "age": 21},
            {"_id": "2", "name": "Barney", "age": 45},
            {"_id": "3", "name": "Wilma", "age": 42},
        ).execute()
        self.assertEqual(3, collection.count())
        result = collection.remove("age == 21").execute()
        self.assertEqual(1, result.get_affected_items_count())
        self.assertEqual(2, collection.count())

        # Collection.remove() is not allowed without a condition
        result = collection.remove(None)
        self.assertRaises(mysqlx.ProgrammingError, result.execute)
        result = collection.remove("")
        self.assertRaises(mysqlx.ProgrammingError, result.execute)
        self.assertRaises(mysqlx.ProgrammingError, collection.remove, " ")

        self.schema.drop_collection(collection_name)

    def _assert_flat_line(self, samples, tolerance):
        for sample in range(1, len(samples)):
            self.assertLessEqual(
                samples[sample] - tolerance,
                samples[sample - 1],
                "For sample {} Objects "
                "{} overpass the tolerance () from previews "
                "sample {}".format(
                    sample, samples[sample], tolerance, samples[sample - 1]
                ),
            )

    def _collect_samples(self, sample_size, funct, param):
        samples = [0] * sample_size
        for num in range(sample_size * 10):
            _ = funct(eval(param)).execute()
            if num % 10 == 0:
                samples[int(num / 10)] = len(gc.get_objects())
        return samples

    def test_memory_use_in_sequential_calls(self):
        "Tests the number of new open objects in sequential usage"
        collection_name = "{0}.test".format(self.schema_name)
        collection = self.schema.create_collection(collection_name)

        sample_size = 100
        param = '{"_id": "{}".format(num), "name": repr(num), "number": num}'
        add_samples = self._collect_samples(sample_size, collection.add, param)

        param = "'$.name == \"{}\"'.format(num)"
        find_samples = self._collect_samples(sample_size, collection.find, param)

        # The tolerance here is the number of new objects that can be created
        # on each sequential method invocation without exceed memory usage.
        tolerance = 12

        self._assert_flat_line(add_samples, tolerance)
        self._assert_flat_line(find_samples, tolerance)

        self.schema.drop_collection(collection_name)

    def test_bind(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add(
            {"_id": "1", "name": "Fred", "age": 21},
            {"_id": "2", "name": "Barney", "age": 28},
            {"_id": "3", "name": "Wilma", "age": 42},
            {"_id": "4", "name": "Betty", "age": 67},
        ).execute()

        # Empty bind should not be allowed
        find = collection.find("$.age == :age")
        self.assertRaises(mysqlx.ProgrammingError, find.bind)

        # Invalid arguments to bind
        find = collection.find("$.age == :age")
        self.assertRaises(mysqlx.ProgrammingError, find.bind, 21, 28, 42)

        # Bind with a dictionary
        result = collection.find("$.age == :age").bind({"age": 67}).execute()
        docs = result.fetch_all()
        self.assertEqual(1, len(docs))
        self.assertEqual("Betty", docs[0]["name"])

        # Bind with a JSON string
        result = collection.find("$.age == :age").bind('{"age": 42}').execute()
        docs = result.fetch_all()
        self.assertEqual(1, len(docs))
        self.assertEqual("Wilma", docs[0]["name"])

        result = collection.find("$.age == :age").bind("age", 28).execute()
        docs = result.fetch_all()
        self.assertEqual(1, len(docs))
        self.assertEqual("Barney", docs[0]["name"])

        self.schema.drop_collection(collection_name)

    def test_find(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add(
            {"_id": "1", "name": "Fred", "age": 21},
            {"_id": "2", "name": "Barney", "age": 28},
            {"_id": "3", "name": "Wilma", "age": 42},
            {"_id": "4", "name": "Betty", "age": 67},
        ).execute()
        result = collection.find("$.age == 67").execute()
        docs = result.fetch_all()
        self.assertEqual(1, len(docs))
        self.assertEqual("Betty", docs[0]["name"])

        result = collection.find("$.age > 28").sort("age DESC, name ASC").execute()
        docs = result.fetch_all()
        self.assertEqual(2, len(docs))
        self.assertEqual(67, docs[0]["age"])

        result = collection.find().fields("age").sort("age DESC").limit(2).execute()
        docs = result.fetch_all()
        self.assertEqual(2, len(docs))
        self.assertEqual(42, docs[1]["age"])
        self.assertEqual(1, len(docs[1].keys()))

        # test flexible params
        result = collection.find("$.age > 28").sort(["age DESC", "name ASC"]).execute()
        docs = result.fetch_all()
        self.assertEqual(2, len(docs))
        self.assertEqual(67, docs[0]["age"])

        # test flexible params
        result = collection.find().fields(["age"]).sort("age DESC").limit(2).execute()
        docs = result.fetch_all()
        self.assertEqual(2, len(docs))
        self.assertEqual(42, docs[1]["age"])
        self.assertEqual(1, len(docs[1].keys()))

        # test like operator
        result = collection.find("$.name like 'B%'").execute()
        docs = result.fetch_all()
        self.assertEqual(2, len(docs))

        # test aggregation functions without alias
        result = collection.find().fields("sum($.age)").execute()
        docs = result.fetch_all()
        self.assertTrue("sum($.age)" in docs[0].keys())
        self.assertEqual(158, docs[0]["sum($.age)"])

        # test operators without alias
        result = collection.find().fields("$.age + 100").execute()
        docs = result.fetch_all()
        self.assertTrue("$.age + 100" in docs[0].keys())

        # tests comma seperated fields
        result = collection.find("$.age = 21").fields("$.age, $.name").execute()
        docs = result.fetch_all()
        self.assertEqual("Fred", docs[0]["$.name"])

        # test limit and offset
        result = collection.find().fields("$.name").limit(2).offset(2).execute()
        docs = result.fetch_all()
        self.assertEqual(2, len(docs))
        self.assertEqual("Wilma", docs[0]["$.name"])
        self.assertEqual("Betty", docs[1]["$.name"])
        self.assertRaises(ValueError, collection.find().limit, -1)
        self.assertRaises(ValueError, collection.find().limit(1).offset, -1)

        # test unread result found
        find = collection.find()
        find.execute()
        find.execute()
        result = find.execute()
        docs = result.fetch_all()
        self.assertEqual(4, len(docs))

        self.schema.drop_collection(collection_name)

    def test_modify(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add(
            {"_id": "1", "name": "Fred", "age": 21},
            {"_id": "2", "name": "Barney", "age": 28},
            {"_id": "3", "name": "Wilma", "age": 42},
            {"_id": "4", "name": "Betty", "age": 67},
        ).execute()

        result = collection.modify("age < 67").set("young", True).execute()
        self.assertEqual(3, result.get_affected_items_count())
        doc = collection.find("name = 'Fred'").execute().fetch_all()[0]
        self.assertEqual(True, doc.young)

        result = collection.modify("age == 28").change("young", False).execute()
        self.assertEqual(1, result.get_affected_items_count())
        docs = collection.find("young = True").execute().fetch_all()
        self.assertEqual(2, len(docs))

        result = collection.modify("young == True").unset("young").execute()
        self.assertEqual(2, result.get_affected_items_count())
        docs = collection.find("young = True").execute().fetch_all()
        self.assertEqual(0, len(docs))

        # test flexible params
        result = collection.modify("TRUE").unset(["young"]).execute()
        self.assertEqual(1, result.get_affected_items_count())

        # Collection.modify() is not allowed without a condition
        result = collection.modify(None).unset(["young"])
        self.assertRaises(mysqlx.ProgrammingError, result.execute)
        result = collection.modify("").unset(["young"])
        self.assertRaises(mysqlx.ProgrammingError, result.execute)

        # test empty strings in collection fields
        for string in ("", " ", "  "):
            self.assertRaises(
                mysqlx.ProgrammingError,
                collection.modify("$._id == 1").set,
                string,
                {"name": "Maria", "age": 78},
            )
            self.assertRaises(
                mysqlx.ProgrammingError,
                collection.modify("$._id == 1").unset,
                string,
            )
            self.assertRaises(
                mysqlx.ProgrammingError,
                collection.modify("$._id == 1").change,
                string,
                {"name": "Maria", "age": 78},
            )
            self.assertRaises(
                mysqlx.ProgrammingError,
                collection.modify("$._id == 1").array_insert,
                string,
                [2, 3, 4],
            )
            self.assertRaises(
                mysqlx.ProgrammingError,
                collection.modify("$._id == 1").array_append,
                string,
                [2, 3, 4],
            )

        self.schema.drop_collection(collection_name)

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 4), "Unavailable")
    def test_modify_patch(self):
        collection_name = "collection_GOT"
        collection = self.schema.create_collection(collection_name)
        collection.add(
            {
                "_id": "1",
                "name": "Bran",
                "family_name": "Stark",
                "age": 18,
                "actors_bio": {"bd": "1999 April 9", "rn": "Isaac Hempstead"},
                "parents": ["Eddard Stark", "Catelyn Stark"],
            },
            {
                "_id": "2",
                "name": "Sansa",
                "family_name": "Stark",
                "age": 21,
                "actors_bio": {
                    "bd": "1996 February 21",
                    "rn": "Sophie Turner",
                },
                "parents": ["Eddard Stark", "Catelyn Stark"],
            },
            {
                "_id": "3",
                "name": "Arya",
                "family_name": "Stark",
                "age": 20,
                "actors_bio": {"bd": "1997 April 15", "rn": "Maisie Williams"},
                "parents": ["Eddard Stark", "Catelyn Stark"],
            },
            {
                "_id": "4",
                "name": "Jon",
                "family_name": "Snow",
                "age": 30,
                "actors_bio": {
                    "bd": "1986 December 26",
                    "rn": "Kit Harington",
                },
            },
            {
                "_id": "5",
                "name": "Daenerys",
                "family_name": "Targaryen",
                "age": 30,
                "actors_bio": {"bd": "1986 October 23", "rn": "Emilia Clarke"},
            },
            {
                "_id": "6",
                "name": "Margaery",
                "family_name": "Tyrell",
                "age": 35,
                "actors_bio": {
                    "bd": "1982 February 11",
                    "rn": "Natalie Dormer",
                },
            },
            {
                "_id": "7",
                "name": "Cersei",
                "family_name": "Lannister",
                "age": 44,
                "actors_bio": {"bd": "1973 October 3", "rn": "Lena Headey"},
                "parents": ["Tywin Lannister, Joanna Lannister"],
            },
            {
                "_id": "8",
                "name": "Tyrion",
                "family_name": "Lannister",
                "age": 48,
                "actors_bio": {"bd": "1969 June 11", "rn": "Peter Dinklage"},
                "parents": ["Tywin Lannister, Joanna Lannister"],
            },
        ).execute()

        # test with empty document
        result = collection.modify("TRUE").patch("{}").execute()
        self.assertEqual(0, result.get_affected_items_count())

        # Test addition of new attribute
        result = collection.modify("age <= 21").patch('{"status": "young"}').execute()
        self.assertEqual(3, result.get_affected_items_count())
        doc = collection.find("name = 'Bran'").execute().fetch_all()[0]
        self.assertEqual("young", doc.status)
        doc = collection.find("name = 'Sansa'").execute().fetch_all()[0]
        self.assertEqual("young", doc.status)
        doc = collection.find("name = 'Arya'").execute().fetch_all()[0]
        self.assertEqual("young", doc.status)

        result = collection.modify("age > 21").patch('{"status": "older"}').execute()
        self.assertEqual(5, result.get_affected_items_count())
        doc = collection.find("name = 'Jon'").execute().fetch_all()[0]
        self.assertEqual("older", doc.status)
        doc = collection.find("name = 'Cersei'").execute().fetch_all()[0]
        self.assertEqual("older", doc.status)
        doc = collection.find("name = 'Tyrion'").execute().fetch_all()[0]
        self.assertEqual("older", doc.status)
        doc = collection.find("name = 'Daenerys'").execute().fetch_all()[0]
        self.assertEqual("older", doc.status)
        doc = collection.find("name = 'Margaery'").execute().fetch_all()[0]
        self.assertEqual("older", doc.status)

        # Test addition of new attribute with array value
        result = (
            collection.modify('family_name == "Tyrell"')
            .patch({"parents": ["Mace Tyrell", "Alerie Tyrell"]})
            .execute()
        )
        self.assertEqual(1, result.get_affected_items_count())
        doc = collection.find("name = 'Margaery'").execute().fetch_all()[0]
        self.assertEqual(["Mace Tyrell", "Alerie Tyrell"], doc.parents)

        result = (
            collection.modify('name == "Jon"')
            .patch(
                '{"parents": ["Lyanna Stark and Rhaegar Targaryen"], ' '"bastard":null}'
            )
            .execute()
        )
        self.assertEqual(1, result.get_affected_items_count())
        doc = collection.find("name = 'Jon'").execute().fetch_all()[0]
        self.assertEqual(["Lyanna Stark and Rhaegar Targaryen"], doc.parents)

        # Test update of attribute with array value
        result = (
            collection.modify('name == "Jon"')
            .patch(
                '{"parents": ["Lyanna Stark", "Rhaegar Targaryen"], ' '"bastard":null}'
            )
            .execute()
        )
        self.assertEqual(1, result.get_affected_items_count())
        doc = collection.find("name = 'Jon'").execute().fetch_all()[0]
        self.assertEqual(["Lyanna Stark", "Rhaegar Targaryen"], doc.parents)

        # Test add and update of a nested attribute with doc value
        result = (
            collection.modify('name == "Daenerys"')
            .patch(
                """
        {"dragons":{"drogon": "dark grayish with red markings",
                    "Rhaegal": "green with bronze markings",
                    "Viserion": "creamy white, with gold markings"}}
                    """
            )
            .execute()
        )
        self.assertEqual(1, result.get_affected_items_count())
        doc = collection.find("name = 'Daenerys'").execute().fetch_all()[0]
        self.assertEqual(
            {
                "drogon": "dark grayish with red markings",
                "Rhaegal": "green with bronze markings",
                "Viserion": "creamy white, with gold markings",
            },
            doc.dragons,
        )

        # test remove attribute by seting it with null value.
        result = collection.modify("TRUE").patch('{"status": null}').execute()
        self.assertEqual(8, result.get_affected_items_count())

        # Test remove a nested attribute with doc value
        result = (
            collection.modify('name == "Daenerys"')
            .patch(
                {
                    "dragons": {
                        "drogon": "dark grayish with red markings",
                        "Rhaegal": "green with bronze markings",
                        "Viserion": None,
                    }
                }
            )
            .execute()
        )
        self.assertEqual(1, result.get_affected_items_count())
        doc = collection.find("name = 'Daenerys'").execute().fetch_all()[0]
        self.assertEqual(
            {
                "drogon": "dark grayish with red markings",
                "Rhaegal": "green with bronze markings",
            },
            doc.dragons,
        )

        # Test add new attribute using expression
        result = (
            collection.modify('name == "Daenerys"')
            .patch(mysqlx.expr('JSON_OBJECT("dragons", JSON_OBJECT("count", 3))'))
            .execute()
        )
        self.assertEqual(1, result.get_affected_items_count())
        doc = collection.find("name = 'Daenerys'").execute().fetch_all()[0]
        self.assertEqual(
            {
                "drogon": "dark grayish with red markings",
                "Rhaegal": "green with bronze markings",
                "count": 3,
            },
            doc.dragons,
        )

        # Test update attribute value using expression
        result = (
            collection.modify('name == "Daenerys"')
            .patch(
                mysqlx.expr(
                    'JSON_OBJECT("dragons",'
                    '    JSON_OBJECT("count", $.dragons.count - 1))'
                )
            )
            .execute()
        )
        self.assertEqual(1, result.get_affected_items_count())
        doc = collection.find("name = 'Daenerys'").execute().fetch_all()[0]
        self.assertEqual(
            {
                "drogon": "dark grayish with red markings",
                "Rhaegal": "green with bronze markings",
                "count": 2,
            },
            doc.dragons,
        )

        # Test update attribute value using expression without JSON functions
        result = (
            collection.modify("TRUE")
            .patch(
                mysqlx.expr(
                    '{"actors_bio": {"current": {"day_of_birth": CAST(SUBSTRING_INDEX('
                    '    $.actors_bio.bd, " ", - 1) AS DECIMAL)}}}'
                )
            )
            .execute()
        )
        self.assertEqual(8, result.get_affected_items_count())

        # Test update attribute value using mysqlx.expr
        result = (
            collection.modify("TRUE")
            .patch(
                {
                    "actors_bio": {
                        "current": {
                            "birth_age": mysqlx.expr(
                                'CAST(SUBSTRING_INDEX($.actors_bio.bd, " ", 1)'
                                " AS DECIMAL)"
                            )
                        }
                    }
                }
            )
            .execute()
        )
        self.assertEqual(8, result.get_affected_items_count())
        doc = (
            collection.find("actors_bio.rn = 'Maisie Williams'")
            .execute()
            .fetch_all()[0]
        )
        self.assertEqual(
            {
                "bd": "1997 April 15",
                "current": {"day_of_birth": 15, "birth_age": 1997},
                "rn": "Maisie Williams",
            },
            doc.actors_bio,
        )

        # Test update attribute value using mysqlx.expr extended without '()'
        result = (
            collection.modify("TRUE")
            .patch(
                {
                    "actors_bio": {
                        "current": {
                            "age": mysqlx.expr(
                                "CAST(Year(CURDATE()) - "
                                'SUBSTRING_INDEX($.actors_bio.bd, " ", 1) AS DECIMAL)'
                            )
                        }
                    }
                }
            )
            .execute()
        )
        self.assertEqual(8, result.get_affected_items_count())
        res = self.session.sql("select Year(CURDATE()) - 1997").execute()
        age = res.fetch_all()[0]["Year(CURDATE()) - 1997"]
        doc = (
            collection.find("actors_bio.rn = 'Maisie Williams'")
            .execute()
            .fetch_all()[0]
        )
        self.assertEqual(
            {
                "bd": "1997 April 15",
                "current": {"age": age, "day_of_birth": 15, "birth_age": 1997},
                "rn": "Maisie Williams",
            },
            doc.actors_bio,
        )

        # test use of year funtion.
        result = (
            collection.modify("TRUE")
            .patch(
                mysqlx.expr(
                    '{"actors_bio": {"current": {"last_update": Year(CURDATE())}}}'
                )
            )
            .execute()
        )
        self.assertEqual(8, result.get_affected_items_count())

        # Collection.modify() is not allowed without a condition
        result = collection.modify(None).patch('{"status":"alive"}')
        self.assertRaises(mysqlx.ProgrammingError, result.execute)
        result = collection.modify("").patch('{"status":"alive"}')
        self.assertRaises(mysqlx.ProgrammingError, result.execute)

        # Collection.modify().patch() is not allowed without a document
        result = collection.modify("TRUE").patch("")
        self.assertRaises(mysqlx.OperationalError, result.execute)
        result = collection.modify("TRUE").patch(None)
        self.assertRaises(mysqlx.OperationalError, result.execute)

        # Collection.modify().patch() must fail is parameter is other
        # than DBdoc, dict or str.
        self.assertRaises(
            mysqlx.ProgrammingError, collection.modify("TRUE").patch, {"a_set"}
        )

        self.schema.drop_collection(collection_name)

    @unittest.skipIf(
        tests.MYSQL_VERSION < (8, 0, 3), "Root level updates not supported"
    )
    def test_replace_one(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add(
            {"_id": "1", "name": "Fred", "age": 21},
            {"_id": "2", "name": "Barney", "age": 28},
            {"_id": "3", "name": "Wilma", "age": 42},
            {"_id": "4", "name": "Betty", "age": 67},
        ).execute()

        result = collection.find("age = 21").execute().fetch_one()
        self.assertEqual("Fred", result["name"])
        result["name"] = "George"
        collection.replace_one(result["_id"], result)

        result = collection.find("age = 21").execute().fetch_one()
        self.assertEqual("George", result["name"])

        doc = {"_id": "5", "name": "Fred", "age": 21}
        self.assertRaises(mysqlx.ProgrammingError, collection.replace_one, "1", doc)
        doc = mysqlx.DbDoc({"_id": "5", "name": "Fred", "age": 21})
        self.assertRaises(mysqlx.ProgrammingError, collection.replace_one, "1", doc)

        self.schema.drop_collection(collection_name)

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 2), "Upsert not supported")
    def test_add_or_replace_one(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add(
            {"_id": "1", "name": "Fred", "age": 21},
            {"_id": "2", "name": "Barney", "age": 28},
            {"_id": "3", "name": "Wilma", "age": 42},
            {"_id": "4", "name": "Betty", "age": 67},
        ).execute()

        result = collection.find("age = 21").execute().fetch_one()
        self.assertEqual("Fred", result["name"])
        result["name"] = "George"
        collection.add_or_replace_one(result["_id"], result)

        result = collection.find("age = 21").execute().fetch_one()
        self.assertEqual("George", result["name"])

        result = collection.find("_id = 'new_id'").execute().fetch_all()
        self.assertEqual(0, len(result))
        upsert = {"name": "Melissandre", "age": 99999}
        collection.add_or_replace_one("new_id", upsert)
        result = collection.find("age = 99999").execute().fetch_one()
        self.assertEqual("Melissandre", result["name"])
        self.assertEqual("new_id", result["_id"])

        doc = {"_id": "5", "name": "Fred", "age": 21}
        self.assertRaises(
            mysqlx.ProgrammingError, collection.add_or_replace_one, "1", doc
        )
        doc = mysqlx.DbDoc({"_id": "5", "name": "Fred", "age": 21})
        self.assertRaises(
            mysqlx.ProgrammingError, collection.add_or_replace_one, "1", doc
        )

        self.schema.drop_collection(collection_name)

    def test_get_one(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add(
            {"_id": "1", "name": "Fred", "age": 21},
            {"_id": "2", "name": "Barney", "age": 28},
            {"_id": "3", "name": "Wilma", "age": 42},
            {"_id": "4", "name": "Betty", "age": 67},
        ).execute()

        result = collection.find("name = 'Fred'").execute().fetch_one()
        result = collection.get_one(result["_id"])
        self.assertEqual("Fred", result["name"])

        self.schema.drop_collection(collection_name)

    def test_remove_one(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add(
            {"_id": "1", "name": "Fred", "age": 21},
            {"_id": "2", "name": "Barney", "age": 28},
            {"_id": "3", "name": "Wilma", "age": 42},
            {"_id": "4", "name": "Betty", "age": 67},
        ).execute()

        result = collection.find("name = 'Fred'").execute().fetch_one()
        result = collection.remove_one(result["_id"])
        result = collection.find("name = 'Fred'").execute().fetch_all()
        self.assertEqual(0, len(result))

        self.schema.drop_collection(collection_name)

    def test_results(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add(
            {"_id": "1", "name": "Fred", "age": 21},
            {"_id": "2", "name": "Barney", "age": 28},
            {"_id": "3", "name": "Wilma", "age": 42},
            {"_id": "4", "name": "Betty", "age": 67},
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

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 4), "Dev API change")
    def test_create_index(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)

        # Create index with single field
        index_name = "age_idx"
        result = collection.create_index(
            index_name,
            {
                "fields": [{"field": "$.age", "type": "INT", "required": True}],
                "unique": True,
            },
        )
        # Unique indexes are not supported
        self.assertRaises(mysqlx.NotSupportedError, result.execute)

        collection.create_index(
            index_name,
            {
                "fields": [{"field": "$.age", "type": "INT", "required": True}],
                "unique": False,
            },
        ).execute()

        result = self.session.sql(
            _SHOW_INDEXES_QUERY.format(self.schema_name, collection_name, index_name)
        ).execute()
        rows = result.fetch_all()
        self.assertEqual(1, len(rows))

        # Create index with multiple fields
        index_name = "streets_idx"
        collection.create_index(
            index_name,
            {
                "fields": [
                    {
                        "field": "$.street",
                        "type": "TEXT(15)",
                        "required": True,
                    },
                    {
                        "field": "$.cross_street",
                        "type": "TEXT(15)",
                        "required": True,
                    },
                ],
                "unique": False,
            },
        ).execute()

        result = self.session.sql(
            _SHOW_INDEXES_QUERY.format(self.schema_name, collection_name, index_name)
        ).execute()
        rows = result.fetch_all()
        self.assertEqual(2, len(rows))

        # Create index using a geojson datatype
        index_name = "geo_idx"
        collection.create_index(
            index_name,
            {
                "fields": [
                    {
                        "field": "$.myGeoJsonField",
                        "type": "GEOJSON",
                        "required": True,
                        "options": 2,
                        "srid": 4326,
                    }
                ],
                "unique": False,
                "type": "SPATIAL",
            },
        ).execute()

        result = self.session.sql(
            _SHOW_INDEXES_QUERY.format(self.schema_name, collection_name, index_name)
        ).execute()
        rows = result.fetch_all()
        self.assertEqual(1, len(rows))

        # Create an index on document fields which contain arrays
        index_name = "emails_idx"
        index_desc = {
            "fields": [{"field": "$.emails", "type": "CHAR(128)", "array": True}]
        }
        collection.create_index(index_name, index_desc).execute()

        result = self.session.sql(
            _SHOW_INDEXES_QUERY.format(self.schema_name, collection_name, index_name)
        ).execute()
        rows = result.fetch_all()
        self.assertEqual(1, len(rows))

        # Error conditions
        # Index name can not be None
        index_name = None
        index_desc = {
            "fields": [{"field": "$.myField", "type": "TEXT(10)"}],
            "unique": False,
            "type": "INDEX",
        }
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.ProgrammingError, create_index.execute)

        # Index name can not be invalid identifier
        index_name = "!invalid"
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.ProgrammingError, create_index.execute)

        index_name = "invalid()"
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.ProgrammingError, create_index.execute)

        index_name = "01invalid"
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.ProgrammingError, create_index.execute)

        # index descriptor wrong format
        # Required "fields" is missing
        index_name = "myIndex"
        index_desc = {
            "fields1": [{"field": "$.myField", "type": "TEXT(10)"}],
            "unique": False,
            "type": "INDEX",
        }
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.ProgrammingError, create_index.execute)

        index_desc = {
            "field": [{"field": "$.myField", "type": "TEXT(10)"}],
            "unique": False,
            "type": "INDEX",
        }
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.ProgrammingError, create_index.execute)

        # index type with invalid type
        index_desc = {
            "field": [{"field": "$.myField", "type": "TEXT(10)"}],
            "unique": False,
            "type": "Invalid",
        }
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.ProgrammingError, create_index.execute)

        # index description contains aditional fields
        index_desc = {
            "field": [{"field": "$.myField", "type": "TEXT(10)"}],
            "unique": False,
            "other": "value",
        }
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.ProgrammingError, create_index.execute)

        # Inner "field" value is not a list
        index_desc = {"fields": "$.myField", "unique": False, "type": "INDEX"}
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.ProgrammingError, create_index.execute)

        # Required inner "field" is missing
        index_desc = {"fields": [{}], "unique": False, "type": "INDEX"}
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.ProgrammingError, create_index.execute)

        # Required inner "field" is misstyped
        index_desc = {
            "fields": [{"field1": "$.myField", "type": "TEXT(10)"}],
            "unique": False,
            "type": "INDEX",
        }
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.ProgrammingError, create_index.execute)

        # Required inner "field" is misstyped
        index_desc = {
            "fields": [{"01field1": "$.myField", "type": "TEXT(10)"}],
            "unique": False,
            "type": "INDEX",
        }
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.ProgrammingError, create_index.execute)

        # Required inner "field.type" is missing
        index_desc = {
            "fields": [{"field": "$.myField"}],
            "unique": False,
            "type": "INDEX",
        }
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.ProgrammingError, create_index.execute)

        # Required inner "field.type" is invalid
        index_desc = {
            "fields": [{"field": "$.myField", "type": "invalid"}],
            "unique": False,
            "type": "INDEX",
        }
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.OperationalError, create_index.execute)

        # By current Server limitations, "unique" can ont be True
        index_desc = {
            "fields": [{"field": "$.myField", "type": "TEXT(10)"}],
            "unique": True,
            "type": "INDEX",
        }
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.NotSupportedError, create_index.execute)

        # index specifiying the 'collation' option for non TEXT data type
        index_desc = {
            "fields": [
                {
                    "field": "$.myField",
                    "type": "int",
                    "collation": "utf8_general_ci",
                }
            ],
            "type": "INDEX",
        }
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.ProgrammingError, create_index.execute)

        # member description contains aditional fields
        index_desc = {
            "fields": [{"field": "$.myField", "type": "int", "additional": "field"}],
            "type": "INDEX",
        }
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.ProgrammingError, create_index.execute)

        # index type SPATIAL requires inner required field to be True
        index_name = "geotrap"
        index_desc = {
            "fields": [
                {"field": "$.intField", "type": "INT", "required": True},
                {"field": "$.floatField", "type": "FLOAT", "required": True},
                {"field": "$.dateField", "type": "DATE"},
                {
                    "field": "$.geoField",
                    "type": "GEOJSON",
                    "required": False,
                    "options": 2,
                    "srid": 4326,
                },
            ],
            "type": "SPATIAL",
        }
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.ProgrammingError, create_index.execute)

        # inner field type GEOJSON requires index type set to SPATIAL
        index_desc = {
            "fields": [
                {"field": "$.intField", "type": "INT", "required": True},
                {"field": "$.floatField", "type": "FLOAT", "required": True},
                {"field": "$.dateField", "type": "DATE"},
                {
                    "field": "$.geoField",
                    "type": "GEOJSON",
                    "required": False,
                    "options": 2,
                    "srid": 4326,
                },
            ],
            "type": "SPATIAL",
        }
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.ProgrammingError, create_index.execute)

        # "srid" fields  can be present only if "type" is set to "GEOJSON"
        index_desc = {
            "fields": [
                {
                    "field": "$.NogeoField",
                    "type": "int",
                    "required": True,
                    "srid": 4326,
                }
            ],
            "type": "SPATIAL",
        }
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.ProgrammingError, create_index.execute)

        # "options" fields  can be present only if "type" is set to "GEOJSON"
        index_desc = {
            "fields": [
                {
                    "field": "$.NogeoField",
                    "type": "int",
                    "required": True,
                    "options": 2,
                }
            ],
            "type": "SPATIAL",
        }
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(mysqlx.ProgrammingError, create_index.execute)

        # "required" fields must be Boolean
        index_name = "age_idx"
        index_desc = {
            "fields": [{"field": "$.age", "type": "INT", "required": "True"}],
            "unique": False,
        }
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(TypeError, create_index.execute)

        # "array" fields must be Boolean
        index_name = "emails_idx"
        index_desc = {
            "fields": [{"field": "$.emails", "type": "CHAR(128)", "array": "True"}]
        }
        create_index = collection.create_index(index_name, index_desc)
        self.assertRaises(TypeError, create_index.execute)

        # check error message for wrong collation
        err_msg = (
            "The 'collation' member can only be used when field type is set to '{}'"
        )
        index_name = "age_idx"
        index_desc = {
            "fields": [
                {
                    "field": "$.age",
                    "type": "INT",
                    "collation": "utf8mb4_0900_ai_ci",
                }
            ],
            "type": "INDEX",
        }
        create_index = collection.create_index(index_name, index_desc)
        try:
            create_index.execute()
        except mysqlx.ProgrammingError as err:
            self.assertEqual(err.msg, err_msg.format("INT"))

        index_name = "emails_idx"
        index_desc = {
            "fields": [
                {
                    "field": "$.emails",
                    "type": "CHAR(128)",
                    "collation": "utf8mb4_0900_ai_ci",
                }
            ],
            "type": "INDEX",
        }
        create_index = collection.create_index(index_name, index_desc)
        try:
            create_index.execute()
        except mysqlx.ProgrammingError as err:
            self.assertEqual(err.msg, err_msg.format("CHAR(128)"))

        self.schema.drop_collection(collection_name)

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 4), "Dev API change")
    def test_drop_index(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)

        index_name = "age_idx"
        collection.create_index(
            index_name,
            {
                "fields": [{"field": "$.age", "type": "INT", "required": True}],
                "unique": False,
            },
        ).execute()

        show_indexes_sql = (
            "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'"
            "".format(self.schema_name, collection_name, index_name)
        )

        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(1, len(rows))

        collection.drop_index(index_name)
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(0, len(rows))

        # dropping an non-existing index should succeed silently
        collection.drop_index(index_name)

        self.schema.drop_collection(collection_name)

    def test_parameter_binding(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add(
            {"_id": "1", "name": "Fred", "age": 21},
            {"_id": "2", "name": "Barney", "age": 28},
            {"_id": "3", "name": "Wilma", "age": 42},
            {"_id": "4", "name": "Betty", "age": 67},
        ).execute()
        result = collection.find("age == :age").bind("age", 67).execute()
        docs = result.fetch_all()
        self.assertEqual(1, len(docs))
        self.assertEqual("Betty", docs[0]["name"])

        result = (
            collection.find("$.age = :age")
            .bind('{"age": 42}')
            .sort("age DESC, name ASC")
            .execute()
        )
        docs = result.fetch_all()
        self.assertEqual(1, len(docs))
        self.assertEqual("Wilma", docs[0]["name"])

        # The number of bind parameters and placeholders do not match
        self.assertRaises(
            mysqlx.ProgrammingError,
            collection.find("$.age = ? and $.name = ?").bind,
            42,
        )

        # Binding anonymous parameters are not allowed in crud operations
        self.assertRaises(
            mysqlx.ProgrammingError, collection.find("$.age = ?").bind, 42
        )
        self.assertRaises(
            mysqlx.ProgrammingError, collection.find("$.name = ?").bind, "Fred"
        )
        self.schema.drop_collection(collection_name)

    def test_unicode_parameter_binding(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add(
            {"_id": "1", "name": "José", "age": 21},
            {"_id": "2", "name": "João", "age": 28},
            {"_id": "3", "name": "Célia", "age": 42},
        ).execute()
        result = collection.find("name == :name").bind("name", "José").execute()
        docs = result.fetch_all()
        self.assertEqual(1, len(docs))
        self.assertEqual("José", docs[0]["name"])

        result = collection.find("$.name = :name").bind('{"name": "João"}').execute()
        docs = result.fetch_all()
        self.assertEqual(1, len(docs))
        self.assertEqual("João", docs[0]["name"])

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

        # Test binding
        modify = collection.modify("$._id == :id").array_insert("$.cards[0]", 0)
        modify.bind("id", 1).execute()
        doc = collection.get_one(1)
        self.assertEqual([0], doc["cards"])

        modify.bind("id", 2).execute()
        doc = collection.get_one(2)
        self.assertEqual([0, 1, 2, 3, 4], doc["cards"])

        modify.bind("id", 3).execute()
        doc = collection.get_one(3)
        self.assertEqual([0], doc["cards"])

        self.schema.drop_collection(collection_name)

    def test_array_append(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add(
            {"_id": 1, "name": "Fred", "cards": [1]},
            {"_id": 2, "name": "Barney", "cards": [1, 2, 4]},
            {"_id": 3, "name": "Wilma", "cards": [1, 2]},
            {"_id": 4, "name": "Betty", "cards": []},
        ).execute()
        collection.modify("$._id == 2").array_append("$.cards[1]", 3).execute()
        docs = collection.find("$._id == 2").execute().fetch_all()
        self.assertEqual([1, [2, 3], 4], docs[0]["cards"])

        # Test binding
        modify = collection.modify("$._id == :id").array_append("$.cards[0]", 5)
        modify.bind("id", 1).execute()
        doc = collection.get_one(1)
        self.assertEqual([[1, 5]], doc["cards"])

        modify.bind("id", 2).execute()
        doc = collection.get_one(2)
        self.assertEqual([[1, 5], [2, 3], 4], doc["cards"])

        modify.bind("id", 3).execute()
        doc = collection.get_one(3)
        self.assertEqual([[1, 5], 2], doc["cards"])

        self.schema.drop_collection(collection_name)

    def test_count(self):
        collection_name = "collection_test"
        collection = self.schema.create_collection(collection_name)
        collection.add(
            {"_id": "1", "name": "Fred", "age": 21},
            {"_id": "2", "name": "Barney", "age": 28},
            {"_id": "3", "name": "Wilma", "age": 42},
            {"_id": "4", "name": "Betty", "age": 67},
        ).execute()
        self.assertEqual(4, collection.count())
        self.schema.drop_collection(collection_name)
        self.assertRaises(mysqlx.OperationalError, collection.count)

    @unittest.skipIf(
        tests.MYSQL_VERSION < (8, 0, 14), "Prepared statements not supported"
    )
    def test_prepared_statements(self):
        session = mysqlx.get_session(self.connect_kwargs)
        schema = session.get_schema(self.schema_name)
        expected_stmt_attrs = (
            lambda stmt, changed, prepared, repeated, exec_counter: stmt.changed
            == changed
            and stmt.prepared == prepared
            and stmt.repeated == repeated
            and stmt.exec_counter == exec_counter
        )

        collection_name = "prepared_collection_test"
        collection = schema.create_collection(collection_name)
        collection.add(
            {"_id": "1", "name": "Fred", "age": 21},
            {"_id": "2", "name": "Barney", "age": 28},
            {"_id": "3", "name": "Wilma", "age": 42},
            {"_id": "4", "name": "Betty", "age": 67},
            {"_id": "5", "name": "Bob", "age": 75},
        ).execute()

        # FindStatement

        find = collection.find("$.age == :age")
        self.assertTrue(expected_stmt_attrs(find, True, False, False, 0))

        # On the first call should: Crud::Find (without prepared statement)
        find.bind("age", 21).execute().fetch_all()
        self.assertTrue(expected_stmt_attrs(find, False, False, False, 1))

        # On the second call should: Prepare::Prepare + Prepare::Execute
        find.bind("age", 28).execute().fetch_all()
        self.assertTrue(expected_stmt_attrs(find, False, True, True, 2))

        # On subsequent calls should: Prepare::Execute
        find.bind("age", 42).execute().fetch_all()
        self.assertTrue(expected_stmt_attrs(find, False, True, True, 3))

        row = session.sql(_PREP_STMT_QUERY).execute().fetch_all()[0]
        expected_sql_text = (
            "SELECT doc FROM `{}`.`{}` "
            "WHERE (JSON_EXTRACT(doc,'$.age') = ?)"
            "".format(self.schema_name, collection_name)
        )
        self.assertEqual(row[0], expected_sql_text)
        self.assertEqual(row[1], find.exec_counter - 1)

        # Using sort() should deallocate the prepared statement:
        # Prepare::Deallocate + Crud::Find
        find.bind("age", 21).sort("age").execute().fetch_all()
        self.assertTrue(expected_stmt_attrs(find, False, False, False, 1))

        # On the second call should: Prepare::Prepare + Prepare::Execute
        find.bind("age", 42).execute().fetch_all()
        self.assertTrue(expected_stmt_attrs(find, False, True, True, 2))

        # The previous statement should be closed since it had no limit/offset
        # Prepare::Deallocate + Prepare::Prepare + Prepare::Execute
        find.bind("age", 67).limit(1).offset(0).execute().fetch_all()
        self.assertTrue(expected_stmt_attrs(find, False, True, False, 1))

        # On the second call should: Prepare::Execute
        find.bind("age", 75).limit(1).offset(0).execute().fetch_all()
        self.assertTrue(expected_stmt_attrs(find, False, True, True, 2))

        row = session.sql(_PREP_STMT_QUERY).execute().fetch_all()[0]
        expected_sql_text = (
            "SELECT doc FROM `{}`.`{}` "
            "WHERE (JSON_EXTRACT(doc,'$.age') = ?) "
            "ORDER BY JSON_EXTRACT(doc,'$.age') LIMIT ?, ?"
            "".format(self.schema_name, collection_name)
        )
        self.assertEqual(row[0], expected_sql_text)
        self.assertEqual(row[1], find.exec_counter)

        # ModifyStatement

        modify = collection.modify("$._id == :id").set("age", 18)
        self.assertTrue(expected_stmt_attrs(modify, True, False, False, 0))

        # On the first call should: Crud::Modify (without prepared statement)
        res = modify.bind("id", "1").execute()
        self.assertTrue(expected_stmt_attrs(modify, False, False, False, 1))

        # On the second call should: Prepare::Prepare + Prepare::Execute
        res = modify.bind("id", "2").execute()
        self.assertTrue(expected_stmt_attrs(modify, False, True, True, 2))

        row = session.sql(_PREP_STMT_QUERY).execute().fetch_all()[1]
        expected_sql_text = (
            "UPDATE `{}`.`{}` "
            "SET doc=JSON_SET(JSON_SET(doc,'$.age',18),"
            "'$._id',JSON_EXTRACT(`doc`,'$._id')) "
            "WHERE (JSON_UNQUOTE(JSON_EXTRACT(doc,'$._id')) = ?)"
            "".format(self.schema_name, collection_name)
        )
        self.assertEqual(row[0], expected_sql_text)
        self.assertEqual(row[1], modify.exec_counter - 1)

        # Using set() should deallocate the prepared statement:
        # Prepare::Deallocate + Crud::Modify
        res = modify.bind("id", "3").set("age", 92).execute()
        self.assertTrue(expected_stmt_attrs(modify, False, False, False, 1))

        # On the second call should: Prepare::Prepare + Prepare::Execute
        res = modify.bind("id", "4").execute()
        self.assertTrue(expected_stmt_attrs(modify, False, True, True, 2))

        row = session.sql(_PREP_STMT_QUERY).execute().fetch_all()[1]
        expected_sql_text = (
            "UPDATE `{}`.`{}` "
            "SET doc=JSON_SET(JSON_SET(doc,'$.age',92),'$._id'"
            ",JSON_EXTRACT(`doc`,'$._id')) "
            "WHERE (JSON_UNQUOTE(JSON_EXTRACT(doc,'$._id')) = ?)"
            "".format(self.schema_name, collection_name)
        )
        self.assertEqual(row[0], expected_sql_text)
        self.assertEqual(row[1], modify.exec_counter - 1)

        # RemoveStatement

        remove = collection.remove("$._id == :id").limit(2)
        self.assertTrue(expected_stmt_attrs(remove, True, False, False, 0))

        # On the first call should: Crud::Remove (without prepared statement)
        remove.bind("id", "1").execute()
        self.assertTrue(expected_stmt_attrs(remove, False, False, False, 1))

        # On the second call should: Prepare::Prepare + Prepare::Execute
        remove.bind("id", "2").execute()
        self.assertTrue(expected_stmt_attrs(remove, False, True, True, 2))

        # On subsequent calls should: Prepare::Execute
        remove.bind("id", "3").execute()
        self.assertTrue(expected_stmt_attrs(remove, False, True, True, 3))

        row = session.sql(_PREP_STMT_QUERY).execute().fetch_all()[2]
        expected_sql_text = (
            "DELETE FROM `{}`.`{}` "
            "WHERE (JSON_UNQUOTE(JSON_EXTRACT(doc,'$._id')) = ?) LIMIT ?"
            "".format(self.schema_name, collection_name)
        )
        self.assertEqual(row[0], expected_sql_text)
        self.assertEqual(row[1], remove.exec_counter - 1)

        # Using sort() should deallocate the prepared statement:
        # Prepare::Deallocate + Crud::Remove
        remove.bind("id", "3").sort("_id ASC").execute()
        self.assertTrue(expected_stmt_attrs(remove, False, False, False, 1))

        # On the second call should: Prepare::Prepare + Prepare::Execute
        remove.bind("id", "4").execute()
        self.assertTrue(expected_stmt_attrs(remove, False, True, True, 2))

        # On subsequent calls should: Prepare::Execute
        remove.bind("id", "5").execute()
        self.assertTrue(expected_stmt_attrs(remove, False, True, True, 3))

        row = session.sql(_PREP_STMT_QUERY).execute().fetch_all()[2]
        expected_sql_text = (
            "DELETE FROM `{}`.`{}` "
            "WHERE (JSON_UNQUOTE(JSON_EXTRACT(doc,'$._id')) = ?) "
            "ORDER BY JSON_EXTRACT(doc,'$._id') LIMIT ?"
            "".format(self.schema_name, collection_name)
        )
        self.assertEqual(row[0], expected_sql_text)
        self.assertEqual(row[1], remove.exec_counter - 1)

        schema.drop_collection(collection_name)
        session.close()

    @unittest.skipIf(
        tests.MYSQL_VERSION < (8, 0, 14), "Prepared statements not supported"
    )
    def test_prepared_statements_find_by_pk(self):
        expected_stmt_attrs = (
            lambda stmt, changed, prepared, repeated, exec_counter: stmt.changed
            == changed
            and stmt.prepared == prepared
            and stmt.repeated == repeated
            and stmt.exec_counter == exec_counter
        )

        session = mysqlx.get_session(self.connect_kwargs)
        schema = session.get_schema(self.schema_name)

        collection_name = "prepared_collection_test_by_pk"
        collection = schema.get_collection(collection_name)
        if collection.exists_in_database():
            schema.drop_collection("mycoll")
        collection = schema.create_collection(collection_name)

        add_stmt = collection.add(
            {"name": "Fred", "age": 21},
            {"name": "Barney", "age": 28},
            {"name": "Wilma", "age": 42},
        ).execute()

        ids = add_stmt.get_generated_ids()

        # Find by primary key
        find_stmt = collection.find("_id = :id").sort("age")
        self.assertTrue(expected_stmt_attrs(find_stmt, True, False, False, 0))

        # On the first call should: Crud::Find (without prepared statement)
        doc = find_stmt.bind("id", ids[0]).execute().fetch_one()
        self.assertEqual(doc["name"], "Fred")
        self.assertTrue(expected_stmt_attrs(find_stmt, False, False, False, 1))

        # On the second call should: Prepare::Prepare + Prepare::Execute
        doc = find_stmt.bind("id", ids[1]).execute().fetch_one()
        self.assertEqual(doc["name"], "Barney")
        self.assertTrue(expected_stmt_attrs(find_stmt, False, True, True, 2))

        # On subsequent calls should: Prepare::Execute
        doc = find_stmt.bind("id", ids[2]).execute().fetch_one()
        self.assertEqual(doc["name"], "Wilma")
        self.assertTrue(expected_stmt_attrs(find_stmt, False, True, True, 3))

        schema.drop_collection(collection_name)
        session.close()


@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 14), "XPlugin not compatible")
class MySQLxTableTests(tests.MySQLxTests):
    def setUp(self):
        self.connect_kwargs = tests.get_mysqlx_config()
        self.schema_name = self.connect_kwargs["schema"]
        try:
            self.session = mysqlx.get_session(self.connect_kwargs)
        except mysqlx.Error as err:
            self.fail("{0}".format(err))
        self.schema = self.session.get_schema(self.schema_name)

    def tearDown(self):
        self.session.close()

    def test_exists_in_database(self):
        table_name = "table_test"
        drop_table(self.schema, table_name)
        try:
            sql = _CREATE_TEST_TABLE_QUERY.format(self.schema_name, table_name)
            self.session.sql(sql).execute()
        except mysqlx.Error as err:
            LOGGER.info("{0}".format(err))
        table = self.schema.get_table(table_name)
        self.assertTrue(table.exists_in_database())
        drop_table(self.schema, table_name)

    def test_select(self):
        table_name = "{0}.test".format(self.schema_name)
        drop_table(self.schema, table_name)

        self.session.sql(
            "CREATE TABLE {0}(age INT, name VARCHAR(50))".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (21, 'Fred')".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (28, 'Barney')".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (42, 'Wilma')".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (67, 'Betty')".format(table_name)
        ).execute()

        table = self.schema.get_table("test")

        # test fetch_one()
        result = table.select("age", "name").where("age = 21").execute()
        row = result.fetch_one()
        self.assertEqual(repr(row), repr([21, "Fred"]))

        result = table.select().order_by("age DESC").execute()
        rows = result.fetch_all()
        self.assertEqual(4, len(rows))
        self.assertEqual(67, rows[0]["age"])
        self.assertEqual(
            repr(rows),
            repr([[67, "Betty"], [42, "Wilma"], [28, "Barney"], [21, "Fred"]]),
        )

        result = table.select("age").where("age = 42").execute()
        self.assertEqual(1, len(result.columns))
        rows = result.fetch_all()
        self.assertEqual(1, len(rows))

        # test flexible params
        result = table.select(["age", "name"]).order_by("age DESC").execute()
        rows = result.fetch_all()
        self.assertEqual(4, len(rows))

        # test like operator
        result = table.select().where("name like 'B%'").execute()
        rows = result.fetch_all()
        self.assertEqual(2, len(rows))

        # test aggregation functions
        result = table.select("sum(age)").execute()
        rows = result.fetch_all()
        self.assertTrue("sum(age)" == result.columns[0].get_column_name())
        self.assertEqual(158, rows[0]["sum(age)"])

        # test operators without alias
        result = table.select("age + 100").execute()
        rows = result.fetch_all()
        self.assertTrue("age + 100" == result.columns[0].get_column_name())

        # test cast operators
        result = table.select("cast(age as binary(10)) as test").execute()
        self.assertEqual(result.columns[0].get_type(), mysqlx.ColumnType.BYTES)

        result = table.select("cast('1994-12-11' as date) as test").execute()
        self.assertEqual(result.columns[0].get_type(), mysqlx.ColumnType.DATE)

        result = table.select(
            "cast('1994-12-11:12:00:00' as datetime) as test"
        ).execute()
        self.assertEqual(result.columns[0].get_type(), mysqlx.ColumnType.DATETIME)

        result = table.select("cast(age as decimal(10, 7)) as test").execute()
        self.assertEqual(result.columns[0].get_type(), mysqlx.ColumnType.DECIMAL)

        result = table.select("cast('{\"a\": 24}' as json) as test").execute()
        self.assertEqual(result.columns[0].get_type(), mysqlx.ColumnType.JSON)

        result = table.select("cast('12:00:00' as time) as test").execute()
        self.assertEqual(result.columns[0].get_type(), mysqlx.ColumnType.TIME)

        drop_table(self.schema, "test")

        coll = self.schema.create_collection("test")
        coll.add(
            {"_id": "1", "a": 21},
            {"_id": "2", "a": 22},
            {"_id": "3", "a": 23},
            {"_id": "4", "a": 24},
        ).execute()

        table = self.schema.get_collection_as_table("test")
        result = table.select("doc->'$.a' as a").execute()
        rows = result.fetch_all()
        self.assertEqual("a", result.columns[0].get_column_name())
        self.assertEqual(4, len(rows))

        self.schema.drop_collection("test")

    @unittest.skipIf(
        tests.MYSQL_EXTERNAL_SERVER,
        "Test not available for external MySQL servers",
    )
    def test_having(self):
        table_name = "{0}.test".format(self.schema_name)
        drop_table(self.schema, "test")

        self.session.sql(
            "CREATE TABLE {0}(age INT, name VARCHAR(50), "
            "gender CHAR(1))".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (21, 'Fred', 'M')".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (28, 'Barney', 'M')".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (42, 'Wilma', 'F')".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (67, 'Betty', 'F')".format(table_name)
        ).execute()

        table = self.schema.get_table("test")
        result = table.select().group_by("gender").order_by("age ASC").execute()
        rows = result.fetch_all()
        self.assertEqual(2, len(rows))
        self.assertEqual(21, rows[0]["age"])
        self.assertEqual(42, rows[1]["age"])

        result = (
            table.select()
            .group_by("gender")
            .having("gender = 'F'")
            .order_by("age ASC")
            .execute()
        )
        rows = result.fetch_all()
        self.assertEqual(1, len(rows))
        self.assertEqual(42, rows[0]["age"])

        # test flexible params
        result = (
            table.select()
            .group_by(["gender"])
            .order_by(["name DESC", "age ASC"])
            .execute()
        )
        rows = result.fetch_all()
        self.assertEqual(2, len(rows))
        self.assertEqual(42, rows[0]["age"])
        self.assertEqual(21, rows[1]["age"])

        drop_table(self.schema, "test")

    def test_insert(self):
        drop_table(self.schema, "test1")

        self.session.sql(
            "CREATE TABLE {0}.test1(age INT, name "
            "VARCHAR(50), gender CHAR(1))"
            "".format(self.schema_name)
        ).execute()
        table = self.schema.get_table("test1")

        result = (
            table.insert("age", "name")
            .values(21, "Fred")
            .values(28, "Barney")
            .values(42, "Wilma")
            .values(67, "Betty")
            .execute()
        )

        result = table.select().execute()
        rows = result.fetch_all()
        self.assertEqual(4, len(rows))

        # test flexible params
        result = (
            table.insert(["age", "name"])
            .values([35, "Eddard"])
            .values(9, "Arya")
            .execute()
        )

        result = table.select().execute()
        rows = result.fetch_all()
        self.assertEqual(6, len(rows))

        # test unicode
        table.insert("age", "name").values(1, "😀").execute()
        result = table.select().execute()
        rows = result.fetch_all()
        self.assertEqual(7, len(rows))

        drop_table(self.schema, "test1")

    def test_update(self):
        drop_table(self.schema, "test")

        self.session.sql(
            "CREATE TABLE {0}.test(age INT, name "
            "VARCHAR(50), gender CHAR(1), `info` json DEFAULT NULL)"
            "".format(self.schema_name)
        ).execute()
        table = self.schema.get_table("test")

        result = (
            table.insert("age", "name", "info")
            .values(21, "Fred", {"married": True, "sons": 0})
            .values(28, "Barney", {"married": True, "sons": 1})
            .values(42, "Wilma", {"married": True, "sons": 0})
            .values(67, "Betty", {"married": True, "sons": 1})
            .execute()
        )

        result = table.update().set("age", 25).where("age == 21").execute()
        self.assertEqual(1, result.get_affected_items_count())

        # Table.update() is not allowed without a condition
        result = table.update().set("age", 25)
        self.assertRaises(mysqlx.ProgrammingError, result.execute)

        # Update with a mysqlx expression
        statement = table.update()
        statement.set("info", mysqlx.expr("JSON_SET(info, '$.sons', $.sons * 2)"))
        result = statement.where("name = 'Barney' or name = 'Betty'").execute()
        assert 2 == result.get_affected_items_count()

        statement = table.update()
        statement.set("info", mysqlx.expr("JSON_REPLACE(info, '$.married', False)"))
        result = statement.where("name = 'Fred' or name = 'Wilma'").execute()
        assert 2 == result.get_affected_items_count()

        drop_table(self.schema, "test")

    def test_delete(self):
        table_name = "table_test"
        drop_table(self.schema, table_name)

        drop_table(self.schema, table_name)

        self.session.sql(
            _CREATE_TEST_TABLE_QUERY.format(self.schema_name, table_name)
        ).execute()
        self.session.sql(
            _INSERT_TEST_TABLE_QUERY.format(self.schema_name, table_name, "1")
        ).execute()
        self.session.sql(
            _INSERT_TEST_TABLE_QUERY.format(self.schema_name, table_name, "2")
        ).execute()
        self.session.sql(
            _INSERT_TEST_TABLE_QUERY.format(self.schema_name, table_name, "3")
        ).execute()
        table = self.schema.get_table(table_name)
        self.assertTrue(table.exists_in_database())
        self.assertEqual(table.count(), 3)
        table.delete().where("id = 1").execute()
        self.assertEqual(table.count(), 2)

        # Table.delete() is not allowed without a condition
        result = table.delete()
        self.assertRaises(mysqlx.ProgrammingError, result.execute)

        drop_table(self.schema, table_name)

    def test_count(self):
        table_name = "table_test"
        drop_table(self.schema, table_name)

        self.session.sql(
            _CREATE_TEST_TABLE_QUERY.format(self.schema_name, table_name)
        ).execute()
        self.session.sql(
            _INSERT_TEST_TABLE_QUERY.format(self.schema_name, table_name, "1")
        ).execute()
        table = self.schema.get_table(table_name)
        self.assertTrue(table.exists_in_database())
        self.assertEqual(table.count(), 1)
        drop_table(self.schema, table_name)
        self.assertRaises(mysqlx.OperationalError, table.count)

    def test_results(self):
        table_name = "{0}.test".format(self.schema_name)
        drop_table(self.schema, "test")

        self.session.sql(
            "CREATE TABLE {0}(age INT, name VARCHAR(50))".format(table_name)
        ).execute()

        # Test if result has no data
        result = self.session.sql(
            "SELECT age, name FROM {0}".format(table_name)
        ).execute()
        self.assertFalse(result.has_data())
        rows = result.fetch_all()
        self.assertEqual(len(rows), 0)

        # Insert data
        self.session.sql(
            "INSERT INTO {0} VALUES (21, 'Fred')".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (28, 'Barney')".format(table_name)
        ).execute()

        # Test if result has data
        result = self.session.sql(
            "SELECT age, name FROM {0}".format(table_name)
        ).execute()
        self.assertTrue(result.has_data())
        rows = result.fetch_all()
        self.assertEqual(len(rows), 2)

        table = self.schema.get_table("test")
        result = table.select().execute()

        row = result.fetch_one()
        # Test access by column name and index
        self.assertEqual("Fred", row["name"])
        self.assertEqual("Fred", row[1])
        # Test if error is raised with negative indexes and out of bounds
        self.assertRaises(IndexError, row.__getitem__, -1)
        self.assertRaises(IndexError, row.__getitem__, -2)
        self.assertRaises(IndexError, row.__getitem__, -3)
        self.assertRaises(IndexError, row.__getitem__, 3)
        # Test if error is raised with an invalid column name
        self.assertRaises(ValueError, row.__getitem__, "last_name")

        row = result.fetch_one()
        self.assertEqual("Barney", row["name"])
        self.assertEqual("Barney", row[1])
        self.assertEqual(None, result.fetch_one())

        # Test result using column label
        table = self.schema.get_table("test")
        result = (
            table.select("age AS the_age, name AS the_name").where("age = 21").execute()
        )
        row = result.fetch_one()
        self.assertEqual(21, row["the_age"])
        self.assertEqual("Fred", row["the_name"])

        drop_table(self.schema, "test")

    def test_multiple_resultsets(self):
        self.session.sql(
            "DROP PROCEDURE IF EXISTS {0}.spProc".format(self.schema_name)
        ).execute()
        self.session.sql(
            "CREATE PROCEDURE {0}.spProc() BEGIN SELECT 1; "
            "SELECT 2; SELECT 'a'; END"
            "".format(self.schema_name)
        ).execute()

        result = self.session.sql(" CALL {0}.spProc".format(self.schema_name)).execute()
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

        self.session.sql(
            "DROP PROCEDURE IF EXISTS {0}.spProc".format(self.schema_name)
        ).execute()

    def test_auto_inc_value(self):
        table_name = "{0}.test".format(self.schema_name)
        drop_table(self.schema, "test")

        self.session.sql(
            "CREATE TABLE {0}(id INT KEY AUTO_INCREMENT, name VARCHAR(50))"
            "".format(table_name)
        ).execute()
        result = self.session.sql(
            "INSERT INTO {0} VALUES (NULL, 'Fred')".format(table_name)
        ).execute()
        self.assertEqual(1, result.get_autoincrement_value())
        table = self.schema.get_table("test")
        result2 = table.insert("id", "name").values(None, "Boo").execute()
        self.assertEqual(2, result2.get_autoincrement_value())

        drop_table(self.schema, "test")

    @unittest.skipIf(
        tests.MYSQL_VERSION < (8, 0, 19),
        "Time zones in TIMETAMP are only available in MySQL servers +8.0.19",
    )
    def test_column_metadata(self):
        table_name = "{0}.test".format(self.schema_name)
        drop_table(self.schema, "test")

        self.session.sql(
            "CREATE TABLE {0}(age INT, name VARCHAR(50), pic VARBINARY(100), "
            "config JSON, created DATE, updated DATETIME(6), ts TIMESTAMP(2), "
            "active BIT)".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (21, 'Fred', NULL, NULL, '2008-07-26', "
            "'2019-01-19 03:14:07.999999', '2020-01-01 10:10:10+05:30', 0)"
            "".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (28, 'Barney', NULL, NULL, '2012-03-12', "
            "'2019-01-19 03:14:07.999999', '2020-01-01 10:10:10+05:30', 0)"
            "".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (42, 'Wilma', NULL, NULL, '1975-11-11', "
            "'2019-01-19 03:14:07.999999', '2020-01-01 10:10:10+05:30', 1)"
            "".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (67, 'Betty', NULL, NULL, '2015-06-21', "
            "'2019-01-19 03:14:07.999999', '2020-01-01 10:10:10+05:30', 0)"
            "".format(table_name)
        ).execute()

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
        if tests.MYSQL_VERSION >= (8, 0, 1):
            self.assertEqual("utf8mb4_0900_ai_ci", col.get_collation_name())
            self.assertEqual("utf8mb4", col.get_character_set_name())

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

        col = result.columns[4]
        self.assertEqual("created", col.get_column_name())
        self.assertEqual("test", col.get_table_name())
        self.assertEqual(mysqlx.ColumnType.DATE, col.get_type())

        col = result.columns[5]
        self.assertEqual("updated", col.get_column_name())
        self.assertEqual("test", col.get_table_name())
        self.assertEqual(mysqlx.ColumnType.DATETIME, col.get_type())

        col = result.columns[6]
        self.assertEqual("ts", col.get_column_name())
        self.assertEqual("test", col.get_table_name())
        self.assertEqual(mysqlx.ColumnType.TIMESTAMP, col.get_type())

        col = result.columns[7]
        self.assertEqual("active", col.get_column_name())
        self.assertEqual("test", col.get_table_name())
        self.assertEqual(mysqlx.ColumnType.BIT, col.get_type())

        self.assertEqual(result.columns, result.get_columns())

        drop_table(self.schema, "test")

    def test_column_type(self):
        table_name = "column_types"
        self.session.use_pure = True
        self.session.sql(
            f"DROP TABLE IF EXISTS {self.schema.name}.{table_name}"
        ).execute()
        columns_names = (
            "my_null",
            "my_bit",
            "my_tinyint",
            "my_smallint",
            "my_mediumint",
            "my_int",
            "my_bigint",
            "my_decimal",
            "my_float",
            "my_double",
            "my_date",
            "my_time",
            "my_datetime",
            "my_year",
            "my_char",
            "my_varchar",
            "my_varbinary",
            "my_enum",
            "my_geometry",
            "my_blob",
        )
        create_table_stmt = (
            f"CREATE TABLE {table_name} ("
            "id INT NOT NULL AUTO_INCREMENT, "
            "my_null INT, "
            "my_bit BIT(7), "
            "my_tinyint TINYINT, "
            "my_smallint SMALLINT, "
            "my_mediumint MEDIUMINT, "
            "my_int INT, "
            "my_bigint BIGINT, "
            "my_decimal DECIMAL(20,10), "
            "my_float FLOAT, "
            "my_double DOUBLE, "
            "my_date DATE, "
            "my_time TIME, "
            "my_datetime DATETIME, "
            "my_year YEAR, "
            "my_char CHAR(100), "
            "my_varchar VARCHAR(100), "
            "my_varbinary VARBINARY(100), "
            "my_enum ENUM('x-small', 'small', 'medium', 'large', 'x-large'), "
            "my_geometry GEOMETRY, "
            "my_blob BLOB, "
            "PRIMARY KEY (id))"
        )
        self.session.sql(create_table_stmt).execute()
        table = self.schema.get_table(table_name)
        table.insert(*columns_names).values(
            None,
            124,
            127,
            32767,
            8388607,
            2147483647,
            4294967295 if ARCH_64BIT else 2147483647,
            "1.2",
            3.14,
            4.28,
            "2018-12-31 00:00:00",
            "12:13:14",
            "2019-02-04 10:36:03",
            2019,
            "abc",
            "abc",
            "MySQL 🐬",
            "x-large",
            mysqlx.expr("ST_GeomFromText('POINT(21 34)')"),
            "random blob data",
        ).execute()

        exp = [
            None,
            124,
            127,
            32767,
            8388607,
            2147483647,
            4294967295 if ARCH_64BIT else 2147483647,
            decimal.Decimal("1.2000000000"),
            3.140000104904175,
            4.28,
            datetime.datetime(2018, 12, 31),
            datetime.timedelta(0, 43994),
            datetime.datetime(2019, 2, 4, 10, 36, 3),
            2019,
            "abc",
            "abc",
            b"MySQL \xf0\x9f\x90\xac",
            b"x-large",
            b"\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x005"
            b"@\x00\x00\x00\x00\x00\x00A@",
            b"random blob data",
        ]

        row = table.select(*columns_names).execute().fetch_one()
        self.assertEqual([row[col] for col in columns_names], exp)

        row = (
            self.session.sql(
                "SELECT {} FROM {}.{}".format(
                    ",".join(columns_names),
                    self.schema.name,
                    table_name,
                )
            )
            .execute()
            .fetch_one()
        )
        self.assertEqual([row[col] for col in columns_names], exp)

        self.session.sql(
            f"DROP TABLE IF EXISTS {self.schema.name}.{table_name}"
        ).execute()

    def test_is_view(self):
        table_name = "table_test"
        view_name = "view_test"
        drop_table(self.schema, table_name)
        drop_view(self.schema, view_name)

        self.session.sql(
            _CREATE_TEST_TABLE_QUERY.format(self.schema_name, table_name)
        ).execute()
        self.session.sql(
            _INSERT_TEST_TABLE_QUERY.format(self.schema_name, table_name, "1")
        ).execute()
        table = self.schema.get_table(table_name)
        self.assertFalse(table.is_view())

        self.session.sql(
            _CREATE_TEST_VIEW_QUERY.format(
                self.schema_name, view_name, self.schema_name, table_name
            )
        ).execute()
        view = self.schema.get_table(view_name)
        self.assertTrue(view.is_view())

        drop_table(self.schema, table_name)
        drop_view(self.schema, view_name)

    @unittest.skipIf(
        tests.MYSQL_VERSION < (8, 0, 14), "Prepared statements not supported"
    )
    def test_prepared_statements(self):
        session = mysqlx.get_session(self.connect_kwargs)
        schema = session.get_schema(self.schema_name)
        expected_stmt_attrs = (
            lambda stmt, changed, prepared, repeated, exec_counter: stmt.changed
            == changed
            and stmt.prepared == prepared
            and stmt.repeated == repeated
            and stmt.exec_counter == exec_counter
        )

        table_name = "prepared_table_test"
        drop_table(schema, table_name)

        session.sql(
            "CREATE TABLE {}.{}(id INT KEY AUTO_INCREMENT, "
            "name VARCHAR(50), age INT)"
            "".format(self.schema_name, table_name)
        ).execute()
        table = schema.get_table(table_name)
        table.insert("name", "age").values("Fred", 21).values("Barney", 28).values(
            "Wilma", 42
        ).values("Betty", 67).values("Bob", 75).execute()

        # SelectStatement

        select = table.select().where("age == :age")
        self.assertTrue(expected_stmt_attrs(select, True, False, False, 0))

        # On the first call should: Crud::Select (without prepared statement)
        select.bind("age", 21).execute().fetch_all()
        self.assertTrue(expected_stmt_attrs(select, False, False, False, 1))

        # On the second call should: Prepare::Prepare + Prepare::Execute
        select.bind("age", 28).execute().fetch_all()
        self.assertTrue(expected_stmt_attrs(select, False, True, True, 2))

        # On subsequent calls should: Prepare::Execute
        select.bind("age", 42).execute().fetch_all()
        self.assertTrue(expected_stmt_attrs(select, False, True, True, 3))

        row = session.sql(_PREP_STMT_QUERY).execute().fetch_all()[0]
        expected_sql_text = (
            "SELECT * FROM `{}`.`{}` "
            "WHERE (`age` = ?)"
            "".format(self.schema_name, table_name)
        )
        self.assertEqual(row[0], expected_sql_text)
        self.assertEqual(row[1], select.exec_counter - 1)

        # Using sort() should deallocate the prepared statement:
        # Prepare::Deallocate + Crud::Select
        select.bind("age", 21).order_by("age").execute().fetch_all()
        self.assertTrue(expected_stmt_attrs(select, False, False, False, 1))

        # On the second call should: Prepare::Prepare + Prepare::Execute
        select.bind("age", 42).execute().fetch_all()
        self.assertTrue(expected_stmt_attrs(select, False, True, True, 2))

        # The previous statement should be closed since it had no limit/offset
        # Prepare::Deallocate + Crud::Find
        select.bind("age", 67).limit(1).offset(0).execute().fetch_all()
        self.assertTrue(expected_stmt_attrs(select, False, True, False, 1))

        # On the second call should: Prepare::Prepare + Prepare::Execute
        select.bind("age", 75).limit(1).offset(0).execute().fetch_all()
        self.assertTrue(expected_stmt_attrs(select, False, True, True, 2))

        row = session.sql(_PREP_STMT_QUERY).execute().fetch_all()[0]
        expected_sql_text = (
            "SELECT * FROM `{}`.`{}` "
            "WHERE (`age` = ?) ORDER BY `age` LIMIT ?, ?"
            "".format(self.schema_name, table_name)
        )
        self.assertEqual(row[0], expected_sql_text)
        self.assertEqual(row[1], select.exec_counter)

        # UpdateStatement

        update = table.update().where("id == :id").set("age", 18)
        self.assertTrue(expected_stmt_attrs(update, True, False, False, 0))

        # On the first call should: Crud::Update (without prepared statement)
        update.bind("id", 1).execute()
        self.assertTrue(expected_stmt_attrs(update, False, False, False, 1))

        # On the second call should: Prepare::Prepare + Prepare::Execute
        update.bind("id", 2).execute()
        self.assertTrue(expected_stmt_attrs(update, False, True, True, 2))

        row = session.sql(_PREP_STMT_QUERY).execute().fetch_all()[1]
        expected_sql_text = (
            "UPDATE `{}`.`{}` SET `age`=18 "
            "WHERE (`id` = ?)"
            "".format(self.schema_name, table_name)
        )
        self.assertEqual(row[0], expected_sql_text)
        self.assertEqual(row[1], update.exec_counter - 1)

        # Using set() should deallocate the prepared statement:
        # Prepare::Deallocate + Crud::Update
        update.bind("id", "3").set("age", 92).execute()
        self.assertTrue(expected_stmt_attrs(update, False, False, False, 1))

        # On the second call should: Prepare::Prepare + Prepare::Execute
        update.bind("id", "4").execute()
        self.assertTrue(expected_stmt_attrs(update, False, True, True, 2))

        row = session.sql(_PREP_STMT_QUERY).execute().fetch_all()[1]
        expected_sql_text = (
            "UPDATE `{}`.`{}` "
            "SET `age`=92 WHERE (`id` = ?)"
            "".format(self.schema_name, table_name)
        )
        self.assertEqual(row[0], expected_sql_text)
        self.assertEqual(row[1], update.exec_counter - 1)

        # DeleteStatement

        delete = table.delete().where("id == :id")
        self.assertTrue(expected_stmt_attrs(delete, True, False, False, 0))

        # On the first call should: Crud::Delete (without prepared statement)
        delete.bind("id", 1).execute()
        self.assertTrue(expected_stmt_attrs(delete, False, False, False, 1))

        # On the second call should: Prepare::Prepare + Prepare::Execute
        delete.bind("id", 2).execute()
        self.assertTrue(expected_stmt_attrs(delete, False, True, True, 2))

        # On subsequent calls should: Prepare::Execute
        delete.bind("id", 3).execute()
        self.assertTrue(expected_stmt_attrs(delete, False, True, True, 3))

        row = session.sql(_PREP_STMT_QUERY).execute().fetch_all()[2]
        expected_sql_text = (
            "DELETE FROM `{}`.`{}` "
            "WHERE (`id` = ?)"
            "".format(self.schema_name, table_name)
        )
        self.assertEqual(row[0], expected_sql_text)
        self.assertEqual(row[1], delete.exec_counter - 1)

        # Using sort() should deallocate the prepared statement:
        # Prepare::Deallocate + Crud::Delete
        delete.bind("id", 3).sort("age ASC").execute()
        self.assertTrue(expected_stmt_attrs(delete, False, False, False, 1))

        # On the second call should: Prepare::Prepare + Prepare::Execute
        delete.bind("id", 4).execute()
        self.assertTrue(expected_stmt_attrs(delete, False, True, True, 2))

        # On subsequent calls should: Prepare::Execute
        delete.bind("id", 5).execute()
        self.assertTrue(expected_stmt_attrs(delete, False, True, True, 3))

        row = session.sql(_PREP_STMT_QUERY).execute().fetch_all()[2]
        expected_sql_text = (
            "DELETE FROM `{}`.`{}` "
            "WHERE (`id` = ?) ORDER BY `age`"
            "".format(self.schema_name, table_name)
        )
        self.assertEqual(row[0], expected_sql_text)
        self.assertEqual(row[1], delete.exec_counter - 1)

        drop_table(schema, table_name)
        session.close()


@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 14), "XPlugin not compatible")
class MySQLxViewTests(tests.MySQLxTests):
    def setUp(self):
        self.connect_kwargs = tests.get_mysqlx_config()
        self.schema_name = self.connect_kwargs["schema"]
        self.table_name = "table_test"
        self.view_name = "view_test"
        try:
            self.session = mysqlx.get_session(self.connect_kwargs)
        except mysqlx.Error as err:
            self.fail("{0}".format(err))
        self.schema = self.session.get_schema(self.schema_name)
        drop_table(self.schema, self.table_name)
        drop_view(self.schema, self.view_name)

    def tearDown(self):
        drop_table(self.schema, self.table_name)
        drop_view(self.schema, self.view_name)
        self.session.close()

    def test_exists_in_database(self):
        view = self.schema.get_view(self.view_name)
        self.assertFalse(view.exists_in_database())
        self.session.sql(
            _CREATE_TEST_TABLE_QUERY.format(self.schema_name, self.table_name)
        ).execute()
        defined_as = "SELECT id FROM {0}.{1}".format(self.schema_name, self.table_name)
        view = create_view(self.schema, self.view_name, defined_as)
        self.assertTrue(view.exists_in_database())

    def test_select(self):
        table_name = "{0}.{1}".format(self.schema_name, self.table_name)

        self.session.sql(
            "CREATE TABLE {0} (age INT, name VARCHAR(50))".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (21, 'Fred')".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (28, 'Barney')".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (42, 'Wilma')".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (67, 'Betty')".format(table_name)
        ).execute()

        defined_as = "SELECT age, name FROM {0}".format(table_name)
        view = create_view(self.schema, self.view_name, defined_as)
        result = view.select().order_by("age DESC").execute()
        rows = result.fetch_all()
        self.assertEqual(4, len(rows))
        self.assertEqual(67, rows[0]["age"])

        result = view.select("age").where("age = 42").execute()
        self.assertEqual(1, len(result.columns))
        rows = result.fetch_all()
        self.assertEqual(1, len(rows))

        # test flexible params
        result = view.select(["age", "name"]).order_by("age DESC").execute()
        rows = result.fetch_all()
        self.assertEqual(4, len(rows))

    @unittest.skipIf(
        tests.MYSQL_EXTERNAL_SERVER,
        "Test not available for external MySQL servers",
    )
    def test_having(self):
        table_name = "{0}.{1}".format(self.schema_name, self.table_name)

        self.session.sql(
            "CREATE TABLE {0} (age INT, name VARCHAR(50), "
            "gender CHAR(1))".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (21, 'Fred', 'M')".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (28, 'Barney', 'M')".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (42, 'Wilma', 'F')".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (67, 'Betty', 'F')".format(table_name)
        ).execute()

        defined_as = "SELECT age, name, gender FROM {0}".format(table_name)
        view = create_view(self.schema, self.view_name, defined_as)
        result = view.select().group_by("gender").order_by("age ASC").execute()
        rows = result.fetch_all()
        self.assertEqual(2, len(rows))
        self.assertEqual(21, rows[0]["age"])
        self.assertEqual(42, rows[1]["age"])

        result = (
            view.select()
            .group_by("gender")
            .having("gender = 'F'")
            .order_by("age ASC")
            .execute()
        )
        rows = result.fetch_all()
        self.assertEqual(1, len(rows))
        self.assertEqual(42, rows[0]["age"])

        # test flexible params
        result = (
            view.select()
            .group_by(["gender"])
            .order_by(["name DESC", "age ASC"])
            .execute()
        )
        rows = result.fetch_all()
        self.assertEqual(2, len(rows))
        self.assertEqual(42, rows[0]["age"])
        self.assertEqual(21, rows[1]["age"])

    def test_insert(self):
        table_name = "{0}.{1}".format(self.schema_name, self.table_name)

        self.session.sql(
            "CREATE TABLE {0} (age INT, name VARCHAR(50), "
            "gender CHAR(1))".format(table_name)
        ).execute()
        defined_as = "SELECT age, name, gender FROM {0}".format(table_name)
        view = create_view(self.schema, self.view_name, defined_as)

        result = (
            view.insert("age", "name")
            .values(21, "Fred")
            .values(28, "Barney")
            .values(42, "Wilma")
            .values(67, "Betty")
            .execute()
        )
        result = view.select().execute()
        rows = result.fetch_all()
        self.assertEqual(4, len(rows))

        # test flexible params
        result = (
            view.insert(["age", "name"])
            .values([35, "Eddard"])
            .values(9, "Arya")
            .execute()
        )
        result = view.select().execute()
        rows = result.fetch_all()
        self.assertEqual(6, len(rows))

    def test_update(self):
        table_name = "{0}.{1}".format(self.schema_name, self.table_name)

        self.session.sql(
            "CREATE TABLE {0} (age INT, name VARCHAR(50), "
            "gender CHAR(1))".format(table_name)
        ).execute()
        defined_as = "SELECT age, name, gender FROM {0}".format(table_name)
        view = create_view(self.schema, self.view_name, defined_as)

        result = (
            view.insert("age", "name")
            .values(21, "Fred")
            .values(28, "Barney")
            .values(42, "Wilma")
            .values(67, "Betty")
            .execute()
        )
        result = view.update().set("age", 25).where("age == 21").execute()
        self.assertEqual(1, result.get_affected_items_count())
        drop_table(self.schema, "test")

    def test_delete(self):
        self.session.sql(
            _CREATE_TEST_TABLE_QUERY.format(self.schema_name, self.table_name)
        ).execute()
        self.session.sql(
            _INSERT_TEST_TABLE_QUERY.format(self.schema_name, self.table_name, "1")
        ).execute()

        defined_as = "SELECT id FROM {0}.{1}".format(self.schema_name, self.table_name)
        view = create_view(self.schema, self.view_name, defined_as)
        self.assertEqual(view.count(), 1)
        view.delete().where("id = 1").execute()
        self.assertEqual(view.count(), 0)

    def test_count(self):
        self.session.sql(
            _CREATE_TEST_TABLE_QUERY.format(self.schema_name, self.table_name)
        ).execute()
        self.session.sql(
            _INSERT_TEST_TABLE_QUERY.format(self.schema_name, self.table_name, "1")
        ).execute()

        defined_as = "SELECT id FROM {0}.{1}".format(self.schema_name, self.table_name)
        view = create_view(self.schema, self.view_name, defined_as)
        self.assertEqual(view.count(), 1)
        drop_view(self.schema, self.view_name)
        self.assertRaises(mysqlx.OperationalError, view.count)

    def test_results(self):
        table_name = "{0}.{1}".format(self.schema_name, self.table_name)

        self.session.sql(
            "CREATE TABLE {0} (age INT, name VARCHAR(50))".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (21, 'Fred')".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (28, 'Barney')".format(table_name)
        ).execute()

        defined_as = "SELECT age, name FROM {0}".format(table_name)
        view = create_view(self.schema, self.view_name, defined_as)
        result = view.select().execute()

        self.assertEqual("Fred", result.fetch_one()["name"])
        self.assertEqual("Barney", result.fetch_one()["name"])
        self.assertEqual(None, result.fetch_one())

    def test_auto_inc_value(self):
        table_name = "{0}.{1}".format(self.schema_name, self.table_name)

        self.session.sql(
            "CREATE TABLE {0} (id INT KEY AUTO_INCREMENT, "
            "name VARCHAR(50))".format(table_name)
        ).execute()
        result = self.session.sql(
            "INSERT INTO {0} VALUES (NULL, 'Fred')".format(table_name)
        ).execute()
        self.assertEqual(1, result.get_autoincrement_value())

        defined_as = "SELECT id, name FROM {0}".format(table_name)
        view = create_view(self.schema, self.view_name, defined_as)
        result2 = view.insert("id", "name").values(None, "Boo").execute()
        self.assertEqual(2, result2.get_autoincrement_value())

    def test_column_metadata(self):
        table_name = "{0}.{1}".format(self.schema_name, self.table_name)

        self.session.sql(
            "CREATE TABLE {0}(age INT, name VARCHAR(50), pic VARBINARY(100), "
            "config JSON, created DATE, active BIT)"
            "".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (21, 'Fred', NULL, NULL, '2008-07-26', 0)"
            "".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (28, 'Barney', NULL, NULL, '2012-03-12'"
            ", 0)".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (42, 'Wilma', NULL, NULL, '1975-11-11', 1)"
            "".format(table_name)
        ).execute()
        self.session.sql(
            "INSERT INTO {0} VALUES (67, 'Betty', NULL, NULL, '2015-06-21', 0)"
            "".format(table_name)
        ).execute()

        defined_as = (
            "SELECT age, name, pic, config, created, active FROM {0}"
            "".format(table_name)
        )
        view = create_view(self.schema, self.view_name, defined_as)

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
