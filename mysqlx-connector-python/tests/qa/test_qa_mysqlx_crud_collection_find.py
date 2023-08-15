# Copyright (c) 2023, Oracle and/or its affiliates.
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

import os
import threading
import time
import unittest

import mysqlx

import tests


@unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 25), "XPlugin not compatible")
class CollectionAddTests(tests.MySQLxTests):
    """Tests for collection.find()."""

    def _drop_collection_if_exists(self, name):
        collection = self.schema.get_collection(name)
        if collection.exists_in_database():
            self.schema.drop_collection(name)

    @tests.foreach_session()
    def test_collection_find1(self):
        """Test collection.find.fields."""
        self._drop_collection_if_exists("mycoll5")
        collection = self.schema.create_collection("mycoll5")
        collection.add(
            {"_id": 1, "name": "a", "age": 21},
            {"_id": 2, "name": "b"},
            {"_id": 3, "name": "c"},
        ).execute()
        result = (
            collection.find().fields("sum($.age)").group_by("$.age").execute()
        ).fetch_all()
        self.assertEqual(len(result), 2)
        self.schema.drop_collection("mycoll5")

    @unittest.skip("TODO: Fix me")
    @tests.foreach_session()
    def test_collection_find2(self):
        """Test the collection.find.groupby and having."""
        self._drop_collection_if_exists("mycoll6")
        collection = self.schema.create_collection("mycoll6")
        collection.add(
            {"_id": 1, "a": 1, "b": 2, "c": 100},
            {"_id": 2, "a": 2, "b": 1, "c": 200},
            {"_id": 3, "a": 3, "b": 2, "c": 300},
        ).execute()
        result = (
            collection.find()
            .fields("$.a, $.b")
            .group_by("$.b")
            .having("$.a > 1")
            .execute()
        ).fetch_all()
        self.assertEqual(len(result), 2)
        self.schema.drop_collection("mycoll6")

    @tests.foreach_session()
    def test_collection_find3(self):
        """Test collection.find with sort."""
        self._drop_collection_if_exists("mycoll9")
        collection = self.schema.create_collection("mycoll9")
        collection.add(
            {"_id": 1, "a": 1, "b": 2, "c": 100},
            {"_id": 2, "a": 2, "b": 1, "c": 200},
            {"_id": 3, "a": 3, "b": 2, "c": 300},
        ).execute()
        result = collection.find().fields("$.a, $b").sort("a DESC").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["$.a"], 3)
        self.schema.drop_collection("mycoll9")

    @tests.foreach_session()
    def test_collection_find4(self):
        """Test collection.find with limit with offset."""
        self._drop_collection_if_exists("mycoll10")
        collection = self.schema.create_collection("mycoll10")
        collection.add(
            {"_id": 1, "a": 1, "b": 2, "c": 100},
            {"_id": 2, "a": 2, "b": 1, "c": 200},
            {"_id": 3, "a": 3, "b": 2, "c": 300},
        ).execute()
        result = collection.find("$.a > 1").fields("$.a").limit(2).offset(1).execute()
        row = result.fetch_all()
        self.schema.drop_collection("mycoll10")

    @unittest.skip("TODO: Fix me")
    @tests.foreach_session()
    def test_collection_find5(self):
        """Test collection.find with like."""
        self._drop_collection_if_exists("mycoll11")
        collection = self.schema.create_collection("mycoll11")
        collection.add(
            {"_id": 1, "name": "Sana"},
            {"_id": 2, "name": "Sam"},
            {"_id": 3, "name": "amr"},
        ).execute()
        result = collection.find("$.name like S*").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 2)
        self.assertEqual(row[1]["name"], "Sam")
        self.schema.drop_collection("mycoll11")

    @tests.foreach_session()
    def test_collection_find6(self):
        """Test collection.find with bind."""
        self._drop_collection_if_exists("mycoll11")
        collection = self.schema.create_collection("mycoll11")
        collection.add(
            {"_id": 1, "name": "Sana"},
            {"_id": 2, "name": "Sam"},
            {"_id": 3, "name": "amr"},
        ).execute()
        result = collection.find("$.name  == :name").bind("name", "Sana").execute()
        row = result.fetch_all()[0]
        self.assertEqual(row["_id"], 1)
        self.schema.drop_collection("mycoll11")

    @tests.foreach_session()
    def test_collection_find7(self):
        """Test collection.find with parameter list."""
        self._drop_collection_if_exists("mycoll19")
        collection = self.schema.create_collection("mycoll19")
        collection.add(
            {"_id": 1, "name": "Sana"},
            {"_id": 2, "name": "Sam"},
            {"_id": 3, "name": "amr"},
        ).execute()
        result = (
            collection.find("$._id > 1").fields("$._id", "$.name").execute()
        ).fetch_all()
        self.assertEqual(len(result), 2)
        self.schema.drop_collection("mycoll19")

    @unittest.skipIf(
        tests.MYSQL_EXTERNAL_SERVER,
        "Test not available for external MySQL servers",
    )
    @tests.foreach_session()
    def test_collection_find8(self):
        """Test collection.find.groupby with parameter list."""
        self._drop_collection_if_exists("mycoll20")
        collection = self.schema.create_collection(
            "mycol20",
        )
        collection.add(
            {"_id": 1, "a": 1, "b": 2, "c": 100},
            {"_id": 2, "a": 2, "b": 2, "c": 300},
            {"_id": 3, "a": 3, "b": 2, "c": 100},
        ).execute()
        result = (
            collection.find().fields("$a,$.b,$.c").group_by("$.b", "$.c").execute()
        ).fetch_all()
        self.assertEqual(len(result), 2)
        self.schema.drop_collection("mycol20")

    @tests.foreach_session()
    def test_collection_find9(self):
        """Test collection.find.sort with param list."""
        self._drop_collection_if_exists("mycoll21")
        collection = self.schema.create_collection("mycoll21")
        collection.add(
            {"_id": 1, "a": 1, "b": 10, "c": 100},
            {"_id": 2, "a": 1, "b": 11, "c": 200},
            {"_id": 3, "a": 2, "b": 10, "c": 300},
        ).execute()
        result = (
            collection.find().fields("$.a, $.b").sort("a ASC", "b DESC").execute()
        ).fetch_all()
        self.assertEqual(result[0]["$.b"], 11)
        self.schema.drop_collection("mycoll21")

    @tests.foreach_session()
    def test_collection_find10(self):
        """Test collection.find using where() condition."""
        self._drop_collection_if_exists("newcoll1")
        collection = self.schema.create_collection("newcoll1")
        collection.add(
            {"_id": 1, "a": 1, "b": 10, "c": 100},
            {"_id": 2, "a": 1, "b": 11, "c": 200},
            {"_id": 3, "a": 2, "b": 10, "c": 300},
        ).execute()
        result = collection.find().where("$.c  >= 200").execute()
        self.assertEqual(len(result.fetch_all()), 2)
        self.schema.drop_collection("newcoll1")

    @unittest.skipUnless(tests.ARCH_64BIT, "Test available only for 64 bit platforms")
    @unittest.skipIf(os.name == "nt", "Test not available for Windows")
    @tests.foreach_session()
    def test_collection_find11(self):
        """Test collection.find with offset as large positive number."""
        self._drop_collection_if_exists("newcoll2")
        collection = self.schema.create_collection(
            "newcoll2",
        )
        collection.add(
            {"_id": 1, "a": 1, "b": 10, "c": 100},
            {"_id": 2, "a": 1, "b": 11, "c": 200},
            {"_id": 3, "a": 2, "b": 10, "c": 300},
        ).execute()
        result = collection.find().limit(2).offset(92898832378723).execute()
        self.assertEqual(len(result.fetch_all()), 0)
        self.schema.drop_collection("newcoll2")

    @tests.foreach_session()
    def test_collection_find12(self):
        """Test collection.find with offset as negative number."""
        self._drop_collection_if_exists("mycoll3")
        collection = self.schema.create_collection("newcoll3")
        collection.add(
            {"_id": 1, "a": 1, "b": 10, "c": 100},
            {"_id": 2, "a": 1, "b": 11, "c": 200},
            {"_id": 3, "a": 2, "b": 10, "c": 300},
        ).execute()
        self.assertRaises(
            ValueError,
            collection.find().limit(2).offset,
            -2378723,
        )
        self.schema.drop_collection("newcoll3")

    @tests.foreach_session()
    def test_operator1(self):
        """Test binary operator and."""
        self._drop_collection_if_exists("mycoll1")
        collection = self.schema.create_collection("mycoll1")
        collection.add(
            {"_id": 1, "name": "Sana"},
            {"_id": 2, "name": "Sam"},
            {"_id": 3, "name": "amr"},
        ).execute()
        result = (
            collection.find("$.name  == :name and $._id == :id")
            .bind('{"name":"Sana" ,"id":1}')
            .execute()
        )
        row = result.fetch_all()[0]
        self.assertEqual(row["name"], "Sana")
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_operator4(self):
        """Test 'between' operator."""
        self._drop_collection_if_exists("mycoll2")
        collection = self.schema.create_collection("mycoll2")
        collection.add(
            {"_id": 1, "name": "Sana"},
            {"_id": 2, "name": "Sam"},
            {"_id": 3, "name": "amr"},
            {"_id": 4, "name": "abc"},
            {"_id": 5, "name": "def"},
        ).execute()
        result = collection.find("$._id between 2 and 4").execute()
        self.assertEqual(len(result.fetch_all()), 3)
        self.schema.drop_collection("mycoll2")

    # Testing the contains operator with single operand on both sides

    @tests.foreach_session()
    def test_contains_operator_test1(self):
        """Test IN operator with string on both sides - With LHS in RHS."""
        self._drop_collection_if_exists("mycoll1")
        collection = self.schema.create_collection("mycoll1")
        collection.add({"name": "a"}, {"name": "b"}).execute()
        result = collection.find("'a' IN $.name").execute()
        self.assertEqual(len(result.fetch_all()), 1)
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_contains_operator2(self):
        """Test IN operator with int as operand - With LHS in RHS."""
        self._drop_collection_if_exists("mycoll2")
        collection = self.schema.create_collection("mycoll2")
        collection.add({"name": "a", "age": 21}, {"name": "b", "age": 21}).execute()
        result = collection.find("21 IN $.age").execute()
        self.assertEqual(len(result.fetch_all()), 2)
        self.schema.drop_collection("mycoll2")

    @unittest.skip("TODO: Fix me")
    @tests.foreach_session()
    def test_contains_operator3(self):
        """Test IN operator with boolean as operand - With LHS in RHS."""
        self._drop_collection_if_exists("mycoll3")
        collection = self.schema.create_collection("mycoll3")
        collection.add(
            {"name": "a", "age": 21, "ARR": [1, 4]},
            {"name": "b", "age": 21, "ARR": 2},
        ).execute()
        result = collection.find("(!false && true) IN [true]").execute()
        self.assertEqual(len(result.fetch_all()), 2)
        self.schema.drop_collection("mycoll3")

    @tests.foreach_session()
    def test_contains_operator4(self):
        """Test NOT IN operator with string operand - With LHS not in RHS."""
        self._drop_collection_if_exists("mycoll4")
        collection = self.schema.create_collection("mycoll4")
        collection.add({"name": "a"}, {"name": "b"}, {"name": "c"}).execute()
        result = collection.find("$.name NOT IN 'a'").execute()
        self.assertEqual(len(result.fetch_all()), 2)
        self.schema.drop_collection("mycoll4")

    @tests.foreach_session()
    def test_contains_operator5(self):
        """Test NOT IN operator with int as operand - With LHS not in RHS."""
        self._drop_collection_if_exists("mycoll5")
        collection = self.schema.create_collection("mycoll5")
        collection.add({"name": "a", "age": 21}, {"name": "b", "age": 22}).execute()
        result = collection.find("21 NOT IN $.age").execute()
        self.assertEqual(len(result.fetch_all()), 1)
        self.schema.drop_collection("mycoll5")

    @tests.foreach_session()
    def test_contains_operator6(self):
        self._drop_collection_if_exists("mycoll6")
        collection = self.schema.create_collection("mycoll6")
        collection.add({"name": "a", "age": 21}, {"name": "b", "age": 21}).execute()
        result = collection.find("'b' NOT IN $.name").execute()
        self.assertEqual(len(result.fetch_all()), 1)
        self.schema.drop_collection("mycoll6")

    @tests.foreach_session()
    def test_contains_operator7(self):
        """Test IN operator with different datatypes as operands."""
        self._drop_collection_if_exists("mycoll7")
        collection = self.schema.create_collection("mycoll7")
        collection.add({"name": "a", "age": 21}, {"name": "b", "age": 22}).execute()
        result = collection.find("21 IN $.name").execute()
        result1 = collection.find("'b' IN $.age").limit(1).execute()
        self.assertEqual(len(result.fetch_all()), 0)
        self.assertEqual(len(result1.fetch_all()), 0),
        self.schema.drop_collection("mycoll7")

    @tests.foreach_session()
    def test_contains_operator8(self):
        """Test IN operator with single element on LHS and array/list on
        RHS and vice versa."""
        self._drop_collection_if_exists("mycoll8")
        collection = self.schema.create_collection("mycoll8")
        collection.add(
            {"_id": 1, "name": "a", "age": 21, "prof": ["x", "y"]},
            {"_id": 2, "name": "b", "age": 24, "prof": ["p", "q"]},
            {"_id": 3, "name": "c", "age": 26},
        ).execute()
        result = collection.find("$.age IN [21,23,24,28]").execute()
        result1 = collection.find("$.name IN ['a','b','c','d','e']").execute()
        result2 = collection.find("$.age IN (21,23)").execute()
        result3 = collection.find().fields("21 IN (22,23) as test").limit(1).execute()
        result4 = collection.find('["p","q"] IN $.prof').execute()
        self.assertEqual(len(result.fetch_all()), 2)
        self.assertEqual(len(result1.fetch_all()), 3)
        self.assertEqual(len(result2.fetch_all()), 1)
        self.assertEqual(result3.fetch_all()[0].test, False)
        self.assertEqual(len(result4.fetch_all()), 1)
        self.schema.drop_collection("mycoll8")

    @tests.foreach_session()
    def test_contains_operator9(self):
        """Test IN operator with single element on LHS and dict on RHS and
        vice versa."""
        self._drop_collection_if_exists("mycoll9")
        collection = self.schema.create_collection("mycoll9")
        collection.add(
            {
                "_id": 1,
                "name": "joy",
                "age": 21,
                "additionalinfo": {
                    "company": "xyz",
                    "vehicle": "bike",
                    "hobbies": [
                        "reading",
                        "music",
                        "playing",
                        {"a1": "x", "b1": "y", "c1": "z"},
                    ],
                },
            }
        ).execute()
        result = collection.find("'reading' IN $.additionalinfo.hobbies").execute()
        result1 = (
            collection.find().fields("'music' IN $.age as test").limit(1).execute()
        )
        result2 = (
            collection.find()
            .fields("'boxing' IN $.additionalinfo.hobbies as test1")
            .limit(1)
            .execute()
        )
        result3 = collection.find(
            '{"a1":"x","b1":"y","c1":"z"} IN $.additionalinfo.hobbies'
        ).execute()
        self.assertEqual(len(result.fetch_all()), 1)
        self.assertFalse(result1.fetch_all()[0].test)
        self.assertFalse(result2.fetch_all()[0].test1)
        self.assertEqual(len(result3.fetch_all()), 1)
        self.schema.drop_collection("mycoll9")

    @tests.foreach_session()
    def test_contains_operator10(self):
        """Test IN operator with array/list operand on LHS and array/list on
        RHS."""
        self._drop_collection_if_exists("mycoll10")
        collection = self.schema.create_collection("mycoll10")
        collection.add(
            {
                "_id": 1,
                "name": "joy",
                "age": 21,
                "additionalinfo": {
                    "company": "xyz",
                    "vehicle": "bike",
                    "hobbies": ["reading", "music", "playing"],
                },
            },
            {
                "_id": 2,
                "name": "happy",
                "age": 24,
                "additionalinfo": {
                    "company": "abc",
                    "vehicle": "car",
                    "hobbies": ["playing", "painting", "boxing"],
                },
            },
        ).execute()
        result = collection.find(
            '["playing","painting","boxing"] IN $.additionalinfo.hobbies'
        ).execute()
        result1 = (
            collection.find()
            .fields('["happy","joy"] IN $.name as test')
            .limit(1)
            .execute()
        )
        result2 = (
            collection.find()
            .fields('["car","bike"] NOT IN $.additionalinfo.vehicle as test1')
            .limit(1)
            .execute()
        )
        self.assertEqual(len(result.fetch_all()), 1)
        self.assertFalse(result1.fetch_all()[0].test)
        self.assertTrue(result2.fetch_all()[0].test1)
        self.schema.drop_collection("mycoll10")

    @tests.foreach_session()
    def test_contains_operator11(self):
        """Test IN operator with dict on LHS and dict on RHS."""
        self._drop_collection_if_exists("mycoll11")
        collection = self.schema.create_collection("mycoll11")
        collection.add(
            {
                "_id": 1,
                "name": "joy",
                "age": 21,
                "additionalinfo": [
                    {"company": "xyz", "vehicle": "bike"},
                    {"company": "abc", "vehicle": "car"},
                    {"company": "mno", "vehicle": "zeep"},
                ],
            },
            {
                "_id": 2,
                "name": "happy",
                "age": 24,
                "additionalinfo": [
                    {"company": "abc", "vehicle": "car"},
                    {"company": "pqr", "vehicle": "bicycle"},
                ],
            },
            {
                "_id": 3,
                "name": "nice",
                "age": 25,
                "additionalinfo": {"company": "def", "vehicle": "none"},
            },
        ).execute()
        result = collection.find(
            '{"company":"abc","vehicle":"car"} IN $.additionalinfo'
        ).execute()
        result1 = (
            collection.find()
            .fields('{"vehicle":"car"} NOT IN $.additionalinfo as test')
            .limit(1)
            .execute()
        )
        result2 = (
            collection.find()
            .fields('{"company":"mno"} IN $.additionalinfo as test1')
            .limit(1)
            .execute()
        )
        result3 = collection.find(
            '{"company":"abc","vehicle":"car"} NOT IN $.additionalinfo'
        ).execute()
        self.assertEqual(len(result.fetch_all()), 2)
        self.assertFalse(result1.fetch_all()[0].test)
        self.assertTrue(result2.fetch_all()[0].test1)
        self.assertEqual(len(result3.fetch_all()), 1)
        self.schema.drop_collection("mycoll11")

    @tests.foreach_session()
    def test_contains_operator12(self):
        """Test IN operator with operands having expressions."""
        self._drop_collection_if_exists("mycoll12")
        collection = self.schema.create_collection("mycoll12")
        collection.add(
            {"_id": 1, "name": "a", "age": 21},
            {"_id": 2, "name": "b"},
            {"_id": 3, "name": "c"},
        ).execute()
        result = (
            collection.find()
            .fields("(1>5) IN (true, false) as test")
            .limit(1)
            .execute()
        )
        result1 = (
            collection.find()
            .fields("('a'>'b') in (true, false) as test1")
            .limit(1)
            .execute()
        )
        result2 = (
            collection.find()
            .fields(
                "true IN [(1>5), !(false), (true || false), (false && true)] as test2"
            )
            .limit(1)
            .execute()
        )
        self.assertTrue(result.fetch_all()[0].test)
        self.assertTrue(result1.fetch_all()[0].test1)
        self.assertTrue(result2.fetch_all()[0].test2)
        self.schema.drop_collection("mycoll12")

    @tests.foreach_session()
    def test_contains_operator13(self):
        """Test IN operator with operands having expressions."""
        self._drop_collection_if_exists("mycoll13")
        collection = self.schema.create_collection("mycoll13")
        collection.add(
            {"_id": 1, "name": "a", "age": 21},
            {"_id": 2, "name": "b"},
            {"_id": 3, "name": "c"},
        ).execute()
        result = collection.find("(1+5) IN (1,2,3,4,5,6)").execute()
        result1 = collection.find("(2+3) IN (1,2,3,4)").limit(1).execute()
        result2 = collection.find("(1+5) IN (1,2,3,4,5,6)").execute()
        self.assertEqual(len(result.fetch_all()), 3)
        self.assertEqual(len(result1.fetch_all()), 0)
        self.assertEqual(len(result2.fetch_all()), 3)
        self.schema.drop_collection("mycoll13")

    @tests.foreach_session()
    def test_contains_operator14(self):
        """Test IN operator: search for empty string in a field and field in
        empty string."""
        self._drop_collection_if_exists("mycoll14")
        collection = self.schema.create_collection("mycoll14")
        collection.add(
            {"_id": 1, "name": "a", "age": 21},
            {"_id": 2, "name": "b"},
            {"_id": 3, "name": "c"},
        ).execute()
        result = collection.find("'' IN $.name").execute()
        result1 = collection.find("$.name IN ['', ' ']").execute()
        result2 = collection.find("$.name IN ('', ' ')").execute()
        self.assertEqual(len(result.fetch_all()), 0)
        self.assertEqual(len(result1.fetch_all()), 0)
        self.assertEqual(len(result2.fetch_all()), 0)
        self.schema.drop_collection("mycoll14")

    @tests.foreach_session()
    def test_collection_s_s_lock(self):
        """Test shared-shared lock."""
        config = tests.get_mysqlx_config()
        self._drop_collection_if_exists("mycoll11")
        collection = self.schema.create_collection("mycoll1")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(config["schema"])
            collection = schema1.get_collection("mycoll1")

            session1.start_transaction()
            collection.find("name = 'James'").lock_shared().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if waiting.is_set():
                session1.commit()
                self.fail(
                    "Collection_S_S_Lock_test IS NOT OK. Other thread is "
                    "waiting while it is not expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(config["schema"])
            collection = schema2.get_collection("mycoll1")

            if not locking.wait(2):
                self.fail(
                    "Collection_S_S_Lock_test IS NOT OK. Other thread has not "
                    "set the lock!"
                )
            session2.start_transaction()

            waiting.set()
            collection.find("name = 'James'").lock_shared().execute()
            waiting.clear()

            session2.commit()

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
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_collection_s_x_lock(self):
        config = tests.get_mysqlx_config()
        """Test shared-exclusive lock."""
        self._drop_collection_if_exists("mycoll2")
        collection = self.schema.create_collection("mycoll2")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(config["schema"])
            collection = schema1.get_collection("mycoll2")

            session1.start_transaction()
            collection.find("name = 'James'").lock_shared().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if not waiting.is_set():
                session1.commit()
                self.fail(
                    "Collection_S_X_Lock_test IS NOT OK. Other thread is not "
                    "waiting while it is expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(config["schema"])
            collection = schema1.get_collection("mycoll2")

            if not locking.wait(2):
                self.fail(
                    "Collection_S_X_Lock_test IS NOT OK. Other thread has not "
                    "set the lock!"
                )
            session1.start_transaction()
            waiting.set()
            collection.find("name = 'James'").lock_exclusive().execute()
            waiting.clear()
            session1.commit()

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
        self.schema.drop_collection("mycoll2")

    @tests.foreach_session()
    def test_collection_x_x_lock(self):
        """Test exclusive-exclusive lock."""
        config = tests.get_mysqlx_config()
        self._drop_collection_if_exists("mycoll3")
        collection = self.schema.create_collection("mycoll3")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(config["schema"])
            collection = schema1.get_collection("mycoll3")

            session1.start_transaction()
            collection.find("name = 'James'").lock_exclusive().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if not waiting.is_set():
                session1.commit()
                self.fail(
                    "Collection_X_X_Lock_test IS NOT OK. Other thread is not "
                    "waiting while it is expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(config["schema"])
            collection = schema2.get_collection("mycoll3")

            if not locking.wait(2):
                self.fail(
                    "Collection_X_X_Lock_test IS NOT OK. Other thread has not "
                    "set the lock!"
                )
            session2.start_transaction()

            waiting.set()
            collection.find("name = 'James'").lock_exclusive().execute()
            waiting.clear()

            session2.commit()

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
        self.schema.drop_collection("mycoll3")

    @tests.foreach_session()
    def test_collection_x_s_lock(self):
        """Test exclusive-exclusive lock."""
        config = tests.get_mysqlx_config()
        self._drop_collection_if_exists("mycoll4")
        collection = self.schema.create_collection("mycoll4")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(config["schema"])
            collection = schema1.get_collection("mycoll4")

            session1.start_transaction()
            collection.find("name = 'James'").lock_exclusive().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if not waiting.is_set():
                session1.commit()
                self.fail(
                    "Collection_X_S_Lock_test IS NOT OK. Other thread is not "
                    "waiting while it is expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(config["schema"])
            collection = schema2.get_collection("mycoll4")

            if not locking.wait(2):
                self.fail(
                    "Collection_X_S_Lock_test IS NOT OK. Other thread has not "
                    "set the lock!"
                )
            session2.start_transaction()

            waiting.set()
            collection.find("name = 'James'").lock_shared().execute()
            waiting.clear()

            session2.commit()

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
        self.schema.drop_collection("mycoll4")

    @tests.foreach_session()
    def test_collection_multiple_lock_calls(self):
        """Test multiple lock calls."""
        config = tests.get_mysqlx_config()
        self._drop_collection_if_exists("mycoll5")
        collection = self.schema.create_collection("mycoll5")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(config["schema"])
            collection = schema1.get_collection("mycoll5")

            session1.start_transaction()
            collection.find(
                "name = 'James'"
            ).lock_exclusive().lock_shared().lock_exclusive().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if not waiting.is_set():
                session1.commit()
                self.fail(
                    "Collection_Multiple_Lock_calls_test IS NOT OK. Other "
                    "thread is not waiting while it is expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(config["schema"])
            collection = schema2.get_collection("mycoll5")

            if not locking.wait(2):
                self.fail(
                    "Collection_Multiple_Lock_calls_test IS NOT OK. Other "
                    "thread has not set the lock!"
                )
            session2.start_transaction()

            waiting.set()
            collection.find(
                "name = 'James'"
            ).lock_shared().lock_exclusive().lock_exclusive().lock_shared().execute()
            waiting.clear()

            session2.commit()

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
        self.schema.drop_collection("mycoll5")

    @tests.foreach_session()
    def test_collection_x_lock_modify(self):
        """Test lock exclusive and modify - modify will be blocked until the
        lock is released."""
        config = tests.get_mysqlx_config()
        self._drop_collection_if_exists("mycoll6")
        collection = self.schema.create_collection("mycoll6")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(config["schema"])
            collection = schema1.get_collection("mycoll6")

            session1.start_transaction()
            collection.find(
                "$.name = 'James'"
            ).lock_exclusive().lock_shared().lock_exclusive().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if not waiting.is_set():
                session1.commit()
                self.fail(
                    "Collection_X_Lock_Modify_test IS NOT OK. Other thread is "
                    "not waiting while it is expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(config["schema"])
            collection = schema2.get_collection("mycoll6")

            if not locking.wait(2):
                self.fail(
                    "Collection_X_Lock_Modify_test IS NOT OK. Other thread has "
                    "not set the lock!"
                )
            session2.start_transaction()

            waiting.set()
            collection.modify("$.name == 'James'").set("$.age", 30).execute()
            waiting.clear()

            session2.commit()

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
        self.schema.drop_collection("mycoll6")

    @tests.foreach_session()
    def test_collection_s_lock_modifyt(self):
        """Test lock shared and modify - modify will be blocked until the lock
        is released, but will be able to read."""
        config = tests.get_mysqlx_config()
        self._drop_collection_if_exists("mycoll7")
        collection = self.schema.create_collection("mycoll7")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(config["schema"])
            collection = schema1.get_collection("mycoll7")

            session1.start_transaction()
            collection.find("$.name = 'James'").lock_exclusive().lock_shared().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if not waiting.is_set():
                session1.commit()
                #                return
                raise Exception(
                    "Collection_S_Lock_Modify_test IS NOT OK. Other thread is "
                    "not waiting while it is expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(config["schema"])
            collection = schema2.get_collection("mycoll7")

            if not locking.wait(2):
                raise Exception(
                    "Collection_S_Lock_Modify_test IS NOT OK. Other thread has "
                    "not set the lock!"
                )
            session2.start_transaction()

            result = collection.find("$.name == 'James'").execute()
            assert result.fetch_all()[0]["age"] == 23
            waiting.set()
            collection.modify("$.name == 'James'").set("$.age", 30).execute()
            waiting.clear()

            session2.commit()

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
        self.schema.drop_collection("mycoll7")

    @tests.foreach_session()
    def _lock_contention_test(self, lock_type_1, lock_type_2, lock_contention):
        """Test reading an exclusively locked document using lock_shared and
        the 'NOWAIT' waiting option."""
        config = tests.get_mysqlx_config()
        self._drop_collection_if_exists("mycoll")
        collection = self.schema.create_collection("mycoll")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        locking = threading.Event()
        waiting = threading.Event()

        errors = []

        def thread_a(locking, waiting):
            session = mysqlx.get_session(config)
            schema = session.get_schema(config["schema"])
            collection = schema.get_collection("mycoll")

            session.start_transaction()
            result = collection.find("name = 'James'")
            if lock_type_1 == "S":
                result.lock_shared().execute()
            else:
                result.lock_exclusive().execute()

            locking.set()
            time.sleep(2)
            locking.clear()

            if not waiting.is_set():
                errors.append(
                    "{0}-{1} lock test failure.".format(lock_type_1, lock_type_2)
                )
                session.commit()
                return

        def thread_b(locking, waiting):
            session = mysqlx.get_session(config)
            schema = session.get_schema(config["schema"])
            collection = schema.get_collection("mycoll")

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
                try:
                    result.execute()
                except Exception:
                    pass
                session.rollback()

            waiting.set()

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
        self.schema.drop_collection("mycoll")

    @unittest.skip("TODO: Fix me")
    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 5), "Lock contention unavailable.")
    def test_lock_shared_with_nowait(self):
        self._lock_contention_test("S", "S", mysqlx.LockContention.NOWAIT)
        self._lock_contention_test("S", "X", mysqlx.LockContention.NOWAIT)

    @tests.foreach_session()
    def test_collection_x_s_nowait(self):
        config = tests.get_mysqlx_config()
        self._drop_collection_if_exists("mycoll8")
        collection = self.schema.create_collection("mycoll8")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(config["schema"])
            collection = schema1.get_collection("mycoll8")

            session1.start_transaction()
            collection.find("name = 'James'").lock_exclusive().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if waiting.is_set():
                session1.commit()
                self.fail(
                    "Collection_X_S_NOWAIT_test IS NOT OK. Other thread is "
                    "waiting while it is not expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(config["schema"])
            collection = schema2.get_collection("mycoll8")

            if not locking.wait(2):
                self.fail(
                    "Collection_X_S_NOWAIT_test IS NOT OK. Other thread has "
                    "not set the lock!"
                )

            session2.start_transaction()
            waiting.set()
            try:
                collection.find("name = 'James'").lock_exclusive(
                    mysqlx.LockContention.NOWAIT
                ).execute()
            except Exception:
                pass

            waiting.clear()

            session2.commit()

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
        self.schema.drop_collection("mycoll8")

    @tests.foreach_session()
    def test_collection_x_x_nowait(self):
        """Test exclusive-exclusive with NOWAIT lockcontention."""
        config = tests.get_mysqlx_config()
        self._drop_collection_if_exists("mycoll")
        collection = self.schema.create_collection("mycoll")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        session1 = mysqlx.get_session(config)
        schema1 = session1.get_schema(config["schema"])
        collection1 = schema1.get_collection("mycoll")
        session1.start_transaction()
        collection1.find("name = 'James'").lock_exclusive().execute()

        session2 = mysqlx.get_session(config)
        schema2 = session2.get_schema(config["schema"])
        collection2 = schema2.get_collection("mycoll")
        session2.start_transaction()
        self.assertRaises(
            mysqlx.OperationalError,
            collection2.find("name = 'James'")
            .lock_exclusive(mysqlx.LockContention.NOWAIT)
            .execute,
        )
        session2.rollback()
        session1.rollback()
        self.schema.drop_collection("mycoll")
        session2.close()
        session1.close()

    @unittest.skip("TODO: Fix me")
    @tests.foreach_session()
    def test_collection_s_s_nowait(self):
        """Test shared-shared with NOWAIT lockcontention."""
        config = tests.get_mysqlx_config()
        self._drop_collection_if_exists("mycoll9")
        collection = self.schema.create_collection("mycoll9")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(config["schema"])
            collection = schema1.get_collection("mycoll9")

            session1.start_transaction()
            collection.find("name = 'James'").lock_shared().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if waiting.is_set():
                session1.commit()
                self.fail(
                    "Collection_S_S_NOWAIT_test IS NOT OK. Other thread "
                    "is waiting while it is not expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(config["schema"])
            collection = schema2.get_collection("mycoll9")

            if not locking.wait(2):
                self.fail(
                    "Collection_S_S_NOWAIT_test IS NOT OK. Other thread "
                    "has not set the lock!"
                )

            session2.start_transaction()
            waiting.set()
            collection.find("name = 'James'").lock_shared(
                mysqlx.LockContention.NOWAIT
            ).execute()
            res = result.fetch_all()
            self.assertEqual(len(res), 1)

            waiting.clear()

            session2.commit()

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
        self.schema.drop_collection("mycoll9")

    @tests.foreach_session()
    def test_collection_s_x_nowait(self):
        """Test shared-exclusive with NOWAIT lockcontention."""
        config = tests.get_mysqlx_config()
        self._drop_collection_if_exists("mycoll10")
        collection = self.schema.create_collection("mycoll10")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        session1 = mysqlx.get_session(config)
        schema1 = session1.get_schema(config["schema"])
        collection1 = schema1.get_collection("mycoll10")
        session1.start_transaction()
        collection1.find("name = 'James'").lock_shared().execute()

        session2 = mysqlx.get_session(config)
        schema2 = session2.get_schema("test")
        collection2 = schema2.get_collection("mycoll10")
        session2.start_transaction()
        self.assertRaises(
            mysqlx.OperationalError,
            collection2.find("name = 'James'")
            .lock_exclusive(mysqlx.LockContention.NOWAIT)
            .execute,
        )
        session2.rollback()
        session1.rollback()

        self.schema.drop_collection("mycoll10")
        session2.close()
        session1.close()

    @tests.foreach_session()
    def test_collection_x_s_skip_locked(self):
        """Test exclusive-shared with SKIP LOCKED lockcontention."""
        config = tests.get_mysqlx_config()
        self._drop_collection_if_exists("mycoll")
        collection = self.schema.create_collection("mycoll")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        # `session2.lock_exclusive(SKIP_LOCKED) returns data immediately.
        session1 = mysqlx.get_session(config)
        schema1 = session1.get_schema(config["schema"])
        collection1 = schema1.get_collection("mycoll")
        session1.start_transaction()
        result = collection1.find("name = 'James'").lock_exclusive().execute()

        session2 = mysqlx.get_session(config)
        schema2 = session2.get_schema(config["schema"])
        collection2 = schema2.get_collection("mycoll")
        session2.start_transaction()
        result = (
            collection2.find("name = 'James'")
            .lock_shared(mysqlx.LockContention.SKIP_LOCKED)
            .execute()
        ).fetch_all()
        self.assertEqual(len(result), 0)
        session2.rollback()

        session1.rollback()
        self.schema.drop_collection("mycoll")
        session2.close()
        session1.close()

    @tests.foreach_session()
    def test_collection_x_x_skip_locked(self):
        """Test exclusive-exclusive with SKIP LOCKED lockcontention."""
        config = tests.get_mysqlx_config()
        self._drop_collection_if_exists("mycoll")
        collection = self.schema.create_collection("mycoll")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        # `session2.lock_exclusive(SKIP_LOCKED) returns data immediately.
        session1 = mysqlx.get_session(config)
        schema1 = session1.get_schema(config["schema"])
        collection1 = schema1.get_collection("mycoll")
        session1.start_transaction()
        result = collection1.find("name = 'James'").lock_exclusive().execute()

        session2 = mysqlx.get_session(config)
        schema2 = session2.get_schema(config["schema"])
        collection2 = schema2.get_collection("mycoll")
        session2.start_transaction()
        result = (
            collection2.find("name = 'James'")
            .lock_exclusive(mysqlx.LockContention.SKIP_LOCKED)
            .execute()
        )
        res = result.fetch_all()
        assert len(res) == 0
        session2.rollback()

        session1.rollback()
        self.schema.drop_collection("mycoll")
        session2.close()
        session1.close()

    @tests.foreach_session()
    def test_collection_s_s_skip_locked(self):
        """Test shared-shared with SKIP LOCKED lockcontention."""
        config = tests.get_mysqlx_config()
        self._drop_collection_if_exists("mycoll")
        collection = self.schema.create_collection("mycoll")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        session1 = mysqlx.get_session(config)
        schema1 = session1.get_schema(config["schema"])
        collection1 = schema1.get_collection("mycoll")
        session1.start_transaction()
        result = collection1.find("name = 'James'").lock_shared().execute()

        session2 = mysqlx.get_session(config)
        schema2 = session2.get_schema(config["schema"])
        collection2 = schema2.get_collection("mycoll")
        session2.start_transaction()
        result = (
            collection2.find("name = 'James'")
            .lock_shared(mysqlx.LockContention.SKIP_LOCKED)
            .execute()
        )
        res = result.fetch_all()
        assert len(res) == 1
        session2.rollback()

        session1.rollback()
        self.schema.drop_collection("mycoll")
        session2.close()
        session1.close()

    @tests.foreach_session()
    def test_collection_s_x_skip_locked(self):
        """Test shared-exclusive with SKIP LOCKED lockcontention."""
        config = tests.get_mysqlx_config()
        self._drop_collection_if_exists("mycoll")
        collection = self.schema.create_collection("mycoll")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        # `session2.lock_exclusive(SKIP_LOCKED) returns data immediately.
        session1 = mysqlx.get_session(config)
        schema1 = session1.get_schema(config["schema"])
        collection1 = schema1.get_collection("mycoll")
        session1.start_transaction()
        result = collection1.find("name = 'James'").lock_shared().execute()

        session2 = mysqlx.get_session(config)
        schema2 = session2.get_schema(config["schema"])
        collection2 = schema2.get_collection("mycoll")
        session2.start_transaction()
        result = (
            collection2.find("name = 'James'")
            .lock_exclusive(mysqlx.LockContention.SKIP_LOCKED)
            .execute()
        )
        res = result.fetch_all()
        assert len(res) == 0
        session2.rollback()

        session1.rollback()
        self.schema.drop_collection("mycoll")
        session2.close()
        session1.close()

    @tests.foreach_session()
    def test_collection_s_s_default(self):
        """Test exclusive-shared with DEFAULT lockcontention."""
        config = tests.get_mysqlx_config()
        self._drop_collection_if_exists("mycoll1")
        collection = self.schema.create_collection("mycoll1")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(config["schema"])
            collection = schema1.get_collection("mycoll1")

            session1.start_transaction()
            collection.find("name = 'James'").lock_shared().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if waiting.is_set():
                session1.commit()
                self.fail(
                    "Collection_S_S_DEFAULT_test IS NOT OK. Other thread is "
                    "waiting while it is not expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(config["schema"])
            collection = schema2.get_collection("mycoll1")

            if not locking.wait(2):
                self.fail(
                    "Collection_S_S_DEFAULT_test IS NOT OK. Other thread has "
                    "not set the lock!"
                )
            session2.start_transaction()

            waiting.set()
            collection.find("name = 'James'").lock_shared(
                mysqlx.LockContention.DEFAULT
            ).execute()
            waiting.clear()

            session2.commit()

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
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_collection_s_x_default(self):
        """Test shared-exclusive lock."""
        config = tests.get_mysqlx_config()
        session1 = mysqlx.get_session(config)
        self._drop_collection_if_exists("mycoll12")
        collection = self.schema.create_collection("mycoll2")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(config["schema"])
            collection = schema1.get_collection("mycoll2")

            session1.start_transaction()
            collection.find("name = 'James'").lock_shared().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if not waiting.is_set():
                session1.commit()
                self.fail(
                    "Collection_S_X_DEFAULT_test IS NOT OK. Other thread is "
                    "not waiting while it is expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(config["schema"])
            collection = schema1.get_collection("mycoll2")

            if not locking.wait(2):
                self.fail(
                    "Collection_S_X_DEFAULT_test IS NOT OK. Other thread has "
                    "not set the lock!"
                )
            session1.start_transaction()
            waiting.set()
            collection.find("name = 'James'").lock_exclusive(
                mysqlx.LockContention.DEFAULT
            ).execute()
            waiting.clear()
            session1.commit()

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
        self.schema.drop_collection("mycoll2")

    @tests.foreach_session()
    def test_collection_x_x_default(self):
        """Test exclusive-exclusive lock."""
        config = tests.get_mysqlx_config()
        self._drop_collection_if_exists("mycoll13")
        collection = self.schema.create_collection("mycoll3")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(config["schema"])
            collection = schema1.get_collection("mycoll3")

            session1.start_transaction()
            collection.find("name = 'James'").lock_exclusive().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if not waiting.is_set():
                session1.commit()
                self.fail(
                    "Collection_X_X_DEFAULT_test IS NOT OK. Other thread is "
                    "not waiting while it is expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(config["schema"])
            collection = schema2.get_collection("mycoll3")

            if not locking.wait(2):
                self.fail(
                    "Collection_X_X_DEFAULT_test IS NOT OK. Other thread has "
                    "not set the lock!"
                )
            session2.start_transaction()

            waiting.set()
            collection.find("name = 'James'").lock_exclusive(
                mysqlx.LockContention.DEFAULT
            ).execute()
            waiting.clear()

            session2.commit()

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
        self.schema.drop_collection("mycoll3")

    @tests.foreach_session()
    def test_collection_x_s_default(self):
        """Test exclusive-exclusive lock."""
        config = tests.get_mysqlx_config()
        self._drop_collection_if_exists("mycoll4")
        collection = self.schema.create_collection("mycoll4")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(config["schema"])
            collection = schema1.get_collection("mycoll4")

            session1.start_transaction()
            collection.find("name = 'James'").lock_exclusive().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if not waiting.is_set():
                session1.commit()
                self.fail(
                    "Collection_X_S_DEFAULT_test IS NOT OK. Other thread is "
                    "not waiting while it is expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(config["schema"])
            collection = schema2.get_collection("mycoll4")

            if not locking.wait(2):
                self.fail(
                    "Collection_X_S_DEFAULT_test IS NOT OK. Other thread has "
                    "not set the lock!"
                )
            session2.start_transaction()

            waiting.set()
            collection.find("name = 'James'").lock_shared(
                mysqlx.LockContention.DEFAULT
            ).execute()
            waiting.clear()

            session2.commit()

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
        self.schema.drop_collection("mycoll4")

    @tests.foreach_session()
    def test_collection_multiple_lock_contention_calls(self):
        """Test multiple lock calls."""
        config = tests.get_mysqlx_config()
        self._drop_collection_if_exists("mycoll5")

        collection = self.schema.create_collection("mycoll5")
        collection.add(
            {"name": "Joe", "age": 21}, {"name": "James", "age": 23}
        ).execute()

        locking = threading.Event()
        waiting = threading.Event()

        errors = []

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(config["schema"])
            collection = schema1.get_collection("mycoll5")

            session1.start_transaction()
            collection.find(
                "name = 'James'"
            ).lock_exclusive().lock_shared().lock_exclusive().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if waiting.is_set():
                session1.commit()
                self.fail(
                    "Collection_Multiple_LockContention_calls_test IS NOT OK. "
                    "Other thread is not waiting while it is expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(config["schema"])
            collection = schema2.get_collection("mycoll5")

            if not locking.wait(2):
                self.fail(
                    "Collection_Multiple_LockContention_calls_test IS NOT OK. "
                    "Other thread has not set the lock!"
                )
            session2.start_transaction()

            waiting.set()
            result = (
                collection.find("name = 'James'")
                .lock_shared(mysqlx.LockContention.DEFAULT)
                .lock_exclusive(mysqlx.LockContention.SKIP_LOCKED)
                .lock_exclusive(mysqlx.LockContention.NOWAIT)
                .lock_shared(mysqlx.LockContention.SKIP_LOCKED)
                .execute()
            )
            res = result.fetch_all()
            self.assertEqual(len(res), 0)
            waiting.clear()

            session2.commit()

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
        self.schema.drop_collection("mycoll5")

    @tests.foreach_session()
    def test_parameter_binding(self):
        """Test the MCPY-354 issue ( parameter_binding) with parameter list."""
        self._drop_collection_if_exists("mycoll1")
        collection = self.schema.create_collection("mycoll1")
        collection.add({"_id": 1, "name": "a"}, {"_id": 2, "name": "b"}).execute()
        result = collection.find("name == :name").bind('{"name":"b"}').execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["_id"], 2)
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_parameter_binding(self):
        """Test the MCPY-354 issue ( parameter_binding) with dict and date
        datatype."""
        self._drop_collection_if_exists("mycoll2")
        collection = self.schema.create_collection("mycoll2")
        collection.add(
            {"_id": 1, "bday": "2000-10-10"},
            {"_id": 2, "bday": "2000-10-11"},
        ).execute()
        result = collection.find("bday == :bday").bind("bday", "2000-10-11").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["_id"], 2)
        self.schema.drop_collection("mycoll2")

    @tests.foreach_session()
    def Csbufferring_test1():
        """Test client-side buffering."""
        self._drop_collection_if_exists("mycoll3")
        collection = self.schema.create_collection("mycoll3")
        collection.add({"_id": 1, "name": "a"}, {"_id": 2, "name": "b"}).execute()
        result1 = collection.find("name == :name").bind('{"name":"b"}').execute()
        result2 = collection.find("name == :name").bind('{"name":"a"}').execute()
        row2 = result2.fetch_all()
        self.assertEqual(row2[0]["_id"], 1)
        row1 = result1.fetch_all()
        self.assertEqual(row1[0]["_id"], 2)
        self.schema.drop_collection("mycoll3")
