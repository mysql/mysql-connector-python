# Copyright (c) 2021, Oracle and/or its affiliates.
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

import unittest

import mysqlx
import tests


@unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 25), "XPlugin not compatible")
class CollectionRemoveTests(tests.MySQLxTests):
    """Tests for collection.remove."""

    def _drop_collection_if_exists(self, name):
        collection = self.schema.get_collection(name)
        if collection.exists_in_database():
            self.schema.drop_collection(name)

    @tests.foreach_session()
    def test_collection_remove1(self):
        """Test the collection.remove with where using "TRUE" condition."""
        self._drop_collection_if_exists("mycoll1")
        collection = self.schema.create_collection("mycoll1")
        collection.add(
            {"_id": 1, "c1": 10},
            {"_id": 2, "c1": 9},
            {"_id": 3, "c1": 8},
            {"_id": 4, "c1": 7},
            {"_id": 5, "c1": 6},
        ).execute()
        collection.remove("TRUE").where("$._id == 1").execute()
        self.assertEqual(collection.count(), 4)
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_collection_remove2(self):
        """Test the collection.remove with bind."""
        self._drop_collection_if_exists("mycoll2")
        collection = self.schema.create_collection("mycoll2")
        collection.add(
            {"_id": 1, "c1": 10},
            {"_id": 2, "c1": 9},
            {"_id": 3, "c1": 8},
            {"_id": 4, "c1": 7},
            {"_id": 5, "c1": 6},
        ).execute()
        collection.remove("$._id == :_id").where("$._id == :_id").bind(
            "_id", 1
        ).execute()  # where() is deprecated since 8.0.12
        self.assertEqual(collection.count(), 4)
        self.schema.drop_collection("mycoll2")

    @tests.foreach_session()
    def test_collection_remove3(self):
        """Test the collection.remove with sort and limit."""
        self._drop_collection_if_exists("mycoll3")
        collection = self.schema.create_collection("mycoll3")
        collection.add(
            {"_id": 1, "c1": 10},
            {"_id": 2, "c1": 9},
            {"_id": 3, "c1": 8},
            {"_id": 4, "c1": 7},
            {"_id": 5, "c1": 6},
        ).execute()
        collection.remove("true").sort("_id DESC").limit(2).execute()
        self.assertEqual(collection.count(), 3)
        collection.remove("true").limit(2).execute()
        result = collection.find().execute()
        self.assertEqual(collection.count(), 1)
        self.schema.drop_collection("mycoll3")

    @tests.foreach_session()
    def test_collection_remove4(self):
        """Test the collection.remove with limit with offeset using a condition
        which results to TRUE - expected to get OperationalError exception as
        non-zero offset is not supported by server for this operation."""
        self._drop_collection_if_exists("mycoll4")
        collection = self.schema.create_collection("mycoll4")
        collection.add(
            {"_id": 1, "c1": 10},
            {"_id": 2, "c1": 9},
            {"_id": 3, "c1": 8},
            {"_id": 4, "c1": 7},
            {"_id": 5, "c1": 6},
        ).execute()
        collection.remove("1 == 1").limit(2, 2).execute()  # deprecated since 8.0.12
        self.assertEqual(collection.count(), 3)
        self.schema.drop_collection("mycoll4")

    @tests.foreach_session()
    def test_collection_remove5(self):
        """Test the collection.remove_one."""
        self._drop_collection_if_exists("mycoll5")
        collection = self.schema.create_collection("mycoll5")
        collection.add(
            {"_id": 1, "c1": 10},
            {"_id": 2, "c1": 9},
            {"_id": 3, "c1": 8},
            {"_id": 4, "c1": 7},
            {"_id": 5, "c1": 6},
        ).execute()
        collection.remove_one(2)
        result = collection.find().execute()
        self.assertEqual(collection.count(), 4)
        self.schema.drop_collection("mycoll5")

    @tests.foreach_session()
    def test_collection_remove6(self):
        """Test the collection.remove_one."""
        self._drop_collection_if_exists("mycoll6")
        collection = self.schema.create_collection("mycoll6")
        collection.add(
            {"_id": 1, "c1": 10},
            {"_id": 2, "c1": 9},
            {"_id": 3, "c1": 8},
            {"_id": 4, "c1": 7},
            {"_id": 5, "c1": 6},
        ).execute()
        collection.remove_one(6)
        self.schema.drop_collection("mycoll6")

    @tests.foreach_session()
    def test_collection_remove7(self):
        """Test the collection.remove_one."""
        self._drop_collection_if_exists("mycoll7")
        collection = self.schema.create_collection("mycoll7")
        collection.add(
            {"_id": 1, "c1": 10},
            {"_id": 2, "c1": 9},
            {"_id": 3, "c1": 8},
            {"_id": 4, "c1": 7},
            {"_id": 5, "c1": 6},
        ).execute()
        self.assertRaises(TypeError, collection.remove_one)
        self.schema.drop_collection("mycoll7")

    @tests.foreach_session()
    def test_collection_remove8(self):
        """Test the collection.remove with sort and param list."""
        self._drop_collection_if_exists("mycoll8")
        collection = self.schema.create_collection("mycoll8")
        collection.add(
            {"i": 1, "c1": 10},
            {"i": 2, "c1": 10},
            {"i": 1, "c1": 11},
            {"i": 2, "c1": 11},
        ).execute()
        collection.remove("false").sort("i ASC", "c1 DESC").limit(3).execute()
        result = collection.find().execute()
        self.assertEqual(collection.count(), 4)
        self.schema.drop_collection("mycoll8")

    @tests.foreach_session()
    def test_collection_remove9(self):
        # testing the collection.remove with no search condition - Expected to throw exception
        self._drop_collection_if_exists("mycoll9")
        collection = self.schema.create_collection("mycoll9")
        collection.add(
            {"_id": 1, "c1": 10},
            {"_id": 2, "c1": 9},
            {"_id": 3, "c1": 8},
            {"_id": 4, "c1": 7},
            {"_id": 5, "c1": 6},
        ).execute()
        try:
            collection.remove().execute()
        except TypeError:
            pass
        self.schema.drop_collection("mycoll9")

    @tests.foreach_session()
    def test_collection_remove10(self):
        """Test the collection.remove with empty search condition."""
        self._drop_collection_if_exists("mycoll10")
        collection = self.schema.create_collection("mycoll10")
        collection.add(
            {"_id": 1, "c1": 10},
            {"_id": 2, "c1": 9},
            {"_id": 3, "c1": 8},
            {"_id": 4, "c1": 7},
            {"_id": 5, "c1": 6},
        ).execute()
        try:
            collection.remove(" ").execute()
        except mysqlx.ProgrammingError:
            pass
        self.schema.drop_collection("mycoll10")

    @tests.foreach_session()
    def test_collection_remove11(self):
        """Test the collection.remove with null search condition."""
        self._drop_collection_if_exists("mycoll11")
        collection = self.schema.create_collection("mycoll11")
        collection.add(
            {"_id": 1, "c1": 10},
            {"_id": 2, "c1": 9},
            {"_id": 3, "c1": 8},
            {"_id": 4, "c1": 7},
            {"_id": 5, "c1": 6},
        ).execute()
        try:
            collection.remove(None).execute()
        except mysqlx.ProgrammingError:
            pass
        self.schema.drop_collection("mycoll11")

    @tests.foreach_session()
    def test_collection_remove12(self):
        """Test the collection.remove with a condition which results to false."""
        self._drop_collection_if_exists("mycoll12")
        collection = self.schema.create_collection("mycoll12")
        collection.add(
            {"i": 1, "c1": 10},
            {"i": 2, "c1": 10},
            {"i": 1, "c1": 11},
            {"i": 2, "c1": 11},
        ).execute()
        collection.remove("1 == 0").sort("i ASC", "c1 DESC").limit(3).execute()
        result = collection.find().execute()
        self.assertEqual(collection.count(), 4)
        self.schema.drop_collection("mycoll12")

    @tests.foreach_session()
    def test_contains_operator_coll_remove1(self):
        """Test IN operator in collection.remove."""
        self._drop_collection_if_exists("mycoll1")
        collection = self.schema.create_collection("mycoll1")
        collection.add(
            {
                "_id": 1,
                "name": "a",
                "age": 22,
                "prof": {"p1": "x1", "q1": "y1"},
            },
            {"_id": 2, "name": "b", "age": 24},
            {
                "_id": 3,
                "name": "c",
                "age": 25,
                "prof": {"a1": "x1", "b1": "y1"},
            },
            {
                "_id": 4,
                "name": "d",
                "age": 23,
                "prof": {"g1": "d1", "h1": "e1"},
            },
        ).execute()
        result = collection.remove("$.name IN 'a'").execute()
        result1 = collection.remove("$.age IN [22,24]").execute()
        result2 = collection.remove('{"a1":"x1"} IN $.prof').execute()
        result3 = collection.remove('"d1" IN $.prof.g1').execute()
        self.assertEqual(result.get_affected_items_count(), 1)
        self.assertEqual(result1.get_affected_items_count(), 1)
        self.assertEqual(result2.get_affected_items_count(), 1)
        self.assertEqual(result3.get_affected_items_count(), 1)
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_Contains_operator_coll_remove2(self):
        """Test not IN operator in collection.remove."""
        self._drop_collection_if_exists("mycoll2")
        collection = self.schema.create_collection("mycoll2")
        collection.add(
            {
                "_id": 1,
                "name": "a",
                "age": 22,
                "prof": {"p1": "x1", "q1": "y1"},
            },
            {"_id": 2, "name": "b", "age": 24},
            {
                "_id": 3,
                "name": "c",
                "age": 25,
                "prof": {"a1": "x1", "b1": "y1"},
            },
            {
                "_id": 4,
                "name": "d",
                "age": 23,
                "prof": {"g1": "d1", "h1": "e1"},
            },
        ).execute()
        result = collection.remove("$.name NOT IN 'a'").execute()
        result1 = collection.remove("$.age NOT IN [22,24]").execute()
        result2 = collection.remove('{"a1":"x1"} NOT IN $.prof').execute()
        result3 = collection.remove('"d1" NOT IN $.prof.g1').execute()
        self.assertEqual(result.get_affected_items_count(), 3)
        self.assertEqual(result1.get_affected_items_count(), 0)
        self.assertEqual(result2.get_affected_items_count(), 1)
        self.assertEqual(result3.get_affected_items_count(), 0)
        self.schema.drop_collection("mycoll2")

    @tests.foreach_session()
    def test_contains_operator_coll_remove3(self):
        """Test IN operator with array/list operand on LHS and array/list on
        RHS."""
        self._drop_collection_if_exists("mycoll3")
        collection = self.schema.create_collection("mycoll3")
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
        result = collection.remove(
            '["playing","painting","boxing"] IN $.additionalinfo.hobbies'
        ).execute()
        result1 = collection.remove('["happy","joy"] IN $.name').execute()
        result2 = collection.remove(
            '["car","bike"] NOT IN $.additionalinfo.vehicle'
        ).execute()
        self.assertEqual(result.get_affected_items_count(), 1)
        self.assertEqual(result1.get_affected_items_count(), 0)
        self.assertEqual(result2.get_affected_items_count(), 1)
        self.schema.drop_collection("mycoll3")

    @tests.foreach_session()
    def test_contains_operator_coll_remove4(self):
        """Text IN operator with dict on LHS and dict on RHS."""
        self._drop_collection_if_exists("mycoll4")
        collection = self.schema.create_collection("mycoll4")
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
        result = collection.remove(
            '{"company":"abc","vehicle":"car"} IN $.additionalinfo'
        ).execute()
        result1 = collection.remove(
            '{"vehicle":"car"} NOT IN $.additionalinfo'
        ).execute()
        result2 = collection.remove('{"company":"mno"} IN $.additionalinfo').execute()
        result3 = collection.remove(
            '{"company":"abc","vehicle":"car"} NOT IN $.additionalinfo'
        ).execute()
        self.assertEqual(result.get_affected_items_count(), 2)
        self.assertEqual(result1.get_affected_items_count(), 1)
        self.assertEqual(result2.get_affected_items_count(), 0)
        self.assertEqual(result3.get_affected_items_count(), 0)
        self.schema.drop_collection("mycoll4")

    @tests.foreach_session()
    def test_overlaps_coll_remove1(self):
        """Overlaps in collection.remove."""
        self._drop_collection_if_exists("mycoll1")
        collection = self.schema.create_collection("mycoll1")
        collection.add(
            {
                "_id": 1,
                "name": "a",
                "age": 22,
                "prof": {"p1": "x1", "q1": "y1"},
            },
            {"_id": 2, "name": "b", "age": 24},
            {
                "_id": 3,
                "name": "c",
                "age": 25,
                "prof": {"a1": "x1", "b1": "y1"},
            },
            {
                "_id": 4,
                "name": "d",
                "age": 23,
                "prof": {"g1": "d1", "h1": "e1"},
            },
        ).execute()
        result = collection.remove("$.name OVERLAPS 'a'").execute()
        result1 = collection.remove("$.age OVERLAPS [22,24]").execute()
        result2 = collection.remove('{"a1":"x1"} OVERLAPS $.prof').execute()
        result3 = collection.remove('"d1" OVERLAPS $.prof.g1').execute()
        self.assertEqual(result.get_affected_items_count(), 1)
        self.assertEqual(result1.get_affected_items_count(), 1)
        self.assertEqual(result2.get_affected_items_count(), 1)
        self.assertEqual(result3.get_affected_items_count(), 1)
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_overlaps_coll_remove2(self):
        """Not Overlaps in collection.remove."""
        self._drop_collection_if_exists("mycoll2")
        collection = self.schema.create_collection("mycoll2")
        collection.add(
            {
                "_id": 1,
                "name": "a",
                "age": 22,
                "prof": {"p1": "x1", "q1": "y1"},
            },
            {"_id": 2, "name": "b", "age": 24},
            {
                "_id": 3,
                "name": "c",
                "age": 25,
                "prof": {"a1": "x1", "b1": "y1"},
            },
            {
                "_id": 4,
                "name": "d",
                "age": 23,
                "prof": {"g1": "d1", "h1": "e1"},
            },
        ).execute()
        result = collection.remove("$.name NOT OVERLAPS 'a'").execute()
        result1 = collection.remove("$.age NOT OVERLAPS [22,24]").execute()
        result2 = collection.remove('{"a1":"x1"} NOT OVERLAPS $.prof').execute()
        result3 = collection.remove('"d1" NOT OVERLAPS $.prof.g1').execute()
        self.assertEqual(result.get_affected_items_count(), 3)
        self.assertEqual(result1.get_affected_items_count(), 0)
        self.assertEqual(result2.get_affected_items_count(), 1)
        self.assertEqual(result3.get_affected_items_count(), 0)
        self.schema.drop_collection("mycoll2")

    @tests.foreach_session()
    def test_overlaps_coll_remove3(self):
        """OVERLAPS operator with array/list operand on LHS and array/list
        on RHS."""
        self._drop_collection_if_exists("mycoll3")
        collection = self.schema.create_collection(
            "mycoll3",
        )
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
        result = collection.remove(
            '["playing","painting","boxing"] OVERLAPS $.additionalinfo.hobbies'
        ).execute()
        # adding data
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
        result1 = collection.remove('["happy","joy"] OVERLAPS $.name').execute()
        # adding data
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
        result2 = collection.remove(
            '["car","bike"] NOT OVERLAPS $.additionalinfo.vehicle'
        ).execute()
        self.assertEqual(result.get_affected_items_count(), 2)
        self.assertEqual(result1.get_affected_items_count(), 2)
        self.assertEqual(result2.get_affected_items_count(), 0)
        self.schema.drop_collection("mycoll3")

    @tests.foreach_session()
    def terst_overlaps_coll_remove4(self):
        """OVERLAPS operator with dict on LHS and dict on RHS."""
        self._drop_collection_if_exists("mycoll4")
        collection = self.schema.create_collection(
            "mycoll4",
        )
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
        result = collection.remove(
            '{"company":"abc","vehicle":"car"} OVERLAPS $.additionalinfo'
        ).execute()
        result1 = collection.remove(
            '{"vehicle":"car"} NOT OVERLAPS $.additionalinfo'
        ).execute()
        result2 = collection.remove(
            '{"company":"mno"} OVERLAPS $.additionalinfo'
        ).execute()
        # adding data
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
        result3 = collection.remove(
            '{"company":"abc","vehicle":"car"} NOT OVERLAPS $.additionalinfo'
        ).execute()
        self.assertEqual(result.get_affected_items_count(), 2)
        self.assertEqual(result1.get_affected_items_count(), 1)
        self.assertEqual(result2.get_affected_items_count(), 0)
        self.assertEqual(result3.get_affected_items_count(), 1)
        self.schema.drop_collection("mycoll4")
