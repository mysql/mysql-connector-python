# Copyright (c) 2021, 2022, Oracle and/or its affiliates.
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
import unittest

import mysqlx
import tests


@unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 25), "XPlugin not compatible")
class CollectionReplaceRemoveOneTests(tests.MySQLxTests):
    """Tests for collection.replace_one."""

    def _drop_collection_if_exists(self, name):
        collection = self.schema.get_collection(name)
        if collection.exists_in_database():
            self.schema.drop_collection(name)

    @tests.foreach_session()
    def test_collection_replace_one1(self):
        """Test collection.replace_one to replace one doc."""
        self._drop_collection_if_exists("mycoll1")
        collection = self.schema.create_collection("mycoll1")
        collection.add(
            {"name": "a", "age": 21},
            {"name": "b", "age": 22},
            {"name": "c", "age": 23},
        ).execute()
        result = collection.find("$.age == 21").execute().fetch_one()
        self.assertEqual(result["name"], "a")
        result["name"] = "abc"
        collection.replace_one(result["_id"], result)
        result = collection.find("$.age == 21").execute().fetch_one()
        self.assertEqual(result["name"], "abc")
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_collection_replace_one2(self):
        """Test replacing multiple values of same doc usingi
        collection.replace_one."""
        self._drop_collection_if_exists("mycoll2")
        collection = self.schema.create_collection("mycoll2")
        collection.add(
            {"name": "a", "age": 21, "company": "pqr"},
            {"name": "b", "age": 21, "company": "xyz"},
        ).execute()
        result = collection.find("age == 21").execute().fetch_one()
        self.assertEqual(result["name"], "a")
        self.assertEqual(result["company"], "pqr")
        result["name"] = "abc"
        result["company"] = "mnc"
        collection.replace_one(result["_id"], result)
        result = collection.find("age == 21").execute().fetch_one()
        self.assertEqual(result["name"], "abc")
        self.assertEqual(result["company"], "mnc")
        self.schema.drop_collection("mycoll2")

    @tests.foreach_session()
    def test_collection_replace_one3(self):
        """Test collection.replace_one() succeeds even when there are no
        matching docs found."""
        self._drop_collection_if_exists("mycoll3")
        collection = self.schema.create_collection("mycoll3")
        collection.add(
            {"name": "a", "age": 21, "company": "pqr"},
            {"name": "b", "age": 21, "company": "xyz"},
        ).execute()
        result = collection.find("age == 21").execute().fetch_one()
        self.assertEqual(result["name"], "a")
        self.assertEqual(result["company"], "pqr")
        result["name"] = "abc"
        result["company"] = "mnc"
        self.assertRaises(
            mysqlx.ProgrammingError, collection.replace_one, "1", result
        )
        self.schema.drop_collection("mycoll3")

    @tests.foreach_session()
    def test_collection_replace_one4(self):
        """Test id cannot be replaced by collection.replace_one."""
        self._drop_collection_if_exists("mycoll4")
        collection = self.schema.create_collection("mycoll4")
        collection.add(
            {"name": "a", "age": 21},
            {"name": "b", "age": 22},
            {"name": "c", "age": 23},
        ).execute()
        result = collection.find("$.age == 21").execute().fetch_one()
        self.assertEqual(result["name"], "a")
        try:
            result["_id"] = 1
        except mysqlx.ProgrammingError as e1:
            # Expected ProgrammingError
            pass
        self.schema.drop_collection("mycoll4")

    @tests.foreach_session()
    def test_collection_add_or_replace_one1(self):
        """Test collection.add_or_replace_one to replace if the id already
        exists or add if not."""
        self._drop_collection_if_exists("mycoll1")
        collection = self.schema.create_collection("mycoll1")
        collection.add(
            {"name": "a", "age": 21},
            {"name": "b", "age": 22},
            {"name": "c", "age": 23},
        ).execute()
        result = collection.find("$.age == 21").execute().fetch_one()
        self.assertEqual(result["name"], "a")
        result["name"] = "abc"
        collection.replace_one(result["_id"], result)
        result = collection.find("$.age == 21").execute().fetch_one()
        self.assertEqual(result["name"], "abc")
        result = collection.find("_id == 1").execute().fetch_all()
        self.assertEqual(len(result), 0)
        upsert = {"name": "new", "age": 30}
        collection.add_or_replace_one(
            1, upsert
        )  # id doesn't exist, so it adds with that id
        result = collection.find("_id == 1").execute().fetch_one()
        self.assertEqual(result["name"], "new")
        upsert1 = {"name": "brand_new", "age": 25}
        collection.add_or_replace_one(
            1, upsert1
        )  # Using the existing id, so it replaces doc of that id
        result = collection.find("_id == 1").execute().fetch_one()
        self.assertEqual(result["name"], "brand_new")
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_collection_add_or_replace_one2(self):
        """Test collection.add_or_replace_one when a unique key matches
        passes, as index is not unique (not supported yet)."""
        self._drop_collection_if_exists("mycoll2")
        collection = self.schema.create_collection("mycoll2")
        index_name = "age_idx"
        collection.create_index(
            index_name,
            {"fields": [{"field": "$.age", "type": "INT", "required": True}]},
        ).execute()
        collection.add(
            {"_id": 1, "name": "a", "age": 21},
            {"_id": 2, "name": "b", "age": 22},
            {"_id": 3, "name": "c", "age": 23},
        ).execute()
        result = collection.find("$.age == 21").execute().fetch_one()
        self.assertEqual(result["name"], "a")
        result["name"] = "abc"
        collection.replace_one(result["_id"], result)
        result = collection.find("$.age == 21").execute().fetch_one()
        self.assertEqual(result["name"], "abc")
        upsert = {"name": "new", "age": 22}
        collection.add_or_replace_one(
            3, upsert
        )  # id exist, so it adds with that id
        result = collection.find("_id == 3").execute().fetch_one()
        self.assertEqual(result["name"], "new")
        self.schema.drop_collection("mycoll2")

    @tests.foreach_session()
    def test_collection_add_or_replace_one3(self):
        """Test collection.add_or_replace_one when 2 different keys matches
        since index is not unique, duplicate data will be added."""
        self._drop_collection_if_exists("mycoll3")
        collection = self.schema.create_collection("mycoll3")
        index_name1 = "age_idx"
        index_name2 = "comp_idx"
        collection.create_index(
            index_name1,
            {"fields": [{"field": "$.age", "type": "INT", "required": True}]},
        ).execute()
        collection.create_index(
            index_name2,
            {"fields": [{"field": "$.comp", "type": "INT", "required": True}]},
        ).execute()
        collection.add(
            {"_id": 1, "name": "a", "age": 21, "comp": 123},
            {"_id": 2, "name": "b", "age": 22, "comp": 789},
            {"_id": 3, "name": "c", "age": 23, "comp": 369},
        ).execute()
        result = collection.find("$.age == 21").execute().fetch_one()
        self.assertEqual(result["name"], "a")
        result["name"] = "abc"
        collection.replace_one(result["_id"], result)
        result = collection.find("$.age == 21").execute().fetch_one()
        self.assertEqual(result["name"], "abc")
        upsert = {"comp": 369, "age": 22}
        collection.add_or_replace_one(
            1, upsert
        )  # id exist, so it adds with that id
        result = collection.find("_id == 1").execute().fetch_one()
        self.assertEqual(result["comp"], 369)
        self.schema.drop_collection("mycoll3")

    @tests.foreach_session()
    def test_collection_get_one1(self):
        """Test collection.replace_one to replace one doc."""
        self._drop_collection_if_exists("mycoll1")
        collection = self.schema.create_collection("mycoll1")
        collection.add(
            {"name": "joy", "age": 21},
            {"name": "happy", "age": 22},
            {"name": "sad", "age": 23},
        ).execute()
        result = collection.find("$.name == 'joy'").execute().fetch_one()
        result1 = collection.get_one(result["_id"])
        self.assertEqual(result1["name"], "joy")
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_collection_get_one2(self):
        """Test get_one() should return None if matching ID is not found."""
        self._drop_collection_if_exists("mycoll2")
        collection = self.schema.create_collection("mycoll2")
        collection.add(
            {"name": "joy", "age": 21},
            {"name": "happy", "age": 22},
            {"name": "sad", "age": 23},
        ).execute()
        _ = collection.find("$.name == 'joy'").execute().fetch_one()
        result1 = collection.get_one(2)
        self.assertEqual(result1, None)
        self.schema.drop_collection("mycoll2")

    @tests.foreach_session()
    def test_collection_remove_one1(self):
        """Test remove_one() removes the doc of matching ID."""
        self._drop_collection_if_exists("mycoll1")
        collection = self.schema.create_collection("mycoll1")
        collection.add(
            {"name": "joy", "age": 21},
            {"name": "happy", "age": 22},
            {"name": "sad", "age": 23},
        ).execute()
        result = collection.find("$.name == 'joy'").execute().fetch_one()
        result = collection.remove_one(result["_id"])
        result = collection.find("$.name == 'joy'").execute().fetch_all()
        self.assertEqual(len(result), 0)
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_collection_remove_one2(self):
        """Test remove_one() when matching ID not found - succeeds and
        returns 0 as affected number of docs."""
        self._drop_collection_if_exists("mycoll2")
        collection = self.schema.create_collection("mycoll2")
        collection.add(
            {"name": "joy", "age": 21},
            {"name": "happy", "age": 22},
            {"name": "sad", "age": 23},
        ).execute()
        result = collection.remove_one(4)
        self.assertEqual(result.get_affected_items_count(), 0)
        self.schema.drop_collection("mycoll2")

    @tests.foreach_session()
    def test_collection_replace1(self):
        """Test None as the value."""
        self._drop_collection_if_exists("mycoll2")
        collection = self.schema.create_collection("mycoll1")
        collection.add(
            {"name": "joy", "age": 21},
            {"name": "happy", "age": 22},
            {"name": "sad", "age": 23},
        ).execute()
        result = collection.find("$.age == 21").execute().fetch_one()
        self.assertEqual(result["name"], "joy")
        result["name"] = "abc"
        self.assertRaises(
            mysqlx.ProgrammingError, collection.replace_one, None, result
        )
        result["name"] = "new"
        self.assertRaises(
            mysqlx.ProgrammingError, collection.replace_one, "", result
        )
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_collection_replace2(self):
        """Test None as the value."""
        self._drop_collection_if_exists("mycoll2")
        collection = self.schema.create_collection("mycoll2")
        collection.add(
            {"name": "joy", "age": 21},
            {"name": "happy", "age": 22},
            {"name": "sad", "age": 23},
        ).execute()
        upsert = {"name": "new", "age": 30}
        collection.add_or_replace_one(None, upsert)
        result = collection.find().execute().fetch_all()
        self.assertEqual(len(result), 4)
        result = collection.find("$.age == 30").execute().fetch_all()
        self.assertIsNotNone(result)
        upsert = {"name": "empty", "age": 20}
        collection.add_or_replace_one("", upsert)
        result = collection.find("$.age == 20").execute().fetch_all()
        self.assertNotEqual(result, "")
        result = collection.find().execute().fetch_all()
        self.assertEqual(len(result), 5)
        self.schema.drop_collection("mycoll2")

    @unittest.skipUnless(
        tests.ARCH_64BIT, "Test available only for 64 bit platforms"
    )
    @unittest.skipIf(os.name == "nt", "Test not available for Windows")
    @tests.foreach_session()
    def test_collection_replace3(self):
        """Test None as the value."""
        self._drop_collection_if_exists("mycoll3")
        collection = self.schema.create_collection("mycoll3")
        collection.add(
            {"name": "joy", "age": 21},
            {"name": "happy", "age": 22},
            {"name": "sad", "age": 23},
        ).execute()
        result = collection.get_one(None)
        result1 = collection.get_one("")
        result2 = collection.get_one(-1)
        result3 = collection.get_one(875984758945556566)
        self.assertIsNone(result)
        self.assertIsNone(result1)
        self.assertIsNone(result2)
        self.assertIsNone(result3)
        self.schema.drop_collection("mycoll3")

    @unittest.skipUnless(
        tests.ARCH_64BIT, "Test available only for 64 bit platforms"
    )
    @unittest.skipIf(os.name == "nt", "Test not available for Windows")
    @tests.foreach_session()
    def test_collection_replace4(self):
        """Test None as the value."""
        self._drop_collection_if_exists("mycoll4")
        collection = self.schema.create_collection("mycoll4")
        collection.add(
            {"_id": 8737358945984785675, "name": "joy", "age": 21},
            {"name": "happy", "age": 22},
            {"name": "sad", "age": 23},
        ).execute()
        result = collection.remove_one(None)
        result1 = collection.remove_one("")
        result2 = collection.remove_one(-1)
        result3 = collection.remove_one(875984758945556566)
        self.assertEqual(result.get_affected_items_count(), 0)
        self.assertEqual(result1.get_affected_items_count(), 0)
        self.assertEqual(result2.get_affected_items_count(), 0)
        self.assertEqual(result3.get_affected_items_count(), 0)
        self.schema.drop_collection("mycoll4")
