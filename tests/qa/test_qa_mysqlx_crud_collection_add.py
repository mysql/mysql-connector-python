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


def is_unique_id(lst):
    """To test if the generated IDs are unique."""
    if len(lst) == len(set(lst)):
        return True
    return False


def getmac(interface):
    """To get the MAC address."""
    try:
        mac = open(f"/sys/class/net/{interface}/address").readline()
    except:
        mac = "00:00:00:00:00:00"
    return mac[0:17]


def create_json_without_id():
    """Create big JSON doc without _id."""
    data = []
    for i in range(-50, 60):
        data1 = {"name": "Sam", "age": i}
        data.append(data1)
    return data


def create_json_with_id():
    """Create big JSON doc with _id."""
    data = []
    for idx in range(-50, 60):
        data1 = {"_id": idx, "name": "Ram", "age": idx}
        data.append(data1)
    return data


@unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 25), "XPlugin not compatible")
class CollectionAddTests(tests.MySQLxTests):
    """Tests for collection.add()."""

    def _drop_collection_if_exists(self, name):
        collection = self.schema.get_collection(name)
        if collection.exists_in_database():
            self.schema.drop_collection(name)

    @tests.foreach_session()
    def test_collection_add_test1(self):
        """Test collection."""
        self._drop_collection_if_exists("mycoll1")
        collection = self.schema.create_collection("mycoll1")
        result = collection.add({"name": "a"}, {"name": "b"}).execute()
        self.assertEqual(collection.count(), 2)
        self.assertEqual(len(result.get_generated_ids()), 2)
        self.assertEqual(len(result.get_generated_ids()[0]), 28)
        self.assertRegex(result.get_generated_ids()[0], r"[a-f0-9]{28}")
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_collection_add_test2(self):
        """Test the collection.add with empty document."""
        self._drop_collection_if_exists("mycoll2")
        collection = self.schema.create_collection("mycoll2")
        collection.add().execute()
        self.schema.drop_collection("mycoll2")

    @tests.foreach_session()
    def test_collection_test1(self):
        """Test the get_name()."""
        self._drop_collection_if_exists("mycoll7")
        collection = self.schema.create_collection("mycoll7")
        self.assertEqual(collection.get_name(), "mycoll7")
        self.schema.drop_collection("mycoll7")

    @tests.foreach_session()
    def test_collection_test2(self):
        """Test the get_schema()."""
        self._drop_collection_if_exists("mycoll8")
        config = tests.get_mysqlx_config()
        collection = self.schema.create_collection("mycoll8")
        self.assertEqual(collection.get_schema().name, config["schema"])
        self.schema.drop_collection("mycoll8")

    @tests.foreach_session()
    def test_collection_test8(self):
        """Test the with big name."""
        self._drop_collection_if_exists(
            "mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm7"
        )
        collection = self.schema.create_collection(
            "mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm7",
        )
        self.assertEqual(
            collection.get_name(),
            "mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm7",
        )
        self.schema.drop_collection(
            "mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm7"
        )

    @tests.foreach_session()
    def test_collection_test4(self):
        """Test the exists_in_database()."""
        self._drop_collection_if_exists("mycoll13")
        collection = self.schema.create_collection("mycoll13")
        self.assertTrue(collection.exists_in_database())
        self.schema.drop_collection("mycoll13")

    @tests.foreach_session()
    def test_collection_test5(self):
        """Test the get_generated_ids."""
        self._drop_collection_if_exists("mycoll14")
        collection = self.schema.create_collection("mycoll14")
        result = collection.add(
            {"_id": "la", "a": 1, "b": 2, "c": 100},
            {"_id": 2, "a": 2, "b": 1, "c": 200},
            {"a": 3, "b": 2, "c": 300},
        ).execute()
        self.assertEqual(len(result.get_generated_ids()), 1)
        self.schema.drop_collection("mycoll14")

    @tests.foreach_session()
    def test_collection_test6(self):
        """Testing the get_generated_ids."""
        self._drop_collection_if_exists("mycoll15")
        collection = self.schema.create_collection("mycoll15")
        result = collection.add(
            {"_id": 1, "a": 1, "b": 2, "c": 100},
            {"_id": 2, "a": 2, "b": 1, "c": 200},
            {"_id": 3, "a": 3, "b": 2, "c": 300},
        ).execute()
        self.assertEqual(len(result.get_generated_ids()), 0)
        self.schema.drop_collection("mycoll15")

    @tests.foreach_session()
    def test_collection_test7(self):
        """Test the get_generated_ids with multiple add() methods."""
        self._drop_collection_if_exists("mycoll16")
        collection = self.schema.create_collection("mycoll16")
        result = (
            collection.add({"a": 1, "b": 2, "c": 100})
            .add({"_id": 2, "a": 2, "b": 1, "c": 200})
            .add({"a": 3, "b": 2, "c": 300})
            .add({})
            .execute()
        )
        self.assertEqual(len(result.get_generated_ids()), 3)
        self.assertTrue(
            result.get_generated_ids()[0]
            < result.get_generated_ids()[1]
            < result.get_generated_ids()[2]
        )
        self.schema.drop_collection("mycoll16")

    @tests.foreach_session()
    def test_collection_add_test3(self):
        """MCPY-384 add an empty array to a collection which doesnt exist."""
        self._drop_collection_if_exists("mycoll17")
        collection = self.schema.create_collection("mycoll17")
        collection.add().execute()
        self.schema.drop_collection("mycoll17")

    @tests.foreach_session()
    def test_collection_add_test4(self):
        """Test the parameter list."""
        self._drop_collection_if_exists("mycoll18")
        collection = self.schema.create_collection("mycoll18")
        collection.add({"name": "a"}, {"name": "b"}, {"name": "c"}).execute()
        self.assertEqual(collection.count(), 3)
        self.schema.drop_collection("mycoll18")

    @tests.foreach_session()
    def test_collection_add_id_test1(self):
        """Test if _id is preserved when JSON doc contains _id field and not
        overridden by generated ID."""
        self._drop_collection_if_exists("mycoll22")
        collection = self.schema.create_collection("mycoll22")
        result = collection.add({"_id": 1, "name": "a"}).execute()
        self.assertEqual(len(result.get_generated_ids()), 0)
        self.schema.drop_collection("mycoll22")

    @tests.foreach_session()
    def test_collection_add_id_test2(self):
        """Test if ID is generated for multiple JSON docs when _id field is
        not provided."""
        self._drop_collection_if_exists("mycoll23")
        collection = self.schema.create_collection("mycoll23")
        result = (
            collection.add({"name": "a", "a": 1, "b": 2, "c": 100})
            .add({"name": "b", "a": 2, "b": 3, "c": 200})
            .add({"name": "c", "a": 3, "b": 4, "c": 300})
            .add({"name": "d", "a": 4, "b": 5, "c": 400})
            .add({"name": "e", "a": 5, "b": 6, "c": 500})
            .execute()
        )
        self.assertEqual(len(result.get_generated_ids()), collection.count())
        self.assertTrue(is_unique_id(result.get_generated_ids()))
        self.schema.drop_collection("mycoll23")

    @tests.foreach_session()
    def test_collection_add_id_test3(self):
        """Test if ID is generated for multiple JSON docs when _id field is
        not provided."""
        self._drop_collection_if_exists("mycoll24")
        collection = self.schema.create_collection("mycoll24")
        result = collection.add(
            {"name": "a", "a": 1, "b": 2, "c": 100},
            {"name": "b", "a": 2, "b": 3, "c": 200},
            {"name": "c", "a": 3, "b": 4, "c": 300},
            {"name": "d", "a": 4, "b": 5, "c": 400},
            {"name": "e", "a": 5, "b": 6, "c": 500},
        ).execute()
        self.assertEqual(len(result.get_generated_ids()), collection.count())
        self.assertTrue(is_unique_id(result.get_generated_ids()))
        self.schema.drop_collection("mycoll24")

    @tests.foreach_session()
    def test_collection_add_id_test4(self):
        """Test ID generation when few docs contain _id while others doesn't
        contain."""
        self._drop_collection_if_exists("mycoll25")
        collection = self.schema.create_collection("mycoll25")
        result = collection.add(
            {"name": "a", "a": 1, "b": 2, "c": 100},
            {"name": "b", "a": 2, "b": 3, "c": 200},
            {"_id": 1, "name": "c", "a": 3, "b": 4, "c": 300},
            {"_id": 2, "name": "d", "a": 4, "b": 5, "c": 400},
            {"name": "e", "a": 5, "b": 6, "c": 500},
        ).execute()
        self.assertEqual(len(result.get_generated_ids()), 3)
        self.assertEqual(collection.count(), 5)
        self.assertTrue(is_unique_id(result.get_generated_ids()))
        self.schema.drop_collection("mycoll25")

    @tests.foreach_session()
    def test_collection_add_id_test5(self):
        """Test ID is not generated when multiple docs are added with
        _id field."""
        self._drop_collection_if_exists("mycoll26")
        collection = self.schema.create_collection("mycoll26")
        result = collection.add(
            {"_id": 1, "name": "a", "a": 1, "b": 2, "c": 100},
            {"_id": 2, "name": "b", "a": 2, "b": 3, "c": 200},
            {"_id": 3, "name": "c", "a": 3, "b": 4, "c": 300},
            {"_id": 4, "name": "d", "a": 4, "b": 5, "c": 400},
            {"_id": 5, "name": "e", "a": 5, "b": 6, "c": 500},
        ).execute()
        self.assertEqual(len(result.get_generated_ids()), 0)
        self.assertEqual(collection.count(), 5)
        res = result.get_generated_ids()
        self.assertEqual(
            res,
            [],
            "IDs are generated while they are not expected ",
        )
        self.schema.drop_collection("mycoll26")

    @tests.foreach_session()
    def test_collection_add_id_test6(self):
        """Testing IDs are not duplicate when data added without _id filed
        from multiple sessions to the same collection."""
        config = tests.get_mysqlx_config()
        session1 = mysqlx.get_session(config)
        schema1 = session1.get_schema(config["schema"])
        self._drop_collection_if_exists("mycoll27")
        collection = self.schema.create_collection("mycoll27")
        collection1 = schema1.get_collection("mycoll27")
        result = collection.add(
            {"name": "a", "a": 1, "b": 2, "c": 100},
            {"name": "b", "a": 2, "b": 3, "c": 200},
            {"name": "c", "a": 3, "b": 4, "c": 300},
            {"name": "d", "a": 4, "b": 5, "c": 400},
            {"name": "e", "a": 5, "b": 6, "c": 500},
        ).execute()
        result1 = collection1.add(
            {"name": "a", "a": 1, "b": 2, "c": 100},
            {"name": "b", "a": 2, "b": 3, "c": 200},
            {"name": "c", "a": 3, "b": 4, "c": 300},
            {"name": "d", "a": 4, "b": 5, "c": 400},
            {"name": "e", "a": 5, "b": 6, "c": 500},
        ).execute()
        overall_ids = result.get_generated_ids() + result1.get_generated_ids()
        self.assertEqual(len(overall_ids), collection.count())
        self.assertTrue(is_unique_id(overall_ids))
        self.schema.drop_collection("mycoll27")

    @tests.foreach_session()
    def test_collection_add_id_test7(self):
        """Test ID is generated when different values are provided to _id
        field."""
        self._drop_collection_if_exists("mycoll28")
        collection = self.schema.create_collection("mycoll28")
        result = collection.add(
            {"_id": -11, "name": "a", "a": 1, "b": 2, "c": 100}
        ).execute()
        self.assertEqual(len(result.get_generated_ids()), 0)
        res = collection.find("$.name == 'a'").execute()
        mydoc = res.fetch_all()[0]
        self.assertEqual(mydoc["_id"], -11)
        self.schema.drop_collection("mycoll28")

    @tests.foreach_session()
    def test_collection_add_id_test8(self):
        """Test when _id is given value 0."""
        self._drop_collection_if_exists("mycoll29")
        collection = self.schema.create_collection("mycoll29")
        result = collection.add(
            {"_id": 0, "name": "a", "a": 1, "b": 2, "c": 100}
        ).execute()
        self.assertEqual(len(result.get_generated_ids()), 0)
        res = collection.find("$.name == 'a'").execute().fetch_all()[0]
        self.assertEqual(res["_id"], 0)
        self.schema.drop_collection("mycoll29")

    @unittest.skipUnless(
        tests.ARCH_64BIT, "Test available only for 64 bit platforms"
    )
    @unittest.skipIf(os.name == "nt", "Test not available for Windows")
    @tests.foreach_session()
    def test_collection_add_id_test9(self):
        """Test when _id is given big positive number."""
        self._drop_collection_if_exists("mycoll30")
        collection = self.schema.create_collection("mycoll30")
        result = collection.add(
            {"_id": 876848644738567, "name": "a", "a": 1, "b": 2, "c": 100}
        ).execute()
        self.assertEqual(len(result.get_generated_ids()), 0)
        res = collection.find("$.name == 'a'").execute().fetch_all()[0]
        self.assertEqual(res["_id"], 876848644738567)
        self.schema.drop_collection("mycoll30")

    @tests.foreach_session()
    def test_collection_add_id_test10(self):
        """Test when _id is given string."""
        self._drop_collection_if_exists("mycoll31")
        collection = self.schema.create_collection("mycoll31")
        result = collection.add(
            {"_id": "It is my ID", "name": "a", "a": 1, "b": 2, "c": 100}
        ).execute()
        self.assertEqual(len(result.get_generated_ids()), 0)
        res = collection.find("$.name == 'a'").execute().fetch_all()[0]
        self.assertEqual(res["_id"], "It is my ID")
        self.schema.drop_collection("mycoll31")

    @tests.foreach_session()
    def test_collection_add_id_test11(self):
        """Test if ID is generated for multiple JSON docs when _id field is
        not provided."""
        self._drop_collection_if_exists("mycoll32")
        collection = self.schema.create_collection("mycoll32")
        result = (
            collection.add({"_id": 101, "name": "a", "a": 1, "b": 2, "c": 100})
            .add(create_json_without_id())
            .add({"name": "c", "a": 3, "b": 4, "c": 300})
            .add({"name": "d", "a": 4, "b": 5, "c": 400})
            .add({"name": "e", "a": 5, "b": 6, "c": 500})
            .execute()
        )
        self.assertEqual(len(result.get_generated_ids()), 113)
        self.assertEqual(collection.count(), 114)
        self.assertTrue(is_unique_id(result.get_generated_ids()))
        for idx in range(0, 112):
            self.assertTrue(
                result.get_generated_ids()[idx]
                < result.get_generated_ids()[idx + 1]
            )
            self.assertRegex(result.get_generated_ids()[idx], r"[a-f0-9]{28}")
            idx = idx + 1
        self.schema.drop_collection("mycoll32")

    @tests.foreach_session()
    def test_collection_add_id_test12(self):
        """Test to ensure that the generated ID depends on client's MAC."""
        self._drop_collection_if_exists("mycoll33")
        collection = self.schema.create_collection("mycoll33")
        result = (
            collection.add({"name": "a", "a": 1, "b": 2, "c": 100})
            .add({"name": "b", "a": 2, "b": 3, "c": 200})
            .add({"name": "c", "a": 3, "b": 4, "c": 300})
            .add({"name": "d", "a": 4, "b": 5, "c": 400})
            .add({"name": "e", "a": 5, "b": 6, "c": 500})
            .execute()
        )
        self.assertEqual(len(result.get_generated_ids()), collection.count())
        self.assertEqual(len(result.get_generated_ids()), 5)
        self.assertTrue(is_unique_id(result.get_generated_ids()))
        self.schema.drop_collection("mycoll33")

    @tests.foreach_session()
    def test_collection_add_id_test13(self):
        """Test if ID is generated with big JSON docs without _id field."""
        self._drop_collection_if_exists("mycoll34")
        collection = self.schema.create_collection("mycoll34")
        result = collection.add(create_json_without_id()).execute()
        self.assertEqual(len(result.get_generated_ids()), collection.count())
        self.assertTrue(is_unique_id(result.get_generated_ids()))
        self.schema.drop_collection("mycoll34")

    @tests.foreach_session()
    def test_collection_add_id_test14(self):
        """Test ID is not generated by adding big JSON docs with _id field."""
        self._drop_collection_if_exists("mycoll35")
        collection = self.schema.create_collection("mycoll35")
        result = collection.add(create_json_with_id()).execute()
        self.assertEqual(len(result.get_generated_ids()), 0)
        self.assertEqual(collection.count(), 110)
        self.schema.drop_collection("mycoll35")

    @tests.foreach_session()
    def test_collection_add_id_test15(self):
        """Test adding duplicate values to _id. User should get error."""
        self._drop_collection_if_exists("mycoll36")
        collection = self.schema.create_collection("mycoll36")
        self.assertRaises(
            mysqlx.errors.OperationalError,
            collection.add({"_id": "abcde1234", "name": "myname1", "age": 28})
            .add({"_id": "abcde1234", "name": "myname2", "age": 30})
            .execute,
        )
        self.schema.drop_collection("mycoll36")

    @tests.foreach_session()
    def test_collection_add_id_test16(self):
        """Test inserting invalid values to _id."""
        self._drop_collection_if_exists("mycoll37")
        collection = self.schema.create_collection("mycoll37")
        res = collection.add(
            {"_id": "", "name": "myname1", "age": 28}
        ).execute()
        res1 = collection.add(
            {"_id": None, "name": "myname2", "age": 30}
        ).execute()
        result = (
            collection.find()
            .fields("_id", "name", "age")
            .sort("age")
            .execute()
        )
        self.assertEqual(len(res.get_generated_ids()), 0)
        self.assertEqual(collection.count(), 2)
        row = result.fetch_all()
        self.assertEqual(row[0]["_id"], "")
        self.assertEqual(row[1]["_id"], None)
        self.schema.drop_collection("mycoll37")

    @tests.foreach_session()
    def test_dbdoc_test1(self):
        self._drop_collection_if_exists("mycoll1")
        collection = self.schema.create_collection("mycoll1")
        collection.add(
            {"_id": 1, "name": "a"}, {"_id": 2, "name": "b"}
        ).execute()
        self.assertEqual(collection.count(), 2)
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_dbdoc_test2(self):
        """Test the json array."""
        data = [{"_id": 1, "a": "b"}, {"_id": 2, "a": "c"}]
        self._drop_collection_if_exists("mycoll2")
        collection = self.schema.create_collection("mycoll2")
        collection.add(*data).execute()
        result = collection.find().execute()
        self.schema.drop_collection("mycoll2")

    @tests.foreach_session()
    def test_dbdoc_test3(self):
        """Test the json doc."""
        data = {
            "_id": 1,
            "name": "abc",
        }
        self._drop_collection_if_exists("mycoll3")
        collection = self.schema.create_collection("mycoll3")
        collection.add(data).execute()
        result = collection.find().execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["_id"], 1)
        self.schema.drop_collection("mycoll3")

    @tests.foreach_session()
    def test_dbdoc_test4(self):
        """Test the json doc array."""
        mydoc = [None] * 5
        for i in range(0, 5):
            mydoc[i] = {}
            mydoc[i]["_id"] = i
            mydoc[i]["name"] = "abc"
            mydoc[i]["age"] = i * 10
        self._drop_collection_if_exists("mycoll4")
        collection = self.schema.create_collection("mycoll4")
        collection.add(*mydoc).execute()
        self.assertEqual(collection.count(), 5)
        self.schema.drop_collection("mycoll4")
