# Copyright (c) 2021, 2023, Oracle and/or its affiliates.
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

import datetime
import unittest

import mysqlx

import tests


@unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 25), "XPlugin not compatible")
class CollectionAddTests(tests.MySQLxTests):
    """Tests for collection.modify()."""

    def _drop_collection_if_exists(self, name):
        collection = self.schema.get_collection(name)
        if collection.exists_in_database():
            self.schema.drop_collection(name)

    @tests.foreach_session()
    def test_collection_modify1(self):
        """Test basic collection.modify."""
        self._drop_collection_if_exists("mycoll1")
        collection = self.schema.create_collection("mycoll1")
        collection.add({"_id": 1, "name": "a"}, {"_id": 2, "name": "b"}).execute()
        collection.modify("$._id==2").set("$.name", "c").execute()
        result = collection.find("$._id==2").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["name"], "c")
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_collection_modify2(self):
        """Test the collection.modify without condition."""
        self._drop_collection_if_exists("mycoll2")
        collection = self.schema.create_collection("mycoll2")
        collection.add({"_id": 1, "name": "a"}, {"_id": 2, "name": "b"}).execute()
        try:
            collection.modify().set("$.name", "c").execute()
        except TypeError:
            # Expected a TypeError:
            # modify() missing 1 required positional argument: 'condition'
            pass
        self.schema.drop_collection("mycoll2")

    @tests.foreach_session()
    def test_collection_modify3(self):
        """Test the collection.modify UNSET."""
        self._drop_collection_if_exists("mycoll3")
        collection = self.schema.create_collection("mycoll3")
        collection.add({"_id": 1, "name": "a"}, {"_id": 2, "name": "b"}).execute()
        collection.modify("$._id==2").set("$.name", "c").execute()
        result = collection.modify("$._id==1").unset("$.name").execute()
        self.assertEqual(result.get_affected_items_count(), 1)
        self.schema.drop_collection("mycoll3")

    @tests.foreach_session()
    def test_collection_modify4(self):
        """Test the collection.modify change."""
        self._drop_collection_if_exists("mycoll4")
        collection = self.schema.create_collection("mycoll4")
        collection.add({"_id": 1, "name": "a"}, {"_id": 2, "name": "b"}).execute()
        collection.modify("$._id==2").set("age", 21).execute()
        collection.modify("$._id ==2").change(
            "age", 22
        ).execute()  # change() deprecated since 8.0.12
        result = collection.find("$._id==2").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["age"], 22)
        self.schema.drop_collection("mycoll4")

    @tests.foreach_session()
    def test_collection_modify5(self):
        """Test the collection.modify set and sort for entire collection
        using TRUE."""
        self._drop_collection_if_exists("mycoll5")
        collection = self.schema.create_collection("mycoll5")
        collection.add(
            {"_id": 2, "c1": 9},
            {"_id": 3, "c1": 8},
            {"_id": 4, "c1": 7},
            {"_id": 5, "c1": 6},
            {"_id": 6, "c1": 5},
            {"_id": 7, "c1": 4},
            {"_id": 8, "c1": 3},
            {"_id": 9, "c1": 2},
        ).execute()
        collection.modify("TRUE").set("$.c1", 100).sort("$._id DESC").limit(3).execute()
        result = collection.find("$._id ==8").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["c1"], 100)
        self.schema.drop_collection("mycoll5")

    @tests.foreach_session()
    def test_collection_modify6(self):
        """Test the collection.modify set bind."""
        self._drop_collection_if_exists("mycoll6")
        collection = self.schema.create_collection("mycoll6")
        collection.add(
            {"_id": 2, "c1": 9},
            {"_id": 3, "c1": 8},
            {"_id": 4, "c1": 7},
            {"_id": 5, "c1": 6},
            {"_id": 6, "c1": 5},
            {"_id": 7, "c1": 4},
            {"_id": 8, "c1": 3},
            {"_id": 9, "c1": 2},
        ).execute()
        collection.modify("$._id == :_id").bind("_id", 2).set("$.c1", 100).execute()
        result = collection.find("$._id ==2").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["c1"], 100)
        self.schema.drop_collection("mycoll6")

    @tests.foreach_session()
    def test_collection_modify7(self):
        """Test the collection.modify.array-insert."""
        self._drop_collection_if_exists("mycoll7")
        collection = self.schema.create_collection("mycoll7")
        collection.add({"_id": 1, "no": [10, 20, 30]}).execute()
        collection.modify("$._id==1").array_insert("$.no[3]", 2).execute()
        result = collection.find("$._id==1").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["no"], [10, 20, 30, 2])
        self.schema.drop_collection("mycoll7")

    @tests.foreach_session()
    def test_collection_modify8(self):
        """Test the collection.modify.array-append."""
        self._drop_collection_if_exists("mycoll8")
        collection = self.schema.create_collection("mycoll8")
        collection.add(
            {
                "_id": 1,
                "no": [10, 20, 30],
                "dates": "24/04/2014",
                "email": "my@email.com",
            }
        ).execute()
        collection.modify("$._id==1").array_append("$.no", 2).execute()
        collection.modify("$._id==1").array_append("$.dates", "04/06/2018").execute()
        collection.modify("$._id==1").array_append(
            "$.email", mysqlx.expr("UPPER($.email)")
        ).execute()
        result = collection.find("$._id==1").execute()
        row = result.fetch_all()[0]
        self.assertEqual(row["no"][3], 2)
        self.assertEqual(row["dates"][1], "04/06/2018")
        self.assertEqual(row["email"][1], "MY@EMAIL.COM")
        self.schema.drop_collection("mycoll8")

    @tests.foreach_session()
    def test_collection_modify9(self):
        """Test the collection.modify.where for entire collection."""
        self._drop_collection_if_exists("mycoll9")
        collection = self.schema.create_collection("mycoll9")
        collection.add(
            {"_id": 1, "no": 10},
            {"_id": 2, "no": 20},
            {"_id": 3, "no": 30},
        ).execute()
        collection.modify("TRUE").set("$.no", 40).where(
            "$._id == 1"
        ).execute()  # where overwrites "true" condition
        result = collection.find().execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["no"], 40)
        self.assertEqual(row[1]["no"], 20)
        self.assertEqual(row[2]["no"], 30)
        self.schema.drop_collection("mycoll9")

    @tests.foreach_session()
    def test_collection_modify10(self):
        """Test the collection.modify set and sort and param."""
        self._drop_collection_if_exists("mycoll10")
        collection = self.schema.create_collection("mycoll10")
        collection.add(
            {"i": 1, "c1": 10},
            {"i": 2, "c1": 10},
            {"i": 1, "c1": 11},
            {"i": 2, "c1": 11},
        ).execute()
        collection.modify("$.c1 == 11").set("$.i", 100).sort(
            "$.i ASC", "$.c1 DESC"
        ).limit(1).execute()
        result = collection.find("$.c1 ==11").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["i"], 100)
        self.schema.drop_collection("mycoll10")

    @tests.foreach_session()
    def test_collection_modify11(self):
        """Test the collection.modify UNSET with param list."""
        self._drop_collection_if_exists("mycoll11")
        collection = self.schema.create_collection("mycoll11")
        collection.add(
            {"_id": 1, "name": "a", "age": 10},
            {"_id": 2, "name": "b", "age": 20},
        ).execute()
        result = collection.modify("$._id==1").unset("$.name", "$.age").execute()
        self.assertEqual(result.get_affected_items_count(), 1)
        self.schema.drop_collection("mycoll11")

    @tests.foreach_session()
    def test_collection_modify12(self):
        """Test the collection.modify with an empty string and is expected to
        return an exception."""
        self._drop_collection_if_exists("mycoll12")
        collection = self.schema.create_collection("mycoll12")
        collection.add(
            {"_id": 1, "name": "a", "age": 10},
            {"_id": 2, "name": "b", "age": 20},
        ).execute()
        try:
            collection.modify("").set("$.name", "c").execute()
        except mysqlx.ProgrammingError:
            pass
        self.schema.drop_collection("mycoll12")

    @tests.foreach_session()
    def test_collection_modify13(self):
        """Test the collection.modify with null condition and is expected to
        return an exception."""
        self._drop_collection_if_exists("mycoll13")
        collection = self.schema.create_collection("mycoll13")
        collection.add(
            {"_id": 1, "name": "a", "age": 10},
            {"_id": 2, "name": "b", "age": 20},
        ).execute()
        try:
            collection.modify(None).set("$.name", "c").execute()
        except mysqlx.ProgrammingError:
            pass
        self.schema.drop_collection("mycoll13")

    @tests.foreach_session()
    def test_collection_modify14(self):
        """Test the collection.modify.where for entire collection."""
        self._drop_collection_if_exists("mycoll14")
        collection = self.schema.create_collection("mycoll14")
        collection.add(
            {"_id": 1, "no": 10},
            {"_id": 2, "no": 20},
            {"_id": 3, "no": 30},
        ).execute()
        collection.modify("TRUE").set("$.no", 40).execute()
        result = collection.find().execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["no"], 40)
        self.assertEqual(row[1]["no"], 40)
        self.assertEqual(row[2]["no"], 40)
        self.schema.drop_collection("mycoll14")

    @tests.foreach_session()
    def test_collection_modify15(self):
        """Test the collection.modify.where with a condition which results
        into true."""
        self._drop_collection_if_exists("mycoll15")
        collection = self.schema.create_collection("mycoll15")
        collection.add(
            {"_id": 1, "name": "a", "age": 10},
            {"_id": 2, "name": "b", "age": 20},
        ).execute()
        collection.modify("1 == 1").set("$.name", "c").where("$._id == 2").execute()
        result = collection.find().execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["name"], "a")
        self.assertEqual(row[1]["name"], "c")
        self.schema.drop_collection("mycoll15")

    @tests.foreach_session()
    def test_collection_modify16(self):
        """Test the collection.modify with a condition which results into
        true."""
        self._drop_collection_if_exists("mycoll16")
        collection = self.schema.create_collection("mycoll16")
        collection.add(
            {"_id": 1, "name": "a", "age": 10},
            {"_id": 2, "name": "b", "age": 20},
        ).execute()
        collection.modify("1 == 1").set("$.name", "c").execute()
        result = collection.find().execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["name"], "c")
        self.assertEqual(row[1]["name"], "c")
        self.schema.drop_collection("mycoll16")

    @tests.foreach_session()
    def test_collection_modify17(self):
        """Test the collection.modify with a false condition.
        if where condition is provided then "false" condition will be
        overwritten and the document gets modified."""
        self._drop_collection_if_exists("mycoll17")
        collection = self.schema.create_collection("mycoll17")
        collection.add(
            {"_id": 1, "name": "a", "age": 10},
            {"_id": 2, "name": "b", "age": 20},
        ).execute()
        collection.modify("false").set("$.name", "c").execute()
        result = collection.find("$._id==2").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["name"], "b")
        self.schema.drop_collection("mycoll17")

    @tests.foreach_session()
    def test_collection_modify18(self):
        """Test the collection.modify with a false condition.
        If where condition is provided then "false" condition will be
        overwritten and the document is getting modified."""
        self._drop_collection_if_exists("mycoll18")
        collection = self.schema.create_collection("mycoll18")
        collection.add(
            {"_id": 1, "name": "a", "age": 10},
            {"_id": 2, "name": "b", "age": 20},
        ).execute()
        collection.modify("1 == 0").set("$.name", "c").execute()
        result = collection.find("$._id==2").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["name"], "b")
        self.schema.drop_collection("mycoll18")

    @tests.foreach_session()
    def test_contains_operator_coll_modify1(self):
        """Test in operator in collection.modify."""
        self._drop_collection_if_exists("mycoll1")
        collection = self.schema.create_collection("mycoll1")
        collection.add(
            {
                "_id": 1,
                "name": "a",
                "age": 22,
                "prof": {"a1": "x1", "b1": "y1"},
            },
            {"_id": 2, "name": "b", "age": 24},
        ).execute()
        result = collection.modify("$.name IN 'a'").set("$.name", "c").execute()
        result1 = (
            collection.modify("$.age IN [22,24]")
            .set("$.name", "changed_age*@#$")
            .execute()
        )
        result2 = collection.modify('{"a1":"x1"} IN $.prof').unset("$.age").execute()
        result3 = collection.modify('"x1" IN $.prof.a1').set("$.name", "xyz").execute()
        self.assertEqual(result.get_affected_items_count(), 1)
        self.assertEqual(result1.get_affected_items_count(), 2)
        self.assertEqual(result2.get_affected_items_count(), 1)
        self.assertEqual(result3.get_affected_items_count(), 1)
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_contains_operator_coll_modify2(self):
        """Test not In operator in collection.modify."""
        self._drop_collection_if_exists("mycoll2")
        collection = self.schema.create_collection("mycoll2")
        collection.add(
            {
                "_id": 1,
                "name": "a",
                "age": 22,
                "prof": {"a1": "x1", "b1": "y1"},
            },
            {"_id": 2, "name": "b", "age": 24},
        ).execute()
        result = collection.modify("$.name NOT IN 'a'").set("$.name", "c").execute()
        result1 = (
            collection.modify("$.age NOT IN [22,23]")
            .set("$.name", "changed_age*@#$")
            .execute()
        )
        result2 = (
            collection.modify('{"a1":"z1"} NOT IN $.prof').unset("$.age").execute()
        )
        result3 = (
            collection.modify('"z1" NOT IN $.prof.a1').set("$.name", "xyz").execute()
        )
        self.assertEqual(result.get_affected_items_count(), 1)
        self.assertEqual(result1.get_affected_items_count(), 1)
        self.assertEqual(result2.get_affected_items_count(), 1)
        self.assertEqual(result3.get_affected_items_count(), 1)
        self.schema.drop_collection("mycoll2")

    @tests.foreach_session()
    def test_contains_operator_coll_modify3(self):
        """Test IN operator with array/list operand on LHS and array/list
        on RHS."""
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
        result = (
            collection.modify(
                '["playing","painting","boxing"] IN $.additionalinfo.hobbies'
            )
            .set("$.additionalinfo.hobbies", "['xyz','pqr','abc']")
            .execute()
        )
        result1 = (
            collection.modify('["happy","joy"] IN $.name')
            .set("$.name", "abc")
            .execute()
        )
        result2 = (
            collection.modify('["car","bike"] NOT IN $.additionalinfo.vehicle')
            .set("$.age", "25")
            .execute()
        )
        self.assertEqual(result.get_affected_items_count(), 1)
        self.assertEqual(result1.get_affected_items_count(), 0)
        self.assertEqual(result2.get_affected_items_count(), 2)
        self.schema.drop_collection("mycoll3")

    @tests.foreach_session()
    def test_contains_operator_coll_modify4(self):
        """Test IN operator with dict on LHS and dict on RHS."""
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
        result = (
            collection.modify('{"company":"abc","vehicle":"car"} IN $.additionalinfo')
            .set("$.name", "sad")
            .sort("$._id DESC")
            .limit(2)
            .execute()
        )
        result1 = (
            collection.modify('{"vehicle":"car"} NOT IN $.additionalinfo')
            .set("$.age", 26)
            .execute()
        )
        result2 = (
            collection.modify('{"company":"mno"} IN $.additionalinfo')
            .unset("$.age")
            .execute()
        )
        result3 = (
            collection.modify(
                '{"company":"abc","vehicle":"car"} NOT IN $.additionalinfo'
            )
            .set("$.name", "changedname")
            .execute()
        )
        self.assertEqual(result.get_affected_items_count(), 2)
        self.assertEqual(result1.get_affected_items_count(), 1)
        self.assertEqual(result2.get_affected_items_count(), 1)
        self.assertEqual(result3.get_affected_items_count(), 1)
        self.schema.drop_collection("mycoll4")

    @tests.foreach_session()
    def test_modify_merge1(self):
        """Test modifying the multiple fields using the .patch()."""
        self._drop_collection_if_exists("mycoll1")
        collection = self.schema.create_collection("mycoll1")
        collection.add(
            {
                "_id": "1",
                "name": "student 1",
                "courses": {"course_name": "comp sci", "marks": "67"},
            },
            {
                "_id": "2",
                "name": "student 2",
                "courses": {"course_name": "electronics", "marks": "81"},
            },
        ).execute()
        collection.modify("$._id == :id").patch(
            {"name": "student 3", "courses": {"marks": "79"}}
        ).bind("id", "1").execute()
        result = collection.find("$._id == '1'").execute().fetch_all()[0]
        self.assertEqual(result["name"], "student 3")
        self.assertEqual(result["courses"]["marks"], "79")
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_modify_merge2(self):
        """Test add new attributes/fields, and try updating the new
        attributes's values."""
        self._drop_collection_if_exists("mycoll2")
        collection = self.schema.create_collection("mycoll2")
        collection.add(
            {
                "_id": "1",
                "name": "student 1",
                "courses": {"course_name": "comp sci", "marks": "67"},
            },
            {
                "_id": "2",
                "name": "student 2",
                "courses": {"course_name": "electronics", "marks": "81"},
            },
        ).execute()
        collection.modify("$._id == :id").patch(
            {"Status": "Pass", "Subjects": ["OOP", "Networking"]}
        ).bind("id", "1").execute()
        result = collection.find("$._id == '1'").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["Status"], "Pass")
        self.assertEqual(row[0]["Subjects"][0], "OOP")
        self.assertEqual(row[0]["Subjects"][1], "Networking")
        collection.modify("$._id == :id").patch(
            {"Status": "Backlog", "Subjects": ["C++", "Programming"]}
        ).bind("id", "1").execute()
        result = collection.find("$._id == '1'").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["Status"], "Backlog")
        self.assertEqual(row[0]["Subjects"][0], "C++")
        self.assertEqual(row[0]["Subjects"][1], "Programming")
        self.schema.drop_collection("mycoll2")

    @tests.foreach_session()
    def test_modify_merge3(self):
        """Test update _id - shouldn't be allowed."""
        self._drop_collection_if_exists("mycoll3")
        collection = self.schema.create_collection("mycoll3")
        collection.add(
            {
                "_id": "1",
                "name": "Europian",
                "address": {"zip": "12345", "street": "32 Main str"},
            },
            {
                "_id": "2",
                "name": "American",
                "address": {
                    "zip": "325226",
                    "city": "San Francisco",
                    "street": "42 2nd str",
                },
            },
        ).execute()
        result1 = (
            collection.modify("$._id == :id")
            .patch({"_id": "1234"})
            .bind("id", "1")
            .execute()
        )
        result = collection.find("$._id == '1'").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["_id"], "1")
        self.assertEqual(result1.get_affected_items_count(), 0)
        self.schema.drop_collection("mycoll3")

    @tests.foreach_session()
    def test_modify_merge4(self):
        """Test add _id at nested level and change."""
        self._drop_collection_if_exists("mycoll4")
        collection = self.schema.create_collection("mycoll4")
        collection.add(
            {
                "_id": "1",
                "name": "Europian",
                "address": {"zip": "12345", "street": "32 Main str"},
            },
            {
                "_id": "2",
                "name": "American",
                "address": {
                    "zip": "325226",
                    "city": "San Francisco",
                    "street": "42 2nd str",
                },
            },
        ).execute()
        collection.modify("$._id == :id").patch({"address": {"_id": "21"}}).bind(
            "id", "2"
        ).execute()
        result = collection.find("$._id == '2'").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["address"]["_id"], "21")
        collection.modify("$._id == :id").patch(
            {"address": {"_id": "New21", "street": "New str"}}
        ).bind("id", "2").execute()
        result = collection.find("$._id == '2'").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["address"]["_id"], "New21")
        self.assertEqual(row[0]["address"]["street"], "New str")
        self.schema.drop_collection("mycoll4")

    @tests.foreach_session()
    def test_modify_merge5(self):
        """Test update _id to null - it will not be modified."""
        self._drop_collection_if_exists("mycoll5")
        collection = self.schema.create_collection("mycoll5")
        collection.add(
            {"_id": 2, "c1": 9},
            {"_id": 3, "c1": 8},
            {"_id": 4, "c1": 7},
            {"_id": 5, "c1": 6},
            {"_id": 6, "c1": 5},
            {"_id": 7, "c1": 4},
            {"_id": 8, "c1": 3},
            {"_id": 9, "c1": 2},
        ).execute()
        collection.modify("true").patch('{"_id":null}').execute()
        result = collection.find().execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["_id"], 2)
        self.schema.drop_collection("mycoll5")

    @tests.foreach_session()
    def test_modify_merge6(self):
        """Test patch a non-JSON like field."""
        self._drop_collection_if_exists("mycoll6")
        collection = self.schema.create_collection("mycoll6")
        collection.add(
            {
                "_id": "1",
                "name": "Europian",
                "address": {"zip": "12345", "street": "32 Main str"},
            },
            {
                "_id": "2",
                "name": "American",
                "address": {
                    "zip": "325226",
                    "city": "San Francisco",
                    "street": "42 2nd str",
                },
            },
        ).execute()
        try:
            collection.modify("$._id == :_id").patch({"Status"}).bind(
                "_id", "2"
            ).execute()
        except mysqlx.ProgrammingError:
            # Expected error
            pass
        self.schema.drop_collection("mycoll6")

    @tests.foreach_session()
    def test_modify_merge7(self):
        """Test flat the sub-document to the top level document using
        expression."""
        self._drop_collection_if_exists("mycoll7")
        collection = self.schema.create_collection("mycoll7")
        collection.add(
            {
                "_id": "1234",
                "name": "Europe",
                "language": "English",
                "actors": {
                    "MainActor": {
                        "name": "Tom C",
                        "birthdate": "1 Jan 1983",
                    },
                    "SideActor": {
                        "name": "Cristy",
                        "birthdate": "26 Jul 1975",
                    },
                },
                "additionalinfo": {
                    "director": {
                        "name": "James C",
                        "age": 57,
                        "awards": {
                            "FirstAward": {
                                "award": "Best Movie",
                                "movie": "THE EGG",
                                "year": 2002,
                            },
                            "SecondAward": {
                                "award": "Best Special Effects",
                                "movie": "AFRICAN EGG",
                                "year": 2006,
                            },
                        },
                    }
                },
            }
        ).execute()
        collection.modify("_id = :id").patch(
            {
                "director": mysqlx.expr("$.additionalinfo.director"),
                "awards": mysqlx.expr("$.additionalinfo.director.awards"),
                "additionalinfo": None,
            }
        ).bind("id", "1234").execute()
        result = collection.find("$._id == '1234'").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["director"]["name"], "James C")
        self.assertEqual(row[0]["awards"]["SecondAward"]["movie"], "AFRICAN EGG")
        collection.modify("_id = :id").patch(
            {
                "$.director.awards.SecondAward": None,
                "SecondAward": mysqlx.expr("$.director.awards.SecondAward"),
            }
        ).bind("id", "1234").execute()
        result = collection.find("$._id == '1234'").execute()
        row = result.fetch_all()[0]
        self.assertEqual(row["SecondAward"]["year"], 2006)
        self.schema.drop_collection("mycoll7")

    @tests.foreach_session()
    def test_modify_merge8(self):
        """Test add Null fields at any nested level."""
        self._drop_collection_if_exists("mycoll8")
        collection = self.schema.create_collection("mycoll8")
        collection.add(
            {
                "_id": "1234",
                "name": "Europe",
                "language": "English",
                "actors": {
                    "MainActor": {
                        "name": "Tom C",
                        "birthdate": "1 Jan 1983",
                    },
                    "SideActor": {
                        "name": "Cristy",
                        "birthdate": "26 Jul 1975",
                    },
                },
                "additionalinfo": {
                    "director": {
                        "name": "James C",
                        "age": 57,
                        "awards": {
                            "FirstAward": {
                                "award": "Best Movie",
                                "movie": "THE EGG",
                                "year": 2002,
                            },
                            "SecondAward": {
                                "award": "Best Special Effects",
                                "movie": "AFRICAN EGG",
                                "year": 2006,
                            },
                        },
                    }
                },
            }
        ).execute()
        try:
            collection.modify("true").patch('{"nullfield" : NULL}').execute()
        except mysqlx.OperationalError:
            # Expected error
            pass
        self.schema.drop_collection("mycoll8")

    @tests.foreach_session()
    def test_modify_merge9(self):
        """Test add, modify, delete fields at any nested level."""
        self._drop_collection_if_exists("mycoll9")
        collection = self.schema.create_collection("mycoll9")
        collection.add(
            {
                "_id": "1234",
                "name": "Europe",
                "language": "English",
                "actors": {
                    "MainActor": {
                        "name": "Tom C",
                        "birthdate": "1 Jan 1983",
                    },
                    "SideActor": {
                        "name": "Cristy",
                        "birthdate": "26 Jul 1975",
                    },
                },
                "additionalinfo": {
                    "director": {
                        "name": "James C",
                        "age": 57,
                        "awards": {
                            "FirstAward": {
                                "award": "Best Movie",
                                "movie": "THE EGG",
                                "year": 2002,
                            },
                            "SecondAward": {
                                "award": "Best Special Effects",
                                "movie": "AFRICAN EGG",
                                "year": 2006,
                            },
                        },
                    }
                },
            }
        ).execute()
        collection.modify("TRUE").patch(
            {"actors": {"SideActor": {"name": "New Actor"}}}
        ).execute()  # modified the "name" field
        collection.modify("additionalinfo.director.name = :director").patch(
            {
                "additionalinfo": {
                    "director": {
                        "field 1": "one",
                        "field 2": "two",
                        "field 3": "three",
                    }
                }
            }
        ).bind("director", "James C").execute()
        # adding new fields at nested level
        collection.modify("additionalinfo.director.name = :director").patch(
            '{"additionalinfo": {"director": {"awards": {"SecondAward":null}}}}'
        ).bind("director", "James C").execute()
        result = collection.find().execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["actors"]["SideActor"]["name"], "New Actor")
        self.assertEqual(row[0]["additionalinfo"]["director"]["field 2"], "two")
        self.assertEqual(
            row[0]["additionalinfo"]["director"]["awards"]["FirstAward"]["year"],
            2002,
        )  # Testing .patch will not modify other fields than target
        self.schema.drop_collection("mycoll9")

    @tests.foreach_session()
    def test_modify_merge10(self):
        """Test .patch() with limit, bind and sort."""
        self._drop_collection_if_exists("mycoll10")
        collection = self.schema.create_collection("mycoll10")
        collection.add(
            {"i": 1, "c1": 10},
            {"i": 2, "c1": 10},
            {"i": 1, "c1": 11},
            {"i": 2, "c1": 11},
        ).execute()
        res = (
            collection.modify("$.c1 == :c1")
            .patch({"i": 50})
            .set("$.i", 100)
            .sort("$.i ASC", "$.c1 DESC")
            .limit(1)
            .bind("c1", 11)
            .execute()
        )
        result = collection.find("$.c1 ==11").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["i"], 100)
        self.assertEqual(row[1]["i"], 2)
        self.assertEqual(res.get_affected_items_count(), 1)
        self.schema.drop_collection("mycoll10")

    @tests.foreach_session()
    def test_modify_merge11(self):
        """Test the collection.modify UNSET with param list."""
        self._drop_collection_if_exists("mycoll11")
        collection = self.schema.create_collection("mycoll11")
        collection.add(
            {"_id": 1, "name": "a", "age": 10},
            {"_id": 2, "name": "b", "age": 20},
        ).execute()
        result = collection.modify("$._id==1").unset("$.name", "$.age").execute()
        self.assertEqual(result.get_affected_items_count(), 1)
        self.schema.drop_collection("mycoll11")

    @tests.foreach_session()
    def test_modify_merge12(self):
        """Test the patch() with an empty string and is expected to return
        an exception."""
        self._drop_collection_if_exists("mycoll12")
        collection = self.schema.create_collection("mycoll12")
        collection.add(
            {"_id": 1, "name": "a", "age": 10},
            {"_id": 2, "name": "b", "age": 20},
        ).execute()
        try:
            collection.modify("true").patch("").execute()
        except mysqlx.OperationalError:
            # Expected a OperationalError
            pass
        self.schema.drop_collection("mycoll12")

    @tests.foreach_session()
    def test_modify_merge13(self):
        """Test patch() using expressions."""
        self._drop_collection_if_exists("mycoll13")
        collection = self.schema.create_collection("mycoll13")
        collection.add(
            {
                "_id": "1234",
                "name": "Europe",
                "language": "English",
                "actors": {
                    "MainActor": {"name": "Tom C", "birthdate": "1 Jan 1983"},
                    "SideActor": {
                        "name": "Cristy",
                        "birthdate": "26 Jul 1975",
                    },
                },
                "additionalinfo": {
                    "director": {
                        "name": "James C",
                        "age": 57,
                        "awards": {
                            "FirstAward": {
                                "award": "Best Movie",
                                "movie": "THE EGG",
                                "year": 2002,
                            },
                            "SecondAward": {
                                "award": "Best Special Effects",
                                "movie": "AFRICAN EGG",
                                "year": 2006,
                            },
                        },
                    }
                },
            }
        ).execute()
        collection.modify("true").patch(
            {
                "actors": {
                    "MainActor": {
                        "birthyear": mysqlx.expr(
                            'CAST(SUBSTRING_INDEX($.actors.MainActor.birthdate, " " , - 1) AS DECIMAL)'
                        )
                    }
                }
            }
        ).execute()
        collection.modify("true").patch(
            {
                "additionalinfo": {
                    "director": {
                        "title": mysqlx.expr(
                            'CONCAT($.additionalinfo.director.awards.FirstAward.award, " of the year")'
                        )
                    }
                }
            }
        ).execute()
        result = collection.get_one("1234")
        self.assertEqual(result["actors"]["MainActor"]["birthyear"], 1983)
        self.assertEqual(
            result["additionalinfo"]["director"]["title"],
            "Best Movie of the year",
        )
        self.schema.drop_collection("mycoll13")

    @tests.foreach_session()
    def test_modify_merge14(self):
        """Test modifying the fields using .patch() with expressions."""
        self._drop_collection_if_exists("mycoll14")
        collection = self.schema.create_collection("mycoll14")
        collection.add(
            {
                "_id": "1234",
                "name": "Europe",
                "language": "English",
                "actors": {
                    "MainActor": {
                        "name": "Tom C",
                        "birthdate": "1 Jan 1983",
                    },
                    "SideActor": {
                        "name": "Cristy",
                        "birthdate": "26 Jul 1975",
                    },
                },
                "additionalinfo": {
                    "director": {
                        "name": "James C",
                        "age": 57,
                        "awards": {
                            "FirstAward": {
                                "award": "Best Movie",
                                "movie": "THE EGG",
                                "year": 2002,
                            },
                            "SecondAward": {
                                "award": "Best Special Effects",
                                "movie": "AFRICAN EGG",
                                "year": 2006,
                            },
                        },
                    }
                },
            }
        ).execute()
        collection.modify("true").patch(
            mysqlx.expr('{"actors": {"MainActor": {"birthdate": "11 Nov 1975"}}}')
        ).execute()
        collection.modify("true").patch(
            mysqlx.expr(
                '{"additionalinfo": {"director": {"title": CONCAT(UPPER($.additionalinfo.director.awards.FirstAward.award), " OF THE YEAR")}}}'
            )
        ).execute()
        result = collection.get_one("1234")
        self.assertEqual(
            result["additionalinfo"]["director"]["title"],
            "BEST MOVIE OF THE YEAR",
        )
        collection.modify("true").patch(
            {"actors": {"MainActor": {"birthdate": "1 Jan 1983"}}}
        ).execute()
        collection.modify("true").patch(
            {
                "actors": {
                    "MainActor": {
                        "age": mysqlx.expr(
                            "CAST(SUBSTRING_INDEX($.actors.MainActor.birthdate, ' ', - 1) AS DECIMAL)"
                        )
                    }
                }
            }
        ).execute()
        result = collection.get_one("1234")
        self.assertEqual(result["actors"]["MainActor"]["age"], 1983)
        collection.modify("true").patch(
            {"actors": {"MainActor": {"age": mysqlx.expr("(12 * 4) + 10")}}}
        ).execute()
        result = collection.get_one("1234")
        self.assertEqual(result["actors"]["MainActor"]["age"], 58)
        collection.modify("true").patch(
            {
                "actors": {
                    "MainActor": {
                        "age": mysqlx.expr(
                            "2018 - CAST(SUBSTRING_INDEX($.actors.MainActor.birthdate, ' ', - 1) AS DECIMAL)"
                        )
                    }
                }
            }
        ).execute()
        result = collection.get_one("1234")
        self.assertEqual(result["actors"]["MainActor"]["age"], 35)
        collection.modify("true").patch(
            mysqlx.expr('{"actors": {"MainActor": {"birthdate": Year(CURDATE())}}}')
        ).execute()
        result = collection.get_one("1234")
        current_year = datetime.date.today().year
        self.assertEqual(result["actors"]["MainActor"]["birthdate"], current_year)
        collection.modify("true").patch(
            {"actors": {"MainActor": {"birthdate": "1 Jan 1983"}}}
        ).execute()
        collection.modify("true").patch(
            {
                "actors": {
                    "MainActor": {
                        "birthdate": mysqlx.expr(
                            "SELECT SUBSTRING_INDEX($.actors.MainActor.birthdate, ' ', 2)"
                        )
                    }
                }
            }
        ).execute()
        result = collection.get_one("1234")
        self.assertEqual(result["actors"]["MainActor"]["birthdate"], "1 Jan")
        self.schema.drop_collection("mycoll14")

    @tests.foreach_session()
    def test_modify_merge15(self):
        """Test inserting null values using .patch()."""
        self._drop_collection_if_exists("mycoll15")
        collection = self.schema.create_collection("mycoll15")
        collection.add({"_id": 1, "name": "a", "age": 10}).execute()
        collection.modify("true").patch('{"nullfield" : [null, null]}').execute()
        result = collection.find().execute()
        row = result.fetch_all()[0]
        self.assertEqual(len(row["nullfield"]), 2)
        self.assertEqual(row["nullfield"][0], None)
        self.assertEqual(row["nullfield"][1], None)
        self.schema.drop_collection("mycoll15")

    @tests.foreach_session()
    def test_modify_merge16(self):
        """Test inserting null values at nested level."""
        self._drop_collection_if_exists("mycoll16")
        collection = self.schema.create_collection("mycoll16")
        collection.add({"_id": 1, "name": "a", "age": 10}).execute()
        collection.modify("true").patch(
            '{"additionalinfo" : {"birthdate": null}}'
        ).execute()
        result = collection.find().execute()
        row = result.fetch_all()[0]
        self.assertEqual(len(row["additionalinfo"]), 0)
        collection.modify("true").patch(
            '{"additionalinfo" : {"birthdate": [null, null]}}'
        ).execute()
        result = collection.find().execute()
        row = result.fetch_all()[0]
        self.assertEqual(len(row["additionalinfo"]["birthdate"]), 2)
        self.assertEqual(row["additionalinfo"]["birthdate"][0], None)
        self.assertEqual(row["additionalinfo"]["birthdate"][1], None)
        self.schema.drop_collection("mycoll16")

    @tests.foreach_session()
    def Overlaps_CollModify_test1():
        """Overlaps in collection.modify."""
        try:
            collection = schema.create_collection("mycoll1")
            collection.add(
                {
                    "_id": 1,
                    "name": "a",
                    "age": 22,
                    "prof": {"a1": "x1", "b1": "y1"},
                },
                {"_id": 2, "name": "b", "age": 24},
            ).execute()
            result = (
                collection.modify("$.name OVERLAPS 'a'").set("$.name", "c").execute()
            )
            result1 = (
                collection.modify("$.age OVERLAPS [22,24]")
                .set("$.name", "changed_age*@#$")
                .execute()
            )
            result2 = (
                collection.modify('{"a1":"x1"} OVERLAPS $.prof')
                .unset("$.age")
                .execute()
            )
            result3 = (
                collection.modify('"x1" OVERLAPS $.prof.a1')
                .set("$.name", "xyz")
                .execute()
            )
            #        result1 = collection.find().execute()
            #        for row in result1.fetch_all():
            #            print(row)
            assert result.get_affected_items_count() == 1
            assert result1.get_affected_items_count() == 2
            assert result2.get_affected_items_count() == 1
            assert result3.get_affected_items_count() == 1
            print("Overlaps_CollModify_test1 IS OK")
        except AssertionError as e:
            print(e)
            print("Overlaps_CollModify_test1 IS NOT OK [" + str(e) + "]")
        except Exception as err:
            print("Overlaps_CollModify_test1 IS NOT OK [" + str(err) + "]")
            print(err)
        finally:
            schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_overlaps_coll_modify2(self):
        """Not overlaps in collection.modify."""
        collection = self.schema.create_collection("mycoll2")
        collection.add(
            {
                "_id": 1,
                "name": "a",
                "age": 22,
                "prof": {"a1": "x1", "b1": "y1"},
            },
            {"_id": 2, "name": "b", "age": 24},
        ).execute()
        result = (
            collection.modify("$.name NOT OVERLAPS 'a'").set("$.name", "c").execute()
        )
        result1 = (
            collection.modify("$.age NOT OVERLAPS [22,23]")
            .set("$.name", "changed_age*@#$")
            .execute()
        )
        result2 = (
            collection.modify('{"a1":"z1"} NOT OVERLAPS $.prof')
            .unset("$.age")
            .execute()
        )
        result3 = (
            collection.modify('"z1" NOT OVERLAPS $.prof.a1')
            .set("$.name", "xyz")
            .execute()
        )
        self.assertEqual(result.get_affected_items_count(), 1)
        self.assertEqual(result1.get_affected_items_count(), 1)
        self.assertEqual(result2.get_affected_items_count(), 1)
        self.assertEqual(result3.get_affected_items_count(), 1)
        self.schema.drop_collection("mycoll2")

    @tests.foreach_session()
    def test_overlaps_coll_modify3(self):
        """OVERLAPS operator with array/list operand.
        On LHS and array/list on RHS.
        """
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
        result = (
            collection.modify(
                '["playing","painting","boxing"] OVERLAPS $.additionalinfo.hobbies'
            )
            .set("$.additionalinfo.hobbies", "['xyz','pqr','abc']")
            .execute()
        )
        result1 = (
            collection.modify('["happy","joy"] OVERLAPS $.name')
            .set("$.name", "abc")
            .execute()
        )
        result2 = (
            collection.modify('["car","bike"] NOT OVERLAPS $.additionalinfo.vehicle')
            .set("$.age", "25")
            .execute()
        )
        self.assertEqual(result.get_affected_items_count(), 2)
        self.assertEqual(result1.get_affected_items_count(), 2)
        self.assertEqual(result2.get_affected_items_count(), 0)
        self.schema.drop_collection("mycoll3")

    @tests.foreach_session()
    def test_overlaps_coll_modify4(self):
        """OVERLAPS operator with dict on LHS and dict on RHS."""
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
        result = (
            collection.modify(
                '{"company":"abc","vehicle":"car"} OVERLAPS $.additionalinfo'
            )
            .set("$.name", "sad")
            .sort("$._id DESC")
            .limit(2)
            .execute()
        )
        result1 = (
            collection.modify('{"vehicle":"car"} NOT OVERLAPS $.additionalinfo')
            .set("$.age", 26)
            .execute()
        )
        result2 = (
            collection.modify('{"company":"mno"} OVERLAPS $.additionalinfo')
            .unset("$.age")
            .execute()
        )
        result3 = (
            collection.modify(
                '{"company":"abc","vehicle":"car"} NOT OVERLAPS $.additionalinfo'
            )
            .set("$.name", "changedname")
            .execute()
        )
        self.assertEqual(result.get_affected_items_count(), 2)
        self.assertEqual(result1.get_affected_items_count(), 3)
        self.assertEqual(result2.get_affected_items_count(), 0)
        self.assertEqual(result3.get_affected_items_count(), 1)
        self.schema.drop_collection("mycoll4")
