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

import unittest

import mysqlx
import tests


@unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 25), "XPlugin not compatible")
class TableUpdateTests(tests.MySQLxTests):
    """Tests for table.update()."""

    @tests.foreach_session()
    def test_table_update1(self):
        """Test the table.update with where."""
        self.session.sql("drop table if exists t1").execute()
        self.session.sql("create table t1(a int , b int)").execute()
        table = self.schema.get_table("t1")
        table.insert().values(1, 1).values(2, 1).values(3, 2).execute()
        result = table.update().set("b", 10).where("a > 1").execute()
        self.assertEqual(result.get_affected_items_count(), 2)
        self.session.sql("drop table if exists t1").execute()

    @tests.foreach_session()
    def test_table_update2(self):
        """Test the table.update with where and limit."""
        self.session.sql("drop table if exists t2").execute()
        self.session.sql("create table t2(a int , b int)").execute()
        table = self.schema.get_table("t2")
        table.insert().values(1, 1).values(2, 1).values(3, 2).execute()
        table.update().set("b", 10).where("a > 1").limit(1).execute()
        result2 = table.select().where("a > 1").execute()
        row = result2.fetch_all()
        self.assertEqual(row[0]["b"], 10)
        self.assertEqual(row[1]["b"], 2)
        self.session.sql("drop table if exists t2").execute()

    @tests.foreach_session()
    def test_table_update3(self):
        """Test the table.update with bind."""
        self.session.sql("drop table if exists t3").execute()
        self.session.sql("create table t3(a int , b int)").execute()
        table = self.schema.get_table("t3")
        table.insert().values(1, 1).values(2, 1).values(3, 2).execute()
        table.update().set("b", 10).where("a == :a").bind("a", 1).execute()
        result2 = table.select().where("a == 1").execute()
        row = result2.fetch_all()
        self.assertEqual(row[0]["b"], 10)
        self.session.sql("drop table if exists t3").execute()

    @tests.foreach_session()
    def test_table_update4(self):
        """Test the table.update with unknown column."""
        self.session.sql("drop table if exists t4").execute()
        self.session.sql("create table t4(a int , b int)").execute()
        table = self.schema.get_table("t4")
        table.insert().values(1, 1).values(2, 1).values(3, 2).execute()
        try:
            table.update().set("c", 10).where("a == 1").execute()
        except mysqlx.OperationalError:
            # Expected OperationalError
            pass
        self.session.sql("drop table if exists t4").execute()

    @tests.foreach_session()
    def test_table_update5(self):
        """Test the table.update with out of range value."""
        self.session.sql("drop table if exists t5").execute()
        self.session.sql("create table t5(a int , b int)").execute()
        table = self.schema.get_table("t5")
        table.insert().values(1, 1).values(2, 1).values(3, 2).execute()
        try:
            table.update().set(
                "b",
                100000000000000000000000000000000000000000000000000000000000000000,
            ).where("a == 1").execute()
        except (ValueError, SystemError):
            # Expected error
            pass
        self.session.sql("drop table if exists t5").execute()

    @tests.foreach_session()
    def test_table_update6(self):
        """Test the table.update with multiple set with different datatypes."""
        self.session.sql("drop table if exists t6").execute()
        self.session.sql(
            "create table t6(a int , b varchar(32), c date)"
        ).execute()
        table = self.schema.get_table("t6")
        table.insert().values(1, "abc", "2000-10-20").values(
            2, "def", "2000-10-20"
        ).execute()
        table.update().set("a", 10).set("b", "lmn").where("a == 1").execute()
        result2 = table.select("a,b").where("c == '2000-10-20'").execute()
        row = result2.fetch_all()
        self.assertEqual(row[0]["a"], 10)
        self.assertEqual(row[0]["b"], "lmn")
        self.session.sql("drop table if exists t6").execute()

    @tests.foreach_session()
    def test_table_update7(self):
        """Test the table.update with sort."""
        self.session.sql("drop table if exists t7").execute()
        self.session.sql("create table t7(a int , b int)").execute()
        table = self.schema.get_table("t7")
        table.insert().values(1, 3).values(2, 2).values(3, 1).execute()
        table.update().set("b", 10).sort("b ASC").limit(1).where(
            "false"
        ).execute()
        result2 = table.select().where("a == 3").execute()
        row = result2.fetch_all()
        self.assertEqual(row[0]["b"], 1)
        self.session.sql("drop table if exists t7").execute()

    @unittest.skipIf(
        tests.MYSQL_EXTERNAL_SERVER,
        "Test not available for external MySQL servers",
    )
    @tests.foreach_session()
    def test_table_update8(self):
        self.session.sql("drop table if exists t8").execute()
        self.session.sql("create table t8(a int , b int)").execute()
        table = self.schema.get_table("t8")
        table.insert().values(1, 3).execute()
        table.update().set("a", 10).set("b", "a+10").set("a", 20).where(
            "1 == 1"
        ).execute()
        result2 = table.select().execute()
        row = result2.fetch_all()
        self.session.sql("drop table if exists t8").execute()

    @tests.foreach_session()
    def test_table_update9(self):
        """Test the table.update with sort and param."""
        self.session.sql("drop table if exists t9").execute()
        self.session.sql("create table t9(a int , b int)").execute()
        table = self.schema.get_table("t9")
        table.insert().values(1, 10).values(2, 11).values(1, 11).values(
            2, 10
        ).execute()
        table.update().set("a", 10).sort("a ASC", "b DESC").limit(1).where(
            "true"
        ).execute()
        result2 = table.select().where("b == 11").execute()
        row = result2.fetch_all()
        self.assertEqual(row[0]["a"], 2)
        self.assertEqual(row[1]["a"], 10)
        self.session.sql("drop table if exists t9").execute()

    @tests.foreach_session()
    def test_table_update10(self):
        """Test table update with no condition - expected an exception."""
        self.session.sql("drop table if exists t10").execute()
        self.session.sql("create table t10(a int , b int)").execute()
        table = self.schema.get_table("t10")
        table.insert().values(1, 1).values(2, 1).values(3, 2).execute()
        try:
            table.update().set("b", 10).execute()
        except mysqlx.ProgrammingError:
            # Expected ProgrammingError
            pass
        self.session.sql("drop table if exists t10").execute()

    @tests.foreach_session()
    def test_table_update11(self):
        """Test table update with empty condition - expected an exception."""
        self.session.sql("drop table if exists t11").execute()
        self.session.sql("create table t11(a int , b int)").execute()
        table = self.schema.get_table("t11")
        table.insert().values(1, 1).values(2, 1).values(3, 2).execute()
        try:
            table.update().set("b", 10).where("").execute()
        except mysqlx.ProgrammingError:
            # Expected ProgrammingError
            pass
        self.session.sql("drop table if exists t11").execute()

    @tests.foreach_session()
    def test_contains_operator_table_update1(self):
        """Test In operator in table.update."""
        self.session.sql("drop table if exists t1").execute()
        self.session.sql("create table t1(id int, n JSON, a JSON)").execute()
        table = self.schema.get_table("t1")
        table.insert().values(1, '{"name":"a"}', '{"age":22}').values(
            2, '{"name":"b"}', '{"age":24}'
        ).execute()
        result = (
            table.update().where("n->'$.name' IN 'a'").set("id", 4).execute()
        )
        result1 = (
            table.update()
            .where("a->'$.age' IN [22,24]")
            .set("id", 5)
            .execute()
        )
        self.assertEqual(result.get_affected_items_count(), 1)
        self.assertEqual(result1.get_affected_items_count(), 2)
        self.session.sql("drop table if exists t1").execute()

    @tests.foreach_session()
    def test_contains_operator_table_update2(self):
        """Test Not In operator in table.update."""
        self.session.sql("drop table if exists t2").execute()
        self.session.sql("create table t2(id int, n JSON, a JSON)").execute()
        table = self.schema.get_table("t2")
        table.insert().values(1, '{"name":"a"}', '{"age":22}').values(
            2, '{"name":"b"}', '{"age":24}'
        ).execute()
        result = (
            table.update()
            .where("n->'$.name' NOT IN 'a'")
            .set("id", 4)
            .execute()
        )
        result1 = (
            table.update()
            .where("a->'$.age' NOT IN [22,24]")
            .set("id", 5)
            .execute()
        )
        self.assertEqual(result.get_affected_items_count(), 1)
        self.assertEqual(result1.get_affected_items_count(), 0)
        self.session.sql("drop table if exists t2").execute()

    @tests.foreach_session()
    def test_contains_operator_table_update3(self):
        """Test IN operator with array/list operand on LHS and array/list on
        RHS."""
        self.session.sql("drop table if exists t3").execute()
        self.session.sql(
            "create table t3(id JSON, n JSON, a JSON, addinfo JSON)"
        ).execute()
        table = self.schema.get_table("t3")
        table.insert().values(
            '{"_id":1}',
            '{"name":"joy"}',
            '{"age":21}',
            '{"additionalinfo":{"company":"xyz","vehicle":"bike","hobbies":["reading","music","playing"]}}',
        ).values(
            '{"_id":2}',
            '{"name":"happy"}',
            '{"age":24}',
            '{"additionalinfo":{"company":"abc","vehicle":"car","hobbies":["playing","painting","boxing"]}}',
        ).execute()
        result = (
            table.update()
            .where(
                '["playing","painting","boxing"] IN addinfo->$.additionalinfo.hobbies'
            )
            .set(
                "addinfo",
                {"additionalinfo": {"hobbies": ["xyz", "pqr", "abc"]}},
            )
            .execute()
        )
        result1 = (
            table.update()
            .where('["happy","joy"] IN n->$.name')
            .set("n", {"name": "abc"})
            .execute()
        )
        result2 = (
            table.update()
            .where('["car","bike"] NOT IN addinfo->$.additionalinfo.vehicle')
            .set("a", {"age": 25})
            .execute()
        )
        self.assertEqual(result.get_affected_items_count(), 1)
        self.assertEqual(result1.get_affected_items_count(), 0)
        self.assertEqual(result2.get_affected_items_count(), 1)
        self.session.sql("drop table if exists t3").execute()

    @tests.foreach_session()
    def test_contains_operator_table_update4(self):
        """IN operator with dict on LHS and dict on RHS."""
        self.session.sql("drop table if exists t4").execute()
        self.session.sql(
            "create table t4(id JSON, n JSON, a JSON, addinfo JSON)"
        ).execute()
        table = self.schema.get_table("t4")
        table.insert().values(
            '{"_id":1}',
            '{"name":"joy"}',
            '{"age":21}',
            '{"additionalinfo":[{"company":"xyz","vehicle":"bike"},{"company":"abc","vehicle":"car"},{"company":"mno","vehicle":"zeep"}]}',
        ).values(
            '{"_id":2}',
            '{"name":"happy"}',
            '{"age":24}',
            '{"additionalinfo":[{"company":"abc","vehicle":"car"},{"company":"pqr","vehicle":"bicycle"}]}',
        ).values(
            '{"_id":3}',
            '{"name":"nice"}',
            '{"age":25}',
            '{"additionalinfo":{"company":"def","vehicle":"none"}}',
        ).execute()
        result = (
            table.update()
            .where(
                '{"company":"abc","vehicle":"car"} IN addinfo->$.additionalinfo'
            )
            .set("n", {"name": "sad"})
            .sort("id DESC")
            .limit(2)
            .execute()
        )
        result1 = (
            table.update()
            .where('{"vehicle":"car"} NOT IN addinfo->$.additionalinfo')
            .set("a", {"age": 26})
            .execute()
        )
        result2 = (
            table.update()
            .where('{"company":"mno"} IN addinfo->$.additionalinfo')
            .set("a", {"age": 20})
            .execute()
        )
        result3 = (
            table.update()
            .where(
                '{"company":"abc","vehicle":"car"} NOT IN addinfo->$.additionalinfo'
            )
            .set("n", {"name": "changedname"})
            .execute()
        )
        self.assertEqual(result.get_affected_items_count(), 2)
        self.assertEqual(result1.get_affected_items_count(), 1)
        self.assertEqual(result2.get_affected_items_count(), 1)
        self.assertEqual(result3.get_affected_items_count(), 1)
        self.session.sql("drop table if exists t4").execute()

    @tests.foreach_session()
    def test_overlaps_table_update1(self):
        """Overlaps in table.update."""
        self.session.sql("drop table if exists t1").execute()
        self.session.sql("create table t1(id int, n JSON, a JSON)").execute()
        table = self.schema.get_table("t1")
        table.insert().values(1, '{"name":"a"}', '{"age":22}').values(
            2, '{"name":"b"}', '{"age":24}'
        ).execute()
        result = (
            table.update()
            .where("n->'$.name' OVERLAPS 'a'")
            .set("id", 4)
            .execute()
        )
        result1 = (
            table.update()
            .where("a->'$.age' OVERLAPS [22,24]")
            .set("id", 5)
            .execute()
        )
        assert result.get_affected_items_count() == 1
        assert result1.get_affected_items_count() == 2
        self.session.sql("drop table if exists t1").execute()

    @tests.foreach_session()
    def test_overlaps_table_update2(self):
        """Not Overlaps in table.update."""
        self.session.sql("drop table if exists t2").execute()
        self.session.sql("create table t2(id int, n JSON, a JSON)").execute()
        table = self.schema.get_table("t2")
        table.insert().values(1, '{"name":"a"}', '{"age":22}').values(
            2, '{"name":"b"}', '{"age":24}'
        ).execute()
        result = (
            table.update()
            .where("n->'$.name' NOT OVERLAPS 'a'")
            .set("id", 4)
            .execute()
        )
        result1 = (
            table.update()
            .where("a->'$.age' NOT OVERLAPS [22,24]")
            .set("id", 5)
            .execute()
        )
        self.assertEqual(result.get_affected_items_count(), 1)
        self.assertEqual(result1.get_affected_items_count(), 0)
        self.session.sql("drop table if exists t2").execute()

    @tests.foreach_session()
    def test_overlaps_table_update3(self):
        """OVERLAPS operator with array/list operand on LHS and array/list
        on RHS."""
        self.session.sql("drop table if exists t3").execute()
        self.session.sql(
            "create table t3(i json, n JSON, a JSON, addinfo JSON,zip int,street varchar(20))"
        ).execute()
        table = self.schema.get_table("t3")
        table.insert().values(
            '{"_id":1}',
            '{"name":"joy"}',
            '{"age":21}',
            '{"additionalinfo":{"company":"xyz","vehicle":"bike","hobbies":["reading","music","playing"]}}',
            12345,
            "street1",
        ).values(
            '{"_id":2}',
            '{"name":"happy"}',
            '{"age":24}',
            '{"additionalinfo":{"company":"abc","vehicle":"car","hobbies":["playing","painting","boxing"]}}',
            11010,
            "street2",
        ).execute()
        result = (
            table.update()
            .where(
                '["playing","painting","boxing"] OVERLAPS addinfo->"$.additionalinfo.hobbies"'
            )
            .set("zip", 00000)
            .execute()
        )
        result1 = (
            table.update()
            .where('["happy","joy"] OVERLAPS n->$.name')
            .set("street", "new street1")
            .set("addinfo", {"additionalinfo": {"company": "none"}})
            .execute()
        )
        self.assertEqual(result.get_affected_items_count(), 2)
        self.assertEqual(result1.get_affected_items_count(), 2)
        self.session.sql("drop table if exists t3").execute()

    @tests.foreach_session()
    def test_overlaps_table_update4(self):
        """ "OVERLAPS operator with dict on LHS and dict on RHS."""
        self.session.sql("drop table if exists t4").execute()
        self.session.sql(
            "create table t4(id JSON, n JSON, a JSON, addinfo JSON)"
        ).execute()
        table = self.schema.get_table("t4")
        table.insert().values(
            '{"_id":1}',
            '{"name":"joy"}',
            '{"age":21}',
            '{"additionalinfo":[{"company":"xyz","vehicle":"bike"},{"company":"abc","vehicle":"car"},{"company":"mno","vehicle":"zeep"}]}',
        ).values(
            '{"_id":2}',
            '{"name":"happy"}',
            '{"age":24}',
            '{"additionalinfo":[{"company":"abc","vehicle":"car"},{"company":"pqr","vehicle":"bicycle"}]}',
        ).values(
            '{"_id":3}',
            '{"name":"nice"}',
            '{"age":25}',
            '{"additionalinfo":{"company":"def","vehicle":"none"}}',
        ).execute()
        result = (
            table.update()
            .where(
                '{"company":"abc","vehicle":"car"} OVERLAPS addinfo->"$.additionalinfo"'
            )
            .set("n", {"name": "sad"})
            .sort("id DESC")
            .limit(2)
            .execute()
        )
        result1 = (
            table.update()
            .where('{"vehicle":"car"} NOT OVERLAPS addinfo->$.additionalinfo')
            .set("a", {"age": 26})
            .execute()
        )
        result2 = (
            table.update()
            .where('{"company":"mno"} OVERLAPS addinfo->$.additionalinfo')
            .set("id", {"_id": 4})
            .execute()
        )
        result3 = (
            table.update()
            .where(
                '{"company":"abc","vehicle":"car"} NOT OVERLAPS addinfo->$.additionalinfo'
            )
            .set("n", {"name": "changedname"})
            .execute()
        )
        self.assertEqual(result.get_affected_items_count(), 2)
        self.assertEqual(result1.get_affected_items_count(), 3)
        self.assertEqual(result2.get_affected_items_count(), 0)
        self.assertEqual(result3.get_affected_items_count(), 1)
        self.session.sql("drop table if exists t4").execute()
