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
class TableDeleteTests(tests.MySQLxTests):
    """Tests for table.delete()."""

    @tests.foreach_session()
    def test_table_delete1(self):
        """Test the table.delete with sort and limit."""
        self.session.sql("drop table if exists t1").execute()
        self.session.sql("create table t1(a int primary key, b int)").execute()
        table = self.schema.get_table("t1")
        table.insert().values(1, 1).values(2, 1).values(3, 2).execute()
        self.assertRaises(
            mysqlx.ProgrammingError,
            table.delete().sort("a DESC").limit(2).execute,
        )
        self.session.sql("drop table if exists t1").execute()

    @tests.foreach_session()
    def test_table_delete2(self):
        """Test the table.delete with where."""
        self.session.sql("drop table if exists t2").execute()
        self.session.sql("create table t2(a int primary key, b int)").execute()
        table = self.schema.get_table("t2")
        table.insert().values(1, 1).values(2, 1).values(3, 2).execute()
        table.delete().where("a==2").execute()
        self.assertEqual(table.count(), 2)
        self.session.sql("drop table if exists t2").execute()

    @tests.foreach_session()
    def test_table_delete3(self):
        """Test the table.delete with bind."""
        self.session.sql("drop table if exists t3").execute()
        self.session.sql("create table t3(a int primary key, b int)").execute()
        table = self.schema.get_table("t3")
        table.insert().values(1, 1).values(2, 1).values(3, 2).execute()
        table.delete().where("a==:a").bind("a", 2).execute()
        self.assertEqual(table.count(), 2)
        self.session.sql("drop table if exists t3").execute()

    @tests.foreach_session()
    def test_table_delete4(self):
        """Test the table.delete with sort and param."""
        self.session.sql("drop table if exists t4").execute()
        self.session.sql("create table t4(id int primary key, a int , b int)").execute()
        table = self.schema.get_table("t4")
        table.insert().values(1, 1, 10).values(2, 2, 10).values(3, 1, 11).values(
            4, 2, 11
        ).execute()
        table.delete().sort("a ASC", "b DESC").limit(3).where("true").execute()
        self.assertEqual(table.count(), 1)
        result = table.select().execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["b"], 10)
        self.session.sql("drop table if exists t4").execute()

    @tests.foreach_session()
    def test_table_delete5(self):
        """Test the table.delete with a false condition."""
        self.session.sql("drop table if exists t5").execute()
        self.session.sql("create table t5(id int primary key, a int , b int)").execute()
        table = self.schema.get_table("t5")
        table.insert().values(1, 1, 10).values(2, 2, 10).values(3, 1, 11).values(
            4, 2, 11
        ).execute()
        table.delete().sort("a ASC", "b DESC").limit(3).where("false").execute()
        self.assertEqual(table.count(), 4)
        result = table.select().execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["b"], 10)
        self.session.sql("drop table if exists t5").execute()

    @tests.foreach_session()
    def test_able_delete6(self):
        """Test the table.delete with a false condition."""
        self.session.sql("drop table if exists t6").execute()
        self.session.sql("create table t6(id int primary key, a int , b int)").execute()
        table = self.schema.get_table("t6")
        table.insert().values(1, 1, 10).values(2, 2, 10).values(3, 1, 11).values(
            4, 2, 11
        ).execute()
        table.delete().sort("a ASC", "b DESC").limit(3).where("1 == 0").execute()
        self.assertEqual(table.count(), 4)
        result = table.select().execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["b"], 10)
        self.session.sql("drop table if exists t6").execute()

    @tests.foreach_session()
    def test_table_delete7(self):
        """Test the table.delete with a true condition."""
        self.session.sql("drop table if exists t7").execute()
        self.session.sql("create table t7(id int primary key, a int , b int)").execute()
        table = self.schema.get_table("t7")
        table.insert().values(1, 1, 10).values(2, 2, 10).values(3, 1, 11).values(
            4, 2, 11
        ).execute()
        table.delete().sort("a ASC", "b DESC").limit(3).where("1 == 1").execute()
        self.assertEqual(table.count(), 1)
        result = table.select().execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["b"], 10)
        self.session.sql("drop table if exists t7").execute()

    @tests.foreach_session()
    def test_table_delete8(self):
        """Test the table.delete with an empty condition."""
        self.session.sql("drop table if exists t8").execute()
        self.session.sql("create table t8(id int primary key, a int , b int)").execute()
        table = self.schema.get_table("t8")
        table.insert().values(1, 1, 10).values(2, 2, 10).values(3, 1, 11).values(
            4, 2, 11
        ).execute()
        try:
            table.delete().sort("a ASC", "b DESC").limit(3).where("").execute()
        except mysqlx.ProgrammingError:
            # Expected ProgrammingError
            pass
        self.session.sql("drop table if exists t8").execute()

    @tests.foreach_session()
    def test_contains_operator_table_delete1(self):
        """Test IN operator in table.delete."""
        self.session.sql("drop table if exists t1").execute()
        self.session.sql(
            "create table t1(id int primary key, n JSON, a JSON)"
        ).execute()
        table = self.schema.get_table("t1")
        table.insert().values(1, '{"name":"a"}', '{"age":22}').values(
            2, '{"name":"b"}', '{"age":24}'
        ).values(3, '{"name":"c"}', '{"age":25}').values(
            4, '{"name":"d"}', '{"age":23}'
        ).execute()
        result = table.delete().where("n->'$.name' IN 'a'").execute()
        result1 = table.delete().where("a->'$.age' IN [22,24]").execute()
        self.assertEqual(result.get_affected_items_count(), 1)
        self.assertEqual(result1.get_affected_items_count(), 1)
        self.session.sql("drop table if exists t1").execute()

    @tests.foreach_session()
    def test_contains_operator_table_delete2(self):
        """Tets NOT IN operator in table.delete."""
        self.session.sql("drop table if exists t2").execute()
        self.session.sql(
            "create table t2(id int primary key, n JSON, a JSON)"
        ).execute()
        table = self.schema.get_table("t2")
        table.insert().values(1, '{"name":"a"}', '{"age":22}').values(
            2, '{"name":"b"}', '{"age":24}'
        ).values(3, '{"name":"c"}', '{"age":25}').values(
            4, '{"name":"d"}', '{"age":23}'
        ).execute()
        result = table.delete().where("n->'$.name' NOT IN 'a'").execute()
        result1 = table.delete().where("a->'$.age' NOT IN [22,24]").execute()
        self.assertEqual(result.get_affected_items_count(), 3)
        self.assertEqual(result1.get_affected_items_count(), 0)
        self.session.sql("drop table if exists t2").execute()

    @tests.foreach_session()
    def test_contains_operator_table_delete3(self):
        """Test IN operator with array/list operand on LHS and array/list on
        RHS."""
        self.session.sql("drop table if exists t3").execute()
        self.session.sql(
            "create table t3(idx int primary key, id JSON, n JSON, a JSON, addinfo JSON)"
        ).execute()
        table = self.schema.get_table("t3")
        table.insert().values(
            1,
            '{"_id":1}',
            '{"name":"joy"}',
            '{"age":21}',
            '{"additionalinfo":{"company":"xyz","vehicle":"bike","hobbies":["reading","music","playing"]}}',
        ).values(
            2,
            '{"_id":2}',
            '{"name":"happy"}',
            '{"age":24}',
            '{"additionalinfo":{"company":"abc","vehicle":"car","hobbies":["playing","painting","boxing"]}}',
        ).execute()
        result = (
            table.delete()
            .where(
                '["playing","painting","boxing"] IN addinfo->$.additionalinfo.hobbies'
            )
            .execute()
        )
        result1 = table.delete().where('["happy","joy"] IN n->$.name').execute()
        result2 = (
            table.delete()
            .where('["car","bike"] NOT IN addinfo->$.additionalinfo.vehicle')
            .execute()
        )
        self.assertEqual(result.get_affected_items_count(), 1)
        self.assertEqual(result1.get_affected_items_count(), 0)
        self.assertEqual(result2.get_affected_items_count(), 1)
        self.session.sql("drop table if exists t3").execute()

    @tests.foreach_session()
    def test_contains_operator_table_delete4(self):
        """Test IN operator with dict on LHS and dict on RHS."""
        self.session.sql("drop table if exists t4").execute()
        self.session.sql(
            "create table t4(idx int primary key, id JSON, n JSON, a JSON, addinfo JSON)"
        ).execute()
        table = self.schema.get_table("t4")
        table.insert().values(
            1,
            '{"_id":1}',
            '{"name":"joy"}',
            '{"age":21}',
            '{"additionalinfo":[{"company":"xyz","vehicle":"bike"},{"company":"abc","vehicle":"car"},{"company":"mno","vehicle":"zeep"}]}',
        ).values(
            2,
            '{"_id":2}',
            '{"name":"happy"}',
            '{"age":24}',
            '{"additionalinfo":[{"company":"abc","vehicle":"car"},{"company":"pqr","vehicle":"bicycle"}]}',
        ).values(
            3,
            '{"_id":3}',
            '{"name":"nice"}',
            '{"age":25}',
            '{"additionalinfo":{"company":"def","vehicle":"none"}}',
        ).execute()
        result = (
            table.delete()
            .where('{"company":"abc","vehicle":"car"} IN addinfo->$.additionalinfo')
            .execute()
        )
        result1 = (
            table.delete()
            .where('{"vehicle":"car"} NOT IN addinfo->$.additionalinfo')
            .execute()
        )
        result2 = (
            table.delete()
            .where('{"company":"mno"} IN addinfo->$.additionalinfo')
            .execute()
        )
        result3 = (
            table.delete()
            .where('{"company":"abc","vehicle":"car"} NOT IN addinfo->$.additionalinfo')
            .execute()
        )
        self.assertEqual(result.get_affected_items_count(), 2)
        self.assertEqual(result1.get_affected_items_count(), 1)
        self.assertEqual(result2.get_affected_items_count(), 0)
        self.assertEqual(result3.get_affected_items_count(), 0)
        self.session.sql("drop table if exists t4").execute()

    @tests.foreach_session()
    def test_overlaps_table_delete1(self):
        """Overlaps in table.delete."""
        self.session.sql("drop table if exists t1").execute()
        self.session.sql(
            "create table t1(id int primary key, n JSON, a JSON)"
        ).execute()
        table = self.schema.get_table("t1")
        table.insert().values(1, '{"name":"a"}', '{"age":22}').values(
            2, '{"name":"b"}', '{"age":24}'
        ).values(3, '{"name":"c"}', '{"age":25}').values(
            4, '{"name":"d"}', '{"age":23}'
        ).execute()
        result = table.delete().where("n->'$.name' OVERLAPS 'a'").execute()
        result1 = table.delete().where("a->'$.age' OVERLAPS [22,24]").execute()
        self.assertEqual(result.get_affected_items_count(), 1)
        self.assertEqual(result1.get_affected_items_count(), 1)
        self.session.sql("drop table if exists t1").execute()

    @tests.foreach_session()
    def test_verlaps_table_delete2(self):
        """Not Overlaps in table.delete."""
        self.session.sql("drop table if exists t2").execute()
        self.session.sql(
            "create table t2(id int primary key, n JSON, a JSON)"
        ).execute()
        table = self.schema.get_table("t2")
        table.insert().values(1, '{"name":"a"}', '{"age":22}').values(
            2, '{"name":"b"}', '{"age":24}'
        ).values(3, '{"name":"c"}', '{"age":25}').values(
            4, '{"name":"d"}', '{"age":23}'
        ).execute()
        result = table.delete().where("n->'$.name' NOT OVERLAPS 'a'").execute()
        result1 = table.delete().where("a->'$.age' NOT OVERLAPS [22,24]").execute()
        self.assertEqual(result.get_affected_items_count(), 3)
        self.assertEqual(result1.get_affected_items_count(), 0)
        self.session.sql("drop table if exists t2").execute()

    @tests.foreach_session()
    def test_overlaps_table_delete3(self):
        """OVERLAPS operator with array/list operand on LHS and array/list
        on RHS."""
        self.session.sql("drop table if exists t3").execute()
        self.session.sql(
            "create table t3(idx int primary key, id JSON, n JSON, a JSON, addinfo JSON)"
        ).execute()
        table = self.schema.get_table("t3")
        table.insert().values(
            1,
            '{"_id":1}',
            '{"name":"joy"}',
            '{"age":21}',
            '{"additionalinfo":{"company":"xyz","vehicle":"bike","hobbies":["reading","music","playing"]}}',
        ).values(
            2,
            '{"_id":2}',
            '{"name":"happy"}',
            '{"age":24}',
            '{"additionalinfo":{"company":"abc","vehicle":"car","hobbies":["playing","painting","boxing"]}}',
        ).execute()
        result = (
            table.delete()
            .where(
                '["playing","painting","boxing"] OVERLAPS addinfo->$.additionalinfo.hobbies'
            )
            .execute()
        )
        # adding data
        table.insert().values(
            3,
            '{"_id":1}',
            '{"name":"joy"}',
            '{"age":21}',
            '{"additionalinfo":{"company":"xyz","vehicle":"bike","hobbies":["reading","music","playing"]}}',
        ).values(
            4,
            '{"_id":2}',
            '{"name":"happy"}',
            '{"age":24}',
            '{"additionalinfo":{"company":"abc","vehicle":"car","hobbies":["playing","painting","boxing"]}}',
        ).execute()
        result1 = table.delete().where('["happy","joy"] OVERLAPS n->$.name').execute()
        # Adding data
        table.insert().values(
            5,
            '{"_id":1}',
            '{"name":"joy"}',
            '{"age":21}',
            '{"additionalinfo":{"company":"xyz","vehicle":"bike","hobbies":["reading","music","playing"]}}',
        ).values(
            6,
            '{"_id":2}',
            '{"name":"happy"}',
            '{"age":24}',
            '{"additionalinfo":{"company":"abc","vehicle":"car","hobbies":["playing","painting","boxing"]}}',
        ).execute()
        result2 = (
            table.delete()
            .where('["car","bike"] NOT OVERLAPS addinfo->$.additionalinfo.vehicle')
            .execute()
        )
        self.assertEqual(result.get_affected_items_count(), 2)
        self.assertEqual(result1.get_affected_items_count(), 2)
        self.assertEqual(result2.get_affected_items_count(), 0)
        self.session.sql("drop table if exists t3").execute()

    @tests.foreach_session()
    def test_overlaps_table_delete4(self):
        """OVERLAPS operator with dict on LHS and dict on RHS."""
        self.session.sql("drop table if exists t4").execute()
        self.session.sql(
            "create table t4(idx int primary key, id JSON, n JSON, a JSON, addinfo JSON)"
        ).execute()
        table = self.schema.get_table("t4")
        table.insert().values(
            1,
            '{"_id":1}',
            '{"name":"joy"}',
            '{"age":21}',
            '{"additionalinfo":[{"company":"xyz","vehicle":"bike"},{"company":"abc","vehicle":"car"},{"company":"mno","vehicle":"zeep"}]}',
        ).values(
            2,
            '{"_id":2}',
            '{"name":"happy"}',
            '{"age":24}',
            '{"additionalinfo":[{"company":"abc","vehicle":"car"},{"company":"pqr","vehicle":"bicycle"}]}',
        ).values(
            3,
            '{"_id":3}',
            '{"name":"nice"}',
            '{"age":25}',
            '{"additionalinfo":{"company":"def","vehicle":"none"}}',
        ).execute()
        result = (
            table.delete()
            .where(
                '{"company":"abc","vehicle":"car"} OVERLAPS addinfo->$.additionalinfo'
            )
            .execute()
        )
        result1 = (
            table.delete()
            .where('{"vehicle":"car"} NOT OVERLAPS addinfo->$.additionalinfo')
            .execute()
        )
        # Adding data
        table.insert().values(
            4,
            '{"_id":1}',
            '{"name":"joy"}',
            '{"age":21}',
            '{"additionalinfo":[{"company":"xyz","vehicle":"bike"},{"company":"abc","vehicle":"car"},{"company":"mno","vehicle":"zeep"}]}',
        ).values(
            5,
            '{"_id":2}',
            '{"name":"happy"}',
            '{"age":24}',
            '{"additionalinfo":[{"company":"abc","vehicle":"car"},{"company":"pqr","vehicle":"bicycle"}]}',
        ).values(
            6,
            '{"_id":3}',
            '{"name":"nice"}',
            '{"age":25}',
            '{"additionalinfo":{"company":"mno"}}',
        ).execute()
        result2 = (
            table.delete()
            .where('{"company":"mno"} OVERLAPS addinfo->$.additionalinfo')
            .execute()
        )
        result3 = (
            table.delete()
            .where(
                '{"company":"abc","vehicle":"car"} NOT OVERLAPS addinfo->$.additionalinfo'
            )
            .execute()
        )
        self.assertEqual(result.get_affected_items_count(), 2)
        self.assertEqual(result1.get_affected_items_count(), 1)
        self.assertEqual(result2.get_affected_items_count(), 1)
        self.assertEqual(result3.get_affected_items_count(), 0)
        self.session.sql("drop table if exists t4").execute()
