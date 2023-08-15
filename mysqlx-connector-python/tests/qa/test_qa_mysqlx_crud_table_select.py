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

import threading
import time
import unittest

import mysqlx

import tests


@unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 25), "XPlugin not compatible")
class TableSelectTests(tests.MySQLxTests):
    """Tests for table.select()."""

    @tests.foreach_session()
    def test_table_select1(self):
        self.session.sql("drop table if exists t1").execute()
        """Test the table.select with where."""
        self.session.sql("create table t1(a int primary key, b int)").execute()
        table = self.schema.get_table("t1")
        table.insert().values(1, 1).values(2, 1).values(3, 2).execute()
        result = table.select("a").where("b== 1").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 2)
        self.assertEqual(row[1]["a"], 2)
        self.session.sql("drop table if exists t1").execute()

    @tests.foreach_session()
    def test_table_select2(self):
        """Test the table.select with sort and limit."""
        self.session.sql("drop table if exists t2").execute()
        self.session.sql("create table t2(a int primary key, b varchar(32))").execute()
        table = self.schema.get_table("t2")
        table.insert().values(1, "a").values(2, "c").values(3, "b").values(
            4, "d"
        ).execute()
        result = (
            table.select("a").sort("b ASC").limit(2).execute()
        )  # sort() is deprecated since 8.0.12, use order_by()
        row = result.fetch_all()
        self.assertEqual(len(row), 2)
        self.assertEqual(row[1]["a"], 3)
        self.session.sql("drop table if exists t2").execute()

    @tests.foreach_session()
    def test_table_select3(self):
        """Test the table.select with bind."""
        self.session.sql("drop table if exists t3").execute()
        self.session.sql("create table t3(a int primary key, b int)").execute()
        table = self.schema.get_table("t3")
        table.insert().values(1, 1).values(2, 1).values(3, 2).execute()
        result = table.select("a").where("b== :b").bind("b", 1).execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 2)
        self.assertEqual(row[1]["a"], 2)
        self.session.sql("drop table if exists t3").execute()

    @tests.foreach_session()
    def test_table_select4(self):
        """Test the table.select with no data."""
        self.session.sql("drop table if exists t4").execute()
        self.session.sql("create table t4(a int primary key, b int)").execute()
        table = self.schema.get_table("t4")
        result = table.select("a").where("b== 1").execute()
        row = result.fetch_all()
        self.assertEqual(row, [])
        self.session.sql("drop table if exists t4").execute()

    @tests.foreach_session()
    def test_table_select5(self):
        """Test the table.select on a invalid column."""
        self.session.sql("drop table if exists t5").execute()
        self.session.sql("create table t5(a int primary key, b int)").execute()
        table = self.schema.get_table("t5")
        table.insert().values(1, 1).values(2, 1).values(3, 2).execute()
        self.assertRaises(
            mysqlx.OperationalError,
            table.select("abc").execute,
        )
        self.session.sql("drop table if exists t5").execute()

    @tests.foreach_session()
    def test_table_select6(self):
        """Test the table.select."""
        self.session.sql("drop table if exists t6").execute()
        self.session.sql(
            "create table t6(a int primary key, name varchar(32),age int)"
        ).execute()
        table = self.schema.get_table("t6")
        table.insert().values(1, "a", 20).values(2, "a", 21).values(3, "b", 34).values(
            4, "b", 35
        ).execute()
        result = table.select().where("a > 1 and a < 4").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 2)
        self.assertEqual(row[0]["name"], "a")
        self.assertEqual(row[1]["name"], "b")
        self.session.sql("drop table if exists t6").execute()

    @tests.foreach_session()
    def test_table_select7(self):
        """Test the table.select with group by."""
        self.session.sql("drop table if exists t7").execute()
        self.session.sql("set sql_mode=''").execute()
        self.session.sql(
            "create table t7(a int primary key, name varchar(32),age int)"
        ).execute()
        table = self.schema.get_table("t7")
        table.insert().values(1, "a", 20).values(2, "a", 21).values(3, "b", 34).values(
            4, "b", 35
        ).execute()
        result = (
            table.select().group_by("name").sort("age ASC").having("age > 30").execute()
        )
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["age"], 34)
        self.session.sql("drop table if exists t7").execute()

    @tests.foreach_session()
    def test_table_select8(self):
        """Test the table.select with param list."""
        self.session.sql("drop table if exists t8").execute()
        self.session.sql("create table t8(a int primary key, b int, c int)").execute()
        table = self.schema.get_table("t8")
        table.insert().values(1, 3, 1).values(2, 1, 2).values(3, 2, 3).execute()
        result = table.select("a", "b").where("c== 1").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["a"], 1)
        self.assertEqual(row[0]["b"], 3)
        self.session.sql("drop table if exists t8").execute()

    @tests.foreach_session()
    def test_table_select9(self):
        """Test the table.select with group by and param."""
        self.session.sql("drop table if exists t9").execute()
        self.session.sql("set sql_mode=''").execute()
        self.session.sql("create table t9(id int primary key, a int , b int)").execute()
        table = self.schema.get_table("t9")
        table.insert().values(1, 1, 10).values(2, 1, 10).values(3, 2, 20).values(
            4, 2, 30
        ).execute()
        result = table.select("a", "b").group_by("a", "b").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["b"], 10)
        self.assertEqual(row[2]["b"], 30)
        self.session.sql("drop table if exists t9").execute()

    @tests.foreach_session()
    def test_table_select10(self):
        self.session.sql("drop table if exists t10").execute()
        self.session.sql(
            "create table t10(id int primary key, a int , b int)"
        ).execute()
        table = self.schema.get_table("t10")
        table.insert().values(1, 1, 10).values(2, 1, 11).values(3, 2, 10).values(
            4, 2, 11
        ).execute()
        result = table.select("a", "b").sort("a ASC", "b DESC").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["b"], 11)
        self.assertEqual(row[3]["b"], 10)
        self.session.sql("drop table if exists t10").execute()

    @tests.foreach_session()
    def test_table_select11(self):
        """Test for bug25519251."""
        self.session.sql("drop table if exists t11").execute()
        self.session.sql(
            "create table t11 (id int primary key, age INT, name VARCHAR(50))"
        ).execute()
        self.session.sql("INSERT INTO t11 VALUES (1, 21, 'Fred')").execute()
        self.session.sql("INSERT INTO t11 VALUES (2, 28, 'Barney')").execute()
        self.session.sql("INSERT INTO t11 VALUES (3, 42, 'Wilma')").execute()
        self.session.sql("INSERT INTO t11 VALUES (4, 67, 'Betty')").execute()
        table = self.schema.get_table("t11")

        result = table.select().order_by("age DESC").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["name"], "Betty")
        self.assertEqual(row[3]["name"], "Fred")
        self.session.sql("drop table if exists t11").execute()

    @tests.foreach_session()
    def test_table_select12(self):
        """Test table.select with limit() and offset() methods."""
        self.session.sql("drop table if exists t12").execute()
        self.session.sql(
            "create table t12(id int primary key, a int , b int)"
        ).execute()
        table = self.schema.get_table("t12")
        table.insert().values(1, 1, 10).values(2, 1, 11).values(3, 2, 10).values(
            4, 2, 11
        ).execute()
        result = (
            table.select("a", "b").limit(1, 1).execute()
        )  # limit(x,y) is deprecated since 8.0.12
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["b"], 11)
        self.session.sql("drop table if exists t12").execute()

    @tests.foreach_session()
    def test_table_select13(self):
        """Test table.select with negative value to limit() method."""
        self.session.sql("drop table if exists t13").execute()
        self.session.sql(
            "create table t13(id int primary key, a int , b int)"
        ).execute()
        table = self.schema.get_table("t13")
        table.insert().values(1, 1, 10).values(2, 1, 11).values(3, 2, 10).values(
            4, 2, 11
        ).execute()
        try:
            table.select("a", "b").limit(-1).execute()
        except ValueError:
            # Expected a ValueError
            pass
        self.session.sql("drop table if exists t13").execute()

    @tests.foreach_session()
    def test_table_select14(self):
        """Test table.select with negative value to offset() method."""
        self.session.sql("drop table if exists t14").execute()
        self.session.sql(
            "create table t14(id int primary key, a int , b int)"
        ).execute()
        table = self.schema.get_table("t14")
        table.insert().values(1, 1, 10).values(2, 1, 11).values(3, 2, 10).values(
            4, 2, 11
        ).execute()
        try:
            result = table.select("a", "b").limit(2).offset(-1).execute()
        except ValueError:
            # Expected a ValueError
            pass
        self.session.sql("drop table if exists t14").execute()

    @tests.foreach_session()
    def test_table_select15(self):
        """Test get_columns()."""
        self.session.sql("drop table if exists t15").execute()
        self.session.sql(
            "create table t15(a int primary key, name varchar(32),age int)"
        ).execute()
        table = self.schema.get_table("t15")
        table.insert().values(1, "a", 20).values(2, "a", 21).values(3, "b", 34).values(
            4, "b", 35
        ).execute()
        result = table.select().where("a > 1 and a < 4").execute()
        cols = result.get_columns()
        self.session.sql("drop table if exists t15").execute()

    @tests.foreach_session()
    def test_table_select16(self):
        """Test new properties of Column."""
        self.session.sql("drop table if exists t16").execute()
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql(
            "create table t16(a int primary key, name varchar(32),age int)"
        ).execute()
        table = self.schema.get_table("t16")
        table.insert().values(1, "a", 20).values(2, "a", 21).values(3, "b", 34).values(
            4, "b", 35
        ).execute()
        result = table.select().where("a > 1 and a < 4").execute()
        cols = result.get_columns()
        col0 = cols[0]
        self.assertEqual(col0.schema_name, schema_name)
        self.assertEqual(col0.table_name, "t16")
        self.assertEqual(col0.table_label, "t16")
        self.assertEqual(col0.column_name, "a")
        self.assertEqual(col0.column_label, "a")
        self.assertEqual(col0.type, mysqlx.ColumnType.INT)
        self.assertEqual(col0.length, 11)
        self.assertEqual(col0.fractional_digits, 0)
        col1 = cols[1]
        self.assertEqual(col1.schema_name, schema_name)
        self.assertEqual(col1.table_name, "t16")
        self.assertEqual(col1.table_label, "t16")
        self.assertEqual(col1.column_name, "name")
        self.assertEqual(col1.column_label, "name")
        self.assertEqual(col1.type, mysqlx.ColumnType.STRING)
        self.assertEqual(col1.collation_name, "utf8mb4_0900_ai_ci")
        self.assertEqual(col1.character_set_name, "utf8mb4")
        self.session.sql("drop table if exists t16").execute()

    @tests.foreach_session()
    def test_operator2(self):
        """Test unary operator not."""
        self.session.sql("drop table if exists t1").execute()
        self.session.sql("create table t1(a int primary key, b boolean)").execute()
        table = self.schema.get_table("t1")
        table.insert().values(1, True).values(2, False).execute()
        result = table.select("not b as b").where("a == 1").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["b"], 0)
        self.session.sql("drop table if exists t1").execute()

    @tests.foreach_session()
    def test_operator3(self):
        """Test binary operator in."""
        self.session.sql("drop table if exists t2").execute()
        self.session.sql("create table t2(a int primary key, b int)").execute()
        table = self.schema.get_table("t2")
        table.insert().values(1, 1).values(2, 2).execute()
        result = table.select("2 IN (0,2,4,6)").execute()
        result.fetch_all()
        self.session.sql("drop table if exists t2").execute()

    @tests.foreach_session()
    def test_operator5(self):
        self.session.sql("drop table if exists t3").execute()
        self.session.sql("create table t3(id int primary key, data json)").execute()
        table = self.schema.get_table("t3")
        table.insert().values(1, '{"_id":1,"age":20}').execute()
        result = table.select("data->'$.age' as age").execute()
        result.fetch_all()
        self.session.sql("drop table if exists t3").execute()

    # Testing the contains operator with single operand on both sides

    @tests.foreach_session()
    def test_contains_operator_select1(self):
        """Test IN operator with string on both sides - With LHS in RHS."""
        self.session.sql("drop table if exists t1").execute()
        self.session.sql("create table t1(a int primary key, b int, c JSON)").execute()
        table = self.schema.get_table("t1")
        table.insert().values(1, 1, '{"comp":"abc"}').values(
            2, 1, '{"comp":"pqr"}'
        ).values(3, 2, '{"comp":"xyz"}').execute()
        result = table.select("a").where("'pqr' IN c->'$.comp'").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["a"], 2)
        self.session.sql("drop table if exists t1").execute()

    @tests.foreach_session()
    def test_contains_operator_select2(self):
        """Test IN operator with int as operand - With LHS in RHS."""
        self.session.sql("drop table if exists t2").execute()
        self.session.sql(
            "create table t2(id int primary key, name varchar(20) , a JSON, c varchar(20))"
        ).execute()
        table = self.schema.get_table("t2")
        table.insert().values(1, "a", '{"age":21}', "abc").values(
            2, "b", '{"age":31}', "pqr"
        ).values(3, "hell", '{"age":22}', "xyz").execute()
        result = table.select().where("a->$.age IN [21,31]").execute()
        self.assertEqual(len(result.fetch_all()), 2)
        self.session.sql("drop table if exists t2").execute()

    @tests.foreach_session()
    def test_contains_operator_select3(self):
        """Test IN operator with boolean as operand - With LHS in RHS."""
        self.session.sql("drop table if exists t3").execute()
        self.session.sql(
            "create table t3(id int primary key, n JSON, a json, c JSON)"
        ).execute()
        table = self.schema.get_table("t3")
        table.insert().values(
            1, '{"name":"joy"}', '{"age":18}', '{"comp":"abc"}'
        ).values(2, '{"name":"happy"}', '{"age":21}', '{"comp":"pqr"}').values(
            3, '{"name":"sad"}', '{"age":32}', '{"comp":"xyz"}'
        ).execute()
        result = table.select().where("21 IN a->'$.age'").execute()
        self.assertEqual(len(result.fetch_all()), 1)
        self.session.sql("drop table if exists t3").execute()

    @tests.foreach_session()
    def test_contains_operator_select4(self):
        """Test NOT IN operator with string operand - With LHS not in RHS."""
        self.session.sql("drop table if exists t4").execute()
        self.session.sql(
            "create table t4(id int primary key, n JSON, age int, c JSON)"
        ).execute()
        table = self.schema.get_table("t4")
        table.insert().values(1, '{"name":"joy"}', 18, '{"comp":"abc"}').values(
            2, '{"name":"happy"}', 21, '{"comp":"pqr"}'
        ).values(3, '{"name":"sad"}', 32, '{"comp":"xyz"}').execute()
        result = table.select().where("'happy' NOT IN n->'$.name'").execute()
        self.assertEqual(len(result.fetch_all()), 2)
        self.session.sql("drop table if exists t4").execute()

    @tests.foreach_session()
    def test_contains_operator_select5(self):
        """Test NOT IN operator with int as operand - With LHS not in RHS."""
        self.session.sql("drop table if exists t5").execute()
        self.session.sql(
            "create table t5(id int primary key, n JSON, a JSON, c JSON)"
        ).execute()
        table = self.schema.get_table("t5")
        table.insert().values(
            1, '{"name":"joy"}', '{"age":18}', '{"comp":"abc"}'
        ).values(2, '{"name":"happy"}', '{"age":21}', '{"comp":"pqr"}').values(
            3, '{"name":"sad"}', '{"age":32}', '{"comp":"xyz"}'
        ).execute()
        result = table.select().where("21 NOT IN a->'$.age'").execute()
        self.assertEqual(len(result.fetch_all()), 2)
        self.session.sql("drop table if exists t5").execute()

    @tests.foreach_session()
    def test_contains_operator_select6(self):
        """Test NOT IN operator with boolean as operand - With LHS not in RHS."""
        self.session.sql("drop table if exists t6").execute()
        self.session.sql(
            "create table t6(id int primary key, name varchar(20) , age int, c json)"
        ).execute()
        table = self.schema.get_table("t6")
        table.insert().values(1, "a", 21, '{"comp":"pqr"}').values(
            2, "b", 31, '{"comp":"xyz"}'
        ).values(3, "e", 22, '{"comp":"xyz"}').execute()
        result = table.select().where("c->'$.comp' IN ['pqr','abc']").execute()
        self.assertEqual(len(result.fetch_all()), 1)
        self.session.sql("drop table if exists t6").execute()

    @tests.foreach_session()
    def test_contains_operator_select7(self):
        """Test IN operator with different datatypes as operands."""
        self.session.sql("drop table if exists t7").execute()
        self.session.sql(
            "create table t7(id int primary key, n JSON, a json, c JSON)"
        ).execute()
        table = self.schema.get_table("t7")
        table.insert().values(
            1, '{"name":"joy"}', '{"age":18}', '{"comp":"abc"}'
        ).values(2, '{"name":"happy"}', '{"age":21}', '{"comp":"pqr"}').values(
            3, '{"name":"sad"}', '{"age":32}', '{"comp":"xyz"}'
        ).execute()
        result = table.select().where("21 IN n->'$.name'").execute()
        result1 = table.select().where("'b' IN a->$.age").limit(1).execute()
        self.assertEqual(len(result.fetch_all()), 0)
        self.assertEqual(len(result1.fetch_all()), 0)
        self.session.sql("drop table if exists t7").execute()

    @tests.foreach_session()
    def test_contains_operator_select8(self):
        """Test IN operator with single element on LHS and array/list on RHS
        and vice versa."""
        self.session.sql("drop table if exists t8").execute()
        self.session.sql(
            "create table t8(id int primary key, n JSON, a JSON, p JSON)"
        ).execute()
        table = self.schema.get_table("t8")
        table.insert().values(
            1, '{"name":"a"}', '{"age":21}', '{"prof":["x","y"]}'
        ).values(2, '{"name":"b"}', '{"age":24}', '{"prof":["p","q"]}').values(
            3, '{"name":"c"}', '{"age":26}', '{"prof":["l","m"]}'
        ).execute()
        result = table.select().where("a->$.age IN [21,23,24,28]").execute()
        result1 = table.select().where("n->'$.name' IN ['a','b','c','d','e']").execute()
        result2 = table.select().where("a->$.age IN (21,23)").execute()
        result3 = table.select().where("21 IN (22,23)").limit(1).execute()
        result4 = table.select().where("['p','q'] IN p->'$.prof'").execute()
        self.assertEqual(len(result.fetch_all()), 2)
        self.assertEqual(len(result1.fetch_all()), 3)
        self.assertEqual(len(result2.fetch_all()), 1)
        self.assertEqual(len(result3.fetch_all()), 0)
        self.assertEqual(len(result4.fetch_all()), 1)
        self.session.sql("drop table if exists t8").execute()

    @tests.foreach_session()
    def test_contains_operator_select9(self):
        """Test IN operator with single element on LHS and dict on RHS and
        vice versa."""
        self.session.sql("drop table if exists t9").execute()
        self.session.sql(
            "create table t9(id int primary key, name varchar(20), a JSON, ai JSON)"
        ).execute()
        table = self.schema.get_table("t9")
        table.insert().values(
            1,
            "a",
            '{"age":23}',
            '{"additionalinfo":["reading","music","playing"]}',
        ).values(
            2, "b", '{"age":21}', '{"additionalinfo":["boxing","music"]}'
        ).execute()
        result = table.select().where("'reading' IN ai->$.additionalinfo").execute()
        result1 = table.select().where("'music' IN a->$.age").execute()
        result2 = table.select().where("'music' IN ai->$.additionalinfo").execute()
        self.assertEqual(len(result.fetch_all()), 1)
        self.assertEqual(len(result1.fetch_all()), 0)
        self.assertEqual(len(result2.fetch_all()), 2)
        self.session.sql("drop table if exists t9").execute()

    @tests.foreach_session()
    def test_contains_operator_select10(self):
        """Test IN operator with array/list operand on LHS and array/list on
        RHS."""
        self.session.sql("drop table if exists t10").execute()
        self.session.sql(
            "create table t10(id int primary key, i JSON, n JSON, a JSON, ai JSON)"
        ).execute()
        table = self.schema.get_table("t10")
        table.insert().values(
            1,
            '{"id":1}',
            '{"name":"joy"}',
            '{"age":21}',
            '{"additionalinfo":["reading","music","playing"]}',
        ).values(
            2,
            '{"id":2}',
            '{"name":"happy"}',
            '{"age":24}',
            '{"additionalinfo":["playing","painting","boxing"]}',
        ).execute()
        result = (
            table.select()
            .where("['playing','painting','boxing'] IN ai->'$.additionalinfo'")
            .execute()
        )
        result1 = (
            table.select().where('["happy","joy"] IN n->$.name').limit(1).execute()
        )
        result2 = (
            table.select()
            .where('["reading"] NOT IN ai->$.additionalinfo')
            .limit(1)
            .execute()
        )
        self.assertEqual(len(result.fetch_all()), 1)
        self.assertEqual(len(result1.fetch_all()), 0)
        self.assertEqual(len(result2.fetch_all()), 1)
        self.session.sql("drop table if exists t10").execute()

    @tests.foreach_session()
    def test_contains_operator_select11(self):
        """Test IN operator with dict on LHS and dict on RHS."""
        self.session.sql("drop table if exists t11").execute()
        self.session.sql(
            "create table t11(id int primary key, i JSON, n JSON, a JSON, ai JSON)"
        ).execute()
        table = self.schema.get_table("t11")
        table.insert().values(
            1,
            '{"id":1}',
            '{"name":"joy"}',
            '{"age":21}',
            '{"additionalinfo":[{"company":"xyz","vehicle":"bike"},{"company":"abc","vehicle":"car"},{"company":"mno","vehicle":"zeep"}]}',
        ).values(
            2,
            '{"id":2}',
            '{"name":"happy"}',
            '{"age":24}',
            '{"additionalinfo":[{"company":"abc","vehicle":"car"},{"company":"pqr","vehicle":"bicycle"}]}',
        ).execute()
        result = table.select(
            '{"company":"abc","vehicle":"car"} IN ai->"$.additionalinfo"'
        ).execute()
        result1 = (
            table.select()
            .where('{"vehicle":"car"} NOT IN ai->"$.additionalinfo"')
            .execute()
        )
        result2 = (
            table.select()
            .where('{"company":"mno"} IN ai->"$.additionalinfo"')
            .execute()
        )
        self.assertEqual(len(result.fetch_all()), 2)
        self.assertEqual(len(result1.fetch_all()), 0)
        self.assertEqual(len(result2.fetch_all()), 1)
        self.session.sql("drop table if exists t11").execute()

    @tests.foreach_session()
    def test_contains_operator_select12(self):
        """Test IN operator with operands having expressions."""
        self.session.sql("drop table if exists t12").execute()
        self.session.sql(
            "create table t12(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t12")
        table.insert().values(1, "a", 21).values(2, "b", 22).values(
            3, "c", 32
        ).execute()
        result = table.select().where("(1>5) IN (true, false)").limit(1).execute()
        result1 = table.select().where("('a'>'b') in (true, false)").limit(1).execute()
        result2 = (
            table.select()
            .where("true IN [(1>5), !(false), (true || false), (false && true)]")
            .limit(1)
            .execute()
        )
        self.assertEqual(len(result.fetch_all()), 1)
        self.assertEqual(len(result1.fetch_all()), 1)
        self.assertEqual(len(result2.fetch_all()), 1)
        self.session.sql("drop table if exists t12").execute()

    @tests.foreach_session()
    def test_contains_operator_select13(self):
        """Test IN operator with operands having expressions."""
        self.session.sql("drop table if exists t13").execute()
        self.session.sql(
            "create table t13(id int primary key, i json, n json, a json)"
        ).execute()
        table = self.schema.get_table("t13")
        table.insert().values(1, '{"id":1}', '{"name":"a"}', '{"age":21}').values(
            2, '{"id":2}', '{"name":"b"}', '{"age":22}'
        ).execute()
        self.assertRaises(
            mysqlx.OperationalError,
            table.select().where("(1+5) IN [1,2,3,4,5,6]").execute,
        )
        table.select().where("(2+3) IN (1,2,3,4)").limit(1).execute()
        self.session.sql("drop table if exists t13").execute()

    @tests.foreach_session()
    def test_contains_operator_select14(self):
        """Test IN operator: search for empty string in a field and field in
        empty string."""
        self.session.sql("drop table if exists t14").execute()
        self.session.sql(
            "create table t14(id int primary key, n JSON, age int)"
        ).execute()
        table = self.schema.get_table("t14")
        table.insert().values(1, '{"name":"a"}', 21).values(
            2, '{"name":"b"}', 22
        ).values(3, '{"name":"c"}', 32).execute()
        result = table.select().where("'' IN n->'$.name'").execute()
        result1 = table.select().where("n->'$.name' IN ['', ' ']").execute()
        result2 = table.select().where("n->'$.name' IN ('', ' ')").execute()
        self.assertEqual(len(result.fetch_all()), 0)
        self.assertEqual(len(result1.fetch_all()), 0)
        self.assertEqual(len(result2.fetch_all()), 0)
        self.session.sql("drop table if exists t14").execute()

    @tests.foreach_session()
    def test_table_s_s_lock(self):
        """Test shared-shared lock."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t1").execute()
        self.session.sql(
            "create table t1(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t1")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(schema_name)
            table = schema1.get_table("t1")

            session1.start_transaction()
            table.select().where("name = 'James'").lock_shared().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if waiting.is_set():
                session1.commit()
                self.fail(
                    "Table_S_S_Lock_test IS NOT OK. Other thread is waiting "
                    "while it is not expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(schema_name)
            table = schema2.get_table("t1")

            if not locking.wait(2):
                self.fail(
                    "Table_S_S_Lock_test IS NOT OK. Other thread has not set "
                    "the lock!"
                )
            session2.start_transaction()

            waiting.set()
            table.select().where("name = 'James'").lock_shared().execute()
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
        self.session.sql("drop table if exists t1").execute()

    @tests.foreach_session()
    def test_table_s_x_lock(self):
        """Test shared-exclusive lock."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t2").execute()
        self.session.sql(
            "create table t2(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t2")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(schema_name)
            table = schema1.get_table("t2")

            session1.start_transaction()
            table.select().where("name = 'James'").lock_shared().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if not waiting.is_set():
                session1.commit()
                self.fail(
                    "Table_S_X_Lock_test IS NOT OK. Other thread is not "
                    "waiting while it is expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(schema_name)
            table = schema2.get_table("t2")

            if not locking.wait(2):
                self.fail(
                    "Table_S_X_Lock_test IS NOT OK. Other thread has not set "
                    "the lock!"
                )
            session2.start_transaction()

            waiting.set()
            table.select().where("name = 'James'").lock_exclusive().execute()
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
        self.session.sql("drop table if exists t2").execute()

    @tests.foreach_session()
    def test_table_x_x_lock(self):
        """Test clusive-exclusive lock."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t3").execute()
        self.session.sql(
            "create table t3(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t3")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(schema_name)
            table = schema1.get_table("t3")

            session1.start_transaction()
            table.select().where("name = 'James'").lock_exclusive().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if not waiting.is_set():
                session1.commit()
                self.fail(
                    "Table_X_X_Lock_test IS NOT OK. Other thread is not "
                    "waiting while it is expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(schema_name)
            table = schema2.get_table("t3")

            if not locking.wait(2):
                self.fail(
                    "Table_X_X_Lock_test IS NOT OK. Other thread has not set "
                    "the lock!"
                )
            session2.start_transaction()

            waiting.set()
            table.select().where("name = 'James'").lock_exclusive().execute()
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
        self.session.sql("drop table if exists t3").execute()

    @tests.foreach_session()
    def test_table_x_s_lock(self):
        """Test exclusive-exclusive lock."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t4").execute()
        self.session.sql(
            "create table t4(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t4")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(schema_name)
            table = schema1.get_table("t4")

            session1.start_transaction()
            table.select().where("name = 'James'").lock_exclusive().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if not waiting.is_set():
                session1.commit()
                self.fail(
                    "Table_X_S_Lock_test IS NOT OK. Other thread is not "
                    "waiting while it is expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(schema_name)
            table = schema2.get_table("t4")

            if not locking.wait(2):
                self.fail(
                    "Table_X_S_Lock_test IS NOT OK. Other thread has not set "
                    "the lock!"
                )
            session2.start_transaction()

            waiting.set()
            table.select().where("name = 'James'").lock_shared().execute()
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
        self.session.sql("drop table if exists t4").execute()

    @tests.foreach_session()
    def test_table_multiple_lock_calls(self):
        """Test multiple lock calls."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t5").execute()
        self.session.sql(
            "create table t5(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t5")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(schema_name)
            table = schema1.get_table("t5")

            session1.start_transaction()
            table.select().where(
                "name = 'James'"
            ).lock_exclusive().lock_shared().lock_exclusive().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if not waiting.is_set():
                session1.commit()
                self.fail(
                    "Table_Multiple_Lock_calls_test IS NOT OK. Other thread "
                    "is not waiting while it is expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(schema_name)
            table = schema2.get_table("t5")

            if not locking.wait(2):
                self.fail(
                    "Table_Multiple_Lock_calls_test IS NOT OK. Other thread "
                    "has not set the lock!"
                )
            session2.start_transaction()

            waiting.set()
            table.select().where(
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
        self.session.sql("drop table if exists t5").execute()

    @tests.foreach_session()
    def test_table_x_lock_update(self):
        """Test lock exclusive and update().where - update().where will be
        blocked until the lock is released."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t6").execute()
        self.session.sql(
            "create table t6(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t6")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(schema_name)
            table = schema1.get_table("t6")

            session1.start_transaction()
            table.select().where(
                "name = 'James'"
            ).lock_exclusive().lock_shared().lock_exclusive().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if not waiting.is_set():
                session1.commit()
                self.fail(
                    "Table_X_Lock_Update_test IS NOT OK. Other thread is not "
                    "waiting while it is expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(schema_name)
            table = schema2.get_table("t6")

            if not locking.wait(2):
                self.fail(
                    "Table_X_Lock_Update_test IS NOT OK. Other thread has not "
                    "set the lock!"
                )
            session2.start_transaction()

            waiting.set()
            table.update().where("name == 'James'").set("age", 30).execute()
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
        self.session.sql("drop table if exists t6").execute()

    @tests.foreach_session()
    def test_table_s_lock_update(self):
        """Test lock shared and update().where - update().where will be blocked
        until the lock is released, but will be able to read."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t7").execute()
        self.session.sql(
            "create table t7(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t7")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(schema_name)
            table = schema1.get_table("t7")

            session1.start_transaction()
            table.select().where(
                "name = 'James'"
            ).lock_exclusive().lock_shared().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if not waiting.is_set():
                session1.commit()
                self.fail(
                    "Table_S_Lock_Update_test IS NOT OK. Other thread is not "
                    "waiting while it is expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(schema_name)
            table = schema2.get_table("t7")

            if not locking.wait(2):
                self.fail(
                    "Table_S_Lock_Update_test IS NOT OK. Other thread has not "
                    "set the lock!"
                )
            session2.start_transaction()
            result = table.select().where("name == 'James'").execute()
            self.assertEqual(result.fetch_all()[0]["age"], 23)
            waiting.set()
            table.update().where("name == 'James'").set("age", 30).execute()
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
        self.session.sql("drop table if exists t7").execute()

    @tests.foreach_session()
    def test_table_s_s_nowait(self):
        """Test shared-shared with NOWAIT lockcontention."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t").execute()
        self.session.sql(
            "create table t(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        # `session2.lock_shared(NOWAIT) returns data immediately.
        session1 = mysqlx.get_session(config)
        schema1 = session1.get_schema(schema_name)
        table1 = schema1.get_table("t")
        session1.start_transaction()
        table1.select().where("name = 'James'").lock_shared().execute()

        session2 = mysqlx.get_session(config)
        schema2 = session2.get_schema(schema_name)
        table2 = schema2.get_table("t")
        session2.start_transaction()
        result = (
            table2.select()
            .where("name = 'James'")
            .lock_shared(mysqlx.LockContention.NOWAIT)
            .execute()
        )
        res = result.fetch_all()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["age"], 23)
        session2.rollback()

        session1.rollback()

        self.session.sql("drop table if exists t").execute()
        session2.close()
        session1.close()

    @tests.foreach_session()
    def test_table_s_x_nowait(self):
        """Test shared-exclusive with NOWAIT lockcontention."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t").execute()
        self.session.sql(
            "create table t(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        # `session2.lock_exclusive(NOWAIT) returns data immediately.
        session1 = mysqlx.get_session(config)
        schema1 = session1.get_schema(schema_name)
        table1 = schema1.get_table("t")
        session1.start_transaction()
        table1.select().where("name = 'James'").lock_shared().execute()

        session2 = mysqlx.get_session(config)
        schema2 = session2.get_schema(schema_name)
        table2 = schema2.get_table("t")
        session2.start_transaction()
        try:
            table2.select().where("name = 'James'").lock_exclusive(
                mysqlx.LockContention.NOWAIT
            ).execute()
        except mysqlx.OperationalError:
            # Expected OperationalError
            pass
        session2.rollback()
        session1.rollback()
        self.session.sql("drop table if exists t").execute()
        session2.close()
        session1.close()

    @tests.foreach_session()
    def test_table_x_x_nowait(self):
        """Test exclusive-exclusive with NOWAIT lockcontention."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t").execute()
        self.session.sql(
            "create table t(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        # `session2.lock_exclusive(NOWAIT) returns data immediately.
        session1 = mysqlx.get_session(config)
        schema1 = session1.get_schema(schema_name)
        table1 = schema1.get_table("t")
        session1.start_transaction()
        table1.select().where("name = 'James'").lock_exclusive().execute()

        session2 = mysqlx.get_session(config)
        schema2 = session2.get_schema(schema_name)
        table2 = schema2.get_table("t")
        session2.start_transaction()
        try:
            table2.select().where("name = 'James'").lock_exclusive(
                mysqlx.LockContention.NOWAIT
            ).execute()
        except mysqlx.OperationalError:
            # Expected OperationalError
            pass
        session2.rollback()
        session1.rollback()
        self.session.sql("drop table if exists t").execute()
        session2.close()
        session1.close()

    @tests.foreach_session()
    def test_table_x_s_nowait(self):
        """Test exclusive-shared with NOWAIT lockcontention."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t").execute()
        self.session.sql(
            "create table t(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        # `session2.lock_exclusive(NOWAIT) returns data immediately.
        session1 = mysqlx.get_session(config)
        schema1 = session1.get_schema(schema_name)
        table1 = schema1.get_table("t")
        session1.start_transaction()
        table1.select().where("name = 'James'").lock_exclusive().execute()

        session2 = mysqlx.get_session(config)
        schema2 = session2.get_schema(schema_name)
        table2 = schema2.get_table("t")
        session2.start_transaction()
        try:
            table2.select().where("name = 'James'").lock_shared(
                mysqlx.LockContention.NOWAIT
            ).execute()
        except mysqlx.OperationalError:
            # Expected OperationalError
            pass
        session2.rollback()
        session1.rollback()
        self.session.sql("drop table if exists t").execute()
        session2.close()
        session1.close()

    @tests.foreach_session()
    def test_table_s_s_skip_locked(self):
        """Test shared-shared with SKIP LOCKED lockcontention."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t").execute()
        self.session.sql(
            "create table t(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        # `session2.lock_exclusive(SKIP_LOCKED) returns data immediately.
        session1 = mysqlx.get_session(config)
        schema1 = session1.get_schema(schema_name)
        table1 = schema1.get_table("t")
        session1.start_transaction()
        table1.select().where("name = 'James'").lock_shared().execute()

        session2 = mysqlx.get_session(config)
        schema2 = session2.get_schema(schema_name)
        table2 = schema2.get_table("t")
        session2.start_transaction()
        result = (
            table2.select()
            .where("name = 'James'")
            .lock_shared(mysqlx.LockContention.SKIP_LOCKED)
            .execute()
        )
        res = result.fetch_all()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["age"], 23)
        session2.rollback()

        session1.rollback()
        self.session.sql("drop table if exists t").execute()
        session2.close()
        session1.close()

    @tests.foreach_session()
    def test_table_s_x_skip_locked(self):
        """Test shared-exclusive with SKIP LOCKED lockcontention."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t").execute()
        self.session.sql(
            "create table t(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        # `session2.lock_exclusive(SKIP_LOCKED) returns data immediately.
        session1 = mysqlx.get_session(config)
        schema1 = session1.get_schema(schema_name)
        table1 = schema1.get_table("t")
        session1.start_transaction()
        table1.select().where("name = 'James'").lock_shared().execute()

        session2 = mysqlx.get_session(config)
        schema2 = session2.get_schema(schema_name)
        table2 = schema2.get_table("t")
        session2.start_transaction()
        result = (
            table2.select()
            .where("name = 'James'")
            .lock_exclusive(mysqlx.LockContention.SKIP_LOCKED)
            .execute()
        )
        res = result.fetch_all()
        self.assertEqual(len(res), 0)
        session2.rollback()
        session1.rollback()
        self.session.sql("drop table if exists t").execute()
        session2.close()
        session1.close()

    @tests.foreach_session()
    def test_table_x_x_skip_locked(self):
        """Test exclusive-exclusive with SKIP LOCKED lockcontention."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t").execute()
        self.session.sql(
            "create table t(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        # `session2.lock_exclusive(SKIP_LOCKED) returns data immediately.
        session1 = mysqlx.get_session(config)
        schema1 = session1.get_schema(schema_name)
        table1 = schema1.get_table("t")
        session1.start_transaction()
        table1.select().where("name = 'James'").lock_exclusive().execute()

        session2 = mysqlx.get_session(config)
        schema2 = session2.get_schema(schema_name)
        table2 = schema2.get_table("t")
        session2.start_transaction()
        result = (
            table2.select()
            .where("name = 'James'")
            .lock_exclusive(mysqlx.LockContention.SKIP_LOCKED)
            .execute()
        )
        res = result.fetch_all()
        self.assertEqual(len(res), 0)
        session2.rollback()
        session1.rollback()
        self.session.sql("drop table if exists t").execute()
        session2.close()
        session1.close()

    @tests.foreach_session()
    def test_table_x_s_skip_locked(self):
        """Test exclusive-shared with SKIP LOCKED lockcontention."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t").execute()
        self.session.sql(
            "create table t(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        # `session2.lock_exclusive(SKIP_LOCKED) returns data immediately.
        session1 = mysqlx.get_session(config)
        schema1 = session1.get_schema(schema_name)
        table1 = schema1.get_table("t")
        session1.start_transaction()
        table1.select().where("name = 'James'").lock_exclusive().execute()

        session2 = mysqlx.get_session(config)
        schema2 = session2.get_schema(schema_name)
        table2 = schema2.get_table("t")
        session2.start_transaction()
        result = (
            table2.select()
            .where("name = 'James'")
            .lock_shared(mysqlx.LockContention.SKIP_LOCKED)
            .execute()
        )
        res = result.fetch_all()
        self.assertEqual(len(res), 0)
        session2.rollback()
        session1.rollback()
        self.session.sql("drop table if exists t").execute()
        session2.close()
        session1.close()

    @tests.foreach_session()
    def test_table_s_s_default(self):
        """Test shared-shared lock."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t1").execute()
        self.session.sql(
            "create table t1(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t1")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(schema_name)
            table = schema1.get_table("t1")

            session1.start_transaction()
            table.select().where("name = 'James'").lock_shared().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if waiting.is_set():
                session1.commit()
                self.fail(
                    "Table_S_S_DEFAULT_test IS NOT OK. Other thread is "
                    "waiting while it is not expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(schema_name)
            table = schema2.get_table("t1")

            if not locking.wait(2):
                self.fail(
                    "Table_S_S_DEFAULT_test IS NOT OK. Other thread has not "
                    "set the lock!"
                )
            session2.start_transaction()

            waiting.set()
            table.select().where("name = 'James'").lock_shared(
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
        self.session.sql("drop table if exists t1").execute()

    @tests.foreach_session()
    def test_table_s_x_default(self):
        """Test shared-exclusive lock."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t2").execute()
        self.session.sql(
            "create table t2(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t2")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(schema_name)
            table = schema1.get_table("t2")

            session1.start_transaction()
            table.select().where("name = 'James'").lock_shared().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if not waiting.is_set():
                session1.commit()
                self.fail(
                    "Table_S_X_DEFAULT_test IS NOT OK. Other thread is not "
                    "waiting while it is expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(schema_name)
            table = schema2.get_table("t2")

            if not locking.wait(2):
                self.fail(
                    "Table_S_X_DEFAULT_test IS NOT OK. Other thread has not "
                    "set the lock!"
                )
            session2.start_transaction()

            waiting.set()
            table.select().where("name = 'James'").lock_exclusive(
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
        self.session.sql("drop table if exists t2").execute()

    @tests.foreach_session()
    def test_table_x_x_default(self):
        """Test exclusive-exclusive lock."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t3").execute()
        self.session.sql(
            "create table t3(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t3")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(schema_name)
            table = schema1.get_table("t3")

            session1.start_transaction()
            table.select().where("name = 'James'").lock_exclusive().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if not waiting.is_set():
                session1.commit()
                self.fail(
                    "Table_X_X_DEFAULT_test IS NOT OK. Other thread is not "
                    "waiting while it is expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(schema_name)
            table = schema2.get_table("t3")

            if not locking.wait(2):
                self.fail(
                    "Table_X_X_DEFAULT_test IS NOT OK. Other thread has not "
                    "set the lock!"
                )
            session2.start_transaction()

            waiting.set()
            table.select().where("name = 'James'").lock_exclusive(
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
        self.session.sql("drop table if exists t3").execute()

    @tests.foreach_session()
    def test_table_x_s_default(self):
        """Test exclusive-exclusive lock."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t4").execute()
        self.session.sql(
            "create table t4(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t4")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(schema_name)
            table = schema1.get_table("t4")

            session1.start_transaction()
            table.select().where("name = 'James'").lock_exclusive().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if not waiting.is_set():
                session1.commit()
                self.fail(
                    "Table_X_S_DEFAULT_test IS NOT OK. Other thread is not "
                    "waiting while it is expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(schema_name)
            table = schema2.get_table("t4")

            if not locking.wait(2):
                self.fail(
                    "Table_X_S_DEFAULT_test IS NOT OK. Other thread has not "
                    "set the lock!"
                )
            session2.start_transaction()

            waiting.set()
            table.select().where("name = 'James'").lock_shared(
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
        self.session.sql("drop table if exists t4").execute()

    @tests.foreach_session()
    def test_table_multiple_lock_contention_calls(self):
        """Test multiple lock calls."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t5").execute()
        self.session.sql(
            "create table t5(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t5")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(schema_name)
            table = schema1.get_table("t5")

            session1.start_transaction()
            table.select().where(
                "name = 'James'"
            ).lock_exclusive().lock_shared().lock_exclusive().execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if waiting.is_set():
                session1.commit()
                self.fail(
                    "Table_Multiple_Lock_calls_test IS NOT OK. Other thread "
                    "is waiting while it is not expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(schema_name)
            table = schema2.get_table("t5")

            if not locking.wait(2):
                self.fail(
                    "Table_Multiple_Lock_calls_test IS NOT OK. Other thread "
                    "has not set the lock!"
                )
            session2.start_transaction()

            waiting.set()
            result = (
                table.select()
                .where("name = 'James'")
                .lock_shared(mysqlx.LockContention.DEFAULT)
                .lock_exclusive(mysqlx.LockContention.SKIP_LOCKED)
                .lock_exclusive(mysqlx.LockContention.NOWAIT)
                .lock_shared(mysqlx.LockContention.SKIP_LOCKED)
                .execute()
            )
            res = result.fetch_all()
            assert len(res) == 0
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

        self.session.sql("drop table if exists t5").execute()

    @tests.foreach_session()
    def test_table_update_x_nowait(self):
        """Test lock exclusive and update().where - update().where will be
        blocked until the lock is released."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t6").execute()
        self.session.sql(
            "create table t6(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t6")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        session1 = mysqlx.get_session(config)
        schema1 = session1.get_schema(schema_name)
        table = schema1.get_table("t6")
        session1.start_transaction()
        table.update().where("name == 'James'").set("age", 30).execute()

        session2 = mysqlx.get_session(config)
        schema2 = session2.get_schema(schema_name)
        table = schema2.get_table("t6")
        session2.start_transaction()
        try:
            table.select().where("name = 'James'").lock_exclusive(
                mysqlx.LockContention.DEFAULT
            ).lock_shared(mysqlx.LockContention.SKIP_LOCKED).lock_exclusive(
                mysqlx.LockContention.NOWAIT
            ).execute()
        except mysqlx.OperationalError:
            # Expected OperationalError
            pass
        session2.rollback()
        session1.rollback()
        self.session.sql("drop table if exists t6").execute()
        session2.close()
        session1.close()

    @tests.foreach_session()
    def test_table_update_s_skip_locked(self):
        """Test lock shared and update().where - update().where will be
        blocked until the lock is released, but will be able to read."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("drop table if exists t7").execute()
        self.session.sql(
            "create table t7(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t7")
        table.insert().values(1, "Joe", 21).values(2, "James", 23).execute()

        locking = threading.Event()
        waiting = threading.Event()

        def thread_a(locking, waiting):
            session1 = mysqlx.get_session(config)
            schema1 = session1.get_schema(schema_name)
            table = schema1.get_table("t7")

            session1.start_transaction()
            result = table.select().where("name == 'James'").execute()
            assert result.fetch_all()[0]["age"] == 23
            table.update().where("name == 'James'").set("age", 30).execute()
            locking.set()
            time.sleep(2)
            locking.clear()
            if waiting.is_set():
                session1.commit()
                self.fail(
                    "Table_Update_S_SKIP_LOCKED_test IS NOT OK. Other thread "
                    "is not waiting while it is expected to!"
                )
            session1.commit()

        def thread_b(locking, waiting):
            session2 = mysqlx.get_session(config)
            schema2 = session2.get_schema(schema_name)
            table = schema2.get_table("t7")

            if not locking.wait(2):
                self.fail(
                    "Table_Update_S_SKIP_LOCKED_test IS NOT OK. Other thread "
                    "has not set the lock!"
                )
            session2.start_transaction()
            waiting.set()
            result = (
                table.select()
                .where("name = 'James'")
                .lock_exclusive(mysqlx.LockContention.NOWAIT)
                .lock_shared(mysqlx.LockContention.SKIP_LOCKED)
                .execute()
            )
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
        self.session.sql("drop table if exists t7").execute()

    @tests.foreach_session()
    def test_overlaps_table_select1(self):
        """OVERLAPS operator with string on both sides - With LHS in RHS."""
        self.session.sql("drop table if exists t1").execute()
        self.session.sql("create table t1(a int primary key, b int, c JSON)").execute()
        table = self.schema.get_table("t1")
        table.insert().values(1, 1, '{"comp":"abc"}').values(
            2, 1, '{"comp":"pqr"}'
        ).values(3, 2, '{"comp":"xyz"}').execute()
        result = table.select("a").where("'pqr' OVERLAPS c->'$.comp'").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["a"], 2)
        self.session.sql("drop table if exists t1").execute()

    @tests.foreach_session()
    def test_overlaps_table_select2(self):
        """OVERLAPS operator with int as operand - With LHS in RHS."""
        self.session.sql("drop table if exists t2").execute()
        self.session.sql(
            "create table t2(id int primary key, name varchar(20) , a JSON, c varchar(20))"
        ).execute()
        table = self.schema.get_table("t2")
        table.insert().values(1, "a", '{"age":21}', "abc").values(
            2, "b", '{"age":31}', "pqr"
        ).values(3, "hell", '{"age":22}', "xyz").execute()
        result = table.select().where("a->$.age OVERLAPS [21,31]").execute()
        self.assertEqual(len(result.fetch_all()), 2)
        self.session.sql("drop table if exists t2").execute()

    @tests.foreach_session()
    def test_overlaps_table_select3(self):
        """OVERLAPS operator with boolean as operand - With LHS in RHS."""
        self.session.sql("drop table if exists t3").execute()
        self.session.sql(
            "create table t3(id int primary key, n JSON, a json, c JSON)"
        ).execute()
        table = self.schema.get_table("t3")
        table.insert().values(
            1, '{"name":"joy"}', '{"age":18}', '{"comp":"abc"}'
        ).values(2, '{"name":"happy"}', '{"age":21}', '{"comp":"pqr"}').values(
            3, '{"name":"sad"}', '{"age":32}', '{"comp":"xyz"}'
        ).execute()
        result = table.select().where("21 OVERLAPS a->'$.age'").execute()
        self.assertEqual(len(result.fetch_all()), 1)
        self.session.sql("drop table if exists t3").execute()

    @tests.foreach_session()
    def test_overlaps_table_select4(self):
        """NOT OVERLAPS operator with string operand - With LHS not in RHS."""
        self.session.sql(
            "create table t4(id int primary key, n JSON, age int, c JSON)"
        ).execute()
        table = self.schema.get_table("t4")
        table.insert().values(1, '{"name":"joy"}', 18, '{"comp":"abc"}').values(
            2, '{"name":"happy"}', 21, '{"comp":"pqr"}'
        ).values(3, '{"name":"sad"}', 32, '{"comp":"xyz"}').execute()
        result = table.select().where("'happy' NOT OVERLAPS n->'$.name'").execute()
        self.assertEqual(len(result.fetch_all()), 2)
        self.session.sql("drop table if exists t4").execute()

    @tests.foreach_session()
    def test_overlaps_table_select5(self):
        """NOT OVERLAPS operator with int as operand - With LHS not in RHS."""
        self.session.sql(
            "create table t5(id int primary key, n JSON, a JSON, c JSON)"
        ).execute()
        table = self.schema.get_table("t5")
        table.insert().values(
            1, '{"name":"joy"}', '{"age":18}', '{"comp":"abc"}'
        ).values(2, '{"name":"happy"}', '{"age":21}', '{"comp":"pqr"}').values(
            3, '{"name":"sad"}', '{"age":32}', '{"comp":"xyz"}'
        ).execute()
        result = table.select().where("[21,32] NOT OVERLAPS a->'$.age'").execute()
        self.assertEqual(len(result.fetch_all()), 1)
        self.session.sql("drop table if exists t5").execute()

    @tests.foreach_session()
    def test_overlaps_table_select6(self):
        """NOT OVERLAPS operator with boolean as operand - With LHS not
        in RHS."""
        self.session.sql(
            "create table t6(id int primary key, name varchar(20), age int, c json)"
        ).execute()
        table = self.schema.get_table("t6")
        table.insert().values(1, "a", 21, '{"comp":"pqr"}').values(
            2, "b", 31, '{"comp":"xyz"}'
        ).values(3, "e", 22, '{"comp":"xyz"}').execute()
        result = table.select().where("c->'$.comp' OVERLAPS ['pqr','abc']").execute()
        self.assertEqual(len(result.fetch_all()), 1)
        self.session.sql("drop table if exists t6").execute()

    @tests.foreach_session()
    def test_overlaps_table_select7(self):
        """OVERLAPS operator with different datatypes as operands."""
        self.session.sql(
            "create table t7(id int primary key, n JSON, a json, c JSON)"
        ).execute()
        table = self.schema.get_table("t7")
        table.insert().values(
            1, '{"name":"joy"}', '{"age":18}', '{"comp":"abc"}'
        ).values(2, '{"name":"happy"}', '{"age":21}', '{"comp":"pqr"}').values(
            3, '{"name":"sad"}', '{"age":32}', '{"comp":"xyz"}'
        ).execute()
        result = table.select().where("21 OVERLAPS n->'$.name'").execute()
        result1 = table.select().where("'b' OVERLAPS a->$.age").limit(1).execute()
        self.assertEqual(len(result.fetch_all()), 0)
        self.assertEqual(len(result1.fetch_all()), 0)
        self.session.sql("drop table if exists t7").execute()

    @tests.foreach_session()
    def test_overlaps_table_select8(self):
        """OVERLAPS operator with single element on LHS and array/list on RHS
        and vice versa."""
        self.session.sql(
            "create table t8(id int primary key, n JSON, a JSON, p JSON)"
        ).execute()
        table = self.schema.get_table("t8")
        table.insert().values(
            1, '{"name":"a"}', '{"age":21}', '{"prof":["x","y"]}'
        ).values(2, '{"name":"b"}', '{"age":24}', '{"prof":["p","q"]}').values(
            3, '{"name":"c"}', '{"age":26}', '{"prof":["l","m"]}'
        ).execute()
        result = table.select().where("a->$.age OVERLAPS [21,23,24,28]").execute()
        result1 = (
            table.select().where("n->'$.name' OVERLAPS ['a','b','c','d','e']").execute()
        )
        result2 = table.select().where("a->$.age OVERLAPS [(10+11)]").execute()
        result3 = table.select().where("21 OVERLAPS [22,23]").limit(1).execute()
        result4 = table.select().where("['p','q'] OVERLAPS p->'$.prof'").execute()
        self.assertEqual(len(result.fetch_all()), 2)
        self.assertEqual(len(result1.fetch_all()), 3)
        self.assertEqual(len(result2.fetch_all()), 1)
        self.assertEqual(len(result3.fetch_all()), 0)
        self.assertEqual(len(result4.fetch_all()), 1)
        self.session.sql("drop table if exists t8").execute()

    @tests.foreach_session()
    def test_overlaps_table_select9(self):
        """OVERLAPS operator with single element on LHS and dict on RHS
        and vice versa."""
        self.session.sql("drop table if exists t9").execute()
        self.session.sql(
            "create table t9(id int primary key, name varchar(20), a JSON, ai JSON)"
        ).execute()
        table = self.schema.get_table("t9")
        table.insert().values(
            1,
            "a",
            '{"age":23}',
            '{"additionalinfo":["reading","music","playing"]}',
        ).values(
            2, "b", '{"age":21}', '{"additionalinfo":["boxing","music"]}'
        ).execute()
        result = (
            table.select().where("'reading' OVERLAPS ai->$.additionalinfo").execute()
        )
        result1 = table.select().where("'music' OVERLAPS a->$.age").execute()
        result2 = (
            table.select().where("'music' OVERLAPS ai->$.additionalinfo").execute()
        )
        self.assertEqual(len(result.fetch_all()), 1)
        self.assertEqual(len(result1.fetch_all()), 0)
        self.assertEqual(len(result2.fetch_all()), 2)
        self.session.sql("drop table if exists t9").execute()

    @tests.foreach_session()
    def test_overlaps_table_select10(self):
        """OVERLAPS operator with array/list operand on LHS and array/list
        on RHS."""
        self.session.sql("drop table if exists t10").execute()
        self.session.sql(
            "create table t10(id int primary key, i JSON, n JSON, a JSON, ai JSON)"
        ).execute()
        table = self.schema.get_table("t10")
        table.insert().values(
            1,
            '{"id":1}',
            '{"name":"joy"}',
            '{"age":21}',
            '{"additionalinfo":["reading","music","playing"]}',
        ).values(
            2,
            '{"id":2}',
            '{"name":"happy"}',
            '{"age":24}',
            '{"additionalinfo":["playing","painting","boxing"]}',
        ).execute()
        result = (
            table.select()
            .where("['playing','painting','boxing'] OVERLAPS ai->'$.additionalinfo'")
            .execute()
        )
        result1 = (
            table.select()
            .where('["happy","joy"] OVERLAPS n->$.name')
            .limit(1)
            .execute()
        )
        result2 = (
            table.select()
            .where('["reading"] NOT OVERLAPS ai->$.additionalinfo')
            .limit(1)
            .execute()
        )
        self.assertEqual(len(result.fetch_all()), 2)
        self.assertEqual(len(result1.fetch_all()), 1)
        self.assertEqual(len(result2.fetch_all()), 1)
        self.session.sql("drop table if exists t10").execute()

    @tests.foreach_session()
    def test_overlaps_table_select11(self):
        """OVERLAPS operator with dict on LHS and dict on RHS."""
        self.session.sql("drop table if exists t11").execute()
        self.session.sql(
            "create table t11(id int primary key, i JSON, n JSON, a JSON, ai JSON)"
        ).execute()
        table = self.schema.get_table("t11")
        table.insert().values(
            1,
            '{"id":1}',
            '{"name":"joy"}',
            '{"age":21}',
            '{"additionalinfo":[{"company":"xyz","vehicle":"bike"},{"company":"abc","vehicle":"car"},{"company":"mno","vehicle":"zeep"}]}',
        ).values(
            2,
            '{"id":2}',
            '{"name":"happy"}',
            '{"age":24}',
            '{"additionalinfo":[{"company":"abc","vehicle":"car"},{"company":"pqr","vehicle":"bicycle"}]}',
        ).execute()
        result = table.select(
            '{"company":"abc","vehicle":"car"} OVERLAPS ai->"$.additionalinfo"'
        ).execute()
        result1 = (
            table.select()
            .where('{"vehicle":"car"} NOT OVERLAPS ai->"$.additionalinfo"')
            .execute()
        )
        result2 = (
            table.select()
            .where('{"company":"mno"} OVERLAPS ai->"$.additionalinfo"')
            .execute()
        )
        self.assertEqual(len(result.fetch_all()), 2)
        self.assertEqual(len(result1.fetch_all()), 2)
        self.assertEqual(len(result2.fetch_all()), 0)
        self.session.sql("drop table if exists t11").execute()

    @tests.foreach_session()
    def test_overlaps_table_select12(self):
        """OVERLAPS operator with operands having expressions."""
        self.session.sql("drop table if exists t12").execute()
        self.session.sql(
            "create table t12(id int primary key, name varchar(20), age int)"
        ).execute()
        table = self.schema.get_table("t12")
        table.insert().values(1, "a", 21).values(2, "b", 22).values(
            3, "c", 32
        ).execute()
        result = (
            table.select().where("[(1>5)] OVERLAPS [true, false]").limit(1).execute()
        )
        result1 = (
            table.select()
            .where("[('a'>'b')] overlaps [true, false]")
            .limit(1)
            .execute()
        )
        result2 = (
            table.select()
            .where("true OVERLAPS [(1>5), !(false), (true || false), (false && true)]")
            .limit(1)
            .execute()
        )
        result3 = (
            table.select()
            .where("cast((2+3) as JSON) OVERLAPS [1,2,3,4,5]")
            .limit(1)
            .execute()
        )
        self.assertEqual(len(result.fetch_all()), 1)
        self.assertEqual(len(result1.fetch_all()), 1)
        self.assertEqual(len(result2.fetch_all()), 1)
        self.assertEqual(len(result3.fetch_all()), 1)
        self.session.sql("drop table if exists t12").execute()

    @tests.foreach_session()
    def test_overlaps_table_select13(self):
        """OVERLAPS operator with operands having expressions."""
        self.session.sql("drop table if exists t13").execute()
        self.session.sql(
            "create table t13(id int primary key, i json, n json, a json)"
        ).execute()
        table = self.schema.get_table("t13")
        table.insert().values(1, '{"id":1}', '{"name":"a"}', '{"age":21}').values(
            2, '{"id":2}', '{"name":"b"}', '{"age":22}'
        ).execute()
        self.assertRaises(
            mysqlx.OperationalError,
            table.select().where("(1+5) OVERLAPS [1,2,3,4,5,6]").execute,
        )
        # self.assertEqual(len(result.fetch_all()), 2)
        self.session.sql("drop table if exists t13").execute()

    @tests.foreach_session()
    def test_overlaps_table_select14(self):
        """OVERLAPS operator: search for empty string in a field and field
        in empty string."""
        self.session.sql("drop table if exists t14").execute()
        self.session.sql(
            "create table t14(id int primary key, n JSON, age int)"
        ).execute()
        table = self.schema.get_table("t14")
        table.insert().values(1, '{"name":"a"}', 21).values(
            2, '{"name":"b"}', 22
        ).values(3, '{"name":"c"}', 32).execute()
        result = table.select().where("'' OVERLAPS n->'$.name'").execute()
        result1 = table.select().where("n->'$.name' OVERLAPS ['', ' ']").execute()
        self.assertEqual(len(result.fetch_all()), 0)
        self.assertEqual(len(result1.fetch_all()), 0)
        self.session.sql("drop table if exists t14").execute()
