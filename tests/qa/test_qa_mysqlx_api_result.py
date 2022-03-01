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

"""MySQL X API results tests."""

import unittest

import tests


@unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 25), "XPlugin not compatible")
class APIResultTests(tests.MySQLxTests):
    """API results tests."""

    @tests.foreach_session()
    def test_get_affected_items_count(self):
        self.session.sql("drop table if exists t1").execute()
        self.session.sql("create table t1(a int, b int)").execute()
        table = self.schema.get_table("t1")
        result = table.insert().values(1, 1).execute()
        self.assertEqual(result.get_affected_items_count(), 1)
        self.session.sql("drop table if exists t1").execute()

    @tests.foreach_session()
    def test_get_warnings(self):
        self.session.sql("use {}".format(self.config["schema"])).execute()
        result = self.session.sql("select 1/0").execute()
        row = result.get_warnings()
        self.assertEqual(len(row), 0)

    @tests.foreach_session()
    def test_get_warnings_count(self):
        self.session.sql("use {}".format(self.config["schema"])).execute()
        result = self.session.sql("select 1/0").execute()
        self.assertEqual(result.get_warnings_count(), 0)

    @tests.foreach_session()
    def test_get_string(self):
        self.session.sql("drop table if exists t2").execute()
        self.session.sql("create table t2(a int , b int)").execute()
        table = self.schema.get_table("t2")
        table.insert("a", "b").values(1, 2).execute()
        result = table.select("a").where("b== 2").execute()
        row = result.fetch_all()[0]
        self.assertEqual(row.get_string("a"), "1")
        self.session.sql("drop table if exists t2").execute()

    @tests.foreach_session()
    def test_index_of(self):
        self.session.sql("drop table if exists t3").execute()
        self.session.sql("create table t3(a int , b int)").execute()
        table = self.schema.get_table("t3")
        table.insert("a", "b").values(1, 2).execute()
        result = table.select("a", "b").where("b== 2").execute()
        self.assertEqual(result.index_of("b"), 1)
        self.assertEqual(result.index_of("a"), 0)
        self.session.sql("drop table if exists t3").execute()

    @tests.foreach_session()
    def test_fetchone_test1(self):
        self.session.sql("drop table if exists t4").execute()
        self.session.sql("create table t4(a int , b int)").execute()
        table = self.schema.get_table("t4")
        table.insert("a", "b").values(1, 2).values(3, 2).execute()
        result = table.select("a").where("b== 2").execute()
        self.assertEqual(result.fetch_one()["a"], 1)
        self.assertEqual(result.fetch_one()["a"], 3)
        self.assertEqual(result.fetch_one(), None)
        self.session.sql("drop table if exists t4").execute()

    @tests.foreach_session()
    def test_fetchone_test2(self):
        self.session.sql("drop table if exists t5").execute()
        self.session.sql("create table t5(a int , b int)").execute()
        table = self.schema.get_table("t5")
        table.insert("a", "b").values(1, 2).execute()
        result = table.select("a").where("b== 3").execute()
        self.assertIsNone(result.fetch_one())
        self.session.sql("drop table if exists t5").execute()

    @tests.foreach_session()
    def test_fetchone_test3(self):
        """MCPY-370."""
        self.session.sql("drop table if exists t6").execute()
        self.session.sql("create table t6(a int , b int)").execute()
        table = self.schema.get_table("t6")
        table.insert("a", "b").values(1, 2).values(3, 2).values(4, 2).execute()
        result = table.select("a").where("b== 2").execute()
        self.assertEqual(result.fetch_one()["a"], 1)
        row2 = result.fetch_all()
        self.assertEqual(row2[0]["a"], 3)
        self.assertEqual(row2[1]["a"], 4)
        self.session.sql("drop table if exists t6").execute()

    @tests.foreach_session()
    def test_multiple_resultset(self):
        """testing the MCPY-357 issue(multiple resultset)."""
        self.session.sql("drop table if exists t7").execute()
        self.session.sql("drop procedure if exists sproc").execute()
        self.session.sql(
            "create table t7(_id int, name varchar(32))"
        ).execute()
        self.session.sql(
            "insert into t7(_id,name) "
            "values (1,'abc'),(2,'def'),(3,'ghi'),(4,'jkl')"
        ).execute()
        self.session.sql(
            "create procedure sproc() begin "
            "select name from t7; select _id from t7 ; end"
        ).execute()
        result = self.session.sql("call sproc").execute()
        row = result.fetch_all()
        self.assertEqual(row[0][0], "abc")
        self.assertTrue(result.next_result())
        row = result.fetch_all()
        self.assertEqual(row[0][0], 1)
        self.assertFalse(result.next_result())
        self.session.sql("drop table if exists t7").execute()
        self.session.sql("drop procedure if exists sproc").execute()
