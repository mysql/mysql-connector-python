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
class ViewTests(tests.MySQLxTests):
    """Tests for View."""

    @tests.foreach_session()
    def test_is_view(self):
        """Test is_view()."""
        self.session.sql("create table t1(a int primary key)").execute()
        self.session.sql("insert into t1 values(1)").execute()
        table = self.schema.get_table("t1")
        self.assertFalse(table.is_view())
        self.session.sql("create view v1 as select * from t1").execute()
        view = self.schema.get_table("v1")
        self.assertTrue(view.is_view())
        self.session.sql("drop view v1").execute()
        self.session.sql("drop table t1").execute()

    @tests.foreach_session()
    def test_view_select1(self):
        self.session.sql(
            "create table t2(id int primary key, name varchar(32))"
        ).execute()
        table = self.schema.get_table("t2")
        table.insert("id", "name").values(1, "amr").values(2, "abc").execute()
        self.session.sql("create view v2 as select * from t2").execute()
        view = self.schema.get_table("v2")
        result = view.select().where("id ==1").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["name"], "amr")
        self.session.sql("drop view v2").execute()
        self.session.sql("drop table t2").execute()

    @tests.foreach_session()
    def test_view_insert1(self):
        self.session.sql(
            "create table t3(_id int primary key, name varchar(32))"
        ).execute()
        table = self.schema.get_table("t3")
        table.insert("_id", "name").values(1, "amr").execute()
        self.session.sql("create view v3 as select * from t3").execute()
        view = self.schema.get_table("v3")
        view.insert("_id", "name").values(2, "abc").execute()
        result1 = view.select().execute()
        row1 = result1.fetch_all()
        self.assertEqual(len(row1), 2)
        self.assertEqual(row1[0]["name"], "amr")
        result2 = table.select().execute()
        row2 = result2.fetch_all()
        self.assertEqual(len(row2), 2)
        self.assertEqual(row2[1]["name"], "abc")
        self.session.sql("drop view v3").execute()
        self.session.sql("drop table t3").execute()

    @tests.foreach_session()
    def test_view_update1(self):
        self.session.sql(
            "create table t4(_id int primary key, name varchar(32))"
        ).execute()
        table = self.schema.get_table("t4")
        table.insert("_id", "name").values(1, "amr").execute()
        self.session.sql("create view v4 as select * from t4").execute()
        view = self.schema.get_table("v4")
        view.update().set("name", "abc").where("_id == 1").execute()
        result1 = view.select().execute()
        row1 = result1.fetch_all()
        self.assertEqual(row1[0]["name"], "abc")
        result2 = table.select().execute()
        row2 = result2.fetch_all()
        self.assertEqual(row2[0]["name"], "abc")
        self.session.sql("drop view v4").execute()
        self.session.sql("drop table t4").execute()

    @tests.foreach_session()
    def test_view_delete1(self):
        self.session.sql(
            "create table t5(_id int primary key, name varchar(32))"
        ).execute()
        table = self.schema.get_table("t5")
        table.insert("_id", "name").values(1, "amr").execute()
        self.session.sql("create view v5 as select * from t5").execute()
        view = self.schema.get_table("v5")
        self.assertEqual(table.count(), 1)
        try:
            view.delete().execute()
        except mysqlx.ProgrammingError:
            # Expected ProgrammingErorr
            pass
        self.assertEqual(table.count(), 1)
        self.session.sql("drop view v5").execute()
        self.session.sql("drop table t5").execute()

    @tests.foreach_session()
    def test_view_insert2(self):
        self.session.sql("create table t6(a int, b int primary key)").execute()
        self.session.sql("create table t7(c int, d int primary key)").execute()
        t1 = self.schema.get_table("t6")
        t2 = self.schema.get_table("t7")
        t1.insert("a", "b").values(1, 2).execute()
        t2.insert("c", "d").values(1, 4).execute()
        self.session.sql(
            "create view v6 as select * from t6 join t7 on t6.a = t7.c"
        ).execute()
        view = self.schema.get_table("v6")
        try:
            view.insert().values(2, 3).execute()
        except mysqlx.OperationalError:
            # Expected OperationalError
            pass
        self.session.sql("drop view v6").execute()
        self.session.sql("drop table t6").execute()
        self.session.sql("drop table t7").execute()

    @tests.foreach_session()
    def test_view_update2(self):
        self.session.sql("create table t8(a int, b int primary key)").execute()
        self.session.sql("create table t9(c int, d int primary key)").execute()
        t1 = self.schema.get_table("t8")
        t2 = self.schema.get_table("t9")
        t1.insert("a", "b").values(1, 2).execute()
        t2.insert("c", "d").values(1, 4).execute()
        self.session.sql(
            "create view v7 as select t9.c as c_t9 from t8 join t9 on t8.a = t9.c"
        ).execute()
        view = self.schema.get_table("v7")
        try:
            view.update().set("c_t9", 100).execute()
        except mysqlx.ProgrammingError:
            # Expected ProgrammingError
            pass
        self.session.sql("drop view v7").execute()
        self.session.sql("drop table t8").execute()
        self.session.sql("drop table t9").execute()

    @tests.foreach_session()
    def test_view_update3(self):
        self.session.sql("create table t12(a int, b int primary key)").execute()
        self.session.sql("create table t13(c int, d int primary key)").execute()
        t1 = self.schema.get_table("t12")
        t2 = self.schema.get_table("t13")
        t1.insert("a", "b").values(1, 2).execute()
        t2.insert("c", "d").values(1, 4).execute()
        self.session.sql(
            "create view v9 as select t13.c as c_t13 from t12 join t13 on t12.a = t13.c"
        ).execute()
        view = self.schema.get_table("v9")
        try:
            view.update().set("c_t13", 100).limit(1).execute()
        except mysqlx.ProgrammingError:
            # Expected ProgrammingError
            pass
        self.session.sql("drop view v9").execute()
        self.session.sql("drop table t12").execute()
        self.session.sql("drop table t13").execute()

    @tests.foreach_session()
    def test_view_delete2(self):
        self.session.sql("create table t10(a int, b int primary key)").execute()
        self.session.sql("create table t11(c int, d int primary key)").execute()
        t1 = self.schema.get_table("t10")
        t2 = self.schema.get_table("t11")
        t1.insert("a", "b").values(1, 2).execute()
        t2.insert("c", "d").values(1, 4).execute()
        self.session.sql(
            "create view v8 as select t11.c as c_t11 from t10 join t11 on t10.a = t11.c"
        ).execute()
        view = self.schema.get_table("v8")
        try:
            view.delete().execute()
        except mysqlx.ProgrammingError:
            # Expected ProgrammingError
            pass
        self.session.sql("drop view v8").execute()
        self.session.sql("drop table t10").execute()
        self.session.sql("drop table t11").execute()

    @tests.foreach_session()
    def test_collection_view(self):
        collection = self.schema.create_collection("mycoll1")
        collection.add({"_id": 1, "name": "a"}, {"_id": 2, "name": "b"}).execute()
        self.session.sql("create view v10 as select * from mycoll1").execute()
        view = self.schema.get_table("v10")
        result = view.select().where("_id ==1").execute()
        result.fetch_all()
        tables = self.schema.get_tables()
        tables[0].get_name()
        self.session.sql("drop view v10").execute()
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_view_drop1(self):
        """Test a valid view drop."""
        self.session.sql(
            "create table t14(_id int primary key, name varchar(32))"
        ).execute()
        table = self.schema.get_table("t14")
        table.insert("_id", "name").values(1, "amr").values(2, "dev").values(
            3, "efg"
        ).execute()
        self.session.sql("create view v14 as select * from t14").execute()
        view = self.schema.get_table("v14")
        self.assertEqual(table.count(), 3)
        self.session.sql("drop view v14").execute()
        self.assertFalse(view.exists_in_database())
        self.session.sql("drop table t14").execute()

    @tests.foreach_session()
    def test_view_drop2(self):
        """Test invalid view drop."""
        self.session.sql(
            "create table t15(_id int primary key, name varchar(32))"
        ).execute()
        table = self.schema.get_table("t15")
        table.insert("_id", "name").values(1, "amr").values(2, "dev").values(
            3, "efg"
        ).execute()
        view = self.schema.get_table("v15")
        self.assertEqual(table.count(), 3)
        self.assertFalse(view.exists_in_database())
        self.session.sql("drop table t15").execute()
