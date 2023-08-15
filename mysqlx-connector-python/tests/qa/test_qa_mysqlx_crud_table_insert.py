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
class TableInsertTests(tests.MySQLxTests):
    """Tests for table.insert()."""

    @tests.foreach_session()
    def test_table1(self):
        """Test the table.getname."""
        self.session.sql("create table t1(a int primary key)").execute()
        table = self.schema.get_table("t1")
        self.assertEqual(table.get_name(), "t1")
        self.session.sql("drop table t1").execute()

    @tests.foreach_session()
    def test_table2(self):
        """Test the table.getschema."""
        self.session.sql("create table t2(a int primary key)").execute()
        table = self.schema.get_table("t2")
        self.assertEqual(table.get_schema(), self.schema)
        self.session.sql("drop table t2").execute()

    @tests.foreach_session()
    def test_table3(self):
        """Test the table.exists_in_database."""
        self.session.sql("create table t3(a int primary key)").execute()
        self.session.sql("show tables")
        table = self.schema.get_table("t3")
        self.assertEqual(table.exists_in_database(), True)
        self.session.sql("drop table t3").execute()

    @tests.foreach_session()
    def test_table4(self):
        self.session.sql("create table t11(a int primary key)").execute()
        self.session.sql("show tables")
        table = self.schema.get_table("t11")
        self.assertEqual(table.who_am_i(), "t11")
        self.session.sql("drop table t11").execute()

    @tests.foreach_session()
    def test_table5(self):
        self.session.sql("create table t12(a int primary key)").execute()
        self.session.sql("show tables")
        table = self.schema.get_table("t12")
        self.assertTrue(table.am_i_real())
        self.session.sql("drop table t12").execute()

    @tests.foreach_session()
    def test_table_insert1(self):
        """Test the table.insert_ with different_datatype."""
        self.session.sql(
            "create table t4(_id int primary key, name varchar(32))"
        ).execute()
        table = self.schema.get_table("t4")
        table.insert("_id", "name").values(1, "amr").execute()
        result = table.select().where("_id ==1").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["name"], "amr")
        self.session.sql("drop table t4").execute()

    @tests.foreach_session()
    def test_table_insert2(self):
        """Test the table.insert_ with different_datatype."""
        self.session.sql(
            "create table t5(_id int primary key, name blob, salary float, title varchar(32))"
        ).execute()
        table = self.schema.get_table("t5")
        large_str = "x" * 1024
        table.insert().values(1, large_str, 1200.64, "aaaaaaaaaaa").execute()
        result = table.select().where("_id ==1").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["title"], "aaaaaaaaaaa")
        self.session.sql("drop table t5").execute()

    @tests.foreach_session()
    def test_table_insert3(self):
        """Test the table.insert."""
        self.session.sql(
            "create table t6(_id int primary key, name varchar(32))"
        ).execute()
        table = self.schema.get_table("t6")
        try:
            table.insert().values().execute()
        except mysqlx.OperationalError:
            # Expected OperationalError
            pass
        self.session.sql("drop table t6").execute()

    @tests.foreach_session()
    def test_table_insert4(self):
        """Test the table.insert with json."""
        data = '{"_id":1,"a":1}'
        self.session.sql("create table t7(id int primary key, a JSON)").execute()
        table = self.schema.get_table("t7")
        table.insert().values(1, data).execute()
        result = table.select().execute()
        row = result.fetch_all()
        self.assertIsNotNone(row)
        self.session.sql("drop table t7").execute()

    @tests.foreach_session()
    def test_table_insert5(self):
        """Test table.insert with date."""
        self.session.sql("create table t8(_id int primary key, a date)").execute()
        table = self.schema.get_table("t8")
        table.insert().values(1, "2016-10-10").execute()
        result = table.select().where("_id ==1").execute()
        row = result.fetch_all()
        self.assertEqual(row[0]["a"], datetime.datetime(2016, 10, 10, 0, 0))
        self.session.sql("drop table t8").execute()

    @tests.foreach_session()
    def test_table_insert6(self):
        """Test table.insert with time."""
        self.session.sql("create table t9(id int primary key, b time)").execute()
        table = self.schema.get_table("t9")
        table.insert().values(1, "20:20:20").execute()
        result1 = table.select().execute()
        row = result1.fetch_all()
        self.assertEqual(row[0]["b"], datetime.timedelta(seconds=73220))
        self.session.sql("drop table t9").execute()

    @tests.foreach_session()
    def test_table_insert7(self):
        """Test the table.auto increment."""
        self.session.sql(
            "create table t10(a int key auto_increment , name varchar(32))"
        ).execute()
        result = self.session.sql("insert into t10 values(Null,'Fred')").execute()
        table = self.schema.get_table("t10")
        table.insert().values(None, "Boo").execute()
        self.session.sql("drop table t10").execute()
