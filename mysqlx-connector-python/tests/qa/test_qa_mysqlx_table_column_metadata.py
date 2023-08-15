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

import unittest

import mysqlx

import tests


@unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 25), "XPlugin not compatible")
class TableColumnMetadataTests(tests.MySQLxTests):
    """Tests for table column metadata."""

    @tests.foreach_session()
    def test_get_column_name(self):
        self.session.sql("create table t1(a int primary key, b int)").execute()
        table = self.schema.get_table("t1")
        table.insert("a", "b").values(1, 1).execute()
        result = table.select("a", "b").execute()
        self.assertEqual(result.columns[0].get_column_name(), "a")
        self.assertEqual(result.columns[1].get_column_name(), "b")
        self.session.sql("drop table t1").execute()

    @tests.foreach_session()
    def test_get_table_name(self):
        self.session.sql("create table t2(a int primary key, b int)").execute()
        table = self.schema.get_table("t2")
        table.insert("a", "b").values(1, 1).execute()
        result = table.select().execute()
        self.assertEqual(result.columns[0].get_table_name(), "t2")
        self.assertEqual(result.columns[1].get_table_name(), "t2")
        self.session.sql("drop table t2").execute()

    @tests.foreach_session()
    def test_get_schema_name(self):
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql("create table t3(a int primary key, b int)").execute()
        table = self.schema.get_table("t3")
        table.insert("a", "b").values(1, 1).execute()
        result = table.select("a", "b").execute()
        self.assertEqual(result.columns[0].get_schema_name(), schema_name)
        self.session.sql("drop table t3").execute()

    @tests.foreach_session()
    def test_get_column_label(self):
        self.session.sql("create table t4(a int primary key, b int)").execute()
        table = self.schema.get_table("t4")
        table.insert("a", "b").values(1, 1).execute()
        result = table.select("a as amr").execute()
        self.assertEqual(result.columns[0].get_column_name(), "a")
        self.assertEqual(result.columns[0].get_column_label(), "amr")
        self.session.sql("drop table t4").execute()

    @tests.foreach_session()
    def test_get_table_label(self):
        self.session.sql("create table t5(a int primary key, b int)").execute()
        table = self.schema.get_table("t5")
        table.insert("a", "b").values(1, 1).execute()
        result = table.select("a", "b").execute()
        self.assertEqual(result.columns[0].get_table_name(), "t5")
        self.assertEqual(result.columns[0].get_table_label(), "t5")
        self.session.sql("drop table t5").execute()

    @tests.foreach_session()
    def test_get_type(self):
        self.session.sql(
            "create table t6 ("
            "id int primary key,"
            "_id varchar(32), "
            "a char(20), "
            "b date, "
            "c int, "
            "d double, "
            "e datetime, "
            "f time, "
            "g linestring, "
            "h tinyint, "
            "i mediumint, "
            "j bigint, "
            "k float, "
            "l set('1','2') "
            ", m enum('1','2'),"
            "n decimal(20,10))"
        ).execute()
        table = self.schema.get_table("t6")
        result = table.select().execute()
        self.assertEqual(result.columns[4].get_type(), 5)
        self.session.sql("drop table t6").execute()

    @tests.foreach_session()
    def test_get_bit_data(self):
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self.session.sql(
            "create table t7(id int auto_increment primary key, a bit)"
        ).execute()
        table = self.schema.get_table("t7")
        table.insert("a").values(0).execute()
        result = table.select().execute()
        self.assertEqual(result.columns[0].get_schema_name(), schema_name)
        self.session.sql("drop table t7").execute()

    @unittest.skip("TODO: Fix me")
    @tests.foreach_session()
    def test_get_length(self):
        self.session.sql(
            "create table t8 ("
            "_id varchar(32), "
            "a char(20), "
            "b date, "
            "c int, "
            "d double, "
            "e datetime, "
            "f time, "
            "g linestring, "
            "h tinyint, "
            "i mediumint, "
            "j bigint, "
            "k float, "
            "l set('1','2'), "
            "m enum('1','2'), "
            "n decimal(20,10))"
        ).execute()
        table = self.schema.get_table("t8")
        result = table.select().execute()
        self.assertEqual(result.columns[0].get_length(), 32)
        self.assertEqual(result.columns[1].get_length(), 20)
        self.session.sql("drop table t8").execute()

    @tests.foreach_session()
    def test_get_fractional_digits(self):
        self.session.sql(
            "create table t9(id INT PRIMARY KEY, m FLOAT(7,4), s DECIMAL(5,2))"
        ).execute()
        table = self.schema.get_table("t9")
        table.insert("id", "m", "s").values(1, 0, 0).execute()
        result = table.select().execute()
        self.assertEqual(result.columns[1].get_fractional_digits(), 4)
        self.assertEqual(result.columns[2].get_fractional_digits(), 2)
        self.session.sql("drop table t9").execute()

    @tests.foreach_session()
    def test_is_number_signed(self):
        self.session.sql(
            "create table t10 ("
            "_id varchar(32), "
            "a char(20), "
            "b date, "
            "c int, "
            "d double, "
            "e datetime, "
            "f time, "
            "g linestring, "
            "h tinyint unsigned, "
            "i mediumint, "
            "j bigint signed, "
            "k float, "
            "l set('1','2'), "
            "m enum('1','2'), "
            "n decimal(20,10))"
        ).execute()
        table = self.schema.get_table("t10")
        result = table.select().execute()
        self.assertFalse(result.columns[0].is_number_signed())
        self.assertFalse(result.columns[8].is_number_signed())
        self.assertTrue(result.columns[10].is_number_signed())
        self.session.sql("drop table t10").execute()

    @tests.foreach_session()
    def test_is_padded(self):
        self.session.sql("create table t11(a int primary key, b char(10))").execute()
        table = self.schema.get_table("t11")
        table.insert("a", "b").values(1, "a").execute()
        result = table.select("a", "b").execute()
        self.assertFalse(result.columns[0].is_padded())
        self.assertTrue(result.columns[1].is_padded())
        self.session.sql("drop table t11").execute()

    @tests.foreach_session()
    def test_get_collation_and_charset(self):
        self.session.sql(
            "create table t12(_id varchar(32) CHARACTER SET utf8 COLLATE utf8_bin)"
        ).execute()
        table = self.schema.get_table("t12")
        result = table.select("_id").execute()
        self.assertTrue(
            result.columns[0].get_character_set_name() in ("utf8", "utf8mb4")
        )
        self.assertTrue(
            result.columns[0].get_collation_name() in ("utf8_bin", "utf8mb4_0900_ai_ci")
        )
        self.session.sql("drop table t12").execute()
