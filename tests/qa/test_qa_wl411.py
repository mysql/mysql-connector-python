# Copyright (c) 2014, 2021, Oracle and/or its affiliates.
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

import mysql.connector
import tests


class WL411Tests(tests.MySQLConnectorTests):
    """Testing the reset_session api."""

    @tests.foreach_cnx()
    def test_create_stored(self):
        """Setting the user defined param while resetting session."""
        with self.cnx.cursor() as cur:
            cur.execute(
                "create table t1 (a int primary key, b int generated always as (a + 2) stored)"
            )
            cur.execute("insert into t1(a) values(1)")
            cur.execute(" SELECT b from t1")
            res = cur.fetchone()[0]
            self.assertEqual(3, res)
            cur.execute("drop table if exists t1")

    @tests.foreach_cnx()
    def test_create_virtual(self):
        """Resetting the session without any setting any parameter."""
        with self.cnx.cursor() as cur:
            cur.execute(
                "create table t2 (a int primary key, b int generated always as (a + 2))"
            )
            cur.execute("insert into t2(a) values(1)")
            cur.execute("SELECT b from t2")
            res = cur.fetchone()[0]
            self.assertEqual(3, res)
            cur.execute("drop table if exists t2")

    @tests.foreach_cnx()
    def test_create_stored_index(self):
        with self.cnx.cursor() as cur:
            cur.execute(
                "create table t3 "
                "(a int primary key, b int generated always as (a+2) stored unique key)"
            )
            cur.execute("insert into t3(a) values(1)")
            self.assertRaises(
                mysql.connector.errors.IntegrityError,
                cur.execute,
                "insert into t3(a) values(1)",
            )
            cur.execute("drop table if exists t3")

    @tests.foreach_cnx()
    def test_create_virtual_index(self):
        """Setting the system variable while resetting the session."""
        with self.cnx.cursor() as cur:
            cur.execute(
                "create table t4 "
                "(a int primary key, b int generated always as (a + 2) unique key, c int)"
            )
            cur.execute("drop table if exists t4")

    @tests.foreach_cnx()
    def test_alter_stored(self):
        with self.cnx.cursor() as cur:
            cur.execute(
                "create table t5 (a int primary key, b int generated always as (a + 2) stored)"
            )
            cur.execute(
                "alter table t5 modify b int generated always as (a + 3) stored"
            )
            cur.execute("insert into t5(a) values(1)")
            cur.execute("SELECT b from t5")
            res = cur.fetchone()[0]
            self.assertEqual(4, res)
            cur.execute("drop table if exists t5")

    @tests.foreach_cnx()
    def test_alter_virtual(self):
        with self.cnx.cursor() as cur:
            cur.execute(
                "create table t6 (a int primary key, b int generated always as (a + 2))"
            )
            cur.execute("alter table t6 modify b int generated always as (a + 3)")
            cur.execute("insert into t6(a) values(1)")
            cur.execute("SELECT b from t6")
            res = cur.fetchone()[0]
            self.assertEqual(4, res)
            cur.execute("drop table if exists t6")

    @tests.foreach_cnx()
    def test_illegal_gc(self):
        with self.cnx.cursor() as cur:
            self.assertRaises(
                mysql.connector.errors.ProgrammingError,
                cur.execute,
                "create table t7 (a int primary key, b int generated always as (c op 3) stored)",
            )
            cur.execute("drop table if exists t7")

    @tests.foreach_cnx()
    def test_nondefault_gc(self):
        with self.cnx.cursor() as cur:
            cur.execute(
                "create table t8 (a int primary key, b int generated always as (a + 2) stored)"
            )
            self.assertRaises(
                mysql.connector.errors.DatabaseError,
                cur.execute,
                "insert into t8  values(1,2)",
            )
            cur.execute("drop table if exists t8")

    @tests.foreach_cnx()
    def test_illegal_ref_stored(self):
        with self.cnx.cursor() as cur:
            self.assertRaises(
                mysql.connector.errors.DatabaseError,
                cur.execute,
                "create table t9 (a int primary key, b int generated always as (c), "
                "c int generated always as (b) stored)",
            )
            cur.execute("drop table if exists t9")

    @tests.foreach_cnx()
    def test_illegal_ref_virtual(self):
        with self.cnx.cursor() as cur:
            self.assertRaises(
                mysql.connector.errors.DatabaseError,
                cur.execute,
                "create table t10 "
                "(a int primary key, b int generated always as (b), c int "
                "generated always as (b))",
            )
            cur.execute("drop table if exists t10")

    @tests.foreach_cnx()
    def test_valid_ref(self):
        with self.cnx.cursor() as cur:
            cur.execute(
                "create table t11 (a int primary key, b int generated always as (a), "
                "c int generated always as (b) stored)"
            )
            cur.execute("insert into t11(a) values(1)")
            cur.execute("SELECT c from t11")
            res = cur.fetchone()[0]
            self.assertEqual(1, res)
            cur.execute("drop table if exists t11")
