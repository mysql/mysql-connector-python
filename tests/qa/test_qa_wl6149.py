# Copyright (c) 2013, 2022, Oracle and/or its affiliates.
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

import argparse
import unittest

from datetime import time

import mysql.connector
import tests

from mysql.connector import errors
from mysql.connector.constants import ClientFlag


@unittest.skipIf(
    tests.MYSQL_VERSION < (5, 6, 4), "Not supported for MySQL <5.6.4 versions"
)
class WL6149Tests(tests.MySQLConnectorTests):
    """Test for WL6149 Fractional Seconds Support."""

    @tests.foreach_cnx()
    def test_timestamp(self):
        """Test for inserting TIMESTAMP value into a table and retrieving
        the same.
        """
        with self.cnx.cursor(raw=True) as cur:
            cur.execute("DROP TABLE IF EXISTS ts")
            cur.execute("CREATE TABLE ts (id int primary key, t1 timestamp(3))")
            cur.execute("insert into ts values (1, '2012-06-05 10:20:49.110')")
            self.cnx.commit()
            cur.execute("SELECT t1 from ts")
            exp = b"2012-06-05 10:20:49.110"
            out = cur.fetchone()[0]
            self.assertEqual(exp, out)

            cur.execute("DROP TABLE IF EXISTS ts")
            self.cnx.commit()

    @tests.foreach_cnx()
    def test_misc_time(self):
        with self.cnx.cursor(raw=True) as cur:
            self.assertRaises(
                mysql.connector.errors.ProgrammingError,
                cur.execute,
                "SELECT NOW(7)",
            )

    @tests.foreach_cnx()
    def test_time(self):
        """Test for inserting TIME value into a table and retrieving
        the same.
        """
        with self.cnx.cursor(raw=True) as cur:
            cur.execute("DROP TABLE IF EXISTS t_time")
            cur.execute("CREATE TABLE t_time (idx int primary key, t1 time(3))")
            cur.execute("insert into t_time values (1, '10:20:49.11')")
            self.cnx.commit()
            cur.execute("SELECT t1 from t_time")
            exp = b"10:20:49.110"
            out = cur.fetchone()[0]
            self.assertEqual(exp, out)

    @tests.foreach_cnx()
    def test_datetime(self):
        """Test for inserting DATETIME value into a table and retrieving
        the same.
        """
        with self.cnx.cursor(raw=True) as cur:
            cur.execute("DROP TABLE IF EXISTS t_datetime")
            cur.execute("CREATE TABLE t_datetime (id int primary key, t1 datetime(3))")
            cur.execute("insert into t_datetime values (1, '2038-01-19 03:14:07.11')")
            self.cnx.commit()
            cur.execute("SELECT t1 from t_datetime")
            exp = b"2038-01-19 03:14:07.110"
            out = cur.fetchone()[0]
            self.assertEqual(exp, out)

            cur.execute("DROP TABLE IF EXISTS t_datetime")
            self.cnx.commit()

    @tests.foreach_cnx()
    def test_alt_datetime(self):
        """Test to create a table with TIME(4), insert values and then alter
        the column.

        Verify if there is any change in precison of stored data.
        """
        with self.cnx.cursor(raw=True) as cur:
            cur.execute("DROP TABLE IF EXISTS t_alt_datetime")
            cur.execute(
                "CREATE TABLE t_alt_datetime (id int primary key, t1 datetime(3))"
            )
            cur.execute(
                "insert into t_alt_datetime values (1, '2038-01-19 03:14:07.11')"
            )
            self.cnx.commit()
            cur.execute("SELECT t1 from t_alt_datetime")
            exp = b"2038-01-19 03:14:07.110"
            out = cur.fetchone()[0]
            self.assertEqual(exp, out)

            cur.execute("ALTER TABLE t_alt_datetime MODIFY t1 datetime(5)")
            cur.execute("SELECT t1 from t_alt_datetime")
            exp = b"2038-01-19 03:14:07.11000"
            out = cur.fetchone()[0]
            self.assertEqual(exp, out)

            cur.execute("DROP TABLE IF EXISTS t_alt_datetime")
            self.cnx.commit()

    @tests.foreach_cnx()
    def test_alt_timestamp(self):
        with self.cnx.cursor(raw=True) as cur:
            cur.execute("DROP TABLE IF EXISTS t_alt_timestamp")
            cur.execute(
                "CREATE TABLE t_alt_timestamp (id int primary key, t1 timestamp(4))"
            )
            cur.execute(
                "insert into t_alt_timestamp values (1, '2011-01-19 03:14:07.11')"
            )
            self.cnx.commit()
            cur.execute("SELECT t1 from t_alt_timestamp")
            exp = b"2011-01-19 03:14:07.1100"
            out = cur.fetchone()[0]
            self.assertEqual(exp, out)

            cur.execute("ALTER TABLE t_alt_timestamp MODIFY t1 timestamp(5)")
            cur.execute("SELECT t1 from t_alt_timestamp")
            exp = b"2011-01-19 03:14:07.11000"
            out = cur.fetchone()[0]
            self.assertEqual(exp, out)

            cur.execute("DROP TABLE IF EXISTS t_alt_timestamp")
            self.cnx.commit()
