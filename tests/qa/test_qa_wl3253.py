# Copyright (c) 2014, 2022, Oracle and/or its affiliates.
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

import mysql.connector
import tests


@unittest.skipIf(
    tests.MYSQL_VERSION < (5, 6, 3), "Not supported for MySQL <5.6.4 versions"
)
class WL3253Tests(tests.MySQLConnectorTests):
    """Testing the reset_session api."""

    def setUp(self):
        config = tests.get_mysql_config()
        with mysql.connector.connect(**config) as cnx:
            with cnx.cursor() as cur:
                # creating the table
                cur.execute("create table account (ac_no int , amount int)")
                # cretaing the first trigger
                cur.execute(
                    "create trigger ac_sum before insert "
                    "on account for each row set @sum=@sum + NEW.amount"
                )
                # creating the second trigger on the same table
                cur.execute(
                    "create trigger ac_trans before insert on account "
                    "for each row precedes ac_sum set @dep=@dep + "
                    "if(NEW.amount>0,NEW.amount,0),@wdr=@wdr + "
                    "if(NEW.amount<0,-NEW.amount,0)"
                )
                cnx.commit()

    def tearDown(self):
        config = tests.get_mysql_config()
        with mysql.connector.connect(**config) as cnx:
            with cnx.cursor() as cur:
                cur.execute("drop trigger if exists ac_sum")
                cur.execute("drop trigger if exists ac_trans")
                cur.execute("drop table if exists account")
                cnx.commit()

    @tests.foreach_cnx()
    def test_trigger(self):
        """Setting the user defined param while resetting session."""
        with self.cnx.cursor() as cur:
            cur.execute("SET @sum=0")
            cur.execute("SET @dep=0")
            cur.execute("SET @wdr=0")
            cur.execute("insert into account values(1,100)")
            cur.execute("insert into account values(2,100)")
            cur.execute("insert into account values(3,-100)")
            cur.execute("select @sum")
            res = cur.fetchone()[0]
            self.assertEqual(100, res)

            cur.execute("select @dep")
            res = cur.fetchone()[0]
            self.assertEqual(200, res)

            cur.execute("select @wdr")
            res = cur.fetchone()[0]
            self.assertEqual(100, res)

    @tests.foreach_cnx()
    def test_action_order(self):
        config = self.get_clean_mysql_config()
        with self.cnx.cursor() as cur:
            cur.execute(
                "select action_order from information_schema.triggers "
                "where trigger_schema='{}' and trigger_name='ac_sum'"
                "".format(config["database"])
            )
            res = cur.fetchone()[0]
            self.assertEqual(2, res)

            cur.execute(
                "select action_order from information_schema.triggers "
                "where trigger_schema='{}' and trigger_name='ac_trans'"
                "".format(config["database"])
            )
            res = cur.fetchone()[0]
            self.assertEqual(1, res)

    @tests.foreach_cnx()
    def test_created(self):
        config = self.get_clean_mysql_config()
        time_1 = datetime.datetime.now()
        time_2 = datetime.datetime.now()
        with self.cnx.cursor() as cur:
            cur.execute(
                "select created from information_schema.triggers "
                "where trigger_schema='{}' and trigger_name='ac_sum'"
                "".format(config["database"])
            )
            res = cur.fetchone()[0]
            self.assertGreater(datetime.timedelta(seconds=10000), time_1 - res)
            cur.execute(
                "select created from information_schema.triggers "
                "where trigger_schema='{}' and trigger_name='ac_trans'"
                "".format(config["database"])
            )
            res = cur.fetchone()[0]
            self.assertGreater(datetime.timedelta(seconds=10000), time_2 - res)
