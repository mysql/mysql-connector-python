# -*- coding: utf-8 -*-

# Copyright (c) 2016, 2017, Oracle and/or its affiliates. All rights reserved.
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
from tests import foreach_cnx, cnx_config
import tests


class Bug21449207(tests.MySQLConnectorTests):
    def setUp(self):
        self.tbl = 'Bug21449207'
        cnx = mysql.connector.connect(**tests.get_mysql_config())
        cnx.cmd_query("DROP TABLE IF EXISTS %s" % self.tbl)

        create_table = (
            "CREATE TABLE {0} ("
            "id INT PRIMARY KEY, "
            "a LONGTEXT "
            ") ENGINE=Innodb DEFAULT CHARSET utf8".format(self.tbl))
        cnx.cmd_query(create_table)
        cnx.close()

    def tearDown(self):
        cnx = mysql.connector.connect(**tests.get_mysql_config())
        cnx.cmd_query("DROP TABLE IF EXISTS %s" % self.tbl)
        cnx.close()

    @foreach_cnx()
    def test_uncompressed(self):
        cur = self.cnx.cursor()
        exp = 'a' * 15 + 'TheEnd'
        insert = "INSERT INTO {0} (a) VALUES ('{1}')".format(self.tbl, exp)
        cur.execute(insert)
        cur.execute("SELECT a FROM {0}".format(self.tbl))
        row = cur.fetchone()
        self.assertEqual(exp, row[0])
        self.assertEqual(row[0][-20:], exp[-20:])

    @foreach_cnx()
    def test_50k_compressed(self):
        cur = self.cnx.cursor()
        exp = 'a' * 50000 + 'TheEnd'
        insert = "INSERT INTO {0} (a) VALUES ('{1}')".format(self.tbl, exp)
        cur.execute(insert)
        cur.execute("SELECT a FROM {0}".format(self.tbl))
        row = cur.fetchone()
        self.assertEqual(exp, row[0])
        self.assertEqual(row[0][-20:], exp[-20:])

    @foreach_cnx()
    def test_16M_compressed(self):
        cur = self.cnx.cursor()
        exp = 'a' * 16777210 + 'TheEnd'
        insert = "INSERT INTO {0} (a) VALUES ('{1}')".format(self.tbl, exp)
        cur.execute(insert)
        cur.execute("SELECT a FROM {0}".format(self.tbl))
        row = cur.fetchone()
        self.assertEqual(exp, row[0])
        self.assertEqual(row[0][-20:], exp[-20:])
