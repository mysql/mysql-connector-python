# -*- coding: utf-8 -*-
# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

# MySQL Connector/Python is licensed under the terms of the GPLv2
# <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most
# MySQL Connectors. There are special exceptions to the terms and
# conditions of the GPLv2 as it is applied to this software, see the
# FOSS License Exception
# <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Incur., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA

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
