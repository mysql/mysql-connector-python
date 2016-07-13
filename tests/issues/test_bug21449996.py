# -*- coding: utf-8 -*-
# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

import os.path
import unittest

import mysql.connector
from tests import foreach_cnx, cnx_config
import tests

DATA_FILE = os.path.join('tests', 'data', 'random_big_bin.csv')

class Bug21449996(tests.MySQLConnectorTests):

    def setUp(self):
        self.table_name = 'Bug21449996'
        cnx = mysql.connector.connect(**tests.get_mysql_config())
        cnx.cmd_query("DROP TABLE IF EXISTS %s" % self.table_name)
        cnx.cmd_query("CREATE TABLE %s (c1 BLOB)" % self.table_name)
        cnx.close()

    def tearDown(self):
        cnx = mysql.connector.connect(**tests.get_mysql_config())
        cnx.cmd_query("DROP TABLE IF EXISTS %s" % self.table_name)
        cnx.close()

    @foreach_cnx()
    def test_load_data_compressed(self):
        try:
            cur = self.cnx.cursor()
            sql = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s" % (
                DATA_FILE, self.table_name)
            cur.execute(sql)
        except mysql.connector.errors.InterfaceError as exc:
            self.fail(exc)

        cur.execute("SELECT COUNT(*) FROM %s" % self.table_name)
        self.assertEqual(11486, cur.fetchone()[0])