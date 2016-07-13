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

""" BUG21879859
"""

import os.path
import unittest
import mysql.connector
from mysql.connector import Error
from tests import foreach_cnx, cnx_config
import tests

try:
    from mysql.connector.connection_cext import CMySQLConnection
except ImportError:
    # Test without C Extension
    CMySQLConnection = None


class Bug21879859(tests.MySQLConnectorTests):
    def setUp(self):
        self.table = "Bug21879859"
        self.proc = "Bug21879859_proc"

        cnx = mysql.connector.connect(**tests.get_mysql_config())
        cur = cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS {0}".format(self.table))
        cur.execute("DROP PROCEDURE IF EXISTS {0}".format(self.proc))
        cur.execute("CREATE TABLE {0} (c1 VARCHAR(1024))".format(self.table))
        cur.execute(
            "CREATE PROCEDURE {1}() BEGIN SELECT 1234; "
            "SELECT t from {0}; SELECT '' from {0}; END".format(
                self.table, self.proc
            ));

    def tearDown(self):
        cnx = mysql.connector.connect(**tests.get_mysql_config())
        cur = cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS {0}".format(self.table))
        cur.execute("DROP PROCEDURE IF EXISTS {0}".format(self.proc))

    @cnx_config(consume_results=True)
    @foreach_cnx()
    def test_consume_after_callproc(self):
        cur = self.cnx.cursor()

        cur.execute("INSERT INTO {0} VALUES ('a'),('b'),('c')".format(self.table))

        # expected to fail
        self.assertRaises(Error, cur.callproc, self.proc)
        try:
            cur.close()
        except mysql.connector.Error as exc:
            self.fail("Failed closing: " + str(exc))
