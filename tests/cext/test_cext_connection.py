# -*- coding: utf-8 -*-
# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2014, Oracle and/or its affiliates. All rights reserved.

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

"""Testing connection.CMySQLConnection class using the C Extension
"""

import tests

from mysql.connector import errors
from mysql.connector.constants import ClientFlag, flag_is_set
from mysql.connector.connection import MySQLConnection
from mysql.connector.connection_cext import CMySQLConnection


class CMySQLConnectionTests(tests.MySQLConnectorTests):

    def setUp(self):
        config = tests.get_mysql_config()
        self.cnx = CMySQLConnection(**config)

        self.pcnx = MySQLConnection(**config)

    def test__info_query(self):
        query = "SELECT 1, 'a', 2, 'b'"
        exp = (1, 'a', 2, 'b')
        self.assertEqual(exp, self.cnx.info_query(query))

        self.assertRaises(errors.InterfaceError, self.cnx.info_query,
                          "SHOW VARIABLES LIKE '%char%'")

    def test_client_flags(self):
        defaults = ClientFlag.default
        set_flags = self.cnx._cmysql.st_client_flag()
        for flag in defaults:
            self.assertTrue(flag_is_set(flag, set_flags))

    def test_get_rows(self):
        self.assertRaises(errors.InternalError, self.cnx.get_rows)

        query = "SHOW STATUS LIKE 'Aborted_c%'"

        self.cnx.cmd_query(query)
        self.assertRaises(AttributeError, self.cnx.get_rows, 0)
        self.assertRaises(AttributeError, self.cnx.get_rows, -10)
        self.assertEqual(2, len(self.cnx.get_rows()))
        self.cnx.free_result()

        self.cnx.cmd_query(query)
        self.assertEqual(1, len(self.cnx.get_rows(count=1)))
        self.assertEqual(1, len(self.cnx.get_rows(count=1)))
        self.assertEqual([], self.cnx.get_rows(count=1))
        self.cnx.free_result()

    def test_cmd_init_db(self):
        query = "SELECT DATABASE()"
        self.cnx.cmd_init_db('mysql')
        self.assertEqual('mysql', self.cnx.info_query(query)[0])

        self.cnx.cmd_init_db('myconnpy')
        self.assertEqual('myconnpy', self.cnx.info_query(query)[0])

    def test_cmd_query(self):
        query = "SHOW STATUS LIKE 'Aborted_c%'"
        info = self.cnx.cmd_query(query)

        exp = {
            'eof': {'status_flag': 32, 'warning_count': 0},
            'columns': [
                ('Variable_name', 253, None, None, None, None, 0, 1),
                ('Value', 253, None, None, None, None, 1, 0)]
        }
        self.assertEqual(exp, info)

        rows = self.cnx.get_rows()
        vars = [ row[0] for row in rows ]
        self.assertEqual(2, len(rows))

        vars.sort()
        exp = ['Aborted_clients', 'Aborted_connects']
        self.assertEqual(exp, vars)

        exp = ['Value', 'Variable_name']
        fields = [fld[0] for fld in info['columns']]
        fields.sort()
        self.assertEqual(exp, fields)

        self.cnx.free_result()

        info = self.cnx.cmd_query("SET @a = 1")
        exp = {
            'warning_count': 0, 'insert_id': 0, 'affected_rows': 0,
            'server_status': 0, 'field_count': 0
        }
        self.assertEqual(exp, info)
