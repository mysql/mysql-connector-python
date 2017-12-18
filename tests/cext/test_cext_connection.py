# -*- coding: utf-8 -*-

# Copyright (c) 2014, 2017, Oracle and/or its affiliates. All rights reserved.
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
                ['Variable_name', 253, None, None, None, None, 0, 1],
                ('Value', 253, None, None, None, None, 1, 0)
            ]
        }

        if tests.MYSQL_VERSION >= (5, 7, 10):
            exp['columns'][0][7] = 4097
            exp['eof']['status_flag'] = 16385

        exp['columns'][0] = tuple(exp['columns'][0])

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
