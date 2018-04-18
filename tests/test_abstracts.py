# Copyright (c) 2014, 2018, Oracle and/or its affiliates. All rights reserved.
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

"""Unittests for mysql.connector.abstracts
"""

from decimal import Decimal
from operator import attrgetter

import unittest
import tests
from tests import PY2, foreach_cnx

from mysql.connector.connection import MySQLConnection
from mysql.connector.constants import RefreshOption
from mysql.connector import errors

try:
    from mysql.connector.connection_cext import CMySQLConnection
except ImportError:
    # Test without C Extension
    CMySQLConnection = None


class ConnectionSubclasses(tests.MySQLConnectorTests):

    """Tests for any subclass of MySQLConnectionAbstract
    """

    def asEq(self, exp, *cases):
        for case in cases:
            self.assertEqual(exp, case)

    @foreach_cnx()
    def test_properties_getter(self):
        properties = [
            (self.config['user'], 'user'),
            (self.config['host'], 'server_host'),
            (self.config['port'], 'server_port'),
            (self.config['unix_socket'], 'unix_socket'),
            (self.config['database'], 'database')
        ]

        for exp, property in properties:
            f = attrgetter(property)
            self.asEq(exp, f(self.cnx))

    @foreach_cnx()
    def test_time_zone(self):
        orig = self.cnx.info_query("SELECT @@session.time_zone")[0]
        self.assertEqual(orig, self.cnx.time_zone)
        self.cnx.time_zone = "+02:00"
        self.assertEqual("+02:00", self.cnx.time_zone)

    @foreach_cnx()
    def test_sql_mode(self):
        orig = self.cnx.info_query("SELECT @@session.sql_mode")[0]
        self.assertEqual(orig, self.cnx.sql_mode)

        try:
            self.cnx.sql_mode = 'SPAM'
        except errors.ProgrammingError:
            pass  # excepted
        else:
            self.fail("ProgrammingError not raises")

        # Set SQL Mode to a list of modes
        if tests.MYSQL_VERSION[0:3] < (5, 7, 4):
            exp = ('STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,'
                   'NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,TRADITIONAL,'
                   'NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION')
        elif tests.MYSQL_VERSION[0:3] < (8, 0, 5):
            exp = ('STRICT_TRANS_TABLES,STRICT_ALL_TABLES,TRADITIONAL,'
                   'NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION')
        else:
            exp = ('STRICT_TRANS_TABLES,STRICT_ALL_TABLES,TRADITIONAL,'
                   'NO_ENGINE_SUBSTITUTION')

        try:
            self.cnx.sql_mode = exp
        except errors.Error as err:
            self.fail("Failed setting SQL Mode with multiple "
                      "modes: {0}".format(str(err)))
        self.assertEqual(exp, self.cnx._sql_mode)

        # SQL Modes must be empty
        self.cnx.sql_mode = ''
        self.assertEqual('', self.cnx.sql_mode)

        # Set SQL Mode and check
        sql_mode = exp = 'STRICT_ALL_TABLES'
        self.cnx.sql_mode = sql_mode
        self.assertEqual(exp, self.cnx.sql_mode)

        # Unset the SQL Mode again
        self.cnx.sql_mode = ''
        self.assertEqual('', self.cnx.sql_mode)

    @foreach_cnx()
    def test_in_transaction(self):
        self.cnx.cmd_query('START TRANSACTION')
        self.assertTrue(self.cnx.in_transaction)
        self.cnx.cmd_query('ROLLBACK')
        self.assertFalse(self.cnx.in_transaction)

        # AUTO_COMMIT turned ON
        self.cnx.autocommit = True
        self.assertFalse(self.cnx.in_transaction)

        self.cnx.cmd_query('START TRANSACTION')
        self.assertTrue(self.cnx.in_transaction)

    @foreach_cnx()
    def test_disconnect(self):
        self.cnx.disconnect()
        self.assertFalse(self.cnx.is_connected())

    @foreach_cnx()
    def test_is_connected(self):
        """Check connection to MySQL Server"""
        self.assertEqual(True, self.cnx.is_connected())
        self.cnx.disconnect()
        self.assertEqual(False, self.cnx.is_connected())

    @foreach_cnx()
    def test_info_query(self):
        queries = [
            ("SELECT 1",
             (1,)),
            ("SELECT 'ham', 'spam'",
             ((u'ham', u'spam')))
        ]
        for query, exp in queries:
            self.assertEqual(exp, self.cnx.info_query(query))

    @foreach_cnx()
    def test_cmd_init_db(self):
        self.assertRaises(errors.ProgrammingError,
                          self.cnx.cmd_init_db, 'unknown_database')
        self.cnx.cmd_init_db(u'INFORMATION_SCHEMA')
        self.assertEqual('INFORMATION_SCHEMA', self.cnx.database.upper())
        self.cnx.cmd_init_db('mysql')
        self.assertEqual(u'mysql', self.cnx.database)
        self.cnx.cmd_init_db('myconnpy')
        self.assertEqual(u'myconnpy', self.cnx.database)

    @foreach_cnx()
    def test_reset_session(self):
        exp = [True, u'STRICT_ALL_TABLES', u'-09:00', 33]
        self.cnx.autocommit = exp[0]
        self.cnx.sql_mode = exp[1]
        self.cnx.time_zone = exp[2]
        self.cnx.set_charset_collation(exp[3])

        user_variables = {'ham': '1', 'spam': '2'}
        session_variables = {'wait_timeout': 100000}
        self.cnx.reset_session(user_variables, session_variables)

        self.assertEqual(exp, [self.cnx.autocommit, self.cnx.sql_mode,
                               self.cnx.time_zone, self.cnx._charset_id])

        exp_user_variables = {'ham': 1, 'spam': 2}
        exp_session_variables = {'wait_timeout': 100000}

        for key, value in exp_user_variables.items():
            row = self.cnx.info_query("SELECT @{0}".format(key))
            self.assertEqual(value, int(row[0]))
        for key, value in exp_session_variables.items():
            row = self.cnx.info_query("SELECT @@session.{0}".format(key))
            self.assertEqual(value, row[0])

    @unittest.skipIf(tests.MYSQL_VERSION > (5, 7, 10),
                     "As of MySQL 5.7.11, mysql_refresh() is deprecated")
    @foreach_cnx()
    def test_cmd_refresh(self):
        refresh = RefreshOption.LOG | RefreshOption.THREADS
        exp = {'insert_id': 0, 'affected_rows': 0,
               'field_count': 0, 'warning_count': 0,
               'status_flag': 0}
        result = self.cnx.cmd_refresh(refresh)
        for key in set(result.keys()) ^ set(exp.keys()):
            try:
                del result[key]
            except KeyError:
                del exp[key]
        self.assertEqual(exp, result)

        query = "SHOW GLOBAL STATUS LIKE 'Uptime_since_flush_status'"
        pre_flush = int(self.cnx.info_query(query)[1])
        self.cnx.cmd_refresh(RefreshOption.STATUS)
        post_flush = int(self.cnx.info_query(query)[1])
        self.assertTrue(post_flush <= pre_flush)

    @foreach_cnx()
    def test_cmd_quit(self):
        self.cnx.cmd_quit()
        self.assertFalse(self.cnx.is_connected())

    @unittest.skipIf(tests.MYSQL_VERSION >= (8, 0, 1),
                     "As of MySQL 8.0.1, CMD_SHUTDOWN is not recognized.")
    @unittest.skipIf(tests.MYSQL_VERSION <= (5, 7, 1),
                     "BugOra17422299 not tested with MySQL version 5.6")
    @foreach_cnx()
    def test_cmd_shutdown(self):
        server = tests.MYSQL_SERVERS[0]
        # We make sure the connection is re-established.
        self.cnx = self.cnx.__class__(**self.config)
        self.cnx.cmd_shutdown()

        if not server.wait_down():
            self.fail("[{0}] ".format(self.cnx.__class__.__name__) +
                      "MySQL not shut down after cmd_shutdown()")

        self.assertRaises(errors.Error, self.cnx.cmd_shutdown)

        server.start()
        if not server.wait_up():
            self.fail("Failed restarting MySQL server after test")

    @foreach_cnx()
    def test_cmd_statistics(self):
        exp = {
            'Uptime': int,
            'Open tables': int,
            'Queries per second avg': Decimal,
            'Slow queries': int,
            'Threads': int,
            'Questions': int,
            'Flush tables': int,
            'Opens': int
        }

        stat = self.cnx.cmd_statistics()
        self.assertEqual(len(exp), len(stat))
        for key, type_ in exp.items():
            self.assertTrue(key in stat)
            self.assertTrue(isinstance(stat[key], type_))

    @foreach_cnx()
    def test_cmd_process_info(self):
        self.assertRaises(errors.NotSupportedError,
                          self.cnx.cmd_process_info)

    @foreach_cnx()
    def test_cmd_process_kill(self):
        other_cnx = self.cnx.__class__(**self.config)
        pid = other_cnx.connection_id

        self.cnx.cmd_process_kill(pid)
        self.assertFalse(other_cnx.is_connected())

    @foreach_cnx()
    def test_start_transaction(self):
        self.cnx.start_transaction()
        self.assertTrue(self.cnx.in_transaction)
        self.cnx.rollback()

        self.cnx.start_transaction(consistent_snapshot=True)
        self.assertTrue(self.cnx.in_transaction)
        self.assertRaises(errors.ProgrammingError,
                          self.cnx.start_transaction)
        self.cnx.rollback()

        levels = ['READ UNCOMMITTED', 'READ COMMITTED', 'REPEATABLE READ',
                  'SERIALIZABLE',
                  'READ-UNCOMMITTED', 'READ-COMMITTED', 'REPEATABLE-READ',
                  'SERIALIZABLE']

        for level in levels:
            level = level.replace(' ', '-')
            self.cnx.start_transaction(isolation_level=level)
            self.assertTrue(self.cnx.in_transaction)
            self.cnx.rollback()

        self.assertRaises(ValueError,
                          self.cnx.start_transaction,
                          isolation_level='spam')
