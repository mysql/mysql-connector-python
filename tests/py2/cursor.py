# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved.

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
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA

"""Unit tests for mysql.connector.cursor specific to Python v2
"""

import itertools
from decimal import Decimal
import time
import datetime
import new

import tests
from mysql.connector import (connection, cursor, errors)


class MySQLCursorTests(tests.TestsCursor):

    def setUp(self):
        self.cur = cursor.MySQLCursor(connection=None)
        self.cnx = None

    def test_init(self):
        """MySQLCursor object init"""
        try:
            cur = cursor.MySQLCursor(connection=None)
        except (SyntaxError, TypeError) as err:
            self.fail("Failed initializing MySQLCursor; {0}".format(err))

        exp = {
            '_connection': None,
            '_stored_results': [],
            '_nextrow': (None, None),
            '_warnings': None,
            '_warning_count': 0,
            '_executed': None,
            '_executed_list': [],
        }

        for key, value in exp.items():
            self.assertEqual(
                value, getattr(cur, key),
                msg="Default for '{0}' did not match.".format(key))

        self.assertRaises(errors.InterfaceError, cursor.MySQLCursor,
                          connection='foo')

    def test__set_connection(self):
        """MySQLCursor object _set_connection()-method"""
        self.check_method(self.cur, '_set_connection')

        self.assertRaises(errors.InterfaceError,
                          self.cur._set_connection, 'foo')
        self.connection = connection.MySQLConnection(**tests.get_mysql_config())
        self.cur._set_connection(self.connection)
        self.cur.close()

    def test__reset_result(self):
        """MySQLCursor object _reset_result()-method"""
        self.check_method(self.cur, '_reset_result')

        def reset(self):
            self._test = "Reset called"
        self.cur.reset = new.instancemethod(reset, self.cur, cursor.MySQLCursor)

        exp = {
            'rowcount': -1,
            '_stored_results': [],
            '_nextrow': (None, None),
            '_warnings': None,
            '_warning_count': 0,
            '_executed': None,
            '_executed_list': [],
        }

        self.cur._reset_result()

        for key, value in exp.items():
            self.assertEqual(value, getattr(self.cur, key),
                             msg="'{0}' was not reset.".format(key))

        # MySQLCursor._reset_result() must call MySQLCursor.reset()
        self.assertEqual('Reset called', self.cur._test)

    def test__have_unread_result(self):
        """MySQLCursor object _have_unread_result()-method"""
        self.check_method(self.cur, '_have_unread_result')

        class FakeConnection(object):

            def __init__(self):
                self.unread_result = False

        self.cur = cursor.MySQLCursor()
        self.cur._connection = FakeConnection()

        self.cur._connection.unread_result = True
        self.assertTrue(self.cur._have_unread_result())
        self.cur._connection.unread_result = False
        self.assertFalse(self.cur._have_unread_result())

    def test_next(self):
        """MySQLCursor object next()-method"""
        self.check_method(self.cur, 'next')

        self.connection = connection.MySQLConnection(**tests.get_mysql_config())
        self.cur = cursor.MySQLCursor(self.connection)
        self.assertRaises(StopIteration, self.cur.next)
        self.cur.execute("SELECT SHA1('myconnpy')")
        exp = (u'c5e24647dbb63447682164d81b34fe493a83610b',)
        self.assertEqual(exp, self.cur.next())
        self.cur.close()

    def test_close(self):
        """MySQLCursor object close()-method"""
        self.check_method(self.cur, 'close')

        self.assertEqual(False, self.cur.close(),
                         "close() should return False with no connection")
        self.assertEqual(None, self.cur._connection)

    def test__process_params(self):
        """MySQLCursor object _process_params()-method"""
        self.check_method(self.cur, '_process_params')

        self.assertRaises(
            errors.ProgrammingError, self.cur._process_params, 'foo')
        self.assertRaises(errors.ProgrammingError, self.cur._process_params, ())

        st_now = time.localtime()
        data = (
            None,
            int(128),
            long(1281288),
            float(3.14),
            Decimal('3.14'),
            r'back\slash',
            'newline\n',
            'return\r',
            "'single'",
            '"double"',
            'windows\032',
            str("Strings are sexy"),
            u'\u82b1',
            datetime.datetime(2008, 5, 7, 20, 0o1, 23),
            datetime.date(2008, 5, 7),
            datetime.time(20, 0o3, 23),
            st_now,
            datetime.timedelta(hours=40, minutes=30, seconds=12),
        )
        exp = (
            'NULL',
            '128',
            '1281288',
            '3.14',
            "'3.14'",
            "'back\\\\slash'",
            "'newline\\n'",
            "'return\\r'",
            "'\\'single\\''",
            '\'\\"double\\"\'',
            "'windows\\\x1a'",
            "'Strings are sexy'",
            "'\xe8\x8a\xb1'",
            "'2008-05-07 20:01:23'",
            "'2008-05-07'",
            "'20:03:23'",
            "'" + time.strftime('%Y-%m-%d %H:%M:%S', st_now) + "'",
            "'40:30:12'",
        )

        self.connection = connection.MySQLConnection(**tests.get_mysql_config())
        self.cur = self.connection.cursor()
        self.assertEqual((), self.cur._process_params(()),
                         "_process_params() should return a tuple")
        res = self.cur._process_params(data)
        for (i, exped) in enumerate(exp):
            self.assertEqual(exped, res[i])
        self.cur.close()

    def test__process_params_dict(self):
        """MySQLCursor object _process_params_dict()-method"""
        self.check_method(self.cur, '_process_params')

        self.assertRaises(
            errors.ProgrammingError, self.cur._process_params, 'foo')
        self.assertRaises(errors.ProgrammingError, self.cur._process_params, ())

        st_now = time.localtime()
        data = {
            'a': None,
            'b': int(128),
            'c': long(1281288),
            'd': float(3.14),
            'e': Decimal('3.14'),
            'f': 'back\slash',  # pylint: disable=W1401
            'g': 'newline\n',
            'h': 'return\r',
            'i': "'single'",
            'j': '"double"',
            'k': 'windows\032',
            'l': str("Strings are sexy"),
            'm': u'\u82b1',
            'n': datetime.datetime(2008, 5, 7, 20, 0o1, 23),
            'o': datetime.date(2008, 5, 7),
            'p': datetime.time(20, 0o3, 23),
            'q': st_now,
            'r': datetime.timedelta(hours=40, minutes=30, seconds=12),
        }
        exp = {
            'a': 'NULL',
            'b': '128',
            'c': '1281288',
            'd': '3.14',
            'e': "'3.14'",
            'f': "'back\\\\slash'",
            'g': "'newline\\n'",
            'h': "'return\\r'",
            'i': "'\\'single\\''",
            'j': '\'\\"double\\"\'',
            'k': "'windows\\\x1a'",
            'l': "'Strings are sexy'",
            'm': "'\xe8\x8a\xb1'",
            'n': "'2008-05-07 20:01:23'",
            'o': "'2008-05-07'",
            'p': "'20:03:23'",
            'q': "'" + time.strftime('%Y-%m-%d %H:%M:%S', st_now) + "'",
            'r': "'40:30:12'",
        }

        self.connection = connection.MySQLConnection(**tests.get_mysql_config())
        self.cur = self.connection.cursor()
        self.assertEqual({}, self.cur._process_params_dict({}),
                         "_process_params_dict() should return a dict")
        self.assertEqual(exp, self.cur._process_params_dict(data))
        self.cur.close()

    def test__fetch_warnings(self):
        """MySQLCursor object _fetch_warnings()-method"""
        self.check_method(self.cur, '_fetch_warnings')

        self.assertRaises(errors.InterfaceError, self.cur._fetch_warnings)

        config = tests.get_mysql_config()
        config['get_warnings'] = True
        self.connection = connection.MySQLConnection(**config)
        self.cur = self.connection.cursor()
        self.cur.execute("SELECT 'a' + 'b'")
        self.cur.fetchone()
        exp = [
            (u'Warning', 1292, u"Truncated incorrect DOUBLE value: 'a'"),
            (u'Warning', 1292, u"Truncated incorrect DOUBLE value: 'b'")
        ]
        self.assertTrue(tests.cmp_result(exp, self.cur._fetch_warnings()))
        self.assertEqual(len(exp), self.cur._warning_count)

    def test__handle_noresultset(self):
        """MySQLCursor object _handle_noresultset()-method"""
        self.check_method(self.cur, '_handle_noresultset')

        self.assertRaises(errors.ProgrammingError,
                          self.cur._handle_noresultset, None)
        data = {
            'affected_rows': 1,
            'insert_id': 10,
            'warning_count': 100,
            'server_status': 8,
        }
        self.connection = connection.MySQLConnection(**tests.get_mysql_config())
        self.cur = self.connection.cursor()
        self.cur._handle_noresultset(data)
        self.assertEqual(data['affected_rows'], self.cur.rowcount)
        self.assertEqual(data['insert_id'], self.cur._last_insert_id)
        self.assertEqual(data['warning_count'], self.cur._warning_count)

        self.cur.close()

    def test__handle_result(self):
        """MySQLCursor object _handle_result()-method"""
        self.connection = connection.MySQLConnection(**tests.get_mysql_config())
        self.cur = self.connection.cursor()

        self.assertRaises(errors.InterfaceError, self.cur._handle_result, None)
        self.assertRaises(errors.InterfaceError, self.cur._handle_result,
                          'spam')
        self.assertRaises(errors.InterfaceError, self.cur._handle_result,
                          {'spam': 5})

        cases = [
            {'affected_rows': 99999,
                'insert_id': 10,
                'warning_count': 100,
                'server_status': 8,
             },
            {'eof': {'status_flag': 0, 'warning_count': 0},
                'columns': [('1', 8, None, None, None, None, 0, 129)]
             },
        ]
        self.cur._handle_result(cases[0])
        self.assertEqual(cases[0]['affected_rows'], self.cur.rowcount)
        self.assertFalse(self.cur._connection.unread_result)
        self.assertFalse(self.cur._have_unread_result())

        self.cur._handle_result(cases[1])
        self.assertEqual(cases[1]['columns'], self.cur.description)
        self.assertTrue(self.cur._connection.unread_result)
        self.assertTrue(self.cur._have_unread_result())

    def test_execute(self):
        """MySQLCursor object execute()-method"""
        self.check_method(self.cur, 'execute')

        self.assertEqual(None, self.cur.execute(None, None))

        config = tests.get_mysql_config()
        config['get_warnings'] = True
        self.connection = connection.MySQLConnection(**config)
        self.cur = self.connection.cursor()

        self.assertRaises(errors.ProgrammingError, self.cur.execute,
                          'SELECT %s,%s,%s', ('foo', 'bar',))
        self.assertRaises(errors.ProgrammingError, self.cur.execute,
                          'SELECT %s,%s', ('foo', 'bar', 'foobar'))

        self.cur.execute("SELECT 'a' + 'b'")
        self.cur.fetchone()
        exp = [
            (u'Warning', 1292, u"Truncated incorrect DOUBLE value: 'a'"),
            (u'Warning', 1292, u"Truncated incorrect DOUBLE value: 'b'")
        ]
        self.assertTrue(tests.cmp_result(exp, self.cur._warnings))

        self.cur.execute("SELECT BINARY 'myconnpy'")
        exp = [(u'myconnpy',)]
        self.assertEqual(exp, self.cur.fetchall())
        self.cur.close()

        tbl = 'myconnpy_cursor'
        self._test_execute_setup(self.connection, tbl)
        stmt_insert = "INSERT INTO {0} (col1,col2) VALUES (%s,%s)".format(tbl)

        self.cur = self.connection.cursor()
        res = self.cur.execute(stmt_insert, (1, 100))
        self.assertEqual(None, res, "Return value of execute() is wrong.")
        stmt_select = "SELECT col1,col2 FROM {0} ORDER BY col1".format(tbl)
        self.cur.execute(stmt_select)
        self.assertEqual([(1, u'100')],
                         self.cur.fetchall(), "Insert test failed")

        data = {'id': 2}
        stmt = "SELECT * FROM {0} WHERE col1 <= %(id)s".format(tbl)
        self.cur.execute(stmt, data)
        self.assertEqual([(1, u'100')], self.cur.fetchall())

        self._test_execute_cleanup(self.connection, tbl)
        self.cur.close()

    def test_executemany(self):
        """MySQLCursor object executemany()-method"""
        self.check_method(self.cur, 'executemany')

        self.assertEqual(None, self.cur.executemany(None, []))

        config = tests.get_mysql_config()
        config['get_warnings'] = True
        self.connection = connection.MySQLConnection(**config)
        self.cur = self.connection.cursor()
        self.assertRaises(errors.InterfaceError, self.cur.executemany,
                          'foo', None)
        self.assertRaises(errors.ProgrammingError, self.cur.executemany,
                          'foo', 'foo')
        self.assertEqual(None, self.cur.executemany('foo', []))
        self.assertRaises(errors.ProgrammingError, self.cur.executemany,
                          'foo', ['foo'])
        self.assertRaises(errors.ProgrammingError, self.cur.executemany,
                          'SELECT %s', [('foo',), 'foo'])
        self.assertRaises(errors.ProgrammingError,
                          self.cur.executemany,
                          "INSERT INTO t1 1 %s", [(1,), (2,)])

        self.cur.executemany("SELECT SHA1(%s)", [('foo',), ('bar',)])
        self.assertEqual(None, self.cur.fetchone())
        self.cur.close()

        tbl = 'myconnpy_cursor'
        self._test_execute_setup(self.connection, tbl)
        stmt_insert = "INSERT INTO {0} (col1,col2) VALUES (%s,%s)".format(tbl)
        stmt_select = "SELECT col1,col2 FROM {0} ORDER BY col1".format(tbl)

        self.cur = self.connection.cursor()

        self.cur.executemany(stmt_insert, [(1, 100), (2, 200), (3, 300)])
        self.assertEqual(3, self.cur.rowcount)

        self.cur.executemany("SELECT %s", [('f',), ('o',), ('o',)])
        self.assertEqual(3, self.cur.rowcount)

        data = [{'id': 2}, {'id': 3}]
        stmt = "SELECT * FROM {0} WHERE col1 <= %(id)s".format(tbl)
        self.cur.executemany(stmt, data)
        self.assertEqual(5, self.cur.rowcount)

        self.cur.execute(stmt_select)
        self.assertEqual([(1, u'100'), (2, u'200'), (3, u'300')],
                         self.cur.fetchall(), "Multi insert test failed")

        data = [{'id': 2}, {'id': 3}]
        stmt = "DELETE FROM {0} WHERE col1 = %(id)s".format(tbl)
        self.cur.executemany(stmt, data)
        self.assertEqual(2, self.cur.rowcount)

        stmt = "TRUNCATE TABLE {0}".format(tbl)
        self.cur.execute(stmt)

        stmt = (
            "/*comment*/INSERT/*comment*/INTO/*comment*/{0}(col1,col2)VALUES"
            "/*comment*/(%s,%s/*comment*/)/*comment()*/ON DUPLICATE KEY UPDATE"
            " col1 = VALUES(col1)"
        ).format(tbl)

        self.cur.executemany(stmt, [(4, 100), (5, 200), (6, 300)])
        self.assertEqual(3, self.cur.rowcount)

        self.cur.execute(stmt_select)
        self.assertEqual([(4, u'100'), (5, u'200'), (6, u'300')],
                         self.cur.fetchall(), "Multi insert test failed")

        stmt = "TRUNCATE TABLE {0}".format(tbl)
        self.cur.execute(stmt)

        stmt = (
            "/*comment*/INSERT/*comment*/INTO/*comment*/{0}(col1,col2)VALUES"
            "/*comment*/(%s,'/*100*/')/*comment()*/ON DUPLICATE KEY UPDATE "
            "col1 = VALUES(col1)"
        ).format(tbl)

        self.cur.executemany(stmt, [(4,), (5,)])
        self.assertEqual(2, self.cur.rowcount)

        self.cur.execute(stmt_select)
        self.assertEqual([(4, u'/*100*/'), (5, u'/*100*/')],
                         self.cur.fetchall(), "Multi insert test failed")
        self._test_execute_cleanup(self.connection, tbl)
        self.cur.close()

    def test_fetchwarnings(self):
        """MySQLCursor object fetchwarnings()-method"""
        self.check_method(self.cur, 'fetchwarnings')

        self.assertEqual(None, self.cur.fetchwarnings(),
                         "There should be no warnings after initiating cursor.")

        exp = ['A warning']
        self.cur._warnings = exp
        self.cur._warning_count = len(self.cur._warnings)
        self.assertEqual(exp, self.cur.fetchwarnings())
        self.cur.close()

    def test_stored_results(self):
        """MySQLCursor object stored_results()-method"""
        self.check_method(self.cur, 'stored_results')

        self.assertEqual([], self.cur._stored_results)
        self.assertTrue(hasattr(self.cur.stored_results(), '__iter__'))
        self.cur._stored_results.append('abc')
        self.assertEqual('abc', self.cur.stored_results().next())
        try:
            _ = self.cur.stored_results().next()
        except StopIteration:
            pass
        except:
            self.fail("StopIteration not raised")

    def _test_callproc_setup(self, connection):

        self._test_callproc_cleanup(connection)
        stmt_create1 = (
            "CREATE PROCEDURE myconnpy_sp_1"
            "(IN pFac1 INT, IN pFac2 INT, OUT pProd INT) "
            "BEGIN SET pProd := pFac1 * pFac2; END;")

        stmt_create2 = (
            "CREATE PROCEDURE myconnpy_sp_2"
            "(IN pFac1 INT, IN pFac2 INT, OUT pProd INT) "
            "BEGIN SELECT 'abc'; SELECT 'def'; SET pProd := pFac1 * pFac2; END;"
        )

        stmt_create3 = (
            "CREATE PROCEDURE myconnpy_sp_3"
            "(IN pStr1 VARCHAR(20), IN pStr2 VARCHAR(20), "
            "OUT pConCat VARCHAR(100)) "
            "BEGIN SET pConCat := CONCAT(pStr1, pStr2); END;")

        stmt_create4 = (
            "CREATE PROCEDURE myconnpy_sp_4"
            "(IN pStr1 VARCHAR(20), INOUT pStr2 VARCHAR(20), "
            "OUT pConCat VARCHAR(100)) "
            "BEGIN SET pConCat := CONCAT(pStr1, pStr2); END;")

        try:
            cursor = connection.cursor()
            cursor.execute(stmt_create1)
            cursor.execute(stmt_create2)
            cursor.execute(stmt_create3)
            cursor.execute(stmt_create4)
        except errors.Error as err:
            self.fail("Failed setting up test stored routine; {0}".format(err))
        cursor.close()

    def _test_callproc_cleanup(self, connection):

        sp_names = ('myconnpy_sp_1', 'myconnpy_sp_2', 'myconnpy_sp_3',
                    'myconnpy_sp_4')
        stmt_drop = "DROP PROCEDURE IF EXISTS {procname}"

        try:
            cursor = connection.cursor()
            for sp_name in sp_names:
                cursor.execute(stmt_drop.format(procname=sp_name))
        except errors.Error as err:
            self.fail(
                "Failed cleaning up test stored routine; {0}".format(err))
        cursor.close()

    def test_callproc(self):
        """MySQLCursor object callproc()-method"""
        self.check_method(self.cur, 'callproc')

        self.assertRaises(ValueError, self.cur.callproc, None)
        self.assertRaises(ValueError, self.cur.callproc, 'sp1', None)

        config = tests.get_mysql_config()
        config['get_warnings'] = True
        self.connection = connection.MySQLConnection(**config)
        self._test_callproc_setup(self.connection)
        self.cur = self.connection.cursor()

        if tests.MYSQL_VERSION < (5, 1):
            exp = ('5', '4', '20')
        else:
            exp = (5, 4, 20)
        result = self.cur.callproc('myconnpy_sp_1', (exp[0], exp[1], 0))
        self.assertEqual([], self.cur._stored_results)
        self.assertEqual(exp, result)

        if tests.MYSQL_VERSION < (5, 1):
            exp = ('6', '5', '30')
        else:
            exp = (6, 5, 30)
        result = self.cur.callproc('myconnpy_sp_2', (exp[0], exp[1], 0))
        self.assertTrue(isinstance(self.cur._stored_results, list))
        self.assertEqual(exp, result)

        exp_results = [
            ('abc',),
            ('def',)
        ]
        for result, exp in itertools.izip(self.cur.stored_results(),
                                          iter(exp_results)):
            self.assertEqual(exp, result.fetchone())

        exp = ('ham', 'spam', 'hamspam')
        result = self.cur.callproc('myconnpy_sp_3', (exp[0], exp[1], ''))
        self.assertTrue(isinstance(self.cur._stored_results, list))
        self.assertEqual(exp, result)

        exp = ('ham', 'spam', 'hamspam')
        result = self.cur.callproc('myconnpy_sp_4',
                              (exp[0], (exp[1], 'CHAR'), (0, 'CHAR')))
        self.assertTrue(isinstance(self.cur._stored_results, list))
        self.assertEqual(exp, result)

        self._test_callproc_cleanup(self.connection)
        self.cur.close()

    def test_fetchone(self):
        """MySQLCursor object fetchone()-method"""
        self.check_method(self.cur, 'fetchone')

        self.assertEqual(None, self.cur.fetchone())

        self.connection = connection.MySQLConnection(**tests.get_mysql_config())
        self.cur = self.connection.cursor()
        self.cur.execute("SELECT SHA1('myconnpy')")
        exp = (u'c5e24647dbb63447682164d81b34fe493a83610b',)
        self.assertEqual(exp, self.cur.fetchone())
        self.assertEqual(None, self.cur.fetchone())
        self.cur.close()

    def test_fetchmany(self):
        """MySQLCursor object fetchmany()-method"""
        self.check_method(self.cur, 'fetchmany')

        self.assertEqual([], self.cur.fetchmany())

        self.connection = connection.MySQLConnection(**tests.get_mysql_config())
        tbl = 'myconnpy_fetch'
        self._test_execute_setup(self.connection, tbl)
        stmt_insert = (
            "INSERT INTO {table} (col1,col2) "
            "VALUES (%s,%s)".format(table=tbl))
        stmt_select = (
            "SELECT col1,col2 FROM {table} "
            "ORDER BY col1 DESC".format(table=tbl))

        self.cur = self.connection.cursor()
        nrrows = 10
        data = [(i, str(i * 100)) for i in range(0, nrrows)]
        self.cur.executemany(stmt_insert, data)
        self.cur.execute(stmt_select)
        exp = [(9, u'900'), (8, u'800'), (7, u'700'), (6, u'600')]
        rows = self.cur.fetchmany(4)
        self.assertTrue(tests.cmp_result(exp, rows),
                        "Fetching first 4 rows test failed.")
        exp = [(5, u'500'), (4, u'400'), (3, u'300')]
        rows = self.cur.fetchmany(3)
        self.assertTrue(tests.cmp_result(exp, rows),
                        "Fetching next 3 rows test failed.")
        exp = [(2, u'200'), (1, u'100'), (0, u'0')]
        rows = self.cur.fetchmany(3)
        self.assertTrue(tests.cmp_result(exp, rows),
                        "Fetching next 3 rows test failed.")
        self.assertEqual([], self.cur.fetchmany())
        self._test_execute_cleanup(self.connection, tbl)
        self.cur.close()

    def test_fetchall(self):
        """MySQLCursor object fetchall()-method"""
        self.check_method(self.cur, 'fetchall')

        self.assertRaises(errors.InterfaceError, self.cur.fetchall)

        self.connection = connection.MySQLConnection(**tests.get_mysql_config())
        tbl = 'myconnpy_fetch'
        self._test_execute_setup(self.connection, tbl)
        stmt_insert = (
            "INSERT INTO {table} (col1,col2) "
            "VALUES (%s,%s)".format(table=tbl))
        stmt_select = (
            "SELECT col1,col2 FROM {table} "
            "ORDER BY col1 ASC".format(table=tbl))

        self.cur = self.connection.cursor()
        self.cur.execute("SELECT * FROM {table}".format(table=tbl))
        self.assertEqual([], self.cur.fetchall(),
                         "fetchall() with empty result should return []")
        nrrows = 10
        data = [(i, str(i * 100)) for i in range(0, nrrows)]
        self.cur.executemany(stmt_insert, data)
        self.cur.execute(stmt_select)
        self.assertTrue(tests.cmp_result(data, self.cur.fetchall()),
                        "Fetching all rows failed.")
        self.assertEqual(None, self.cur.fetchone())
        self._test_execute_cleanup(self.connection, tbl)
        self.cur.close()

    def test_raise_on_warning(self):
        self.connection = connection.MySQLConnection(**tests.get_mysql_config())
        self.connection.raise_on_warnings = True
        self.cur = self.connection.cursor()
        try:
            self.cur.execute("SELECT 'a' + 'b'")
            self.cur.fetchall()
        except errors.Error:
            pass
        else:
            self.fail("Did not get exception while raising warnings.")

    def test__unicode__(self):
        """MySQLCursor object __unicode__()-method"""
        self.assertEqual("MySQLCursor: (Nothing executed yet)",
                         str(self.cur.__unicode__()))

        self.connection = connection.MySQLConnection(**tests.get_mysql_config())
        self.cur = self.connection.cursor()
        self.cur.execute("SELECT VERSION()")
        self.cur.fetchone()
        self.assertEqual("MySQLCursor: SELECT VERSION()",
                         str(self.cur.__unicode__()))
        stmt = "SELECT VERSION(),USER(),CURRENT_TIME(),NOW(),SHA1('myconnpy')"
        self.cur.execute(stmt)
        self.cur.fetchone()
        self.assertEqual("MySQLCursor: {0}..".format(stmt[:30]),
                         str(self.cur.__unicode__()))
        self.cur.close()

    def test__str__(self):
        self.assertEqual("'MySQLCursor: (Nothing executed yet)'",
                         self.cur.__str__())

    def test_column_names(self):
        self.cnx = connection.MySQLConnection(**tests.get_mysql_config())
        self.cur = self.cnx.cursor()
        stmt = "SELECT NOW() as now, 'The time' as label, 123 FROM dual"
        exp = (u'now', u'label', u'123')
        self.cur.execute(stmt)
        self.cur.fetchone()
        self.assertEqual(exp, self.cur.column_names)
        self.cur.close()

    def test_statement(self):
        self.cur = cursor.MySQLCursor()
        exp = 'SELECT * FROM ham'
        self.cur._executed = exp
        self.assertEqual(exp, self.cur.statement)
        self.cur._executed = '  ' + exp + '    '
        self.assertEqual(exp, self.cur.statement)

    def test_with_rows(self):
        self.cur = cursor.MySQLCursor()
        self.assertFalse(self.cur.with_rows)
        self.cur._description = ('ham', 'spam')
        self.assertTrue(self.cur.with_rows)


class MySQLCursorBufferedTests(tests.TestsCursor):

    def setUp(self):
        self.cur = cursor.MySQLCursorBuffered(connection=None)
        self.connection = None

    def test_init(self):
        """MySQLCursorBuffered object init"""
        try:
            cur = cursor.MySQLCursorBuffered(connection=None)
        except (SyntaxError, TypeError) as err:
            self.fail("Failed initializing MySQLCursorBuffered; {0}".format(
                err))
        else:
            cur.close()

        self.assertRaises(errors.InterfaceError, cursor.MySQLCursorBuffered,
                          connection='foo')

    def test__next_row(self):
        """MySQLCursorBuffered object _next_row-attribute"""
        self.check_attr(self.cur, '_next_row', 0)

    def test__rows(self):
        """MySQLCursorBuffered object _rows-attribute"""
        self.check_attr(self.cur, '_rows', None)

    def test_execute(self):
        """MySQLCursorBuffered object execute()-method
        """
        self.check_method(self.cur, 'execute')

        self.assertEqual(None, self.cur.execute(None, None))

        config = tests.get_mysql_config()
        config['buffered'] = True
        config['get_warnings'] = True
        self.connection = connection.MySQLConnection(**config)
        self.cur = self.connection.cursor()

        self.assertEqual(True, isinstance(self.cur, cursor.MySQLCursorBuffered))

        self.cur.execute("SELECT 1")
        self.assertEqual([('1',)], self.cur._rows)

    def test_raise_on_warning(self):
        config = tests.get_mysql_config()
        config['buffered'] = True
        config['raise_on_warnings'] = True
        self.connection = connection.MySQLConnection(**config)
        self.cur = self.connection.cursor()
        try:
            self.cur.execute("SELECT 'a' + 'b'")
        except errors.Error:
            pass
        else:
            self.fail("Did not get exception while raising warnings.")

    def test_with_rows(self):
        cur = cursor.MySQLCursorBuffered()
        self.assertFalse(cur.with_rows)
        cur._rows = [('ham',)]
        self.assertTrue(cur.with_rows)


class MySQLCursorRawTests(tests.TestsCursor):

    def setUp(self):
        config = tests.get_mysql_config()
        config['raw'] = True

        self.connection = connection.MySQLConnection(**config)
        self.cur = self.connection.cursor()

    def tearDown(self):
        self.cur.close()
        self.connection.close()

    def test_fetchone(self):
        self.check_method(self.cur, 'fetchone')

        self.assertEqual(None, self.cur.fetchone())

        self.cur.execute("SELECT 1, 'string', MAKEDATE(2010,365), 2.5")
        exp = ('1', 'string', '2010-12-31', '2.5')
        self.assertEqual(exp, self.cur.fetchone())


class MySQLCursorRawBufferedTests(tests.TestsCursor):

    def setUp(self):
        config = tests.get_mysql_config()
        config['raw'] = True
        config['buffered'] = True

        self.connection = connection.MySQLConnection(**config)
        self.cur = self.connection.cursor()

    def tearDown(self):
        self.cur.close()
        self.connection.close()

    def test_fetchone(self):
        self.check_method(self.cur, 'fetchone')

        self.assertEqual(None, self.cur.fetchone())

        self.cur.execute("SELECT 1, 'string', MAKEDATE(2010,365), 2.5")
        exp = ('1', 'string', '2010-12-31', '2.5')
        self.assertEqual(exp, self.cur.fetchone())

    def test_fetchall(self):
        self.check_method(self.cur, 'fetchall')

        self.assertRaises(errors.InterfaceError, self.cur.fetchall)

        self.cur.execute("SELECT 1, 'string', MAKEDATE(2010,365), 2.5")
        exp = [('1', 'string', '2010-12-31', '2.5')]
        self.assertEqual(exp, self.cur.fetchall())


class MySQLCursorPreparedTests(tests.TestsCursor):

    def setUp(self):
        config = tests.get_mysql_config()
        config['raw'] = True
        config['buffered'] = True
        self.cnx = connection.MySQLConnection(**config)

    def test_callproc(self):
        cur = self.cnx.cursor(cursor_class=cursor.MySQLCursorPrepared)
        self.assertRaises(errors.NotSupportedError, cur.callproc)

    def test_close(self):
        cur = self.cnx.cursor(cursor_class=cursor.MySQLCursorPrepared)
        cur.close()
        self.assertEqual(None, cur._prepared)

    def test_fetch_row(self):
        cur = self.cnx.cursor(cursor_class=cursor.MySQLCursorPrepared)
        self.assertEqual(None, cur._fetch_row())
        cur._description = [('c1', 5, None, None, None, None, 1, 128)]

        # Monkey patch the get_row method of the connection for testing
        def _get_row(binary, columns):  # pylint: disable=W0613
            try:
                row = self.cnx._test_fetch_row[0]
                self.cnx._test_fetch_row = self.cnx._test_fetch_row[1:]
            except IndexError:
                return None
            return row
        self.cnx.get_row = _get_row

        eof_info = {'status_flag': 0, 'warning_count': 2}
        self.cnx.unread_result = True

        self.cnx._test_fetch_row = [('1', None), (None, eof_info)]
        self.assertEqual('1', cur._fetch_row())
        self.assertEqual((None, None), cur._nextrow)
        self.assertEqual(eof_info['warning_count'], cur._warning_count)

        cur._reset_result()
        self.cnx.unread_result = True
        self.cnx._test_fetch_row = [(None, eof_info)]
        self.assertEqual(None, cur._fetch_row())
        self.assertEqual((None, None), cur._nextrow)
        self.assertEqual(eof_info['warning_count'], cur._warning_count)

    def test_execute(self):
        cur = self.cnx.cursor(cursor_class=cursor.MySQLCursorPrepared)
        cur2 = self.cnx.cursor(cursor_class=cursor.MySQLCursorPrepared)

        # No 1
        stmt = "SELECT (? * 2) AS c1"
        cur.execute(stmt, (5,))
        self.assertEqual(stmt, cur._executed)
        exp = {
            'num_params': 1, 'statement_id': 1,
            'parameters': [('?', 253, None, None, None, None, 1, 128)],
            'warning_count': 0, 'num_columns': 1,
            'columns': [('c1', 5, None, None, None, None, 1, 128)]
        }
        self.assertEqual(exp, cur._prepared)

        # No 2
        stmt = "SELECT (? * 3) AS c2"
        # first, execute should fail, because unread results of No 1
        self.assertRaises(errors.InternalError, cur2.execute, stmt)
        cur.fetchall()
        # We call with wrong number of values for paramaters
        self.assertRaises(errors.ProgrammingError, cur2.execute, stmt, (1, 3))
        cur2.execute(stmt, (5,))
        self.assertEqual(stmt, cur2._executed)
        exp = {
            'num_params': 1, 'statement_id': 2,
            'parameters': [('?', 253, None, None, None, None, 1, 128)],
            'warning_count': 0, 'num_columns': 1,
            'columns': [('c2', 5, None, None, None, None, 1, 128)]
        }
        self.assertEqual(exp, cur2._prepared)
        self.assertEqual([(15,)], cur2.fetchall())

        # No 3
        data = (3, 4)
        exp = [(5.0,)]
        stmt = "SELECT SQRT(POW(?, 2) + POW(?, 2)) AS hypotenuse"
        cur.execute(stmt, data)
        self.assertEqual(3, cur._prepared['statement_id'])
        self.assertEqual(exp, cur.fetchall())

        # Execute the already prepared statement
        data = (4, 5)
        exp = (6.4031242374328485,)
        cur.execute(stmt, data)
        self.assertEqual(3, cur._prepared['statement_id'])
        self.assertEqual(exp, cur.fetchone())

    def test_executemany(self):
        cur = self.cnx.cursor(cursor_class=cursor.MySQLCursorPrepared)

        self.assertEqual(None, cur.executemany(None, []))

        self.assertRaises(errors.InterfaceError, cur.executemany,
                          'ham', None)
        self.assertRaises(errors.ProgrammingError, cur.executemany,
                          'ham', 'ham')
        self.assertEqual(None, cur.executemany('ham', []))
        self.assertRaises(errors.ProgrammingError, cur.executemany,
                          'ham', ['ham'])

        cur.executemany("SELECT SHA1(%s)", [('ham',), ('bar',)])
        self.assertEqual(None, cur.fetchone())
        cur.close()

        cur = self.cnx.cursor(cursor_class=cursor.MySQLCursorPrepared)

        tbl = 'myconnpy_cursor'
        self._test_execute_setup(self.cnx, tbl)
        stmt_insert = "INSERT INTO {table} (col1,col2) VALUES (%s, %s)".format(
            table=tbl)
        stmt_select = "SELECT col1,col2 FROM {table} ORDER BY col1".format(
            table=tbl)

        cur.executemany(stmt_insert, [(1, 100), (2, 200), (3, 300)])
        self.assertEqual(3, cur.rowcount)

        cur.executemany("SELECT %s", [('h',), ('a',), ('m',)])
        self.assertEqual(3, cur.rowcount)

        cur.execute(stmt_select)
        self.assertEqual([(1, u'100'), (2, u'200'), (3, u'300')],
                         cur.fetchall(), "Multi insert test failed")

        data = [(2,), (3,)]
        stmt = "DELETE FROM {table} WHERE col1 = %s".format(table=tbl)
        cur.executemany(stmt, data)
        self.assertEqual(2, cur.rowcount)

        self._test_execute_cleanup(self.cnx, tbl)
        cur.close()

    def test_fetchone(self):
        cur = self.cnx.cursor(cursor_class=cursor.MySQLCursorPrepared)

        def _fetch_row():
            try:
                row = cur._test_fetch_row[0]
                cur._test_fetch_row = cur._test_fetch_row[1:]
            except IndexError:
                return None
            return row
        cur._fetch_row = _fetch_row

        cur._test_fetch_row = [('ham',)]
        self.assertEqual(('ham',), cur.fetchone())
        self.assertEqual(None, cur.fetchone())

    def test_fetchmany(self):
        cur = self.cnx.cursor(cursor_class=cursor.MySQLCursorPrepared)

        def _fetch_row():
            try:
                row = cur._test_fetch_row[0]
                cur._test_fetch_row = cur._test_fetch_row[1:]
            except IndexError:
                return None
            return row
        cur._fetch_row = _fetch_row

        rows = [(1, 100), (2, 200), (3, 300)]
        cur._test_fetch_row = rows
        self.cnx.unread_result = True
        self.assertEqual(rows[0:2], cur.fetchmany(2))
        self.assertEqual([rows[2]], cur.fetchmany(2))
        self.assertEqual([], cur.fetchmany())

    def test_fetchall(self):
        cur = self.cnx.cursor(cursor_class=cursor.MySQLCursorPrepared)

        def _get_rows(binary, columns):  # pylint: disable=W0613
            self.unread_result = False  # pylint: disable=W0201
            return (
                self.cnx._test_fetch_row,
                {'status_flag': 0, 'warning_count': 3}
            )
        self.cnx.get_rows = _get_rows

        rows = [(1, 100), (2, 200), (3, 300)]
        self.cnx._test_fetch_row = rows
        self.cnx.unread_result = True

        self.assertEqual(rows, cur.fetchall())
        self.assertEqual(len(rows), cur._rowcount)
        self.assertEqual(3, cur._warning_count)
        self.assertRaises(errors.InterfaceError, cur.fetchall)
