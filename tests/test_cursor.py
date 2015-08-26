# -*- coding: utf-8 -*-
# Copyright (c) 2009, 2015, Oracle and/or its affiliates. All rights reserved.

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

"""Test module for bugs

Bug test cases specific to a particular Python (major) version are loaded
from py2.bugs or py3.bugs.

This module was originally located in python2/tests and python3/tests. It
should contain bug test cases which work for both Python v2 and v3.

Whenever a bug is bout to a specific Python version, put the test cases
in tests/py2/bugs.py or tests/py3/bugs.py. It might be that these files need
to be created first.
"""

import datetime
from collections import namedtuple
from decimal import Decimal
import re
import time

from . import PY2
import tests
from mysql.connector import (connection, cursor, errors)


class CursorModule(tests.MySQLConnectorTests):

    """
    Tests for the cursor module functions and attributes
    """

    def test_RE_SQL_INSERT_VALUES(self):
        regex = cursor.RE_SQL_INSERT_VALUES

        cases = [
            ("(%s, %s)",
             "INSERT INTO t1 VALUES (%s, %s)"),
            ("( %s, \n  %s   )",
             "INSERT INTO t1 VALUES  ( %s, \n  %s   )"),
            ("(%(c1)s, %(c2)s)",
             "INSERT INTO t1 VALUES (%(c1)s, %(c2)s)"),
            ("(\n%(c1)s\n, \n%(c2)s\n)",
             "INSERT INTO t1 VALUES \n(\n%(c1)s\n, \n%(c2)s\n)"),
            ("(  %(c1)s  ,  %(c2)s  )",
             "INSERT INTO t1 VALUES   (  %(c1)s  ,  %(c2)s  ) ON DUPLICATE"),
            ("(%s, %s, NOW())",
             "INSERT INTO t1 VALUES (%s, %s, NOW())"),
            ("(%s, CONCAT('a', 'b'), %s, NOW())",
             "INSERT INTO t1 VALUES (%s, CONCAT('a', 'b'), %s, NOW())"),
            ("( NOW(),  %s, \n, CONCAT('a', 'b'), %s   )",
             "INSERT INTO t1 VALUES "
             " ( NOW(),  %s, \n, CONCAT('a', 'b'), %s   )"),
            ("(%(c1)s, NOW(6), %(c2)s)",
             "INSERT INTO t1 VALUES (%(c1)s, NOW(6), %(c2)s)"),
            ("(\n%(c1)s\n, \n%(c2)s, REPEAT('a', 20)\n)",
             "INSERT INTO t1 VALUES "
             "\n(\n%(c1)s\n, \n%(c2)s, REPEAT('a', 20)\n)"),
            ("(  %(c1)s  ,NOW(),REPEAT('a', 20)\n),  %(c2)s  )",
             "INSERT INTO t1 VALUES "
             " (  %(c1)s  ,NOW(),REPEAT('a', 20)\n),  %(c2)s  ) ON DUPLICATE"),
            ("(  %(c1)s, %(c2)s  )",
             "INSERT INTO `values` VALUES "
             "  (  %(c1)s, %(c2)s  ) ON DUPLICATE"),
        ]

        for exp, stmt in cases:
            self.assertEqual(exp, re.search(regex, stmt).group(1))


class CursorBaseTests(tests.MySQLConnectorTests):

    def setUp(self):
        self.cur = cursor.CursorBase()

    def test___init__(self):
        exp = {
            '_description': None,
            '_rowcount': -1,
            'arraysize': 1,
        }

        for key, value in exp.items():
            self.assertEqual(value, getattr(self.cur, key),
                             msg="Default for '%s' did not match." % key)

    def test_callproc(self):
        """CursorBase object callproc()-method"""
        self.check_method(self.cur, 'callproc')

        try:
            self.cur.callproc('foo', args=(1, 2, 3))
        except (SyntaxError, TypeError):
            self.fail("Cursor callproc(): wrong arguments")

    def test_close(self):
        """CursorBase object close()-method"""
        self.check_method(self.cur, 'close')

    def test_execute(self):
        """CursorBase object execute()-method"""
        self.check_method(self.cur, 'execute')

        try:
            self.cur.execute('select', params=(1, 2, 3))
        except (SyntaxError, TypeError):
            self.fail("Cursor execute(): wrong arguments")

    def test_executemany(self):
        """CursorBase object executemany()-method"""
        self.check_method(self.cur, 'executemany')

        try:
            self.cur.executemany('select', [()])
        except (SyntaxError, TypeError):
            self.fail("Cursor executemany(): wrong arguments")

    def test_fetchone(self):
        """CursorBase object fetchone()-method"""
        self.check_method(self.cur, 'fetchone')

    def test_fetchmany(self):
        """CursorBase object fetchmany()-method"""
        self.check_method(self.cur, 'fetchmany')

        try:
            self.cur.fetchmany(size=1)
        except (SyntaxError, TypeError):
            self.fail("Cursor fetchmany(): wrong arguments")

    def test_fetchall(self):
        """CursorBase object fetchall()-method"""
        self.check_method(self.cur, 'fetchall')

    def test_nextset(self):
        """CursorBase object nextset()-method"""
        self.check_method(self.cur, 'nextset')

    def test_setinputsizes(self):
        """CursorBase object setinputsizes()-method"""
        self.check_method(self.cur, 'setinputsizes')

        try:
            self.cur.setinputsizes((1,))
        except (SyntaxError, TypeError):
            self.fail("CursorBase setinputsizes(): wrong arguments")

    def test_setoutputsize(self):
        """CursorBase object setoutputsize()-method"""
        self.check_method(self.cur, 'setoutputsize')

        try:
            self.cur.setoutputsize(1, column=None)
        except (SyntaxError, TypeError):
            self.fail("CursorBase setoutputsize(): wrong arguments")

    def test_description(self):
        self.assertEqual(None, self.cur.description)
        self.assertEqual(self.cur._description, self.cur.description)
        self.cur._description = 'ham'
        self.assertEqual('ham', self.cur.description)
        if tests.OLD_UNITTEST:
            try:
                self.cur.description = 'spam'
            except AttributeError as err:
                # Exception should be raised
                pass
            else:
                self.fail("AttributeError was not raised")
        else:
            with self.assertRaises(AttributeError):
                self.cur.description = 'spam'

    def test_rowcount(self):
        self.assertEqual(-1, self.cur.rowcount)
        self.assertEqual(self.cur._rowcount, self.cur.rowcount)
        self.cur._rowcount = 2
        self.assertEqual(2, self.cur.rowcount)
        if tests.OLD_UNITTEST:
            try:
                self.cur.description = 'spam'
            except AttributeError as err:
                # Exception should be raised
                pass
            else:
                self.fail("AttributeError was not raised")
        else:
            with self.assertRaises(AttributeError):
                self.cur.rowcount = 3

    def test_last_insert_id(self):
        self.assertEqual(None, self.cur.lastrowid)
        self.assertEqual(self.cur._last_insert_id, self.cur.lastrowid)
        self.cur._last_insert_id = 2
        self.assertEqual(2, self.cur.lastrowid)
        if tests.OLD_UNITTEST:
            try:
                self.cur.description = 'spam'
            except AttributeError as err:
                # Exception should be raised
                pass
            else:
                self.fail("AttributeError was not raised")
        else:
            with self.assertRaises(AttributeError):
                self.cur.lastrowid = 3


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
        self.cnx = connection.MySQLConnection(**tests.get_mysql_config())
        self.cur._set_connection(self.cnx)
        self.cur.close()

    def test__reset_result(self):
        """MySQLCursor object _reset_result()-method"""
        self.check_method(self.cur, '_reset_result')

        def reset(self):
            self._test = "Reset called"
        self.cur.reset = reset.__get__(self.cur, cursor.MySQLCursor)

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
        self.assertEqual('Reset called',
                         self.cur._test)  # pylint: disable=E1103

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

        self.cnx = connection.MySQLConnection(**tests.get_mysql_config())
        self.cur = cursor.MySQLCursor(self.cnx)
        self.assertRaises(StopIteration, self.cur.__next__)
        self.cur.execute("SELECT BINARY 'ham'")
        exp = (b'ham',)
        self.assertEqual(exp, next(self.cur))
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
            int(1281288),
            float(3.14),
            Decimal('3.14'),
            r'back\slash',
            'newline\n',
            'return\r',
            "'single'",
            '"double"',
            'windows\032',
            "Strings are sexy",
            u'\u82b1',
            datetime.datetime(2008, 5, 7, 20, 1, 23),
            datetime.date(2008, 5, 7),
            datetime.time(20, 3, 23),
            st_now,
            datetime.timedelta(hours=40, minutes=30, seconds=12),
        )
        exp = (
            b'NULL',
            b'128',
            b'1281288',
            repr(float(3.14)) if PY2 else b'3.14',
            b"'3.14'",
            b"'back\\\\slash'",
            b"'newline\\n'",
            b"'return\\r'",
            b"'\\'single\\''",
            b'\'\\"double\\"\'',
            b"'windows\\\x1a'",
            b"'Strings are sexy'",
            b"'\xe8\x8a\xb1'",
            b"'2008-05-07 20:01:23'",
            b"'2008-05-07'",
            b"'20:03:23'",
            b"'" + time.strftime('%Y-%m-%d %H:%M:%S', st_now).encode('ascii')
            + b"'",
            b"'40:30:12'",
        )

        self.cnx = connection.MySQLConnection(**tests.get_mysql_config())
        self.cur = self.cnx.cursor()
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
            'c': int(1281288),
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
            'n': datetime.datetime(2008, 5, 7, 20, 1, 23),
            'o': datetime.date(2008, 5, 7),
            'p': datetime.time(20, 3, 23),
            'q': st_now,
            'r': datetime.timedelta(hours=40, minutes=30, seconds=12),
        }
        exp = {
            b'%(a)s': b'NULL',
            b'%(b)s': b'128',
            b'%(c)s': b'1281288',
            b'%(d)s': repr(float(3.14)) if PY2 else b'3.14',
            b'%(e)s': b"'3.14'",
            b'%(f)s': b"'back\\\\slash'",
            b'%(g)s': b"'newline\\n'",
            b'%(h)s': b"'return\\r'",
            b'%(i)s': b"'\\'single\\''",
            b'%(j)s': b'\'\\"double\\"\'',
            b'%(k)s': b"'windows\\\x1a'",
            b'%(l)s': b"'Strings are sexy'",
            b'%(m)s': b"'\xe8\x8a\xb1'",
            b'%(n)s': b"'2008-05-07 20:01:23'",
            b'%(o)s': b"'2008-05-07'",
            b'%(p)s': b"'20:03:23'",
            b'%(q)s': b"'" +
            time.strftime('%Y-%m-%d %H:%M:%S', st_now).encode('ascii')
            + b"'",
            b'%(r)s': b"'40:30:12'",
        }

        self.cnx = connection.MySQLConnection(**tests.get_mysql_config())
        self.cur = self.cnx.cursor()
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
        self.cnx = connection.MySQLConnection(**config)
        self.cur = self.cnx.cursor()
        self.cur.execute("SELECT 'a' + 'b'")
        self.cur.fetchone()
        exp = [
            ('Warning', 1292, "Truncated incorrect DOUBLE value: 'a'"),
            ('Warning', 1292, "Truncated incorrect DOUBLE value: 'b'")
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
        config = tests.get_mysql_config()
        config['get_warnings'] = True
        self.cnx = connection.MySQLConnection(**config)
        self.cur = self.cnx.cursor()
        self.cur._handle_noresultset(data)
        self.assertEqual(data['affected_rows'], self.cur.rowcount)
        self.assertEqual(data['insert_id'], self.cur.lastrowid)
        self.assertEqual(data['warning_count'], self.cur._warning_count)

    def test__handle_result(self):
        """MySQLCursor object _handle_result()-method"""
        self.cnx = connection.MySQLConnection(**tests.get_mysql_config())
        self.cur = self.cnx.cursor()

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
        self.cnx = connection.MySQLConnection(**config)
        self.cur = self.cnx.cursor()

        self.assertRaises(errors.ProgrammingError, self.cur.execute,
                          'SELECT %s,%s,%s', ('foo', 'bar',))

        self.cur.execute("SELECT 'a' + 'b'")
        self.cur.fetchone()
        exp = [
            ('Warning', 1292, "Truncated incorrect DOUBLE value: 'a'"),
            ('Warning', 1292, "Truncated incorrect DOUBLE value: 'b'")
        ]
        self.assertTrue(tests.cmp_result(exp, self.cur._warnings))

        self.cur.execute("SELECT BINARY 'ham'")
        exp = [(b'ham',)]
        self.assertEqual(exp, self.cur.fetchall())
        self.cur.close()

        tbl = 'myconnpy_cursor'
        self._test_execute_setup(self.cnx, tbl)
        stmt_insert = "INSERT INTO {0} (col1,col2) VALUES (%s,%s)".format(tbl)
        self.cur = self.cnx.cursor()
        res = self.cur.execute(stmt_insert, (1, 100))
        self.assertEqual(None, res, "Return value of execute() is wrong.")
        stmt_select = "SELECT col1,col2 FROM {0} ORDER BY col1".format(tbl)
        self.cur.execute(stmt_select)
        self.assertEqual([(1, '100')],
                         self.cur.fetchall(), "Insert test failed")

        data = {'id': 2}
        stmt = "SELECT col1,col2 FROM {0} WHERE col1 <= %(id)s".format(tbl)
        self.cur.execute(stmt, data)
        self.assertEqual([(1, '100')], self.cur.fetchall())

        self._test_execute_cleanup(self.cnx, tbl)
        self.cur.close()

        self.cur = self.cnx.cursor()
        self.cur.execute("DROP PROCEDURE IF EXISTS multi_results")
        procedure = (
            "CREATE PROCEDURE multi_results () "
            "BEGIN SELECT 1; SELECT 'ham'; END"
        )
        self.cur.execute(procedure)
        exp_stmt = "CALL multi_results()"
        if not PY2:
            exp_stmt = b"CALL multi_results()"
        exp_result = [[(1,)], [(u'ham',)]]
        results = []
        for result in self.cur.execute(exp_stmt, multi=True):
            if result.with_rows:
                self.assertEqual(exp_stmt, result._executed)
                results.append(result.fetchall())

        self.assertEqual(exp_result, results)
        self.cur.execute("DROP PROCEDURE multi_results")

    def test_executemany(self):
        """MySQLCursor object executemany()-method"""
        self.check_method(self.cur, 'executemany')

        self.assertEqual(None, self.cur.executemany(None, []))

        config = tests.get_mysql_config()
        config['get_warnings'] = True
        self.cnx = connection.MySQLConnection(**config)
        self.cur = self.cnx.cursor()
        self.assertRaises(errors.ProgrammingError, self.cur.executemany,
                          'programming error with string', 'foo')
        self.assertRaises(errors.ProgrammingError, self.cur.executemany,
                          'programming error with 1 element list', ['foo'])
        self.assertEqual(None, self.cur.executemany('empty params', []))
        self.assertEqual(None, self.cur.executemany('params is None', None))
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
        self._test_execute_setup(self.cnx, tbl)
        stmt_insert = "INSERT INTO {0} (col1,col2) VALUES (%s,%s)".format(tbl)
        stmt_select = "SELECT col1,col2 FROM {0} ORDER BY col1".format(tbl)

        self.cur = self.cnx.cursor()

        res = self.cur.executemany(stmt_insert, [(1, 100), (2, 200), (3, 300)])
        self.assertEqual(3, self.cur.rowcount)

        res = self.cur.executemany("SELECT %s", [('f',), ('o',), ('o',)])
        self.assertEqual(3, self.cur.rowcount)

        data = [{'id': 2}, {'id': 3}]
        stmt = "SELECT * FROM {0} WHERE col1 <= %(id)s".format(tbl)
        self.cur.executemany(stmt, data)
        self.assertEqual(5, self.cur.rowcount)

        self.cur.execute(stmt_select)
        self.assertEqual([(1, '100'), (2, '200'), (3, '300')],
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
        self.assertEqual([(4, '100'), (5, '200'), (6, '300')],
                         self.cur.fetchall(), "Multi insert test failed")

        stmt = "TRUNCATE TABLE {0}".format(tbl)
        self.cur.execute(stmt)

        stmt = (
            "INSERT INTO/*comment*/{0}(col1,col2)VALUES"
            "/*comment*/(%s,'/*100*/')/*comment()*/ON DUPLICATE KEY UPDATE "
            "col1 = VALUES(col1)"
        ).format(tbl)

        self.cur.executemany(stmt, [(4,), (5,)])
        self.assertEqual(2, self.cur.rowcount)

        self.cur.execute(stmt_select)
        self.assertEqual([(4, '/*100*/'), (5, '/*100*/')],
                         self.cur.fetchall(), "Multi insert test failed")
        self._test_execute_cleanup(self.cnx, tbl)
        self.cur.close()

    def test_fetchwarnings(self):
        """MySQLCursor object fetchwarnings()-method"""
        self.check_method(self.cur, 'fetchwarnings')

        self.assertEqual(
            None, self.cur.fetchwarnings(),
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
        self.assertEqual('abc', next(self.cur.stored_results()))
        try:
            _ = next(self.cur.stored_results())
        except StopIteration:
            pass
        except:
            self.fail("StopIteration not raised")

    def _test_callproc_setup(self, cnx):

        self._test_callproc_cleanup(cnx)
        stmt_create1 = (
            "CREATE PROCEDURE myconnpy_sp_1 "
            "(IN pFac1 INT, IN pFac2 INT, OUT pProd INT) "
            "BEGIN SET pProd := pFac1 * pFac2; END;")

        stmt_create2 = (
            "CREATE PROCEDURE myconnpy_sp_2 "
            "(IN pFac1 INT, IN pFac2 INT, OUT pProd INT) "
            "BEGIN SELECT 'abc'; SELECT 'def'; SET pProd := pFac1 * pFac2; "
            "END;")

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
            cur = cnx.cursor()
            cur.execute(stmt_create1)
            cur.execute(stmt_create2)
            cur.execute(stmt_create3)
            cur.execute(stmt_create4)
        except errors.Error as err:
            self.fail("Failed setting up test stored routine; {0}".format(err))
        cur.close()

    def _test_callproc_cleanup(self, cnx):

        sp_names = ('myconnpy_sp_1', 'myconnpy_sp_2', 'myconnpy_sp_3',
                    'myconnpy_sp_4')
        stmt_drop = "DROP PROCEDURE IF EXISTS {procname}"

        try:
            cur = cnx.cursor()
            for sp_name in sp_names:
                cur.execute(stmt_drop.format(procname=sp_name))
        except errors.Error as err:
            self.fail(
                "Failed cleaning up test stored routine; {0}".format(err))
        cur.close()

    def test_callproc(self):
        """MySQLCursor object callproc()-method"""
        self.check_method(self.cur, 'callproc')

        self.assertRaises(ValueError, self.cur.callproc, None)
        self.assertRaises(ValueError, self.cur.callproc, 'sp1', None)

        config = tests.get_mysql_config()
        config['get_warnings'] = True
        self.cnx = connection.MySQLConnection(**config)
        self._test_callproc_setup(self.cnx)
        self.cur = self.cnx.cursor()

        if tests.MYSQL_VERSION < (5, 1):
            exp = ('5', '4', b'20')
        else:
            exp = (5, 4, 20)
        result = self.cur.callproc('myconnpy_sp_1', (exp[0], exp[1], 0))
        self.assertEqual([], self.cur._stored_results)
        self.assertEqual(exp, result)

        if tests.MYSQL_VERSION < (5, 1):
            exp = ('6', '5', b'30')
        else:
            exp = (6, 5, 30)
        result = self.cur.callproc('myconnpy_sp_2', (exp[0], exp[1], 0))
        self.assertTrue(isinstance(self.cur._stored_results, list))
        self.assertEqual(exp, result)

        exp_results = [
            ('abc',),
            ('def',)
        ]
        for result, exp in zip(self.cur.stored_results(),
                               iter(exp_results)):
            self.assertEqual(exp, result.fetchone())

        exp = ('ham', 'spam', 'hamspam')
        result = self.cur.callproc('myconnpy_sp_3', (exp[0], exp[1], 0))
        self.assertTrue(isinstance(self.cur._stored_results, list))
        self.assertEqual(exp, result)

        exp = ('ham', 'spam', 'hamspam')
        result = self.cur.callproc('myconnpy_sp_4',
                              (exp[0], (exp[1], 'CHAR'), (0, 'CHAR')))
        self.assertTrue(isinstance(self.cur._stored_results, list))
        self.assertEqual(exp, result)

        self._test_callproc_cleanup(self.cnx)
        self.cur.close()

    def test_fetchone(self):
        """MySQLCursor object fetchone()-method"""
        self.check_method(self.cur, 'fetchone')

        self.assertEqual(None, self.cur.fetchone())

        self.cnx = connection.MySQLConnection(**tests.get_mysql_config())
        self.cur = self.cnx.cursor()
        self.cur.execute("SELECT BINARY 'ham'")
        exp = (b'ham',)
        self.assertEqual(exp, self.cur.fetchone())
        self.assertEqual(None, self.cur.fetchone())
        self.cur.close()

    def test_fetchmany(self):
        """MySQLCursor object fetchmany()-method"""
        self.check_method(self.cur, 'fetchmany')

        self.assertEqual([], self.cur.fetchmany())

        self.cnx = connection.MySQLConnection(**tests.get_mysql_config())
        tbl = 'myconnpy_fetch'
        self._test_execute_setup(self.cnx, tbl)
        stmt_insert = (
            "INSERT INTO {table} (col1,col2) "
            "VALUES (%s,%s)".format(table=tbl))
        stmt_select = (
            "SELECT col1,col2 FROM {table} "
            "ORDER BY col1 DESC".format(table=tbl))

        self.cur = self.cnx.cursor()
        nrrows = 10
        data = [(i, str(i * 100)) for i in range(0, nrrows)]
        self.cur.executemany(stmt_insert, data)
        self.cur.execute(stmt_select)
        exp = [(9, '900'), (8, '800'), (7, '700'), (6, '600')]
        rows = self.cur.fetchmany(4)
        self.assertTrue(tests.cmp_result(exp, rows),
                        "Fetching first 4 rows test failed.")
        exp = [(5, '500'), (4, '400'), (3, '300')]
        rows = self.cur.fetchmany(3)
        self.assertTrue(tests.cmp_result(exp, rows),
                        "Fetching next 3 rows test failed.")
        exp = [(2, '200'), (1, '100'), (0, '0')]
        rows = self.cur.fetchmany(3)
        self.assertTrue(tests.cmp_result(exp, rows),
                        "Fetching next 3 rows test failed.")
        self.assertEqual([], self.cur.fetchmany())
        self._test_execute_cleanup(self.cnx, tbl)
        self.cur.close()

    def test_fetchall(self):
        """MySQLCursor object fetchall()-method"""
        self.check_method(self.cur, 'fetchall')

        self.assertRaises(errors.InterfaceError, self.cur.fetchall)

        self.cnx = connection.MySQLConnection(**tests.get_mysql_config())
        tbl = 'myconnpy_fetch'
        self._test_execute_setup(self.cnx, tbl)
        stmt_insert = (
            "INSERT INTO {table} (col1,col2) "
            "VALUES (%s,%s)".format(table=tbl))
        stmt_select = (
            "SELECT col1,col2 FROM {table} "
            "ORDER BY col1 ASC".format(table=tbl))

        self.cur = self.cnx.cursor()
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
        self._test_execute_cleanup(self.cnx, tbl)
        self.cur.close()

    def test_raise_on_warning(self):
        self.cnx = connection.MySQLConnection(**tests.get_mysql_config())
        self.cnx.raise_on_warnings = True
        self.cur = self.cnx.cursor()
        try:
            self.cur.execute("SELECT 'a' + 'b'")
            self.cur.fetchall()
        except errors.Error:
            pass
        else:
            self.fail("Did not get exception while raising warnings.")

    def test__str__(self):
        """MySQLCursor object __str__()-method"""
        self.assertEqual("MySQLCursor: (Nothing executed yet)",
                         self.cur.__str__())

        self.cnx = connection.MySQLConnection(**tests.get_mysql_config())
        self.cur = self.cnx.cursor()
        self.cur.execute("SELECT VERSION()")
        self.cur.fetchone()
        self.assertEqual("MySQLCursor: SELECT VERSION()",
                         self.cur.__str__())
        stmt = "SELECT VERSION(),USER(),CURRENT_TIME(),NOW(),SHA1('myconnpy')"
        self.cur.execute(stmt)
        self.cur.fetchone()
        self.assertEqual("MySQLCursor: {0}..".format(stmt[:40]),
                         self.cur.__str__())
        self.cur.close()

    def test_column_names(self):
        self.cnx = connection.MySQLConnection(**tests.get_mysql_config())
        self.cur = self.cnx.cursor()
        stmt = "SELECT NOW() as now, 'The time' as label, 123 FROM dual"
        exp = ('now', 'label', '123')
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
        self.cur._executed = b'SELECT * FROM ham'
        self.assertEqual(exp, self.cur.statement)

    def test_with_rows(self):
        self.cur = cursor.MySQLCursor()
        self.assertFalse(self.cur.with_rows)
        self.cur._description = ('ham', 'spam')
        self.assertTrue(self.cur.with_rows)

    def test_unicode(self):
        self.cnx = connection.MySQLConnection(**tests.get_mysql_config())
        self.cur = self.cnx.cursor()
        stmt = "DROP TABLE IF EXISTS test_unicode"
        self.cur.execute(stmt)

        stmt = (
            "CREATE TABLE test_unicode(`aé` INTEGER AUTO_INCREMENT, "
            "`測試` INTEGER, PRIMARY KEY (`aé`))ENGINE=InnoDB"
        )
        self.cur.execute(stmt)
        stmt = "INSERT INTO test_unicode(`aé`, `測試`) VALUES (%(aé)s, %(測試)s)"
        params = {'aé': 1, '測試': 2}
        self.cur.execute(stmt, params)

        stmt = "SELECT * FROM test_unicode"
        self.cur.execute(stmt)
        exp = [(1, 2)]
        self.assertEqual(exp, self.cur.fetchall())

        stmt = "DROP TABLE IF EXISTS test_unicode"
        self.cur.execute(stmt)


class MySQLCursorBufferedTests(tests.TestsCursor):

    def setUp(self):
        self.cur = cursor.MySQLCursorBuffered(connection=None)
        self.cnx = None

    def tearDown(self):
        if self.cnx:
            self.cnx.close()

    def test_init(self):
        """MySQLCursorBuffered object init"""
        try:
            cur = cursor.MySQLCursorBuffered(connection=None)
        except (SyntaxError, TypeError) as err:
            self.fail("Failed initializing MySQLCursorBuffered; {0}".format(
                err))
        else:
            cur.close()
        self.assertRaises(errors.InterfaceError,
                          cursor.MySQLCursorBuffered, connection='foo')

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
        self.cnx = connection.MySQLConnection(**config)
        self.cur = self.cnx.cursor()

        self.assertEqual(True,
                         isinstance(self.cur, cursor.MySQLCursorBuffered))

        self.cur.execute("SELECT 1")
        self.assertEqual([(b'1',)], self.cur._rows)

    def test_raise_on_warning(self):
        config = tests.get_mysql_config()
        config['buffered'] = True
        config['raise_on_warnings'] = True
        self.cnx = connection.MySQLConnection(**config)
        self.cur = self.cnx.cursor()
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

        self.cnx = connection.MySQLConnection(**config)
        self.cur = self.cnx.cursor()

    def tearDown(self):
        self.cur.close()
        self.cnx.close()

    def test_fetchone(self):
        self.check_method(self.cur, 'fetchone')

        self.assertEqual(None, self.cur.fetchone())

        self.cur.execute("SELECT 1, 'string', MAKEDATE(2010,365), 2.5")
        exp = (b'1', b'string', b'2010-12-31', b'2.5')
        self.assertEqual(exp, self.cur.fetchone())


class MySQLCursorRawBufferedTests(tests.TestsCursor):

    def setUp(self):
        config = tests.get_mysql_config()
        config['raw'] = True
        config['buffered'] = True

        self.cnx = connection.MySQLConnection(**config)
        self.cur = self.cnx.cursor()

    def tearDown(self):
        self.cur.close()
        self.cnx.close()

    def test_fetchone(self):
        self.check_method(self.cur, 'fetchone')

        self.assertEqual(None, self.cur.fetchone())

        self.cur.execute("SELECT 1, 'string', MAKEDATE(2010,365), 2.5")
        exp = (b'1', b'string', b'2010-12-31', b'2.5')
        self.assertEqual(exp, self.cur.fetchone())

    def test_fetchall(self):
        self.check_method(self.cur, 'fetchall')

        self.assertRaises(errors.InterfaceError, self.cur.fetchall)

        self.cur.execute("SELECT 1, 'string', MAKEDATE(2010,365), 2.5")
        exp = [(b'1', b'string', b'2010-12-31', b'2.5')]
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

        self.cnx._test_fetch_row = [(b'1', None), (None, eof_info)]
        self.assertEqual(b'1', cur._fetch_row())
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
        self.assertEqual([(1, b'100'), (2, b'200'), (3, b'300')],
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

        cur._test_fetch_row = [(b'ham',)]
        self.assertEqual((b'ham',), cur.fetchone())
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

        rows = [(1, b'100'), (2, b'200'), (3, b'300')]
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


class MySQLCursorDictTests(tests.TestsCursor):

    def setUp(self):
        config = tests.get_mysql_config()

        self.connection = connection.MySQLConnection(**config)
        self.cur = self.connection.cursor(dictionary=True)
        self.cur.execute('DROP TABLE IF EXISTS MySQLCursorDictTests')
        self.cur.execute('CREATE TABLE MySQLCursorDictTests(id INT(10), name '
                         'VARCHAR(20), city VARCHAR(20))')

    def tearDown(self):
        self.cur.execute('DROP TABLE IF EXISTS MySQLCursorDictTests')
        self.cur.close()
        self.connection.close()

    def test_fetchone(self):
        self.check_method(self.cur, 'fetchone')

        self.assertEqual(None, self.cur.fetchone())
        self.cur.execute("INSERT INTO MySQLCursorDictTests VALUES(%s, %s, %s)",
                         (1, 'ham', 'spam'))


        self.cur.execute("SELECT * FROM MySQLCursorDictTests")
        exp = {u'id': 1, u'name': u'ham', u'city': u'spam'}
        self.assertEqual(exp, self.cur.fetchone())


class MySQLCursorBufferedDictTests(tests.TestsCursor):

    def setUp(self):
        config = tests.get_mysql_config()

        self.connection = connection.MySQLConnection(**config)
        self.cur = self.connection.cursor(dictionary=True, buffered=True)
        self.cur.execute('DROP TABLE IF EXISTS MySQLCursorBufferedDictTests')
        self.cur.execute('CREATE TABLE MySQLCursorBufferedDictTests(id INT(10),'
                         'name VARCHAR(20), city VARCHAR(20))')

    def tearDown(self):
        self.cur.close()
        self.connection.close()

    def test_fetchone(self):
        self.check_method(self.cur, 'fetchone')

        self.assertEqual(None, self.cur.fetchone())
        self.cur.execute("INSERT INTO MySQLCursorBufferedDictTests VALUE"
                         "(%s, %s, %s)", (1, 'ham', 'spam'))

        self.cur.execute("SELECT * FROM MySQLCursorBufferedDictTests")
        exp = {u'id': 1, u'name': u'ham', u'city': u'spam'}
        self.assertEqual(exp, self.cur.fetchone())

    def test_fetchall(self):
        self.check_method(self.cur, 'fetchall')

        self.assertRaises(errors.InterfaceError, self.cur.fetchall)
        self.cur.execute("INSERT INTO MySQLCursorBufferedDictTests VALUE"
                         "(%s, %s, %s)", (1, 'ham', 'spam'))

        self.cur.execute("SELECT * FROM MySQLCursorBufferedDictTests")
        exp = [{u'id': 1, u'name': u'ham', u'city': u'spam'}]
        self.assertEqual(exp, self.cur.fetchall())


class MySQLCursorNamedTupleTests(tests.TestsCursor):

    def setUp(self):
        config = tests.get_mysql_config()

        self.connection = connection.MySQLConnection(**config)
        self.cur = self.connection.cursor(named_tuple=True)
        self.cur.execute('DROP TABLE IF EXISTS MySQLCursorNamedTupleTests')
        self.cur.execute('CREATE TABLE MySQLCursorNamedTupleTests(id INT(10),'
                         'name VARCHAR(20), city VARCHAR(20))')

    def tearDown(self):
        self.cur.execute('DROP TABLE IF EXISTS MySQLCursorNamedTupleTests')
        self.cur.close()
        self.connection.close()

    def test_fetchone(self):
        self.check_method(self.cur, 'fetchone')

        self.assertEqual(None, self.cur.fetchone())

        self.cur.execute("INSERT INTO MySQLCursorNamedTupleTests VALUES"
                         "(%s, %s, %s)", (1, 'ham', 'spam'))


        self.cur.execute("SELECT * FROM MySQLCursorNamedTupleTests")
        named_tuple = namedtuple('Row', ['id', 'name', 'city'])
        exp = named_tuple(1, u'ham', u'spam')
        row = self.cur.fetchone()

        self.assertEqual(exp.id, row.id)
        self.assertEqual(exp.name, row.name)
        self.assertEqual(exp.city, row.city)


class MySQLCursorBufferedNamedTupleTests(tests.TestsCursor):

    def setUp(self):
        config = tests.get_mysql_config()

        self.connection = connection.MySQLConnection(**config)
        self.cur = self.connection.cursor(named_tuple=True, buffered=True)
        self.cur.execute('DROP TABLE IF EXISTS '
                         'MySQLCursorBufferedNamedTupleTests')
        self.cur.execute('CREATE TABLE MySQLCursorBufferedNamedTupleTests('
                         'id INT(10), name VARCHAR(20), city VARCHAR(20))')

    def tearDown(self):
        self.cur.close()
        self.connection.close()

    def test_fetchone(self):
        self.check_method(self.cur, 'fetchone')

        self.assertEqual(None, self.cur.fetchone())
        self.cur.execute("INSERT INTO MySQLCursorBufferedNamedTupleTests VALUES"
                         "(%s, %s, %s)", (1, 'ham', 'spam'))


        self.cur.execute("SELECT * FROM MySQLCursorBufferedNamedTupleTests")
        named_tuple = namedtuple('Row', ['id', 'name', 'city'])
        exp = named_tuple(1, u'ham', u'spam')
        row = self.cur.fetchone()

        self.assertEqual(exp.id, row.id)
        self.assertEqual(exp.name, row.name)
        self.assertEqual(exp.city, row.city)

    def test_fetchall(self):
        self.check_method(self.cur, 'fetchall')

        self.assertRaises(errors.InterfaceError, self.cur.fetchall)
        self.cur.execute("INSERT INTO MySQLCursorBufferedNamedTupleTests VALUES"
                         "(%s, %s, %s)", (1, 'ham', 'spam'))


        self.cur.execute("SELECT * FROM MySQLCursorBufferedNamedTupleTests")
        named_tuple = namedtuple('Row', ['id', 'name', 'city'])
        exp = named_tuple(1, u'ham', u'spam')
        row = self.cur.fetchall()

        self.assertEqual(exp.id, row[0].id)
        self.assertEqual(exp.name, row[0].name)
        self.assertEqual(exp.city, row[0].city)
