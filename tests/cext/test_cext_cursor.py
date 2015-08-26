# -*- coding: utf-8 -*-
# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2014, 2015, Oracle and/or its affiliates. All rights reserved.

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

"""Testing the C Extension cursors
"""

import logging
import unittest

from mysql.connector import errors, errorcode

import tests

try:
    from _mysql_connector import (
        MySQL, MySQLError, MySQLInterfaceError,
    )
except ImportError:
    HAVE_CMYSQL = False
else:
    HAVE_CMYSQL = True

from mysql.connector.connection_cext import CMySQLConnection
from mysql.connector.cursor_cext import (
    CMySQLCursor, CMySQLCursorBuffered, CMySQLCursorRaw
)

LOGGER = logging.getLogger(tests.LOGGER_NAME)

@unittest.skipIf(HAVE_CMYSQL == False, "C Extension not available")
class CExtMySQLCursorTests(tests.CMySQLCursorTests):

    def _get_cursor(self, cnx=None):
        if not cnx:
            cnx = CMySQLConnection(**self.config)
        return CMySQLCursor(connection=cnx)

    def test___init__(self):
        self.assertRaises(errors.InterfaceError, CMySQLCursor, connection='ham')
        cur = self._get_cursor(self.cnx)
        self.assertTrue(hex(id(self.cnx)).upper()[2:]
                        in repr(cur._cnx).upper())

    def test_lastrowid(self):
        cur = self._get_cursor(self.cnx)

        tbl = 'test_lastrowid'
        self.setup_table(self.cnx, tbl)

        cur.execute("INSERT INTO {0} (col1) VALUES (1)".format(tbl))
        self.assertEqual(1, cur.lastrowid)

        cur.execute("INSERT INTO {0} () VALUES ()".format(tbl))
        self.assertEqual(2, cur.lastrowid)

        cur.execute("INSERT INTO {0} () VALUES (),()".format(tbl))
        self.assertEqual(3, cur.lastrowid)

        cur.execute("INSERT INTO {0} () VALUES ()".format(tbl))
        self.assertEqual(5, cur.lastrowid)

    def test__fetch_warnings(self):
        self.cnx.get_warnings = True
        cur = self._get_cursor(self.cnx)

        cur._cnx = None
        self.assertRaises(errors.InterfaceError, cur._fetch_warnings)

        cur = self._get_cursor(self.cnx)

        cur.execute("SELECT 'a' + 'b'")
        cur.fetchall()
        exp = [
            ('Warning', 1292, "Truncated incorrect DOUBLE value: 'a'"),
            ('Warning', 1292, "Truncated incorrect DOUBLE value: 'b'")
        ]
        res = cur._fetch_warnings()
        self.assertTrue(tests.cmp_result(exp, res))
        self.assertEqual(len(exp), cur._warning_count)

    def test_execute(self):
        self.cnx.get_warnings = True
        cur = self._get_cursor(self.cnx)

        self.assertEqual(None, cur.execute(None))

        self.assertRaises(errors.ProgrammingError, cur.execute,
                          'SELECT %s,%s,%s', ('foo', 'bar',))

        cur.execute("SELECT 'a' + 'b'")
        cur.fetchall()
        exp = [
            ('Warning', 1292, "Truncated incorrect DOUBLE value: 'a'"),
            ('Warning', 1292, "Truncated incorrect DOUBLE value: 'b'")
        ]
        self.assertTrue(tests.cmp_result(exp, cur._warnings))
        self.cnx.get_warnings = False

        cur.execute("SELECT BINARY 'ham'")
        exp = [(b'ham',)]
        self.assertEqual(exp, cur.fetchall())
        cur.close()

        tbl = 'myconnpy_cursor'
        self.setup_table(self.cnx, tbl)

        cur = self._get_cursor(self.cnx)
        stmt_insert = "INSERT INTO {0} (col1,col2) VALUES (%s,%s)".format(tbl)
        res = cur.execute(stmt_insert, (1, 100))
        self.assertEqual(None, res, "Return value of execute() is wrong.")

        stmt_select = "SELECT col1,col2 FROM {0} ORDER BY col1".format(tbl)
        cur.execute(stmt_select)
        self.assertEqual([(1, '100')],
                         cur.fetchall(), "Insert test failed")

        data = {'id': 2}
        stmt = "SELECT col1,col2 FROM {0} WHERE col1 <= %(id)s".format(tbl)
        cur.execute(stmt, data)
        self.assertEqual([(1, '100')], cur.fetchall())

        cur.close()

    def test_executemany__errors(self):
        self.cnx.get_warnings = True
        cur = self._get_cursor(self.cnx)

        self.assertEqual(None, cur.executemany(None, []))

        cur = self._get_cursor(self.cnx)
        self.assertRaises(errors.ProgrammingError, cur.executemany,
                          'programming error with string', 'foo')
        self.assertRaises(errors.ProgrammingError, cur.executemany,
                          'programming error with 1 element list', ['foo'])

        self.assertEqual(None, cur.executemany('empty params', []))
        self.assertEqual(None, cur.executemany('params is None', None))

        self.assertRaises(errors.ProgrammingError, cur.executemany,
                          'foo', ['foo'])
        self.assertRaises(errors.ProgrammingError, cur.executemany,
                          'SELECT %s', [('foo',), 'foo'])

        self.assertRaises(errors.ProgrammingError,
                          cur.executemany,
                          "INSERT INTO t1 1 %s", [(1,), (2,)])

        cur.executemany("SELECT SHA1(%s)", [('foo',), ('bar',)])
        self.assertEqual(None, cur.fetchone())

    def test_executemany(self):
        tbl = 'myconnpy_cursor'
        self.setup_table(self.cnx, tbl)
        
        stmt_insert = "INSERT INTO {0} (col1,col2) VALUES (%s,%s)".format(tbl)
        stmt_select = "SELECT col1,col2 FROM {0} ORDER BY col1".format(tbl)

        cur = self._get_cursor(self.cnx)

        res = cur.executemany(stmt_insert, [(1, 100), (2, 200), (3, 300)])
        self.assertEqual(3, cur.rowcount)

        res = cur.executemany("SELECT %s", [('f',), ('o',), ('o',)])
        self.assertEqual(3, cur.rowcount)

        data = [{'id': 2}, {'id': 3}]
        stmt = "SELECT * FROM {0} WHERE col1 <= %(id)s".format(tbl)
        cur.executemany(stmt, data)
        self.assertEqual(5, cur.rowcount)

        cur.execute(stmt_select)
        self.assertEqual([(1, '100'), (2, '200'), (3, '300')],
                         cur.fetchall(), "Multi insert test failed")

        data = [{'id': 2}, {'id': 3}]
        stmt = "DELETE FROM {0} WHERE col1 = %(id)s".format(tbl)
        cur.executemany(stmt, data)
        self.assertEqual(2, cur.rowcount)

        stmt = "TRUNCATE TABLE {0}".format(tbl)
        cur.execute(stmt)

        stmt = (
            "/*comment*/INSERT/*comment*/INTO/*comment*/{0}(col1,col2)VALUES"
            "/*comment*/(%s,%s/*comment*/)/*comment()*/ON DUPLICATE KEY UPDATE"
            " col1 = VALUES(col1)"
        ).format(tbl)

        cur.executemany(stmt, [(4, 100), (5, 200), (6, 300)])
        self.assertEqual(3, cur.rowcount)

        cur.execute(stmt_select)
        self.assertEqual([(4, '100'), (5, '200'), (6, '300')],
                         cur.fetchall(), "Multi insert test failed")

        stmt = "TRUNCATE TABLE {0}".format(tbl)
        cur.execute(stmt)

        stmt = (
            "INSERT INTO/*comment*/{0}(col1,col2)VALUES"
            "/*comment*/(%s,'/*100*/')/*comment()*/ON DUPLICATE KEY UPDATE "
            "col1 = VALUES(col1)"
        ).format(tbl)

        cur.executemany(stmt, [(4,), (5,)])
        self.assertEqual(2, cur.rowcount)

        cur.execute(stmt_select)
        self.assertEqual([(4, '/*100*/'), (5, '/*100*/')],
                         cur.fetchall(), "Multi insert test failed")
        cur.close()

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
        cur = self._get_cursor(self.cnx)
        self.check_method(cur, 'callproc')

        self.assertRaises(ValueError, cur.callproc, None)
        self.assertRaises(ValueError, cur.callproc, 'sp1', None)

        config = tests.get_mysql_config()
        self.cnx.get_warnings = True

        self._test_callproc_setup(self.cnx)
        cur = self.cnx.cursor()

        if tests.MYSQL_VERSION < (5, 1):
            exp = ('5', '4', b'20')
        else:
            exp = (5, 4, 20)
        result = cur.callproc('myconnpy_sp_1', (exp[0], exp[1], 0))
        self.assertEqual(exp, result)

        if tests.MYSQL_VERSION < (5, 1):
            exp = ('6', '5', b'30')
        else:
            exp = (6, 5, 30)
        result = cur.callproc('myconnpy_sp_2', (exp[0], exp[1], 0))
        self.assertTrue(isinstance(cur._stored_results, list))
        self.assertEqual(exp, result)

        exp_results = [
            ('abc',),
            ('def',)
        ]
        for i, result in enumerate(cur.stored_results()):
            self.assertEqual(exp_results[i], result.fetchone())


        exp = ('ham', 'spam', 'hamspam')
        result = cur.callproc('myconnpy_sp_3', (exp[0], exp[1], 0))
        self.assertTrue(isinstance(cur._stored_results, list))
        self.assertEqual(exp, result)

        exp = ('ham', 'spam', 'hamspam')
        result = cur.callproc('myconnpy_sp_4',
                              (exp[0], (exp[1], 'CHAR'), (0, 'CHAR')))
        self.assertTrue(isinstance(cur._stored_results, list))
        self.assertEqual(exp, result)

        cur.close()
        self._test_callproc_cleanup(self.cnx)

    def test_fetchone(self):
        cur = self._get_cursor(self.cnx)

        self.assertEqual(None, cur.fetchone())

        cur = self.cnx.cursor()
        cur.execute("SELECT BINARY 'ham'")
        exp = (b'ham',)
        self.assertEqual(exp, cur.fetchone())
        self.assertEqual(None, cur.fetchone())
        cur.close()

    def test_fetchmany(self):
        """MySQLCursor object fetchmany()-method"""
        cur = self._get_cursor(self.cnx)

        self.assertEqual([], cur.fetchmany())

        tbl = 'myconnpy_fetch'
        self.setup_table(self.cnx, tbl)
        stmt_insert = (
            "INSERT INTO {table} (col1,col2) "
            "VALUES (%s,%s)".format(table=tbl))
        stmt_select = (
            "SELECT col1,col2 FROM {table} "
            "ORDER BY col1 DESC".format(table=tbl))

        cur = self.cnx.cursor()
        nrrows = 10
        data = [(i, str(i * 100)) for i in range(1, nrrows+1)]
        cur.executemany(stmt_insert, data)
        cur.execute(stmt_select)
        exp = [(10, '1000'), (9, '900'), (8, '800'), (7, '700')]
        rows = cur.fetchmany(4)
        self.assertTrue(tests.cmp_result(exp, rows),
                        "Fetching first 4 rows test failed.")
        exp = [(6, '600'), (5, '500'), (4, '400')]
        rows = cur.fetchmany(3)
        self.assertTrue(tests.cmp_result(exp, rows),
                        "Fetching next 3 rows test failed.")
        exp = [(3, '300'), (2, '200'), (1, '100')]
        rows = cur.fetchmany(3)
        self.assertTrue(tests.cmp_result(exp, rows),
                        "Fetching next 3 rows test failed.")
        self.assertEqual([], cur.fetchmany())

        cur.close()

    def test_fetchall(self):
        cur = self._get_cursor(self.cnx)

        self.assertRaises(errors.InterfaceError, cur.fetchall)

        tbl = 'myconnpy_fetch'
        self.setup_table(self.cnx, tbl)
        stmt_insert = (
            "INSERT INTO {table} (col1,col2) "
            "VALUES (%s,%s)".format(table=tbl))
        stmt_select = (
            "SELECT col1,col2 FROM {table} "
            "ORDER BY col1 ASC".format(table=tbl))

        cur = self.cnx.cursor()
        cur.execute("SELECT * FROM {table}".format(table=tbl))
        self.assertEqual([], cur.fetchall(),
                         "fetchall() with empty result should return []")
        nrrows = 10
        data = [(i, str(i * 100)) for i in range(1, nrrows+1)]
        cur.executemany(stmt_insert, data)
        cur.execute(stmt_select)
        self.assertTrue(tests.cmp_result(data, cur.fetchall()),
                        "Fetching all rows failed.")
        self.assertEqual(None, cur.fetchone())
        cur.close()

    def test_raise_on_warning(self):
        self.cnx.raise_on_warnings = True
        cur = self._get_cursor(self.cnx)
        cur.execute("SELECT 'a' + 'b'")
        try:
            cur.execute("SELECT 'a' + 'b'")
            cur.fetchall()
        except errors.DatabaseError:
            pass
        else:
            self.fail("Did not get exception while raising warnings.")

    def test__str__(self):
        cur = self._get_cursor(self.cnx)
        self.assertEqual("CMySQLCursor: (Nothing executed yet)",
                         cur.__str__())
        
        cur.execute("SELECT VERSION()")
        cur.fetchone()
        self.assertEqual("CMySQLCursor: SELECT VERSION()",
                         cur.__str__())
        stmt = "SELECT VERSION(),USER(),CURRENT_TIME(),NOW(),SHA1('myconnpy')"
        cur.execute(stmt)
        cur.fetchone()
        self.assertEqual("CMySQLCursor: {0}..".format(stmt[:40]),
                         cur.__str__())
        cur.close()

    def test_column_names(self):
        cur = self._get_cursor(self.cnx)
        stmt = "SELECT NOW() as now, 'The time' as label, 123 FROM dual"
        exp = (b'now', 'label', b'123')
        cur.execute(stmt)
        cur.fetchone()
        self.assertEqual(exp, cur.column_names)
        cur.close()

    def test_statement(self):
        cur = CMySQLCursor(self.cnx)
        exp = 'SELECT * FROM ham'
        cur._executed = exp
        self.assertEqual(exp, cur.statement)
        cur._executed = '  ' + exp + '    '
        self.assertEqual(exp, cur.statement)
        cur._executed = b'SELECT * FROM ham'
        self.assertEqual(exp, cur.statement)

    def test_with_rows(self):
        cur = CMySQLCursor(self.cnx)
        self.assertFalse(cur.with_rows)
        cur._description = ('ham', 'spam')
        self.assertTrue(cur.with_rows)

    def tests_nextset(self):
        cur = CMySQLCursor(self.cnx)
        stmt = "SELECT 'result', 1; SELECT 'result', 2; SELECT 'result', 3"
        cur.execute(stmt)
        self.assertEqual([('result', 1)], cur.fetchall())
        self.assertTrue(cur.nextset())
        self.assertEqual([('result', 2)], cur.fetchall())
        self.assertTrue(cur.nextset())
        self.assertEqual([('result', 3)], cur.fetchall())
        self.assertEqual(None, cur.nextset())

        tbl = 'myconnpy_nextset'
        stmt = "SELECT 'result', 1; INSERT INTO {0} () VALUES (); " \
               "SELECT * FROM {0}".format(tbl)
        self.setup_table(self.cnx, tbl)

        cur.execute(stmt)
        self.assertEqual([('result', 1)], cur.fetchall())
        try:
            cur.nextset()
        except errors.Error as exc:
            self.assertEqual(errorcode.CR_NO_RESULT_SET, exc.errno)
            self.assertEqual(1, cur._affected_rows)
        self.assertTrue(cur.nextset())
        self.assertEqual([(1, None, 0)], cur.fetchall())
        self.assertEqual(None, cur.nextset())

        cur.close()
        self.cnx.rollback()

    def tests_execute_multi(self):
        tbl = 'myconnpy_execute_multi'
        stmt = "SELECT 'result', 1; INSERT INTO {0} () VALUES (); " \
               "SELECT * FROM {0}".format(tbl)
        self.setup_table(self.cnx, tbl)

        multi_cur = CMySQLCursor(self.cnx)
        results = []
        exp = [
            (u"SELECT 'result', 1", [(u'result', 1)]),
            (u"INSERT INTO {0} () VALUES ()".format(tbl), 1, 1),
            (u"SELECT * FROM {0}".format(tbl), [(1, None, 0)]),
        ]
        for cur in multi_cur.execute(stmt, multi=True):
            if cur.with_rows:
                results.append((cur.statement, cur.fetchall()))
            else:
                results.append(
                    (cur.statement, cur._affected_rows, cur.lastrowid)
                )

        self.assertEqual(exp, results)

        cur.close()
        self.cnx.rollback()


class CExtMySQLCursorBufferedTests(tests.CMySQLCursorTests):

    def _get_cursor(self, cnx=None):
        if not cnx:
            cnx = CMySQLConnection(**self.config)
        self.cnx.buffered = True
        return CMySQLCursorBuffered(connection=cnx)

    def test___init__(self):
        self.assertRaises(errors.InterfaceError, CMySQLCursorBuffered,
                          connection='ham')

        cur = self._get_cursor(self.cnx)
        self.assertTrue(hex(id(self.cnx)).upper()[2:]
                        in repr(cur._cnx).upper())

    def test_execute(self):
        self.cnx.get_warnings = True
        cur = self._get_cursor(self.cnx)

        self.assertEqual(None, cur.execute(None, None))

        self.assertEqual(True,
                         isinstance(cur, CMySQLCursorBuffered))

        cur.execute("SELECT 1")
        self.assertEqual((1,), cur.fetchone())

    def test_raise_on_warning(self):
        self.cnx.raise_on_warnings = True
        cur = self._get_cursor(self.cnx)
        self.assertRaises(errors.DatabaseError,
                          cur.execute, "SELECT 'a' + 'b'")

    def test_with_rows(self):
        cur = self._get_cursor(self.cnx)
        cur.execute("SELECT 1")
        self.assertTrue(cur.with_rows)


class CMySQLCursorRawTests(tests.CMySQLCursorTests):

    def _get_cursor(self, cnx=None):
        if not cnx:
            cnx = CMySQLConnection(**self.config)
        return CMySQLCursorRaw(connection=cnx)

    def test_fetchone(self):
        cur = self._get_cursor(self.cnx)
        self.assertEqual(None, cur.fetchone())

        cur.execute("SELECT 1, 'string', MAKEDATE(2010,365), 2.5")
        exp = (b'1', b'string', b'2010-12-31', b'2.5')
        self.assertEqual(exp, cur.fetchone())
