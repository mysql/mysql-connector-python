# -*- coding: utf-8 -*-

# Copyright (c) 2014, 2022, Oracle and/or its affiliates.
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

"""Testing the C Extension cursors
"""

import datetime
import decimal
import logging
import sys
import unittest

import tests

from mysql.connector import errorcode, errors

try:
    from _mysql_connector import MySQL, MySQLError, MySQLInterfaceError
except ImportError:
    HAVE_CMYSQL = False
else:
    HAVE_CMYSQL = True

from mysql.connector.connection_cext import CMySQLConnection
from mysql.connector.cursor_cext import (
    CMySQLCursor,
    CMySQLCursorBuffered,
    CMySQLCursorPrepared,
    CMySQLCursorRaw,
)

ARCH_64BIT = sys.maxsize > 2**32 and sys.platform != "win32"
LOGGER = logging.getLogger(tests.LOGGER_NAME)


@unittest.skipIf(HAVE_CMYSQL == False, "C Extension not available")
class CExtMySQLCursorTests(tests.CMySQLCursorTests):
    def _get_cursor(self, cnx=None):
        if not cnx:
            cnx = CMySQLConnection(**self.config)
        return CMySQLCursor(connection=cnx)

    def test___init__(self):
        self.assertRaises(errors.InterfaceError, CMySQLCursor, connection="ham")
        cur = self._get_cursor(self.cnx)
        self.assertTrue(hex(id(self.cnx)).upper()[2:-1] in repr(cur._cnx).upper())

    def test_lastrowid(self):
        cur = self._get_cursor(self.cnx)

        tbl = "test_lastrowid"
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
            ("Warning", 1292, "Truncated incorrect DOUBLE value: 'a'"),
            ("Warning", 1292, "Truncated incorrect DOUBLE value: 'b'"),
        ]
        res = cur._fetch_warnings()
        self.assertTrue(tests.cmp_result(exp, res))
        self.assertEqual(len(exp), cur._warning_count)
        self.assertEqual(len(res), cur.warning_count)

    def test_execute(self):
        self.cnx.get_warnings = True
        cur = self._get_cursor(self.cnx)

        self.assertEqual(None, cur.execute(None))

        self.assertRaises(
            errors.ProgrammingError,
            cur.execute,
            "SELECT %s,%s,%s",
            (
                "foo",
                "bar",
            ),
        )

        cur.execute("SELECT 'a' + 'b'")
        cur.fetchall()
        exp = [
            ("Warning", 1292, "Truncated incorrect DOUBLE value: 'a'"),
            ("Warning", 1292, "Truncated incorrect DOUBLE value: 'b'"),
        ]
        self.assertTrue(tests.cmp_result(exp, cur._warnings))
        self.cnx.get_warnings = False

        cur.execute("SELECT BINARY 'ham'")
        exp = [(bytearray(b"ham"),)]
        self.assertEqual(exp, cur.fetchall())
        cur.close()

        tbl = "myconnpy_cursor"
        self.setup_table(self.cnx, tbl)

        cur = self._get_cursor(self.cnx)
        stmt_insert = "INSERT INTO {0} (col1,col2) VALUES (%s,%s)".format(tbl)
        res = cur.execute(stmt_insert, (1, 100))
        self.assertEqual(None, res, "Return value of execute() is wrong.")

        stmt_select = "SELECT col1,col2 FROM {0} ORDER BY col1".format(tbl)
        cur.execute(stmt_select)
        self.assertEqual([(1, "100")], cur.fetchall(), "Insert test failed")

        data = {"id": 2}
        stmt = "SELECT col1,col2 FROM {0} WHERE col1 <= %(id)s".format(tbl)
        cur.execute(stmt, data)
        self.assertEqual([(1, "100")], cur.fetchall())

        cur.close()

    def test_executemany__errors(self):
        self.cnx.get_warnings = True
        cur = self._get_cursor(self.cnx)

        self.assertEqual(None, cur.executemany(None, []))

        cur = self._get_cursor(self.cnx)
        self.assertRaises(
            errors.ProgrammingError,
            cur.executemany,
            "programming error with string",
            "foo",
        )
        self.assertRaises(
            errors.ProgrammingError,
            cur.executemany,
            "programming error with 1 element list",
            ["foo"],
        )

        self.assertEqual(None, cur.executemany("empty params", []))
        self.assertEqual(None, cur.executemany("params is None", None))

        self.assertRaises(errors.ProgrammingError, cur.executemany, "foo", ["foo"])
        self.assertRaises(
            errors.ProgrammingError,
            cur.executemany,
            "SELECT %s",
            [("foo",), "foo"],
        )

        self.assertRaises(
            errors.ProgrammingError,
            cur.executemany,
            "INSERT INTO t1 1 %s",
            [(1,), (2,)],
        )

        cur.executemany("SELECT SHA1(%s)", [("foo",), ("bar",)])

    def test_executemany(self):
        tbl = "myconnpy_cursor"
        self.setup_table(self.cnx, tbl)

        stmt_insert = "INSERT INTO {0} (col1,col2) VALUES (%s,%s)".format(tbl)
        stmt_select = "SELECT col1,col2 FROM {0} ORDER BY col1".format(tbl)

        cur = self._get_cursor(self.cnx)

        res = cur.executemany(stmt_insert, [(1, 100), (2, 200), (3, 300)])
        self.assertEqual(3, cur.rowcount)

        res = cur.executemany("SELECT %s", [("f",), ("o",), ("o",)])
        self.assertEqual(3, cur.rowcount)

        data = [{"id": 2}, {"id": 3}]
        stmt = "SELECT * FROM {0} WHERE col1 <= %(id)s".format(tbl)
        cur.executemany(stmt, data)
        self.assertEqual(5, cur.rowcount)

        cur.execute(stmt_select)
        self.assertEqual(
            [(1, "100"), (2, "200"), (3, "300")],
            cur.fetchall(),
            "Multi insert test failed",
        )

        data = [{"id": 2}, {"id": 3}]
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
        self.assertEqual(
            [(4, "100"), (5, "200"), (6, "300")],
            cur.fetchall(),
            "Multi insert test failed",
        )

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
        self.assertEqual(
            [(4, "/*100*/"), (5, "/*100*/")],
            cur.fetchall(),
            "Multi insert test failed",
        )
        cur.close()

    def _test_callproc_setup(self, cnx):

        self._test_callproc_cleanup(cnx)
        stmt_create1 = (
            "CREATE PROCEDURE myconnpy_sp_1 "
            "(IN pFac1 INT, IN pFac2 INT, OUT pProd INT) "
            "BEGIN SET pProd := pFac1 * pFac2; END;"
        )

        stmt_create2 = (
            "CREATE PROCEDURE myconnpy_sp_2 "
            "(IN pFac1 INT, IN pFac2 INT, OUT pProd INT) "
            "BEGIN SELECT 'abc'; SELECT 'def'; SET pProd := pFac1 * pFac2; "
            "END;"
        )

        stmt_create3 = (
            "CREATE PROCEDURE myconnpy_sp_3"
            "(IN pStr1 VARCHAR(20), IN pStr2 VARCHAR(20), "
            "OUT pConCat VARCHAR(100)) "
            "BEGIN SET pConCat := CONCAT(pStr1, pStr2); END;"
        )

        stmt_create4 = (
            "CREATE PROCEDURE myconnpy_sp_4"
            "(IN pStr1 VARCHAR(20), INOUT pStr2 VARCHAR(20), "
            "OUT pConCat VARCHAR(100)) "
            "BEGIN SET pConCat := CONCAT(pStr1, pStr2); END;"
        )

        stmt_create5 = f"""
            CREATE PROCEDURE {cnx.database}.myconnpy_sp_5(IN user_value INT)
            BEGIN
                SET @user_value = user_value;
                SELECT @user_value AS 'user_value', CURRENT_TIMESTAMP as
                'timestamp';
            END
        """

        try:
            cur = cnx.cursor()
            cur.execute(stmt_create1)
            cur.execute(stmt_create2)
            cur.execute(stmt_create3)
            cur.execute(stmt_create4)
            cur.execute(stmt_create5)
        except errors.Error as err:
            self.fail("Failed setting up test stored routine; {0}".format(err))
        cur.close()

    def _test_callproc_cleanup(self, cnx):

        sp_names = (
            "myconnpy_sp_1",
            "myconnpy_sp_2",
            "myconnpy_sp_3",
            "myconnpy_sp_4",
            f"{cnx.database}.myconnpy_sp_5",
        )
        stmt_drop = "DROP PROCEDURE IF EXISTS {procname}"

        try:
            cur = cnx.cursor()
            for sp_name in sp_names:
                cur.execute(stmt_drop.format(procname=sp_name))
        except errors.Error as err:
            self.fail("Failed cleaning up test stored routine; {0}".format(err))
        cur.close()

    def test_callproc(self):
        cur = self._get_cursor(self.cnx)
        self.check_method(cur, "callproc")

        self.assertRaises(ValueError, cur.callproc, None)
        self.assertRaises(ValueError, cur.callproc, "sp1", None)

        config = tests.get_mysql_config()
        self.cnx.get_warnings = True

        self._test_callproc_setup(self.cnx)
        cur = self.cnx.cursor()

        if tests.MYSQL_VERSION < (5, 1):
            exp = ("5", "4", b"20")
        else:
            exp = (5, 4, 20)
        result = cur.callproc("myconnpy_sp_1", (exp[0], exp[1], 0))
        self.assertEqual(exp, result)

        if tests.MYSQL_VERSION < (5, 1):
            exp = ("6", "5", b"30")
        else:
            exp = (6, 5, 30)
        result = cur.callproc("myconnpy_sp_2", (exp[0], exp[1], 0))
        self.assertTrue(isinstance(cur._stored_results, list))
        self.assertEqual(exp, result)

        exp_results = [("abc",), ("def",)]
        for i, result in enumerate(cur.stored_results()):
            self.assertEqual(exp_results[i], result.fetchone())

        exp = ("ham", "spam", "hamspam")
        result = cur.callproc("myconnpy_sp_3", (exp[0], exp[1], 0))
        self.assertTrue(isinstance(cur._stored_results, list))
        self.assertEqual(exp, result)

        exp = ("ham", "spam", "hamspam")
        result = cur.callproc("myconnpy_sp_4", (exp[0], (exp[1], "CHAR"), (0, "CHAR")))
        self.assertTrue(isinstance(cur._stored_results, list))
        self.assertEqual(exp, result)

        exp = (5,)
        result = cur.callproc(f"{self.cnx.database}.myconnpy_sp_5", exp)
        self.assertTrue(isinstance(cur._stored_results, list))
        self.assertEqual(exp, result)

        cur.close()
        self._test_callproc_cleanup(self.cnx)

    def test_fetchone(self):
        cur = self._get_cursor(self.cnx)

        self.assertRaises(errors.InterfaceError, cur.fetchone)

        cur = self.cnx.cursor()
        cur.execute("SELECT BINARY 'ham'")
        exp = (bytearray(b"ham"),)
        self.assertEqual(exp, cur.fetchone())
        self.assertEqual(None, cur.fetchone())
        cur.close()

    def test_fetchmany(self):
        """MySQLCursor object fetchmany()-method"""
        cur = self._get_cursor(self.cnx)

        self.assertRaises(errors.InterfaceError, cur.fetchmany)

        tbl = "myconnpy_fetch"
        self.setup_table(self.cnx, tbl)
        stmt_insert = "INSERT INTO {table} (col1,col2) VALUES (%s,%s)".format(table=tbl)
        stmt_select = "SELECT col1,col2 FROM {table} ORDER BY col1 DESC".format(
            table=tbl
        )

        cur = self.cnx.cursor()
        nrrows = 10
        data = [(i, str(i * 100)) for i in range(1, nrrows + 1)]
        cur.executemany(stmt_insert, data)
        cur.execute(stmt_select)
        exp = [(10, "1000"), (9, "900"), (8, "800"), (7, "700")]
        rows = cur.fetchmany(4)
        self.assertTrue(
            tests.cmp_result(exp, rows), "Fetching first 4 rows test failed."
        )
        exp = [(6, "600"), (5, "500"), (4, "400")]
        rows = cur.fetchmany(3)
        self.assertTrue(
            tests.cmp_result(exp, rows), "Fetching next 3 rows test failed."
        )
        exp = [(3, "300"), (2, "200"), (1, "100")]
        rows = cur.fetchmany(3)
        self.assertTrue(
            tests.cmp_result(exp, rows), "Fetching next 3 rows test failed."
        )
        self.assertEqual([], cur.fetchmany())

        # Fetch more than we have.
        cur.execute(stmt_select)
        rows = cur.fetchmany(100)
        exp = [
            (10, "1000"),
            (9, "900"),
            (8, "800"),
            (7, "700"),
            (6, "600"),
            (5, "500"),
            (4, "400"),
            (3, "300"),
            (2, "200"),
            (1, "100"),
        ]
        self.assertTrue(
            tests.cmp_result(exp, rows), "Fetching next 3 rows test failed."
        )

        # Fetch iteratively without full batch
        cur.execute(stmt_select)
        rows = cur.fetchmany(6)
        self.assertEqual(6, len(rows))

        rows = cur.fetchmany(6)
        self.assertEqual(4, len(rows))

        rows = cur.fetchmany(6)
        self.assertEqual(0, len(rows))
        self.assertEqual([], cur.fetchmany())

        cur.close()

    def test_fetchall(self):
        cur = self._get_cursor(self.cnx)

        self.assertRaises(errors.InterfaceError, cur.fetchall)

        tbl = "myconnpy_fetch"
        self.setup_table(self.cnx, tbl)
        stmt_insert = "INSERT INTO {table} (col1,col2) VALUES (%s,%s)".format(table=tbl)
        stmt_select = "SELECT col1,col2 FROM {table} ORDER BY col1 ASC".format(
            table=tbl
        )

        cur = self.cnx.cursor()
        cur.execute("SELECT * FROM {table}".format(table=tbl))
        self.assertEqual(
            [], cur.fetchall(), "fetchall() with empty result should return []"
        )
        nrrows = 10
        data = [(i, str(i * 100)) for i in range(1, nrrows + 1)]
        cur.executemany(stmt_insert, data)
        cur.execute(stmt_select)
        self.assertTrue(
            tests.cmp_result(data, cur.fetchall()), "Fetching all rows failed."
        )
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
        self.assertEqual("CMySQLCursor: (Nothing executed yet)", cur.__str__())

        cur.execute("SELECT VERSION()")
        cur.fetchone()
        self.assertEqual("CMySQLCursor: SELECT VERSION()", cur.__str__())
        stmt = "SELECT VERSION(),USER(),CURRENT_TIME(),NOW(),SHA1('myconnpy')"
        cur.execute(stmt)
        cur.fetchone()
        self.assertEqual("CMySQLCursor: {0}..".format(stmt[:40]), cur.__str__())
        cur.close()

    def test_column_names(self):
        cur = self._get_cursor(self.cnx)
        stmt = "SELECT NOW() as now, 'The time' as label, 123 FROM dual"
        exp = ("now", "label", "123")
        cur.execute(stmt)
        cur.fetchone()
        self.assertEqual(exp, cur.column_names)
        cur.close()

    def test_statement(self):
        cur = CMySQLCursor(self.cnx)
        exp = "SELECT * FROM ham"
        cur._executed = exp
        self.assertEqual(exp, cur.statement)
        cur._executed = "  " + exp + "    "
        self.assertEqual(exp, cur.statement)
        cur._executed = b"SELECT * FROM ham"
        self.assertEqual(exp, cur.statement)

    def test_with_rows(self):
        cur = CMySQLCursor(self.cnx)
        self.assertFalse(cur.with_rows)
        cur._description = ("ham", "spam")
        self.assertTrue(cur.with_rows)

    def tests_nextset(self):
        cur = CMySQLCursor(self.cnx)
        stmt = "SELECT 'result', 1; SELECT 'result', 2; SELECT 'result', 3"
        cur.execute(stmt)
        self.assertEqual([("result", 1)], cur.fetchall())
        self.assertTrue(cur.nextset())
        self.assertEqual([("result", 2)], cur.fetchall())
        self.assertTrue(cur.nextset())
        self.assertEqual([("result", 3)], cur.fetchall())
        self.assertEqual(None, cur.nextset())

        tbl = "myconnpy_nextset"
        stmt = (
            "SELECT 'result', 1; INSERT INTO {0} () VALUES (); "
            "SELECT * FROM {0}".format(tbl)
        )
        self.setup_table(self.cnx, tbl)

        cur.execute(stmt)
        self.assertEqual([("result", 1)], cur.fetchall())
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
        tbl = "myconnpy_execute_multi"
        stmt = (
            "SELECT 'result', 1; INSERT INTO {0} () VALUES (); "
            "SELECT * FROM {0}".format(tbl)
        )
        self.setup_table(self.cnx, tbl)

        multi_cur = CMySQLCursor(self.cnx)
        results = []
        exp = [
            ("SELECT 'result', 1", [("result", 1)]),
            ("INSERT INTO {0} () VALUES ()".format(tbl), 1, 1),
            ("SELECT * FROM {0}".format(tbl), [(1, None, 0)]),
        ]
        for cur in multi_cur.execute(stmt, multi=True):
            if cur.with_rows:
                results.append((cur.statement, cur.fetchall()))
            else:
                results.append((cur.statement, cur._affected_rows, cur.lastrowid))

        self.assertEqual(exp, results)

        cur.close()
        self.cnx.rollback()

        cur = self._get_cursor(self.cnx)
        cur.execute("DROP PROCEDURE IF EXISTS multi_results")
        procedure = (
            "CREATE PROCEDURE multi_results () BEGIN SELECT 1; SELECT 'ham'; END"
        )
        cur.execute(procedure)
        stmt = b"CALL multi_results()"
        exp_result = [[(1,)], [("ham",)]]
        results = []
        for result in cur.execute(stmt, multi=True):
            if result.with_rows:
                self.assertEqual(stmt, result._executed)
                results.append(result.fetchall())

        self.assertEqual(exp_result, results)
        cur.execute("DROP PROCEDURE multi_results")

        cur.close()


class CExtMySQLCursorBufferedTests(tests.CMySQLCursorTests):
    def _get_cursor(self, cnx=None):
        if not cnx:
            cnx = CMySQLConnection(**self.config)
        self.cnx.buffered = True
        return CMySQLCursorBuffered(connection=cnx)

    def test___init__(self):
        self.assertRaises(errors.InterfaceError, CMySQLCursorBuffered, connection="ham")

        cur = self._get_cursor(self.cnx)
        self.assertTrue(hex(id(self.cnx)).upper()[2:-1] in repr(cur._cnx).upper())

    def test_execute(self):
        self.cnx.get_warnings = True
        cur = self._get_cursor(self.cnx)

        self.assertEqual(None, cur.execute(None, None))

        self.assertEqual(True, isinstance(cur, CMySQLCursorBuffered))

        cur.execute("SELECT 1")
        self.assertEqual((1,), cur.fetchone())

    def test_raise_on_warning(self):
        self.cnx.raise_on_warnings = True
        cur = self._get_cursor(self.cnx)
        self.assertRaises(errors.DatabaseError, cur.execute, "SELECT 'a' + 'b'")

    def test_with_rows(self):
        cur = self._get_cursor(self.cnx)
        cur.execute("SELECT 1")
        self.assertTrue(cur.with_rows)

    def test_executemany(self):
        tbl = "myconnpy_cursor"
        self.setup_table(self.cnx, tbl)

        stmt_insert = "INSERT INTO {0} (col1,col2) VALUES (%s,%s)".format(tbl)
        stmt_select = "SELECT col1,col2 FROM {0} ORDER BY col1".format(tbl)

        cur = self._get_cursor(self.cnx)

        res = cur.executemany(stmt_insert, [(1, 100), (2, 200), (3, 300)])
        self.assertEqual(3, cur.rowcount)

        res = cur.executemany("SELECT %s", [("f",), ("o",), ("o",)])
        self.assertEqual(3, cur.rowcount)

        data = [{"id": 2}, {"id": 3}]
        stmt = "SELECT * FROM {0} WHERE col1 <= %(id)s".format(tbl)
        cur.executemany(stmt, data)
        self.assertEqual(5, cur.rowcount)

        cur.execute(stmt_select)
        self.assertEqual(
            [(1, "100"), (2, "200"), (3, "300")],
            cur.fetchall(),
            "Multi insert test failed",
        )

        data = [{"id": 2}, {"id": 3}]
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
        self.assertEqual(
            [(4, "100"), (5, "200"), (6, "300")],
            cur.fetchall(),
            "Multi insert test failed",
        )

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
        self.assertEqual(
            [(4, "/*100*/"), (5, "/*100*/")],
            cur.fetchall(),
            "Multi insert test failed",
        )

        # BugOra21529893
        table_name = "BugOra21529893"
        data_list = [
            [
                ("A", "B"),
            ],
            [
                ("C", "D"),
                ("O", "P"),
            ],
        ]
        stmt = f"select %s,%s from {table_name} a,{table_name} b"
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.execute(
            f"create table {table_name}(c1 char(32),c2 longblob)DEFAULT CHARSET utf8"
        )
        cur.execute(f"insert into {table_name} values('1','1'),('2','2'),('3','3')")
        for data in data_list:
            cur.executemany(stmt, data)
            self.assertEqual(9 * len(data), cur.rowcount)
            self.assertEqual([data[-1] for _ in range(9)], cur.fetchall())
            self.assertEqual(9 * len(data), cur.rowcount)
            self.assertEqual([], cur.fetchall())
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")

        cur.close()


class CMySQLCursorRawTests(tests.CMySQLCursorTests):
    def _get_cursor(self, cnx=None):
        if not cnx:
            cnx = CMySQLConnection(**self.config)
        return CMySQLCursorRaw(connection=cnx)

    def test_fetchone(self):
        cur = self._get_cursor(self.cnx)
        self.assertRaises(errors.InterfaceError, cur.fetchone)

        cur.execute("SELECT 1, 'string', MAKEDATE(2010,365), 2.5")
        exp = (b"1", b"string", b"2010-12-31", b"2.5")
        self.assertEqual(exp, cur.fetchone())


class CMySQLCursorPreparedTests(tests.CMySQLCursorTests):

    tbl = "prep_stmt"

    create_table_stmt = (
        "CREATE TABLE {0} ("
        "my_null INT, "
        "my_bit BIT(7), "
        "my_tinyint TINYINT, "
        "my_smallint SMALLINT, "
        "my_mediumint MEDIUMINT, "
        "my_int INT, "
        "my_bigint BIGINT, "
        "my_decimal DECIMAL(20,10), "
        "my_float FLOAT, "
        "my_double DOUBLE, "
        "my_date DATE, "
        "my_time TIME, "
        "my_datetime DATETIME, "
        "my_year YEAR, "
        "my_char CHAR(100), "
        "my_varchar VARCHAR(100), "
        "my_enum ENUM('x-small', 'small', 'medium', 'large', 'x-large'), "
        "my_geometry POINT, "
        "my_blob BLOB)"
    )

    insert_stmt = (
        "INSERT INTO {0} ("
        "my_null, "
        "my_bit, "
        "my_tinyint, "
        "my_smallint, "
        "my_mediumint, "
        "my_int, "
        "my_bigint, "
        "my_decimal, "
        "my_float, "
        "my_double, "
        "my_date, "
        "my_time, "
        "my_datetime, "
        "my_year, "
        "my_char, "
        "my_varchar, "
        "my_enum, "
        "my_geometry, "
        "my_blob) "
        "VALUES (?, B'1111100', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
        "POINT(21.2, 34.2), ?)"
    )

    data = (
        None,
        127,
        32767,
        8388607,
        2147483647,
        4294967295 if ARCH_64BIT else 2147483647,
        decimal.Decimal("1.2"),
        3.14,
        4.28,
        datetime.date(2018, 12, 31),
        datetime.time(12, 13, 14),
        datetime.datetime(2019, 2, 4, 10, 36, 00),
        2019,
        "abc",
        "MySQL 🐬",
        "x-large",
        bytearray(b"random blob data"),
    )

    exp = (
        None,
        124,
        127,
        32767,
        8388607,
        2147483647,
        4294967295 if ARCH_64BIT else 2147483647,
        decimal.Decimal("1.2000000000"),
        3.140000104904175,
        4.28000020980835,
        datetime.date(2018, 12, 31),
        datetime.timedelta(0, 43994),
        datetime.datetime(2019, 2, 4, 10, 36),
        2019,
        "abc",
        "MySQL \U0001f42c",
        "x-large",
        bytearray(
            b"\x00\x00\x00\x00\x01\x01\x00\x00\x003333335"
            b"@\x9a\x99\x99\x99\x99\x19A@"
        ),
        bytearray(b"random blob data"),
    )

    def setUp(self):
        config = tests.get_mysql_config()
        self.cnx = CMySQLConnection(**config)
        self.cur = self.cnx.cursor(prepared=True)
        self.cur.execute(self.create_table_stmt.format(self.tbl))

    def tearDown(self):
        self.cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))
        self.cur.close()
        self.cnx.close()

    def test___init__(self):
        self.assertIsInstance(self.cur, CMySQLCursorPrepared)

    def test_callproc(self):
        self.assertRaises(errors.NotSupportedError, self.cur.callproc, None)

    def test_close(self):
        cur = self.cnx.cursor(prepared=True)
        self.assertEqual(None, cur._stmt)
        cur.close()

    def test_fetchone(self):
        self.cur.execute(self.insert_stmt.format(self.tbl), self.data)
        self.cur.execute("SELECT * FROM {0}".format(self.tbl))
        row = self.cur.fetchone()
        self.assertEqual(row, self.exp)
        row = self.cur.fetchone()
        self.assertIsNone(row)

    def test_fetchall(self):
        self.cur.execute(self.insert_stmt.format(self.tbl), self.data)
        self.cur.execute("SELECT * FROM {0}".format(self.tbl))
        rows = self.cur.fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0], self.exp)

    def test_fetchmany(self):
        data = [self.data[:], self.data[:], self.data[:]]
        self.cur.executemany(self.insert_stmt.format(self.tbl), data)
        self.cur.execute("SELECT * FROM {0}".format(self.tbl))
        rows = self.cur.fetchmany(size=2)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0], self.exp)
        self.assertEqual(rows[1], self.exp)
        rows = self.cur.fetchmany(1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0], self.exp)

    def test_executemany(self):
        data = [self.data[:], self.data[:]]
        self.cur.executemany(self.insert_stmt.format(self.tbl), data)
        self.cur.execute("SELECT * FROM {0}".format(self.tbl))
        rows = self.cur.fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0], self.exp)
        self.assertEqual(rows[1], self.exp)
