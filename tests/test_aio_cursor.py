# Copyright (c) 2023, Oracle and/or its affiliates.
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

import tests

from mysql.connector.aio.cursor import (
    MySQLCursor,
    MySQLCursorBuffered,
    MySQLCursorBufferedDict,
    MySQLCursorBufferedNamedTuple,
    MySQLCursorBufferedRaw,
    MySQLCursorDict,
    MySQLCursorNamedTuple,
    MySQLCursorPrepared,
    MySQLCursorPreparedDict,
    MySQLCursorPreparedNamedTuple,
    MySQLCursorPreparedRaw,
    MySQLCursorRaw,
)
from mysql.connector.errors import Error, InterfaceError, ProgrammingError
from tests import cmp_result, cnx_aio_config, foreach_cnx_aio


class MySQLCursorTestsMixin:
    async def _test_execute(self, cnx, cursor_class):
        async with await cnx.cursor(cursor_class=cursor_class) as cur:
            self.assertIsInstance(cur, cursor_class)
            self.assertEqual(None, await cur.execute(None, None))

            # Invalid number of placeholders
            with self.assertRaises(ProgrammingError):
                await cur.execute("SELECT %s,%s,%s", ("foo", "bar"))

            # `cursor.executemulti()` should be used to execute multiple statements
            with self.assertRaises(ProgrammingError):
                await cur.execute("SELECT 1; SELECT 2; SELECT 3;", multi=True)

            await cur.execute("SELECT 'a' + 'b' as res")
            await cur.fetchone()
            exp = [
                ("Warning", 1292, "Truncated incorrect DOUBLE value: 'a'"),
                ("Warning", 1292, "Truncated incorrect DOUBLE value: 'b'"),
            ]
            self.assertTrue(cmp_result(exp, cur._warnings))

            if issubclass(type(cur), MySQLCursorDict):
                exp = [{"res": bytearray(b"ham")}]
            else:
                exp = [(bytearray(b"ham"),)]
            await cur.execute("SELECT BINARY 'ham' AS res")
            self.assertEqual(exp, await cur.fetchall())

        tbl = "myconnpy_cursor"
        await self._test_execute_setup(self.cnx, tbl)

        async with await self.cnx.cursor(cursor_class=cursor_class) as cur:
            if issubclass(type(cur), MySQLCursorRaw):
                exp = [(bytearray(b"1"), bytearray(b"100"))]
            elif issubclass(type(cur), MySQLCursorDict):
                exp = [{"col1": 1, "col2": "100"}]
            else:
                exp = [(1, "100")]

            res = await cur.execute(
                f"INSERT INTO {tbl} (col1,col2) VALUES (%s,%s)", (1, 100)
            )
            self.assertEqual(None, res, "Return value of execute() is wrong.")
            await cur.execute(f"SELECT col1,col2 FROM {tbl} ORDER BY col1")
            self.assertEqual(exp, await cur.fetchall(), "Insert test failed")

            await cur.execute(
                f"SELECT col1,col2 FROM {tbl} WHERE col1 <= %(id)s", {"id": 2}
            )
            self.assertEqual(exp, await cur.fetchall())

        await self._test_execute_cleanup(self.cnx, tbl)

    async def _test_executemulti(self, cnx, cursor_class):
        async with cursor_class(connection=cnx) as cur:
            await cur.execute("DROP PROCEDURE IF EXISTS multi_results")
            await cur.execute(
                "CREATE PROCEDURE multi_results () BEGIN "
                "SELECT 1 AS res; SELECT 'ham'; END"
            )

            exp_stmt = b"CALL multi_results()"
            if issubclass(type(cur), MySQLCursorRaw):
                exp_results = [[(bytearray(b"1"),)], [(bytearray(b"ham"),)]]
            elif issubclass(type(cur), MySQLCursorDict):
                exp_results = [[{"res": 1}], [{"ham": "ham"}]]
            else:
                exp_results = [[(1,)], [("ham",)]]

            results = []
            async for res in cur.executemulti(exp_stmt):
                if res.with_rows:
                    self.assertEqual(exp_stmt, res._executed)
                    results.append(await res.fetchall())

            self.assertEqual(exp_results, results)
            await cur.execute("DROP PROCEDURE multi_results")

            # Check the statements split
            operations = [
                'select 1 AS res1; SELECT "`" AS res2;',
                "SELECT '\"' AS res3; SELECT 2 AS res4; select '```' AS res5;",
                "select 1 AS res6; select '`' AS res7; select 3 AS res8;",
                "select \"'''''\" AS res9;",
            ]
            control = [
                ["select 1 AS res1", 'SELECT "`" AS res2'],
                ["SELECT '\"' AS res3", "SELECT 2 AS res4", "select '```' AS res5"],
                ["select 1 AS res6", "select '`' AS res7", "select 3 AS res8"],
                ["select \"'''''\" AS res9"],
            ]
            for operation, exps in zip(operations, control):
                i = 0
                async for res in cur.executemulti(operation):
                    self.assertEqual(exps[i], res.statement)
                    await res.fetchall()
                    i += 1

    async def _test_executemany(self, cnx, cursor_class):
        async with cursor_class(cnx) as cur:
            self.assertIsInstance(cur, cursor_class)
            self.assertEqual(None, await cur.executemany(None, []))
            self.assertEqual(None, await cur.executemany("empty params", []))

            with self.assertRaises(ProgrammingError):
                await cur.executemany("programming error with string", "foo")

            with self.assertRaises(ProgrammingError):
                await cur.executemany(
                    "programming error with 1 element list",
                    ["foo"],
                )

            with self.assertRaises(ProgrammingError):
                await cur.executemany("params is None", 1)

            with self.assertRaises(ProgrammingError):
                await cur.executemany("SELECT %s AS res", [("foo",), "foo"])

            with self.assertRaises(ProgrammingError):
                await cur.executemany("INSERT INTO t1 1 %s", [(1,), (2,)])

            # TODO: Fix
            # await cur.executemany("SELECT SHA1(%s) AS res", [("foo",), ("bar",)])
            # self.assertEqual(None, await cur.fetchone())

            tbl = "myconnpy_cursor"
            stmt_select = f"SELECT col1,col2 FROM {tbl} ORDER BY col1"

            await self._test_execute_setup(cnx, tbl)

            await cur.executemany(
                f"INSERT INTO {tbl} (col1,col2) VALUES (%s,%s)",
                [(1, 100), (2, 200), (3, 300)],
            )
            self.assertEqual(3, cur.rowcount)

            await cur.executemany("SELECT %s AS res", [("f",), ("o",), ("o",)])
            self.assertEqual(3, cur.rowcount)

            data = [{"id": 2}, {"id": 3}]
            stmt = f"SELECT * FROM {tbl} WHERE col1 <= %(id)s"
            await cur.executemany(stmt, data)
            self.assertEqual(5, cur.rowcount)

            await cur.execute(stmt_select)

            exp = [(1, "100"), (2, "200"), (3, "300")]
            if issubclass(type(cur), MySQLCursorRaw):
                exp = [(str(x).encode(), y.encode()) for x, y in exp]
            elif issubclass(type(cur), MySQLCursorDict):
                exp = [{"col1": x, "col2": y} for x, y in exp]
            self.assertEqual(exp, await cur.fetchall(), "Multi insert test failed")

            data = [{"id": 2}, {"id": 3}]
            await cur.executemany(f"DELETE FROM {tbl} WHERE col1 = %(id)s", data)
            self.assertEqual(2, cur.rowcount)

            await cur.execute(f"TRUNCATE TABLE {tbl}")

            stmt = (
                f"/*comment*/INSERT/*comment*/INTO/*comment*/{tbl}(col1,col2)"
                "VALUES/*comment*/(%s,%s/*comment*/)/*comment()*/"
                "ON DUPLICATE KEY UPDATE col1 = VALUES(col1)"
            )
            await cur.executemany(stmt, [(4, 100), (5, 200), (6, 300)])
            self.assertEqual(3, cur.rowcount)

            await cur.execute(stmt_select)
            self.maxDiff = 2000

            exp = [(4, "100"), (5, "200"), (6, "300")]
            if issubclass(type(cur), MySQLCursorRaw):
                exp = [(str(x).encode(), y.encode()) for x, y in exp]
            elif issubclass(type(cur), MySQLCursorDict):
                exp = [{"col1": x, "col2": y} for x, y in exp]
            self.assertEqual(exp, await cur.fetchall(), "Multi insert test failed")

            await cur.execute(f"TRUNCATE TABLE {tbl}")

            stmt = (
                f"INSERT INTO/*comment*/{tbl}(col1,col2)VALUES"
                "/*comment*/(%s,'/*100*/')/*comment()*/ON DUPLICATE KEY UPDATE "
                "col1 = VALUES(col1)"
            )
            await cur.executemany(stmt, [(4,), (5,)])
            self.assertEqual(2, cur.rowcount)

            await cur.execute(stmt_select)

            exp = [(4, "/*100*/"), (5, "/*100*/")]
            if issubclass(type(cur), MySQLCursorRaw):
                exp = [(str(x).encode(), y.encode()) for x, y in exp]
            elif issubclass(type(cur), MySQLCursorDict):
                exp = [{"col1": x, "col2": y} for x, y in exp]
            self.assertEqual(exp, await cur.fetchall(), "Multi insert test failed")

            self._test_execute_cleanup(cnx, tbl)

    async def _test_fetchone(self, cnx, cursor_class):
        async with await cnx.cursor(cursor_class=cursor_class) as cur:
            self.assertIsInstance(cur, cursor_class)

            with self.assertRaises(InterfaceError):
                await cur.fetchone()

            if issubclass(type(cur), MySQLCursorRaw):
                res = (bytearray(b"1"),)
            elif issubclass(type(cur), MySQLCursorDict):
                res = {"res": 1}
            else:
                res = (1,)
            await cur.execute("SELECT 1 AS res")
            self.assertEqual(res, await cur.fetchone())

    async def _test_fetchall(self, cnx, cursor_class):
        async with await cnx.cursor(cursor_class=cursor_class) as cur:
            self.assertIsInstance(cur, cursor_class)

            with self.assertRaises(InterfaceError):
                await cur.fetchall()

            if issubclass(type(cur), MySQLCursorRaw):
                res = [(bytearray(b"1"),)]
            elif issubclass(type(cur), MySQLCursorDict):
                res = [{"res": 1}]
            else:
                res = [(1,)]
            await cur.execute("SELECT 1 AS res")
            self.assertEqual(res, await cur.fetchall())

    async def _test_fetchmany(self, cnx, cursor_class):
        tbl = "myconnpy_fetch"
        await self._test_execute_setup(cnx, tbl)

        stmt_insert = f"INSERT INTO {tbl} (col1,col2) VALUES (%s,%s)"
        stmt_select = f"SELECT col1,col2 FROM {tbl} ORDER BY col1 DESC"

        async with cursor_class(connection=cnx) as cur:
            self.assertIsInstance(cur, cursor_class)
            with self.assertRaises(InterfaceError):
                await cur.fetchmany()

            nrrows = 10
            data = [(i, str(i * 100)) for i in range(0, nrrows)]
            await cur.executemany(stmt_insert, data)
            await cur.execute(stmt_select)

            exp = [(9, "900"), (8, "800"), (7, "700"), (6, "600")]
            if issubclass(type(cur), MySQLCursorRaw):
                exp = [(str(x).encode(), y.encode()) for x, y in exp]
            elif issubclass(type(cur), MySQLCursorDict):
                exp = [{"col1": x, "col2": y} for x, y in exp]
            rows = await cur.fetchmany(4)
            self.assertTrue(
                tests.cmp_result(exp, rows), "Fetching first 4 rows test failed"
            )

            exp = [(5, "500"), (4, "400"), (3, "300")]
            if issubclass(type(cur), MySQLCursorRaw):
                exp = [(str(x).encode(), y.encode()) for x, y in exp]
            elif issubclass(type(cur), MySQLCursorDict):
                exp = [{"col1": x, "col2": y} for x, y in exp]
            rows = await cur.fetchmany(3)
            self.assertTrue(
                tests.cmp_result(exp, rows), "Fetching next 3 rows test failed"
            )

            exp = [(2, "200"), (1, "100"), (0, "0")]
            if issubclass(type(cur), MySQLCursorRaw):
                exp = [(str(x).encode(), y.encode()) for x, y in exp]
            elif issubclass(type(cur), MySQLCursorDict):
                exp = [{"col1": x, "col2": y} for x, y in exp]
            rows = await cur.fetchmany(3)
            self.assertTrue(
                tests.cmp_result(exp, rows), "Fetching next 3 rows test failed"
            )
            self.assertEqual([], await cur.fetchmany())

        await self._test_execute_cleanup(cnx, tbl)

    @foreach_cnx_aio()
    async def _test_callproc(self, cnx, cursor_class):
        async with await cnx.cursor(cursor_class=cursor_class) as cur:
            self.assertIsInstance(cur, cursor_class)
            with self.assertRaises(ValueError):
                await cur.callproc(None)
                await cur.callproc("sp1", None)

        await self._callproc_setup(cnx)

        async with await cnx.cursor(cursor_class=cursor_class) as cur:
            data = (5, 4, 20)
            if issubclass(type(cur), MySQLCursorRaw):
                exp = tuple(str(val).encode() for val in data)
            elif issubclass(type(cur), MySQLCursorDict):
                exp = {f"myconnpy_sp_1_arg{i}": val for i, val in enumerate(data, 1)}
            else:
                exp = data
            result = await cur.callproc("myconnpy_sp_1", (data[0], data[1], 0))
            self.assertEqual([], cur._stored_results)
            self.assertEqual(exp, result)

            data = (6, 5, 30)
            if issubclass(type(cur), MySQLCursorRaw):
                exp = tuple(str(val).encode() for val in data)
            elif issubclass(type(cur), MySQLCursorDict):
                exp = {f"myconnpy_sp_2_arg{i}": val for i, val in enumerate(data, 1)}
            else:
                exp = data
            result = await cur.callproc("myconnpy_sp_2", (data[0], data[1], 0))
            self.assertTrue(isinstance(cur._stored_results, list))
            self.assertEqual(exp, result)

            data = ("ham", "spam", "hamspam")
            if issubclass(type(cur), MySQLCursorRaw):
                exp = tuple(str(val).encode() for val in data)
                result = await cur.callproc("myconnpy_sp_3", (exp[0], exp[1], 0))
            elif issubclass(type(cur), MySQLCursorDict):
                exp = {f"myconnpy_sp_3_arg{i}": val for i, val in enumerate(data, 1)}
                result = await cur.callproc(
                    "myconnpy_sp_3",
                    (exp["myconnpy_sp_3_arg1"], exp["myconnpy_sp_3_arg2"], 0),
                )
            else:
                exp = data
                result = await cur.callproc("myconnpy_sp_3", (exp[0], exp[1], 0))
            self.assertTrue(isinstance(cur._stored_results, list))
            self.assertEqual(exp, result)

            data = ("ham", "spam", "hamspam")
            if issubclass(type(cur), MySQLCursorRaw):
                exp = tuple(str(val).encode() for val in data)
                result = await cur.callproc("myconnpy_sp_4", (exp[0], exp[1], 0))
            elif issubclass(type(cur), MySQLCursorDict):
                exp = {f"myconnpy_sp_4_arg{i}": val for i, val in enumerate(data, 1)}
                result = await cur.callproc(
                    "myconnpy_sp_4",
                    (exp["myconnpy_sp_4_arg1"], exp["myconnpy_sp_4_arg2"], 0),
                )
            else:
                exp = data
                result = await cur.callproc("myconnpy_sp_4", (exp[0], exp[1], 0))
            self.assertTrue(isinstance(cur._stored_results, list))
            self.assertEqual(exp, result)

            data = (5,)
            if issubclass(type(cur), MySQLCursorRaw):
                exp = tuple(str(val).encode() for val in data)
            elif issubclass(type(cur), MySQLCursorDict):
                exp = {f"myconnpy_sp_5_arg{i}": val for i, val in enumerate(data, 1)}
            else:
                exp = data
            database = await cnx.get_database()
            result = await cur.callproc(f"{database}.myconnpy_sp_5", data)
            self.assertTrue(isinstance(cur._stored_results, list))
            self.assertEqual(exp, result)

            await self._callproc_cleanup(cnx)
        await cnx.close()

    async def _callproc_setup(self, cnx):
        await self._callproc_cleanup(cnx)
        database = await cnx.get_database()

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
            CREATE PROCEDURE {database}.myconnpy_sp_5(IN user_value INT)
            BEGIN
                SET @user_value = user_value;
                SELECT @user_value AS 'user_value', CURRENT_TIMESTAMP as
                'timestamp';
            END
        """

        try:
            async with await cnx.cursor() as cur:
                await cur.execute(stmt_create1)
                await cur.execute(stmt_create2)
                await cur.execute(stmt_create3)
                await cur.execute(stmt_create4)
                await cur.execute(stmt_create5)
        except Error as err:
            self.fail(f"Failed setting up test stored routine; {err}")

    async def _callproc_cleanup(self, cnx):
        database = await cnx.get_database()
        names = (
            "myconnpy_sp_1",
            "myconnpy_sp_2",
            "myconnpy_sp_3",
            "myconnpy_sp_4",
            f"{database}.myconnpy_sp_5",
        )
        stmt_drop = "DROP PROCEDURE IF EXISTS {procname}"

        try:
            async with await cnx.cursor() as cur:
                for name in names:
                    await cur.execute(stmt_drop.format(procname=name))
        except Error as err:
            self.fail(f"Failed cleaning up test stored routine; {err}")


class MySQLCursorTests(tests.MySQLConnectorAioTestCase, MySQLCursorTestsMixin):
    @foreach_cnx_aio()
    async def test__init__(self):
        exp = {
            "_connection": self.cnx,
            "_description": None,
            "_last_insert_id": None,
            "_warnings": None,
            "_warning_count": 0,
            "_executed": None,
            "_executed_list": [],
            "_stored_results": [],
            "_binary": False,
            "_rowcount": -1,
            "_nextrow": (None, None),
            "arraysize": 1,
            "description": None,
            "rowcount": -1,
            "lastrowid": None,
            "warnings": None,
            "warning_count": 0,
        }

        async with await self.cnx.cursor() as cur:
            for key, value in exp.items():
                self.assertEqual(
                    value,
                    getattr(cur, key),
                    f"Default for '{key}' did not match.",
                )
            self.assertEqual(None, cur.getlastrowid())

        # Assign an invalid connection object
        self.assertRaises(AttributeError, MySQLCursor, connection="invalid")

    @foreach_cnx_aio()
    async def test_connection_cursor(self):
        async with await self.cnx.cursor() as cur:
            self.assertIsInstance(cur, MySQLCursor)

    @foreach_cnx_aio()
    async def test_close(self):
        cur = await self.cnx.cursor()
        self.assertIsInstance(cur, MySQLCursor)
        self.assertTrue(await cur.close())
        self.assertFalse(await cur.close())

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_execute(self):
        await self._test_execute(self.cnx, MySQLCursor)

    @foreach_cnx_aio()
    async def test_executemulti(self):
        await self._test_executemulti(self.cnx, MySQLCursor)

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_executemany(self):
        await self._test_executemany(self.cnx, MySQLCursor)

    @foreach_cnx_aio()
    async def test_fetchone(self):
        await self._test_fetchone(self.cnx, MySQLCursor)

    @foreach_cnx_aio()
    async def test_fetchall(self):
        await self._test_fetchall(self.cnx, MySQLCursor)

    @foreach_cnx_aio()
    async def test_fetchmany(self):
        await self._test_fetchmany(self.cnx, MySQLCursor)

    @foreach_cnx_aio()
    async def test_fetchwarnings(self):
        async with await self.cnx.cursor() as cur:
            self.assertEqual(
                None,
                cur.fetchwarnings(),
                "There should be no warnings after initiating cursor",
            )
            exp = ["A warning"]
            cur._warnings = exp
            cur._warning_count = len(cur._warnings)
            self.assertEqual(exp, cur.warnings)
            self.assertEqual(exp, cur.fetchwarnings())

    @foreach_cnx_aio()
    async def test_stored_results(self):
        async with await self.cnx.cursor() as cur:
            self.assertEqual([], cur._stored_results)
            self.assertTrue(hasattr(cur.stored_results(), "__iter__"))
            cur._stored_results.append("abc")
            self.assertEqual("abc", next(cur.stored_results()))
            try:
                next(cur.stored_results())
            except StopIteration:
                pass
            except Exception:
                self.fail("StopIteration not raised")

    @foreach_cnx_aio()
    async def test_callproc(self):
        await self._test_callproc(self.cnx, MySQLCursor)


class MySQLCursorBufferedTests(tests.MySQLConnectorAioTestCase, MySQLCursorTestsMixin):
    @foreach_cnx_aio()
    async def test__init__(self):
        async with MySQLCursorBuffered(connection=self.cnx) as cur:
            self.assertEqual(None, await cur.execute(None, None))

    @cnx_aio_config(buffered=True)
    @foreach_cnx_aio()
    async def test_connection_cursor_options(self):
        async with await self.cnx.cursor() as cur:
            self.assertIsInstance(cur, MySQLCursorBuffered)

    @foreach_cnx_aio()
    async def test_connection_cursor(self):
        async with await self.cnx.cursor(buffered=True) as cur:
            self.assertIsInstance(cur, MySQLCursorBuffered)

    @cnx_aio_config(buffered=True, raise_on_warnings=True)
    @foreach_cnx_aio()
    async def test_raise_on_warning(self):
        async with await self.cnx.cursor(buffered=True) as cur:
            try:
                await cur.execute("SELECT 'a' + 'b'")
            except Error:
                pass
            else:
                self.fail("Did not get exception while raising warnings")

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_execute(self):
        await self._test_execute(self.cnx, MySQLCursorBuffered)

    @cnx_aio_config()
    @foreach_cnx_aio()
    async def test_executemulti(self):
        await self._test_executemulti(self.cnx, MySQLCursorBuffered)

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_executemany(self):
        await self._test_executemany(self.cnx, MySQLCursorBuffered)

    @foreach_cnx_aio()
    async def test_fetchone(self):
        await self._test_fetchone(self.cnx, MySQLCursorBuffered)

    @foreach_cnx_aio()
    async def test_fetchall(self):
        await self._test_fetchall(self.cnx, MySQLCursorBuffered)

    @foreach_cnx_aio()
    async def test_fetchmany(self):
        await self._test_fetchmany(self.cnx, MySQLCursorBuffered)

    @foreach_cnx_aio()
    async def test_callproc(self):
        await self._test_callproc(self.cnx, MySQLCursorBuffered)


class MySQLCursorRawTests(tests.MySQLConnectorAioTestCase, MySQLCursorTestsMixin):
    @foreach_cnx_aio()
    async def test__init__(self):
        async with MySQLCursorRaw(connection=self.cnx) as cur:
            self.assertEqual(None, await cur.execute(None, None))

    @cnx_aio_config(raw=True)
    @foreach_cnx_aio()
    async def test_connection_cursor_options(self):
        async with await self.cnx.cursor() as cur:
            self.assertIsInstance(cur, MySQLCursorRaw)

    @foreach_cnx_aio()
    async def test_connection_cursor(self):
        async with await self.cnx.cursor(raw=True) as cur:
            self.assertTrue(isinstance(cur, MySQLCursorRaw))

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_execute(self):
        await self._test_execute(self.cnx, MySQLCursorRaw)

    @foreach_cnx_aio()
    async def test_executemulti(self):
        await self._test_executemulti(self.cnx, MySQLCursorRaw)

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_executemany(self):
        await self._test_executemany(self.cnx, MySQLCursorRaw)

    @foreach_cnx_aio()
    async def test_fetchone(self):
        await self._test_fetchone(self.cnx, MySQLCursorRaw)

    @foreach_cnx_aio()
    async def test_fetchall(self):
        await self._test_fetchall(self.cnx, MySQLCursorRaw)

    @foreach_cnx_aio()
    async def test_fetchmany(self):
        await self._test_fetchmany(self.cnx, MySQLCursorRaw)

    @foreach_cnx_aio()
    async def test_callproc(self):
        await self._test_callproc(self.cnx, MySQLCursorRaw)


class MySQLCursorBufferedRawTests(
    tests.MySQLConnectorAioTestCase, MySQLCursorTestsMixin
):
    @foreach_cnx_aio()
    async def test__init__(self):
        async with MySQLCursorBufferedRaw(connection=self.cnx) as cur:
            self.assertEqual(None, await cur.execute(None, None))

    @cnx_aio_config(buffered=True, raw=True)
    @foreach_cnx_aio()
    async def test_connection_options(self):
        async with await self.cnx.cursor() as cur:
            self.assertIsInstance(cur, MySQLCursorBufferedRaw)

    @foreach_cnx_aio()
    async def test_connection_cursor(self):
        async with await self.cnx.cursor(buffered=True, raw=True) as cur:
            self.assertTrue(isinstance(cur, MySQLCursorBufferedRaw))

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_execute(self):
        await self._test_execute(self.cnx, MySQLCursorBufferedRaw)

    @foreach_cnx_aio()
    async def test_executemulti(self):
        await self._test_executemulti(self.cnx, MySQLCursorBufferedRaw)

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_executemany(self):
        await self._test_executemany(self.cnx, MySQLCursorBufferedRaw)

    @foreach_cnx_aio()
    async def test_fetchone(self):
        await self._test_fetchone(self.cnx, MySQLCursorBufferedRaw)

    @foreach_cnx_aio()
    async def test_fetchall(self):
        await self._test_fetchall(self.cnx, MySQLCursorBufferedRaw)

    @foreach_cnx_aio()
    async def test_fetchmany(self):
        await self._test_fetchmany(self.cnx, MySQLCursorBufferedRaw)

    @foreach_cnx_aio()
    async def test_callproc(self):
        await self._test_callproc(self.cnx, MySQLCursorBufferedRaw)


class MySQLCursorDictTests(tests.MySQLConnectorAioTestCase, MySQLCursorTestsMixin):
    @foreach_cnx_aio()
    async def test__init__(self):
        async with MySQLCursorDict(connection=self.cnx) as cur:
            self.assertEqual(None, await cur.execute(None, None))

    @foreach_cnx_aio()
    async def test_connection_cursor(self):
        async with await self.cnx.cursor(dictionary=True) as cur:
            self.assertTrue(isinstance(cur, MySQLCursorDict))

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_execute(self):
        await self._test_execute(self.cnx, MySQLCursorDict)

    @foreach_cnx_aio()
    async def test_executemulti(self):
        await self._test_executemulti(self.cnx, MySQLCursorDict)

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_executemany(self):
        await self._test_executemany(self.cnx, MySQLCursorDict)

    @foreach_cnx_aio()
    async def test_fetchone(self):
        await self._test_fetchone(self.cnx, MySQLCursorDict)

    @foreach_cnx_aio()
    async def test_fetchall(self):
        await self._test_fetchall(self.cnx, MySQLCursorDict)

    @foreach_cnx_aio()
    async def test_fetchmany(self):
        await self._test_fetchmany(self.cnx, MySQLCursorDict)

    @foreach_cnx_aio()
    async def test_callproc(self):
        await self._test_callproc(self.cnx, MySQLCursorDict)


class MySQLCursorBufferedDictTests(
    tests.MySQLConnectorAioTestCase, MySQLCursorTestsMixin
):
    @foreach_cnx_aio()
    async def test__init__(self):
        async with MySQLCursorBufferedDict(connection=self.cnx) as cur:
            self.assertEqual(None, await cur.execute(None, None))

    @foreach_cnx_aio()
    async def test_connection_cursor(self):
        async with await self.cnx.cursor(buffered=True, dictionary=True) as cur:
            self.assertTrue(isinstance(cur, MySQLCursorBufferedDict))

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_execute(self):
        await self._test_execute(self.cnx, MySQLCursorBufferedDict)

    @foreach_cnx_aio()
    async def test_executemulti(self):
        await self._test_executemulti(self.cnx, MySQLCursorBufferedDict)

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_executemany(self):
        await self._test_executemany(self.cnx, MySQLCursorBufferedDict)

    @foreach_cnx_aio()
    async def test_fetchone(self):
        await self._test_fetchone(self.cnx, MySQLCursorBufferedDict)

    @foreach_cnx_aio()
    async def test_fetchall(self):
        await self._test_fetchall(self.cnx, MySQLCursorBufferedDict)

    @foreach_cnx_aio()
    async def test_fetchmany(self):
        await self._test_fetchmany(self.cnx, MySQLCursorBufferedDict)

    @foreach_cnx_aio()
    async def test_callproc(self):
        await self._test_callproc(self.cnx, MySQLCursorBufferedDict)


class MySQLCursorNamedTupleTests(
    tests.MySQLConnectorAioTestCase, MySQLCursorTestsMixin
):
    @foreach_cnx_aio()
    async def test__init__(self):
        async with MySQLCursorNamedTuple(connection=self.cnx) as cur:
            self.assertEqual(None, await cur.execute(None, None))

    @foreach_cnx_aio()
    async def test_connection_cursor(self):
        async with await self.cnx.cursor(named_tuple=True) as cur:
            self.assertIsInstance(cur, MySQLCursorNamedTuple)

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_execute(self):
        await self._test_execute(self.cnx, MySQLCursorNamedTuple)

    @foreach_cnx_aio()
    async def test_executemulti(self):
        await self._test_executemulti(self.cnx, MySQLCursorNamedTuple)

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_executemany(self):
        await self._test_executemany(self.cnx, MySQLCursorNamedTuple)

    @foreach_cnx_aio()
    async def test_fetchone(self):
        await self._test_fetchone(self.cnx, MySQLCursorNamedTuple)

    @foreach_cnx_aio()
    async def test_fetchall(self):
        await self._test_fetchall(self.cnx, MySQLCursorNamedTuple)

    @foreach_cnx_aio()
    async def test_fetchmany(self):
        await self._test_fetchmany(self.cnx, MySQLCursorNamedTuple)

    @foreach_cnx_aio()
    async def test_callproc(self):
        await self._test_callproc(self.cnx, MySQLCursorNamedTuple)


class MySQLCursorBufferedNamedTupleTests(
    tests.MySQLConnectorAioTestCase, MySQLCursorTestsMixin
):
    @foreach_cnx_aio()
    async def test__init__(self):
        async with MySQLCursorBufferedNamedTuple(connection=self.cnx) as cur:
            self.assertEqual(None, await cur.execute(None, None))

    @foreach_cnx_aio()
    async def test_connection_cursor(self):
        async with await self.cnx.cursor(buffered=True, named_tuple=True) as cur:
            self.assertIsInstance(cur, MySQLCursorBufferedNamedTuple)

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_execute(self):
        await self._test_execute(self.cnx, MySQLCursorBufferedNamedTuple)

    @foreach_cnx_aio()
    async def test_executemulti(self):
        await self._test_executemulti(self.cnx, MySQLCursorBufferedNamedTuple)

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_executemany(self):
        await self._test_executemany(self.cnx, MySQLCursorBufferedNamedTuple)

    @foreach_cnx_aio()
    async def test_fetchone(self):
        await self._test_fetchone(self.cnx, MySQLCursorBufferedNamedTuple)

    @foreach_cnx_aio()
    async def test_fetchall(self):
        await self._test_fetchall(self.cnx, MySQLCursorBufferedNamedTuple)

    @foreach_cnx_aio()
    async def test_fetchmany(self):
        await self._test_fetchmany(self.cnx, MySQLCursorBufferedNamedTuple)

    @foreach_cnx_aio()
    async def test_callproc(self):
        await self._test_callproc(self.cnx, MySQLCursorBufferedNamedTuple)


class MySQLCursorPreparedTests(tests.MySQLConnectorAioTestCase, MySQLCursorTestsMixin):
    @foreach_cnx_aio()
    async def test__init__(self):
        async with MySQLCursorPrepared(connection=self.cnx) as cur:
            self.assertEqual(None, await cur.execute(None, None))

    @foreach_cnx_aio()
    async def test_connection_cursor(self):
        async with await self.cnx.cursor(prepared=True) as cur:
            self.assertIsInstance(cur, MySQLCursorPrepared)

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_execute(self):
        await self._test_execute(self.cnx, MySQLCursorPrepared)

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_executemany(self):
        await self._test_executemany(self.cnx, MySQLCursorPrepared)

    @foreach_cnx_aio()
    async def test_fetchone(self):
        await self._test_fetchone(self.cnx, MySQLCursorPrepared)

    @foreach_cnx_aio()
    async def test_fetchall(self):
        await self._test_fetchall(self.cnx, MySQLCursorPrepared)

    @foreach_cnx_aio()
    async def test_fetchmany(self):
        await self._test_fetchmany(self.cnx, MySQLCursorPrepared)


class MySQLCursorPreparedRawTests(
    tests.MySQLConnectorAioTestCase, MySQLCursorTestsMixin
):
    @foreach_cnx_aio()
    async def test__init__(self):
        async with MySQLCursorPreparedRaw(connection=self.cnx) as cur:
            self.assertEqual(None, await cur.execute(None, None))

    @foreach_cnx_aio()
    async def test_connection_cursor(self):
        async with await self.cnx.cursor(prepared=True, raw=True) as cur:
            self.assertIsInstance(cur, MySQLCursorPreparedRaw)

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_execute(self):
        await self._test_execute(self.cnx, MySQLCursorPreparedRaw)

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_executemany(self):
        await self._test_executemany(self.cnx, MySQLCursorPreparedRaw)

    @foreach_cnx_aio()
    async def test_fetchone(self):
        await self._test_fetchone(self.cnx, MySQLCursorPreparedRaw)

    @foreach_cnx_aio()
    async def test_fetchall(self):
        await self._test_fetchall(self.cnx, MySQLCursorPreparedRaw)

    @foreach_cnx_aio()
    async def test_fetchmany(self):
        await self._test_fetchmany(self.cnx, MySQLCursorPreparedRaw)


class MySQLCursorPreparedDictTests(
    tests.MySQLConnectorAioTestCase, MySQLCursorTestsMixin
):
    @foreach_cnx_aio()
    async def test__init__(self):
        async with MySQLCursorPreparedDict(connection=self.cnx) as cur:
            self.assertEqual(None, await cur.execute(None, None))

    @foreach_cnx_aio()
    async def test_connection_cursor(self):
        async with await self.cnx.cursor(prepared=True, dictionary=True) as cur:
            self.assertIsInstance(cur, MySQLCursorPreparedDict)

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_execute(self):
        await self._test_execute(self.cnx, MySQLCursorPreparedDict)

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_executemany(self):
        await self._test_executemany(self.cnx, MySQLCursorPreparedDict)

    @foreach_cnx_aio()
    async def test_fetchone(self):
        await self._test_fetchone(self.cnx, MySQLCursorPreparedDict)

    @foreach_cnx_aio()
    async def test_fetchall(self):
        await self._test_fetchall(self.cnx, MySQLCursorPreparedDict)

    @foreach_cnx_aio()
    async def test_fetchmany(self):
        await self._test_fetchmany(self.cnx, MySQLCursorPreparedDict)


class MySQLCursorPreparedNamedTupleTests(
    tests.MySQLConnectorAioTestCase, MySQLCursorTestsMixin
):
    @foreach_cnx_aio()
    async def test__init__(self):
        async with MySQLCursorPreparedNamedTuple(connection=self.cnx) as cur:
            self.assertEqual(None, await cur.execute(None, None))

    @foreach_cnx_aio()
    async def test_connection_cursor(self):
        async with await self.cnx.cursor(prepared=True, named_tuple=True) as cur:
            self.assertIsInstance(cur, MySQLCursorPreparedNamedTuple)

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_execute(self):
        await self._test_execute(self.cnx, MySQLCursorPreparedNamedTuple)

    @cnx_aio_config(get_warnings=True)
    @foreach_cnx_aio()
    async def test_executemany(self):
        await self._test_executemany(self.cnx, MySQLCursorPreparedNamedTuple)

    @foreach_cnx_aio()
    async def test_fetchone(self):
        await self._test_fetchone(self.cnx, MySQLCursorPreparedNamedTuple)

    @foreach_cnx_aio()
    async def test_fetchall(self):
        await self._test_fetchall(self.cnx, MySQLCursorPreparedNamedTuple)

    @foreach_cnx_aio()
    async def test_fetchmany(self):
        await self._test_fetchmany(self.cnx, MySQLCursorPreparedNamedTuple)
