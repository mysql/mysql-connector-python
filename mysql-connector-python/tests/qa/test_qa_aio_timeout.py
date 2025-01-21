# Copyright (c) 2024, Oracle and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0, as
# published by the Free Software Foundation.
#
# This program is designed to work with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation. The authors of MySQL hereby grant you an
# additional permission to link the program and your derivative works
# with the separately licensed software that they have either included with
# the program or referenced in the documentation.
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

"""
Unittests to test the functionality of read and write timeout feature for asynchronous
implementation of Connector/Python
"""


import unittest
from mysql.connector.errors import InterfaceError, ReadTimeoutError, WriteTimeoutError
import tests
import datetime
import mysql.connector.aio

try:
    anext
except NameError:
    # anext() is only available as of Python 3.10
    async def anext(ait):
        return await ait.__anext__()

ERR_NO_EXTERNAL_SERVER = "Test not available to be run using bootstrapped (local) server"


class ReadWriteTimeoutAioTests(tests.MySQLConnectorAioTestCase):

    def __init__(self, methodName="runTest"):
        # Used for all of the `write_timeout` testcases
        self.test_values = ("LONG_BLOB" * (2**21),)
        self.table_name = "test_timeouts"
        self.stmt_for_write_TLE = f"INSERT INTO {self.table_name} (my_blob) VALUES ('{self.test_values[0]}')"
        self.multi_stmt_write_TLE = f"SELECT 'abcd';SELECT 123;INSERT INTO {self.table_name} (my_blob) VALUES ('{self.test_values[0]}')"
        self.config = tests.get_mysql_config()
        self.config["unix_socket"] = None
        super().__init__(methodName)

    async def asyncSetUp(self):
        async with await mysql.connector.aio.connect(**self.config) as cnx:
            await cnx.cmd_query(
                f"CREATE TABLE IF NOT EXISTS {self.table_name} (my_blob LONGBLOB)"
            )
        return await super().asyncSetUp()

    async def asyncTearDown(self):
        async with await mysql.connector.aio.connect(**self.config) as cnx:
            await cnx.cmd_query(f"DROP TABLE IF EXISTS {self.table_name}")
        return await super().asyncTearDown()

    async def test_read_timeout_incorrect_data_type(self) -> None:
        config = self.config
        config["read_timeout"] = 55.3  # can't accept an float
        with self.assertRaises(InterfaceError) as _:
            async with await mysql.connector.aio.connect(**config) as _:
                pass
        config["read_timeout"] = -2  # can't accept a negative integer
        with self.assertRaises(InterfaceError) as _:
            async with await mysql.connector.aio.connect(**config) as _:
                pass
        config["read_timeout"] = "12"  # can't accept a string
        with self.assertRaises(InterfaceError) as _:
            async with await mysql.connector.aio.connect(**config) as _:
                pass

        config["read_timeout"] = 5
        async with await mysql.connector.aio.connect(**config) as cnx:
            with self.assertRaises(InterfaceError) as _:
                cnx.read_timeout = 55.3
            with self.assertRaises(InterfaceError) as _:
                cnx.read_timeout = -4
            with self.assertRaises(InterfaceError) as _:
                cnx.read_timeout = "1"

        async with await mysql.connector.aio.connect(**config) as cnx:
            with self.assertRaises(InterfaceError) as _:
                async with await cnx.cursor(read_timeout=12.3) as _:
                    pass
            with self.assertRaises(InterfaceError) as _:
                async with await cnx.cursor(read_timeout=-10) as _:
                    pass
            with self.assertRaises(InterfaceError) as _:
                async with await cnx.cursor(read_timeout="4") as _:
                    pass
            async with await cnx.cursor() as cur:
                with self.assertRaises(InterfaceError) as _:
                    cur.read_timeout = 55.3
                with self.assertRaises(InterfaceError) as _:
                    cur.read_timeout = -4
                with self.assertRaises(InterfaceError) as _:
                    cur.read_timeout = "1"

    async def test_write_timeout_incorrect_data_type(self) -> None:
        config = self.config
        config["write_timeout"] = 55.5  # can't accept an float
        with self.assertRaises(InterfaceError) as _:
            async with await mysql.connector.aio.connect(**config) as _:
                pass
        config["write_timeout"] = -2  # can't accept a negative integer
        with self.assertRaises(InterfaceError) as _:
            async with await mysql.connector.aio.connect(**config) as _:
                pass
        config["write_timeout"] = "12"  # can't accept a string
        with self.assertRaises(InterfaceError) as _:
            async with await mysql.connector.aio.connect(**config) as _:
                pass

        config["write_timeout"] = 12
        async with await mysql.connector.aio.connect(**config) as cnx:
            with self.assertRaises(InterfaceError) as _:
                cnx.write_timeout = 55.3
            with self.assertRaises(InterfaceError) as _:
                cnx.write_timeout = -4
            with self.assertRaises(InterfaceError) as _:
                cnx.write_timeout = "1"

        async with await mysql.connector.aio.connect(**config) as cnx:
            with self.assertRaises(InterfaceError) as _:
                async with await cnx.cursor(read_timeout=12.3) as _:
                    pass
            with self.assertRaises(InterfaceError) as _:
                async with await cnx.cursor(read_timeout=-10) as _:
                    pass
            with self.assertRaises(InterfaceError) as _:
                async with await cnx.cursor(read_timeout="4") as _:
                    pass
            async with await cnx.cursor() as cur:
                with self.assertRaises(InterfaceError) as _:
                    cur.write_timeout = 55.3
                with self.assertRaises(InterfaceError) as _:
                    cur.write_timeout = -4
                with self.assertRaises(InterfaceError) as _:
                    cur.write_timeout = "1"

    async def test_read_timeout_connection_query_TLE(self) -> None:
        try:
            config = self.config
            # Should raise a ReadTimeoutError if a read-event takes more than 1 second
            config["read_timeout"] = 1
            async with await mysql.connector.aio.connect(**config) as cnx:
                curr_time = datetime.datetime.now()
                with self.assertRaises(ReadTimeoutError) as _:
                    await cnx.cmd_query("SELECT SLEEP(10)")
                # make sure the timeout was called only after the timeout as elapsed
                time_diff = datetime.datetime.now() - curr_time
                self.assertTrue(
                    tests.cmp_timeout_tolerance(time_diff.total_seconds(), cnx.read_timeout),
                    f"ReadTimeoutError was raised after {time_diff.total_seconds()}s instead of {cnx.read_timeout}s.",
                )
                await cnx.reconnect()
                # changing the value of read_timeout on-the-fly
                cnx.read_timeout = 6
                curr_time = datetime.datetime.now()
                # now the same query executed above won't raise a timeout error
                with self.assertRaises(ReadTimeoutError) as _:
                    await cnx.cmd_query("SELECT SLEEP(10)")
                time_diff = datetime.datetime.now() - curr_time
                self.assertTrue(
                    tests.cmp_timeout_tolerance(time_diff.total_seconds(), cnx.read_timeout),
                    f"ReadTimeoutError was raised after {time_diff.total_seconds()}s instead of {cnx.read_timeout}s.",
                )
                await cnx.reconnect()
                # making sure that there's no issues with data inconsistency
                await cnx.cmd_query("SELECT 'abcd'")
                res = await cnx.get_rows()
                self.assertEqual(res[0][0][0], "abcd")
        except Exception as e:
            # unexpected failure
            self.fail(e)

    async def test_read_timeout_connection_query_success(self) -> None:
        config = self.config
        # Should raise a ReadTimeoutError if a read-event takes more than 1 second
        config["read_timeout"] = 6
        try:
            async with await mysql.connector.aio.connect(**config) as cnx:
                # This query should not raise a timeout error
                # as read_timeout is set to 6 seconds while this query
                # will take less than 3 seconds to complete
                await cnx.cmd_query("SELECT SLEEP(3)")
                _ = await cnx.get_rows()
                # changing the value of read_timeout on-the-fly
                cnx.read_timeout = 5
                # This query should not raise a timeout error
                # as read_timeout is set to 4 seconds now while this query
                # will take less than 3 seconds to complete
                await cnx.cmd_query("SELECT SLEEP(3)")
                _ = await cnx.get_rows()
                # The query below should fail as the current read_timeout is 4 seconds
                with self.assertRaises(ReadTimeoutError) as _:
                    await cnx.cmd_query("SELECT SLEEP(10)")
        except Exception as err:
            self.fail(err)

    async def test_read_timeout_cursor_query_TLE(self) -> None:
        config = self.config
        config["read_timeout"] = 5
        async with await mysql.connector.aio.connect(**config) as cnx:
            try:
                await cnx.cmd_query("SELECT SLEEP(3)")
                _ = await cnx.get_rows()
            except Exception as err:
                self.fail(err)
            async with await cnx.cursor(read_timeout=2) as cur:
                # now as the cursor's read_timeout is set to 2 seconds
                # the query below will raise an error
                curr_time = datetime.datetime.now()
                with self.assertRaises(ReadTimeoutError) as _:
                    await cur.execute("SELECT SLEEP(5)")
                # make sure the timeout was called only after the timeout as elapsed
                time_diff = datetime.datetime.now() - curr_time
                self.assertTrue(
                    tests.cmp_timeout_tolerance(time_diff.total_seconds(), cur.read_timeout),
                    f"ReadTimeoutError was raised after {time_diff.total_seconds()}s instead of {cur.read_timeout}s.",
                )
            await cnx.reconnect()
            # another isolated cursor with different timeouts
            async with await cnx.cursor(read_timeout=3) as curTwo:
                try:
                    self.assertEqual(curTwo.read_timeout, 3)
                    await curTwo.execute("SELECT SLEEP(1)")
                    _ = await curTwo.fetchall()
                except Exception as err:
                    # unexpected timeout raised
                    self.fail(err)
            # The Connection's read_timeout parameter remains unchanged
            self.assertEqual(cnx.read_timeout, config["read_timeout"])
            try:
                await cnx.cmd_query("SELECT SLEEP(3)")
                _ = await cnx.get_rows()
            except Exception as err:
                self.fail(err)

    async def test_read_timeout_cursor_query_success(self) -> None:
        config = self.config
        config["read_timeout"] = 2
        async with await mysql.connector.aio.connect(**config) as cnx:
            self.assertEqual(cnx.read_timeout, config["read_timeout"])
            async with await cnx.cursor(read_timeout=4) as curOne:
                self.assertEqual(curOne.read_timeout, 4)
                try:
                    await curOne.execute("SELECT SLEEP(3)")
                    _ = await curOne.fetchone()

                    curOne.read_timeout = 5
                    self.assertEqual(curOne.read_timeout, 5)
                    # without changing the read_timeout to 5 seconds
                    # the query below will fail
                    await curOne.execute("SELECT SLEEP(4)")
                    _ = await curOne.fetchone()
                except Exception as err:
                    # unexpected timeout raised
                    self.fail(err)
                # another isolated cursor with different timeouts
            async with await cnx.cursor(read_timeout=5) as curTwo:
                self.assertEqual(curTwo.read_timeout, 5)
                try:
                    await curTwo.execute("SELECT SLEEP(1)")
                    _ = await curTwo.fetchall()
                except Exception as err:
                    # unexpected timeout raised
                    self.fail(err)
            # making sure that connection's read_timeout remains unchanged
            self.assertEqual(cnx.read_timeout, config["read_timeout"])

    async def test_read_timeout_connection_query_iter_TLE(self) -> None:
        config = self.config
        config["read_timeout"] = 2
        async with await mysql.connector.aio.connect(**config) as cnx:
            try:
                # first scenario: 1st statement will fail
                statement = "SELECT SLEEP(3);SELECT SLEEP(1)"
                curr_time = datetime.datetime.now()
                with self.assertRaises(ReadTimeoutError) as _:
                    _ = await anext(cnx.cmd_query_iter(statement))
                # make sure the timeout was called only after the timeout as elapsed
                time_diff = datetime.datetime.now() - curr_time
                self.assertTrue(
                    tests.cmp_timeout_tolerance(time_diff.total_seconds(), cnx.read_timeout),
                    f"ReadTimeoutError was raised after {time_diff.total_seconds()}s instead of {cnx.read_timeout}s.",
                )
                await cnx.reconnect()
                # second scenario: 2nd statement will fail
                statement = "SELECT SLEEP(1);SELECT SLEEP(3)"
                stmt_exec = cnx.cmd_query_iter(statement)
                _ = await anext(stmt_exec)
                _ = await cnx.get_rows()
                curr_time = datetime.datetime.now()
                with self.assertRaises(ReadTimeoutError) as _:
                    _ = await anext(stmt_exec)
                # make sure the timeout was called only after the timeout as elapsed
                time_diff = datetime.datetime.now() - curr_time
                self.assertTrue(
                    tests.cmp_timeout_tolerance(time_diff.total_seconds(), cnx.read_timeout),
                    f"ReadTimeoutError was raised after {time_diff.total_seconds()}s instead of {cnx.read_timeout}s.",
                )
            except Exception as e:
                self.fail(e)

    async def test_read_timeout_connection_query_iter_success(self) -> None:
        config = self.config
        config["read_timeout"] = 10
        async with await mysql.connector.aio.connect(**config) as cnx:
            try:
                statement = "SELECT SLEEP(1);SELECT SLEEP(2);SELECT SLEEP(3);SELECT SLEEP(4);SELECT 'read_timeout'"
                async for _ in cnx.cmd_query_iter(statement):
                    _ = await cnx.get_rows()
            except Exception as err:
                # Unexpected timeout raised
                self.fail(err)

    async def test_read_timeout_cursor_query_multi_TLE(self) -> None:
        config = self.config
        async with await mysql.connector.aio.connect(**config) as cnx:
            async with await cnx.cursor(read_timeout=2) as cur:
                # first scenario: 1st statement will fail
                statement = "SELECT SLEEP(3);SELECT SLEEP(1)"
                curr_time = datetime.datetime.now()
                with self.assertRaises(ReadTimeoutError) as _:
                    await cur.execute(statement)
                    async for _ in cur.fetchsets():
                        pass
                # make sure the timeout was called only after the timeout as elapsed
                time_diff = datetime.datetime.now() - curr_time
                self.assertTrue(
                    tests.cmp_timeout_tolerance(time_diff.total_seconds(), cur.read_timeout),
                    f"ReadTimeoutError was raised after {time_diff.total_seconds()}s instead of {cur.read_timeout}s.",
                )
            await cnx.reconnect()
            async with await cnx.cursor(read_timeout=4) as curTwo:
                # second scenario: 2nd statement will fail
                time_taken_for_first_stmt = 1
                statement = "SELECT SLEEP(1);SELECT SLEEP(15)"
                curr_time = datetime.datetime.now()
                with self.assertRaises(ReadTimeoutError) as _:
                    await curTwo.execute(statement)
                    async for _ in curTwo.fetchsets():
                        pass
                # make sure the timeout was called only after the timeout as elapsed
                time_diff = datetime.datetime.now() - curr_time
                self.assertTrue(
                    tests.cmp_timeout_tolerance(time_diff.total_seconds(), curTwo.read_timeout + time_taken_for_first_stmt),
                    f"ReadTimeoutError was raised after {time_diff.total_seconds()}s instead of {curTwo.read_timeout}s.",
                )

    async def test_read_timeout_cursor_query_multi_success(self) -> None:
        config = self.config
        async with await mysql.connector.aio.connect(**config) as cnx:
            async with await cnx.cursor(read_timeout=6) as cur:
                try:
                    statement = "SELECT SLEEP(1);SELECT SLEEP(2);SELECT SLEEP(3);SELECT SLEEP(4);SELECT 'read_timeout'"
                    await cur.execute(statement)
                    async for _ in cur.fetchsets():
                        pass
                except Exception as err:
                    # Unexpected timeout raised
                    self.fail(err)

    async def test_read_timeout_prepared_stmt_TLE(self) -> None:
        config = self.config
        async with await mysql.connector.aio.connect(**config) as cnx:
            async with await cnx.cursor(prepared=True, read_timeout=2) as cur:
                prepared_stmt = "SELECT SLEEP(?)"
                curr_time = datetime.datetime.now()
                with self.assertRaises(ReadTimeoutError) as _:
                    await cur.execute(prepared_stmt, (3,))
                # make sure the timeout was called only after the timeout as elapsed
                time_diff = datetime.datetime.now() - curr_time
                self.assertTrue(
                    tests.cmp_timeout_tolerance(time_diff.total_seconds(), cur.read_timeout),
                    f"ReadTimeoutError was raised after {time_diff.total_seconds()}s instead of {cur.read_timeout}s.",
                )
            await cnx.reconnect()
            async with await cnx.cursor(prepared=True, read_timeout=3) as curTwo:
                try:
                    await curTwo.execute(prepared_stmt, (1,))
                    _ = await curTwo.fetchall()
                except Exception as err:
                    # Unexpected failure
                    self.fail(err)

    async def test_read_timeout_prepared_stmt_success(self) -> None:
        config = self.config
        try:
            async with await mysql.connector.aio.connect(**config) as cnx:
                async with await cnx.cursor(prepared=True, read_timeout=5) as cur:
                    prepared_stmt = "SELECT SLEEP(?)"
                    await cur.execute(prepared_stmt, (3,))
                    _ = await cur.fetchall()
        except Exception as e:
            # Unexpected failure
            self.fail(e)

    @unittest.skipIf(not tests.MYSQL_EXTERNAL_SERVER, ERR_NO_EXTERNAL_SERVER)
    async def test_write_timeout_connection_query_TLE(self) -> None:
        """
        This test requires external server (not via localhost) as reproducing a WriteTimeoutError
        a realistic environment where the server is far away from the client to make sure that
        sending a big packet to the server takes more than a second.
        """
        config = self.config
        async with await mysql.connector.aio.connect(**config) as cnx:
            cnx.write_timeout = 1
            with self.assertRaises(WriteTimeoutError) as _:
                await cnx.cmd_query(self.stmt_for_write_TLE)

    async def test_write_timeout_connection_query_success(self) -> None:
        config = self.config
        config["write_timeout"] = 15
        try:
            async with await mysql.connector.aio.connect(**config) as cnx:
                self.assertEqual(cnx.write_timeout, config["write_timeout"])
                await cnx.cmd_query(self.stmt_for_write_TLE)
        except Exception as err:
            # Unexpected timeout error raised
            self.fail(err)

    @unittest.skipIf(not tests.MYSQL_EXTERNAL_SERVER, ERR_NO_EXTERNAL_SERVER)
    async def test_write_timeout_connection_query_iter_TLE(self) -> None:
        """
        This test requires external server (not via localhost) as reproducing a WriteTimeoutError
        a realistic environment where the server is far away from the client to make sure that
        sending a big packet to the server takes more than a second.
        """
        config = self.config
        async with await mysql.connector.aio.connect(**config) as cnx:
            cnx.write_timeout = 1
            # The behaviour of write_timeout here is different
            # as the whole command is sent at once, the WriteTimeoutError
            # will be raised after the first send cmd is executed
            with self.assertRaises(WriteTimeoutError) as _:
                _ = await anext(cnx.cmd_query_iter(self.multi_stmt_write_TLE))

    async def test_write_timeout_connection_query_iter_success(self) -> None:
        config = self.config
        config["write_timeout"] = 15
        async with await mysql.connector.aio.connect(**config) as cnx:
            try:
                _ = await anext(cnx.cmd_query_iter(self.multi_stmt_write_TLE))
            except Exception as err:
                self.fail(err)

    @unittest.skipIf(not tests.MYSQL_EXTERNAL_SERVER, ERR_NO_EXTERNAL_SERVER)
    async def test_write_timeout_cursor_query_TLE(self) -> None:
        """
        This test requires external server (not via localhost) as reproducing a WriteTimeoutError
        a realistic environment where the server is far away from the client to make sure that
        sending a big packet to the server takes more than a second.
        """
        config = self.config
        config["write_timeout"] = 5
        async with await mysql.connector.aio.connect(**config) as cnx:
            self.assertEqual(cnx.write_timeout, config["write_timeout"])
            async with await cnx.cursor(write_timeout=3) as cur:
                self.assertEqual(cur.write_timeout, cur.write_timeout)
                self.assertNotEqual(
                    cur.write_timeout, cnx.write_timeout
                )
                cur.write_timeout = 1
                with self.assertRaises(WriteTimeoutError) as _:
                    await cur.execute(self.stmt_for_write_TLE)
            await cnx.reconnect()
            # another isolated cursor with different timeouts
            async with await cnx.cursor(write_timeout=12) as curTwo:
                self.assertEqual(curTwo.write_timeout, 12)
                try:
                    await curTwo.execute(self.stmt_for_write_TLE)
                except Exception as err:
                    self.fail(err)
            self.assertEqual(cnx.write_timeout, config["write_timeout"])

    async def test_write_timeout_cursor_query_success(self) -> None:
        config = self.config
        config["write_timeout"] = 2
        try:
            async with await mysql.connector.aio.connect(**config) as cnx:
                self.assertEqual(cnx.write_timeout, config["write_timeout"])
                async with await cnx.cursor(write_timeout=17) as curOne:
                    await curOne.execute(self.stmt_for_write_TLE)
                    # changing the cursor's write_timeout on-the-fly
                    curOne.write_timeout = 15
                    self.assertEqual(curOne.write_timeout, 15)
                    await curOne.execute(self.stmt_for_write_TLE)
                # another isolated cursor with different timeouts
                async with await cnx.cursor(write_timeout=5) as curTwo:
                    self.assertEqual(curTwo.write_timeout, 5)
                    await curTwo.execute("SELECT 'write_timeout'")
                    _ = await curTwo.fetchall()
        except Exception as err:
            self.fail(err)

    @unittest.skipIf(not tests.MYSQL_EXTERNAL_SERVER, ERR_NO_EXTERNAL_SERVER)
    async def test_write_timeout_cursor_query_multi_TLE(self) -> None:
        """
        This test requires external server (not via localhost) as reproducing a WriteTimeoutError
        a realistic environment where the server is far away from the client to make sure that
        sending a big packet to the server takes more than a second.
        """
        config = self.config
        try:
            async with await mysql.connector.aio.connect(**config) as cnx:
                async with await cnx.cursor() as cur:
                    cur.write_timeout = 1
                    # As the whole query is sent at once, order doesn't matter here
                    # as it might do during read_timeout scenarios
                    with self.assertRaises(WriteTimeoutError) as _:
                        await cur.execute(self.multi_stmt_write_TLE)
                        async for _ in cur.fetchsets():
                            pass
        except Exception as err:
            self.fail(err)

    async def test_write_timeout_cursor_query_multi_success(self) -> None:
        config = self.config
        try:
            async with await mysql.connector.aio.connect(**config) as cnx:
                async with await cnx.cursor(write_timeout=17) as cur:
                    # As the whole query is sent at once, order doesn't matter here
                    # as it might do during read_timeout scenarios
                    await cur.execute(self.multi_stmt_write_TLE)
                    async for _ in cur.fetchsets():
                        pass
        except Exception as err:
            self.fail(err)

    @unittest.skipIf(not tests.MYSQL_EXTERNAL_SERVER, ERR_NO_EXTERNAL_SERVER)
    async def test_write_timeout_prepared_stmt_TLE(self) -> None:
        """
        This test requires external server (not via localhost) as reproducing a WriteTimeoutError
        a realistic environment where the server is far away from the client to make sure that
        sending a big packet to the server takes more than a second.
        """
        config = self.config
        async with await mysql.connector.aio.connect(**config) as cnx:
            async with await cnx.cursor(prepared=True) as cur:
                cur.write_timeout = 1
                prepared_stmt = f"INSERT INTO {self.table_name} (my_blob) VALUES (?)"
                with self.assertRaises(WriteTimeoutError) as _:
                    await cur.execute(prepared_stmt, self.test_values)

    async def test_write_timeout_prepared_stmt_success(self) -> None:
        config = self.config
        try:
            async with await mysql.connector.aio.connect(**config) as cnx:
                async with await cnx.cursor(prepared=True, write_timeout=19) as cur:
                    prepared_stmt = f"INSERT INTO {self.table_name} (my_blob) VALUES (?)"
                    await cur.execute(prepared_stmt, self.test_values)
        except Exception as err:
            self.fail(err)
