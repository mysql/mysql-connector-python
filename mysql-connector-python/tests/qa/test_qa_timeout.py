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

"""Unittests to test the functionality of read and write timeout feature"""

import unittest
import mysql.connector
from datetime import datetime
from mysql.connector.errorcode import CR_SERVER_LOST
from mysql.connector.errors import (
    OperationalError,
    ProgrammingError,
    ReadTimeoutError,
    WriteTimeoutError,
    InterfaceError,
)
import tests

try:
    from mysql.connector.connection_cext import CMySQLConnection
except ImportError:
    # test with C-Extension
    CMySQLConnection = None

ERR_NO_CEXT = "C Extension not available"
ERR_NO_EXTERNAL_SERVER = "Test not available to be run using bootstrapped (local) server"


class ReadWriteTimeoutTests(tests.MySQLConnectorTests):

    def __init__(self, methodName="runTest"):
        # Used for all of the `write_timeout` testcases
        self.test_values = ("LONG_BLOB" * (2**20),)
        self.table_name = "test_timeouts"
        self.stmt_for_write_TLE = f"INSERT INTO {self.table_name} (my_blob) VALUES ('{self.test_values[0]}')"
        self.multi_stmt_write_TLE = f"SELECT 'abcd';SELECT 123;INSERT INTO {self.table_name} (my_blob) VALUES ('{self.test_values[0]}')"
        self.config = tests.get_mysql_config()
        self.config["unix_socket"] = None
        super().__init__(methodName)

    def setUp(self):
        config = self.config
        config["use_pure"] = True
        with mysql.connector.connect(**config) as cnx:
            cnx.cmd_query(
                f"CREATE TABLE IF NOT EXISTS {self.table_name} (my_blob LONGBLOB)"
            )

    def tearDown(self):
        config = self.config
        config["use_pure"] = True
        with mysql.connector.connect(**config) as cnx:
            cnx.cmd_query(f"DROP TABLE IF EXISTS {self.table_name}")

    @unittest.skipIf(not CMySQLConnection, ERR_NO_CEXT)
    def test_read_timeout_incorrect_data_type(self) -> None:
        config = self.config
        for use_pure in (True, False):
            config["use_pure"] = use_pure
            config["read_timeout"] = 55.3  # can't accept an float
            with self.assertRaises(InterfaceError) as _:
                with mysql.connector.connect(**config) as _:
                    pass
            config["read_timeout"] = -2  # can't accept a negative integer
            with self.assertRaises(InterfaceError) as _:
                with mysql.connector.connect(**config) as _:
                    pass
            config["read_timeout"] = "12"  # can't accept a string
            with self.assertRaises(InterfaceError) as _:
                with mysql.connector.connect(**config) as _:
                    pass

            config["read_timeout"] = 5
            with mysql.connector.connect(**config) as cnx:
                with self.assertRaises(
                    (
                        InterfaceError,
                        ProgrammingError,
                    )
                ) as _:
                    cnx.read_timeout = 55.3
                with self.assertRaises(
                    (
                        InterfaceError,
                        ProgrammingError,
                    )
                ) as _:
                    cnx.read_timeout = -4
                with self.assertRaises(
                    (
                        InterfaceError,
                        ProgrammingError,
                    )
                ) as _:
                    cnx.read_timeout = "1"

            with mysql.connector.connect(**config) as cnx:
                if use_pure:
                    with self.assertRaises(InterfaceError) as _:
                        with cnx.cursor(read_timeout=12.3) as _:
                            pass
                    with self.assertRaises(InterfaceError) as _:
                        with cnx.cursor(read_timeout=-10) as _:
                            pass
                    with self.assertRaises(InterfaceError) as _:
                        with cnx.cursor(read_timeout="4") as _:
                            pass
                with cnx.cursor() as cur:
                    with self.assertRaises(
                        (
                            InterfaceError,
                            ProgrammingError,
                        )
                    ) as _:
                        cur.read_timeout = 55.3
                    with self.assertRaises(
                        (
                            InterfaceError,
                            ProgrammingError,
                        )
                    ) as _:
                        cur.read_timeout = -4
                    with self.assertRaises(
                        (
                            InterfaceError,
                            ProgrammingError,
                        )
                    ) as _:
                        cur.read_timeout = "1"

    @unittest.skipIf(not CMySQLConnection, ERR_NO_CEXT)
    def test_write_timeout_incorrect_data_type(self) -> None:
        config = self.config
        for use_pure in (True, False):
            config["use_pure"] = use_pure
            config["write_timeout"] = 55.5  # can't accept an float
            with self.assertRaises(InterfaceError) as _:
                with mysql.connector.connect(**config) as cnx:
                    pass
            config["write_timeout"] = -2  # can't accept a negative integer
            with self.assertRaises(InterfaceError) as _:
                with mysql.connector.connect(**config) as cnx:
                    pass
            config["write_timeout"] = "12"  # can't accept a string
            with self.assertRaises(InterfaceError) as _:
                with mysql.connector.connect(**config) as cnx:
                    pass

            config["write_timeout"] = 12
            with mysql.connector.connect(**config) as cnx:
                with self.assertRaises(
                    (
                        InterfaceError,
                        ProgrammingError,
                    )
                ) as _:
                    cnx.write_timeout = 55.3
                with self.assertRaises(
                    (
                        InterfaceError,
                        ProgrammingError,
                    )
                ) as _:
                    cnx.write_timeout = -4
                with self.assertRaises(
                    (
                        InterfaceError,
                        ProgrammingError,
                    )
                ) as _:
                    cnx.write_timeout = "1"

            with mysql.connector.connect(**config) as cnx:
                if use_pure:
                    with self.assertRaises(InterfaceError) as _:
                        with cnx.cursor(write_timeout=12.3) as _:
                            pass
                    with self.assertRaises(InterfaceError) as _:
                        with cnx.cursor(write_timeout=-10) as _:
                            pass
                    with self.assertRaises(InterfaceError) as _:
                        with cnx.cursor(write_timeout="4") as _:
                            pass
                with cnx.cursor() as cur:
                    with self.assertRaises((InterfaceError, ProgrammingError,)) as _:
                        cur.write_timeout = 55.3
                    with self.assertRaises((InterfaceError, ProgrammingError,)) as _:
                        cur.write_timeout = -4
                    with self.assertRaises((InterfaceError, ProgrammingError,)) as _:
                        cur.write_timeout = "1"

    def test_read_timeout_connection_query_TLE(self) -> None:
        try:
            config = self.config
            config["use_pure"] = True
            # Should raise a ReadTimeoutError if a read-event takes more than 1 second
            config["read_timeout"] = 1
            with mysql.connector.connect(**config) as cnx:
                curr_time = datetime.now()
                with self.assertRaises(ReadTimeoutError) as _:
                    cnx.cmd_query("SELECT SLEEP(10)")
                # make sure the timeout was called only after the timeout as elapsed
                time_diff = datetime.now() - curr_time
                self.assertTrue(
                    tests.cmp_timeout_tolerance(time_diff.total_seconds(), cnx.read_timeout),
                    f"ReadTimeoutError was raised after {time_diff.total_seconds()}s instead of {cnx.read_timeout}s.",
                )
                cnx.reconnect()
                # changing the value of read_timeout on-the-fly
                cnx.read_timeout = 6
                curr_time = datetime.now()
                # now the same query executed above won't raise a timeout error
                with self.assertRaises(ReadTimeoutError) as _:
                    cnx.cmd_query("SELECT SLEEP(10)")
                time_diff = datetime.now() - curr_time
                self.assertTrue(
                    tests.cmp_timeout_tolerance(time_diff.total_seconds(), cnx.read_timeout),
                    f"ReadTimeoutError was raised after {time_diff.total_seconds()}s instead of {cnx.read_timeout}s.",
                )
                cnx.reconnect()
                # making sure that there's no issues with data inconsistency
                cnx.cmd_query("SELECT 'abcd'")
                self.assertEqual(cnx.get_rows()[0][0][0], "abcd")
        except Exception as e:
            # Unexpected failure
            self.fail(e)

    @unittest.skipIf(not CMySQLConnection, ERR_NO_CEXT)
    def test_read_timeout_cext_connection_query_TLE(self) -> None:
        try:
            config = self.config
            config["use_pure"] = False
            # Should raise a ReadTimeoutError if a read-event takes more than 1 second
            config["read_timeout"] = 1
            with mysql.connector.connect(**config) as cnx:
                curr_time = datetime.now()
                with self.assertRaises(OperationalError) as err:
                    cnx.cmd_query("SELECT SLEEP(10)")
                    self.assertTrue(err.exception.errno, CR_SERVER_LOST)
                    self.assertTrue(
                        "Lost connection to MySQL server during query"
                        in err.exception.msg
                    )
                # make sure the timeout was called only after the timeout as elapsed
                time_diff = datetime.now() - curr_time
                self.assertTrue(
                    tests.cmp_timeout_tolerance(time_diff.total_seconds(), cnx.read_timeout),
                    f"ReadTimeoutError was raised after {time_diff.total_seconds()}s instead of {cnx.read_timeout}s.",
                )

                try:
                    cnx.reconnect()
                    cnx.cmd_query("SELECT 123")
                    self.assertEqual(cnx.get_rows()[0][0][0], 123)
                except Exception as err:
                    self.fail(err)
        except Exception as err:
            self.fail(err)

    def test_read_timeout_connection_query_success(self) -> None:
        config = self.config
        config["use_pure"] = True
        config["read_timeout"] = 6
        with mysql.connector.connect(**config) as cnx:
            try:
                # This query should not raise a timeout error
                # as read_timeout is set to 6 seconds while this query
                # will take less than 3 seconds to complete
                cnx.cmd_query("SELECT SLEEP(3)")
                _ = cnx.get_rows()
            except Exception as err:
                self.fail(err)
            # changing the value of read_timeout on-the-fly
            cnx.read_timeout = 4
            try:
                # This query should not raise a timeout error
                # as read_timeout is set to 4 seconds now while this query
                # will take less than 3 seconds to complete
                cnx.cmd_query("SELECT SLEEP(3)")
                _ = cnx.get_rows()
            except Exception as err:
                self.fail(err)
            # The query below should fail as the current read_timeout is 4 seconds
            with self.assertRaises(ReadTimeoutError) as _:
                cnx.cmd_query("SELECT SLEEP(5)")

    @unittest.skipIf(not CMySQLConnection, ERR_NO_CEXT)
    def test_read_timeout_cext_connection_query_success(self) -> None:
        config = self.config
        config["use_pure"] = False
        config["read_timeout"] = 4
        with mysql.connector.connect(**config) as cnx:
            try:
                # This query should not raise a timeout error
                # as read_timeout is set to 6 seconds while this query
                # will take less than 3 seconds to complete
                cnx.cmd_query("SELECT SLEEP(3)")
                _ = cnx.get_rows()
            except Exception as err:
                self.fail(err)
            # The query below will raise an OperationalError for c-extension
            with self.assertRaises(OperationalError) as err:
                cnx.cmd_query("SELECT SLEEP(5)")
                self.assertTrue(err.exception.errno, CR_SERVER_LOST)
                self.assertTrue(
                    "Lost connection to MySQL server during query" in err.exception.msg
                )

    def test_read_timeout_cursor_query_TLE(self) -> None:
        config = self.config
        config["use_pure"] = True
        config["read_timeout"] = 7
        try:
            with mysql.connector.connect(**config) as cnx:
                cnx.cmd_query("SELECT SLEEP(3)")
                _ = cnx.get_rows()
                with cnx.cursor(read_timeout=2) as cur:
                    # now as the cursor's read_timeout is set to 2 seconds
                    # the query below will raise an error
                    curr_time = datetime.now()
                    with self.assertRaises(ReadTimeoutError) as _:
                        cur.execute("SELECT SLEEP(5)")
                    # make sure the timeout was called only after the timeout as elapsed
                    time_diff = datetime.now() - curr_time
                    self.assertTrue(
                        tests.cmp_timeout_tolerance(time_diff.total_seconds(), cur.read_timeout),
                        f"ReadTimeoutError was raised after {time_diff.total_seconds()}s instead of {cur.read_timeout}s.",
                    )
                    cnx.reconnect()
                    # Changing cursor context's read_timeout parameter on-the-fly
                    cur.read_timeout = 3
                    curr_time = datetime.now()
                    with self.assertRaises(ReadTimeoutError) as _:
                        cur.execute("SELECT SLEEP(5)")
                    time_diff = datetime.now() - curr_time
                    self.assertTrue(
                        tests.cmp_timeout_tolerance(time_diff.total_seconds(), cur.read_timeout),
                        f"ReadTimeoutError was raised after {time_diff.total_seconds()}s instead of {cur.read_timeout}s.",
                    )
                cnx.reconnect()
                # another isolated cursor with different timeouts
                with cnx.cursor(read_timeout=3) as curTwo:
                    self.assertEqual(curTwo.read_timeout, 3)
                    curTwo.execute("SELECT SLEEP(1)")
                    _ = curTwo.fetchall()
                # The Connection's read_timeout parameter remains unchanged
                self.assertEqual(cnx.read_timeout, config["read_timeout"])
                cnx.cmd_query("SELECT SLEEP(3)")
                _ = cnx.get_rows()
        except Exception as err:
            self.fail(err)

    def test_read_timeout_cursor_query_success(self) -> None:
        config = self.config
        config["use_pure"] = True
        config["read_timeout"] = 2
        try:
            with mysql.connector.connect(**config) as cnx:
                self.assertEqual(cnx.read_timeout, config["read_timeout"])
                with cnx.cursor(read_timeout=4) as curOne:
                    self.assertEqual(curOne.read_timeout, 4)
                    curOne.execute("SELECT SLEEP(3)")
                    _ = curOne.fetchone()

                    curOne.read_timeout = 5
                    self.assertEqual(curOne.read_timeout, 5)
                    # without changing the read_timeout to 5 seconds
                    # the query below will fail
                    curOne.execute("SELECT SLEEP(4)")
                    _ = curOne.fetchone()
                # another isolated cursor with different timeouts
                with cnx.cursor(read_timeout=5) as curTwo:
                    self.assertEqual(curTwo.read_timeout, 5)
                    curTwo.execute("SELECT SLEEP(1)")
                    _ = curTwo.fetchall()
                # making sure that connection's read_timeout remains unchanged
                self.assertEqual(cnx.read_timeout, config["read_timeout"])
        except Exception as err:
            self.fail(err)

    def test_read_timeout_connection_query_iter_TLE(self) -> None:
        config = self.config
        config["use_pure"] = True
        config["read_timeout"] = 2
        with mysql.connector.connect(**config) as cnx:
            # first scenario: 1st statement will fail
            statement = "SELECT SLEEP(3);SELECT SLEEP(1)"
            curr_time = datetime.now()
            with self.assertRaises(ReadTimeoutError) as _:
                _ = next(iter(cnx.cmd_query_iter(statement)))
            # make sure the timeout was called only after the timeout as elapsed
            time_diff = datetime.now() - curr_time
            self.assertTrue(
                tests.cmp_timeout_tolerance(time_diff.total_seconds(), cnx.read_timeout),
                f"ReadTimeoutError was raised after {time_diff.total_seconds()}s instead of {cnx.read_timeout}s.",
            )
            cnx.reconnect()
            # second scenario: 2nd statement will fail
            statement = "SELECT SLEEP(1);SELECT SLEEP(3)"
            stmt_exec = iter(cnx.cmd_query_iter(statement))
            _ = next(stmt_exec)
            _ = cnx.get_rows()
            curr_time = datetime.now()
            with self.assertRaises(ReadTimeoutError) as _:
                _ = next(stmt_exec)
            # make sure the timeout was called only after the timeout as elapsed
            time_diff = datetime.now() - curr_time
            self.assertTrue(
                tests.cmp_timeout_tolerance(time_diff.total_seconds(), cnx.read_timeout),
                f"ReadTimeoutError was raised after {time_diff.total_seconds()}s instead of {cnx.read_timeout}s.",
            )

    def test_read_timeout_connection_query_iter_success(self) -> None:
        config = self.config
        config["use_pure"] = True
        config["read_timeout"] = 6
        with mysql.connector.connect(**config) as cnx:
            try:
                statement = "SELECT SLEEP(1);SELECT SLEEP(2);SELECT SLEEP(3);SELECT SLEEP(4);SELECT 'read_timeout'"
                for _ in cnx.cmd_query_iter(statement):
                    _ = cnx.get_rows()
            except Exception as err:
                # Unexpected timeout raised
                self.fail(err)

    def test_read_timeout_cursor_query_multi_TLE(self) -> None:
        config = self.config
        config["use_pure"] = True
        with mysql.connector.connect(**config) as cnx:
            with cnx.cursor(read_timeout=2) as cur:
                # first scenario: 1st statement will fail
                statement = "SELECT SLEEP(3);SELECT SLEEP(1)"
                curr_time = datetime.now()
                with self.assertRaises(ReadTimeoutError) as _:
                    cur.execute(statement)
                # make sure the timeout was called only after the timeout as elapsed
                time_diff = datetime.now() - curr_time
                self.assertTrue(
                    tests.cmp_timeout_tolerance(time_diff.total_seconds(), cur.read_timeout),
                    f"ReadTimeoutError was raised after {time_diff.total_seconds()}s instead of {cur.read_timeout}s.",
                )
            cnx.reconnect()
            with cnx.cursor(read_timeout=4) as curTwo:
                # second scenario: 2nd statement will fail
                time_taken_for_first_stmt = 1
                statement = f"SELECT SLEEP({time_taken_for_first_stmt});SELECT SLEEP(5)"
                curr_time = datetime.now()
                with self.assertRaises(ReadTimeoutError) as _:
                    curTwo.execute(statement)
                    _ = list(curTwo.fetchsets())
                # make sure the timeout was called only after the timeout as elapsed
                time_diff = datetime.now() - curr_time
                self.assertTrue(
                    tests.cmp_timeout_tolerance(time_diff.total_seconds(), curTwo.read_timeout + time_taken_for_first_stmt),
                    f"ReadTimeoutError was raised after {time_diff.total_seconds()}s instead of {curTwo.read_timeout}s.",
                )

    def test_read_timeout_cursor_query_multi_success(self) -> None:
        config = self.config
        config["use_pure"] = True
        with mysql.connector.connect(**config) as cnx:
            with cnx.cursor(read_timeout=10) as cur:
                try:
                    statement = "SELECT SLEEP(1);SELECT SLEEP(2);SELECT SLEEP(3);SELECT SLEEP(4);SELECT 'read_timeout'"
                    cur.execute(statement)
                    _ = list(cur.fetchsets())
                except Exception as err:
                    # Unexpected timeout raised
                    self.fail(err)

    def test_read_timeout_prepared_stmt_TLE(self) -> None:
        config = self.config
        config["use_pure"] = True
        with mysql.connector.connect(**config) as cnx:
            with cnx.cursor(prepared=True, read_timeout=3) as cur:
                prepared_stmt = "SELECT SLEEP(?)"
                curr_time = datetime.now()
                with self.assertRaises(ReadTimeoutError) as _:
                    cur.execute(prepared_stmt, (6,))
                # make sure the timeout was called only after the timeout as elapsed
                time_diff = datetime.now() - curr_time
                self.assertTrue(
                    tests.cmp_timeout_tolerance(time_diff.total_seconds(), cur.read_timeout),
                    f"ReadTimeoutError was raised after {time_diff.total_seconds()}s instead of {cur.read_timeout}s.",
                )
                cnx.reconnect()
                try:
                    cur.execute(prepared_stmt, (1,))
                    _ = cur.fetchall()
                except Exception as err:
                    # Unexpected timeout error raised
                    self.fail(err)

    @unittest.skipIf(not CMySQLConnection, ERR_NO_CEXT)
    def test_read_timeout_cext_prepared_stmt_TLE(self) -> None:
        config = self.config
        config["use_pure"] = False
        config["read_timeout"] = 2
        with mysql.connector.connect(**config) as cnx:
            with self.assertRaises(InterfaceError) as err:
                with cnx.cursor(prepared=True) as cur:
                    prepared_stmt = "SELECT SLEEP(?)"
                    curr_time = datetime.now()
                    cur.execute(prepared_stmt, (3,))
            self.assertTrue(err.exception.errno, CR_SERVER_LOST)
            self.assertTrue(
                "Lost connection to MySQL server during query" in err.exception.msg
            )
            # make sure the timeout was called only after the timeout as elapsed
            time_diff = datetime.now() - curr_time
            self.assertTrue(
                tests.cmp_timeout_tolerance(time_diff.total_seconds(), cnx.read_timeout),
                f"ReadTimeoutError was raised after {time_diff.total_seconds()}s instead of {cnx.read_timeout}s.",
            )

    def test_read_timeout_prepared_stmt_success(self) -> None:
        config = self.config
        config["use_pure"] = True
        try:
            with mysql.connector.connect(**config) as cnx:
                with cnx.cursor(prepared=True, read_timeout=5) as cur:
                    prepared_stmt = "SELECT SLEEP(?)"
                    cur.execute(prepared_stmt, (3,))
                    _ = cur.fetchall()
        except Exception as err:
            # unexpected error raised
            self.fail(err)

    @unittest.skipIf(not CMySQLConnection, ERR_NO_CEXT)
    def test_read_timeout_cext_prepared_stmt_success(self) -> None:
        config = self.config
        config["use_pure"] = False
        config["read_timeout"] = 5
        try:
            with mysql.connector.connect(**config) as cnx:
                with cnx.cursor(prepared=True) as cur:
                    prepared_stmt = "SELECT SLEEP(?)"
                    cur.execute(prepared_stmt, (3,))
                    _ = cur.fetchall()
        except Exception as err:
            # unexpected error raised
            self.fail(err)

    @unittest.skipIf(not tests.MYSQL_EXTERNAL_SERVER, ERR_NO_EXTERNAL_SERVER)
    def test_write_timeout_connection_query_TLE(self) -> None:
        config = self.config
        config["use_pure"] = True
        with mysql.connector.connect(**config) as cnx:
            try:
                # changing the connection's write_timeout on-the-fly
                cnx.write_timeout = 15
                cnx.cmd_query(self.stmt_for_write_TLE)
                # The query below will fail
                cnx.write_timeout = 1
                with self.assertRaises(WriteTimeoutError) as _:
                    cnx.cmd_query(f'SELECT "{b"A" * 2**25}"')
            except Exception as err:
                # Unexpected timeout error raised
                self.fail(err)

    @unittest.skipIf(not CMySQLConnection, ERR_NO_CEXT)
    def test_write_timeout_cext_connection_query_success(self) -> None:
        try:
            config = self.config
            config["use_pure"] = False
            config["write_timeout"] = 5
            with mysql.connector.connect(**config) as cnx:
                cnx.cmd_query(self.stmt_for_write_TLE)
        except Exception as err:
            self.fail(err)

    def test_write_timeout_connection_query_success(self) -> None:
        config = self.config
        config["use_pure"] = True
        config["write_timeout"] = 15
        try:
            with mysql.connector.connect(**config) as cnx:
                self.assertTrue(cnx.write_timeout, config["write_timeout"])
                cnx.cmd_query(self.stmt_for_write_TLE)
        except Exception as err:
            # Unexpected timeout error raised
            self.fail(err)

    @unittest.skipIf(not tests.MYSQL_EXTERNAL_SERVER, ERR_NO_EXTERNAL_SERVER)
    def test_write_timeout_connection_query_iter_TLE(self) -> None:
        """
        This test requires external server (not via localhost) as reproducing a WriteTimeoutError
        a realistic environment where the server is far away from the client to make sure that
        sending a big packet to the server takes more than a second.
        """
        config = self.config
        config["use_pure"] = True
        with mysql.connector.connect(**config) as cnx:
            cnx.write_timeout = 1
            # The behaviour of write_timeout here is different
            # as the whole command is sent at once, the WriteTimeoutError
            # will be raised after the first send cmd is executed
            with self.assertRaises(WriteTimeoutError) as _:
                _ = next(iter(cnx.cmd_query_iter(self.multi_stmt_write_TLE)))

    def test_write_timeout_connection_query_iter_success(self) -> None:
        config = self.config
        config["use_pure"] = True
        config["write_timeout"] = 12
        with mysql.connector.connect(**config) as cnx:
            try:
                _ = next(iter(cnx.cmd_query_iter(self.multi_stmt_write_TLE)))
            except Exception as err:
                # Unexpected timeout raised
                self.fail(err)

    @unittest.skipIf(not tests.MYSQL_EXTERNAL_SERVER, ERR_NO_EXTERNAL_SERVER)
    def test_write_timeout_cursor_query_TLE(self) -> None:
        """
        This test requires external server (not via localhost) as reproducing a WriteTimeoutError
        a realistic environment where the server is far away from the client to make sure that
        sending a big packet to the server takes more than a second.
        """
        config = self.config
        config["use_pure"] = True
        config["write_timeout"] = 5
        try:
            with mysql.connector.connect(**config) as cnx:
                self.assertEqual(cnx.write_timeout, config["write_timeout"])
                with cnx.cursor(write_timeout=3) as cur:
                    self.assertEqual(cur.write_timeout, 3)
                    cur.write_timeout = 1
                    with self.assertRaises(WriteTimeoutError) as _:
                        cur.execute(self.stmt_for_write_TLE)
                    cnx.reconnect()
                    # making sure everything's fine with the cursor after reconnection
                    cur.execute("SELECT 'write_timeout'")
                    self.assertEqual(cur.fetchall()[0][0], "write_timeout")
                # another isolated cursor with different timeouts
                with cnx.cursor(write_timeout=13) as curTwo:
                    self.assertEqual(curTwo.write_timeout, 13)
                    curTwo.execute(self.stmt_for_write_TLE)
                # no change in connection's write_timeout
                self.assertEqual(cnx.write_timeout, config["write_timeout"])
        except Exception as err:
            self.fail(err)

    def test_write_timeout_cursor_query_success(self) -> None:
        config = self.config
        config["use_pure"] = True
        config["write_timeout"] = 15
        try:
            with mysql.connector.connect(**config) as cnx:
                self.assertEqual(cnx.write_timeout, config["write_timeout"])
                with cnx.cursor(write_timeout=10) as curOne:
                    self.assertTrue(curOne.write_timeout, 10)
                    curOne.execute(self.stmt_for_write_TLE)
                    # changing the cursor's write_timeout on-the-fly
                    curOne.write_timeout = 7
                    self.assertEqual(curOne.write_timeout, 7)
                    curOne.execute(self.stmt_for_write_TLE)
                # another isolated cursor with different timeouts
                with cnx.cursor(write_timeout=5) as curTwo:
                    self.assertEqual(curTwo.write_timeout, 5)
                    curTwo.execute("SELECT 'write_timeout in Connector/Python'")
                    _ = curTwo.fetchall()
        except Exception as err:
            # unexpected timeout raised
            self.fail(err)

    @unittest.skipIf(not tests.MYSQL_EXTERNAL_SERVER, ERR_NO_EXTERNAL_SERVER)
    def test_write_timeout_cursor_query_multi_TLE(self) -> None:
        """
        This test requires external server (not via localhost) as reproducing a WriteTimeoutError
        a realistic environment where the server is far away from the client to make sure that
        sending a big packet to the server takes more than a second.
        """
        config = self.config
        config["use_pure"] = True
        with mysql.connector.connect(**config) as cnx:
            with cnx.cursor() as cur:
                cur.write_timeout = 1
                with self.assertRaises(WriteTimeoutError) as _:
                    cur.execute(self.multi_stmt_write_TLE)
                    _ = list(cur.fetchsets())

    def test_write_timeout_cursor_query_multi_success(self) -> None:
        config = self.config
        config["use_pure"] = True
        with mysql.connector.connect(**config) as cnx:
            with cnx.cursor(write_timeout=3) as cur:
                # As the whole query is sent at once, order doesn't matter here
                # as it might do during read_timeout scenarios
                try:
                    cur.execute(self.multi_stmt_write_TLE)
                    _ = list(cur.fetchsets())
                except Exception as err:
                    # Unexpected timeout was raised
                    self.fail(err)

    @unittest.skipIf(not tests.MYSQL_EXTERNAL_SERVER, ERR_NO_EXTERNAL_SERVER)
    def test_write_timeout_prepared_stmt_TLE(self) -> None:
        """
        This test requires external server (not via localhost) as reproducing a WriteTimeoutError
        a realistic environment where the server is far away from the client to make sure that
        sending a big packet to the server takes more than a second.
        """
        config = self.config
        config["use_pure"] = True
        with mysql.connector.connect(**config) as cnx:
            with cnx.cursor(prepared=True) as cur:
                cur.write_timeout = 1
                prepared_stmt = f"INSERT INTO {self.table_name} (my_blob) VALUES (?)"
                with self.assertRaises(WriteTimeoutError) as _:
                    cur.execute(prepared_stmt, self.test_values)
                cnx.reconnect()
                cur.write_timeout = 5
                try:
                    cur.execute(prepared_stmt, self.test_values)
                except Exception as err:
                    # Unexpected error raised
                    self.fail(err)

    def test_write_timeout_prepared_stmt_success(self) -> None:
        config = self.config
        config["use_pure"] = True
        with mysql.connector.connect(**config) as cnx:
            with cnx.cursor(prepared=True, write_timeout=5) as cur:
                prepared_stmt = f"INSERT INTO {self.table_name} (my_blob) VALUES (?)"
                try:
                    cur.execute(prepared_stmt, self.test_values)
                except Exception as err:
                    # Unexpected timeout was raised
                    self.fail(err)

    @unittest.skipIf(not CMySQLConnection, ERR_NO_CEXT)
    def test_write_timeout_cext_prepared_stmt_success(self) -> None:
        config = self.config
        config["use_pure"] = False
        config["write_timeout"] = 5
        prepared_stmt = f"INSERT INTO {self.table_name} (my_blob) VALUES (?)"
        try:
            with mysql.connector.connect(**config) as cnx:
                with cnx.cursor(prepared=True) as cur:
                    cur.execute(prepared_stmt, self.test_values)
                    _ = cur.fetchall()
        except Exception as err:
            # unexpected error raised
            self.fail(err)
