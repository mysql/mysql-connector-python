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

"""Testing connection.CMySQLConnection class using the C Extension
"""

import unittest

import tests

from mysql.connector import cursor, cursor_cext, errors
from mysql.connector.connection import MySQLConnection
from mysql.connector.connection_cext import CMySQLConnection
from mysql.connector.constants import DEFAULT_CONFIGURATION, ClientFlag, flag_is_set


class CMySQLConnectionTests(tests.MySQLConnectorTests):
    def setUp(self):
        config = tests.get_mysql_config()
        self.cnx = CMySQLConnection(**config)

        self.pcnx = MySQLConnection(**config)

    def test__info_query(self):
        query = "SELECT 1, 'a', 2, 'b'"
        exp = (1, "a", 2, "b")
        self.assertEqual(exp, self.cnx.info_query(query))

        self.assertRaises(
            errors.InterfaceError,
            self.cnx.info_query,
            "SHOW VARIABLES LIKE '%char%'",
        )

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
        self.assertEqual(2, len(self.cnx.get_rows()[0]))
        self.cnx.free_result()

        self.cnx.cmd_query(query)
        self.assertEqual(1, len(self.cnx.get_rows(count=1)[0]))
        self.assertEqual(1, len(self.cnx.get_rows(count=1)[0]))
        self.assertEqual([], self.cnx.get_rows(count=1)[0])
        self.cnx.free_result()

    def test_cmd_init_db(self):
        query = "SELECT DATABASE()"
        self.cnx.cmd_init_db("mysql")
        self.assertEqual("mysql", self.cnx.info_query(query)[0])

        self.cnx.cmd_init_db("myconnpy")
        self.assertEqual("myconnpy", self.cnx.info_query(query)[0])

    def test_cmd_query(self):
        query = "SHOW STATUS LIKE 'Aborted_c%'"
        info = self.cnx.cmd_query(query)

        charset = 45 if tests.MYSQL_VERSION < (8, 0, 0) else 255
        exp = {
            "eof": {"status_flag": 32, "warning_count": 0},
            "columns": [
                ["Variable_name", 253, None, None, None, None, 0, 1, charset],
                ("Value", 253, None, None, None, None, 1, 0, charset),
            ],
        }

        if tests.MYSQL_VERSION >= (5, 7, 10):
            exp["columns"][0][7] = 4097
            exp["eof"]["status_flag"] = 16385

        exp["columns"][0] = tuple(exp["columns"][0])

        self.assertEqual(exp, info)

        rows = self.cnx.get_rows()[0]
        vars = [row[0] for row in rows]
        self.assertEqual(2, len(rows))

        vars.sort()
        exp = ["Aborted_clients", "Aborted_connects"]
        self.assertEqual(exp, vars)

        exp = ["Value", "Variable_name"]
        fields = [fld[0] for fld in info["columns"]]
        fields.sort()
        self.assertEqual(exp, fields)

        self.cnx.free_result()

        info = self.cnx.cmd_query("SET @a = 1")
        exp = {
            "warning_count": 0,
            "insert_id": 0,
            "affected_rows": 0,
            "server_status": 0,
            "field_count": 0,
        }
        self.assertEqual(exp, info)

    @unittest.skipIf(
        tests.MYSQL_VERSION < (5, 7, 3),
        "MySQL >= 5.7.3 is required for reset command",
    )
    def test_cmd_reset_connection(self):
        """Resets session without re-authenticating"""
        exp_session_id = self.cnx.connection_id
        self.cnx.cmd_query("SET @ham = 2")
        self.cnx.cmd_reset_connection()

        self.cnx.cmd_query("SELECT @ham")
        self.assertEqual(exp_session_id, self.cnx.connection_id)

        exp = (b"2",)
        self.assertNotEqual(exp, self.cnx.get_rows()[0][0])

    def test_connection_id(self):
        """MySQL connection ID"""
        self.assertEqual(self.cnx._cmysql.thread_id(), self.cnx.connection_id)
        self.cnx.close()
        self.assertIsNone(self.cnx.connection_id)
        self.cnx.connect()

    def test_cursor(self):
        """Test CEXT cursors."""

        class FalseCursor:
            ...

        class TrueCursor(cursor_cext.CMySQLCursor):
            ...

        self.assertRaises(
            errors.ProgrammingError, self.cnx.cursor, cursor_class=FalseCursor
        )
        self.assertTrue(
            isinstance(self.cnx.cursor(cursor_class=TrueCursor), TrueCursor)
        )

        self.assertRaises(
            errors.ProgrammingError, self.cnx.cursor, cursor_class=cursor.MySQLCursor
        )
        self.assertRaises(
            errors.ProgrammingError,
            self.cnx.cursor,
            cursor_class=cursor.MySQLCursorBuffered,
        )

        cases = [
            ({}, cursor_cext.CMySQLCursor),
            ({"buffered": True}, cursor_cext.CMySQLCursorBuffered),
            ({"raw": True}, cursor_cext.CMySQLCursorRaw),
            ({"buffered": True, "raw": True}, cursor_cext.CMySQLCursorBufferedRaw),
            ({"prepared": True}, cursor_cext.CMySQLCursorPrepared),
            ({"dictionary": True}, cursor_cext.CMySQLCursorDict),
            ({"named_tuple": True}, cursor_cext.CMySQLCursorNamedTuple),
            (
                {"dictionary": True, "buffered": True},
                cursor_cext.CMySQLCursorBufferedDict,
            ),
            (
                {"named_tuple": True, "buffered": True},
                cursor_cext.CMySQLCursorBufferedNamedTuple,
            ),
        ]
        for kwargs, exp in cases:
            self.assertTrue(isinstance(self.cnx.cursor(**kwargs), exp))

        self.assertRaises(ValueError, self.cnx.cursor, prepared=True, buffered=True)
        self.assertRaises(ValueError, self.cnx.cursor, dictionary=True, raw=True)
        self.assertRaises(ValueError, self.cnx.cursor, named_tuple=True, raw=True)

        # Test when connection is closed
        self.cnx.close()
        self.assertRaises(errors.OperationalError, self.cnx.cursor)

    def test_non_existent_database(self):
        """Test the raise of ProgrammingError when using a non existent database."""
        with self.assertRaises(errors.ProgrammingError) as context:
            self.cnx.database = "non_existent_database"
        self.assertIn("Unknown database", context.exception.msg)

    def test_set_charset_collation(self):
        """Set the character set and collation"""
        for charset in {None, "", 0}:
            # expecting default charset
            self.cnx.set_charset_collation(charset=charset)
            self.assertEqual(DEFAULT_CONFIGURATION["charset"], self.cnx.charset)

        for collation in {None, ""}:
            # expecting default charset
            self.cnx.set_charset_collation(collation=collation)
            self.assertEqual(DEFAULT_CONFIGURATION["charset"], self.cnx.charset)

    def test_character_set(self):
        # Test character set
        config = tests.get_mysql_config()

        config["charset"] = "latin1"
        with CMySQLConnection(**config) as cnx:
            self.assertEqual(8, cnx._charset_id)
            with cnx.cursor() as cur:
                cur.execute("SELECT @@character_set_client")
                res = cur.fetchone()
                self.assertTupleEqual((config["charset"],), res)

            cnx.set_charset_collation(charset="ascii", collation="ascii_general_ci")
            self.assertEqual(11, cnx._charset_id)
            with cnx.cursor() as cur:
                cur.execute("SELECT @@character_set_client, @@collation_connection")
                res = cur.fetchone()
                self.assertTupleEqual(("ascii", "ascii_general_ci"), res)

        for charset_id, charset, collation in [
            (26, "cp1250", "cp1250_general_ci"),
            (8, "latin1", "latin1_swedish_ci"),
        ]:
            config["charset"] = charset
            config["collation"] = collation
            with CMySQLConnection(**config) as cnx:
                self.assertEqual(charset_id, cnx._charset_id)
                with cnx.cursor() as cur:
                    cur.execute("SELECT @@character_set_client, @@collation_connection")
                    res = cur.fetchone()
                    self.assertTupleEqual((config["charset"], config["collation"]), res)

        config["client_flags"] = (
            ClientFlag.get_default() | ClientFlag.CAN_HANDLE_EXPIRED_PASSWORDS
        )
        _ = config.pop("charset")
        config["collation"] = "ascii_general_ci"
        with CMySQLConnection(**config) as cnx:
            self.assertEqual(11, cnx._charset_id)
            with cnx.cursor() as cur:
                cur.execute("SELECT @@collation_connection")
                res = cur.fetchone()
                self.assertTupleEqual((config["collation"],), res)

        _ = config.pop("collation")
        config["charset"] = "latin1"
        with CMySQLConnection(**config) as cnx:
            self.assertEqual(8, cnx._charset_id)
            with cnx.cursor() as cur:
                cur.execute("SELECT @@character_set_client")
                res = cur.fetchone()
                self.assertTupleEqual((config["charset"],), res)


@unittest.skipIf(
    tests.MYSQL_EXTERNAL_SERVER,
    "Test not available for external MySQL servers",
)
class WL13335(tests.MySQLConnectorTests):
    """WL#13335: Avoid set config values with flag CAN_HANDLE_EXPIRED_PASSWORDS"""

    def setUp(self):
        self.config = tests.get_mysql_config()
        cnx = CMySQLConnection(**self.config)
        self.user = "expired_pw_user"
        self.passw = "i+QEqGkFr8h5"
        self.hosts = ["localhost", "127.0.0.1"]
        for host in self.hosts:
            cnx.cmd_query(
                "CREATE USER '{}'@'{}' IDENTIFIED BY "
                "'{}'".format(self.user, host, self.passw)
            )
            cnx.cmd_query("GRANT ALL ON *.* TO '{}'@'{}'".format(self.user, host))
            cnx.cmd_query(
                "ALTER USER '{}'@'{}' PASSWORD EXPIRE".format(self.user, host)
            )
        cnx.close()

    def tearDown(self):
        cnx = CMySQLConnection(**self.config)
        for host in self.hosts:
            cnx.cmd_query("DROP USER '{}'@'{}'".format(self.user, host))
        cnx.close()

    @tests.foreach_cnx()
    def test_connect_with_can_handle_expired_pw_flag(self):
        cnx_config = self.config.copy()
        cnx_config["user"] = self.user
        cnx_config["password"] = self.passw
        flags = ClientFlag.get_default()
        flags |= ClientFlag.CAN_HANDLE_EXPIRED_PASSWORDS
        cnx_config["client_flags"] = flags

        # no error expected
        cnx = self.cnx.__class__(**cnx_config)

        # connection should be in sandbox mode, trying an operation should produce
        # `DatabaseError: 1862 (HY000): Your password has expired. To log in you
        # must change it using a client that supports expired passwords`
        self.assertRaises(errors.DatabaseError, cnx.cmd_query, "SELECT 1")

        cnx.close()
