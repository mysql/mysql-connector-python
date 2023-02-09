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

"""Unittests for mysql.connector.aio.connection."""

import asyncio
import copy
import io
import logging
import os
import platform
import socket
import sys
import unittest
import warnings

from datetime import datetime, time
from decimal import Decimal

import tests

from mysql.connector.abstracts import DEFAULT_CONFIGURATION
from mysql.connector.aio import HAVE_DNSPYTHON, connect
from mysql.connector.aio.charsets import charsets
from mysql.connector.aio.connection import MySQLConnection
from mysql.connector.aio.cursor import (
    MySQLCursor,
    MySQLCursorBuffered,
    MySQLCursorBufferedDict,
    MySQLCursorBufferedNamedTuple,
    MySQLCursorBufferedRaw,
    MySQLCursorDict,
    MySQLCursorNamedTuple,
    MySQLCursorPrepared,
    MySQLCursorRaw,
)
from mysql.connector.aio.network import MySQLTcpSocket, MySQLUnixSocket
from mysql.connector.constants import ClientFlag, RefreshOption, ServerCmd, ServerFlag
from mysql.connector.conversion import MySQLConverter, MySQLConverterBase
from mysql.connector.errors import (
    DatabaseError,
    Error,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from mysql.connector.utils import linux_distribution
from mysql.connector.version import LICENSE, VERSION
from tests import (
    AioDummySocket,
    MySQLConnectorAioTestCase,
    foreach_cnx_aio,
    get_mysql_config,
)

from . import check_tls_versions_support, get_scenarios_matrix
from .test_connection import (
    COLUMNS_SINGLE,
    COLUMNS_SINGLE_COUNT,
    EOF_PACKET,
    EOF_PACKET_RESULT,
    ERR_PACKET,
    OK_PACKET,
    OK_PACKET_RESULT,
)

try:
    from mysql.connector.aio.connection_cext import HAVE_CMYSQL, CMySQLConnection
    from mysql.connector.aio.cursor_cext import CMySQLCursor, CMySQLCursorBufferedRaw
except ImportError:
    # Test without C Extension
    CMySQLConnection = None
    HAVE_CMYSQL = False

try:
    import ssl

    TLS_V1_3_SUPPORTED = hasattr(ssl, "HAS_TLSv1_3") and ssl.HAS_TLSv1_3
except ImportError:
    # If import fails, we don't have SSL support.
    TLS_V1_3_SUPPORTED = False
    ssl = None

try:
    import gssapi
except ImportError:
    gssapi = None

try:
    anext
except NameError:
    # anext() is only available as of Python 3.10
    async def anext(ait):
        return await ait.__anext__()


LOGGER = logging.getLogger(tests.LOGGER_NAME)


class MySQLConnectionAioTests(MySQLConnectorAioTestCase):
    @foreach_cnx_aio()
    async def test_init(self):
        cls = self.cnx.__class__
        cnx = cls()
        exp = {
            "_user": None,
            "_password": "",
            "_host": "127.0.0.1",
            "_port": 3306,
            "_database": None,
            "_password1": "",
            "_password2": "",
            "_password3": "",
            "_unix_socket": None,
            "_connection_timeout": None,
            "_compress": False,
            "_consume_results": False,
            "_ssl_active": False,
            "_ssl_disabled": False,
            "_ssl_ca": None,
            "_ssl_cert": None,
            "_ssl_key": None,
            "_ssl_verify_cert": False,
            "_ssl_verify_identity": False,
            "_auth_plugin": None,
            "_auth_plugin_class": None,
            "_converter_class": MySQLConverter,
            "_client_flags": ClientFlag.get_default(),
            "_get_warnings": False,
            "_raise_on_warnings": False,
            "_buffered": False,
            "_raw": False,
            "_have_next_result": False,
            "_unread_result": False,
        }
        for key, value in exp.items():
            self.assertEqual(
                value, cnx.__dict__[key], msg=f"Default for '{key}' did not match"
            )

    @foreach_cnx_aio()
    async def test_get_self(self):
        self.assertEqual(self.cnx, self.cnx.get_self())

    @foreach_cnx_aio(MySQLConnection)
    async def test__send_cmd(self):
        """Send a command to MySQL"""
        cmd = ServerCmd.QUERY
        arg = "SELECT 1".encode("utf-8")
        pktnr = 2

        await self.cnx._socket.close_connection()
        self.cnx._socket._writer = None
        with self.assertRaises(OperationalError):
            await self.cnx._send_cmd(cmd, arg, pktnr)

        dummy_socket = AioDummySocket()
        await dummy_socket.set_connection_socket(self.cnx)
        exp = OK_PACKET
        self.cnx._socket._writer.add_packet(exp)
        res = await self.cnx._send_cmd(cmd, arg, pktnr, expect_response=True)
        self.assertEqual(exp, res)

        # Send an unknown command, the result should be an error packet
        exp = ERR_PACKET
        self.cnx._socket._writer.reset()
        self.cnx._socket._writer.add_packet(exp)
        res = await self.cnx._send_cmd(90, b"spam", 0)
        self.assertEqual(exp, res)

    @foreach_cnx_aio(MySQLConnection)
    async def test__handle_server_status(self):
        """Handle the server/status flags"""
        cases = [
            # (serverflag, attribute_name, value when set, value when unset)
            (ServerFlag.MORE_RESULTS_EXISTS, "_have_next_result", True, False),
            (ServerFlag.STATUS_IN_TRANS, "_in_transaction", True, False),
        ]
        for flag, attr, when_set, when_unset in cases:
            setattr(self.cnx, attr, when_unset)
            self.cnx._handle_server_status(flag)
            self.assertEqual(when_set, getattr(self.cnx, attr))
            self.cnx._handle_server_status(0)
            self.assertEqual(when_unset, getattr(self.cnx, attr))

    @foreach_cnx_aio(MySQLConnection)
    async def test__handle_ok(self):
        """Handle an OK-packet sent by MySQL"""
        self.assertEqual(OK_PACKET_RESULT, self.cnx._handle_ok(OK_PACKET))
        self.assertRaises(ProgrammingError, self.cnx._handle_ok, ERR_PACKET)
        self.assertRaises(InterfaceError, self.cnx._handle_ok, EOF_PACKET)

        # Test for multiple results
        self.cnx._have_next_result = False
        packet = OK_PACKET[:-4] + b"\x08" + OK_PACKET[-3:]
        self.cnx._handle_ok(packet)
        self.assertTrue(self.cnx.have_next_result)

    @foreach_cnx_aio(MySQLConnection)
    async def test__handle_eof(self):
        """Handle an EOF-packet sent by MySQL"""
        self.assertEqual(EOF_PACKET_RESULT, self.cnx._handle_eof(EOF_PACKET))
        self.assertRaises(ProgrammingError, self.cnx._handle_eof, ERR_PACKET)
        self.assertRaises(InterfaceError, self.cnx._handle_eof, OK_PACKET)

        # Test for multiple results
        self.cnx._have_next_result = False
        packet = EOF_PACKET[:-2] + b"\x08" + EOF_PACKET[-1:]
        self.cnx._handle_eof(packet)
        self.assertTrue(self.cnx.have_next_result)

    @foreach_cnx_aio(MySQLConnection)
    async def test__handle_result(self):
        """Handle the result after sending a command to MySQL"""
        with self.assertRaises(InterfaceError):
            await self.cnx._handle_result("\x00")
        with self.assertRaises(InterfaceError):
            await self.cnx._handle_result(None)
        dummy_socket = AioDummySocket()
        await dummy_socket.set_connection_socket(self.cnx)
        self.cnx._allow_local_infile = 1
        eof_packet = EOF_PACKET
        eof_packet[3] = 3
        self.cnx._socket._writer.add_packets([COLUMNS_SINGLE, eof_packet])
        exp = {
            "eof": {"status_flag": 0, "warning_count": 0},
            "columns": [("1", 8, None, None, None, None, 0, 129, 63)],
        }
        res = await self.cnx._handle_result(COLUMNS_SINGLE_COUNT)
        self.assertEqual(exp, res)

        self.assertEqual(EOF_PACKET_RESULT, await self.cnx._handle_result(EOF_PACKET))
        self.cnx._unread_result = False

        # Handle LOAD DATA INFILE
        self.cnx._socket._writer.reset()
        packet = bytearray(
            b"\x1A\x00\x00\x01\xfb"
            b"\x74\x65\x73\x74\x73\x2f\x64\x61\x74\x61\x2f\x6c\x6f"
            b"\x63\x61\x6c\x5f\x64\x61\x74\x61\x2e\x63\x73\x76"
        )
        self.cnx._socket._writer.add_packet(
            bytearray(
                b"\x37\x00\x00\x04\x00\x06\x00\x01\x00\x00\x00\x2f\x52"
                b"\x65\x63\x6f\x72\x64\x73\x3a\x20\x36\x20\x20\x44\x65"
                b"\x6c\x65\x74\x65\x64\x3a\x20\x30\x20\x20\x53\x6b\x69"
                b"\x70\x70\x65\x64\x3a\x20\x30\x20\x20\x57\x61\x72\x6e"
                b"\x69\x6e\x67\x73\x3a\x20\x30"
            )
        )
        exp = {
            "info_msg": "Records: 6  Deleted: 0  Skipped: 0  Warnings: 0",
            "insert_id": 0,
            "field_count": 0,
            "warning_count": 0,
            "status_flag": 1,
            "affected_rows": 6,
        }
        self.assertEqual(exp, await self.cnx._handle_result(packet))

        exp = [
            bytearray(
                b"\x47\x00\x00\x04\x31\x09\x63\x31\x5f\x31\x09\x63\x32"
                b"\x5f\x31\x0a\x32\x09\x63\x31\x5f\x32\x09\x63\x32\x5f"
                b"\x32\x0a\x33\x09\x63\x31\x5f\x33\x09\x63\x32\x5f\x33"
                b"\x0a\x34\x09\x63\x31\x5f\x34\x09\x63\x32\x5f\x34\x0a"
                b"\x35\x09\x63\x31\x5f\x35\x09\x63\x32\x5f\x35\x0a\x36"
                b"\x09\x63\x31\x5f\x36\x09\x63\x32\x5f\x36"
            ),
            bytearray(b"\x00\x00\x00\x05"),
        ]
        self.assertEqual(exp, self.cnx._socket._writer._client_sends)

        # Column count is invalid ( more than 4096)
        self.cnx._socket._writer.reset()
        packet = bytearray(b"\x01\x00\x00\x01\xfc\xff\xff\xff")
        with self.assertRaises(InterfaceError):
            await self.cnx._handle_result(packet)

        # First byte in first packet is wrong
        self.cnx._socket._writer.add_packets(
            [
                bytearray(
                    b"\x00\x00\x00\x02\x03\x64\x65\x66\x00\x00\x00\x01"
                    b"\x31\x00\x0c\x3f\x00\x01\x00\x00\x00\x08\x81\x00"
                    b"\x00\x00\x00"
                ),
                bytearray(b"\x05\x00\x00\x03\xfe\x00\x00\x00\x00"),
            ]
        )

        with self.assertRaises(InterfaceError):
            await self.cnx._handle_result(b"\x01\x00\x00\x01\x00")

    def __helper_get_rows_buffer(self, toggle_next_result=False):
        self.cnx._socket._writer.reset()

        packets = [
            bytearray(b"\x07\x00\x00\x04\x06\x4d\x79\x49\x53\x41\x4d"),
            bytearray(b"\x07\x00\x00\x05\x06\x49\x6e\x6e\x6f\x44\x42"),
            bytearray(b"\x0a\x00\x00\x06\x09\x42\x4c" b"\x41\x43\x4b\x48\x4f\x4c\x45"),
            bytearray(b"\x04\x00\x00\x07\x03\x43\x53\x56"),
            bytearray(b"\x07\x00\x00\x08\x06\x4d\x45\x4d\x4f\x52\x59"),
            bytearray(b"\x0a\x00\x00\x09\x09\x46\x45" b"\x44\x45\x52\x41\x54\x45\x44"),
            bytearray(b"\x08\x00\x00\x0a\x07\x41\x52\x43\x48\x49\x56\x45"),
            bytearray(
                b"\x0b\x00\x00\x0b\x0a\x4d\x52" b"\x47\x5f\x4d\x59\x49\x53\x41\x4d"
            ),
            bytearray(b"\x05\x00\x00\x0c\xfe\x00\x00\x20\x00"),
        ]

        if toggle_next_result:
            packets[-1] = packets[-1][:-2] + b"\x08" + packets[-1][-1:]

        self.cnx._socket._writer.add_packets(packets)
        self.cnx.unread_result = True

    @foreach_cnx_aio()
    async def test_get_rows(self):
        """Get rows from the MySQL resultset"""
        dummy_socket = AioDummySocket()
        await dummy_socket.set_connection_socket(self.cnx)

        self.__helper_get_rows_buffer()
        exp = (
            [
                (b"MyISAM",),
                (b"InnoDB",),
                (b"BLACKHOLE",),
                (b"CSV",),
                (b"MEMORY",),
                (b"FEDERATED",),
                (b"ARCHIVE",),
                (b"MRG_MYISAM",),
            ],
            {"status_flag": 32, "warning_count": 0},
        )
        res = await self.cnx.get_rows(raw=True)
        self.assertEqual(exp, res)

        self.__helper_get_rows_buffer()
        rows = exp[0]
        i = 0
        while i < len(rows):
            exp = (rows[i : i + 2], None)
            res = await self.cnx.get_rows(count=2, raw=True)
            self.assertEqual(exp, res)
            i += 2
        exp = ([], {"status_flag": 32, "warning_count": 0})
        self.assertEqual(exp, await self.cnx.get_rows(raw=True))

        # Test unread results
        self.cnx.unread_result = False
        with self.assertRaises(InternalError):
            await self.cnx.get_rows()

        # Test multiple results
        self.cnx._have_next_results = False
        self.__helper_get_rows_buffer(toggle_next_result=True)
        exp = {"status_flag": 8, "warning_count": 0}
        res = await self.cnx.get_rows(raw=True)
        self.assertEqual(exp, res[-1])
        self.assertTrue(self.cnx.have_next_result)

    @foreach_cnx_aio()
    async def test_get_row(self):
        """Get a row from the MySQL resultset"""
        dummy_socket = AioDummySocket()
        await dummy_socket.set_connection_socket(self.cnx)

        self.__helper_get_rows_buffer()
        expall = (
            [
                (b"MyISAM",),
                (b"InnoDB",),
                (b"BLACKHOLE",),
                (b"CSV",),
                (b"MEMORY",),
                (b"FEDERATED",),
                (b"ARCHIVE",),
                (b"MRG_MYISAM",),
            ],
            {"status_flag": 32, "warning_count": 0},
        )

        rows = expall[0]
        for row in rows:
            res = await self.cnx.get_row(raw=True)
            exp = (row, None)
            self.assertEqual(exp, res)
        exp = ([], {"status_flag": 32, "warning_count": 0})
        self.assertEqual(exp, await self.cnx.get_rows(raw=True))

    @foreach_cnx_aio()
    async def test_cmd_init_db(self):
        """Send the Init_db-command to MySQL"""
        dummy_socket = AioDummySocket()
        await dummy_socket.set_connection_socket(self.cnx)

        self.cnx._socket._writer.add_packet(OK_PACKET)
        self.assertEqual(OK_PACKET_RESULT, await self.cnx.cmd_init_db("test"))

        self.cnx._socket._writer.reset()
        self.cnx._socket._writer.add_packet(
            bytearray(
                b"\x2c\x00\x00\x01\xff\x19\x04\x23\x34\x32\x30\x30"
                b"\x30\x55\x6e\x6b\x6e\x6f\x77\x6e\x20\x64\x61\x74"
                b"\x61\x62\x61\x73\x65\x20\x27\x75\x6e\x6b\x6e\x6f"
                b"\x77\x6e\x5f\x64\x61\x74\x61\x62\x61\x73\x65\x27"
            )
        )
        with self.assertRaises(ProgrammingError):
            await self.cnx.cmd_init_db("unknown_database")

    @foreach_cnx_aio()
    async def test_cmd_query(self):
        """Send a query to MySQL"""
        dummy_socket = AioDummySocket()
        await dummy_socket.set_connection_socket(self.cnx)

        self.cnx._socket._writer.add_packet(OK_PACKET)
        res = await self.cnx.cmd_query("SET AUTOCOMMIT = OFF")
        self.assertEqual(OK_PACKET_RESULT, res)

        packets = [
            COLUMNS_SINGLE_COUNT,
            COLUMNS_SINGLE,
            bytearray(b"\x05\x00\x00\x03\xfe\x00\x00\x00\x00"),
        ]

        # query = "SELECT 1"
        self.cnx._socket._writer.reset()
        self.cnx._socket._writer.add_packets(packets)
        exp = {
            "eof": {"status_flag": 0, "warning_count": 0},
            "columns": [("1", 8, None, None, None, None, 0, 129, 63)],
        }
        res = await self.cnx.cmd_query("SELECT 1")
        self.assertEqual(exp, res)
        with self.assertRaises(InternalError):
            await self.cnx.cmd_query("SELECT 2")
        self.cnx.unread_result = False

        # Forge the packets so the multiple result flag is set
        packets[-1] = packets[-1][:-2] + b"\x08" + packets[-1][-1:]
        self.cnx._socket._writer.reset()
        self.cnx._socket._writer.add_packets(packets)
        with self.assertRaises(InterfaceError):
            await self.cnx.cmd_query("SELECT 1")

    @foreach_cnx_aio()
    async def test_cmd_query_iter(self):
        """Send queries to MySQL"""
        dummy_socket = AioDummySocket()
        await dummy_socket.set_connection_socket(self.cnx)

        self.cnx._socket._writer.add_packet(OK_PACKET)
        res = await anext(self.cnx.cmd_query_iter("SET AUTOCOMMIT = OFF"))
        self.assertEqual(OK_PACKET_RESULT, res)

        packets = [
            COLUMNS_SINGLE_COUNT,
            COLUMNS_SINGLE,
            bytearray(b"\x05\x00\x00\x03\xfe\x00\x00\x08\x00"),
            bytearray(b"\x02\x00\x00\x04\x01\x31"),
            bytearray(b"\x05\x00\x00\x05\xfe\x00\x00\x08\x00"),
            bytearray(b"\x07\x00\x00\x06\x00\x01\x00\x08\x00\x00\x00"),
            bytearray(b"\x01\x00\x00\x07\x01"),
            bytearray(
                b"\x17\x00\x00\x08\x03\x64\x65\x66\x00\x00\x00\x01"
                b"\x32\x00\x0c\x3f\x00\x01\x00\x00\x00\x08\x81\x00"
                b"\x00\x00\x00"
            ),
            bytearray(b"\x05\x00\x00\x09\xfe\x00\x00\x00\x00"),
            bytearray(b"\x02\x00\x00\x0a\x01\x32"),
            bytearray(b"\x05\x00\x00\x0b\xfe\x00\x00\x00\x00"),
        ]
        exp = [
            {
                "columns": [("1", 8, None, None, None, None, 0, 129, 63)],
                "eof": {"status_flag": 8, "warning_count": 0},
            },
            ([(1,)], {"status_flag": 8, "warning_count": 0}),
            {
                "affected_rows": 1,
                "field_count": 0,
                "insert_id": 0,
                "status_flag": 8,
                "warning_count": 0,
            },
            {
                "columns": [("2", 8, None, None, None, None, 0, 129, 63)],
                "eof": {"status_flag": 0, "warning_count": 0},
            },
            ([(2,)], {"status_flag": 0, "warning_count": 0}),
        ]
        self.cnx._socket._writer.reset()
        self.cnx._socket._writer.add_packets(packets)
        results = []
        stmt = "SELECT 1; SELECT 2".encode("utf-8")
        async for result in self.cnx.cmd_query_iter(stmt):
            results.append(result)
            if "columns" in result:
                results.append(await self.cnx.get_rows())
        self.assertEqual(exp, results)

    @foreach_cnx_aio()
    async def test_cmd_refresh(self):
        """Send the Refresh-command to MySQL"""
        refresh_options = (
            RefreshOption.GRANT,
            RefreshOption.LOG,
            RefreshOption.TABLES,
            RefreshOption.HOST,
            RefreshOption.STATUS,
            RefreshOption.REPLICA,
        )
        for option in refresh_options:
            self.assertEqual(OK_PACKET_RESULT, await self.cnx.cmd_refresh(option))

        # Test combined options
        options = RefreshOption.LOG | RefreshOption.STATUS
        self.assertEqual(OK_PACKET_RESULT, await self.cnx.cmd_refresh(options))

    @foreach_cnx_aio()
    async def test_cmd_quit(self):
        """Send the Quit-command to MySQL"""
        dummy_socket = AioDummySocket()
        await dummy_socket.set_connection_socket(self.cnx)

        self.assertEqual(b"\x01", await self.cnx.cmd_quit())

    @foreach_cnx_aio()
    async def test_cmd_statistics(self):
        """Send the Statistics-command to MySQL"""
        dummy_socket = AioDummySocket()
        await dummy_socket.set_connection_socket(self.cnx)

        goodpkt = bytearray(
            b"\x88\x00\x00\x01\x55\x70\x74\x69\x6d\x65\x3a\x20"
            b"\x31\x34\x36\x32\x34\x35\x20\x20\x54\x68\x72\x65"
            b"\x61\x64\x73\x3a\x20\x32\x20\x20\x51\x75\x65\x73"
            b"\x74\x69\x6f\x6e\x73\x3a\x20\x33\x36\x33\x35\x20"
            b"\x20\x53\x6c\x6f\x77\x20\x71\x75\x65\x72\x69\x65"
            b"\x73\x3a\x20\x30\x20\x20\x4f\x70\x65\x6e\x73\x3a"
            b"\x20\x33\x39\x32\x20\x20\x46\x6c\x75\x73\x68\x20"
            b"\x74\x61\x62\x6c\x65\x73\x3a\x20\x31\x20\x20\x4f"
            b"\x70\x65\x6e\x20\x74\x61\x62\x6c\x65\x73\x3a\x20"
            b"\x36\x34\x20\x20\x51\x75\x65\x72\x69\x65\x73\x20"
            b"\x70\x65\x72\x20\x73\x65\x63\x6f\x6e\x64\x20\x61"
            b"\x76\x67\x3a\x20\x30\x2e\x32\x34"
        )
        self.cnx._socket._writer.add_packet(goodpkt)
        exp = {
            "Uptime": 146245,
            "Open tables": 64,
            "Queries per second avg": Decimal("0.24"),
            "Slow queries": 0,
            "Threads": 2,
            "Questions": 3635,
            "Flush tables": 1,
            "Opens": 392,
        }
        self.assertEqual(exp, await self.cnx.cmd_statistics())

        badpkt = bytearray(
            b"\x88\x00\x00\x01\x55\x70\x74\x69\x6d\x65\x3a\x20"
            b"\x31\x34\x36\x32\x34\x35\x20\x54\x68\x72\x65"
            b"\x61\x64\x73\x3a\x20\x32\x20\x20\x51\x75\x65\x73"
            b"\x74\x69\x6f\x6e\x73\x3a\x20\x33\x36\x33\x35\x20"
            b"\x20\x53\x6c\x6f\x77\x20\x71\x75\x65\x72\x69\x65"
            b"\x73\x3a\x20\x30\x20\x20\x4f\x70\x65\x6e\x73\x3a"
            b"\x20\x33\x39\x32\x20\x20\x46\x6c\x75\x73\x68\x20"
            b"\x74\x61\x62\x6c\x65\x73\x3a\x20\x31\x20\x20\x4f"
            b"\x70\x65\x6e\x20\x74\x61\x62\x6c\x65\x73\x3a\x20"
            b"\x36\x34\x20\x20\x51\x75\x65\x72\x69\x65\x73\x20"
            b"\x70\x65\x72\x20\x73\x65\x63\x6f\x6e\x64\x20\x61"
            b"\x76\x67\x3a\x20\x30\x2e\x32\x34"
        )
        self.cnx._socket._writer.reset()
        self.cnx._socket._writer.add_packet(badpkt)
        with self.assertRaises(InterfaceError):
            await self.cnx.cmd_statistics()

        badpkt = bytearray(
            b"\x88\x00\x00\x01\x55\x70\x74\x69\x6d\x65\x3a\x20"
            b"\x55\x70\x36\x32\x34\x35\x20\x20\x54\x68\x72\x65"
            b"\x61\x64\x73\x3a\x20\x32\x20\x20\x51\x75\x65\x73"
            b"\x74\x69\x6f\x6e\x73\x3a\x20\x33\x36\x33\x35\x20"
            b"\x20\x53\x6c\x6f\x77\x20\x71\x75\x65\x72\x69\x65"
            b"\x73\x3a\x20\x30\x20\x20\x4f\x70\x65\x6e\x73\x3a"
            b"\x20\x33\x39\x32\x20\x20\x46\x6c\x75\x73\x68\x20"
            b"\x74\x61\x62\x6c\x65\x73\x3a\x20\x31\x20\x20\x4f"
            b"\x70\x65\x6e\x20\x74\x61\x62\x6c\x65\x73\x3a\x20"
            b"\x36\x34\x20\x20\x51\x75\x65\x72\x69\x65\x73\x20"
            b"\x70\x65\x72\x20\x73\x65\x63\x6f\x6e\x64\x20\x61"
            b"\x76\x67\x3a\x20\x30\x2e\x32\x34"
        )
        self.cnx._socket._writer.reset()
        self.cnx._socket._writer.add_packet(badpkt)
        with self.assertRaises(InterfaceError):
            await self.cnx.cmd_statistics()

    @foreach_cnx_aio()
    async def test_cmd_debug(self):
        """Send the Debug-command to MySQL"""
        dummy_socket = AioDummySocket()
        await dummy_socket.set_connection_socket(self.cnx)

        pkt = bytearray(b"\x05\x00\x00\x01\xfe\x00\x00\x00\x00")
        self.cnx._socket._writer.add_packet(pkt)
        exp = {"status_flag": 0, "warning_count": 0}
        self.assertEqual(exp, await self.cnx.cmd_debug())

        pkt = bytearray(
            b"\x47\x00\x00\x01\xff\xcb\x04\x23\x34\x32\x30\x30"
            b"\x30\x41\x63\x63\x65\x73\x73\x20\x64\x65\x6e\x69"
            b"\x65\x64\x3b\x20\x79\x6f\x75\x20\x6e\x65\x65\x64"
            b"\x20\x74\x68\x65\x20\x53\x55\x50\x45\x52\x20\x70"
            b"\x72\x69\x76\x69\x6c\x65\x67\x65\x20\x66\x6f\x72"
            b"\x20\x74\x68\x69\x73\x20\x6f\x70\x65\x72\x61\x74"
            b"\x69\x6f\x6e"
        )
        self.cnx._socket._writer.reset()
        self.cnx._socket._writer.add_packet(pkt)
        with self.assertRaises(ProgrammingError):
            await self.cnx.cmd_debug()

    @foreach_cnx_aio()
    async def test_cmd_ping(self):
        """Send the Ping-command to MySQL"""
        dummy_socket = AioDummySocket()
        await dummy_socket.set_connection_socket(self.cnx)

        self.cnx._socket._writer.add_packet(OK_PACKET)
        self.assertEqual(OK_PACKET_RESULT, await self.cnx.cmd_ping())

        with self.assertRaises(Error):
            await self.cnx.cmd_ping()

    @foreach_cnx_aio()
    async def test_cmd_change_user(self):
        """Send the Change-User-command to MySQL"""
        dummy_socket = AioDummySocket()
        await dummy_socket.set_connection_socket(self.cnx)

        self.cnx._handshake = {
            "protocol": 10,
            "server_version_original": "5.0.30-enterprise-gpl-log",
            "charset": 8,
            "server_threadid": 265,
            "capabilities": 41516,
            "server_status": 2,
            "auth_data": b"h4i6oP!OLng9&PD@WrYH",
            "auth_plugin": "mysql_native_password",
        }

        self.cnx._socket._writer.reset()
        self.cnx._socket._writer.add_packet(
            bytearray(
                b"\x45\x00\x00\x01\xff\x14\x04\x23\x34\x32\x30\x30"
                b"\x30\x41\x63\x63\x65\x73\x73\x20\x64\x65\x6e\x69"
                b"\x65\x64\x20\x66\x6f\x72\x20\x75\x73\x65\x72\x20"
                b"\x27\x68\x61\x6d\x27\x40\x27\x6c\x6f\x63\x61\x6c"
                b"\x68\x6f\x73\x74\x27\x20\x74\x6f\x20\x64\x61\x74"
                b"\x61\x62\x61\x73\x65\x20\x27\x6d\x79\x73\x71\x6c"
                b"\x27"
            )
        )
        with self.assertRaises(ProgrammingError):
            await self.cnx.cmd_change_user(
                user="ham",
                password="spam",
                database="mysql",
            )

        with self.assertRaises(ValueError):
            await self.cnx.cmd_change_user(
                user="ham",
                password="spam",
                database="mysql",
                charset=-1,
            )

    @foreach_cnx_aio(MySQLConnection)
    async def test__do_handshake(self):
        """Handle the handshake-packet sent by MySQL."""
        config = get_mysql_config()
        config["connection_timeout"] = 1
        cnx = MySQLConnection(**config)
        self.assertEqual(None, cnx._handshake)

        await cnx.connect()

        exp = {
            "protocol": int,
            "server_version_original": str,
            "charset": int,
            "server_threadid": int,
            "capabilities": int,
            "server_status": int,
            "auth_data": (bytearray, bytes),
            "auth_plugin": str,
        }

        self.assertEqual(len(exp), len(cnx._handshake))
        for key, type_ in exp.items():
            self.assertTrue(key in cnx._handshake)
            self.assertTrue(
                isinstance(cnx._handshake[key], type_),
                "type check failed for '{0}', expected {1}, we got {2}".format(
                    key, type_, type(cnx._handshake[key])
                ),
            )
        await cnx.close()

        class FakeSocket:
            def __init__(self, packet):
                self.packet = packet

            async def read(self):
                return self.packet

        correct_handshake = bytearray(
            b"\x47\x00\x00\x00\x0a\x35\x2e\x30\x2e\x33\x30\x2d"
            b"\x65\x6e\x74\x65\x72\x70\x72\x69\x73\x65\x2d\x67"
            b"\x70\x6c\x2d\x6c\x6f\x67\x00\x09\x01\x00\x00\x68"
            b"\x34\x69\x36\x6f\x50\x21\x4f\x00\x2c\xa2\x08\x02"
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
            b"\x00\x00\x4c\x6e\x67\x39\x26\x50\x44\x40\x57\x72"
            b"\x59\x48\x00"
        )

        cnx = MySQLConnection(**config)
        cnx._socket = FakeSocket(correct_handshake)
        exp = {
            "protocol": 10,
            "server_version_original": "5.0.30-enterprise-gpl-log",
            "charset": 8,
            "server_threadid": 265,
            "capabilities": 41516,
            "server_status": 2,
            "auth_data": bytearray(b"h4i6oP!OLng9&PD@WrYH"),
            "auth_plugin": "mysql_native_password",
        }

        await cnx._do_handshake()
        self.assertEqual(exp, cnx._handshake)

        # Handshake with version set to Z.Z.ZZ to simulate bad version
        false_handshake = bytearray(
            b"\x47\x00\x00\x00\x0a\x5a\x2e\x5a\x2e\x5a\x5a\x2d"
            b"\x65\x6e\x74\x65\x72\x70\x72\x69\x73\x65\x2d\x67"
            b"\x70\x6c\x2d\x6c\x6f\x67\x00\x09\x01\x00\x00\x68"
            b"\x34\x69\x36\x6f\x50\x21\x4f\x00\x2c\xa2\x08\x02"
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
            b"\x00\x00\x4c\x6e\x67\x39\x26\x50\x44\x40\x57\x72"
            b"\x59\x48\x00"
        )
        cnx._socket = FakeSocket(false_handshake)
        with self.assertRaises(InterfaceError):
            await cnx._do_handshake()

        # Handshake with version set to 4.0.23
        unsupported_handshake = bytearray(
            b"\x47\x00\x00\x00\x0a\x34\x2e\x30\x2e\x32\x33\x2d"
            b"\x65\x6e\x74\x65\x72\x70\x72\x69\x73\x65\x2d\x67"
            b"\x70\x6c\x2d\x6c\x6f\x67\x00\x09\x01\x00\x00\x68"
            b"\x34\x69\x36\x6f\x50\x21\x4f\x00\x2c\xa2\x08\x02"
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
            b"\x00\x00\x4c\x6e\x67\x39\x26\x50\x44\x40\x57\x72"
            b"\x59\x48\x00"
        )
        cnx._socket = FakeSocket(unsupported_handshake)
        with self.assertRaises(InterfaceError):
            await cnx._do_handshake()

    @foreach_cnx_aio(MySQLConnection)
    async def test__do_auth(self):
        """Authenticate with the MySQL server"""
        config = get_mysql_config()
        cnx = MySQLConnection(**config)
        await cnx.connect()

        dummy_socket = AioDummySocket()
        await dummy_socket.set_connection_socket(cnx)

        cnx._handshake["auth_plugin"] = "mysql_native_password"
        flags = ClientFlag.get_default()
        cnx._user = "ham"
        cnx._password = "spam"
        cnx._database = "test"
        cnx._charset = charsets.get_by_id(45)
        cnx._client_flags = flags

        cnx._socket._writer.add_packet(OK_PACKET)
        await cnx._do_auth()

        cnx._socket._writer.reset()
        cnx._socket._writer.add_packet(bytearray(b"\x01\x00\x00\x02\xfe"))
        with self.assertRaises(NotSupportedError):
            await cnx._do_auth()

        cnx._socket._writer.reset()
        cnx._socket._writer.add_packets(
            [
                bytearray(b"\x07\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00"),
                bytearray(b"\x07\x00\x00\x02\x00\x00\x00\x02\x00\x00\x00"),
            ]
        )
        cnx.set_client_flags([-ClientFlag.CONNECT_WITH_DB])
        await cnx._do_auth()

        # Using an unknown database should raise an error
        cnx._socket._writer.reset()
        cnx._socket._writer.add_packets(
            [
                bytearray(b"\x07\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00"),
                bytearray(
                    b"\x24\x00\x00\x01\xff\x19\x04\x23\x34\x32\x30\x30"
                    b"\x30\x55\x6e\x6b\x6e\x6f\x77\x6e\x20\x64\x61\x74"
                    b"\x61\x62\x61\x73\x65\x20\x27\x61\x73\x64\x66\x61"
                    b"\x73\x64\x66\x27"
                ),
            ]
        )
        flags &= ~ClientFlag.CONNECT_WITH_DB
        cnx._client_flags = flags
        with self.assertRaises(ProgrammingError):
            await cnx._do_auth()

    @unittest.skipUnless(os.name == "posix", "Platform does not support unix sockets")
    @unittest.skipUnless(tests.SSL_AVAILABLE, "Python has no SSL support")
    @foreach_cnx_aio()
    async def test_connect_with_unix_socket(self):
        """Test connect with unix_socket and SSL connection options."""
        dummy_socket = AioDummySocket()
        await dummy_socket.set_connection_socket(self.cnx)

        config = tests.get_mysql_config()
        if tests.MYSQL_EXTERNAL_SERVER:
            if config.get("unix_socket") is None:
                self.skipTest(
                    "The 'unix_socket' is not present in the external server "
                    "connection arguments"
                )
                return
        else:
            config.update(
                {
                    "ssl_ca": os.path.abspath(
                        os.path.join(tests.SSL_DIR, "tests_CA_cert.pem")
                    ),
                    "ssl_cert": os.path.abspath(
                        os.path.join(tests.SSL_DIR, "tests_client_cert.pem")
                    ),
                    "ssl_key": os.path.abspath(
                        os.path.join(tests.SSL_DIR, "tests_client_key.pem")
                    ),
                }
            )
            config["unix_socket"] = tests.MYSQL_SERVERS[0].unix_socket

        # SSL should be disabled when using unix socket
        async with self.cnx.__class__(**config) as cnx:
            self.assertTrue(cnx._ssl_disabled)
            if isinstance(cnx, MySQLConnection):
                self.assertIsInstance(cnx._socket, MySQLUnixSocket)
        del config["unix_socket"]

        # SSL should be enabled when unix socket is not being used
        async with self.cnx.__class__(**config) as cnx:
            self.assertFalse(cnx._ssl_disabled)
            if isinstance(cnx, MySQLConnection):
                self.assertIsInstance(cnx._socket, MySQLTcpSocket)

    @foreach_cnx_aio
    async def test_reconnect(self):
        """Reconnecting to the MySQL Server"""
        supported_arguments = {
            "attempts": 1,
            "delay": 0,
        }
        self.check_args(self.cnx.reconnect, supported_arguments)

        async def _test_reconnect_delay():
            config = {
                "unix_socket": None,
                "host": tests.fake_hostname(),
                "connection_timeout": 1,
            }
            cnx = self.cnx.__class__()
            cnx.config(**config)
            try:
                await cnx.reconnect(attempts=2, delay=3)
            except Exception:
                pass

        tries = 3
        results = []
        reconnect_time = 45
        while tries:
            # Check the delay
            import timeit

            timer = timeit.Timer(await _test_reconnect_delay)
            result = timer.timeit(number=1)
            if results and result < results[0]:
                results.insert(0, result)
            else:
                results.append(result)
            if results[0] < reconnect_time:
                break
            tries -= 1
        self.assertTrue(
            results[0] < reconnect_time,
            "best time: {} > expected: {}, others: {}".format(
                results[0], reconnect_time, results
            ),
        )

        # Check reconnect stops when successful
        config = tests.get_mysql_config()
        cnx = self.cnx.__class__()
        cnx.connect(**config)
        conn_id = cnx.connection_id
        await cnx.reconnect(attempts=5)
        exp = conn_id + 1
        self.assertGreaterEqual(cnx.connection_id, exp)

    async def test_character_set(self):
        """Test character set."""
        config = tests.get_mysql_config()

        config["charset"] = "latin1"
        async with MySQLConnection(**config) as cnx:
            self.assertEqual(8, cnx._charset.charset_id)
            async with await cnx.cursor() as cur:
                await cur.execute("SELECT @@character_set_client")
                res = await cur.fetchone()
                self.assertTupleEqual((config["charset"],), res)

            await cnx.set_charset_collation(
                charset="ascii", collation="ascii_general_ci"
            )
            self.assertEqual(11, cnx._charset.charset_id)
            async with await cnx.cursor() as cur:
                await cur.execute(
                    "SELECT @@character_set_client, @@collation_connection"
                )
                res = await cur.fetchone()
                self.assertTupleEqual(("ascii", "ascii_general_ci"), res)

        for charset_id, charset, collation in [
            (45, "utf8mb4", "utf8mb4_general_ci"),
            (48, "latin1", "latin1_general_ci"),
        ]:
            config["charset"] = charset
            config["collation"] = collation
            async with MySQLConnection(**config) as cnx:
                self.assertEqual(charset_id, cnx._charset.charset_id)
                async with await cnx.cursor() as cur:
                    await cur.execute(
                        "SELECT @@character_set_client, @@collation_connection"
                    )
                    res = await cur.fetchone()
                    self.assertTupleEqual((config["charset"], config["collation"]), res)

        config["client_flags"] = (
            ClientFlag.get_default() | ClientFlag.CAN_HANDLE_EXPIRED_PASSWORDS
        )
        _ = config.pop("charset")
        config["collation"] = "ascii_general_ci"
        async with MySQLConnection(**config) as cnx:
            self.assertEqual(11, cnx._charset.charset_id)
            async with await cnx.cursor() as cur:
                await cur.execute("SELECT @@collation_connection")
                res = await cur.fetchone()
                self.assertTupleEqual((config["collation"],), res)

        _ = config.pop("collation")
        config["charset"] = "latin1"
        async with MySQLConnection(**config) as cnx:
            self.assertEqual(8, cnx._charset.charset_id)
            async with await cnx.cursor() as cur:
                await cur.execute("SELECT @@character_set_client")
                res = await cur.fetchone()
                self.assertTupleEqual((config["charset"],), res)

    @foreach_cnx_aio(connection_timeout=1)
    async def test_connect_timeout(self):
        """Test connect_timeout.

        The connect_timeout should be applied only for establishing the
        connection and not for all blocking socket operations.
        """
        async with await self.cnx.cursor() as cur:
            await cur.execute("SELECT SLEEP(2)")
            await cur.fetchall()
            await cur.close()

    @unittest.skipIf(
        tests.MYSQL_VERSION < (8, 0, 24),
        "MySQL 8.0.24+ is required for this test",
    )
    @foreach_cnx_aio()
    async def test_timeout(self):
        """Test the improved timeout error messages when server disconnects.

        Implemented by WL#14424."""
        # Expected result
        exp_err_no = 4033
        exp_err_msg = (
            "The client was disconnected by the server because of inactivity. "
            "See wait_timeout and interactive_timeout for configuring this "
            "behavior."
        )
        # Expected result if the connection is closed before the error above is
        # retrieved
        exp_err_no2 = 2013
        exp_err_msg2 = "Lost connection to MySQL server during query"

        # Do not connect with unix_socket
        config = tests.get_mysql_config()
        del config["unix_socket"]

        # Set a low value for wait_timeout and wait more
        async with self.cnx.__class__(**config) as cnx:
            await cnx.cmd_query("SET SESSION wait_timeout=1")
            asyncio.sleep(2)
            with self.assertRaises(Exception) as context:
                await cnx.cmd_query("SELECT VERSION()")
                self.assertIn(context.exception.errno, [exp_err_no, exp_err_no2])
                self.assertIn(context.exception.msg, [exp_err_msg, exp_err_msg2])

    @foreach_cnx_aio()
    async def test_ping(self):
        """Ping the MySQL server."""
        try:
            await self.cnx.ping()
        except InterfaceError:
            self.fail("Ping should have not raised an error")

        await self.cnx.disconnect()
        with self.assertRaises(InterfaceError):
            await self.cnx.ping()

    @foreach_cnx_aio()
    async def test_set_converter_class(self):
        """Set the converter class"""

        class TestConverterWrong:
            ...

        self.assertRaises(TypeError, self.cnx.set_converter_class, TestConverterWrong)

        class TestConverter(MySQLConverterBase):
            ...

        self.cnx.set_converter_class(TestConverter)
        self.assertTrue(isinstance(self.cnx.converter, TestConverter))
        self.assertEqual(self.cnx._converter_class, TestConverter)

    @foreach_cnx_aio()
    async def test_get_server_version(self):
        """Get the MySQL version"""
        self.assertEqual(self.cnx._server_info.version, self.cnx.get_server_version())

    @foreach_cnx_aio()
    async def test_get_server_info(self):
        """Get the original MySQL version information"""
        self.assertEqual(
            self.cnx._handshake["server_version_original"],
            self.cnx.get_server_info(),
        )

        del self.cnx._handshake["server_version_original"]
        self.assertEqual(None, self.cnx.get_server_info())

    @foreach_cnx_aio()
    async def test_connection_id(self):
        """MySQL connection ID"""
        self.assertEqual(self.cnx._handshake["server_threadid"], self.cnx.connection_id)

        del self.cnx._handshake["server_threadid"]
        self.assertIsNone(self.cnx.connection_id)

        await self.cnx.close()
        self.assertIsNone(self.cnx.connection_id)
        await self.cnx.connect()
        await self.cnx.close()

    @foreach_cnx_aio()
    async def test_unread_results(self):
        """Check and toggle unread result using property"""
        self.cnx.unread_result = True
        self.assertEqual(True, self.cnx._unread_result)
        self.assertEqual(True, self.cnx.unread_result)
        self.cnx.unread_result = False
        self.assertEqual(False, self.cnx._unread_result)
        self.assertEqual(False, self.cnx.unread_result)

        try:
            self.cnx.unread_result = 1
        except ValueError:
            pass  # Expected
        except Exception:
            self.fail("Expected ValueError to be raised")

    @foreach_cnx_aio()
    async def test_get_warnings(self):
        """Check and toggle the get_warnings property"""
        self.cnx.get_warnings = True
        self.assertEqual(True, self.cnx._get_warnings)
        self.assertEqual(True, self.cnx.get_warnings)
        self.cnx.get_warnings = False
        self.assertEqual(False, self.cnx._get_warnings)
        self.assertEqual(False, self.cnx.get_warnings)

        try:
            self.cnx.get_warnings = 1
        except ValueError:
            pass  # Expected
        except Exception:
            self.fail("Expected ValueError to be raised")

    @foreach_cnx_aio()
    async def test_set_charset_collation(self):
        """Set the character set and collation"""
        await self.cnx.set_charset_collation("latin1")
        self.assertEqual(8, self.cnx._charset.charset_id)
        await self.cnx.set_charset_collation("latin1", "latin1_general_ci")
        self.assertEqual(48, self.cnx._charset.charset_id)
        await self.cnx.set_charset_collation("latin1", None)
        self.assertEqual(8, self.cnx._charset.charset_id)

        await self.cnx.set_charset_collation(collation="greek_bin")
        self.assertEqual(70, self.cnx._charset.charset_id)

        for charset in {None, "", 0}:
            # expecting default charset
            await self.cnx.set_charset_collation(charset=charset)
            self.assertEqual(DEFAULT_CONFIGURATION["charset"], self.cnx.charset)

        for collation in {None, ""}:
            # expecting default charset
            await self.cnx.set_charset_collation(collation=collation)
            self.assertEqual(DEFAULT_CONFIGURATION["charset"], self.cnx.charset)

        utf8_charset = "utf8mb3" if tests.MYSQL_VERSION[:2] >= (8, 0) else "utf8"
        await self.cnx.set_charset_collation(utf8_charset)
        self.assertEqual(33, self.cnx._charset.charset_id)

        with self.assertRaises(ProgrammingError):
            await self.cnx.set_charset_collation(666)
        with self.assertRaises(ProgrammingError):
            await self.cnx.set_charset_collation("spam")
        with self.assertRaises(ProgrammingError):
            await self.cnx.set_charset_collation("latin1", "spam")
        with self.assertRaises(ProgrammingError):
            await self.cnx.set_charset_collation(None, "spam")
        with self.assertRaises(ValueError):
            await self.cnx.set_charset_collation(object())

    @foreach_cnx_aio()
    async def test_charset(self):
        """Get character set name."""
        await self.cnx.set_charset_collation("latin1", "latin1_general_ci")
        self.assertEqual("latin1", self.cnx.charset)

        charset = charsets.get_by_id(70)
        await self.cnx.set_charset_collation(charset.name, charset.collation)
        self.assertEqual("greek", self.cnx.charset)

        charset = charsets.get_by_id(9)
        await self.cnx.set_charset_collation(charset.name, charset.collation)
        self.assertEqual("latin2", self.cnx.charset)

        try:
            charsets.get_by_id(123457)
        except ProgrammingError:
            pass  # This is expected
        except Exception:
            self.fail("Expected errors.ProgrammingError to be raised")

    @foreach_cnx_aio
    async def test_collation(self):
        """Get collation name."""
        exp = "latin2_general_ci"
        await self.cnx.set_charset_collation(collation=exp)
        self.assertEqual(exp, self.cnx.collation)

        charset = charsets.get_by_id(70)
        await self.cnx.set_charset_collation(charset.name, charset.collation)
        self.assertEqual("greek_bin", self.cnx.collation)

        charset = charsets.get_by_id(9)
        await self.cnx.set_charset_collation(charset.name, charset.collation)
        self.assertEqual("latin2_general_ci", self.cnx.collation)

    @foreach_cnx_aio()
    async def test_set_client_flags(self):
        """Set the client flags."""
        self.assertRaises(ProgrammingError, self.cnx.set_client_flags, "Spam")
        self.assertRaises(ProgrammingError, self.cnx.set_client_flags, 0)

        default_flags = ClientFlag.get_default()

        exp = default_flags
        self.assertEqual(exp, self.cnx.set_client_flags(exp))
        self.assertEqual(exp, self.cnx._client_flags)

        exp = default_flags
        exp |= ClientFlag.SSL
        exp |= ClientFlag.FOUND_ROWS
        exp &= ~ClientFlag.MULTI_RESULTS
        flags = [
            ClientFlag.SSL,
            ClientFlag.FOUND_ROWS,
            -ClientFlag.MULTI_RESULTS,
        ]
        self.assertEqual(exp, self.cnx.set_client_flags(flags))
        self.assertEqual(exp, self.cnx._client_flags)

    @foreach_cnx_aio()
    def test_user(self):
        exp = "ham"
        self.cnx._user = exp
        self.assertEqual(exp, self.cnx.user)

    @foreach_cnx_aio()
    def test_host(self):
        exp = "ham"
        self.cnx._host = exp
        self.assertEqual(exp, self.cnx.server_host)

    @foreach_cnx_aio()
    def test_port(self):
        exp = "ham"
        self.cnx._port = exp
        self.assertEqual(exp, self.cnx.server_port)

    @foreach_cnx_aio()
    def test_unix_socket(self):
        exp = "ham"
        self.cnx._unix_socket = exp
        self.assertEqual(exp, self.cnx.unix_socket)

    @foreach_cnx_aio()
    async def test_database(self):
        res = await self.cnx.info_query("SELECT DATABASE()")
        exp = res[0]
        self.assertEqual(exp, await self.cnx.get_database())
        exp = "mysql"
        await self.cnx.set_database(exp)
        self.assertEqual(exp, await self.cnx.get_database())

    @foreach_cnx_aio()
    async def test_non_existent_database(self):
        """Test the raise of ProgrammingError when using a non-existent database."""
        with self.assertRaises(ProgrammingError) as context:
            await self.cnx.set_database("non_existent_database")
        self.assertIn("Unknown database", context.exception.msg)

    @foreach_cnx_aio()
    async def test_autocommit(self):
        for exp in [False, True]:
            await self.cnx.set_autocommit(exp)
            self.assertEqual(exp, await self.cnx.get_autocommit())

    @foreach_cnx_aio()
    def test_raise_on_warnings(self):
        """Check and toggle the get_warnings property"""
        self.cnx.raise_on_warnings = True
        self.assertEqual(True, self.cnx._raise_on_warnings)
        self.assertEqual(True, self.cnx.raise_on_warnings)
        self.cnx.raise_on_warnings = False
        self.assertEqual(False, self.cnx._raise_on_warnings)
        self.assertEqual(False, self.cnx.raise_on_warnings)

        try:
            self.cnx.raise_on_warnings = 1
        except ValueError:
            pass  # Expected
        except Exception:
            self.fail("Expected ValueError to be raised")

    @foreach_cnx_aio(MySQLConnection)
    async def test_cursor(self):
        class FalseCursor:
            pass

        class TrueCursor(MySQLCursor):
            def __init__(self, cnx=None):
                super().__init__(connection=cnx)

        with self.assertRaises(ProgrammingError):
            await self.cnx.cursor(cursor_class=FalseCursor)

        self.assertTrue(
            isinstance(await self.cnx.cursor(cursor_class=TrueCursor), TrueCursor)
        )

        if HAVE_CMYSQL:
            with self.assertRaises(ProgrammingError):
                await self.cnx.cursor(cursor_class=CMySQLCursor)
            with self.assertRaises(ProgrammingError):
                await self.cnx.cursor(cursor_class=CMySQLCursorBufferedRaw)

        cases = [
            ({}, MySQLCursor),
            ({"buffered": True}, MySQLCursorBuffered),
            ({"raw": True}, MySQLCursorRaw),
            ({"buffered": True, "raw": True}, MySQLCursorBufferedRaw),
            ({"prepared": True}, MySQLCursorPrepared),
            ({"dictionary": True}, MySQLCursorDict),
            ({"named_tuple": True}, MySQLCursorNamedTuple),
            ({"dictionary": True, "buffered": True}, MySQLCursorBufferedDict),
            ({"named_tuple": True, "buffered": True}, MySQLCursorBufferedNamedTuple),
        ]
        for kwargs, exp in cases:
            self.assertTrue(isinstance(await self.cnx.cursor(**kwargs), exp))

        with self.assertRaises(ValueError):
            await self.cnx.cursor(prepared=True, buffered=True)
        with self.assertRaises(ValueError):
            await self.cnx.cursor(dictionary=True, raw=True)
        with self.assertRaises(ValueError):
            await self.cnx.cursor(named_tuple=True, raw=True)

        # Test when connection is closed
        await self.cnx.close()
        with self.assertRaises(OperationalError):
            await self.cnx.cursor()

    @foreach_cnx_aio(MySQLConnection)
    async def test__send_data(self):
        with self.assertRaises(ValueError):
            await self.cnx._send_data("spam")

        dummy_socket = AioDummySocket()
        await dummy_socket.set_connection_socket(self.cnx)

        data = b"1\tham\t'ham spam'\n2\tfoo\t'foo bar'"

        fp = io.BytesIO(data)
        self.cnx._socket._writer.reset()
        self.cnx._socket._writer.add_packet(OK_PACKET)
        exp = [
            bytearray(
                b"\x20\x00\x00\x02\x31\x09\x68\x61\x6d\x09\x27\x68\x61\x6d"
                b"\x20\x73\x70\x61\x6d\x27\x0a\x32\x09\x66\x6f\x6f\x09\x27"
                b"\x66\x6f\x6f\x20\x62\x61\x72\x27"
            )
        ]

        self.assertEqual(OK_PACKET, await self.cnx._send_data(fp, False))
        self.assertEqual(exp, self.cnx._socket._writer._client_sends)

        fp = io.BytesIO(data)
        self.cnx._socket._writer.reset()
        self.cnx._socket._writer.add_packet(OK_PACKET)
        exp.append(bytearray(b"\x00\x00\x00\x03"))
        self.assertEqual(OK_PACKET, await self.cnx._send_data(fp, True))
        self.assertEqual(exp, self.cnx._socket._writer._client_sends)

        fp = io.BytesIO(data)
        self.cnx._socket = None
        with self.assertRaises(OperationalError):
            await self.cnx._send_data(fp, False)
        # Nothing to read, but try to send empty packet
        with self.assertRaises(OperationalError):
            await self.cnx._send_data(fp, True)

    @foreach_cnx_aio(MySQLConnection)
    def test__handle_binary_ok(self):
        """Handle a Binary OK packet."""
        packet = bytearray(
            b"\x0c\x00\x00\x01" b"\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        )

        exp = {
            "num_params": 0,
            "statement_id": 1,
            "warning_count": 0,
            "num_columns": 0,
        }
        self.assertEqual(exp, self.cnx._handle_binary_ok(packet))

        # Raise an error
        packet = bytearray(
            b"\x2a\x00\x00\x01\xff\x19\x05\x23\x34\x32\x30\x30\x30\x46\x55"
            b"\x4e\x43\x54\x49\x4f\x4e\x20\x74\x65\x73\x74\x2e\x53\x50\x41"
            b"\x4d\x20\x64\x6f\x65\x73\x20\x6e\x6f\x74\x20\x65\x78\x69\x73\x74"
        )
        self.assertRaises(ProgrammingError, self.cnx._handle_binary_ok, packet)

    @foreach_cnx_aio(MySQLConnection)
    async def test_cmd_stmt_prepare(self):
        """Prepare a MySQL statement."""
        dummy_socket = AioDummySocket()
        await dummy_socket.set_connection_socket(self.cnx)

        stmt = b"SELECT CONCAT(?, ?) AS c1"
        self.cnx._socket._writer.add_packets(
            [
                bytearray(
                    b"\x0c\x00\x00\x01\x00\x01\x00\x00\x00\x01"
                    b"\x00\x02\x00\x00\x00\x00"
                ),
                bytearray(
                    b"\x17\x00\x00\x02\x03\x64\x65\x66\x00\x00\x00\x01\x3f\x00\x0c"
                    b"\x3f\x00\x00\x00\x00\x00\xfd\x80\x00\x00\x00\x00"
                ),
                bytearray(
                    b"\x17\x00\x00\x03\x03\x64\x65\x66\x00\x00\x00\x01\x3f\x00\x0c"
                    b"\x3f\x00\x00\x00\x00\x00\xfd\x80\x00\x00\x00\x00"
                ),
                EOF_PACKET,
                bytearray(
                    b"\x18\x00\x00\x05\x03\x64\x65\x66\x00\x00\x00\x02\x63\x31\x00"
                    b"\x0c\x3f\x00\x00\x00\x00\x00\xfd\x80\x00\x1f\x00\x00"
                ),
                EOF_PACKET,
            ]
        )
        exp = {
            "num_params": 2,
            "statement_id": 1,
            "parameters": [
                ("?", 253, None, None, None, None, 1, 128, 63),
                ("?", 253, None, None, None, None, 1, 128, 63),
            ],
            "warning_count": 0,
            "num_columns": 1,
            "columns": [("c1", 253, None, None, None, None, 1, 128, 63)],
        }
        self.assertEqual(exp, await self.cnx.cmd_stmt_prepare(stmt))

        stmt = b"DO 1"
        self.cnx._socket._writer.reset()
        self.cnx._socket._writer.add_packets(
            [
                bytearray(
                    b"\x0c\x00\x00\x01\x00\x01\x00\x00\x00"
                    b"\x00\x00\x00\x00\x00\x00\x00"
                )
            ]
        )
        exp = {
            "num_params": 0,
            "statement_id": 1,
            "parameters": [],
            "warning_count": 0,
            "num_columns": 0,
            "columns": [],
        }
        self.assertEqual(exp, await self.cnx.cmd_stmt_prepare(stmt))

        # Raise an error using illegal SPAM() MySQL function
        stmt = b"SELECT SPAM(?) AS c1"
        self.cnx._socket._writer.reset()
        self.cnx._socket._writer.add_packets(
            [
                bytearray(
                    b"\x2a\x00\x00\x01\xff\x19\x05\x23\x34\x32\x30\x30\x30"
                    b"\x46\x55\x4e\x43\x54\x49\x4f\x4e\x20\x74\x65\x73\x74"
                    b"\x2e\x53\x50\x41\x4d\x20\x64\x6f\x65\x73\x20\x6e\x6f"
                    b"\x74\x20\x65\x78\x69\x73\x74"
                )
            ]
        )
        with self.assertRaises(ProgrammingError):
            await self.cnx.cmd_stmt_prepare(stmt)

    @foreach_cnx_aio(MySQLConnection)
    async def test__handle_binary_result(self):
        dummy_socket = AioDummySocket()
        await dummy_socket.set_connection_socket(self.cnx)

        with self.assertRaises(InterfaceError):
            await self.cnx._handle_binary_result(None)
        with self.assertRaises(InterfaceError):
            await self.cnx._handle_binary_result(bytearray(b"\x00\x00\x00"))

        self.assertEqual(
            OK_PACKET_RESULT, await self.cnx._handle_binary_result(OK_PACKET)
        )
        self.assertEqual(
            EOF_PACKET_RESULT, await self.cnx._handle_binary_result(EOF_PACKET)
        )

        with self.assertRaises(ProgrammingError):
            await self.cnx._handle_binary_result(ERR_PACKET)

        # handle result set
        self.cnx._socket._writer.reset()
        self.cnx._socket._writer.add_packets(
            [
                bytearray(
                    b"\x18\x00\x00\x02\x03\x64\x65\x66\x00\x00\x00\x02\x63"
                    b"\x31\x00\x0c\x21\x00\x09\x00\x00\x00\xfd\x01\x00\x00"
                    b"\x00\x00"
                ),
                EOF_PACKET,
            ]
        )
        exp = (
            1,
            [("c1", 253, None, None, None, None, 0, 1, 33)],
            {"status_flag": 0, "warning_count": 0},
        )
        self.assertEqual(
            exp,
            await self.cnx._handle_binary_result(bytearray(b"\x01\x00\x00\x01\x01")),
        )

    @foreach_cnx_aio()
    async def test_cmd_stmt_execute(self):
        stmt = b"SELECT ? as c1"
        params = (
            1,
            ("ham",),
            [("c1", 253, None, None, None, None, 1, 128)],
            0,
        )

        # statement does not exists
        with self.assertRaises(DatabaseError):
            await self.cnx.cmd_stmt_execute(*params)

        # prepare and execute
        await self.cnx.cmd_stmt_prepare(stmt)
        if tests.MYSQL_VERSION < (8, 0, 22):
            columns = [("c1", 254, None, None, None, None, 0, 1, 45)]
        else:
            columns = [("c1", 253, None, None, None, None, 1, 0, 45)]
        exp = (1, columns, {"status_flag": 0, "warning_count": 0})
        self.assertEqual(exp, await self.cnx.cmd_stmt_execute(*params))

    @foreach_cnx_aio()
    async def test_cmd_stmt_close(self):
        # statement does not exists, does not return or raise anything
        try:
            await self.cnx.cmd_stmt_close(99)
        except Error as err:
            self.fail(f"cmd_stmt_close raised: {err}")

        # after closing, should not be able to execute
        stmt_info = await self.cnx.cmd_stmt_prepare(b"SELECT ? as c1")
        await self.cnx.cmd_stmt_close(stmt_info["statement_id"])
        params = (
            stmt_info["statement_id"],
            ("ham",),
            stmt_info["parameters"],
            0,
        )
        with self.assertRaises(ProgrammingError):
            await self.cnx.cmd_stmt_execute(*params)

    @foreach_cnx_aio()
    async def test_cmd_reset_connection(self):
        """Resets session without re-authenticating"""
        if tests.MYSQL_VERSION < (5, 7, 3):
            with self.assertRaises(NotSupportedError):
                await self.cnx.cmd_reset_connection()
        else:
            exp_session_id = self.cnx.connection_id
            await self.cnx.cmd_query("SET @ham = 2")
            await self.cnx.cmd_reset_connection()

            await self.cnx.cmd_query("SELECT @ham")
            self.assertEqual(exp_session_id, self.cnx.connection_id)
            res = await self.cnx.get_rows()
            self.assertNotEqual((b"2",), res[0][0])

    @unittest.skip("TODO: Fix me")
    @unittest.skipIf(os.environ.get("PB2WORKDIR"), "Do not run on PB2")
    @unittest.skipIf(
        tests.MYSQL_VERSION <= (5, 7, 1),
        "Shutdown CMD not tested with MySQL version 5.6 (BugOra17422299)",
    )
    @foreach_cnx_aio()
    async def test_shutdown(self):
        """Shutting down a connection"""
        config = tests.get_mysql_config()
        cnx = self.cnx.__class__(**config)
        await cnx.connect()
        sql = "SHOW GLOBAL STATUS WHERE variable_name LIKE 'Aborted_clients'"
        cur = await cnx.cursor()
        await cur.execute(sql)
        res = await cur.fetchone()
        aborted_clients = res[1]

        test_close_cnx = cnx.__class__(**config)
        await test_close_cnx.connect()
        test_shutdown_cnx = cnx.__class__(**config)
        await test_shutdown_cnx.connect()

        await test_close_cnx.close()
        await cur.execute(sql)
        res = await cur.fetchone()
        self.assertEqual(aborted_clients, res[1])

        await test_shutdown_cnx.shutdown()
        await cur.execute(sql)
        res = await cur.fetchone()
        self.assertEqual(str(int(aborted_clients)), res[1])

        await test_shutdown_cnx.close()
        await cur.close()
        await cnx.close()

    async def test_handle_unread_result(self):
        config = tests.get_mysql_config()
        config["consume_results"] = True
        async with MySQLConnection(**config) as cnx:
            async with await cnx.cursor() as cur:
                await cur.execute("SELECT 1,2,3")
                await cur.execute("SELECT 1,2,3")
                await cnx.handle_unread_result()
                self.assertEqual(False, cnx.unread_result)

        config["consume_results"] = False
        async with MySQLConnection(**config) as cnx:
            async with await cnx.cursor() as cur:
                await cur.execute("SELECT 1,2,3")
                with self.assertRaises(InternalError):
                    await cur.execute("SELECT 1,2,3")
                await cnx.consume_results()

    @unittest.skipIf(
        tests.MYSQL_VERSION < (8, 0, 0),
        "The local_infile option is disabled only in MySQL 8.0.",
    )
    @foreach_cnx_aio()
    async def test_default_allow_local_infile(self):
        async with await self.cnx.cursor() as cur:
            await cur.execute("DROP TABLE IF EXISTS local_data")
            await cur.execute(
                "CREATE TABLE local_data (id int, c1 VARCHAR(6), c2 VARCHAR(6))"
            )
            data_file = os.path.join("tests", "data", "local_data.csv")
            sql = "LOAD DATA LOCAL INFILE %s INTO TABLE local_data"
            with self.assertRaises((DatabaseError, ProgrammingError)):
                await cur.execute(sql, (data_file,))
            await cur.execute("DROP TABLE IF EXISTS local_data")

    @unittest.skipIf(
        tests.MYSQL_EXTERNAL_SERVER,
        "Test not available for external MySQL servers",
    )
    @foreach_cnx_aio(allow_local_infile=True)
    async def test_allow_local_infile(self):
        async with await self.cnx.cursor() as cur:
            await cur.execute("DROP TABLE IF EXISTS local_data")
            await cur.execute(
                "CREATE TABLE local_data (id int, c1 VARCHAR(6), c2 VARCHAR(6))"
            )
            data_file = os.path.join("tests", "data", "local_data.csv")
            sql = "LOAD DATA LOCAL INFILE %s INTO TABLE local_data"
            await cur.execute(sql, (data_file,))
            await cur.execute("SELECT * FROM local_data")

            exp = [
                (1, "c1_1", "c2_1"),
                (2, "c1_2", "c2_2"),
                (3, "c1_3", "c2_3"),
                (4, "c1_4", "c2_4"),
                (5, "c1_5", "c2_5"),
                (6, "c1_6", "c2_6"),
            ]
            self.assertEqual(exp, await cur.fetchall())
            cur.execute("DROP TABLE IF EXISTS local_data")

    @unittest.skipIf(
        tests.MYSQL_EXTERNAL_SERVER,
        "Test not available for external MySQL servers",
    )
    @foreach_cnx_aio(allow_local_infile=True)
    async def test_allow_local_infile_in_path(self):
        connector_class = self.cnx.__class__
        def_settings = tests.get_mysql_config()
        database = def_settings["database"]
        if "unix_socket" in def_settings:
            def_settings.pop("unix_socket")
        def_cur = await self.cnx.cursor()

        async def create_table():
            await def_cur.execute(f"DROP TABLE IF EXISTS {database}.local_data_in_path")
            await def_cur.execute(
                f"CREATE TABLE {database}.local_data_in_path "
                "(id int, c1 VARCHAR(6), c2 VARCHAR(6))"
            )

        async def verify_load_success(cur, data_file, exp):
            sql = f"LOAD DATA LOCAL INFILE %s INTO TABLE {database}.local_data_in_path"
            await cur.execute(sql, (data_file,))
            await cur.execute(f"SELECT * FROM {database}.local_data_in_path")

            self.assertEqual(exp, await cur.fetchall())
            await cur.execute(f"TRUNCATE TABLE {database}.local_data_in_path")
            await cur.close()

        async def verify_load_fails(cur, data_file, err_msgs, exception=DatabaseError):
            sql = (
                "LOAD DATA LOCAL INFILE %s INTO TABLE {}.local_data_in_path"
                "".format(database)
            )
            with self.assertRaises(exception) as context:
                await cur.execute(sql, (data_file,))

            exception_msg = str(context.exception)
            if isinstance(err_msgs, (list, tuple)):
                self.assertTrue(
                    [err for err in err_msgs if err in exception_msg],
                    f"Unexpected exception message found: {context.exception}",
                )
            else:
                self.assertTrue(
                    (err_msgs in exception_msg),
                    f"Unexpected exception message found: {context.exception}",
                )
            await cur.close()

        exp1 = [
            (1, "c1_1", "c2_1"),
            (2, "c1_2", "c2_2"),
            (3, "c1_3", "c2_3"),
            (4, "c1_4", "c2_4"),
            (5, "c1_5", "c2_5"),
            (6, "c1_6", "c2_6"),
        ]

        exp2 = [
            (10, "c1_10", "c2_10"),
            (20, "c1_20", "c2_20"),
            (30, "c1_30", "c2_30"),
            (40, "c1_40", "c2_40"),
            (50, "c1_50", "c2_50"),
            (60, "c1_60", "c2_60"),
        ]

        await create_table()
        # Verify defaults
        settings = def_settings.copy()
        self.assertEqual(DEFAULT_CONFIGURATION["allow_local_infile"], False)
        self.assertEqual(DEFAULT_CONFIGURATION["allow_local_infile_in_path"], None)

        # With allow_local_infile default value (False), upload must remain
        # disabled regardless of allow_local_infile_in_path value.
        cnx = connector_class(**settings)
        cur = cnx.cursor()
        data_file = os.path.join("tests", "data", "local_data.csv")
        verify_load_fails(
            cur,
            data_file,
            ["INFILE file request rejected", "command is not allowed"],
        )

        # With allow_local_infile set to  True without setting a value or
        # with None value or empty string for allow_local_infile_in_path user
        # must be able to upload files from any location.
        # allow_local_infile_in_path is None by default
        settings = def_settings.copy()
        settings["allow_local_infile"] = True
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_success(cur, data_file, exp1)

        # allow_local_infile_in_path as empty string
        settings["allow_local_infile"] = True
        settings["allow_local_infile_in_path"] = ""
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join(
                    "tests", "data", "in_file_path", "local_data_in_path.csv"
                )
                await verify_load_success(cur, data_file, exp2)

        # allow_local_infile_in_path as certain base_path but not used
        settings["allow_local_infile"] = True
        settings["allow_local_infile_in_path"] = os.path.join(
            "tests", "data", "in_file_path"
        )
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_success(cur, data_file, exp1)

        # allow_local_infile_in_path as certain base_path and using it
        settings["allow_local_infile"] = True
        settings["allow_local_infile_in_path"] = os.path.join(
            "tests", "data", "in_file_path"
        )
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join(
                    "tests", "data", "in_file_path", "local_data_in_path.csv"
                )
                await verify_load_success(cur, data_file, exp2)

        # With allow_local_infile set to False, upload must remain disabled
        # with default value of allow_local_infile_in_path or empty string.
        settings = def_settings.copy()
        settings["allow_local_infile"] = False
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_fails(
                    cur,
                    data_file,
                    ["INFILE file request rejected", "command is not allowed"],
                )

        settings["allow_local_infile_in_path"] = None
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_fails(
                    cur,
                    data_file,
                    ["INFILE file request rejected", "command is not allowed"],
                )

        settings["allow_local_infile_in_path"] = ""
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_fails(
                    cur,
                    data_file,
                    ["INFILE file request rejected", "command is not allowed"],
                )

        # With allow_local_infile set to False and allow_local_infile_in_path
        # set to <base_path> user must be able to upload files from <base_path>
        # and any subfolder.
        settings = def_settings.copy()
        settings["allow_local_infile"] = False
        settings["allow_local_infile_in_path"] = os.path.join("tests", "data")
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_success(cur, data_file, exp1)

        settings["allow_local_infile_in_path"] = "tests"
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_success(cur, data_file, exp1)

        # Using subtree
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join(
                    "tests", "data", "in_file_path", "local_data_in_path.csv"
                )
                await verify_load_success(cur, data_file, exp2)

        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join(
                    "tests", "data", "in_file_path", "..", "local_data.csv"
                )
                await verify_load_success(cur, data_file, exp1)

        # Upload from a file located outside allow_local_infile_in_path must
        # raise an error
        settings = def_settings.copy()
        settings["allow_local_infile"] = False
        settings["allow_local_infile_in_path"] = os.path.join(
            "tests", "data", "in_file_path"
        )
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_fails(
                    cur, data_file, ("file request rejected", "not found in")
                )

        # Changing allow_local_infile_in_path
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                cnx.set_allow_local_infile_in_path(os.path.join("tests", "data"))
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_success(cur, data_file, exp1)

        # Changing allow_local_infile_in_path to disallow upload
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                cnx.set_allow_local_infile_in_path("")
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_fails(cur, data_file, "file request rejected")

        # Changing disabled allow_local_infile_in_path to allow upload
        settings["allow_local_infile_in_path"] = None
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                cnx.set_allow_local_infile_in_path("")
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_fails(
                    cur, data_file, ("file request rejected", "command is not allowed")
                )

        # relative path that results outside of infile_in_path
        settings["allow_local_infile_in_path"] = os.path.join(
            "tests", "data", "in_file_path"
        )
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join(
                    "tests", "data", "in_file_path", "..", "local_data.csv"
                )
                await verify_load_fails(
                    cur,
                    data_file,
                    ("file request rejected", "not found", "command is not allowed"),
                )

        # Using a file instead of a directory
        settings = def_settings.copy()
        settings["allow_local_infile"] = False
        settings["allow_local_infile_in_path"] = os.path.join(
            "tests", "data", "local_data.csv"
        )
        with self.assertRaises(AttributeError):
            cnx = connector_class(**settings)
            await cnx.connect()

        if os.name != "nt":
            # Using a Symlink in allow_local_infile_in_path is forbiden
            target = os.path.abspath(os.path.join("tests", "data"))
            link = os.path.join(os.path.abspath("tests"), "data_sl")
            os.symlink(target, link)
            settings = def_settings.copy()
            settings["allow_local_infile"] = False
            settings["allow_local_infile_in_path"] = link
            with self.assertRaises(AttributeError):
                cnx = connector_class(**settings)
                await cnx.close()
            try:
                os.remove(link)
            except FileNotFoundError:
                pass

        if os.name != "nt" and connector_class != CMySQLConnection:
            # Load from a Symlink is not allowed
            data_dir = os.path.abspath(os.path.join("tests", "data"))
            target = os.path.abspath(os.path.join(data_dir, "local_data.csv"))
            link = os.path.join(data_dir, "local_data_sl.csv")
            os.symlink(target, link)
            settings = def_settings.copy()
            settings["allow_local_infile"] = False
            settings["allow_local_infile_in_path"] = data_dir
            async with connector_class(**settings) as cnx:
                async with await cnx.cursor() as cur:
                    await verify_load_fails(
                        cur, link, "link is not allowed", OperationalError
                    )
            try:
                os.remove(link)
            except FileNotFoundError:
                pass

        # Clean up
        await def_cur.execute(f"DROP TABLE IF EXISTS {database}.local_data_in_path")
        await def_cur.close()

    @foreach_cnx_aio()
    async def test_connection_attributes_defaults(self):
        """Test default connection attributes"""
        if os.name == "nt":
            if "64" in platform.architecture()[0]:
                platform_arch = "x86_64"
            elif "32" in platform.architecture()[0]:
                platform_arch = "i386"
            else:
                platform_arch = platform.architecture()
            os_ver = "Windows-{}".format(platform.win32_ver()[1])
        else:
            platform_arch = platform.machine()
            if platform.system() == "Darwin":
                os_ver = "{}-{}".format("macOS", platform.mac_ver()[0])
            else:
                os_ver = "-".join(linux_distribution()[0:2])

        license_chunks = LICENSE.split(" ")
        if license_chunks[0] == "GPLv2":
            client_license = "GPL-2.0"
        else:
            client_license = "Commercial"

        async with await self.cnx.cursor() as cur:
            # Verify user defined session-connection-attributes are in the server
            await cur.execute('SHOW VARIABLES LIKE "pseudo_thread_id"')
            res = await cur.fetchall()
            pseudo_thread_id = res[0][1]
            get_attrs = (
                "SELECT ATTR_NAME, ATTR_VALUE FROM "
                "performance_schema.session_account_connect_attrs "
                'where PROCESSLIST_ID = "{}"'
            )
            await cur.execute(get_attrs.format(pseudo_thread_id))
            rows = await cur.fetchall()
            res_dict = dict(rows)
            if CMySQLConnection is not None and isinstance(self.cnx, CMySQLConnection):
                expected_attrs = {
                    "_source_host": socket.gethostname(),
                    "_connector_name": "mysql-connector-python",
                    "_connector_license": client_license,
                    "_connector_version": ".".join([str(x) for x in VERSION[0:3]]),
                }
            else:
                expected_attrs = {
                    "_pid": str(os.getpid()),
                    "_platform": platform_arch,
                    "_source_host": socket.gethostname(),
                    "_client_name": "mysql-connector-python",
                    "_client_license": client_license,
                    "_client_version": ".".join([str(x) for x in VERSION[0:3]]),
                    "_os": os_ver,
                }
            # Note that for an empty string "" value the server stores a Null value
            for attr_name in expected_attrs:
                self.assertEqual(
                    expected_attrs[attr_name],
                    res_dict[attr_name],
                    f"Attribute {attr_name} with value {res_dict[attr_name]} "
                    f"differs of {expected_attrs[attr_name]}",
                )

    @unittest.skipIf(
        sys.platform == "darwin" and platform.mac_ver()[0].startswith("12"),
        "This test fails due to a bug on macOS 12",
    )
    @unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 40), "TLSv1.1 incompatible")
    @unittest.skipIf(
        tests.MYSQL_VERSION > (8, 0, 27),
        "TLSv1 and TLSv1.1 support removed as of MySQL 8.0.28",
    )
    @unittest.skipIf(
        tests.MYSQL_VERSION > (5, 7, 36) and tests.MYSQL_VERSION < (5, 8, 0),
        "TLSv1 and TLSv1.1 support removed as of MySQL 5.7.37",
    )
    @foreach_cnx_aio()
    async def test_get_connection_with_tls_version(self):
        connector_class = self.cnx.__class__

        # Test None value is returned if no schema name is specified
        settings = tests.get_mysql_config()
        if "unix_socket" in settings:
            settings.pop("unix_socket")

        list_a = ("TLSv1.2", "TLSv1.3", ("TLSv1.2", "TLSv1.3"), None)
        list_b = ("TLSv1", "TLSv1.1", ("TLSv1", "TLSv1.1"), None)
        list_c = ("foo", "bar", ("foo", "bar"), None)
        scenarios = [list_a, list_b, list_c]

        test_scenarios = get_scenarios_matrix(scenarios)
        for scen in test_scenarios:
            tls_versions_arg = [elem for elem in scen if elem]
            settings["tls_ciphersuites"] = ["DHE-RSA-AES256-SHA256"]
            settings["tls_versions"] = tls_versions_arg

            # The test should pass only if the following condition is hold:
            # -  Contain one the following values  ["TLSv1.2", "TLSv1.3"]
            if [arg for arg in ["TLSv1.2", "TLSv1.3"] if arg in tls_versions_arg]:
                if not TLS_V1_3_SUPPORTED and "TLSv1.2" not in tls_versions_arg:
                    with self.assertRaises(NotSupportedError) as context:
                        cnx = connector_class(**settings)
                        await cnx.connect()
                        self.assertIn(
                            "No supported TLS protocol version found",
                            str(context.exception),
                            "Unexpected exception message found: "
                            f"{context.exception}, with tls_versions_arg: "
                            f" {tls_versions_arg}",
                        )
                else:
                    async with connector_class(**settings) as cnx:
                        async with await cnx.cursor() as cur:
                            await cur.execute("SHOW STATUS LIKE 'Ssl_version%'")
                            res = await cur.fetchall()
                            self.assertTrue(
                                res[0][1] in ["TLSv1.2", "TLSv1.3"], f"found {res} "
                            )

            # The test should fail with error indicating that TLSv1 and TLSv1.1
            # are no longer allowed if the following conditions hold:
            # - Does not contain one the following values  ["TLSv1.2", "TLSv1.3"]
            # - Contain one the following values  ["TLSv1", "TLSv1.1"]
            elif [arg for arg in ["TLSv1", "TLSv1.1"] if arg in tls_versions_arg]:
                with self.assertRaises(NotSupportedError) as context:
                    cnx = connector_class(**settings)
                    await cnx.connect()
                self.assertTrue(
                    ("are no longer allowed" in str(context.exception)),
                    f"Unexpected exception message found: {context.exception}, "
                    f"with tls_versions_arg: {tls_versions_arg}",
                )

            # The test should fail with error indicating that the given values
            # are not recognized as a valid TLS protocol version if the following
            # conditions hold:
            # - Does not contain one the following values  ["TLSv1.2", "TLSv1.3"]
            # - Does not Contain one the following values  ["TLSv1", "TLSv1.1"]
            elif tls_versions_arg:
                with self.assertRaises(AttributeError) as context:
                    cnx = connector_class(**settings)
                    await cnx.connect()
                self.assertTrue(
                    ("not recognized" in str(context.exception)),
                    f"Unexpected exception message found: {context.exception}",
                )

            # The test should fail with error indicating that at least one TLS
            # protocol version must be specified in 'tls_versions' list if the
            # following condition hold:
            # - combination results in an empty list.
            else:
                with self.assertRaises(AttributeError) as context:
                    cnx = connector_class(**settings)
                    await cnx.connect()
                self.assertTrue(
                    ("At least one" in str(context.exception)),
                    f"Unexpected exception message found: {context.exception}",
                )

        # Empty tls_version list
        settings["tls_ciphersuites"] = ["DHE-RSA-AES256-SHA"]
        settings["tls_versions"] = []
        with self.assertRaises(AttributeError) as context:
            cnx = connector_class(**settings)
            await cnx.connect()
        self.assertTrue(
            ("At least one" in str(context.exception)),
            f"Unexpected exception message found: {context.exception}",
        )

        # Empty tls_ciphersuites list using dict settings
        settings["tls_ciphersuites"] = []
        settings["tls_versions"] = ["TLSv1.2"]
        with self.assertRaises(AttributeError) as context:
            cnx = connector_class(**settings)
            await cnx.connect()
        self.assertTrue(
            ("No valid cipher suite" in str(context.exception)),
            f"Unexpected exception message found: {context.exception}",
        )

        # Empty tls_ciphersuites list without tls-versions
        settings["tls_ciphersuites"] = []
        settings.pop("tls_versions")
        with self.assertRaises(AttributeError) as context:
            cnx = connector_class(**settings)
            await cnx.connect()
        self.assertTrue(
            ("No valid cipher suite" in str(context.exception)),
            f"Unexpected exception message found: {context.exception}",
        )

        # Given tls-version not in ["TLSv1.1", "TLSv1.2", "TLSv1.3"]
        settings["tls_ciphersuites"] = ["DHE-RSA-AES256-SHA"]
        settings["tls_versions"] = ["TLSv0.2", "TLSv1.7", "TLSv10.2"]
        with self.assertRaises(AttributeError) as context:
            cnx = connector_class(**settings)
            await cnx.connect()

        # Repeated values in tls_versions on dict settings
        settings["tls_ciphersuites"] = ["DHE-RSA-AES256-SHA"]
        settings["tls_versions"] = ["TLSv1.2", "TLSv1.1", "TLSv1.2"]
        with self.assertRaises(AttributeError) as context:
            cnx = connector_class(**settings)
            await cnx.connect()

        # Empty tls_versions on dict settings
        settings["tls_ciphersuites"] = ["DHE-RSA-AES256-SHA"]
        settings["tls_versions"] = []
        with self.assertRaises(AttributeError) as context:
            cnx = connector_class(**settings)
            await cnx.connect()
        self.assertTrue(
            ("At least one TLS" in str(context.exception)),
            f"Unexpected exception message found: {context.exception}",
        )

        # Verify unkown cipher suite case?
        settings["tls_ciphersuites"] = ["NOT-KNOWN"]
        settings["tls_versions"] = ["TLSv1.2"]
        with self.assertRaises(AttributeError) as context:
            cnx = connector_class(**settings)
            await cnx.connect()

        # Verify AttributeError exception is raised With invalid TLS version
        settings["tls_ciphersuites"] = ["DHE-RSA-AES256-SHA"]
        settings["tls_versions"] = ["TLSv8"]
        with self.assertRaises(AttributeError) as context:
            cnx = connector_class(**settings)
            await cnx.connect()
        self.assertTrue(
            ("not recognized" in str(context.exception)),
            f"Unexpected exception message found: {context.exception}",
        )

        # Verify unkown cipher suite case?
        settings["tls_ciphersuites"] = ["NOT-KNOWN"]
        settings["tls_versions"] = ["TLSv1.2"]
        with self.assertRaises(AttributeError) as context:
            cnx = connector_class(**settings)
            await cnx.connect()

        # Verify unsupported TLSv1.3 version is accepted (connection success)
        settings["tls_ciphersuites"] = None
        settings["tls_versions"] = ["TLSv1.3", "TLSv1.2"]
        # Connection must be successfully
        cnx = connector_class(**settings)
        await cnx.connect()

        err_msg = (
            "Not using the expected TLS version: {}, instead the "
            "connection used: {}."
        )

        supported_tls = check_tls_versions_support(["TLSv1.2", "TLSv1.1", "TLSv1"])
        if not supported_tls:
            self.fail(f"No TLS version to test: {supported_tls}")

        # Following tests requires TLSv1.2
        if tests.MYSQL_VERSION < (8, 0, 17):
            return

        if len(supported_tls) > 1:
            # Verify given TLS version is used
            settings["tls_ciphersuites"] = ["DHE-RSA-AES256-SHA"]
            for test_ver in supported_tls:
                expected_ssl_version = test_ver
                settings["tls_versions"] = [test_ver]
                async with connector_class(**settings) as cnx:
                    async with await cnx.cursor() as cur:
                        await cur.execute("SHOW STATUS LIKE 'Ssl_version%'")
                        res = await cur.fetchall()
                        self.assertEqual(
                            res[0][1],
                            expected_ssl_version,
                            err_msg.format(expected_ssl_version, res),
                        )

        # Verify the newest TLS version is used from the given list
        exp_res = ["TLSv1.2", "TLSv1.2", "TLSv1.2"]
        test_vers = [
            ["TLSv1", "TLSv1.2", "TLSv1.1"],
            ["TLSv1.1", "TLSv1.2"],
            ["TLSv1.2", "TLSv1.1"],
        ]
        for test_ver, exp_ver in zip(test_vers, exp_res):
            settings["tls_versions"] = test_ver
            async with connector_class(**settings) as cnx:
                async with await cnx.cursor() as cur:
                    res = await cur.execute("SHOW STATUS LIKE 'ssl_version%'")
                    res = await cur.fetchall()
                    self.assertEqual(
                        res[0][1],
                        exp_ver,
                        "On test case {}, {}".format(
                            test_ver, err_msg.format(exp_ver, res)
                        ),
                    )

        # Verify given TLS cipher suite is used
        exp_res = [
            "DHE-RSA-AES128-GCM-SHA256",
            "DHE-RSA-AES128-GCM-SHA256",
            "DHE-RSA-AES128-GCM-SHA256",
        ]
        test_ciphers = [
            ["DHE-RSA-AES128-GCM-SHA256"],
            ["DHE-RSA-AES128-GCM-SHA256"],
            ["TLS_DHE_RSA_WITH_AES_128_GCM_SHA256"],
        ]
        settings["tls_versions"] = ["TLSv1.2"]
        for test_cipher, exp_ver in zip(test_ciphers, exp_res):
            settings["tls_ciphersuites"] = test_cipher
            async with connector_class(**settings) as cnx:
                async with await cnx.cursor() as cur:
                    await cur.execute("SHOW STATUS LIKE 'Ssl_cipher%'")
                    res = await cur.fetchall()
                    self.assertEqual(
                        res[0][1],
                        exp_ver,
                        "Unexpected TLS version found:"
                        " {} for: {}".format(res[0][1], test_cipher),
                    )

        # Verify one of TLS cipher suite is used from the given list
        exp_res = [
            "DHE-RSA-AES256-SHA256",
            "DHE-RSA-AES256-SHA256",
            "DHE-RSA-AES128-GCM-SHA256",
        ]
        test_ciphers = [
            "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256",
            "DHE-RSA-AES256-SHA256",
            "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
        ]
        settings["tls_ciphersuites"] = test_ciphers
        async with connector_class(**settings) as cnx:
            async with cnx.cursor() as cur:
                await cur.execute("SHOW STATUS LIKE 'Ssl_cipher%'")
                res = await cur.fetchall()
                self.assertIn(
                    res[0][1],
                    exp_res,
                    f"Unexpected TLS version found: {res[0][1]} not in {exp_res}",
                )

        if "TLSv1.1" in supported_tls:
            # Verify connection success with either TLS given version when.
            # TLSv1.3 is not supported.
            settings["tls_ciphersuites"] = ["DHE-RSA-AES256-SHA"]
            settings["tls_versions"] = ["TLSv1.3", "TLSv1.1"]

            async with connector_class(**settings) as cnx:
                async with await cnx.cursor() as cur:
                    cur.execute("SHOW STATUS LIKE 'ssl_version%'")
                    res = await cur.fetchall()
                    self.assertNotEqual(
                        res[0][1],
                        "TLSv1.2",
                        f"Unexpected TLS version found: {res[0][1]}",
                    )

        # Verify error when TLSv1.3 is not supported.
        if not TLS_V1_3_SUPPORTED:
            settings["tls_versions"] = ["TLSv1.3"]
            with self.assertRaises(NotSupportedError) as context:
                cnx = connector_class(**settings)
                await cnx.connect()
        else:
            settings["tls_versions"] = ["TLSv1.3"]
            async with connector_class(**settings) as cnx:
                async with await cnx.cursor() as cur:
                    await cur.execute("SHOW STATUS LIKE 'ssl_version%'")
                    res = await cur.fetchall()
                    self.assertEqual(
                        res[0][1],
                        "TLSv1.3",
                        f"Unexpected TLS version found: {res}",
                    )

    async def test_connection_attributes_user_defined(self):
        """Tests defined connection attributes"""
        config = tests.get_mysql_config()
        use_pure_values = [True]
        if HAVE_CMYSQL:
            use_pure_values.append(False)
        for use_pure in use_pure_values:
            config["use_pure"] = use_pure

            # Validate an error is raised if user defined connection attributes
            # are invalid
            invalid_conn_attrs = [
                {1: "1"},
                {1: 2},
                {"_invalid": ""},
                {"_": ""},
                123,
                123.45,
                "text",
                {"_invalid"},
                [
                    "_a1=2",
                ],
            ]

            for invalid_attr in invalid_conn_attrs:
                test_config = copy.deepcopy(config)
                test_config["conn_attrs"] = invalid_attr
                with self.assertRaises(InterfaceError):
                    cnx = await connect(**test_config)
                    LOGGER.error(
                        "InterfaceError not raised while testing "
                        f"invalid attribute: {invalid_attr}"
                    )

            # Test error is raised for attribute name starting with '_'
            connection_attributes = [
                {"foo": "bar", "_baz": "zoom"},
                {"_baz": "zoom"},
                {"foo": "bar", "_baz": "zoom", "puuuuum": "kaplot"},
            ]
            for invalid_attr in connection_attributes:
                test_config = copy.deepcopy(config)
                test_config["conn_attrs"] = invalid_attr
                with self.assertRaises(InterfaceError):
                    _ = await connect(**test_config)
                    LOGGER.error(
                        "InterfaceError not raised while testing "
                        f"invalid attribute: {invalid_attr}"
                    )

            # Test error is raised for attribute name size exceeds 32 characters
            connection_attributes = [
                {"foo": "bar", "p{}w".format("o" * 31): "kaplot"},
                {"p{}w".format("o" * 31): "kaplot"},
                {"baz": "zoom", "p{}w".format("o" * 31): "kaplot", "a": "b"},
            ]
            for invalid_attr in connection_attributes:
                test_config = copy.deepcopy(config)
                test_config["conn_attrs"] = invalid_attr
                with self.assertRaises(InterfaceError) as context:
                    _ = await connect(**test_config)
                    LOGGER.error(
                        "InterfaceError not raised while testing "
                        f"invalid attribute: {invalid_attr}"
                    )

                self.assertTrue(
                    "exceeds 32 characters limit size" in context.exception.msg
                )

            # Test error is raised for attribute value size exceeds 1024 characters
            connection_attributes = [
                {"foo": "bar", "pum": "kr{}nk".format("u" * 1024)},
                {"pum": "kr{}nk".format("u" * 1024)},
                {"baz": "zoom", "pum": "kr{}nk".format("u" * 1024), "a": "b"},
            ]
            for invalid_attr in connection_attributes:
                test_config = copy.deepcopy(config)
                test_config["conn_attrs"] = invalid_attr
                with self.assertRaises(InterfaceError) as context:
                    _ = await connect(**test_config)
                    LOGGER.error(
                        "InterfaceError not raised while testing "
                        f"invalid attribute: {invalid_attr}"
                    )

            self.assertTrue(
                "exceeds 1024 characters limit size" in context.exception.msg
            )

            # Validate the user defined attributes are created in the server
            connection_attributes = {
                "foo": "bar",
                "baz": "zoom",
                "quash": "",
                "puuuuum": "kaplot",
            }
            test_config = copy.deepcopy(config)
            test_config["conn_attrs"] = connection_attributes
            cnx = await connect(**test_config)
            cur = await cnx.cursor()
            await cur.execute('SHOW VARIABLES LIKE "pseudo_thread_id"')
            res = await cur.fetchall()
            pseudo_thread_id = res[0][1]
            get_attrs = (
                "SELECT ATTR_NAME, ATTR_VALUE FROM "
                "performance_schema.session_account_connect_attrs "
                'where PROCESSLIST_ID = "{}"'
            )
            await cur.execute(get_attrs.format(pseudo_thread_id))
            rows = await cur.fetchall()
            res_dict = dict(rows)
            expected_attrs = copy.deepcopy(connection_attributes)
            # Note: For an empty string "" value the server stores a Null value
            expected_attrs["quash"] = None
            for attr_name in expected_attrs:
                self.assertEqual(
                    expected_attrs[attr_name],
                    res_dict[attr_name],
                    f"Attribute {attr_name} with value {res_dict[attr_name]} differs "
                    f"of {expected_attrs[attr_name]}",
                )
            await cur.close()
            await cnx.close()

    @unittest.skipIf(
        tests.MYSQL_VERSION < (8, 0, 19),
        "MySQL 8.0.19+ is required for DNS SRV",
    )
    @unittest.skipIf(not HAVE_DNSPYTHON, "dnspython module is required for DNS SRV")
    async def test_dns_srv(self):
        config = get_mysql_config().copy()
        config.pop("unix_socket", None)
        config.pop("port", None)

        # The value of 'dns-srv' must be a boolean
        config["dns_srv"] = "true"
        with self.assertRaises(InterfaceError):
            await connect(**config)
        config["dns_srv"] = "false"
        with self.assertRaises(InterfaceError):
            await connect(**config)
        config["dns_srv"] = 0
        with self.assertRaises(InterfaceError):
            await connect(**config)
        config["dns_srv"] = 1
        with self.assertRaises(InterfaceError):
            await connect(**config)
        config["dns_srv"] = True

        # Specifying a port number with DNS SRV lookup is not allowed
        config["port"] = 3306
        with self.assertRaises(InterfaceError):
            await connect(**config)
        del config["port"]

        # Using Unix domain sockets with DNS SRV lookup is not allowed
        config["unix_socket"] = "/tmp/mysql.sock"
        with self.assertRaises(InterfaceError):
            await connect(**config)
        del config["unix_socket"]

    async def test_context_manager_pure(self):
        """Test connection and cursor context manager using pure Python."""
        config = get_mysql_config().copy()
        config["use_pure"] = True
        async with await connect(**config) as conn:
            self.assertTrue(await conn.is_connected())
            async with await conn.cursor() as cur:
                self.assertIsInstance(cur, MySQLCursor)
            self.assertIsNone(cur._connection)
        self.assertFalse(await conn.is_connected())


@unittest.skipIf(
    tests.MYSQL_EXTERNAL_SERVER,
    "Test not available for external MySQL servers",
)
class WL13335(tests.MySQLConnectorTests):
    """WL#13335: Avoid set config values with flag CAN_HANDLE_EXPIRED_PASSWORDS"""

    async def asyncSetUp(self):
        self.config = get_mysql_config()
        async with MySQLConnection(**self.config) as cnx:
            self.user = "expired_pw_user"
            self.passw = "i+QEqGkFr8h5"
            self.hosts = ["localhost", "127.0.0.1"]
            for host in self.hosts:
                await cnx.cmd_query(
                    f"CREATE USER '{self.user}'@'{host}' IDENTIFIED BY '{self.passw}'"
                )
                await cnx.cmd_query(f"GRANT ALL ON *.* TO '{self.user}'@'{host}'")
                await cnx.cmd_query(
                    f"ALTER USER '{self.user}'@'{host}' PASSWORD EXPIRE"
                )

    async def asyncTearDown(self):
        async with MySQLConnection(**self.config) as cnx:
            for host in self.hosts:
                cnx.cmd_query(f"DROP USER '{self.user}'@'{host}'")

    @foreach_cnx_aio()
    async def test_connect_with_can_handle_expired_pw_flag(self):
        cnx_config = self.config.copy()
        cnx_config["user"] = self.user
        cnx_config["password"] = self.passw
        flags = ClientFlag.get_default()
        flags |= ClientFlag.CAN_HANDLE_EXPIRED_PASSWORDS
        cnx_config["client_flags"] = flags

        # no error expected
        async with self.cnx.__class__(**cnx_config) as cnx:
            # connection should be in sandbox mode, trying an operation should produce
            # `DatabaseError: 1862 (HY000): Your password has expired. To log in you
            # must change it using a client that supports expired passwords`
            with self.assertRaises(DatabaseError):
                await cnx.cmd_query("SELECT 1")


@unittest.skipIf(
    tests.MYSQL_VERSION < (5, 7), "Authentication with ldap_sasl not supported"
)
class WL14110(tests.MySQLConnectorTests):
    """WL#14110: Add support for SCRAM-SHA-1"""

    async def asyncSetUp(self):
        self.server = tests.MYSQL_SERVERS[0]
        if "com" not in self.server.license:
            self.skipTest("Plugin not available in this version")
        if not tests.is_host_reachable("10.172.166.126"):
            # Skip if remote ldap server is not reachable.
            self.skipTest("Remote ldap server is not reachable")

        self.server_cnf = self.server._cnf
        self.config = get_mysql_config()
        self.config.pop("unix_socket", None)
        self.user = "sadmin"
        self.host = "%"

        cnx = await MySQLConnection(**self.config)
        ext = "dll" if os.name == "nt" else "so"
        plugin_name = f"authentication_ldap_sasl.{ext}"

        ldap_sasl_config = {
            "plugin-load-add": plugin_name,
            "authentication_ldap_sasl_auth_method_name": "SCRAM-SHA-1",
            "authentication_ldap_sasl_bind_base_dn": '"dc=my-domain,dc=com"',
            "authentication_ldap_sasl_log_status": 6,
            "authentication_ldap_sasl_server_host": "10.172.166.126",
            "authentication_ldap_sasl_group_search_attr": "",
            "authentication_ldap_sasl_user_search_attr": "cn",
        }
        cnf = "\n# ldap_sasl"
        for key in ldap_sasl_config:
            cnf = "{}\n{}={}".format(cnf, key, ldap_sasl_config[key])
        self.server_cnf += cnf

        await cnx.close()
        self.server.stop()
        self.server.wait_down()

        self.server.start(my_cnf=self.server_cnf)
        self.server.wait_up()
        asyncio.sleep(1)

        async with MySQLConnection(**self.config) as cnx:
            await cnx.cmd_query("SHOW PLUGINS")
            res = await cnx.get_rows()
            available = False
            for row in res[0]:
                if row[0].lower() == "authentication_ldap_sasl":
                    if row[1] == "ACTIVE":
                        available = True
            if not available:
                self.skipTest("Plugin authentication_ldap_sasl not available")

            try:
                await cnx.cmd_query(f"DROP USER '{self.user}'@'{self.host}'")
                await cnx.cmd_query(f"DROP USER 'common'@'{self.host}'")
            except Exception:
                pass

            await cnx.cmd_query(
                f"CREATE USER '{self.user}'@'{self.host}' IDENTIFIED "
                "WITH authentication_ldap_sasl"
            )

            await cnx.cmd_query(f"CREATE USER 'common'@'{self.host}'")
            cnx.cmd_query(f"GRANT ALL ON *.* TO 'common'@'{self.host}'")

    async def asyncTearDown(self):
        async with MySQLConnection(**self.config) as cnx:
            try:
                await cnx.cmd_query(f"DROP USER '{self.user}'@'{self.host}'")
                await cnx.cmd_query(f"DROP USER 'common'@'{self.host}'")
            except Exception:
                pass
            await cnx.cmd_query("UNINSTALL PLUGIN authentication_ldap_sasl")

    @foreach_cnx_aio()
    async def test_authentication_ldap_sasl_client_with_SCRAM_SHA_1(self):
        """test_authentication_ldap_sasl_client_with_SCRAM-SHA-1"""
        # Not running with c-ext if plugin libraries are not setup
        if (
            self.cnx.__class__ == CMySQLConnection
            and os.getenv("TEST_AUTHENTICATION_LDAP_SASL_CLIENT_CEXT", None) is None
        ):
            return
        conn_args = {
            "user": "sadmin",
            "host": self.config["host"],
            "port": self.config["port"],
            "password": "perola",
        }

        # Attempt connection with wrong password
        bad_pass_args = conn_args.copy()
        bad_pass_args["password"] = "wrong_password"
        with self.assertRaises(ProgrammingError) as context:
            cnx = self.cnx.__class__(**bad_pass_args)
            await cnx.connect()
        self.assertIn(
            "Access denied for user",
            context.exception.msg,
            f"not the expected error {context.exception.msg}",
        )

        # Attempt connection with correct password
        async with self.cnx.__class__(**conn_args) as cnx:
            await cnx.cmd_query("SELECT USER()")
            res = await cnx.get_rows()
            self.assertIn(self.user, res[0][0][0], f"not the expected user {res}")

        # Force unix_socket to None
        conn_args["unix_socket"] = None
        async with self.cnx.__class__(**conn_args) as cnx:
            await cnx.cmd_query("SELECT USER()")
            res = await cnx.get_rows()
            self.assertIn(self.user, res[0][0][0], f"not the expected user {res}")

        # Attempt connection with verify certificate set to True
        conn_args.update(
            {
                "ssl_ca": os.path.abspath(
                    os.path.join(tests.SSL_DIR, "tests_CA_cert.pem")
                ),
                "ssl_cert": os.path.abspath(
                    os.path.join(tests.SSL_DIR, "tests_client_cert.pem")
                ),
                "ssl_key": os.path.abspath(
                    os.path.join(tests.SSL_DIR, "tests_client_key.pem")
                ),
            }
        )
        conn_args["ssl_verify_cert"] = True
        async with self.cnx.__class__(**conn_args) as cnx:
            await cnx.cmd_query("SELECT USER()")
            res = cnx.get_rows()
            self.assertIn(self.user, res[0][0][0], f"not the expected user {res}")


@unittest.skipIf(
    tests.MYSQL_VERSION < (5, 7),
    "Authentication with ldap_simple not supported",
)
class WL13994(tests.MySQLConnectorTests):
    """WL#13994: Support clear text passwords"""

    async def asyncSetUp(self):
        self.server = tests.MYSQL_SERVERS[0]
        if "com" not in self.server.license:
            self.skipTest("Plugin not available in this version")
        if not tests.is_host_reachable("100.103.18.98"):
            # Skip test, remote ldap server is not reachable.
            self.skipTest("Remote ldap server is not reachable")

        self.server_cnf = self.server._cnf
        self.config = tests.get_mysql_config()
        self.config.pop("unix_socket", None)
        self.user = "test1@MYSQL.LOCAL"
        self.host = "%"

        async with MySQLConnection(**self.config) as cnx:
            ext = "dll" if os.name == "nt" else "so"
            plugin_name = f"authentication_ldap_simple.{ext}"

            ldap_simple_config = {
                "plugin-load-add": plugin_name,
                "authentication_ldap_simple_auth_method_name": "simple",
                "authentication_ldap_simple_bind_base_dn": '"dc=MYSQL,dc=local"',
                "authentication_ldap_simple_init_pool_size": 1,
                "authentication_ldap_simple_bind_root_dn": "",
                "authentication_ldap_simple_bind_root_pwd": "",
                "authentication_ldap_simple_ca_path": '""',
                "authentication_ldap_simple_log_status": 6,
                "authentication_ldap_simple_server_host": "100.103.18.98",
                "authentication_ldap_simple_user_search_attr": "cn",
                "authentication_ldap_simple_group_search_attr": "cn",
            }
            cnf = "\n# ldap_simple"
            for key in ldap_simple_config:
                cnf = f"{cnf}\n{key}={ldap_simple_config[key]}"
            self.server_cnf += cnf

        self.server.stop()
        self.server.wait_down()

        self.server.start(my_cnf=self.server_cnf)
        self.server.wait_up()
        asyncio.sleep(1)

        async with MySQLConnection(**self.config) as cnx:
            await cnx.cmd_query("SHOW PLUGINS")
            res = await cnx.get_rows()
            available = False
            for row in res[0]:
                if row[0].lower() == "authentication_ldap_simple":
                    if row[1] == "ACTIVE":
                        available = True

            if not available:
                self.skipTest("Plugin authentication_ldap_simple not available")

            identified_by = "CN=test1,CN=Users,DC=mysql,DC=local"

            await cnx.cmd_query(
                f"CREATE USER '{self.user}'@'{self.host}' IDENTIFIED "
                f"WITH authentication_ldap_simple AS '{identified_by}'"
            )
            await cnx.cmd_query(f"GRANT ALL ON *.* TO '{self.user}'@'{self.host}'")
            await cnx.cmd_query("FLUSH PRIVILEGES")

    async def asyncTearDown(self):
        async with MySQLConnection(**self.config) as cnx:
            try:
                await cnx.cmd_query(f"DROP USER '{self.user}'@'{self.host}'")
            except Exception:
                pass
            await cnx.cmd_query("UNINSTALL PLUGIN authentication_ldap_simple")
            await cnx.cmd_query('show variables like "have_ssl"')
            res = await cnx.get_rows()
            if res[0][0] == ("have_ssl", "DISABLED"):
                await self._enable_ssl()

    async def _disable_ssl(self):
        self.server.stop()
        self.server.wait_down()

        self.server.start(
            ssl_ca="", ssl_cert="", ssl_key="", ssl=0, my_cnf=self.server_cnf
        )
        self.server.wait_up()
        asyncio.sleep(1)
        async with MySQLConnection(**self.config) as cnx:
            await cnx.cmd_query('show variables like "have_ssl"')
            res = await cnx.get_rows()
            self.assertEqual(
                res[0][0], ("have_ssl", "DISABLED"), f"can not disable ssl: {res}"
            )

    async def _enable_ssl(self):
        self.server.stop()
        self.server.wait_down()
        self.server.start()
        self.server.wait_up()
        asyncio.sleep(1)

    @foreach_cnx_aio()
    async def test_clear_text_pass(self):
        """test_clear_text_passwords_without_secure_connection"""
        conn_args = {
            "user": "test1@MYSQL.LOCAL",
            "host": self.config["host"],
            "port": self.config["port"],
            "password": "Testpw1",
            "auth_plugin": "mysql_clear_password",
        }

        # Attempt connection with wrong password
        bad_pass_args = conn_args.copy()
        bad_pass_args["password"] = "wrong_password"
        with self.assertRaises(ProgrammingError) as context:
            cnx = self.cnx.__class__(**bad_pass_args)
            await cnx.connect()
        self.assertIn(
            "Access denied for user",
            context.exception.msg,
            f"not the expected error {context.exception.msg}",
        )

        # connect using mysql clear password and ldap simple auth method
        async with self.cnx.__class__(**conn_args) as cnx:
            await cnx.cmd_query("SELECT USER()")
            res = await cnx.get_rows()
            self.assertIn(self.user, res[0][0][0], f"not the expected user {res}")

        # Disabling ssl must raise an error.
        conn_args["ssl_disabled"] = True
        with self.assertRaises(InterfaceError) as context:
            cnx = self.cnx.__class__(**conn_args)
            await cnx.connect()
        self.assertEqual(
            "Clear password authentication is not supported over insecure channels",
            context.exception.msg,
            f"Unexpected exception message found: {context.exception.msg}",
        )

        # Unix socket is used in unix by default if not popped or set to None
        conn_args["unix_socket"] = get_mysql_config().get("unix_socket", None)
        with self.assertRaises(InterfaceError) as context:
            cnx = self.cnx.__class__(**conn_args)
            await cnx.connect()
        self.assertEqual(
            "Clear password authentication is not supported over insecure channels",
            context.exception.msg,
            f"Unexpected exception message found: {context.exception.msg}",
        )

        # Attempt connection with verify certificate set to True
        conn_args.pop("ssl_disabled")
        conn_args.pop("unix_socket")
        conn_args.update(
            {
                "ssl_ca": os.path.abspath(
                    os.path.join(tests.SSL_DIR, "tests_CA_cert.pem")
                ),
                "ssl_cert": os.path.abspath(
                    os.path.join(tests.SSL_DIR, "tests_client_cert.pem")
                ),
                "ssl_key": os.path.abspath(
                    os.path.join(tests.SSL_DIR, "tests_client_key.pem")
                ),
            }
        )
        conn_args["ssl_verify_cert"] = True
        async with self.cnx.__class__(**conn_args) as cnx:
            await cnx.cmd_query("SELECT USER()")
            res = await cnx.get_rows()
            self.assertIn(self.user, res[0][0][0], f"not the expected user {res}")

        if CMySQLConnection is not None and isinstance(cnx, CMySQLConnection):
            # Not testing cext without ssl
            return
        await self._disable_ssl()
        # Error must be raised to avoid send the password insecurely
        with self.assertRaises(InterfaceError):
            cnx = self.cnx.__class__(**conn_args)
            await cnx.connect()


@unittest.skipIf(
    tests.MYSQL_VERSION < (8, 0, 22),
    "Authentication with ldap_sasl not supported",
)
class WL14263(tests.MySQLConnectorTests):
    """WL#14110: Add support for SCRAM-SHA-256"""

    async def asyncSetUp(self):
        self.server = tests.MYSQL_SERVERS[0]
        if "com" in self.server.license:
            self.skipTest("Plugin not available in this version")
        if not tests.is_host_reachable("100.103.19.5"):
            # Skip if remote ldap server is not reachable.
            self.skipTest("Remote ldap server is not reachable")

        self.server_cnf = self.server._cnf
        self.config = get_mysql_config()
        self.config.pop("unix_socket", None)
        self.user = "sadmin"
        self.host = "%"

        async with connection.MySQLConnection(**self.config) as cnx:
            ext = "dll" if os.name == "nt" else "so"
            plugin_name = f"authentication_ldap_sasl.{ext}"

            ldap_sasl_config = {
                "plugin-load-add": plugin_name,
                "authentication_ldap_sasl_auth_method_name": "SCRAM-SHA-256",
                "authentication_ldap_sasl_bind_base_dn": '"dc=my-domain,dc=com"',
                "authentication_ldap_sasl_log_status": 5,
                "authentication_ldap_sasl_server_host": "100.103.19.5",  # ldap-mtr.no.oracle.com
                "authentication_ldap_sasl_group_search_attr": "",
                "authentication_ldap_sasl_user_search_attr": "cn",
            }
            cnf = "\n# ldap_sasl"
            for key in ldap_sasl_config:
                cnf = f"{cnf}\n{key}={ldap_sasl_config[key]}"
            self.server_cnf += cnf

        self.server.stop()
        self.server.wait_down()

        self.server.start(my_cnf=self.server_cnf)
        self.server.wait_up()
        asyncio.sleep(1)

        async with MySQLConnection(**self.config) as cnx:
            await cnx.cmd_query("SHOW PLUGINS")
            res = await cnx.get_rows()
            available = False
            for row in res[0]:
                if row[0].lower() == "authentication_ldap_sasl":
                    if row[1] == "ACTIVE":
                        available = True
            if not available:
                self.skipTest("Plugin authentication_ldap_sasl not available")

            try:
                await cnx.cmd_query(f"DROP USER '{self.user}'@'{self.host}'")
                await cnx.cmd_query(f"DROP USER 'common'@'{self.host}'")
            except Exception:
                pass

            await cnx.cmd_query(
                f"CREATE USER '{self.user}'@'{self.host}' IDENTIFIED "
                "WITH authentication_ldap_sasl"
            )

            await cnx.cmd_query(f"CREATE USER 'common'@'{self.host}'")
            await cnx.cmd_query(f"GRANT ALL ON *.* TO 'common'@'{self.host}'")

    async def asyncTearDown(self):
        async with MySQLConnection(**self.config) as cnx:
            try:
                await cnx.cmd_query(f"DROP USER '{self.user}'@'{self.host}'")
                await cnx.cmd_query(f"DROP USER 'common'@'{self.host}'")
            except Exception:
                pass
            await cnx.cmd_query("UNINSTALL PLUGIN authentication_ldap_sasl")

    @unittest.skipIf(gssapi is None, "Module gssapi is required")
    @foreach_cnx_aio()
    async def test_authentication_ldap_sasl_client_with_SCRAM_SHA_256(self):
        """test_authentication_ldap_sasl_client_with_SCRAM-SHA-256"""
        # Not running with c-ext if plugin libraries are not setup
        if (
            self.cnx.__class__ == CMySQLConnection
            and os.getenv("TEST_AUTHENTICATION_LDAP_SASL_CLIENT_CEXT", None) is None
        ):
            return
        conn_args = {
            "user": "sadmin",
            "host": self.config["host"],
            "port": self.config["port"],
            "password": "perola",
        }

        # Atempt connection with wrong password
        bad_pass_args = conn_args.copy()
        bad_pass_args["password"] = "wrong_password"
        with self.assertRaises(ProgrammingError) as context:
            cnx = self.cnx.__class__(**bad_pass_args)
            await cnx.connect()
            self.assertIn(
                "Access denied for user",
                context.exception.msg,
                f"not the expected error {context.exception.msg}",
            )

        # Atempt connection with correct password
        async with self.cnx.__class__(**conn_args) as cnx:
            await cnx.cmd_query("SELECT USER()")
            res = await cnx.get_rows()
            self.assertIn(self.user, res[0][0][0], f"not the expected user {res}")

        # Force unix_socket to None
        conn_args["unix_socket"] = None
        async with self.cnx.__class__(**conn_args) as cnx:
            await cnx.cmd_query("SELECT USER()")
            res = await cnx.get_rows()
            self.assertIn(self.user, res[0][0][0], f"not the expected user {res}")

        # Attempt connection with verify certificate set to True
        conn_args.update(
            {
                "ssl_ca": os.path.abspath(
                    os.path.join(tests.SSL_DIR, "tests_CA_cert.pem")
                ),
                "ssl_cert": os.path.abspath(
                    os.path.join(tests.SSL_DIR, "tests_client_cert.pem")
                ),
                "ssl_key": os.path.abspath(
                    os.path.join(tests.SSL_DIR, "tests_client_key.pem")
                ),
            }
        )
        conn_args["ssl_verify_cert"] = True
        async with self.cnx.__class__(**conn_args) as cnx:
            await cnx.cmd_query("SELECT USER()")
            res = await cnx.get_rows()
            self.assertIn(self.user, res[0][0][0], f"not the expected user {res}")


@unittest.skipIf(
    tests.MYSQL_VERSION < (5, 7),
    "Authentication with sasl GSSAPI not supported",
)
@unittest.skipIf(os.name == "nt", "Not available on Windows")
@unittest.skipIf(gssapi is None, "GSSAPI Module not installed")
class WL14213(tests.MySQLConnectorTests):
    """WL#14213: Add support for Kerberos authentication."""

    async def asyncSetUp(self):
        self.server = tests.MYSQL_SERVERS[0]
        if "com" not in self.server.license:
            self.skipTest("Plugin not available in this version")
        if not tests.is_host_reachable("100.103.18.98"):
            # Skip if remote ldap server is not reachable.
            self.skipTest("Remote ldap server is not reachable")

        self.server_cnf = self.server._cnf
        self.config = get_mysql_config()
        self.user = "test3"
        self.host = "MYSQL.LOCAL"

        async with MySQLConnection(**self.config) as cnx:
            ext = "dll" if os.name == "nt" else "so"
            plugin_name = f"authentication_ldap_sasl.{ext}"

            ldap_sasl_config = {
                "plugin-load-add": plugin_name,
                "authentication_ldap_sasl_auth_method_name": "GSSAPI",
                "authentication_ldap_sasl_bind_base_dn": '"DC=mysql,DC=local"',
                "authentication_ldap_sasl_bind_root_dn": '"CN=test2,CN=Users,DC=mysql,DC=local"',
                "authentication_ldap_sasl_bind_root_pwd": '"Testpw1"',
                "authentication_ldap_sasl_log_status": 6,
                "authentication_ldap_sasl_server_host": "100.103.18.98",
                "authentication_ldap_sasl_server_port": "389",
                "authentication_ldap_sasl_group_search_attr": "'cn'",
                "authentication_ldap_sasl_user_search_attr": "sAMAccountName",
            }
            cnf = "\n# ldap_sasl"
            for key in ldap_sasl_config:
                cnf = f"{cnf}\n{key}={ldap_sasl_config[key]}"
            self.server_cnf += cnf

        self.server.stop()
        self.server.wait_down()

        self.server.start(my_cnf=self.server_cnf)
        self.server.wait_up()
        asyncio.sleep(1)

        async with MySQLConnection(**self.config) as cnx:
            await cnx.cmd_query("SHOW PLUGINS")
            res = await cnx.get_rows()
            available = False
            for row in res[0]:
                if row[0].lower() == "authentication_ldap_sasl":
                    if row[1] == "ACTIVE":
                        available = True
            if not available:
                self.skipTest("Plugin authentication_ldap_sasl not available")

            try:
                await cnx.cmd_query(f"DROP USER '{self.user}@{self.host}'")
                await cnx.cmd_query("DROP USER 'mysql_engineering'")
            except Exception:
                pass

            await cnx.cmd_query(
                f"CREATE USER '{self.user}@{self.host}' IDENTIFIED "
                "WITH authentication_ldap_sasl "
                'BY "#testgrp=mysql_engineering"'
            )

            await cnx.cmd_query("CREATE USER 'mysql_engineering'")
            await cnx.cmd_query("GRANT ALL ON myconnpy.* TO 'mysql_engineering'")
            await cnx.cmd_query(
                f"GRANT PROXY on 'mysql_engineering' TO '{self.user}@{self.host}'"
            )

    async def asyncTearDown(self):
        async with MySQLConnection(**self.config) as cnx:
            try:
                await cnx.cmd_query(f"DROP USER '{self.user}'@'{self.host}'")
                await cnx.cmd_query("DROP USER 'mysql_engineering'")
            except Exception:
                pass
            cnx.cmd_query("UNINSTALL PLUGIN authentication_ldap_sasl")

    @foreach_cnx_aio()
    async def test_authentication_ldap_sasl_krb(self):
        """test_authentication_ldap_sasl_client_with_GSSAPI."""
        # Not running with c-ext if plugin libraries are not setup
        if (
            self.cnx.__class__ == CMySQLConnection
            and os.getenv("TEST_AUTHENTICATION_LDAP_SASL_KRB_CEXT", None) is None
        ):
            return
        conn_args = {
            "user": "test3@MYSQL.LOCAL",
            "host": self.config["host"],
            "port": self.config["port"],
            "password": "Testpw1",
            "krb_service_principal": "ldap/ldapauth@MYSQL.LOCAL",
        }

        # Attempt connection with wrong password
        bad_pass_args = conn_args.copy()
        bad_pass_args["password"] = "wrong_password"
        with self.assertRaises((ProgrammingError, InterfaceError)) as context:
            cnx = self.cnx.__class__(**bad_pass_args)
            await cnx.connect()
        if os.name == "nt":
            self.assertIn(
                "Lost connection to MySQL server",
                context.exception.msg,
                f"not the expected error {context.exception.msg}",
            )
        else:
            self.assertIn(
                "Unable to retrieve credentials with the given password",
                context.exception.msg,
                f"not the expected error {context.exception.msg}",
            )

        # Attempt connection with empty krb_service_principal
        bad_pass_args = conn_args.copy()
        bad_pass_args["krb_service_principal"] = ""
        with self.assertRaises((ProgrammingError, InterfaceError)) as context:
            cnx = self.cnx.__class__(**bad_pass_args)
            await cnx.connect()
        self.assertIn(
            "can not be an empty string",
            context.exception.msg,
            f"not the expected error {context.exception.msg}",
        )

        # Attempt connection with an incorrectly formatted krb_service_principal
        bad_pass_args = conn_args.copy()
        bad_pass_args["krb_service_principal"] = "service_principal123"
        with self.assertRaises((ProgrammingError, InterfaceError)) as context:
            cnx = self.cnx.__class__(**bad_pass_args)
            await cnx.connect()
        self.assertIn(
            "incorrectly formatted",
            context.exception.msg,
            f"not the expected error {context.exception.msg}",
        )

        # Attempt connection with an incorrectly formatted krb_service_principal
        bad_pass_args = conn_args.copy()
        bad_pass_args["krb_service_principal"] = 3308
        with self.assertRaises((ProgrammingError, InterfaceError)) as context:
            cnx = self.cnx.__class__(**bad_pass_args)
            await cnx.connect()
        self.assertIn(
            "not a string",
            context.exception.msg,
            f"not the expected error {context.exception.msg}",
        )

        # Attempt connection with a false krb_service_principal
        bad_pass_args = conn_args.copy()
        bad_pass_args["krb_service_principal"] = "principal/instance@realm"
        with self.assertRaises((ProgrammingError, InterfaceError)) as context:
            cnx = self.cnx.__class__(**bad_pass_args)
            await cnx.connect()
        self.assertIn(
            "Unable to initiate security context",
            context.exception.msg,
            f"not the expected error {context.exception.msg}",
        )

        # Attempt connection with correct password
        async with self.cnx.__class__(**conn_args) as cnx:
            await cnx.cmd_query("SELECT USER()")
            res = await cnx.get_rows()
            self.assertIn(self.user, res[0][0][0], f"not the expected user {res}")

        # Force unix_socket to None
        conn_args["unix_socket"] = None
        async with self.cnx.__class__(**conn_args) as cnx:
            await cnx.cmd_query("SELECT USER()")
            res = await cnx.get_rows()
            self.assertIn(self.user, res[0][0][0], f"not the expected user {res}")

        # Attempt connection with verify certificate set to True
        conn_args.update(
            {
                "ssl_ca": os.path.abspath(
                    os.path.join(tests.SSL_DIR, "tests_CA_cert.pem")
                ),
                "ssl_cert": os.path.abspath(
                    os.path.join(tests.SSL_DIR, "tests_client_cert.pem")
                ),
                "ssl_key": os.path.abspath(
                    os.path.join(tests.SSL_DIR, "tests_client_key.pem")
                ),
            }
        )
        conn_args["ssl_verify_cert"] = True
        async with self.cnx.__class__(**conn_args) as cnx:
            await cnx.cmd_query("SELECT USER()")
            res = await cnx.get_rows()[0][0][0]
            self.assertIn(self.user, res, f"not the expected user {res}")


class WL14852(tests.MySQLConnectorTests):
    """WL#14852: Align TLS and SSL options checking and behavior."""

    @foreach_cnx_aio(MySQLConnection)
    async def test_giving_ssl_disable_does_not_raise_error(self):
        """Verify no error with ssl_disable and other TLS or SSL options."""
        config = self.config
        config["ssl_disabled"] = True

        sl_options = {
            "ssl_ca": "",
            "ssl_cert": "",
            "ssl_key": "",
            "tls_versions": "TLSv1",
            "tls_ciphersuites": "DHE-RSA-AES256-SHA256",
            "ssl_cipher": "",
            "ssl_verify_cert": True,
            "ssl_verify_identity": True,
        }

        for sl_option in sl_options:
            settings = config.copy()
            settings[sl_option] = sl_options[sl_option]
            async with MySQLConnection(**config) as cnx:
                async with await cnx.cursor() as cur:
                    await cur.execute("SHOW STATUS LIKE 'ssl_version%'")
                    res = await cur.fetchall()
                    self.assertEqual(res, [("Ssl_version", "")])


@unittest.skipIf(
    tests.MYSQL_VERSION >= (8, 0, 23),
    "Query Attributes are supported, req. ver <8.0.23",
)
class WL14237_not_supported(tests.MySQLConnectorTests):
    """WL#14213: Add support for Query Attributes. not supported scenario."""

    async def _test_query_attrs_not_supported_behavior(self, prepared=False):
        """Verify a warning is raised if QA are given but not supported by the server."""
        async with MySQLConnection(**self.config) as cnx:
            async with await cnx.cursor(prepared=prepared) as cur:
                self.assertListEqual([], cur.get_attributes())
                cur.add_attribute("attr_1", "attr_val")
                # verify get_attributes returns a single attribute that was set
                self.assertListEqual([("attr_1", "attr_val")], cur.get_attributes())

                with warnings.catch_warnings(record=True) as warn:
                    warnings.resetwarnings()
                    warnings.simplefilter("always")
                    if prepared:
                        await cur.execute("SELECT ? as 'PS'", ("some_parameter",))
                    else:
                        await cur.execute("SELECT 'some_parameter'")
                    self.assertGreaterEqual(len(warn), 1)
                    self.assertEqual(warn[-1].category, Warning)
                    self.assertIn(
                        "This version of the server does not support Query "
                        "Attributes",
                        str(warn[-1].message),
                    )

                res = await cur.fetchall()
                # Check that attribute values are correct
                self.assertIn("some_parameter", res[0][0])
                cur.clear_attributes()

    @tests.foreach_cnx()
    def test_1_test_query_attrs_not_supported_behavior(self):
        "Check warning if QA are given but not supported by the server."
        self._test_query_attrs_not_supported_behavior()

    @tests.foreach_cnx()
    def test_2_test_query_attrs_not_supported_behavior(self):
        "Check warning if QA are given but not supported by the server."
        self._test_query_attrs_not_supported_behavior(prepared=True)


@unittest.skipIf(
    tests.MYSQL_VERSION < (8, 0, 25),
    "Query Attributes not supported, req. ver >=8.0.25",
)
class WL14237(tests.MySQLConnectorTests):
    """WL#14213: Add support for Query Attributes."""

    query_insert = """
        INSERT INTO {test_table} (name, value)
        VALUES ('{attr_name}', mysql_query_attribute_string('{attr_name}'))
        """

    @classmethod
    def setUpClass(cls):
        async def setup():
            test_table = "wl14237"
            async with MySQLConnection(**tests.get_mysql_config()) as cnx:
                async with await cnx.cursor() as cur:
                    try:
                        await cur.execute(
                            'INSTALL COMPONENT "file://component_query_attributes"'
                        )
                    except Error:
                        pass  # The component is already installed
                    await cur.execute(f"DROP TABLE IF EXISTS {test_table}")
                    await cur.execute(
                        f"""
                        CREATE TABLE {test_table} (
                            id INT AUTO_INCREMENT KEY,
                            name VARCHAR(50),
                            value VARCHAR(50)
                        )
                        """
                    )

        asyncio.run(setup())

    @classmethod
    def tearDownClass(cls):
        async def teardown():
            test_table = "wl14237"
            async with MySQLConnection(**tests.get_mysql_config()) as cnx:
                async with await cnx.cursor() as cur:
                    cur.execute(
                        'UNINSTALL COMPONENT "file://component_query_attributes"'
                    )
                    cur.execute(f"DROP TABLE {test_table}")

        asyncio.run(teardown())

    async def asyncSetUp(self):
        self.config = get_mysql_config()
        self.test_table = "wl14237"

        self.test_attributes = [
            ("attr1", "foo", "foo"),
            ("attr2", 7, "7"),
            ("attr3", 3.14, "3.14"),
            (
                "attr4",
                datetime(2021, 3, 11, 19, 27, 30),
                "2021-03-11 19:27:30.000000",
            ),
            ("attr5", time(19, 27, 30), "19:27:30.000000"),
            ("attr6", b"\x31\x32\x61\x62", "12ab"),
        ]

    async def _empty_table(self):
        async with MySQLConnection(**tests.get_mysql_config()) as cnx:
            async with await cnx.cursor() as cur:
                await cur.execute(f"DROP TABLE {self.test_table}")
                await cur.execute(
                    f"""
                    CREATE TABLE {self.test_table} (
                        id INT AUTO_INCREMENT KEY,
                        name VARCHAR(50),
                        value VARCHAR(50)
                    )"""
                )
            await cnx.commit()

    async def _check_attribute_values_are_correct(self, attr_name, attr_val):
        async with MySQLConnection(**self.config) as cnx:
            async with await cnx.cursor() as cur_check:
                await cur_check.execute(
                    f"SELECT count(*) FROM {self.test_table} "
                    f"WHERE name = '{attr_name}' and value like '{attr_val}%'"
                )
                self.assertEqual(
                    [(1,)],
                    await cur_check.fetchall(),
                    f"Not found {attr_name}: {attr_val}",
                )

    async def _test_1_query_attr_individual_send(self, prepared=False):
        "Test query_attributes are send individually."
        async with self.cnx.__class__(**self.config) as cnx:
            async with await cnx.cursor(prepared=prepared) as cur:
                for attr_name, attr_val, my_value in self.test_attributes:
                    self.assertListEqual([], cur.get_attributes())
                    cur.add_attribute(attr_name, attr_val)
                    # verify get_attributes returns the single attribute set
                    self.assertListEqual([(attr_name, attr_val)], cur.get_attributes())
                    await cur.execute(
                        self.query_insert.format(
                            test_table=self.test_table,
                            attr_name=attr_name,
                            attr_val=attr_val,
                        )
                    )
                    await cnx.commit()
                    # Check that attribute values are correct
                    await self._check_attribute_values_are_correct(attr_name, my_value)
                    cur.clear_attributes()

        await self._empty_table()

    async def _test_2_query_attr_group_send(self, prepared=False):
        "Test query_attributes are send in group."
        async with self.cnx.__class__(**self.config) as cnx:
            cur = await cnx.cursor(prepared=prepared)
            added_attrs = []
            for attr_name, attr_val, my_value in self.test_attributes:
                cur.add_attribute(attr_name, attr_val)
                added_attrs.append((attr_name, attr_val))
                self.assertListEqual(added_attrs, cur.get_attributes())
                await cur.execute(
                    self.query_insert.format(
                        test_table=self.test_table,
                        attr_name=attr_name,
                        attr_val=attr_val,
                    )
                )
                await cnx.commit()
                # Check the number of attributes values sent so far
                await cur.execute(
                    "SELECT count(*) FROM wl14237 WHERE value IS NOT NULL"
                )
                self.assertEqual([(len(added_attrs),)], await cur.fetchall())
                # Check that attribute values are correct
                await self._check_attribute_values_are_correct(attr_name, my_value)
                # verify that `get_attributes()` returns an empty list
            cur.clear_attributes()
            self.assertListEqual([], cur.get_attributes())
            await cur.close()

        await self._empty_table()

    async def _test_3_query_attr_add_attribute_error_bad_name_par(self, prepared=False):
        "Test add_attribute() invalid name parameter."
        attr_name_invalid = [1, 1.5, ["invalid"], b"invalid", object]
        attr_val = "valid"

        async with self.cnx.__class__(**self.config) as cnx:
            async with await cnx.cursor(prepared=prepared) as cur:
                for attr_name in attr_name_invalid:
                    with self.assertRaises(ProgrammingError) as context:
                        cur.add_attribute(name=attr_name, value=attr_val)
                    self.assertIn(
                        "`name` must be a string type",
                        context.exception.msg,
                        f"Unexpected message found: {context.exception}",
                    )

                    with self.assertRaises(ProgrammingError):
                        cur.add_attribute(name=attr_name, value=attr_val)

    async def _test_4_query_attr_add_attribute_error_bad_value_par(
        self, prepared=False
    ):
        "Test add_attribute() invalid value parameter."
        attr_name = "invalid"
        attr_values_not_supported = [
            ["l", "i", "s", "t"],
            ("t", "p", "l", "e"),
            {"d": "ict"},
            object,
        ]

        async with self.cnx.__class__(**self.config) as cnx:
            async with await cnx.cursor(prepared=prepared) as cur:
                for attr_val in attr_values_not_supported:
                    with self.assertRaises(ProgrammingError) as context:
                        cur.add_attribute(**{"name": attr_name, "value": attr_val})
                    self.assertIn(
                        "cannot be converted to a MySQL type",
                        context.exception.msg,
                        f"Unexpected message found: {context.exception}",
                    )

    async def _test_5_query_attr_individual_send_simple_check(self, prepared=False):
        "Test query_attributes are send individually and simple recover."
        async with self.cnx.__class__(**self.config) as cnx:
            async with await cnx.cursor(prepared=prepared) as cur:
                for attr_name, attr_val, my_value in self.test_attributes:
                    self.assertListEqual([], cur.get_attributes())
                    cur.add_attribute(attr_name, attr_val)
                    # verify get_attributes returns a single attribute that was set
                    self.assertListEqual([(attr_name, attr_val)], cur.get_attributes())
                    if prepared:
                        await cur.execute(
                            f"SELECT "
                            f" mysql_query_attribute_string('{attr_name}') AS 'QA',"
                            f" ? as 'PS'",
                            (f"parameter-{attr_name}",),
                        )
                    else:
                        await cur.execute(
                            f"SELECT mysql_query_attribute_string('{attr_name}') "
                            "AS 'QA'"
                        )
                    res = await cur.fetchall()
                    # Check that attribute values are correct
                    exp = my_value if isinstance(my_value, str) else repr(my_value)
                    if (
                        CMySQLConnection
                        and exp == "3.14"
                        and isinstance(self.cnx, CMySQLConnection)
                    ):
                        exp = "3.140000104904175"
                    self.assertIn(exp, res[0][0])
                    cur.clear_attributes()

    async def _check_two_cursor_can_have_different_query_attrs(self, prepared=False):
        "Check No strange QA values are returned for other cursor"
        cnx1 = self.cnx.__class__(**self.config)
        await cnx1.connect()
        cnx2 = self.cnx.__class__(**self.config)
        await cnx2.connect()
        cur1 = await cnx1.cursor(prepared=prepared)
        cur2 = await cnx2.cursor(prepared=prepared)

        cur1.add_attribute("attr_1", 1)
        cur2.add_attribute("attr_2", 2)
        cur1.add_attribute("attr_3", 3)

        self.assertListEqual([("attr_1", 1), ("attr_3", 3)], cur1.get_attributes())
        self.assertListEqual([("attr_2", 2)], cur2.get_attributes())

        await cur1.execute(
            "SELECT"
            " mysql_query_attribute_string('attr_1') AS 'QA1',"
            " mysql_query_attribute_string('attr_3') AS 'QA2'"
        )

        await cur2.execute("SELECT mysql_query_attribute_string('attr_2') AS 'QA1'")

        res = await cur1.fetchall()
        # Check that attribute values are correct in cur1
        self.assertEqual(("1", "3"), res[0])

        res = await cur2.fetchall()
        # Check that attribute values are correct in cur2
        self.assertEqual(("2",), res[0])

        await cur1.close()
        await cur2.close()
        await cnx1.close()
        await cnx2.close()

    async def _check_query_attrs_names_not_checked_for_uniqueness(self, prepared=False):
        "Check attribute names are not checked for uniqueness"
        async with self.cnx.__class__(**self.config) as cnx:
            async with await cnx.cursor(prepared=prepared) as cur:
                cur.add_attribute("attr_1", 1)
                cur.add_attribute("attr_2", 2)
                cur.add_attribute("attr_1", 3)

                await cur.execute(
                    "SELECT"
                    " mysql_query_attribute_string('attr_1') AS 'QA1',"
                    " mysql_query_attribute_string('attr_2') AS 'QA2'"
                )

                res = await cur.fetchall()
                # Check that attribute values are correct in cur1
                self.assertEqual(("1", "2"), res[0])

    async def _check_expected_behavior_for_unnamed_query_attrs(self, prepared=False):
        "Check behavior add_attribute() and get_attributes() when the name is ''"
        async with self.cnx.__class__(**self.config) as cnx:
            async with await cnx.cursor(prepared=prepared) as cur:
                cur.add_attribute("", 1)
                cur.add_attribute("attr_1", 3)

                await cur.execute(
                    "SELECT mysql_query_attribute_string('attr_1') AS 'QA'"
                )

                res = await cur.fetchall()
                # Check that attribute values are correct in cur1
                self.assertEqual(("3",), res[0])

    @foreach_cnx_aio()
    async def test_1_query_attr_individual_send(self):
        "Test query_attributes are send individually."
        await self._test_1_query_attr_individual_send()

    @foreach_cnx_aio()
    async def test_2_query_attr_group_send(self):
        "Test query_attributes are send in group."
        await self._test_2_query_attr_group_send()

    @foreach_cnx_aio()
    async def test_3_query_attr_add_attribute_error_bad_name_par(self):
        "Test add_attribute() invalid name parameter."
        await self._test_3_query_attr_add_attribute_error_bad_name_par()

    @foreach_cnx_aio()
    async def test_4_query_attr_add_attribute_error_bad_value_par(self):
        "Test add_attribute() invalid value parameter."
        await self._test_4_query_attr_add_attribute_error_bad_value_par()

    @foreach_cnx_aio()
    async def test_5_query_attr_individual_send_simple_check(self):
        "Test query_attributes are send individually and simple recover."
        await self._test_5_query_attr_individual_send_simple_check()

    @foreach_cnx_aio()
    async def test_6_query_attr_individual_send_prepared_cur(self):
        "Test query_attributes are send individually, prepared stmt."
        await self._test_1_query_attr_individual_send(prepared=True)

    @foreach_cnx_aio(MySQLConnection)
    async def test_7_query_attr_group_send_prepared_cur(self):
        "Test query_attributes are send in group, prepared stmt."
        await self._test_2_query_attr_group_send(prepared=True)

    @foreach_cnx_aio()
    async def test_8_query_attr_add_attribute_error_bad_name_par_prepared_cur(self):
        "Test add_attribute() invalid name parameter, prepared stmt."
        await self._test_3_query_attr_add_attribute_error_bad_name_par(prepared=True)

    @foreach_cnx_aio()
    async def test_9_query_attr_add_attribute_error_bad_value_par_prepared_cur(self):
        "Test add_attribute() invalid value parameter, prepared stmt."
        await self._test_4_query_attr_add_attribute_error_bad_value_par(prepared=True)

    @foreach_cnx_aio(MySQLConnection)
    async def test_10_query_attr_individual_send_simple_check_prepared_cur(self):
        "Test query_attributes are send individually and simple recover."
        await self._test_5_query_attr_individual_send_simple_check(prepared=True)

    @foreach_cnx_aio()
    async def test_11_check_two_cursor_can_have_different_query_attrs(self):
        "Check No strange QA values are returned for other cursor"
        await self._check_two_cursor_can_have_different_query_attrs()

    @foreach_cnx_aio()
    async def test_12__check_query_attrs_names_not_checked_for_uniqueness(self):
        "Check attribute names are not checked for uniqueness"
        await self._check_query_attrs_names_not_checked_for_uniqueness()

    @foreach_cnx_aio()
    async def test_13_check_expected_behavior_for_unnamed_query_attrs(self):
        "Check behavior add_attribute() and get_attributes() when the name is ''"
        await self._check_expected_behavior_for_unnamed_query_attrs()

    @foreach_cnx_aio(MySQLConnection)
    async def test_14_check_two_cursor_can_have_different_query_attrs_prepared_cur(
        self,
    ):
        "Check No strange QA values are returned for other cursor"
        await self._check_two_cursor_can_have_different_query_attrs(prepared=True)

    @foreach_cnx_aio(MySQLConnection)
    async def test_15__check_query_attrs_names_not_checked_for_uniqueness_prepared_cur(
        self,
    ):
        "Check attribute names are not checked for uniqueness"
        await self._check_query_attrs_names_not_checked_for_uniqueness(prepared=True)

    @foreach_cnx_aio(MySQLConnection)
    async def test_16_check_expected_behavior_for_unnamed_query_attrs_prepared_cur(
        self,
    ):
        "Check behavior add_attribute() and get_attributes() when the name is ''"
        await self._check_expected_behavior_for_unnamed_query_attrs(prepared=True)
