# MySQL Connector/Python - MySQL driver written in Python.
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
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA

"""Unittests for mysql.connector.connection
"""

import os
import logging
import timeit
import unittest
from decimal import Decimal
import io
import socket

import tests
from . import PY2

from mysql.connector.conversion import (MySQLConverterBase, MySQLConverter)
from mysql.connector import (connection, network, errors,
                             constants, cursor, abstracts, catch23)
from mysql.connector.optionfiles import read_option_files

LOGGER = logging.getLogger(tests.LOGGER_NAME)

OK_PACKET = bytearray(b'\x07\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00')
OK_PACKET_RESULT = {
    'insert_id': 0,
    'affected_rows': 0,
    'field_count': 0,
    'warning_count': 0,
    'server_status': 0
}

ERR_PACKET = bytearray(
    b'\x47\x00\x00\x02\xff\x15\x04\x23\x32\x38\x30\x30\x30'
    b'\x41\x63\x63\x65\x73\x73\x20\x64\x65\x6e\x69\x65\x64'
    b'\x20\x66\x6f\x72\x20\x75\x73\x65\x72\x20\x27\x68\x61'
    b'\x6d\x27\x40\x27\x6c\x6f\x63\x61\x6c\x68\x6f\x73\x74'
    b'\x27\x20\x28\x75\x73\x69\x6e\x67\x20\x70\x61\x73\x73'
    b'\x77\x6f\x72\x64\x3a\x20\x59\x45\x53\x29'
)

EOF_PACKET = bytearray(b'\x05\x00\x00\x00\xfe\x00\x00\x00\x00')
EOF_PACKET_RESULT = {'status_flag': 0, 'warning_count': 0}

COLUMNS_SINGLE = bytearray(
    b'\x17\x00\x00\x02\x03\x64\x65\x66\x00\x00\x00\x01'
    b'\x31\x00\x0c\x3f\x00\x01\x00\x00\x00\x08\x81\x00'
    b'\x00\x00\x00'
)

COLUMNS_SINGLE_COUNT = bytearray(b'\x01\x00\x00\x01\x01')

class _DummyMySQLConnection(connection.MySQLConnection):
    def _open_connection(self, *args, **kwargs):
        pass

    def _post_connection(self, *args, **kwargs):
        pass


class ConnectionTests(object):
    def test_DEFAULT_CONFIGURATION(self):
        exp = {
            'database': None,
            'user': '',
            'password': '',
            'host': '127.0.0.1',
            'port': 3306,
            'unix_socket': None,
            'use_unicode': True,
            'charset': 'utf8',
            'collation': None,
            'converter_class': MySQLConverter,
            'autocommit': False,
            'time_zone': None,
            'sql_mode': None,
            'get_warnings': False,
            'raise_on_warnings': False,
            'connection_timeout': None,
            'client_flags': 0,
            'compress': False,
            'buffered': False,
            'raw': False,
            'ssl_ca': None,
            'ssl_cert': None,
            'ssl_key': None,
            'ssl_verify_cert': False,
            'passwd': None,
            'db': None,
            'connect_timeout': None,
            'dsn': None,
            'force_ipv6': False,
            'auth_plugin': None,
            'allow_local_infile': True,
            'consume_results': False,
        }
        self.assertEqual(exp, connection.DEFAULT_CONFIGURATION)


class MySQLConnectionTests(tests.MySQLConnectorTests):
    def setUp(self):
        config = tests.get_mysql_config()
        self.cnx = connection.MySQLConnection(**config)

    def tearDown(self):
        try:
            self.cnx.close()
        except:
            pass

    def test_init(self):
        """MySQLConnection initialization"""
        cnx = connection.MySQLConnection()
        exp = {
            'converter': None,
            '_converter_class': MySQLConverter,
            '_client_flags': constants.ClientFlag.get_default(),
            '_charset_id': 33,
            '_user': '',
            '_password': '',
            '_database': '',
            '_host': '127.0.0.1',
            '_port': 3306,
            '_unix_socket': None,
            '_use_unicode': True,
            '_get_warnings': False,
            '_raise_on_warnings': False,
            '_connection_timeout': None,
            '_buffered': False,
            '_unread_result': False,
            '_have_next_result': False,
            '_raw': False,
            '_client_host': '',
            '_client_port': 0,
            '_ssl': {},
            '_in_transaction': False,
            '_force_ipv6': False,
            '_auth_plugin': None,
            '_pool_config_version': None,
            '_consume_results': False,
        }
        for key, value in exp.items():
            self.assertEqual(
                value, cnx.__dict__[key],
                msg="Default for '{0}' did not match.".format(key))

        # Make sure that when at least one argument is given,
        # connect() is called
        class FakeMySQLConnection(connection.MySQLConnection):
            def connect(self, **kwargs):
                self._database = kwargs['database']

        exp = 'test'
        cnx = FakeMySQLConnection(database=exp)
        self.assertEqual(exp, cnx._database)

    def test__get_self(self):
        """Return self"""
        self.assertEqual(self.cnx, self.cnx._get_self())

    def test__send_cmd(self):
        """Send a command to MySQL"""
        cmd = constants.ServerCmd.QUERY
        arg = 'SELECT 1'.encode('utf-8')
        pktnr = 2

        self.cnx._socket.sock = None
        self.assertRaises(errors.OperationalError, self.cnx._send_cmd,
                          cmd, arg, pktnr)

        self.cnx._socket.sock = tests.DummySocket()
        exp = OK_PACKET
        self.cnx._socket.sock.add_packet(exp)
        res = self.cnx._send_cmd(cmd, arg, pktnr)
        self.assertEqual(exp, res)

        # Send an unknown command, the result should be an error packet
        exp = ERR_PACKET
        self.cnx._socket.sock.reset()
        self.cnx._socket.sock.add_packet(exp)
        res = self.cnx._send_cmd(90, b'spam', 0)
        self.assertEqual(exp, res)

    def test__handle_server_status(self):
        """Handle the server/status flags"""
        cases = [
            # (serverflag, attribute_name, value when set, value when unset)
            (constants.ServerFlag.MORE_RESULTS_EXISTS,
             '_have_next_result', True, False),
            (constants.ServerFlag.STATUS_IN_TRANS,
             '_in_transaction', True, False),
        ]
        for (flag, attr, when_set, when_unset) in cases:
            setattr(self.cnx, attr, when_unset)
            self.cnx._handle_server_status(flag)
            self.assertEqual(when_set, getattr(self.cnx, attr))
            self.cnx._handle_server_status(0)
            self.assertEqual(when_unset, getattr(self.cnx, attr))

    def test__handle_ok(self):
        """Handle an OK-packet sent by MySQL"""
        self.assertEqual(OK_PACKET_RESULT, self.cnx._handle_ok(OK_PACKET))
        self.assertRaises(errors.ProgrammingError,
                          self.cnx._handle_ok, ERR_PACKET)
        self.assertRaises(errors.InterfaceError,
                          self.cnx._handle_ok, EOF_PACKET)

        # Test for multiple results
        self.cnx._have_next_result = False
        packet = OK_PACKET[:-4] + b'\x08' + OK_PACKET[-3:]
        self.cnx._handle_ok(packet)
        self.assertTrue(self.cnx._have_next_result)

    def test__handle_eof(self):
        """Handle an EOF-packet sent by MySQL"""
        self.assertEqual(EOF_PACKET_RESULT, self.cnx._handle_eof(EOF_PACKET))
        self.assertRaises(errors.ProgrammingError,
                          self.cnx._handle_eof, ERR_PACKET)
        self.assertRaises(errors.InterfaceError,
                          self.cnx._handle_eof, OK_PACKET)

        # Test for multiple results
        self.cnx._have_next_result = False
        packet = EOF_PACKET[:-2] + b'\x08' + EOF_PACKET[-1:]
        self.cnx._handle_eof(packet)
        self.assertTrue(self.cnx._have_next_result)

    def test__handle_result(self):
        """Handle the result after sending a command to MySQL"""
        self.assertRaises(errors.InterfaceError, self.cnx._handle_result,
                          '\x00')
        self.assertRaises(errors.InterfaceError, self.cnx._handle_result,
                          None)
        self.cnx._socket.sock = tests.DummySocket()
        eof_packet = EOF_PACKET
        eof_packet[3] = 3
        self.cnx._socket.sock.add_packets([COLUMNS_SINGLE, eof_packet])
        exp = {
            'eof': {'status_flag': 0, 'warning_count': 0},
            'columns': [('1', 8, None, None, None, None, 0, 129)]
        }
        res = self.cnx._handle_result(COLUMNS_SINGLE_COUNT)
        self.assertEqual(exp, res)

        self.assertEqual(EOF_PACKET_RESULT,
                         self.cnx._handle_result(EOF_PACKET))
        self.cnx._unread_result = False

        # Handle LOAD DATA INFILE
        self.cnx._socket.sock.reset()
        packet = bytearray(
            b'\x1A\x00\x00\x01\xfb'
            b'\x74\x65\x73\x74\x73\x2f\x64\x61\x74\x61\x2f\x6c\x6f'
            b'\x63\x61\x6c\x5f\x64\x61\x74\x61\x2e\x63\x73\x76')
        self.cnx._socket.sock.add_packet(bytearray(
            b'\x37\x00\x00\x04\x00\x06\x00\x01\x00\x00\x00\x2f\x52'
            b'\x65\x63\x6f\x72\x64\x73\x3a\x20\x36\x20\x20\x44\x65'
            b'\x6c\x65\x74\x65\x64\x3a\x20\x30\x20\x20\x53\x6b\x69'
            b'\x70\x70\x65\x64\x3a\x20\x30\x20\x20\x57\x61\x72\x6e'
            b'\x69\x6e\x67\x73\x3a\x20\x30'))
        exp = {
            'info_msg': 'Records: 6  Deleted: 0  Skipped: 0  Warnings: 0',
            'insert_id': 0, 'field_count': 0, 'warning_count': 0,
            'server_status': 1, 'affected_rows': 6}
        self.assertEqual(exp, self.cnx._handle_result(packet))

        exp = [
            bytearray(b'\x47\x00\x00\x04\x31\x09\x63\x31\x5f\x31\x09\x63\x32'
                      b'\x5f\x31\x0a\x32\x09\x63\x31\x5f\x32\x09\x63\x32\x5f'
                      b'\x32\x0a\x33\x09\x63\x31\x5f\x33\x09\x63\x32\x5f\x33'
                      b'\x0a\x34\x09\x63\x31\x5f\x34\x09\x63\x32\x5f\x34\x0a'
                      b'\x35\x09\x63\x31\x5f\x35\x09\x63\x32\x5f\x35\x0a\x36'
                      b'\x09\x63\x31\x5f\x36\x09\x63\x32\x5f\x36'),
            bytearray(b'\x00\x00\x00\x05')
        ]
        self.assertEqual(exp, self.cnx._socket.sock._client_sends)

        # Column count is invalid ( more than 4096)
        self.cnx._socket.sock.reset()
        packet = bytearray(b'\x01\x00\x00\x01\xfc\xff\xff\xff')
        self.assertRaises(errors.InterfaceError,
                          self.cnx._handle_result, packet)

        # First byte in first packet is wrong
        self.cnx._socket.sock.add_packets([
            bytearray(b'\x00\x00\x00\x02\x03\x64\x65\x66\x00\x00\x00\x01'
                      b'\x31\x00\x0c\x3f\x00\x01\x00\x00\x00\x08\x81\x00'
                      b'\x00\x00\x00'),
            bytearray(b'\x05\x00\x00\x03\xfe\x00\x00\x00\x00')])

        self.assertRaises(errors.InterfaceError,
                          self.cnx._handle_result, b'\x01\x00\x00\x01\x00')

    def __helper_get_rows_buffer(self, toggle_next_result=False):
        self.cnx._socket.sock.reset()

        packets = [
            bytearray(b'\x07\x00\x00\x04\x06\x4d\x79\x49\x53\x41\x4d'),
            bytearray(b'\x07\x00\x00\x05\x06\x49\x6e\x6e\x6f\x44\x42'),
            bytearray(b'\x0a\x00\x00\x06\x09\x42\x4c'
                      b'\x41\x43\x4b\x48\x4f\x4c\x45'),
            bytearray(b'\x04\x00\x00\x07\x03\x43\x53\x56'),
            bytearray(b'\x07\x00\x00\x08\x06\x4d\x45\x4d\x4f\x52\x59'),
            bytearray(b'\x0a\x00\x00\x09\x09\x46\x45'
                      b'\x44\x45\x52\x41\x54\x45\x44'),
            bytearray(b'\x08\x00\x00\x0a\x07\x41\x52\x43\x48\x49\x56\x45'),
            bytearray(b'\x0b\x00\x00\x0b\x0a\x4d\x52'
                      b'\x47\x5f\x4d\x59\x49\x53\x41\x4d'),
            bytearray(b'\x05\x00\x00\x0c\xfe\x00\x00\x20\x00'),
        ]

        if toggle_next_result:
            packets[-1] = packets[-1][:-2] + b'\x08' + packets[-1][-1:]

        self.cnx._socket.sock.add_packets(packets)
        self.cnx.unread_result = True

    def test_get_rows(self):
        """Get rows from the MySQL resultset"""
        self.cnx._socket.sock = tests.DummySocket()
        self.__helper_get_rows_buffer()
        exp = (
            [(b'MyISAM',), (b'InnoDB',), (b'BLACKHOLE',), (b'CSV',),
             (b'MEMORY',), (b'FEDERATED',), (b'ARCHIVE',), (b'MRG_MYISAM',)],
            {'status_flag': 32, 'warning_count': 0}
        )
        res = self.cnx.get_rows()
        self.assertEqual(exp, res)

        self.__helper_get_rows_buffer()
        rows = exp[0]
        i = 0
        while i < len(rows):
            exp = (rows[i:i + 2], None)
            res = self.cnx.get_rows(2)
            self.assertEqual(exp, res)
            i += 2
        exp = ([], {'status_flag': 32, 'warning_count': 0})
        self.assertEqual(exp, self.cnx.get_rows())

        # Test unread results
        self.cnx.unread_result = False
        self.assertRaises(errors.InternalError, self.cnx.get_rows)

        # Test multiple results
        self.cnx._have_next_results = False
        self.__helper_get_rows_buffer(toggle_next_result=True)
        exp = {'status_flag': 8, 'warning_count': 0}
        self.assertEqual(exp, self.cnx.get_rows()[-1])
        self.assertTrue(self.cnx._have_next_result)

    def test_get_row(self):
        """Get a row from the MySQL resultset"""
        self.cnx._socket.sock = tests.DummySocket()
        self.__helper_get_rows_buffer()
        expall = (
            [(b'MyISAM',), (b'InnoDB',), (b'BLACKHOLE',), (b'CSV',),
             (b'MEMORY',), (b'FEDERATED',), (b'ARCHIVE',), (b'MRG_MYISAM',)],
            {'status_flag': 32, 'warning_count': 0}
        )

        rows = expall[0]
        for row in rows:
            res = self.cnx.get_row()
            exp = (row, None)
            self.assertEqual(exp, res)
        exp = ([], {'status_flag': 32, 'warning_count': 0})
        self.assertEqual(exp, self.cnx.get_rows())

    def test_cmd_init_db(self):
        """Send the Init_db-command to MySQL"""
        self.cnx._socket.sock = tests.DummySocket()
        self.cnx._socket.sock.add_packet(OK_PACKET)
        self.assertEqual(OK_PACKET_RESULT, self.cnx.cmd_init_db('test'))

        self.cnx._socket.sock.reset()
        self.cnx._socket.sock.add_packet(bytearray(
            b'\x2c\x00\x00\x01\xff\x19\x04\x23\x34\x32\x30\x30'
            b'\x30\x55\x6e\x6b\x6e\x6f\x77\x6e\x20\x64\x61\x74'
            b'\x61\x62\x61\x73\x65\x20\x27\x75\x6e\x6b\x6e\x6f'
            b'\x77\x6e\x5f\x64\x61\x74\x61\x62\x61\x73\x65\x27')
        )
        self.assertRaises(errors.ProgrammingError,
                          self.cnx.cmd_init_db, 'unknown_database')

    def test_cmd_query(self):
        """Send a query to MySQL"""
        self.cnx._socket.sock = tests.DummySocket()
        self.cnx._socket.sock.add_packet(OK_PACKET)
        res = self.cnx.cmd_query("SET AUTOCOMMIT = OFF")
        self.assertEqual(OK_PACKET_RESULT, res)

        packets = [
            COLUMNS_SINGLE_COUNT,
            COLUMNS_SINGLE,
            bytearray(b'\x05\x00\x00\x03\xfe\x00\x00\x00\x00')
        ]

        # query = "SELECT 1"
        self.cnx._socket.sock.reset()
        self.cnx._socket.sock.add_packets(packets)
        exp = {
            'eof': {'status_flag': 0, 'warning_count': 0},
            'columns': [('1', 8, None, None, None, None, 0, 129)]
        }
        res = self.cnx.cmd_query("SELECT 1")
        self.assertEqual(exp, res)
        self.assertRaises(errors.InternalError,
                          self.cnx.cmd_query, 'SELECT 2')
        self.cnx.unread_result = False

        # Forge the packets so the multiple result flag is set
        packets[-1] = packets[-1][:-2] + b'\x08' + packets[-1][-1:]
        self.cnx._socket.sock.reset()
        self.cnx._socket.sock.add_packets(packets)
        self.assertRaises(errors.InterfaceError,
                          self.cnx.cmd_query, "SELECT 1")

    def test_cmd_query_iter(self):
        """Send queries to MySQL"""
        self.cnx._socket.sock = tests.DummySocket()
        self.cnx._socket.sock.add_packet(OK_PACKET)
        res = next(self.cnx.cmd_query_iter(
            "SET AUTOCOMMIT = OFF"))
        self.assertEqual(OK_PACKET_RESULT, res)

        packets = [
            COLUMNS_SINGLE_COUNT,
            COLUMNS_SINGLE,
            bytearray(b'\x05\x00\x00\x03\xfe\x00\x00\x08\x00'),
            bytearray(b'\x02\x00\x00\x04\x01\x31'),
            bytearray(b'\x05\x00\x00\x05\xfe\x00\x00\x08\x00'),
            bytearray(b'\x07\x00\x00\x06\x00\x01\x00\x08\x00\x00\x00'),
            bytearray(b'\x01\x00\x00\x07\x01'),
            bytearray(b'\x17\x00\x00\x08\x03\x64\x65\x66\x00\x00\x00\x01'
                      b'\x32\x00\x0c\x3f\x00\x01\x00\x00\x00\x08\x81\x00'
                      b'\x00\x00\x00'),
            bytearray(b'\x05\x00\x00\x09\xfe\x00\x00\x00\x00'),
            bytearray(b'\x02\x00\x00\x0a\x01\x32'),
            bytearray(b'\x05\x00\x00\x0b\xfe\x00\x00\x00\x00'),
        ]
        exp = [
            {'columns': [('1', 8, None, None, None, None, 0, 129)],
             'eof': {'status_flag': 8, 'warning_count': 0}},
            ([(b'1',)], {'status_flag': 8, 'warning_count': 0}),
            {'affected_rows': 1,
             'field_count': 0,
             'insert_id': 0,
             'server_status': 8,
             'warning_count': 0},
            {'columns': [('2', 8, None, None, None, None, 0, 129)],
             'eof': {'status_flag': 0, 'warning_count': 0}},
            ([(b'2',)], {'status_flag': 0, 'warning_count': 0}),
        ]
        self.cnx._socket.sock.reset()
        self.cnx._socket.sock.add_packets(packets)
        results = []
        stmt = "SELECT 1; SELECT 2".encode('utf-8')
        for result in self.cnx.cmd_query_iter(stmt):
            results.append(result)
            if 'columns' in result:
                results.append(self.cnx.get_rows())
        self.assertEqual(exp, results)

    def test_cmd_refresh(self):
        """Send the Refresh-command to MySQL"""
        self.cnx._socket.sock = tests.DummySocket()
        self.cnx._socket.sock.add_packet(OK_PACKET)
        refresh = constants.RefreshOption.LOG | constants.RefreshOption.THREADS

        self.assertEqual(OK_PACKET_RESULT, self.cnx.cmd_refresh(refresh))

    def test_cmd_quit(self):
        """Send the Quit-command to MySQL"""
        self.cnx._socket.sock = tests.DummySocket()
        self.assertEqual(b'\x01', self.cnx.cmd_quit())

    def test_cmd_statistics(self):
        """Send the Statistics-command to MySQL"""
        self.cnx._socket.sock = tests.DummySocket()
        goodpkt = bytearray(
            b'\x88\x00\x00\x01\x55\x70\x74\x69\x6d\x65\x3a\x20'
            b'\x31\x34\x36\x32\x34\x35\x20\x20\x54\x68\x72\x65'
            b'\x61\x64\x73\x3a\x20\x32\x20\x20\x51\x75\x65\x73'
            b'\x74\x69\x6f\x6e\x73\x3a\x20\x33\x36\x33\x35\x20'
            b'\x20\x53\x6c\x6f\x77\x20\x71\x75\x65\x72\x69\x65'
            b'\x73\x3a\x20\x30\x20\x20\x4f\x70\x65\x6e\x73\x3a'
            b'\x20\x33\x39\x32\x20\x20\x46\x6c\x75\x73\x68\x20'
            b'\x74\x61\x62\x6c\x65\x73\x3a\x20\x31\x20\x20\x4f'
            b'\x70\x65\x6e\x20\x74\x61\x62\x6c\x65\x73\x3a\x20'
            b'\x36\x34\x20\x20\x51\x75\x65\x72\x69\x65\x73\x20'
            b'\x70\x65\x72\x20\x73\x65\x63\x6f\x6e\x64\x20\x61'
            b'\x76\x67\x3a\x20\x30\x2e\x32\x34'
        )
        self.cnx._socket.sock.add_packet(goodpkt)
        exp = {
            'Uptime': 146245,
            'Open tables': 64,
            'Queries per second avg': Decimal('0.24'),
            'Slow queries': 0,
            'Threads': 2,
            'Questions': 3635,
            'Flush tables': 1,
            'Opens': 392
        }
        self.assertEqual(exp, self.cnx.cmd_statistics())

        badpkt = bytearray(
            b'\x88\x00\x00\x01\x55\x70\x74\x69\x6d\x65\x3a\x20'
            b'\x31\x34\x36\x32\x34\x35\x20\x54\x68\x72\x65'
            b'\x61\x64\x73\x3a\x20\x32\x20\x20\x51\x75\x65\x73'
            b'\x74\x69\x6f\x6e\x73\x3a\x20\x33\x36\x33\x35\x20'
            b'\x20\x53\x6c\x6f\x77\x20\x71\x75\x65\x72\x69\x65'
            b'\x73\x3a\x20\x30\x20\x20\x4f\x70\x65\x6e\x73\x3a'
            b'\x20\x33\x39\x32\x20\x20\x46\x6c\x75\x73\x68\x20'
            b'\x74\x61\x62\x6c\x65\x73\x3a\x20\x31\x20\x20\x4f'
            b'\x70\x65\x6e\x20\x74\x61\x62\x6c\x65\x73\x3a\x20'
            b'\x36\x34\x20\x20\x51\x75\x65\x72\x69\x65\x73\x20'
            b'\x70\x65\x72\x20\x73\x65\x63\x6f\x6e\x64\x20\x61'
            b'\x76\x67\x3a\x20\x30\x2e\x32\x34'
        )
        self.cnx._socket.sock.reset()
        self.cnx._socket.sock.add_packet(badpkt)
        self.assertRaises(errors.InterfaceError, self.cnx.cmd_statistics)

        badpkt = bytearray(
            b'\x88\x00\x00\x01\x55\x70\x74\x69\x6d\x65\x3a\x20'
            b'\x55\x70\x36\x32\x34\x35\x20\x20\x54\x68\x72\x65'
            b'\x61\x64\x73\x3a\x20\x32\x20\x20\x51\x75\x65\x73'
            b'\x74\x69\x6f\x6e\x73\x3a\x20\x33\x36\x33\x35\x20'
            b'\x20\x53\x6c\x6f\x77\x20\x71\x75\x65\x72\x69\x65'
            b'\x73\x3a\x20\x30\x20\x20\x4f\x70\x65\x6e\x73\x3a'
            b'\x20\x33\x39\x32\x20\x20\x46\x6c\x75\x73\x68\x20'
            b'\x74\x61\x62\x6c\x65\x73\x3a\x20\x31\x20\x20\x4f'
            b'\x70\x65\x6e\x20\x74\x61\x62\x6c\x65\x73\x3a\x20'
            b'\x36\x34\x20\x20\x51\x75\x65\x72\x69\x65\x73\x20'
            b'\x70\x65\x72\x20\x73\x65\x63\x6f\x6e\x64\x20\x61'
            b'\x76\x67\x3a\x20\x30\x2e\x32\x34'
        )
        self.cnx._socket.sock.reset()
        self.cnx._socket.sock.add_packet(badpkt)
        self.assertRaises(errors.InterfaceError, self.cnx.cmd_statistics)

    def test_cmd_process_info(self):
        """Send the Process-Info-command to MySQL"""
        self.cnx._socket.sock = tests.DummySocket()
        self.assertRaises(errors.NotSupportedError, self.cnx.cmd_process_info)

    def test_cmd_process_kill(self):
        """Send the Process-Kill-command to MySQL"""
        self.cnx._socket.sock = tests.DummySocket()
        self.cnx._socket.sock.add_packet(OK_PACKET)
        self.assertEqual(OK_PACKET_RESULT, self.cnx.cmd_process_kill(1))

        pkt = bytearray(
            b'\x1f\x00\x00\x01\xff\x46\x04\x23\x48\x59\x30\x30'
            b'\x30\x55\x6e\x6b\x6e\x6f\x77\x6e\x20\x74\x68\x72'
            b'\x65\x61\x64\x20\x69\x64\x3a\x20\x31\x30\x30'
        )
        self.cnx._socket.sock.reset()
        self.cnx._socket.sock.add_packet(pkt)
        self.assertRaises(errors.DatabaseError,
                          self.cnx.cmd_process_kill, 100)

        pkt = bytearray(
            b'\x29\x00\x00\x01\xff\x47\x04\x23\x48\x59\x30\x30'
            b'\x30\x59\x6f\x75\x20\x61\x72\x65\x20\x6e\x6f\x74'
            b'\x20\x6f\x77\x6e\x65\x72\x20\x6f\x66\x20\x74\x68'
            b'\x72\x65\x61\x64\x20\x31\x36\x30\x35'
        )
        self.cnx._socket.sock.reset()
        self.cnx._socket.sock.add_packet(pkt)
        self.assertRaises(errors.DatabaseError,
                          self.cnx.cmd_process_kill, 1605)

    def test_cmd_debug(self):
        """Send the Debug-command to MySQL"""
        self.cnx._socket.sock = tests.DummySocket()
        pkt = bytearray(b'\x05\x00\x00\x01\xfe\x00\x00\x00\x00')
        self.cnx._socket.sock.add_packet(pkt)
        exp = {
            'status_flag': 0,
            'warning_count': 0
        }
        self.assertEqual(exp, self.cnx.cmd_debug())

        pkt = bytearray(
            b'\x47\x00\x00\x01\xff\xcb\x04\x23\x34\x32\x30\x30'
            b'\x30\x41\x63\x63\x65\x73\x73\x20\x64\x65\x6e\x69'
            b'\x65\x64\x3b\x20\x79\x6f\x75\x20\x6e\x65\x65\x64'
            b'\x20\x74\x68\x65\x20\x53\x55\x50\x45\x52\x20\x70'
            b'\x72\x69\x76\x69\x6c\x65\x67\x65\x20\x66\x6f\x72'
            b'\x20\x74\x68\x69\x73\x20\x6f\x70\x65\x72\x61\x74'
            b'\x69\x6f\x6e'
        )
        self.cnx._socket.sock.reset()
        self.cnx._socket.sock.add_packet(pkt)
        self.assertRaises(errors.ProgrammingError, self.cnx.cmd_debug)

    def test_cmd_ping(self):
        """Send the Ping-command to MySQL"""
        self.cnx._socket.sock = tests.DummySocket()
        self.cnx._socket.sock.add_packet(OK_PACKET)
        self.assertEqual(OK_PACKET_RESULT, self.cnx.cmd_ping())

        self.assertRaises(errors.Error, self.cnx.cmd_ping)

    def test_cmd_change_user(self):
        """Send the Change-User-command to MySQL"""
        self.cnx._socket.sock = tests.DummySocket()
        self.cnx._handshake = {
            'protocol': 10,
            'server_version_original': '5.0.30-enterprise-gpl-log',
            'charset': 8,
            'server_threadid': 265,
            'capabilities': 41516,
            'server_status': 2,
            'auth_data': b'h4i6oP!OLng9&PD@WrYH',
            'auth_plugin': 'mysql_native_password',
        }

        self.cnx._socket.sock.reset()
        self.cnx._socket.sock.add_packet(bytearray(
            b'\x45\x00\x00\x01\xff\x14\x04\x23\x34\x32\x30\x30'
            b'\x30\x41\x63\x63\x65\x73\x73\x20\x64\x65\x6e\x69'
            b'\x65\x64\x20\x66\x6f\x72\x20\x75\x73\x65\x72\x20'
            b'\x27\x68\x61\x6d\x27\x40\x27\x6c\x6f\x63\x61\x6c'
            b'\x68\x6f\x73\x74\x27\x20\x74\x6f\x20\x64\x61\x74'
            b'\x61\x62\x61\x73\x65\x20\x27\x6d\x79\x73\x71\x6c'
            b'\x27'))
        self.assertRaises(errors.ProgrammingError, self.cnx.cmd_change_user,
                          username='ham', password='spam', database='mysql')

    def test__do_handshake(self):
        """Handle the handshake-packet sent by MySQL"""
        config = tests.get_mysql_config()
        config['connection_timeout'] = 1
        cnx = connection.MySQLConnection()
        self.assertEqual(None, cnx._handshake)

        cnx.connect(**config)

        exp = {
            'protocol': int,
            'server_version_original': catch23.STRING_TYPES,
            'charset': int,
            'server_threadid': int,
            'capabilities': catch23.INT_TYPES,
            'server_status': int,
            'auth_data': catch23.BYTE_TYPES,
            'auth_plugin': catch23.STRING_TYPES,
        }

        self.assertEqual(len(exp), len(cnx._handshake))
        for key, type_ in exp.items():
            self.assertTrue(key in cnx._handshake)
            self.assertTrue(
                isinstance(cnx._handshake[key], type_),
                "type check failed for '{0}', expected {1}, we got {2}".format(
                    key, type_, type(cnx._handshake[key])))
        self.assertRaises(errors.OperationalError, cnx._do_handshake)

        class FakeSocket(object):
            def __init__(self, packet):
                self.packet = packet
            def recv(self):
                return self.packet

        correct_handshake =  bytearray(
            b'\x47\x00\x00\x00\x0a\x35\x2e\x30\x2e\x33\x30\x2d'
            b'\x65\x6e\x74\x65\x72\x70\x72\x69\x73\x65\x2d\x67'
            b'\x70\x6c\x2d\x6c\x6f\x67\x00\x09\x01\x00\x00\x68'
            b'\x34\x69\x36\x6f\x50\x21\x4f\x00\x2c\xa2\x08\x02'
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
            b'\x00\x00\x4c\x6e\x67\x39\x26\x50\x44\x40\x57\x72'
            b'\x59\x48\x00'
        )

        cnx = connection.MySQLConnection(**config)
        cnx._socket = FakeSocket(correct_handshake)
        exp = {
            'protocol': 10,
            'server_version_original': '5.0.30-enterprise-gpl-log',
            'charset': 8,
            'server_threadid': 265,
            'capabilities': 41516,
            'server_status': 2,
            'auth_data': bytearray(b'h4i6oP!OLng9&PD@WrYH'),
            'auth_plugin': u'mysql_native_password',
        }

        cnx._do_handshake()
        self.assertEqual(exp, cnx._handshake)

        # Handshake with version set to Z.Z.ZZ to simulate bad version
        false_handshake = bytearray(
            b'\x47\x00\x00\x00\x0a\x5a\x2e\x5a\x2e\x5a\x5a\x2d'
            b'\x65\x6e\x74\x65\x72\x70\x72\x69\x73\x65\x2d\x67'
            b'\x70\x6c\x2d\x6c\x6f\x67\x00\x09\x01\x00\x00\x68'
            b'\x34\x69\x36\x6f\x50\x21\x4f\x00\x2c\xa2\x08\x02'
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
            b'\x00\x00\x4c\x6e\x67\x39\x26\x50\x44\x40\x57\x72'
            b'\x59\x48\x00'
        )
        cnx._socket = FakeSocket(false_handshake)
        self.assertRaises(errors.InterfaceError, cnx._do_handshake)

        # Handshake with version set to 4.0.23
        unsupported_handshake = bytearray(
            b'\x47\x00\x00\x00\x0a\x34\x2e\x30\x2e\x32\x33\x2d'
            b'\x65\x6e\x74\x65\x72\x70\x72\x69\x73\x65\x2d\x67'
            b'\x70\x6c\x2d\x6c\x6f\x67\x00\x09\x01\x00\x00\x68'
            b'\x34\x69\x36\x6f\x50\x21\x4f\x00\x2c\xa2\x08\x02'
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
            b'\x00\x00\x4c\x6e\x67\x39\x26\x50\x44\x40\x57\x72'
            b'\x59\x48\x00'
        )
        cnx._socket = FakeSocket(unsupported_handshake)
        self.assertRaises(errors.InterfaceError, cnx._do_handshake)

    def test__do_auth(self):
        """Authenticate with the MySQL server"""
        self.cnx._socket.sock = tests.DummySocket()
        flags = constants.ClientFlag.get_default()
        kwargs = {
            'username': 'ham',
            'password': 'spam',
            'database': 'test',
            'charset': 33,
            'client_flags': flags,
        }

        self.cnx._socket.sock.add_packet(OK_PACKET)
        self.assertEqual(True, self.cnx._do_auth(**kwargs))

        self.cnx._socket.sock.reset()
        self.cnx._socket.sock.add_packet(bytearray(b'\x01\x00\x00\x02\xfe'))
        self.assertRaises(errors.NotSupportedError,
                          self.cnx._do_auth, **kwargs)

        self.cnx._socket.sock.reset()
        self.cnx._socket.sock.add_packets([
            bytearray(b'\x07\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00'),
            bytearray(b'\x07\x00\x00\x02\x00\x00\x00\x02\x00\x00\x00')
        ])
        self.cnx.set_client_flags([-constants.ClientFlag.CONNECT_WITH_DB])
        self.assertEqual(True, self.cnx._do_auth(**kwargs))

        # Using an unknown database should raise an error
        self.cnx._socket.sock.reset()
        self.cnx._socket.sock.add_packets([
            bytearray(b'\x07\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00'),
            bytearray(b'\x24\x00\x00\x01\xff\x19\x04\x23\x34\x32\x30\x30'
                      b'\x30\x55\x6e\x6b\x6e\x6f\x77\x6e\x20\x64\x61\x74'
                      b'\x61\x62\x61\x73\x65\x20\x27\x61\x73\x64\x66\x61'
                      b'\x73\x64\x66\x27')
        ])
        flags &= ~constants.ClientFlag.CONNECT_WITH_DB
        kwargs['client_flags'] = flags
        self.assertRaises(errors.ProgrammingError,
                          self.cnx._do_auth, **kwargs)

    @unittest.skipIf(not tests.SSL_AVAILABLE, "Python has no SSL support")
    def test__do_auth_ssl(self):
        """Authenticate with the MySQL server using SSL"""
        self.cnx._socket.sock = tests.DummySocket()
        flags = constants.ClientFlag.get_default()
        flags |= constants.ClientFlag.SSL
        kwargs = {
            'username': 'ham',
            'password': 'spam',
            'database': 'test',
            'charset': 33,
            'client_flags': flags,
            'ssl_options': {
                'ca': os.path.join(tests.SSL_DIR, 'tests_CA_cert.pem'),
                'cert': os.path.join(tests.SSL_DIR, 'tests_client_cert.pem'),
                'key': os.path.join(tests.SSL_DIR, 'tests_client_key.pem'),
            },
        }

        self.cnx._handshake['auth_data'] = b'h4i6oP!OLng9&PD@WrYH'

        # We check if do_auth send the autherization for SSL and the
        # normal authorization.
        exp = [
            self.cnx._protocol.make_auth_ssl(
                charset=kwargs['charset'],
                client_flags=kwargs['client_flags']),
            self.cnx._protocol.make_auth(
                self.cnx._handshake, kwargs['username'],
                kwargs['password'], kwargs['database'],
                charset=kwargs['charset'],
                client_flags=kwargs['client_flags']),
        ]
        self.cnx._socket.switch_to_ssl = lambda ca, cert, key: None
        self.cnx._socket.sock.reset()
        self.cnx._socket.sock.add_packets([
            bytearray(b'\x07\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00'),
            bytearray(b'\x07\x00\x00\x02\x00\x00\x00\x02\x00\x00\x00')
        ])
        self.cnx._do_auth(**kwargs)
        self.assertEqual(
            exp, [p[4:] for p in self.cnx._socket.sock._client_sends])

    def test_config(self):
        """Configure the MySQL connection

        These tests do not actually connect to MySQL, but they make
        sure everything is setup before calling _open_connection() and
        _post_connection().
        """
        cnx = _DummyMySQLConnection()
        default_config = abstracts.DEFAULT_CONFIGURATION.copy()

        # Should fail because 'dsn' is given
        self.assertRaises(errors.NotSupportedError, cnx.config,
                          **default_config)

        # Remove unsupported arguments
        del default_config['dsn']
        default_config.update({
            'ssl_ca': 'CACert',
            'ssl_cert': 'ServerCert',
            'ssl_key': 'ServerKey',
            'ssl_verify_cert': False
        })
        default_config['converter_class'] = MySQLConverter
        try:
            cnx.config(**default_config)
        except AttributeError as err:
            self.fail("Config does not accept a supported"
                      " argument: {}".format(str(err)))

        # Add an argument which we don't allow
        default_config['spam'] = 'SPAM'
        self.assertRaises(AttributeError, cnx.config, **default_config)

        # We do not support dsn
        self.assertRaises(errors.NotSupportedError, cnx.connect, dsn='ham')

        exp = {
            'host': 'localhost.local',
            'port': 3306,
            'unix_socket': '/tmp/mysql.sock'
        }
        cnx.config(**exp)
        self.assertEqual(exp['port'], cnx._port)
        self.assertEqual(exp['host'], cnx._host)
        self.assertEqual(exp['unix_socket'], cnx._unix_socket)

        exp = (None, 'test', 'mysql  ')
        for database in exp:
            cnx.config(database=database)
            if database is not None:
                database = database.strip()
            failmsg = "Failed setting database to '{0}'".format(database)
            self.assertEqual(database, cnx._database, msg=failmsg)
        cnx.config(user='ham')
        self.assertEqual('ham', cnx._user)

        cnx.config(raise_on_warnings=True)
        self.assertEqual(True, cnx._raise_on_warnings)
        cnx.config(get_warnings=False)
        self.assertEqual(False, cnx._get_warnings)
        cnx.config(connection_timeout=123)
        self.assertEqual(123, cnx._connection_timeout)
        for toggle in [True, False]:
            cnx.config(buffered=toggle)
            self.assertEqual(toggle, cnx._buffered)
            cnx.config(raw=toggle)
            self.assertEqual(toggle, cnx._raw)
        for toggle in [False, True]:
            cnx.config(use_unicode=toggle)
            self.assertEqual(toggle, cnx._use_unicode)

        # Test client flags
        cnx = _DummyMySQLConnection()
        cnx.set_client_flags(constants.ClientFlag.get_default())
        flag = exp = constants.ClientFlag.COMPRESS
        cnx.config(client_flags=flag)
        self.assertEqual(exp, cnx._client_flags)

        # Setting client flags using a list
        cnx = _DummyMySQLConnection()
        cnx.set_client_flags(constants.ClientFlag.get_default())
        flags = [constants.ClientFlag.COMPRESS,
                 constants.ClientFlag.FOUND_ROWS]
        exp = constants.ClientFlag.get_default()
        for flag in flags:
            exp |= flag
        cnx.config(client_flags=flags)
        self.assertEqual(exp, cnx._client_flags)

        # and unsetting client flags again
        exp = constants.ClientFlag.get_default()
        flags = [-constants.ClientFlag.COMPRESS,
                 -constants.ClientFlag.FOUND_ROWS]
        cnx.config(client_flags=flags)
        self.assertEqual(exp, cnx._client_flags)

        # Test compress argument
        cnx.config(compress=True)
        exp = constants.ClientFlag.COMPRESS
        self.assertEqual(exp, cnx._client_flags &
                              constants.ClientFlag.COMPRESS)

        # Test character set
        # utf8 is default, which is mapped to 33
        self.assertEqual(33, cnx._charset_id)
        cnx.config(charset='latin1')
        self.assertEqual(8, cnx._charset_id)
        cnx.config(charset='latin1', collation='latin1_general_ci')
        self.assertEqual(48, cnx._charset_id)
        cnx.config(collation='latin1_general_ci')
        self.assertEqual(48, cnx._charset_id)

        # Test converter class
        class TestConverter(MySQLConverterBase):

            def __init__(self, charset=None, unicode=True):
                pass

        self.cnx.config(converter_class=TestConverter)
        self.assertTrue(isinstance(self.cnx.converter, TestConverter))
        self.assertEqual(self.cnx._converter_class, TestConverter)

        class TestConverterWrong(object):

            def __init__(self, charset, unicode):
                pass

        self.assertRaises(AttributeError,
                          self.cnx.config, converter_class=TestConverterWrong)

        # Test SSL configuration
        exp = {
            'ca': 'CACert',
            'cert': 'ServerCert',
            'key': 'ServerKey',
            'verify_cert': False
        }
        cnx.config(ssl_ca=exp['ca'], ssl_cert=exp['cert'], ssl_key=exp['key'])
        self.assertEqual(exp, cnx._ssl)

        exp['verify_cert'] = True

        cnx.config(ssl_ca=exp['ca'], ssl_cert=exp['cert'],
                   ssl_key=exp['key'], ssl_verify_cert=exp['verify_cert'])
        self.assertEqual(exp, cnx._ssl)

        # Missing SSL configuration should raise an AttributeError
        cnx._ssl = {}
        cases = [
            {'ssl_key': exp['key']},
            {'ssl_cert': exp['cert']},
        ]
        for case in cases:
            cnx._ssl = {}
            try:
                cnx.config(ssl_ca=exp['ca'], **case)
            except AttributeError as err:
                errmsg = str(err)
                self.assertTrue(list(case.keys())[0] in errmsg)
            else:
                self.fail("Testing SSL attributes failed.")

        # Compatibility tests: MySQLdb
        cnx = _DummyMySQLConnection()
        cnx.connect(db='mysql', passwd='spam', connect_timeout=123)
        self.assertEqual('mysql', cnx._database)
        self.assertEqual('spam', cnx._password)
        self.assertEqual(123, cnx._connection_timeout)

        # Option Files tests
        option_file_dir = os.path.join('tests', 'data', 'option_files')
        cfg = read_option_files(option_files=os.path.join(option_file_dir,
                                                          'my.cnf'))
        cnx.config(**cfg)
        self.assertEqual(cnx._port, 1000)
        self.assertEqual(cnx._unix_socket, '/var/run/mysqld/mysqld.sock')

        cfg = read_option_files(option_files=os.path.join(option_file_dir,
                                                          'my.cnf'), port=2000)
        cnx.config(**cfg)
        self.assertEqual(cnx._port, 2000)

        cfg = read_option_files(option_files=os.path.join(
            option_file_dir, 'my.cnf'), option_groups=[ 'client', 'mysqld'])
        cnx.config(**cfg)

        self.assertEqual(cnx._port, 1001)
        self.assertEqual(cnx._unix_socket, '/var/run/mysqld/mysqld2.sock')
        self.assertEqual(cnx._ssl['ca'], 'dummyCA')
        self.assertEqual(cnx._ssl['cert'], 'dummyCert')
        self.assertEqual(cnx._ssl['key'], 'dummyKey')

    def test__get_connection(self):
        """Get connection based on configuration"""
        if os.name != 'nt':
            res = self.cnx._get_connection()
            self.assertTrue(isinstance(res, network.MySQLUnixSocket))

        self.cnx._unix_socket = None
        self.cnx._connection_timeout = 123
        res = self.cnx._get_connection()
        self.assertTrue(isinstance(res, network.MySQLTCPSocket))
        self.assertEqual(self.cnx._connection_timeout,
                         res._connection_timeout)

    def test__open_connection(self):
        """Open the connection to the MySQL server"""
        # Force TCP Connection
        self.cnx._unix_socket = None
        self.cnx._open_connection()
        self.assertTrue(isinstance(self.cnx._socket,
                                   network.MySQLTCPSocket))
        self.cnx.close()

        self.cnx._client_flags |= constants.ClientFlag.COMPRESS
        self.cnx._open_connection()
        self.assertEqual(self.cnx._socket.recv_compressed,
                         self.cnx._socket.recv)
        self.assertEqual(self.cnx._socket.send_compressed,
                         self.cnx._socket.send)

    def test__post_connection(self):
        """Executes commands after connection has been established"""
        self.cnx._charset_id = 33
        self.cnx._autocommit = True
        self.cnx._time_zone = "-09:00"
        self.cnx._sql_mode = "STRICT_ALL_TABLES"
        self.cnx._post_connection()
        self.assertEqual('utf8', self.cnx.charset)
        self.assertEqual(self.cnx._autocommit, self.cnx.autocommit)
        self.assertEqual(self.cnx._time_zone, self.cnx.time_zone)
        self.assertEqual(self.cnx._sql_mode, self.cnx.sql_mode)

    def test_connect(self):
        """Connect to the MySQL server"""
        config = tests.get_mysql_config()
        config['unix_socket'] = None
        config['connection_timeout'] = 1

        cnx = connection.MySQLConnection()
        config['host'] = tests.get_mysql_config()['host']
        try:
            cnx.connect(**config)
            cnx.close()
        except errors.Error as err:
            self.fail("Failed connecting to '{}': {}".format(
                config['host'], str(err)))

        config['host'] = tests.fake_hostname()
        self.assertRaises(errors.InterfaceError, cnx.connect, **config)

    def test_reconnect(self):
        """Reconnecting to the MySQL Server"""
        supported_arguments = {
            'attempts': 1,
            'delay': 0,
        }
        self.check_args(self.cnx.reconnect, supported_arguments)

        def _test_reconnect_delay():
            config = {
                'unix_socket': None,
                'host': tests.fake_hostname(),
                'connection_timeout': 1,
            }
            cnx = connection.MySQLConnection()
            cnx.config(**config)
            try:
                cnx.reconnect(attempts=2, delay=3)
            except:
                pass

        # Check the delay
        timer = timeit.Timer(_test_reconnect_delay)
        result = timer.timeit(number=1)
        self.assertTrue(result > 3 and result < 12,
                        "3 <= result < 12, was {0}".format(result))

        # Check reconnect stops when successful
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection()
        cnx.connect(**config)
        conn_id = cnx.connection_id
        cnx.reconnect(attempts=5)
        exp = conn_id + 1
        self.assertEqual(exp, cnx.connection_id)

    def test_ping(self):
        """Ping the MySQL server"""
        supported_arguments = {
            'reconnect': False,
            'attempts': 1,
            'delay': 0,
        }
        self.check_args(self.cnx.ping, supported_arguments)

        try:
            self.cnx.ping()
        except errors.InterfaceError:
            self.fail("Ping should have not raised an error")

        self.cnx.disconnect()
        self.assertRaises(errors.InterfaceError, self.cnx.ping)

    def test_set_converter_class(self):
        """Set the converter class"""

        class TestConverterWrong(object):
            def __init__(self, charset, unicode):
                pass

        self.assertRaises(TypeError,
                          self.cnx.set_converter_class, TestConverterWrong)

        class TestConverter(MySQLConverterBase):
            def __init__(self, charset, unicode):
                pass

        self.cnx.set_converter_class(TestConverter)
        self.assertTrue(isinstance(self.cnx.converter, TestConverter))
        self.assertEqual(self.cnx._converter_class, TestConverter)

    def test_get_server_version(self):
        """Get the MySQL version"""
        self.assertEqual(self.cnx._server_version,
                         self.cnx.get_server_version())

    def test_get_server_info(self):
        """Get the original MySQL version information"""
        self.assertEqual(self.cnx._handshake['server_version_original'],
                         self.cnx.get_server_info())

        del self.cnx._handshake['server_version_original']
        self.assertEqual(None, self.cnx.get_server_info())

    def test_connection_id(self):
        """MySQL connection ID"""
        self.assertEqual(self.cnx._handshake['server_threadid'],
                         self.cnx.connection_id)

        del self.cnx._handshake['server_threadid']
        self.assertEqual(None, self.cnx.connection_id)

    def test_set_login(self):
        """Set login information for MySQL"""
        exp = ('Ham ', ' Spam ')
        self.cnx.set_login(*exp)
        self.assertEqual(exp[0].strip(), self.cnx._user)
        self.assertEqual(exp[1], self.cnx._password)

        self.cnx.set_login()
        self.assertEqual('', self.cnx._user)
        self.assertEqual('', self.cnx._password)

    def test_unread_results(self):
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
        except:
            self.fail("Expected ValueError to be raised")

    def test_get_warnings(self):
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
        except:
            self.fail("Expected ValueError to be raised")

    def test_set_charset_collation(self):
        """Set the character set and collation"""
        self.cnx.set_charset_collation('latin1')
        self.assertEqual(8, self.cnx._charset_id)
        self.cnx.set_charset_collation('latin1', 'latin1_general_ci')
        self.assertEqual(48, self.cnx._charset_id)
        self.cnx.set_charset_collation('latin1', None)
        self.assertEqual(8, self.cnx._charset_id)

        self.cnx.set_charset_collation(collation='greek_bin')
        self.assertEqual(70, self.cnx._charset_id)

        self.assertRaises(errors.ProgrammingError,
                          self.cnx.set_charset_collation, 666)
        self.assertRaises(errors.ProgrammingError,
                          self.cnx.set_charset_collation, 'spam')
        self.assertRaises(errors.ProgrammingError,
                          self.cnx.set_charset_collation, 'latin1', 'spam')
        self.assertRaises(errors.ProgrammingError,
                          self.cnx.set_charset_collation, None, 'spam')
        self.assertRaises(ValueError,
                          self.cnx.set_charset_collation, object())

    def test_charset(self):
        """Get character set name"""
        self.cnx.set_charset_collation('latin1', 'latin1_general_ci')
        self.assertEqual('latin1', self.cnx.charset)
        self.cnx._charset_id = 70
        self.assertEqual('greek', self.cnx.charset)
        self.cnx._charset_id = 9
        self.assertEqual('latin2', self.cnx.charset)

        self.cnx._charset_id = 1234567
        try:
            self.cnx.charset
        except errors.ProgrammingError:
            pass  # This is expected
        except:
            self.fail("Expected errors.ProgrammingError to be raised")

    def test_collation(self):
        """Get collation name"""
        exp = 'latin2_general_ci'
        self.cnx.set_charset_collation(collation=exp)
        self.assertEqual(exp, self.cnx.collation)
        self.cnx._charset_id = 70
        self.assertEqual('greek_bin', self.cnx.collation)
        self.cnx._charset_id = 9
        self.assertEqual('latin2_general_ci', self.cnx.collation)

        self.cnx._charset_id = 1234567
        try:
            self.cnx.collation
        except errors.ProgrammingError:
            pass  # This is expected
        except:
            self.fail("Expected errors.ProgrammingError to be raised")

    def test_set_client_flags(self):
        """Set the client flags"""
        self.assertRaises(errors.ProgrammingError,
                          self.cnx.set_client_flags, 'Spam')
        self.assertRaises(errors.ProgrammingError,
                          self.cnx.set_client_flags, 0)

        default_flags = constants.ClientFlag.get_default()

        exp = default_flags
        self.assertEqual(exp, self.cnx.set_client_flags(exp))
        self.assertEqual(exp, self.cnx._client_flags)

        exp = default_flags
        exp |= constants.ClientFlag.SSL
        exp |= constants.ClientFlag.FOUND_ROWS
        exp &= ~constants.ClientFlag.MULTI_RESULTS
        flags = [
            constants.ClientFlag.SSL,
            constants.ClientFlag.FOUND_ROWS,
            -constants.ClientFlag.MULTI_RESULTS
        ]
        self.assertEqual(exp, self.cnx.set_client_flags(flags))
        self.assertEqual(exp, self.cnx._client_flags)

    def test_user(self):
        exp = 'ham'
        self.cnx._user = exp
        self.assertEqual(exp, self.cnx.user)

    def test_host(self):
        exp = 'ham'
        self.cnx._host = exp
        self.assertEqual(exp, self.cnx.server_host)

    def test_port(self):
        exp = 'ham'
        self.cnx._port = exp
        self.assertEqual(exp, self.cnx.server_port)

    def test_unix_socket(self):
        exp = 'ham'
        self.cnx._unix_socket = exp
        self.assertEqual(exp, self.cnx.unix_socket)

    def test_database(self):
        exp = self.cnx.info_query("SELECT DATABASE()")[0]
        self.assertEqual(exp, self.cnx.database)
        exp = 'mysql'
        self.cnx.database = exp
        self.assertEqual(exp, self.cnx.database)

    def test_autocommit(self):
        for exp in [False, True]:
            self.cnx.autocommit = exp
            self.assertEqual(exp, self.cnx.autocommit)

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
        except:
            self.fail("Expected ValueError to be raised")

    def test_cursor(self):
        class FalseCursor(object):
            pass

        class TrueCursor(cursor.CursorBase):

            def __init__(self, cnx=None):
                if PY2:
                    super(TrueCursor, self).__init__()
                else:
                    super().__init__()

        self.assertRaises(errors.ProgrammingError, self.cnx.cursor,
                          cursor_class=FalseCursor)
        self.assertTrue(isinstance(self.cnx.cursor(cursor_class=TrueCursor),
                                   TrueCursor))

        cases = [
            ({}, cursor.MySQLCursor),
            ({'buffered': True}, cursor.MySQLCursorBuffered),
            ({'raw': True}, cursor.MySQLCursorRaw),
            ({'buffered': True, 'raw': True}, cursor.MySQLCursorBufferedRaw),
            ({'prepared': True}, cursor.MySQLCursorPrepared),
            ({'dictionary': True}, cursor.MySQLCursorDict),
            ({'named_tuple': True}, cursor.MySQLCursorNamedTuple),
            ({'dictionary': True, 'buffered': True},
             cursor.MySQLCursorBufferedDict),
            ({'named_tuple': True, 'buffered': True},
             cursor.MySQLCursorBufferedNamedTuple)
        ]
        for kwargs, exp in cases:
            self.assertTrue(isinstance(self.cnx.cursor(**kwargs), exp))

        self.assertRaises(ValueError, self.cnx.cursor, prepared=True,
                          buffered=True)
        self.assertRaises(ValueError, self.cnx.cursor, dictionary=True,
                          raw=True)
        self.assertRaises(ValueError, self.cnx.cursor, named_tuple=True,
                          raw=True)

        # Test when connection is closed
        self.cnx.close()
        self.assertRaises(errors.OperationalError, self.cnx.cursor)

    def test__send_data(self):
        self.assertRaises(ValueError, self.cnx._send_data, 'spam')

        self.cnx._socket.sock = tests.DummySocket()

        data = b"1\tham\t'ham spam'\n2\tfoo\t'foo bar'"

        fp = io.BytesIO(data)
        self.cnx._socket.sock.reset()
        self.cnx._socket.sock.add_packet(OK_PACKET)
        exp = [bytearray(
            b"\x20\x00\x00\x02\x31\x09\x68\x61\x6d\x09\x27\x68\x61\x6d"
            b"\x20\x73\x70\x61\x6d\x27\x0a\x32\x09\x66\x6f\x6f\x09\x27"
            b"\x66\x6f\x6f\x20\x62\x61\x72\x27"
        )]

        self.assertEqual(OK_PACKET, self.cnx._send_data(fp, False))
        self.assertEqual(exp, self.cnx._socket.sock._client_sends)

        fp = io.BytesIO(data)
        self.cnx._socket.sock.reset()
        self.cnx._socket.sock.add_packet(OK_PACKET)
        exp.append(bytearray(b'\x00\x00\x00\x03'))
        self.assertEqual(OK_PACKET, self.cnx._send_data(fp, True))
        self.assertEqual(exp, self.cnx._socket.sock._client_sends)

        fp = io.BytesIO(data)
        self.cnx._socket = None
        self.assertRaises(errors.OperationalError,
                          self.cnx._send_data, fp, False)
        # Nothing to read, but try to send empty packet
        self.assertRaises(errors.OperationalError,
                          self.cnx._send_data, fp, True)

    def test__handle_binary_ok(self):
        """Handle a Binary OK packet"""
        packet = bytearray(
            b'\x0c\x00\x00\x01'
            b'\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        )

        exp = {
            'num_params': 0,
            'statement_id': 1,
            'warning_count': 0,
            'num_columns': 0
        }
        self.assertEqual(exp, self.cnx._handle_binary_ok(packet))

        # Raise an error
        packet = bytearray(
            b'\x2a\x00\x00\x01\xff\x19\x05\x23\x34\x32\x30\x30\x30\x46\x55'
            b'\x4e\x43\x54\x49\x4f\x4e\x20\x74\x65\x73\x74\x2e\x53\x50\x41'
            b'\x4d\x20\x64\x6f\x65\x73\x20\x6e\x6f\x74\x20\x65\x78\x69\x73\x74'
        )
        self.assertRaises(errors.ProgrammingError,
                          self.cnx._handle_binary_ok, packet)

    def test_cmd_stmt_prepare(self):
        """Prepare a MySQL statement"""
        self.cnx._socket.sock = tests.DummySocket()

        stmt = b"SELECT CONCAT(?, ?) AS c1"
        self.cnx._socket.sock.add_packets([
            bytearray(
                b'\x0c\x00\x00\x01\x00\x01\x00\x00\x00\x01'
                b'\x00\x02\x00\x00\x00\x00'),
            bytearray(
                b'\x17\x00\x00\x02\x03\x64\x65\x66\x00\x00\x00\x01\x3f\x00\x0c'
                b'\x3f\x00\x00\x00\x00\x00\xfd\x80\x00\x00\x00\x00'),
            bytearray(
                b'\x17\x00\x00\x03\x03\x64\x65\x66\x00\x00\x00\x01\x3f\x00\x0c'
                b'\x3f\x00\x00\x00\x00\x00\xfd\x80\x00\x00\x00\x00'),
            EOF_PACKET,
            bytearray(
                b'\x18\x00\x00\x05\x03\x64\x65\x66\x00\x00\x00\x02\x63\x31\x00'
                b'\x0c\x3f\x00\x00\x00\x00\x00\xfd\x80\x00\x1f\x00\x00'),
            EOF_PACKET
        ])
        exp = {
            'num_params': 2,
            'statement_id': 1,
            'parameters': [
                ('?', 253, None, None, None, None, 1, 128),
                ('?', 253, None, None, None, None, 1, 128)
            ],
            'warning_count': 0,
            'num_columns': 1,
            'columns': [('c1', 253, None, None, None, None, 1, 128)]
        }
        self.assertEqual(exp, self.cnx.cmd_stmt_prepare(stmt))

        stmt = b"DO 1"
        self.cnx._socket.sock.reset()
        self.cnx._socket.sock.add_packets([
            bytearray(b'\x0c\x00\x00\x01\x00\x01\x00\x00\x00'
                      b'\x00\x00\x00\x00\x00\x00\x00')
        ])
        exp = {
            'num_params': 0,
            'statement_id': 1,
            'parameters': [],
            'warning_count': 0,
            'num_columns': 0,
            'columns': []
        }
        self.assertEqual(exp, self.cnx.cmd_stmt_prepare(stmt))

        # Raise an error using illegal SPAM() MySQL function
        stmt = b"SELECT SPAM(?) AS c1"
        self.cnx._socket.sock.reset()
        self.cnx._socket.sock.add_packets([
            bytearray(b'\x2a\x00\x00\x01\xff\x19\x05\x23\x34\x32\x30\x30\x30'
                      b'\x46\x55\x4e\x43\x54\x49\x4f\x4e\x20\x74\x65\x73\x74'
                      b'\x2e\x53\x50\x41\x4d\x20\x64\x6f\x65\x73\x20\x6e\x6f'
                      b'\x74\x20\x65\x78\x69\x73\x74')
        ])
        self.assertRaises(errors.ProgrammingError,
                          self.cnx.cmd_stmt_prepare, stmt)

    def test__handle_binary_result(self):
        self.cnx._socket.sock = tests.DummySocket()

        self.assertRaises(errors.InterfaceError,
                          self.cnx._handle_binary_result, None)
        self.assertRaises(errors.InterfaceError,
                          self.cnx._handle_binary_result,
                          bytearray(b'\x00\x00\x00'))

        self.assertEqual(OK_PACKET_RESULT,
                         self.cnx._handle_binary_result(OK_PACKET))
        self.assertEqual(EOF_PACKET_RESULT,
                         self.cnx._handle_binary_result(EOF_PACKET))

        self.assertRaises(errors.ProgrammingError,
                          self.cnx._handle_binary_result, ERR_PACKET)

        # handle result set
        self.cnx._socket.sock.reset()
        self.cnx._socket.sock.add_packets([
            bytearray(b'\x18\x00\x00\x02\x03\x64\x65\x66\x00\x00\x00\x02\x63'
                      b'\x31\x00\x0c\x21\x00\x09\x00\x00\x00\xfd\x01\x00\x00'
                      b'\x00\x00'),
            EOF_PACKET,
        ])
        exp = (
            1,
            [('c1', 253, None, None, None, None, 0, 1)],
            {'status_flag': 0, 'warning_count': 0}
        )
        self.assertEqual(
            exp, self.cnx._handle_binary_result(
                bytearray(b'\x01\x00\x00\x01\x01'))
        )

    def test_cmd_stmt_execute(self):
        stmt = b"SELECT ? as c1"
        params = (
            1,
            ('ham',),
            [('c1', 253, None, None, None, None, 1, 128)],
            0
        )

        # statement does not exists
        self.assertRaises(errors.DatabaseError, self.cnx.cmd_stmt_execute,
                          *params)

        # prepare and execute
        self.cnx.cmd_stmt_prepare(stmt)
        exp = (
            1,
            [('c1', 253, None, None, None, None, 0, 1)],
            {'status_flag': 0, 'warning_count': 0}
        )
        self.assertEqual(exp, self.cnx.cmd_stmt_execute(*params))

    def test_cmd_stmt_close(self):
        # statement does not exists, does not return or raise anything
        try:
            self.cnx.cmd_stmt_close(99)
        except errors.Error as err:
            self.fail("cmd_stmt_close raised: {0}".format(err))

        # after closing, should not be able to execute
        stmt_info = self.cnx.cmd_stmt_prepare(b"SELECT ? as c1")
        self.cnx.cmd_stmt_close(stmt_info['statement_id'])
        params = (
            stmt_info['statement_id'],
            ('ham',),
            stmt_info['parameters'],
            0
        )
        self.assertRaises(errors.ProgrammingError, self.cnx.cmd_stmt_execute,
                          *params)

    def test_cmd_reset_connection(self):
        """Resets session without re-authenticating"""
        if tests.MYSQL_VERSION < (5, 7, 3):
            self.assertRaises(errors.NotSupportedError,
                              self.cnx.cmd_reset_connection)
        else:
            exp_session_id = self.cnx.connection_id
            self.cnx.cmd_query("SET @ham = 2")
            self.cnx.cmd_reset_connection()

            self.cnx.cmd_query("SELECT @ham")
            self.assertEqual(exp_session_id, self.cnx.connection_id)
            if PY2:
                self.assertNotEqual(('2',), self.cnx.get_rows()[0][0])
            else:
                self.assertNotEqual((b'2',), self.cnx.get_rows()[0][0])

    def test_shutdown(self):
        """Shutting down a connection"""
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        sql = "SHOW GLOBAL STATUS WHERE variable_name LIKE 'Aborted_clients'"
        cur = cnx.cursor()
        cur.execute(sql)
        aborted_clients = cur.fetchone()[1]

        test_close_cnx = connection.MySQLConnection(**config)
        test_shutdown_cnx = connection.MySQLConnection(**config)

        test_close_cnx.close()
        cur.execute(sql)
        self.assertEqual(aborted_clients, cur.fetchone()[1])

        test_shutdown_cnx.shutdown()
        self.assertRaises(socket.error,
                          test_shutdown_cnx._socket.sock.getsockname)
        cur.execute(sql)
        self.assertEqual(str(int(aborted_clients) + 1), cur.fetchone()[1])

        cur.close()
        cnx.close()

    def test_handle_unread_result(self):
        config = tests.get_mysql_config()
        config['consume_results'] = True
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()
        cur.execute("SELECT 1,2,3")
        cur.execute("SELECT 1,2,3")
        cnx.handle_unread_result()
        self.assertEqual(False, cnx.unread_result)
        cur.close()

        config['consume_results'] = False
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()
        cur.execute("SELECT 1,2,3")
        self.assertRaises(errors.InternalError, cur.execute, "SELECT 1,2,3")
        cnx.consume_results()
        cur.close()
        cnx.close()


class WL7937(tests.MySQLConnectorTests):
    """Allow 'LOAD DATA LOCAL INFILE' by default
    """
    def setUp(self):
        config = tests.get_mysql_config()
        self.cnx = connection.MySQLConnection(**config)
        self.cur = self.cnx.cursor()

        self.data_file = os.path.join('tests', 'data', 'local_data.csv')

        self.cur.execute("DROP TABLE IF EXISTS local_data")
        self.cur.execute(
            "CREATE TABLE local_data (id int, c1 VARCHAR(6), c2 VARCHAR(6))")

    def tearDown(self):
        try:
            self.cur.execute("DROP TABLE IF EXISTS local_data")
            self.cur.close()
            self.cnx.close()
        except:
            pass

    def test_load_local_infile(self):
        sql = "LOAD DATA LOCAL INFILE %s INTO TABLE local_data"
        self.cur.execute(sql, (self.data_file, ))
        self.cur.execute("SELECT * FROM local_data")

        exp = [
            (1, 'c1_1', 'c2_1'), (2, 'c1_2', 'c2_2'),
            (3, 'c1_3', 'c2_3'), (4, 'c1_4', 'c2_4'),
            (5, 'c1_5', 'c2_5'), (6, 'c1_6', 'c2_6')]
        self.assertEqual(exp, self.cur.fetchall())

    def test_without_load_local_infile(self):
        config = tests.get_mysql_config()
        config['allow_local_infile'] = False

        self.cnx = connection.MySQLConnection(**config)
        self.cur = self.cnx.cursor()

        sql = "LOAD DATA LOCAL INFILE %s INTO TABLE local_data"
        self.assertRaises(errors.ProgrammingError, self.cur.execute, sql,
                          (self.data_file, ))
