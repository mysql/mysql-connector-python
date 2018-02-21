# Copyright (c) 2009, 2018, Oracle and/or its affiliates. All rights reserved.
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

"""Unittests for mysql.connector.protocol
"""

import struct
import datetime
import decimal

import tests
from mysql.connector import (protocol, errors)
from mysql.connector.constants import (ClientFlag, FieldType, FieldFlag)

OK_PACKET = bytearray(b'\x07\x00\x00\x01\x00\x01\x00\x00\x00\x01\x00')
OK_PACKET_RESULT = {
    'insert_id': 0,
    'affected_rows': 1,
    'field_count': 0,
    'warning_count': 1,
    'status_flag': 0
}

ERR_PACKET = bytearray(
    b'\x47\x00\x00\x02\xff\x15\x04\x23\x32\x38\x30\x30\x30'
    b'\x41\x63\x63\x65\x73\x73\x20\x64\x65\x6e\x69\x65\x64'
    b'\x20\x66\x6f\x72\x20\x75\x73\x65\x72\x20\x27\x68\x61'
    b'\x6d\x27\x40\x27\x6c\x6f\x63\x61\x6c\x68\x6f\x73\x74'
    b'\x27\x20\x28\x75\x73\x69\x6e\x67\x20\x70\x61\x73\x73'
    b'\x77\x6f\x72\x64\x3a\x20\x59\x45\x53\x29'
)

EOF_PACKET = bytearray(b'\x01\x00\x00\x00\xfe\x00\x00\x00\x00')
EOF_PACKET_RESULT = {'status_flag': 0, 'warning_count': 0}

SEED = bytearray(
    b'\x3b\x55\x78\x7d\x2c\x5f\x7c\x72\x49\x52'
    b'\x3f\x28\x47\x6f\x77\x28\x5f\x28\x46\x69'
)


class MySQLProtocolTests(tests.MySQLConnectorTests):
    def setUp(self):
        self._protocol = protocol.MySQLProtocol()

    def test_make_auth(self):
        """Make a MySQL authentication packet"""
        exp = {
            'allset': bytearray(
                b'\x8d\xa2\x03\x00\x00\x00\x00\x40'
                b'\x21\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
                b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
                b'\x68\x61\x6d\x00\x14\x3a\x07\x66\xba\xba\x01\xce'
                b'\xbe\x55\xe6\x29\x88\xaa\xae\xdb\x00\xb3\x4d\x91'
                b'\x5b\x74\x65\x73\x74\x00'),
            'nopass': bytearray(
                b'\x8d\xa2\x03\x00\x00\x00\x00\x40'
                b'\x21\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
                b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
                b'\x68\x61\x6d\x00\x00\x74\x65\x73\x74\x00'),
            'nouser': bytearray(
                b'\x8d\xa2\x03\x00\x00\x00\x00\x40'
                b'\x21\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
                b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
                b'\x00\x14\x3a\x07\x66\xba\xba\x01\xce'
                b'\xbe\x55\xe6\x29\x88\xaa\xae\xdb\x00\xb3\x4d\x91'
                b'\x5b\x74\x65\x73\x74\x00'),
            'nodb': bytearray(
                b'\x8d\xa2\x03\x00\x00\x00\x00\x40'
                b'\x21\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
                b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
                b'\x68\x61\x6d\x00\x14\x3a\x07\x66\xba\xba\x01\xce'
                b'\xbe\x55\xe6\x29\x88\xaa\xae\xdb\x00\xb3\x4d\x91'
                b'\x5b\x00'),
        }
        flags = ClientFlag.get_default()
        kwargs = {
            'handshake': None,
            'username': 'ham',
            'password': 'spam',
            'database': 'test',
            'charset': 33,
            'client_flags': flags
        }

        self.assertRaises(errors.ProgrammingError,
                          self._protocol.make_auth, **kwargs)

        kwargs['handshake'] = {'auth_data': SEED}
        self.assertRaises(errors.ProgrammingError,
                          self._protocol.make_auth, **kwargs)

        kwargs['handshake'] = {
            'auth_data': SEED,
            'auth_plugin': 'mysql_native_password'
        }
        res = self._protocol.make_auth(**kwargs)
        self.assertEqual(exp['allset'], res)

        kwargs['password'] = None
        res = self._protocol.make_auth(**kwargs)
        self.assertEqual(exp['nopass'], res)

        kwargs['password'] = 'spam'
        kwargs['database'] = None
        res = self._protocol.make_auth(**kwargs)
        self.assertEqual(exp['nodb'], res)

        kwargs['username'] = None
        kwargs['database'] = 'test'
        res = self._protocol.make_auth(**kwargs)
        self.assertEqual(exp['nouser'], res)

    def test_make_auth_ssl(self):
        """Make a SSL authentication packet"""
        cases = [
            ({},
             b'\x00\x00\x00\x00\x00\x00\x00\x40\x21\x00\x00\x00\x00\x00'
             b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
             b'\x00\x00\x00\x00'),
            ({'charset': 8},
             b'\x00\x00\x00\x00\x00\x00\x00\x40\x08\x00\x00\x00\x00\x00'
             b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
             b'\x00\x00\x00\x00'),
            ({'client_flags': 240141},
             b'\x0d\xaa\x03\x00\x00\x00\x00\x40\x21\x00\x00\x00\x00\x00'
             b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
             b'\x00\x00\x00\x00'),
            ({'charset': 8, 'client_flags': 240141,
              'max_allowed_packet': 2147483648},
             b'\x0d\xaa\x03\x00\x00\x00\x00\x80\x08\x00\x00\x00\x00\x00'
             b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
             b'\x00\x00\x00\x00'),
        ]
        for kwargs, exp in cases:
            self.assertEqual(exp, self._protocol.make_auth_ssl(**kwargs))

    def test_make_command(self):
        """Make a generic MySQL command packet"""
        exp = bytearray(b'\x01\x68\x61\x6d')
        arg = 'ham'.encode('utf-8')
        res = self._protocol.make_command(1, arg)
        self.assertEqual(exp, res)
        res = self._protocol.make_command(1, argument=arg)
        self.assertEqual(exp, res)

        exp = b'\x03'
        res = self._protocol.make_command(3)
        self.assertEqual(exp, res)

    def test_make_changeuser(self):
        """Make a change user MySQL packet"""
        exp = {
            'allset': bytearray(
                b'\x11\x68\x61\x6d\x00\x14\x3a\x07'
                b'\x66\xba\xba\x01\xce\xbe\x55\xe6\x29\x88\xaa\xae'
                b'\xdb\x00\xb3\x4d\x91\x5b\x74\x65\x73\x74\x00\x08'
                b'\x00'),
            'nopass': bytearray(
                b'\x11\x68\x61\x6d\x00\x00\x74\x65'
                b'\x73\x74\x00\x08\x00'),
        }
        kwargs = {
            'handshake': None,
            'username': 'ham',
            'password': 'spam',
            'database': 'test',
            'charset': 8,
            'client_flags': ClientFlag.get_default()
        }
        self.assertRaises(errors.ProgrammingError,
                          self._protocol.make_change_user, **kwargs)

        kwargs['handshake'] = {'auth_data': SEED}
        self.assertRaises(errors.ProgrammingError,
                          self._protocol.make_change_user, **kwargs)

        kwargs['handshake'] = {
            'auth_data': SEED,
            'auth_plugin': 'mysql_native_password'
        }
        res = self._protocol.make_change_user(**kwargs)
        self.assertEqual(exp['allset'], res)

        kwargs['password'] = None
        res = self._protocol.make_change_user(**kwargs)
        self.assertEqual(exp['nopass'], res)

    def test_parse_handshake(self):
        """Parse handshake-packet sent by MySQL"""
        handshake = bytearray(
            b'\x47\x00\x00\x00\x0a\x35\x2e\x30\x2e\x33\x30\x2d'
            b'\x65\x6e\x74\x65\x72\x70\x72\x69\x73\x65\x2d\x67'
            b'\x70\x6c\x2d\x6c\x6f\x67\x00\x09\x01\x00\x00\x68'
            b'\x34\x69\x36\x6f\x50\x21\x4f\x00\x2c\xa2\x08\x02'
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
            b'\x00\x00\x4c\x6e\x67\x39\x26\x50\x44\x40\x57\x72'
            b'\x59\x48\x00'
        )
        exp = {
            'protocol': 10,
            'server_version_original': '5.0.30-enterprise-gpl-log',
            'charset': 8,
            'server_threadid': 265,
            'capabilities': 41516,
            'server_status': 2,
            'auth_data': b'h4i6oP!OLng9&PD@WrYH',
            'auth_plugin': 'mysql_native_password',
        }

        res = self._protocol.parse_handshake(handshake)
        self.assertEqual(exp, res)

        # Test when end byte \x00 is not present for server 5.5.8
        handshake = handshake[:-1]
        res = self._protocol.parse_handshake(handshake)
        self.assertEqual(exp, res)

    def test_parse_ok(self):
        """Parse OK-packet sent by MySQL"""
        res = self._protocol.parse_ok(OK_PACKET)
        self.assertEqual(OK_PACKET_RESULT, res)

        okpkt = OK_PACKET + b'\x04spam'
        exp = OK_PACKET_RESULT.copy()
        exp['info_msg'] = 'spam'
        res = self._protocol.parse_ok(okpkt)
        self.assertEqual(exp, res)

    def test_parse_column_count(self):
        """Parse the number of columns"""
        packet = bytearray(b'\x01\x00\x00\x01\x03')
        res = self._protocol.parse_column_count(packet)
        self.assertEqual(3, res)

        packet = bytearray(b'\x01\x00')
        self.assertRaises(errors.InterfaceError,
                          self._protocol.parse_column_count, packet)

    def test_parse_column(self):
        """Parse field-packet sent by MySQL"""
        column_packet = bytearray(
            b'\x1a\x00\x00\x02\x03\x64\x65\x66\x00\x00\x00\x04'
            b'\x53\x70\x61\x6d\x00\x0c\x21\x00\x09\x00\x00\x00'
            b'\xfd\x01\x00\x1f\x00\x00')
        exp = ('Spam', 253, None, None, None, None, 0, 1)
        res = self._protocol.parse_column(column_packet)
        self.assertEqual(exp, res)

    def test_parse_eof(self):
        """Parse EOF-packet sent by MySQL"""
        res = self._protocol.parse_eof(EOF_PACKET)
        self.assertEqual(EOF_PACKET_RESULT, res)

    def test_read_text_result(self):
        # Tested by MySQLConnectionTests.test_get_rows() and .test_get_row()
        pass

    def test_parse_binary_prepare_ok(self):
        """Parse Prepare OK packet"""
        cases = [
            # SELECT CONCAT(?, ?) AS c1
            (bytearray(b'\x0c\x00\x00\x01'
                       b'\x00\x01\x00\x00\x00\x01\x00\x02\x00\x00\x00\x00'),
             {'num_params': 2,
              'statement_id': 1,
              'warning_count': 0,
              'num_columns': 1
             }
            ),
            # DO 1
            (bytearray(b'\x0c\x00\x00\x01'
                       b'\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'),
             {'num_params': 0,
              'statement_id': 1,
              'warning_count': 0,
              'num_columns': 0
             }
            ),
        ]
        for packet, exp in cases:
            self.assertEqual(exp,
                             self._protocol.parse_binary_prepare_ok(packet))

    def test__parse_binary_integer(self):
        """Parse an integer from a binary packet"""
        # Case = Expected value; pack format; field type; field flag
        cases = [
            (-128, 'b', FieldType.TINY, 0),
            (-32768, 'h', FieldType.SHORT, 0),
            (-2147483648, 'i', FieldType.LONG, 0),
            (-9999999999, 'q', FieldType.LONGLONG, 0),
            (255, 'B', FieldType.TINY, FieldFlag.UNSIGNED),
            (65535, 'H', FieldType.SHORT, FieldFlag.UNSIGNED),
            (4294967295, 'I', FieldType.LONG, FieldFlag.UNSIGNED),
            (9999999999, 'Q', FieldType.LONGLONG, FieldFlag.UNSIGNED),
        ]
        field_info = [None] * 8
        field_info[0] = 'c1'
        for exp, fmt, field_type, flag in cases:
            field_info[1] = field_type
            field_info[7] = flag
            data = struct.pack(fmt, exp) + b'\x00\x00'
            res = self._protocol._parse_binary_integer(data, field_info)
            self.assertEqual((b'\x00\x00', exp), res,
                             "Failed parsing binary integer '{0}'".format(exp))

    def test__parse_binary_float(self):
        """Parse a float/double from a binary packet"""
        # Case = Expected value; data; field type
        cases = [
            (-3.14159, bytearray(b'\x6e\x86\x1b\xf0\xf9\x21\x09\xc0'),
             FieldType.DOUBLE),
            (3.14159, bytearray(b'\x6e\x86\x1b\xf0\xf9\x21\x09\x40'),
             FieldType.DOUBLE),
            (-3.14, bytearray(b'\xc3\xf5\x48\xc0'), FieldType.FLOAT),
            (3.14, bytearray(b'\xc3\xf5\x48\x40'), FieldType.FLOAT),
        ]
        field_info = [None] * 8
        field_info[0] = 'c1'
        for exp, data, field_type in cases:
            field_info[1] = field_type
            res = self._protocol._parse_binary_float(data + b'\x00\x00',
                                                     field_info)
            self.assertEqual(bytearray(b'\x00\x00'), res[0],
                             "Failed parsing binary float '{0}'".format(exp))
            self.assertAlmostEqual(
                exp, res[1], places=5,
                msg="Failed parsing binary float '{0}'".format(exp))

    def test__parse_binary_timestamp(self):
        """Parse a timestamp from a binary packet"""
        # Case = Expected value; data
        cases = [
            (datetime.date(1977, 6, 14), bytearray(b'\x04\xb9\x07\x06\x0e')),
            (datetime.datetime(1977, 6, 14, 21, 33, 14),
             bytearray(b'\x07\xb9\x07\x06\x0e\x15\x21\x0e')),
            (datetime.datetime(1977, 6, 14, 21, 33, 14, 345),
             bytearray(b'\x0b\xb9\x07\x06\x0e\x15\x21\x0e\x59\x01\x00\x00'))
        ]
        for exp, data in cases:
            res = self._protocol._parse_binary_timestamp(data + b'\x00\x00',
                                                         None)
            self.assertEqual((b'\x00\x00', exp), res,
                             "Failed parsing timestamp '{0}'".format(exp))

    def test__parse_binary_time(self):
        """Parse a time value from a binary packet"""
        cases = [
            (datetime.timedelta(0, 44130),
             bytearray(b'\x08\x00\x00\x00\x00\x00\x0c\x0f\x1e')),
            (datetime.timedelta(14, 15330),
             bytearray(b'\x08\x00\x0e\x00\x00\x00\x04\x0f\x1e')),
            (datetime.timedelta(-14, 15330),
             bytearray(b'\x08\x01\x0e\x00\x00\x00\x04\x0f\x1e')),
            (datetime.timedelta(10, 58530, 230000),
             bytearray(b'\x0c\x00\x0a\x00\x00\x00'
                       b'\x10\x0f\x1e\x70\x82\x03\x00')),
        ]
        for exp, data in cases:
            res = self._protocol._parse_binary_time(data + b'\x00\x00', None)
            self.assertEqual((bytearray(b'\x00\x00'), exp), res,
                             "Failed parsing time '{0}'".format(exp))

    def test__parse_binary_values(self):
        """Parse values from a binary result packet"""
        # The packet in this test is result of the following query:
        #       SELECT 'abc' AS aStr,"
        #        "3.14 AS aFloat,"
        #        "-3.14159 AS aDouble, "
        #        "MAKEDATE(2003, 31) AS aDate, "
        #        "TIMESTAMP('1977-06-14', '21:33:14') AS aDateTime, "
        #        "MAKETIME(256,15,30.23) AS aTime, "
        #        "NULL AS aNull"
        #

        fields = [('aStr', 253, None, None, None, None, 0, 1),
                  ('aFloat', 246, None, None, None, None, 0, 129),
                  ('aDouble', 246, None, None, None, None, 0, 129),
                  ('aDate', 10, None, None, None, None, 1, 128),
                  ('aDateTime', 12, None, None, None, None, 1, 128),
                  ('aTime', 11, None, None, None, None, 1, 128),
                  ('aNull', 6, None, None, None, None, 1, 128)]

        packet = bytearray(b'\x00\x01\x03\x61\x62\x63\x04\x33\x2e\x31\x34\x08'
                           b'\x2d\x33\x2e\x31\x34\x31\x35\x39\x04\xd3\x07'
                           b'\x01\x1f\x07\xb9\x07\x06\x0e\x15\x21\x0e\x0c'
                           b'\x00\x0a\x00\x00\x00\x10\x0f\x1e\x70\x82\x03\x00')

        # float/double are returned as DECIMAL by MySQL
        exp = ('abc',
               '3.14',
               '-3.14159',
               datetime.date(2003, 1, 31),
               datetime.datetime(1977, 6, 14, 21, 33, 14),
               datetime.timedelta(10, 58530, 230000),
               None)
        res = self._protocol._parse_binary_values(fields, packet)
        self.assertEqual(exp, res)

    def test_read_binary_result(self):
        """Read MySQL binary protocol result"""

    def test__prepare_binary_integer(self):
        """Prepare an integer for the MySQL binary protocol"""
        # Case = Data; expected value
        cases = [
            (-128, (struct.pack('b', -128), FieldType.TINY, 0)),
            (-32768, (struct.pack('h', -32768), FieldType.SHORT, 0)),
            (-2147483648,
             (struct.pack('i', -2147483648), FieldType.LONG, 0)),
            (-9999999999,
             (struct.pack('q', -9999999999), FieldType.LONGLONG, 0)),

            (255, (struct.pack('B', 255), FieldType.TINY, 128)),
            (65535, (struct.pack('H', 65535), FieldType.SHORT, 128)),
            (4294967295,
             (struct.pack('I', 4294967295), FieldType.LONG, 128)),
            (9999999999,
             (struct.pack('Q', 9999999999), FieldType.LONGLONG, 128)),
        ]
        for data, exp in cases:
            res = self._protocol._prepare_binary_integer(data)
            self.assertEqual(exp, res,
                             "Failed preparing value '{0}'".format(data))

    def test__prepare_binary_timestamp(self):
        """Prepare a timestamp object for the MySQL binary protocol"""
        cases = [
            (datetime.date(1977, 6, 14),
             (bytearray(b'\x04\xb9\x07\x06\x0e'), 10)),
            (datetime.datetime(1977, 6, 14),
             (bytearray(b'\x07\xb9\x07\x06\x0e\x00\x00\x00'), 12)),
            (datetime.datetime(1977, 6, 14, 21, 33, 14),
             (bytearray(b'\x07\xb9\x07\x06\x0e\x15\x21\x0e'), 12)),
            (datetime.datetime(1977, 6, 14, 21, 33, 14, 345),
             (bytearray(b'\x0b\xb9\x07\x06\x0e\x15'
                        b'\x21\x0e\x59\x01\x00\x00'), 12)),
        ]
        for data, exp in cases:
            self.assertEqual(exp,
                             self._protocol._prepare_binary_timestamp(data),
                             "Failed preparing value '{0}'".format(data))

        # Raise an error
        self.assertRaises(ValueError,
                          self._protocol._prepare_binary_timestamp, 'spam')

    def test__prepare_binary_time(self):
        """Prepare a time object for the MySQL binary protocol"""
        cases = [
            (datetime.timedelta(hours=123, minutes=45, seconds=16),
             (bytearray(b'\x08\x00\x05\x00\x00\x00\x03\x2d\x10'), 11)),
            (datetime.timedelta(hours=-123, minutes=45, seconds=16),
             (bytearray(b'\x08\x01\x06\x00\x00\x00\x15\x2d\x10'), 11)),
            (datetime.timedelta(hours=123, minutes=45, seconds=16,
                                microseconds=345),
             (bytearray(b'\x0c\x00\x05\x00\x00\x00\x03'
                        b'\x2d\x10\x59\x01\x00\x00'), 11)),
            (datetime.timedelta(days=123, minutes=45, seconds=16),
             (bytearray(b'\x08\x00\x7b\x00\x00\x00\x00\x2d\x10'), 11)),
            (datetime.time(14, 53, 36),
             (bytearray(b'\x08\x00\x00\x00\x00\x00\x0e\x35\x24'), 11)),
            (datetime.time(14, 53, 36, 345),
             (bytearray(b'\x0c\x00\x00\x00\x00\x00\x0e'
                        b'\x35\x24\x59\x01\x00\x00'), 11))
        ]
        for data, exp in cases:
            self.assertEqual(exp,
                             self._protocol._prepare_binary_time(data),
                             "Failed preparing value '{0}'".format(data))

        # Raise an error
        self.assertRaises(ValueError,
                          self._protocol._prepare_binary_time, 'spam')

    def test_make_stmt_execute(self):
        """Make a MySQL packet with the STMT_EXECUTE command"""
        statement_id = 1
        self.assertRaises(errors.InterfaceError,
                          self._protocol.make_stmt_execute, statement_id,
                          ('ham', 'spam'), (1, 2, 3))

        data = ('ham', 'spam')
        exp = bytearray(
            b'\x01\x00\x00\x00\x00\x01\x00\x00\x00\x00\x01\x0f'
            b'\x00\x0f\x00\x03\x68\x61\x6d\x04\x73\x70\x61\x6d'
        )
        res = self._protocol.make_stmt_execute(statement_id, data, (1, 2))
        self.assertEqual(exp, res)

        # Testing types
        cases = [
            ('ham',
             bytearray(b'\x01\x00\x00\x00\x00\x01\x00\x00\x00\x00'
                       b'\x01\x0f\x00\x03\x68\x61\x6d')),
            (decimal.Decimal('3.14'),
             bytearray(b'\x01\x00\x00\x00\x00\x01\x00\x00\x00\x00'
                       b'\x01\x00\x00\x04\x33\x2e\x31\x34')),
            (255,
             bytearray(b'\x01\x00\x00\x00\x80\x01\x00'
                       b'\x00\x00\x00\x01\x01\x80\xff')),
            (-128,
             bytearray(b'\x01\x00\x00\x00\x00\x01\x00'
                       b'\x00\x00\x00\x01\x01\x00\x80')),
            (datetime.datetime(1977, 6, 14, 21, 20, 30),
             bytearray(b'\x01\x00\x00\x00\x00\x01\x00\x00\x00\x00'
                       b'\x01\x0c\x00\x07\xb9\x07\x06\x0e\x15\x14\x1e')),
            (datetime.time(14, 53, 36, 345),
             bytearray(b'\x01\x00\x00\x00\x00\x01\x00\x00\x00\x00\x01\x0b\x00'
                       b'\x0c\x00\x00\x00\x00\x00\x0e\x35\x24\x59\x01\x00\x00')),
            (3.14,
             bytearray(b'\x01\x00\x00\x00\x00\x01\x00\x00\x00\x00\x01\x05\x00'
                       b'\x1f\x85\xeb\x51\xb8\x1e\x09\x40')),
        ]
        for data, exp in cases:
            res = self._protocol.make_stmt_execute(statement_id, (data,), (1,))
            self.assertEqual(
                exp, res, "Failed preparing statement with '{0}'".format(data))

        # Testing null bitmap
        data = (None, None)
        exp = bytearray(b'\x01\x00\x00\x00\x00\x01\x00\x00\x00\x03\x01\x06'
                        b'\x00\x06\x00')
        res = self._protocol.make_stmt_execute(statement_id, data, (1, 2))
        self.assertEqual(exp, res)

        data = (None, 'Ham')
        exp = bytearray(
            b'\x01\x00\x00\x00\x00\x01\x00\x00\x00\x01\x01\x06\x00\x0f\x00'
            b'\x03\x48\x61\x6d'
        )
        res = self._protocol.make_stmt_execute(statement_id, data, (1, 2))
        self.assertEqual(exp, res)

        data = ('a',) * 11
        exp = bytearray(
            b'\x01\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x01'
            b'\x0f\x00\x0f\x00\x0f\x00\x0f\x00\x0f\x00\x0f\x00\x0f\x00'
            b'\x0f\x00\x0f\x00\x0f\x00\x0f\x00\x01\x61\x01\x61\x01\x61'
            b'\x01\x61\x01\x61\x01\x61\x01\x61\x01\x61\x01\x61\x01\x61'
            b'\x01\x61'
        )
        res = self._protocol.make_stmt_execute(statement_id, data, (1,) * 11)
        self.assertEqual(exp, res)

        # Raise an error passing an unsupported object as parameter value
        class UnSupportedObject(object):
            pass

        data = (UnSupportedObject(), UnSupportedObject())
        self.assertRaises(errors.ProgrammingError,
                          self._protocol.make_stmt_execute,
                          statement_id, data, (1, 2))
