# Copyright (c) 2012, 2017, Oracle and/or its affiliates. All rights reserved.
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

"""Unittests for mysql.connector.network
"""

import os
import socket
import logging
from collections import deque
import unittest

import tests
from mysql.connector import (network, errors, constants)

LOGGER = logging.getLogger(tests.LOGGER_NAME)


class NetworkTests(tests.MySQLConnectorTests):

    """Testing mysql.connector.network functions"""

    def test__prepare_packets(self):
        """Prepare packets for sending"""
        data = (b'abcdefghijklmn', 1)
        exp = [b'\x0e\x00\x00\x01abcdefghijklmn']
        self.assertEqual(exp, network._prepare_packets(*(data)))

        data = (b'a' * (constants.MAX_PACKET_LENGTH + 1000), 2)
        exp = [
            b'\xff\xff\xff\x02' + (b'a' * constants.MAX_PACKET_LENGTH),
            b'\xe8\x03\x00\x03' + (b'a' * 1000)
        ]
        self.assertEqual(exp, network._prepare_packets(*(data)))


class BaseMySQLSocketTests(tests.MySQLConnectorTests):

    """Testing mysql.connector.network.BaseMySQLSocket"""

    def setUp(self):
        config = tests.get_mysql_config()
        self._host = config['host']
        self._port = config['port']
        self.cnx = network.BaseMySQLSocket()

    def tearDown(self):
        try:
            self.cnx.close_connection()
        except:
            pass

    def _get_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        LOGGER.debug("Get socket for {host}:{port}".format(
            host=self._host, port=self._port))
        sock.connect((self._host, self._port))
        return sock

    def test_init(self):
        """MySQLSocket initialization"""
        exp = {
            'sock': None,
            '_connection_timeout': None,
            '_packet_queue': deque(),
            'recvsize': 1024 * 8,
        }

        for key, value in exp.items():
            self.assertEqual(value, self.cnx.__dict__[key])

    def test_next_packet_number(self):
        """Test packet number property"""
        self.assertEqual(0, self.cnx.next_packet_number)
        self.assertEqual(0, self.cnx._packet_number)
        self.assertEqual(1, self.cnx.next_packet_number)
        self.assertEqual(1, self.cnx._packet_number)
        self.cnx._packet_number = 255
        self.assertEqual(0, self.cnx.next_packet_number)

    def test_open_connection(self):
        """Opening a connection"""
        self.assertRaises(NotImplementedError, self.cnx.open_connection)

    def test_get_address(self):
        """Get the address of a connection"""
        self.assertRaises(NotImplementedError, self.cnx.get_address)

    def test_shutdown(self):
        """Shutting down a connection"""
        self.cnx.shutdown()
        self.assertEqual(None, self.cnx.sock)

    def test_close_connection(self):
        """Closing a connection"""
        self.cnx.close_connection()
        self.assertEqual(None, self.cnx.sock)

    def test_send_plain(self):
        """Send plain data through the socket"""
        data = b'asddfasdfasdf'
        self.assertRaises(errors.OperationalError, self.cnx.send_plain,
                          data, 0)

        self.cnx.sock = tests.DummySocket()
        data = [
            (b'\x03\x53\x45\x4c\x45\x43\x54\x20\x22\x61\x62\x63\x22', 1),
            (b'\x03\x53\x45\x4c\x45\x43\x54\x20\x22'
             + (b'\x61' * (constants.MAX_PACKET_LENGTH + 1000)) + b'\x22', 2)]

        self.assertRaises(Exception, self.cnx.send_plain, None, None)

        for value in data:
            exp = network._prepare_packets(*value)
            try:
                self.cnx.send_plain(*value)
            except errors.Error as err:
                self.fail("Failed sending pktnr {}: {}".format(value[1],
                                                               str(err)))
            self.assertEqual(exp, self.cnx.sock._client_sends)
            self.cnx.sock.reset()

    def test_send_compressed(self):
        """Send compressed data through the socket"""
        data = b'asddfasdfasdf'
        self.assertRaises(errors.OperationalError, self.cnx.send_compressed,
                          data, 0)

        self.cnx.sock = tests.DummySocket()
        self.assertRaises(Exception, self.cnx.send_compressed, None, None)

        # Small packet
        data = (b'\x03\x53\x45\x4c\x45\x43\x54\x20\x22\x61\x62\x63\x22', 1)
        exp = [b'\x11\x00\x00\x02\x00\x00\x00\r\x00\x00\x01\x03SELECT "abc"']
        try:
            self.cnx.send_compressed(*data)
        except errors.Error as err:
            self.fail("Failed sending pktnr {}: {}".format(data[1], err))
        self.assertEqual(exp, self.cnx.sock._client_sends)
        self.cnx.sock.reset()

        # Slightly bigger packet (not getting compressed)
        data = (b'\x03\x53\x45\x4c\x45\x43\x54\x20\x22\x61\x62\x63\x22', 1)
        exp = (24, b'\x11\x00\x00\x03\x00\x00\x00\x0d\x00\x00\x01\x03'
               b'\x53\x45\x4c\x45\x43\x54\x20\x22')
        try:
            self.cnx.send_compressed(*data)
        except errors.Error as err:
            self.fail("Failed sending pktnr {}: {}".format(data[1], str(err)))
        received = self.cnx.sock._client_sends[0]
        self.assertEqual(exp, (len(received), received[:20]))
        self.cnx.sock.reset()

        # Big packet
        data = (b'\x03\x53\x45\x4c\x45\x43\x54\x20\x22'
                + b'\x61' * (constants.MAX_PACKET_LENGTH + 1000) + b'\x22', 2)
        exp = [
            (63, b'\x38\x00\x00\x04\x00\x40\x00\x78\x9c\xed\xc1\x31'
                b'\x0d\x00\x20\x0c\x00\xb0\x04\x8c'),
            (16322, b'\xbb\x3f\x00\x05\xf9\xc3\xff\x78\x9c\xec\xc1\x81'
                b'\x00\x00\x00\x00\x80\x20\xd6\xfd')]
        try:
            self.cnx.send_compressed(*data)
        except errors.Error as err:
            self.fail("Failed sending pktnr {}: {}".format(data[1], str(err)))
        received = [(len(r), r[:20]) for r in self.cnx.sock._client_sends]
        self.assertEqual(exp, received)
        self.cnx.sock.reset()

    def test_recv_plain(self):
        """Receive data from the socket"""
        self.cnx.sock = tests.DummySocket()

        def get_address():
            return 'dummy'
        self.cnx.get_address = get_address

        # Receive a packet which is not 4 bytes long
        self.cnx.sock.add_packet(b'\01\01\01')
        self.assertRaises(errors.InterfaceError, self.cnx.recv_plain)

        # Socket fails to receive and produces an error
        self.cnx.sock.raise_socket_error()
        self.assertRaises(errors.OperationalError, self.cnx.recv_plain)

        # Receive packets after a query, SELECT "Ham"
        exp = [
            b'\x01\x00\x00\x01\x01',
            b'\x19\x00\x00\x02\x03\x64\x65\x66\x00\x00\x00\x03\x48\x61\x6d\x00'
            b'\x0c\x21\x00\x09\x00\x00\x00\xfd\x01\x00\x1f\x00\x00',
            b'\x05\x00\x00\x03\xfe\x00\x00\x02\x00',
            b'\x04\x00\x00\x04\x03\x48\x61\x6d',
            b'\x05\x00\x00\x05\xfe\x00\x00\x02\x00',
        ]
        self.cnx.sock.reset()
        self.cnx.sock.add_packets(exp)
        length_exp = len(exp)
        result = []
        packet = self.cnx.recv_plain()
        while packet:
            result.append(packet)
            if length_exp == len(result):
                break
            packet = self.cnx.recv_plain()
        self.assertEqual(exp, result)

    def test_recv_compressed(self):
        """Receive compressed data from the socket"""
        self.cnx.sock = tests.DummySocket()

        def get_address():
            return 'dummy'
        self.cnx.get_address = get_address

        # Receive a packet which is not 7 bytes long
        self.cnx.sock.add_packet(b'\01\01\01\01\01\01')
        self.assertRaises(errors.InterfaceError, self.cnx.recv_compressed)

        # Receive the header of a packet, but nothing more
        self.cnx.sock.add_packet(b'\01\00\00\00\00\00\00')
        self.assertRaises(errors.InterfaceError, self.cnx.recv_compressed)

        # Socket fails to receive and produces an error
        self.cnx.sock.raise_socket_error()
        self.assertRaises(errors.OperationalError, self.cnx.recv_compressed)

    def test_set_connection_timeout(self):
        """Set the connection timeout"""
        exp = 5
        self.cnx.set_connection_timeout(exp)
        self.assertEqual(exp, self.cnx._connection_timeout)


@unittest.skipIf(os.name == 'nt', "Skip UNIX Socket tests on Windows")
class MySQLUnixSocketTests(tests.MySQLConnectorTests):

    """Testing mysql.connector.network.MySQLUnixSocket"""

    def setUp(self):
        config = tests.get_mysql_config()
        self._unix_socket = config['unix_socket']
        self.cnx = network.MySQLUnixSocket(unix_socket=config['unix_socket'])

    def tearDown(self):
        try:
            self.cnx.close_connection()
        except:
            pass

    def test_init(self):
        """MySQLUnixSocket initialization"""
        exp = {
            'unix_socket': self._unix_socket,
        }

        for key, value in exp.items():
            self.assertEqual(value, self.cnx.__dict__[key])

    def test_get_address(self):
        """Get path to the Unix socket"""
        exp = self._unix_socket
        self.assertEqual(exp, self.cnx.get_address())

    def test_open_connection(self):
        """Open a connection using a Unix socket"""
        if os.name == 'nt':
            self.assertRaises(errors.InterfaceError, self.cnx.open_connection)
        else:
            try:
                self.cnx.open_connection()
            except errors.Error as err:
                self.fail(str(err))

    @unittest.skipIf(not tests.SSL_AVAILABLE,
                     "Could not test switch to SSL. Make sure Python supports "
                     "SSL.")
    def test_switch_to_ssl(self):
        """Switch the socket to use SSL"""
        args = {
            'ca': os.path.join(tests.SSL_DIR, 'tests_CA_cert.pem'),
            'cert': os.path.join(tests.SSL_DIR, 'tests_client_cert.pem'),
            'key': os.path.join(tests.SSL_DIR, 'tests_client_key.pem'),
            'cipher': 'AES256-SHA'
        }
        self.assertRaises(errors.InterfaceError,
                          self.cnx.switch_to_ssl, **args)

        # Handshake failure
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(4)
        sock.connect(self._unix_socket)
        self.cnx.sock = sock
        self.assertRaises(errors.InterfaceError,
                          self.cnx.switch_to_ssl, **args)


class MySQLTCPSocketTests(tests.MySQLConnectorTests):

    """Testing mysql.connector.network..MySQLTCPSocket"""

    def setUp(self):
        config = tests.get_mysql_config()
        self._host = config['host']
        self._port = config['port']
        self.cnx = network.MySQLTCPSocket(host=self._host, port=self._port)

    def tearDown(self):
        try:
            self.cnx.close_connection()
        except:
            pass

    def test_init(self):
        """MySQLTCPSocket initialization"""
        exp = {
            'server_host': self._host,
            'server_port': self._port,
        }

        for key, value in exp.items():
            self.assertEqual(value, self.cnx.__dict__[key])

    def test_get_address(self):
        """Get TCP/IP address"""
        exp = "%s:%s" % (self._host, self._port)
        self.assertEqual(exp, self.cnx.get_address())

    @unittest.skipIf(tests.IPV6_AVAILABLE, "Testing IPv6, not testing IPv4")
    def test_open_connection__ipv4(self):
        """Open a connection using TCP"""
        try:
            self.cnx.open_connection()
        except errors.Error as err:
            self.fail(str(err))

        config = tests.get_mysql_config()
        self._host = config['host']
        self._port = config['port']

        cases = [
            # Address, Expected Family, Should Raise, Force IPv6
            (tests.get_mysql_config()['host'], socket.AF_INET, False, False),
            ]

        for case in cases:
            self._test_open_connection(*case)

    @unittest.skipIf(not tests.IPV6_AVAILABLE, "IPv6 testing disabled")
    def test_open_connection__ipv6(self):
        """Open a connection using TCP"""
        config = tests.get_mysql_config()
        self._host = config['host']
        self._port = config['port']

        cases = [
            # Address, Expected Family, Should Raise, Force IPv6
            ('::1', socket.AF_INET6, False, False),
            ('2001::14:06:77', socket.AF_INET6, True, False),
            ('xx:00:xx', socket.AF_INET6, True, False),
            ]

        for case in cases:
            self._test_open_connection(*case)

    def _test_open_connection(self, addr, family, should_raise, force):
        try:
            sock = network.MySQLTCPSocket(host=addr,
                                          port=self._port,
                                          force_ipv6=force)
            sock.set_connection_timeout(1)
            sock.open_connection()
        except (errors.InterfaceError, socket.error):
            if not should_raise:
                self.fail('{0} incorrectly raised socket.error'.format(
                    addr))
        else:
            if should_raise:
                self.fail('{0} should have raised socket.error'.format(
                    addr))
            else:
                self.assertEqual(family, sock._family,
                                 "Family for {0} did not match".format(
                                     addr, family, sock._family))
            sock.close_connection()

    @unittest.skipIf(not tests.SSL_AVAILABLE,
                     "Could not test switch to SSL. Make sure Python supports "
                     "SSL.")
    def test_switch_to_ssl(self):
        """Switch the socket to use SSL"""
        args = {
            'ca': os.path.join(tests.SSL_DIR, 'tests_CA_cert.pem'),
            'cert': os.path.join(tests.SSL_DIR, 'tests_client_cert.pem'),
            'key': os.path.join(tests.SSL_DIR, 'tests_client_key.pem'),
        }
        self.assertRaises(errors.InterfaceError,
                          self.cnx.switch_to_ssl, **args)

        # Handshake failure
        (family, socktype, proto, _,
         sockaddr) = socket.getaddrinfo(self._host, self._port,
                                        socket.AF_INET, socket.SOCK_STREAM)[0]
        sock = socket.socket(family, socktype, proto)
        sock.settimeout(4)
        sock.connect(sockaddr)
        self.cnx.sock = sock
        self.assertRaises(errors.InterfaceError,
                          self.cnx.switch_to_ssl, **args)
