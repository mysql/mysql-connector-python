# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2013, Oracle and/or its affiliates. All rights reserved.

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


import errno
import socket
from functools import wraps
import types


class SkipTest(Exception):

    """Exception compatible with SkipTest of Python v2.7 and later"""


def _id(obj):
    """Function defined in unittest.case which is needed for decorators"""
    return obj


def test_skip(reason):
    """Skip test

    This decorator is used by Python v2.6 code to keep compatible with
    Python v2.7 (and later) unittest.skip.
    """
    def decorator(test):
        if not isinstance(test, (type, types.ClassType)):
            @wraps(test)
            def wrapper(*args, **kwargs):
                raise SkipTest(reason)
            test = wrapper

        test.__unittest_skip__ = True
        test.__unittest_skip_why__ = reason
        return test
    return decorator


def test_skip_if(condition, reason):
    """Skip test if condition is true

    This decorator is used by Python v2.6 code to keep compatible with
    Python v2.7 (and later) unittest.skipIf.
    """
    if condition:
        return test_skip(reason)
    return _id


class DummySocket(object):

    """Dummy socket class

    This class helps to test socket connection without actually making any
    network activity. It is a proxy class using socket.socket.
    """

    def __init__(self, *args):
        self._socket = socket.socket(*args)
        self._server_replies = ''
        self._client_sends = []
        self._raise_socket_error = 0

    def __getattr__(self, attr):
        return getattr(self._socket, attr)

    def raise_socket_error(self, err=errno.EPERM):
        self._raise_socket_error = err

    def recv(self, bufsize=4096, flags=0):
        if self._raise_socket_error:
            raise socket.error(self._raise_socket_error)
        res = self._server_replies[0:bufsize]
        self._server_replies = self._server_replies[bufsize:]
        return res

    def send(self, string, flags=0):
        if self._raise_socket_error:
            raise socket.error(self._raise_socket_error)
        self._client_sends.append(string)
        return len(string)

    def sendall(self, string, flags=0):
        self._client_sends.append(string)
        return None

    def add_packet(self, packet):
        self._server_replies += packet

    def add_packets(self, packets):
        for packet in packets:
            self._server_replies += packet

    def reset(self):
        self._raise_socket_error = 0
        self._server_replies = ''
        self._client_sends = []

    def get_address(self):
        return 'dummy'
