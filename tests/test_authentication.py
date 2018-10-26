# -*- coding: utf-8 -*-

# Copyright (c) 2014, 2017, Oracle and/or its affiliates. All rights reserved.
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

"""Test module for authentication

"""

import inspect
import sys

import mysql.connector
from mysql.connector import authentication

import tests
from . import PY2


_STANDARD_PLUGINS = (
    'mysql_native_password',
    'mysql_clear_password',
    'sha256_password',
)


class AuthenticationModuleTests(tests.MySQLConnectorTests):

    """Tests globals and functions of the authentication module"""

    def test_get_auth_plugin(self):
        self.assertRaises(mysql.connector.NotSupportedError,
                          authentication.get_auth_plugin, 'spam')

        self.assertRaises(mysql.connector.NotSupportedError,
                          authentication.get_auth_plugin, '')

        # Test using standard plugins
        plugin_classes = {}
        for name, obj in inspect.getmembers(authentication):
            if inspect.isclass(obj) and hasattr(obj, 'plugin_name'):
                if obj.plugin_name:
                    plugin_classes[obj.plugin_name] = obj
        for plugin_name in _STANDARD_PLUGINS:
            self.assertEqual(plugin_classes[plugin_name],
                             authentication.get_auth_plugin(plugin_name),
                             "Failed getting class for {0}".format(plugin_name))


class BaseAuthPluginTests(tests.MySQLConnectorTests):

    """Tests authentication.BaseAuthPlugin"""

    def test_class(self):
        self.assertEqual('', authentication.BaseAuthPlugin.plugin_name)
        self.assertEqual(False, authentication.BaseAuthPlugin.requires_ssl)

    def test___init__(self):
        base = authentication.BaseAuthPlugin('ham')
        self.assertEqual('ham', base._auth_data)
        self.assertEqual(None, base._username)
        self.assertEqual(None, base._password)
        self.assertEqual(None, base._database)
        self.assertEqual(False, base._ssl_enabled)

        base = authentication.BaseAuthPlugin(
            'spam', username='ham', password='secret',
            database='test', ssl_enabled=True)
        self.assertEqual('spam', base._auth_data)
        self.assertEqual('ham', base._username)
        self.assertEqual('secret', base._password)
        self.assertEqual('test', base._database)
        self.assertEqual(True, base._ssl_enabled)

    def test_prepare_password(self):
        base = authentication.BaseAuthPlugin('ham')
        self.assertRaises(NotImplementedError, base.prepare_password)

    def test_auth_response(self):
        base = authentication.BaseAuthPlugin('ham')
        self.assertRaises(NotImplementedError, base.auth_response)

        base.requires_ssl = True
        self.assertRaises(mysql.connector.InterfaceError, base.auth_response)


class MySQLNativePasswordAuthPluginTests(tests.MySQLConnectorTests):

    """Tests authentication.MySQLNativePasswordAuthPlugin"""

    def setUp(self):
        self.plugin_class = authentication.MySQLNativePasswordAuthPlugin

    def test_class(self):
        self.assertEqual('mysql_native_password', self.plugin_class.plugin_name)
        self.assertEqual(False, self.plugin_class.requires_ssl)

    def test_prepare_password(self):

        auth_plugin = self.plugin_class(None, password='spam')
        self.assertRaises(mysql.connector.InterfaceError,
                          auth_plugin.prepare_password)

        auth_plugin = self.plugin_class(123456, password='spam')  # too long
        self.assertRaises(mysql.connector.InterfaceError,
                          auth_plugin.prepare_password)

        if PY2:
            empty = ''
            auth_data = (
                '\x2d\x3e\x33\x25\x5b\x7d\x25\x3c\x40\x6b'
                '\x7b\x47\x30\x5b\x57\x25\x51\x48\x55\x53'
                )
            auth_response = (
                '\x73\xb8\xf0\x4b\x3a\xa5\x7c\x46\xb9\x84'
                '\x90\x50\xab\xc0\x3a\x0f\x8f\xad\x51\xa3'
            )
        else:
            empty = b''
            auth_data = (
                b'\x2d\x3e\x33\x25\x5b\x7d\x25\x3c\x40\x6b'
                b'\x7b\x47\x30\x5b\x57\x25\x51\x48\x55\x53'
                )
            auth_response = (
                b'\x73\xb8\xf0\x4b\x3a\xa5\x7c\x46\xb9\x84'
                b'\x90\x50\xab\xc0\x3a\x0f\x8f\xad\x51\xa3'
            )

        auth_plugin = self.plugin_class('\x3f'*20, password=None)
        self.assertEqual(empty, auth_plugin.prepare_password())

        auth_plugin = self.plugin_class(auth_data, password='spam')
        self.assertEqual(auth_response, auth_plugin.prepare_password())
        self.assertEqual(auth_response, auth_plugin.auth_response())


class MySQLClearPasswordAuthPluginTests(tests.MySQLConnectorTests):

    """Tests authentication.MySQLClearPasswordAuthPlugin"""

    def setUp(self):
        self.plugin_class = authentication.MySQLClearPasswordAuthPlugin

    def test_class(self):
        self.assertEqual('mysql_clear_password', self.plugin_class.plugin_name)
        self.assertEqual(True, self.plugin_class.requires_ssl)

    def test_prepare_password(self):
        if PY2:
            exp = 'spam\x00'
        else:
            exp = b'spam\x00'
        auth_plugin = self.plugin_class(None, password='spam', ssl_enabled=True)
        self.assertEqual(exp, auth_plugin.prepare_password())
        self.assertEqual(exp, auth_plugin.auth_response())


class MySQLSHA256PasswordAuthPluginTests(tests.MySQLConnectorTests):

    """Tests authentication.MySQLSHA256PasswordAuthPlugin"""

    def setUp(self):
        self.plugin_class = authentication.MySQLSHA256PasswordAuthPlugin

    def test_class(self):
        self.assertEqual('sha256_password', self.plugin_class.plugin_name)
        self.assertEqual(True, self.plugin_class.requires_ssl)

    def test_prepare_password(self):
        if PY2:
            exp = 'spam\x00'
        else:
            exp = b'spam\x00'
        auth_plugin = self.plugin_class(None, password='spam', ssl_enabled=True)
        self.assertEqual(exp, auth_plugin.prepare_password())
        self.assertEqual(exp, auth_plugin.auth_response())
