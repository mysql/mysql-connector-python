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
from mysql.connector.errors import InterfaceError

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


class MySQLLdapSaslPasswordAuthPluginTests(tests.MySQLConnectorTests):
    """Tests authentication.MySQLLdapSaslPasswordAuthPlugin"""

    def setUp(self):
        self.plugin_class = authentication.MySQLLdapSaslPasswordAuthPlugin

    def test_class(self):
        self.assertEqual("authentication_ldap_sasl_client",
                         self.plugin_class.plugin_name)
        self.assertEqual(False, self.plugin_class.requires_ssl)

    def test_auth_response(self):
        # Test unsupported mechanism error message
        auth_data = b'UNKOWN-METHOD'
        auth_plugin = self.plugin_class(auth_data, username="user",
                                        password="spam")
        with self.assertRaises(InterfaceError) as context:
            auth_plugin.auth_response()
        self.assertIn("sasl authentication method", context.exception.msg,
                      "not the expected error {}".format(context.exception.msg))
        self.assertIn("is not supported", context.exception.msg,
                      "not the expected error {}".format(context.exception.msg))
        with self.assertRaises(NotImplementedError) as context:
            auth_plugin.prepare_password()

        # Test SCRAM-SHA-1 mechanism is accepted
        auth_data = b'SCRAM-SHA-1'

        auth_plugin = self.plugin_class(auth_data, username="",
                                        password="")

        # Verify the format of the first message from client.
        exp = b'n,a=,n=,r='
        client_first_nsg = auth_plugin.auth_response()
        self.assertTrue(client_first_nsg.startswith(exp),
                        "got header: {}".format(auth_plugin.auth_response()))

        auth_plugin = self.plugin_class(auth_data, username="user",
                                        password="spam")

        # Verify the length of the client's nonce in r=
        cnonce = client_first_nsg[(len(b'n,a=,n=,r=')):]
        r_len = len(cnonce)
        self.assertEqual(32, r_len, "Unexpected legth {}".format(len(cnonce)))

        # Verify the format of the first message from client.
        exp = b'n,a=user,n=user,r='
        client_first_nsg = auth_plugin.auth_response()
        self.assertTrue(client_first_nsg.startswith(exp),
                        "got header: {}".format(auth_plugin.auth_response()))

        # Verify the length of the client's nonce in r=
        cnonce = client_first_nsg[(len(exp)):]
        r_len = len(cnonce)
        self.assertEqual(32, r_len, "Unexpected cnonce legth {}, response {}"
                         "".format(len(cnonce), client_first_nsg))

        # Verify that a user name that requires character mapping is mapped
        auth_plugin = self.plugin_class(auth_data, username=u"u\u1680ser",
                                        password="spam")
        exp = b'n,a=u ser,n=u ser,r='
        client_first_nsg = auth_plugin.auth_response()
        self.assertTrue(client_first_nsg.startswith(exp),
                        "got header: {}".format(auth_plugin.auth_response()))

        # Verify the length of the client's nonce in r=
        cnonce = client_first_nsg[(len(exp)):]
        r_len = len(cnonce)
        self.assertEqual(32, r_len, "Unexpected legth {}".format(len(cnonce)))

        bad_responses = [None, "", "v=5H6b+IApa7ZwqQ/ZT33fXoR/BTM=", b"", 123]
        for bad_res in bad_responses:
            # verify an error is shown if server response is not as expected.
            with self.assertRaises(InterfaceError) as context:
                auth_plugin.auth_continue(bad_res)
            self.assertIn("Unexpected server message", context.exception.msg,
                          "not the expected: {}".format(context.exception.msg))

        # verify an error is shown if server response is not well formated.
        with self.assertRaises(InterfaceError) as context:
            auth_plugin.auth_continue("r=/ZT33fXoR/BZT,s=IApa7ZwqQ/ZT,w54")
        self.assertIn("Incomplete reponse", context.exception.msg,
                      "not the expected error {}".format(context.exception.msg))

        # verify an error is shown if server does not authenticate response.
        with self.assertRaises(InterfaceError) as context:
            auth_plugin.auth_continue("r=/ZT33fXoR/BZT,s=IApa7ZwqQ/ZT,i=40")
        self.assertIn("Unable to authenticate resp", context.exception.msg,
                      "not the expected error {}".format(context.exception.msg))

        bad_proofs = [None, "", b"5H6b+IApa7ZwqQ/ZT33fXoR/BTM=", b"", 123]
        for bad_proof in bad_proofs:
            # verify an error is shown if server proof is not well formated.
            with self.assertRaises(InterfaceError) as context:
                auth_plugin.auth_finalize(bad_proof)
            self.assertIn("proof is not well formated.", context.exception.msg,
                          "not the expected: {}".format(context.exception.msg))

        # verify an error is shown it the server can not prove it self.
        with self.assertRaises(InterfaceError) as context:
            auth_plugin.auth_finalize(
                bytearray(b"v=5H6b+IApa7ZwqQ/ZT33fXoR/BTM="))
        self.assertIn("Unable to proof server identity", context.exception.msg,
                      "not the expected error {}".format(context.exception.msg))
