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

"""Test module for authentication

"""

import getpass
import inspect
import itertools
import logging
import os
import subprocess
import tests
import time
import unittest

import mysql.connector
from mysql.connector import authentication
from mysql.connector.errors import (
    DatabaseError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)

try:
    import oci
except ImportError:
    oci = None

try:
    from mysql.connector.connection_cext import HAVE_CMYSQL, CMySQLConnection
except ImportError:
    # Test without C Extension
    CMySQLConnection = None
    HAVE_CMYSQL = False

LOGGER = logging.getLogger(tests.LOGGER_NAME)

_STANDARD_PLUGINS = (
    "mysql_native_password",
    "mysql_clear_password",
    "sha256_password",
    "authentication_ldap_sasl_client",
    "authentication_kerberos_client",
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
            auth_plugin.auth_continue(
                bytearray("r=/ZT33fXoR/BZT,s=IApa7ZwqQ/ZT,w54".encode()))
        self.assertIn("Incomplete reponse", context.exception.msg,
                      "not the expected error {}".format(context.exception.msg))

        # verify an error is shown if server does not authenticate response.
        with self.assertRaises(InterfaceError) as context:
            auth_plugin.auth_continue(
                bytearray("r=/ZT33fXoR/BZT,s=IApa7ZwqQ/ZT,i=40".encode()))
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

    def test_auth_response256(self):
        # Test unsupported mechanism error message
        auth_data = b'UNKOWN-METHOD'
        auth_plugin = self.plugin_class(auth_data, username="user",
                                        password="spam")
        with self.assertRaises(InterfaceError) as context:
            auth_plugin.auth_response()
        self.assertIn('sasl authentication method "UNKOWN-METHOD"',
                      context.exception.msg, "not the expected error {}"
                      "".format(context.exception.msg))
        self.assertIn("is not supported", context.exception.msg,
                      "not the expected error {}".format(context.exception.msg))
        with self.assertRaises(NotImplementedError) as context:
            auth_plugin.prepare_password()

        # Test SCRAM-SHA-256 mechanism is accepted
        auth_data = b'SCRAM-SHA-256'

        auth_plugin = self.plugin_class(auth_data, username="", password="")

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
            auth_plugin.auth_continue(
                bytearray(b"r=/ZT33fXoR/BZT,s=IApa7ZwqQ/ZT,w54"))
        self.assertIn("Incomplete reponse", context.exception.msg,
                      "not the expected error {}".format(context.exception.msg))

        # verify an error is shown if server does not authenticate response.
        with self.assertRaises(InterfaceError) as context:
            auth_plugin.auth_continue(
                bytearray(b"r=/ZT33fXoR/BZT,s=IApa7ZwqQ/ZT,i=40"))
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


@unittest.skipIf(
    os.getenv("TEST_AUTHENTICATION_KERBEROS") is None,
    "Run tests only if the plugin is configured"
)
@unittest.skipIf(os.name == "nt", "Tests not available for Windows")
@unittest.skipIf(
    tests.MYSQL_VERSION < (8, 0, 24),
    "Authentication with Kerberos not supported"
)
class MySQLKerberosAuthPluginTests(tests.MySQLConnectorTests):
    """Test authentication.MySQLKerberosAuthPlugin.

    Implemented by WL#14440: Support for authentication kerberos.
    """

    user = "test1"
    password = "Testpw1"
    other_user = "test3"
    realm = "MYSQL.LOCAL"
    badrealm = "MYSQL2.LOCAL"
    default_config = {}
    plugin_installed_and_active = False
    skip_reason = None

    @classmethod
    def setUpClass(cls):
        is_plugin_available = tests.is_plugin_available(
            "authentication_kerberos",
            config_vars=(
                (
                    "authentication_kerberos_service_principal",
                    "mysql_service/kerberos_auth_host@MYSQL.LOCAL"
                ),
                (
                    "authentication_kerberos_service_key_tab",
                    os.path.join(
                        os.path.dirname(os.path.realpath(__file__)),
                        "data",
                        "kerberos",
                        "mysql.keytab",
                    ),
                ),
            ),
        )

        if not is_plugin_available:
            cls.skip_reason = "Plugin authentication_kerberos not available"
            return

        if not tests.is_host_reachable("100.103.18.98"):
            cls.skip_reason = "Kerberos server is not reachable"
            return

        config = tests.get_mysql_config()
        cls.default_config = {
            "host": config["host"],
            "port": config["port"],
            "user": cls.user,
            "password": cls.password,
            "auth_plugin": "authentication_kerberos_client",
        }

        with mysql.connector.connection.MySQLConnection(**config) as cnx:
            cnx.cmd_query(f"DROP USER IF EXISTS'{cls.user}'")
            cnx.cmd_query(
                f"""
                CREATE USER '{cls.user}'
                IDENTIFIED WITH authentication_kerberos BY '{cls.realm}'
                """
            )
            cnx.cmd_query(f"GRANT ALL ON *.* to '{cls.user}'")
            cnx.cmd_query("FLUSH PRIVILEGES")

    @classmethod
    def tearDownClass(cls):
        config = tests.get_mysql_config()
        with mysql.connector.connection.MySQLConnection(**config) as cnx:
            cnx.cmd_query(f"DROP USER IF EXISTS '{cls.user}'")
            cnx.cmd_query("FLUSH PRIVILEGES")

    def setUp(self):
        self.plugin_class = authentication.MySQLKerberosAuthPlugin
        if self.skip_reason is not None:
            self.skipTest(self.skip_reason)

    def _get_kerberos_tgt(
        self, user=None, password=None, realm=None, expired=False
    ):
        """Obtain and cache Kerberos ticket-granting ticket.

        Call `kinit` with a specified user and password for obtaining and
        caching Kerberos ticket-granting ticket.
        """
        cmd = ["kinit", "{}@{}".format(user or self.user, realm or self.realm)]
        if expired:
            cmd.extend(["-l", "0:0:6"])

        if password is None:
            password = self.password

        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdout=subprocess.DEVNULL
        )
        _, err = proc.communicate(password.encode("utf-8"))

        if err:
            raise InterfaceError(
                "Failing obtaining Kerberos ticket-granting ticket: {}"
                "".format(err.decode("utf-8"))
            )

        if expired:
            time.sleep(8)

    def _test_connection(self, conn_class, config, fail=False):
        """Test a MySQL connection.

        Try to connect to a MySQL server using a specified connection class
        and config.
        """
        if fail:
            self.assertRaises(
                (
                    DatabaseError,
                    InterfaceError,
                    OperationalError,
                    ProgrammingError,
                ),
                conn_class,
                **config
            )
            return

        with conn_class(**config) as cnx:
            self.assertTrue(cnx.is_connected)
            with cnx.cursor() as cur:
                cur.execute("SELECT @@version")
                res = cur.fetchone()
                self.assertIsNotNone(res[0])

    def _test_with_tgt_cache(
        self,
        conn_class,
        config,
        user=None,
        password=None,
        realm=None,
        expired=False,
        fail=False,
    ):
        """Test with cached valid TGT."""
        # Destroy Kerberos tickets
        subprocess.run(["kdestroy"], check=True, stderr=subprocess.DEVNULL)

        # Obtain and cache Kerberos ticket-granting ticket
        self._get_kerberos_tgt(
            user=user, password=password, realm=realm, expired=expired
        )

        # Test connection
        self._test_connection(conn_class, config, fail=fail)

        # Destroy Kerberos tickets
        subprocess.run(["kdestroy"], check=True, stderr=subprocess.DEVNULL)

    def _test_with_st_cache(self, conn_class, config, fail=False):
        """Test with cached valid ST."""
        # Destroy Kerberos tickets
        subprocess.run(["kdestroy"], check=True, stderr=subprocess.DEVNULL)

        # Obtain and cache Kerberos ticket-granting ticket
        self._get_kerberos_tgt()

        # Obtain the service ticket
        cnx = conn_class(**self.default_config)
        cnx.close()

        # Test connection
        self._test_connection(conn_class, config, fail=fail)

        # Destroy Kerberos tickets
        subprocess.run(["kdestroy"], check=True, stderr=subprocess.DEVNULL)

    def test_class(self):
        self.assertEqual(
            "authentication_kerberos_client",
            self.plugin_class.plugin_name
        )
        self.assertEqual(False, self.plugin_class.requires_ssl)

    # Test with TGT in the cache

    @tests.foreach_cnx()
    def test_tgt_cache(self):
        """Test with cached valid TGT."""
        config = self.default_config.copy()
        self._test_with_tgt_cache(self.cnx.__class__, config, fail=False)

    @tests.foreach_cnx()
    def test_tgt_cache_wrongpassword(self):
        """Test with cached valid TGT with a wrong password."""
        config = self.default_config.copy()
        config["password"] = "wrong_password"
        self._test_with_tgt_cache(self.cnx.__class__, config, fail=False)

    @tests.foreach_cnx()
    def test_tgt_cache_nouser(self):
        """Test with cached valid TGT with no user."""
        config = self.default_config.copy()
        del config["user"]
        self._test_with_tgt_cache(self.cnx.__class__, config, fail=False)

    @tests.foreach_cnx()
    def test_tgt_cache_nouser_wrongpassword(self):
        """Test with cached valid TGT with no user and a wrong password."""
        config = self.default_config.copy()
        config["password"] = "wrong_password"
        del config["user"]
        self._test_with_tgt_cache(self.cnx.__class__, config, fail=False)

    @tests.foreach_cnx()
    def test_tgt_cache_nopassword(self):
        """Test with cached valid TGT with no password."""
        config = self.default_config.copy()
        del config["password"]
        self._test_with_tgt_cache(self.cnx.__class__, config, fail=False)

    @tests.foreach_cnx()
    def test_tgt_cache_nouser_nopassword(self):
        """Test with cached valid TGT with no user and no password."""
        config = self.default_config.copy()
        del config["user"]
        del config["password"]
        self._test_with_tgt_cache(self.cnx.__class__, config, fail=False)

    # Tests with ST in the cache

    @tests.foreach_cnx()
    def test_st_cache(self):
        """Test with cached valid ST."""
        config = self.default_config.copy()
        self._test_with_st_cache(self.cnx.__class__, config, fail=False)

    @tests.foreach_cnx()
    def test_st_cache_wrongpassword(self):
        """Test with cached valid ST with a wrong password."""
        config = self.default_config.copy()
        config["password"] = "wrong_password"
        self._test_with_st_cache(self.cnx.__class__, config, fail=False)

    @tests.foreach_cnx()
    def test_st_cache_nouser(self):
        """Test with cached valid ST with no user."""
        config = self.default_config.copy()
        del config["user"]
        self._test_with_st_cache(self.cnx.__class__, config, fail=False)

    @tests.foreach_cnx()
    def test_st_cache_nouser_wrongpassword(self):
        """Test with cached valid ST with no user and a wrong password."""
        config = self.default_config.copy()
        config["password"] = "wrong_password"
        del config["user"]
        self._test_with_st_cache(self.cnx.__class__, config, fail=False)

    @tests.foreach_cnx()
    def test_st_cache_nopassword(self):
        """Test with cached valid ST with no password."""
        config = self.default_config.copy()
        del config["password"]
        self._test_with_st_cache(self.cnx.__class__, config, fail=False)

    @tests.foreach_cnx()
    def test_st_cache_nouser_nopassword(self):
        """Test with cached valid ST with no user and no password."""
        config = self.default_config.copy()
        del config["user"]
        del config["password"]
        self._test_with_st_cache(self.cnx.__class__, config, fail=False)

    # Tests with cache is present but contains expired TGT

    @tests.foreach_cnx()
    def test_tgt_expired(self):
        """Test with cache expired."""
        config = self.default_config.copy()
        self._test_with_tgt_cache(
            self.cnx.__class__,
            config,
            expired=True,
            fail=False,
        )

    @tests.foreach_cnx()
    def test_tgt_expired_wrongpassword(self):
        """Test with cache expired with a wrong password."""
        config = self.default_config.copy()
        config["password"] = "wrong_password"
        self._test_with_tgt_cache(
            self.cnx.__class__,
            config,
            expired=True,
            fail=True,
        )

    @unittest.skipIf(not HAVE_CMYSQL, "C Extension not available")
    @tests.foreach_cnx(CMySQLConnection)
    def test_tgt_expired_nouser(self):
        """Test with cache expired with no user."""
        config = self.default_config.copy()
        del config["user"]
        self._test_with_tgt_cache(
            self.cnx.__class__,
            config,
            expired=True,
            fail=False,
        )

    @tests.foreach_cnx()
    def test_tgt_expired_nouser_wrongpassword(self):
        """Test with cache expired with no user and a wrong password."""
        config = self.default_config.copy()
        config["password"] = "wrong_password"
        del config["user"]
        self._test_with_tgt_cache(
            self.cnx.__class__,
            config,
            expired=True,
            fail=True,
        )

    @tests.foreach_cnx()
    def test_tgt_expired_nopassword(self):
        """Test with cache expired with no password."""
        config = self.default_config.copy()
        del config["password"]
        self._test_with_tgt_cache(
            self.cnx.__class__,
            config,
            expired=True,
            fail=True,
        )

    @tests.foreach_cnx()
    def test_tgt_expired_nouser_nopassword(self):
        """Test with cache expired with no user and no password."""
        config = self.default_config.copy()
        del config["user"]
        del config["password"]
        self._test_with_tgt_cache(
            self.cnx.__class__,
            config,
            expired=True,
            fail=True,
        )

    # Tests with TGT in the cache for a different UPN

    @tests.foreach_cnx()
    def test_tgt_badupn(self):
        """Test with cached valid TGT with a bad UPN."""
        config = self.default_config.copy()
        self._test_with_tgt_cache(
            self.cnx.__class__,
            config,
            user=self.other_user,
            fail=False,
        )

    @tests.foreach_cnx()
    def test_tgt_badupn_wrongpassword(self):
        """Test with cached valid TGT with a wrong password with a bad UPN."""
        config = self.default_config.copy()
        config["password"] = "wrong_password"
        self._test_with_tgt_cache(
            self.cnx.__class__,
            config,
            user=self.other_user,
            fail=True,
        )

    @tests.foreach_cnx()
    def test_tgt_badupn_nouser(self):
        """Test with cached valid TGT with no user with a bad UPN."""
        config = self.default_config.copy()
        del config["user"]
        self._test_with_tgt_cache(
            self.cnx.__class__,
            config,
            user=self.other_user,
            fail=True,
        )

    @tests.foreach_cnx()
    def test_tgt_badupn_nouser_wrongpassword(self):
        """Test with cached valid TGT with no user and a wrong password and
        bad UPN."""
        config = self.default_config.copy()
        config["password"] = "wrong_password"
        del config["user"]
        self._test_with_tgt_cache(
            self.cnx.__class__,
            config,
            user=self.other_user,
            fail=True,
        )

    @tests.foreach_cnx()
    def test_tgt_badupn_nopassword(self):
        """Test with cached valid TGT with no password and bad UPN."""
        config = self.default_config.copy()
        del config["password"]
        self._test_with_tgt_cache(
            self.cnx.__class__,
            config,
            user=self.other_user,
            fail=True,
        )

    @tests.foreach_cnx()
    def test_tgt_badupn_nouser_nopassword(self):
        """Test with cached valid TGT with no user and no password."""
        config = self.default_config.copy()
        del config["user"]
        del config["password"]
        self._test_with_tgt_cache(
            self.cnx.__class__,
            config,
            user=self.other_user,
            fail=True,
        )

    # Tests with TGT in the cache with for a different realm

    @tests.foreach_cnx()
    def test_tgt_badrealm(self):
        """Test with cached valid TGT with a bad realm."""
        config = self.default_config.copy()
        self._test_with_tgt_cache(
            self.cnx.__class__,
            config,
            user=self.user,
            password=self.password,
            realm=self.badrealm,
            fail=False,
        )

    @tests.foreach_cnx()
    def test_tgt_badrealm_wrongpassword(self):
        """Test with cached valid TGT with a wrong password with a bad realm."""
        config = self.default_config.copy()
        config["password"] = "wrong_password"
        self._test_with_tgt_cache(
            self.cnx.__class__,
            config,
            user=self.user,
            password=self.password,
            realm=self.badrealm,
            fail=True,
        )

    @tests.foreach_cnx()
    def test_tgt_badrealm_nouser(self):
        """Test with cached valid TGT with no user with a bad realm."""
        config = self.default_config.copy()
        del config["user"]
        self._test_with_tgt_cache(
            self.cnx.__class__,
            config,
            user=self.user,
            password=self.password,
            realm=self.badrealm,
            fail=False,
        )

    @tests.foreach_cnx()
    def test_tgt_badrealm_nouser_wrongpassword(self):
        """Test with cached valid TGT with no user and a wrong password and
        bad realm."""
        config = self.default_config.copy()
        config["password"] = "wrong_password"
        del config["user"]
        self._test_with_tgt_cache(
            self.cnx.__class__,
            config,
            user=self.user,
            password=self.password,
            realm=self.badrealm,
            fail=True,
        )

    @tests.foreach_cnx()
    def test_tgt_badrealm_nopassword(self):
        """Test with cached valid TGT with no password and bad realm."""
        config = self.default_config.copy()
        del config["password"]
        self._test_with_tgt_cache(
            self.cnx.__class__,
            config,
            user=self.user,
            password=self.password,
            realm=self.badrealm,
            fail=True,
        )

    @tests.foreach_cnx()
    def test_tgt_badrealm_nouser_nopassword(self):
        """Test with cached valid TGT with no user and no password."""
        config = self.default_config.copy()
        del config["user"]
        del config["password"]
        self._test_with_tgt_cache(
            self.cnx.__class__,
            config,
            user=self.user,
            password=self.password,
            realm=self.badrealm,
            fail=True,
        )

    @unittest.skipIf(
        getpass.getuser() != "test1",
        "Test only available for system user 'test1'"
    )
    @tests.foreach_cnx()
    def test_nocache_nouser(self):
        """Test with no valid TGT cache, no user and with password."""
        config = self.default_config.copy()
        del config["user"]

        # Destroy Kerberos tickets
        subprocess.run(["kdestroy"], check=True, stderr=subprocess.DEVNULL)

        # Test connection
        self._test_connection(self.cnx.__class__, config, fail=False)


@unittest.skipIf(
    tests.MYSQL_VERSION < (8, 0, 28),
    "Multi Factor Authentication not supported"
)
class MySQLMultiFactorAuthenticationTests(tests.MySQLConnectorTests):
    """Test Multi Factor Authentication.

    Implemented by WL#14667: Support for MFA authentication.

    The initialization of the passwords permutations creates a tuple with
    two values:
       - The first is a tuple with the passwords to be set:
         + True: Valid password provided
         + False: Invalid password provived
         + None: No password provided
       - The second is the expected connection result:
         + True: Connection established
         + False: Connection denied
    """

    user_1f = "user_1f"
    user_2f = "user_2f"
    user_3f = "user_3f"
    password1 = "Testpw1"
    password2 = "Testpw2"
    password3 = "Testpw3"
    base_config = {}
    skip_reason = None

    @classmethod
    def setUpClass(cls):
        config = tests.get_mysql_config()
        cls.base_config = {
            "host": config["host"],
            "port": config["port"],
            "auth_plugin": "mysql_clear_password",
        }
        plugin_ext = "dll" if os.name == "nt" else "so"
        with mysql.connector.connection.MySQLConnection(**config) as cnx:
            try:
                cnx.cmd_query("UNINSTALL PLUGIN cleartext_plugin_server")
            except ProgrammingError:
                pass
            try:
                cnx.cmd_query(
                    f"""
                    INSTALL PLUGIN cleartext_plugin_server
                    SONAME 'auth_test_plugin.{plugin_ext}'
                    """
                )
            except DatabaseError:
                cls.skip_reason = (
                    "Plugin cleartext_plugin_server not available"
                )
                return
            cnx.cmd_query(f"DROP USER IF EXISTS '{cls.user_1f}'")
            cnx.cmd_query(f"DROP USER IF EXISTS '{cls.user_2f}'")
            cnx.cmd_query(f"DROP USER IF EXISTS '{cls.user_3f}'")
            cnx.cmd_query(
                f"""
                CREATE USER '{cls.user_1f}'
                IDENTIFIED WITH cleartext_plugin_server BY '{cls.password1}'
                """
            )
            try:
                cnx.cmd_query(
                    f"""
                    CREATE USER '{cls.user_2f}'
                    IDENTIFIED WITH cleartext_plugin_server BY '{cls.password1}'
                    AND
                    IDENTIFIED WITH cleartext_plugin_server BY '{cls.password2}'
                    """
                )
                cnx.cmd_query(
                    f"""
                    CREATE USER '{cls.user_3f}'
                    IDENTIFIED WITH cleartext_plugin_server BY '{cls.password1}'
                    AND
                    IDENTIFIED WITH cleartext_plugin_server BY '{cls.password2}'
                    AND
                    IDENTIFIED WITH cleartext_plugin_server BY '{cls.password3}'
                    """
                )
            except ProgrammingError:
                cls.skip_reason = "Multi Factor Authentication not supported"
                return

    @classmethod
    def tearDownClass(cls):
        config = tests.get_mysql_config()
        with mysql.connector.connection.MySQLConnection(**config) as cnx:
            cnx.cmd_query(f"DROP USER IF EXISTS '{cls.user_1f}'")
            cnx.cmd_query(f"DROP USER IF EXISTS '{cls.user_2f}'")
            cnx.cmd_query(f"DROP USER IF EXISTS '{cls.user_3f}'")
            try:
                cnx.cmd_query("UNINSTALL PLUGIN cleartext_plugin_server")
            except ProgrammingError:
                pass

    def setUp(self):
        if self.skip_reason is not None:
            self.skipTest(self.skip_reason)

    def _test_connection(self, cls, permutations, user):
        """Helper method for testing connection with MFA."""
        LOGGER.debug("Running %d permutations...", len(permutations))
        for perm, valid in permutations:
            config = self.base_config.copy()
            config["user"] = user
            if perm[0] is not None:
                config["password"] = self.password1 if perm[0] else "invalid"
            if perm[1] is not None:
                config["password1"] = self.password1 if perm[1] else "invalid"
            if perm[2] is not None:
                config["password2"] = self.password2 if perm[2] else "invalid"
            if perm[3] is not None:
                config["password3"] = self.password3 if perm[3] else "invalid"
            LOGGER.debug(
                "Test connection with user '%s' using '%s'. (Expected %s)",
                user, perm, "SUCCESS" if valid else "FAIL",
            )
            if valid:
                with cls(**config) as cnx:
                    self.assertTrue(cnx.is_connected())
                    cnx.cmd_query("SELECT @@version")
                    res = cnx.get_rows()
                    self.assertIsNotNone(res[0][0][0])
            else:
                self.assertRaises(ProgrammingError, cls, **config)

    def _test_change_user(self, cls, permutations, user):
        """Helper method for testing cnx.cmd_change_user() with MFA."""
        LOGGER.debug("Running %d permutations...", len(permutations))
        for perm, valid in permutations:
            # Connect with 'user_1f'
            config = self.base_config.copy()
            config["user"] = self.user_1f
            config["password"] = self.password1
            with cls(**config) as cnx:
                cnx.cmd_query("SELECT @@version")
                res = cnx.get_rows()
                self.assertIsNotNone(res[0][0][0])
                # Create kwargs options for the provided user
                kwargs = {"username": user}
                if perm[0] is not None:
                    kwargs["password"] = self.password1 if perm[0] else "invalid"
                if perm[1] is not None:
                    kwargs["password1"] = self.password1 if perm[1] else "invalid"
                if perm[2] is not None:
                    kwargs["password2"] = self.password2 if perm[2] else "invalid"
                if perm[3] is not None:
                    kwargs["password3"] = self.password3 if perm[3] else "invalid"
                LOGGER.debug(
                    "Test change user to '%s' using '%s'. (Expected %s)",
                    user, perm, "SUCCESS" if valid else "FAIL",
                )
                # Change user to the provided user
                if valid:
                    cnx.cmd_change_user(**kwargs)
                    cnx.cmd_query("SELECT @@version")
                    res = cnx.get_rows()
                    self.assertIsNotNone(res[0][0][0])
                else:
                    self.assertRaises(
                        (DatabaseError, OperationalError, ProgrammingError),
                        cnx.cmd_change_user,
                        **kwargs,
                    )

    @tests.foreach_cnx()
    def test_user_1f(self):
        """Test connection 'user_1f' password permutations."""
        permutations = []
        for perm in itertools.product([True, False, None], repeat=4):
            permutations.append(
                (perm, perm[1] or (perm[0] and perm[1] is None))
            )
        self._test_connection(self.cnx.__class__, permutations, self.user_1f)

    @tests.foreach_cnx()
    def test_user_2f(self):
        """Test connection and change user 'user_2f' password permutations."""
        permutations = []
        for perm in itertools.product([True, False, None], repeat=4):
            permutations.append(
                (
                    perm,
                    perm[2] and
                    (
                        (perm[0] and perm[1] is not False) or perm[1]
                    ),
                )
            )
        self._test_connection(self.cnx.__class__, permutations, self.user_2f)
        # The cmd_change_user() tests are temporarily disabled due to server BUG#33110621
        # self._test_change_user(self.cnx.__class__, permutations, self.user_2f)

    @tests.foreach_cnx()
    def test_user_3f(self):
        """Test connection and change user 'user_3f' password permutations."""
        permutations = []
        for perm in itertools.product([True, False, None], repeat=4):
            permutations.append(
                (
                    perm,
                    perm[2] and perm[3] and
                    (
                        (perm[0] and perm[1] is not False) or perm[1]
                    ),
                )
            )
        self._test_connection(self.cnx.__class__, permutations, self.user_3f)
        # The cmd_change_user() tests are temporarily disabled due to server BUG#33110621
        # self._test_change_user(self.cnx.__class__, permutations, self.user_2f)


@unittest.skipIf(
    tests.MYSQL_VERSION < (8, 0, 27),
    "Authentication with OCI IAM not supported"
)
class MySQLIAMAuthPluginTests(tests.MySQLConnectorTests):
    """Test authentication.MySQLKerberosAuthPlugin.

    Implemented by WL#14710: Support for OCI IAM authentication.
    """

    @unittest.skipIf(oci, "Not testing with OCI is installed")
    def test_OCI_SDK_not_installed_error(self):
        # verify an error is raised due to missing OCI SDK
        plugin_name = "authentication_oci_client"
        auth_plugin_class = authentication.get_auth_plugin(plugin_name)
        auth_plugin = auth_plugin_class("spam_auth_data")
        self.assertIsInstance(auth_plugin, authentication.MySQL_OCI_AuthPlugin)
        self.assertRaises(
            ProgrammingError,
            auth_plugin.auth_response
        )


@unittest.skipIf(
    tests.MYSQL_VERSION < (8, 0, 29),
    "Authentication with FIDO not supported"
)
class MySQLFIDOAuthPluginTests(tests.MySQLConnectorTests):
    """Test authentication.MySQLFIDOAuthPlugin.

    Implemented by WL#14860: Support FIDO authentication (c-ext)
    """

    @tests.foreach_cnx(CMySQLConnection)
    def test_invalid_fido_callback(self):
        """Test invalid 'fido_callback' option."""
        def my_callback():
            ...

        test_cases = (
            "abc",  # No callable named 'abc'
            "abc.abc",  # module 'abc' has no attribute 'abc'
            my_callback,  # 1 positional argument required
        )
        config = tests.get_mysql_config()
        config["auth_plugin"] = "authentication_fido_client"
        for case in test_cases:
            config["fido_callback"] = case
            self.assertRaises(ProgrammingError, self.cnx.__class__, **config)
