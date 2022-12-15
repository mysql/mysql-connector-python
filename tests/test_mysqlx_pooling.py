# -*- coding: utf-8 -*-

# Copyright (c) 2018, 2022, Oracle and/or its affiliates.
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

"""Unittests for mysqlx.client
"""

import os
import platform
import re
import socket
import sys
import unittest

from threading import Thread
from time import sleep

import mysqlx
import tests

from mysql.connector.utils import linux_distribution
from mysql.connector.version import LICENSE, VERSION
from mysqlx.connection import update_timeout_penalties_by_error
from mysqlx.errors import InterfaceError, OperationalError, PoolError, ProgrammingError

from . import check_tls_versions_support, shutdown_mysql_server
from .test_mysqlx_connection import build_uri

CREATE_USER = "CREATE USER '{user}'@'{host}' IDENTIFIED BY '{password}'"
GRANT_USER = "GRANT ALL ON {database}.* TO '{user}'@'{host}'"
DROP_USER = "DROP USER IF EXISTS '{user}'@'{host}'"


def get_current_connections(session):
    """Retrieves open connections using the the given session"""
    # Use Show process list to count the open sesions.
    res = session.sql("SHOW PROCESSLIST").execute()
    rows = res.fetch_all()
    connections = {}
    for row in rows:
        if row.get_string("User") not in connections:
            connections[row.get_string("User")] = [row.get_string("Host")]
        else:
            connections[row.get_string("User")].append(row.get_string("Host"))
    return connections


def wait_for_connections(session, user, exp_connections, tries=5):
    """Waits for current connections to be closed or be the expected"""
    open_connections = -1
    while tries > 0:
        current_conns = get_current_connections(session)
        open_connections = len(current_conns.get(user, []))
        if open_connections <= exp_connections:
            return open_connections
        sleep(1)
        tries -= 1
    return open_connections


@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 14), "XPlugin not compatible")
class MySQLxClientTests(tests.MySQLxTests):
    def setUp(self):
        self.connect_kwargs = tests.get_mysqlx_config()
        self.schema_name = self.connect_kwargs["schema"]
        self.host = self.connect_kwargs["host"]

        try:
            self.session = mysqlx.get_session(self.connect_kwargs)
        except mysqlx.Error as err:
            self.fail("{0}".format(err))

        self.users = [("client_user", "passclient"), ("max_size", "max_pass")]

        for user, password in self.users:
            self.session.sql(DROP_USER.format(user=user, host=self.host)).execute()
            self.session.sql(
                CREATE_USER.format(user=user, host=self.host, password=password)
            ).execute()
            # Grant all to new user on database
            self.session.sql(
                GRANT_USER.format(database=self.schema_name, user=user, host=self.host)
            ).execute()

    def tearDown(self):
        for user, _ in self.users:
            self.session.sql(DROP_USER.format(user=user, host=self.host)).execute()

    def test_get_client(self):
        """Test valid and invalid parameters of get_client()."""
        # Test invalid settings
        invalid_params = [(), (""), ({}), ("1", "2", "3"), ({}, {}, {})]
        for params in invalid_params:
            with self.assertRaises(TypeError, msg="with params {}".format(params)):
                mysqlx.get_client(*params)

        invalid_params = [
            ("", ""),
            ({}, {}),
        ]
        for params in invalid_params:
            with self.assertRaises(InterfaceError, msg="with params {}".format(params)):
                mysqlx.get_client(*params)

        settings = self.connect_kwargs.copy()
        # Raise error for invalid values for pooling option
        invalid_values = [
            False,
            True,
            "False",
            "true",
            "",
            -1,
            0,
            1,
            "1",
            None,
        ]
        for value in invalid_values:
            cnx_options = {"pooling": value}
            with self.assertRaises(InterfaceError, msg="with value {}".format(value)):
                mysqlx.get_client(settings, cnx_options)

        # Raise error for unrecognized settings for pooling option
        invalid_values = [{"max_pool_size": 100}, {"min_pool_size": "10"}]
        for value in invalid_values:
            pooling_dict = value
            cnx_options = {"pooling": pooling_dict}
            with self.assertRaises(InterfaceError):
                mysqlx.get_client(settings, cnx_options)

        # Raise error for invalid values for max_size option
        invalid_values = [
            False,
            True,
            "False",
            "true",
            "",
            -1,
            0,
            "1",
            None,
            {},
            (),
        ]
        for value in invalid_values:
            pooling_dict = {"max_size": value}
            cnx_options = {"pooling": pooling_dict}
            with self.assertRaises(AttributeError, msg="with value {}".format(value)):
                mysqlx.get_client(settings, cnx_options)

        # Test valid values for max_size option
        valid_values = [1, 100]
        for value in valid_values:
            pooling_dict = {"max_size": value}
            cnx_options = {"pooling": pooling_dict}
            client = mysqlx.get_client(settings, cnx_options)
            self.assertEqual(client.max_size, value)
            client.close()

        # Raise error for invalid values for max_idle_time option
        invalid_values = [
            False,
            True,
            "False",
            "true",
            "",
            -1,
            "1",
            None,
            {},
            (),
        ]
        for value in invalid_values:
            pooling_dict = {"max_idle_time": value}
            cnx_options = {"pooling": pooling_dict}
            with self.assertRaises(AttributeError, msg="with value {}".format(value)):
                mysqlx.get_client(settings, cnx_options)

        # Test valid values for max_idle_time option
        valid_values = [0, 1000, 10000]
        exp_values = [0, 1000, 10000]
        for indx_value in range(len(valid_values)):
            value = valid_values[indx_value]
            pooling_dict = {"max_idle_time": value}
            cnx_options = {"pooling": pooling_dict}
            client = mysqlx.get_client(settings, cnx_options)
            self.assertEqual(client.max_idle_time, exp_values[indx_value])
            client.close()

        # Raise error for invalid values for queue_timeout option
        invalid_values = [
            False,
            True,
            "False",
            "true",
            "",
            -1,
            "1",
            None,
            {},
            (),
        ]
        for value in invalid_values:
            pooling_dict = {"queue_timeout": value}
            cnx_options = {"pooling": pooling_dict}
            with self.assertRaises(AttributeError, msg="with value {}".format(value)):
                mysqlx.get_client(settings, cnx_options)

        # Test valid values for queue_timeout option
        valid_values = [0, 1000, 10000]
        exp_values = [0, 1000, 10000]
        for indx_value in range(len(valid_values)):
            value = valid_values[indx_value]
            pooling_dict = {"queue_timeout": value}
            cnx_options = {"pooling": pooling_dict}
            client = mysqlx.get_client(settings, cnx_options)
            self.assertEqual(client.queue_timeout, exp_values[indx_value])
            client.close()

        # Raise error for invalid values for enabled option
        invalid_values = ["False", "true", "", -1, 1.5, "0", "1", 0, 1, None]
        for value in invalid_values:
            pooling_dict = {"enabled": value}
            cnx_options = {"pooling": pooling_dict}
            with self.assertRaises(AttributeError, msg="with value {}".format(value)):
                mysqlx.get_client(settings, cnx_options)

        # Test valid values for enabled option
        valid_values = [True, False]
        for value in valid_values:
            pooling_dict = {"enabled": value}
            cnx_options = {"pooling": pooling_dict}
            client = mysqlx.get_client(settings, cnx_options)
            self.assertEqual(client.pooling_enabled, value)
            client.close()

        # Test default values for pooling option
        for value in valid_values:
            pooling_dict = {}
            cnx_options = {"pooling": pooling_dict}
            client = mysqlx.get_client(settings, cnx_options)
            self.assertEqual(client.max_idle_time, 0)
            self.assertEqual(client.queue_timeout, 0)
            self.assertEqual(client.pooling_enabled, True)
            self.assertEqual(client.max_size, 25)
            client.close()

    @unittest.skipIf(
        tests.MYSQL_EXTERNAL_SERVER,
        "Test not available for external MySQL servers",
    )
    def test_get_session(self):
        """Test get_session() opens new connections."""
        # Auxiliary session to query server
        old_session = mysqlx.get_session(self.connect_kwargs.copy())
        # Setup a client to get sessions from
        settings = self.connect_kwargs.copy()
        pooling_dict = {"enabled": True, "max_idle_time": 3000}
        cnx_options = {"pooling": pooling_dict}
        settings["user"] = self.users[0][0]
        settings["password"] = self.users[0][1]
        settings["host"] = self.host
        settings.pop("socket")
        client = mysqlx.get_client(settings, cnx_options)

        # Get 10 sessions, client will start 10 connections
        total_connections = 10
        sessions = []
        for _ in range(0, total_connections):
            session = client.get_session()
            self.assertTrue(isinstance(session, mysqlx.connection.Session))
            sessions.append(session)

        # Verify the number of connections open in the server
        connections = get_current_connections(old_session)
        self.assertEqual(len(connections[self.users[0][0]]), total_connections)

        # Verify that clossing the session returns the connection
        # to the pool instead of being closed
        sessions[5].close()
        sessions[9].close()
        connections = get_current_connections(old_session)
        self.assertTrue(len(connections[self.users[0][0]]) >= (total_connections - 2))

        if tests.MYSQL_VERSION < (8, 0, 16):
            # Send reset message requires the user to re-authentificate
            # the connection user stays in unauthenticated user
            open_connections = wait_for_connections(
                old_session, "unauthenticated user", 2
            )
            self.assertGreaterEqual(open_connections, 2)
        else:
            open_connections = wait_for_connections(
                old_session, "unauthenticated user", 0
            )
            self.assertEqual(open_connections, 0)

        # Connections must be closed when client.close() is invoked
        # check len(pool) == total_connections
        client.close()
        # Verify the connections on the pool are closed
        open_connections = wait_for_connections(old_session, self.users[0][0], 0)
        self.assertEqual(open_connections, 0)

    @unittest.skipIf(
        tests.MYSQL_EXTERNAL_SERVER,
        "Test not available for external MySQL servers",
    )
    def test_max_pool_size(self):
        """Test exausted pool behavior"""
        # Initial pool limit size
        pool_limit = 5
        # Setup a client to get sessions from.
        settings = self.connect_kwargs.copy()
        pooling_dict = {
            "enabled": True,
            "max_idle_time": 10000,
            "queue_timeout": 3000,
            "max_size": pool_limit,  # initial pool limit
        }
        cnx_options = {"pooling": pooling_dict}
        settings["user"] = self.users[1][0]
        settings["password"] = self.users[1][1]
        settings["host"] = self.host
        settings.pop("socket")
        client = mysqlx.get_client(settings, cnx_options)

        # Get sessions as in pool_limit
        sessions = []
        last_session = None
        for _ in range(0, pool_limit):
            session = client.get_session()
            self.assertTrue(isinstance(session, mysqlx.connection.Session))
            sessions.append(session)
            last_session = session
        self.assertEqual(len(sessions), pool_limit)

        # Verify the server has open sessions as in pool_limit
        connections = get_current_connections(self.session)
        open_connections = connections.get(self.users[1][0], -1)
        self.assertEqual(len(open_connections), pool_limit)

        # verify exception is raised if the pool is exausted
        with self.assertRaises(mysqlx.errors.PoolError):
            client.get_session()

        # Verify that closing the last open session is returned to the pool
        # so then can be retrieved again, which is only possibble after the
        # pool has become available one more time (timeout has been reached).
        last_session.close()
        sleep(3)
        _ = client.get_session()
        client.close()

        # verify all sessions are closed
        for session in sessions:
            with self.assertRaises((mysqlx.errors.OperationalError, InterfaceError)):
                session.sql("SELECT 1").execute()
            session.get_schema(settings["schema"])

    def test_pooling(self):
        """Test pooled Session works as a normal Session."""
        settings = self.connect_kwargs.copy()
        pooling_dict = {"enabled": True}
        cnx_options = {"pooling": pooling_dict}
        client = mysqlx.get_client(settings, cnx_options)

        session = client.get_session()
        session.get_schema(settings["schema"])
        session.close()
        # Verify that clossing the session again does not raise eceptions
        session.close()
        # Verify that trying to use a closed session raises error
        with self.assertRaises((mysqlx.errors.OperationalError, InterfaceError)):
            session.sql("SELECT 1").execute()
        client.close()
        # Verify that clossing the client again does not raise eceptions
        client.close()

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 16), "not reset compatible")
    def test_reset_keeps_same_id(self):
        """Test pooled Session keeps the same session id."""
        settings = self.connect_kwargs.copy()
        pooling_dict = {"enabled": True, "max_size": 1}
        cnx_options = {"pooling": pooling_dict}
        client = mysqlx.get_client(settings, cnx_options)

        session1 = client.get_session()
        conn_id1 = session1.sql("select connection_id()").execute().fetch_all()[0][0]
        session1.close()
        # Verify that new session is has the same id from previous one
        session2 = client.get_session()
        conn_id2 = session2.sql("select connection_id()").execute().fetch_all()[0][0]
        self.assertEqual(conn_id1, conn_id2, "The connection id was not the same")
        session2.close()
        client.close()

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 16), "not reset compatible")
    def test_reset_get_new_connection(self):
        """Test connection is closed by max idle time."""
        # Verify max_idle_time closses an idle session and so for the
        # connection id must increase due to the pool which has created a
        # new connection to the server to return a new session.
        settings = self.connect_kwargs.copy()
        pooling_dict = {
            "enabled": True,
            "max_idle_time": 2000,
            "max_size": 3,
            "queue_timeout": 1000,
        }
        cnx_options = {"pooling": pooling_dict}
        client = mysqlx.get_client(settings, cnx_options)

        # Getting session 0
        session0 = client.get_session()
        conn_id0 = session0.sql("select connection_id()").execute().fetch_all()[0][0]
        # Closing session 0
        session0.close()

        # Getting session 1
        session1 = client.get_session()
        conn_id1 = session1.sql("select connection_id()").execute().fetch_all()[0][0]
        # Closing session 1
        self.assertEqual(conn_id1, conn_id0, "The connection id was not greater")

        session1.close()
        # Verify that new session does not has the same id from previous one
        # goint to sleep 2 sec just above the max idle time
        sleep(4)
        # Getting session 2
        session2 = client.get_session()
        conn_id2 = session2.sql("select connection_id()").execute().fetch_all()[0][0]
        self.assertNotEqual(
            conn_id0, conn_id2, "The connection id was the same from the old"
        )
        self.assertNotEqual(
            conn_id1, conn_id2, "The connection id was the same from the old"
        )

        # Verify pool integrity
        # Open the 4th connection in client life time, when max size is 3
        # Getting the max allowed connections
        # Getting session 3
        session3 = client.get_session()
        conn_id3 = session3.sql("select connection_id()").execute().fetch_all()[0][0]
        self.assertGreater(conn_id3, conn_id2, "The connection id was not greater")

        # Getting session 4
        session4 = client.get_session()
        # Verify exception is raised if the pool is exausted
        with self.assertRaises(mysqlx.errors.PoolError):
            client.get_session()

        # closing all connections
        session2.close()
        session3.close()
        session4.close()
        # No errors should raise by closing it again
        session4.close()
        # No errors should raise from closing client.
        client.close()

    @unittest.skipIf(
        sys.platform == "darwin" and platform.mac_ver()[0].startswith("12"),
        "This test fails due to a bug on macOS 12",
    )
    @unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 40), "TLSv1.1 incompatible")
    def test_get_client_with_tls_version(self):
        # Test None value is returned if no schema name is specified
        settings = self.connect_kwargs.copy()
        settings.pop("schema")
        settings.pop("socket")

        pooling_dict = {
            "enabled": True,
            "max_idle_time": 2000,
            "max_size": 3,
            "queue_timeout": 1000,
        }
        cnx_options = {"pooling": pooling_dict}

        # Dictionary connection settings tests
        # Empty tls_version list
        settings["tls-ciphersuites"] = ["DHE-RSA-AES256-SHA"]
        settings["tls-versions"] = []
        with self.assertRaises(InterfaceError) as context:
            _ = mysqlx.get_client(settings, cnx_options)
        self.assertTrue(
            ("At least one" in context.exception.msg),
            "Unexpected "
            "exception message found: {}"
            "".format(context.exception.msg),
        )

        # Empty tls_ciphersuites list
        settings["tls-ciphersuites"] = []
        settings["tls-versions"] = ["TLSv1.2"]
        with self.assertRaises(InterfaceError) as context:
            _ = mysqlx.get_client(settings, cnx_options)
        self.assertTrue(
            ("No valid cipher" in context.exception.msg),
            "Unexpected exception message found: {}".format(context.exception.msg),
        )

        # URI string connection settings tests
        # Empty tls_version list
        settings["tls-ciphersuites"] = ["DHE-RSA-AES256-SHA"]
        settings["tls-versions"] = []
        uri_settings = build_uri(**settings)
        with self.assertRaises(InterfaceError) as context:
            _ = mysqlx.get_client(uri_settings, cnx_options)
        self.assertTrue(
            ("At least one" in context.exception.msg),
            "Unexpected "
            "exception message found: {}"
            "".format(context.exception.msg),
        )

        # Empty tls_ciphersuites list
        settings["tls-ciphersuites"] = []
        settings["tls-versions"] = ["TLSv1.2"]
        uri_settings = build_uri(**settings)
        with self.assertRaises(InterfaceError) as context:
            _ = mysqlx.get_client(uri_settings, cnx_options)
        self.assertTrue(
            ("No valid cipher" in context.exception.msg),
            "Unexpected exception message found: {}".format(context.exception.msg),
        )

        # Verify InterfaceError exception is raised With invalid TLS version
        settings["tls-ciphersuites"] = ["DHE-RSA-AES256-SHA"]
        settings["tls-versions"] = ["TLSv8"]
        uri_settings = build_uri(**settings)

        with self.assertRaises(InterfaceError) as context:
            _ = mysqlx.get_client(uri_settings, cnx_options)
        self.assertTrue(
            ("not recognized" in context.exception.msg),
            "Unexpected exception message found: {}".format(context.exception.msg),
        )

        # Verify unkown cipher suite case
        settings["tls-ciphersuites"] = ["NOT-KNOWN"]
        settings["tls-versions"] = ["TLSv1.2"]
        uri_settings = build_uri(**settings)
        with self.assertRaises(InterfaceError) as context:
            _ = mysqlx.get_client(uri_settings, cnx_options)

        # Verify unsupported TLSv1.3 version is ignored (connection success)
        # when is not supported, TLSv1.2 is used
        settings["tls-ciphersuites"] = ["DHE-RSA-AES256-SHA256"]
        settings["tls-versions"] = ["TLSv1.3", "TLSv1.2"]
        uri_settings = build_uri(**settings)
        client = mysqlx.get_client(uri_settings, cnx_options)
        session = client.get_session()
        session.close()
        client.close()

        supported_tls = check_tls_versions_support(["TLSv1.2", "TLSv1.1", "TLSv1"])
        if not supported_tls:
            self.fail("No TLS version to test: {}".format(supported_tls))
        if len(supported_tls) > 1:
            # Verify given TLS version is used
            settings["tls-ciphersuites"] = ["DHE-RSA-AES256-SHA"]
            for tes_ver in supported_tls:
                settings["tls-versions"] = [tes_ver]
                uri_settings = build_uri(**settings)
                client = mysqlx.get_client(uri_settings, cnx_options)
                session = client.get_session()
                res = session.sql("SHOW STATUS").execute().fetch_all()
                for row in res:
                    if row.get_string("Variable_name") == "Mysqlx_ssl_version":
                        self.assertEqual(
                            row.get_string("Value"),
                            tes_ver,
                            "Unexpected TLS version found: {} for: {}"
                            "".format(row.get_string("Value"), tes_ver),
                        )
                session.close()
                client.close()

        # Following tests requires TLSv1.2
        if tests.MYSQL_VERSION < (8, 0, 17):
            return

        if "TLSv1.1" in supported_tls:
            # Verify the newest TLS version is used from the given list
            exp_res = ["TLSv1.2", "TLSv1.1", "TLSv1.2"]
            test_vers = [
                ["TLSv1", "TLSv1.2", "TLSv1.1"],
                ["TLSv1", "TLSv1.1"],
                ["TLSv1.2", "TLSv1"],
            ]
            for tes_ver, exp_ver in zip(test_vers, exp_res):
                settings["tls-versions"] = tes_ver
                uri_settings = build_uri(**settings)
                client = mysqlx.get_client(uri_settings, cnx_options)
                session = client.get_session()
                res = session.sql("SHOW STATUS").execute().fetch_all()
                for row in res:
                    if row.get_string("Variable_name") == "Mysqlx_ssl_version":
                        self.assertEqual(
                            row.get_string("Value"),
                            exp_ver,
                            "Unexpected TLS version found: {}"
                            "".format(row.get_string("Value")),
                        )
                session.close()
                client.close()

        # Verify given TLS cipher suite is used
        exp_res = [
            "DHE-RSA-AES256-SHA256",
            "DHE-RSA-AES256-SHA256",
            "DHE-RSA-AES128-GCM-SHA256",
        ]
        test_ciphers = [
            ["TLS_DHE_RSA_WITH_AES_256_CBC_SHA256"],
            ["DHE-RSA-AES256-SHA256"],
            ["TLS_DHE_RSA_WITH_AES_128_GCM_SHA256"],
        ]
        settings["tls-versions"] = "TLSv1.2"
        for test_cipher, exp_ver in zip(test_ciphers, exp_res):
            settings["tls-ciphersuites"] = test_cipher
            uri_settings = build_uri(**settings)
            client = mysqlx.get_client(uri_settings, cnx_options)
            session = client.get_session()
            res = session.sql("SHOW STATUS").execute().fetch_all()
            for row in res:
                if row.get_string("Variable_name") == "Mysqlx_ssl_cipher":
                    self.assertEqual(
                        row.get_string("Value"),
                        exp_ver,
                        "Unexpected TLS version found: {} for: {}"
                        "".format(row.get_string("Value"), test_cipher),
                    )
            session.close()
            client.close()

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
        settings["tls_versions"] = "TLSv1.2"
        uri_settings = build_uri(**settings)
        client = mysqlx.get_client(uri_settings, cnx_options)
        session = client.get_session()
        res = session.sql("SHOW STATUS").execute().fetch_all()
        for row in res:
            if row.get_string("Variable_name") == "Mysqlx_ssl_cipher":
                self.assertIn(
                    row.get_string("Value"),
                    exp_res,
                    "Unexpected TLS version found: {} not in {}"
                    "".format(row.get_string("Value"), exp_res),
                )
        session.close()
        client.close()

    def test_context_manager(self):
        """Test mysqlx.get_client() context manager."""
        settings = self.connect_kwargs.copy()
        pooling_dict = {"enabled": True, "max_size": 5}
        cnx_options = {"pooling": pooling_dict}
        with mysqlx.get_client(settings, cnx_options) as client:
            with client.get_session() as session:
                self.assertIsInstance(session, mysqlx.Session)
                self.assertTrue(session.is_open())
            self.assertFalse(session.is_open())
            # Create one more session
            _ = client.get_session()
        for session in client.sessions:
            self.assertFalse(session.is_open())


@unittest.skipIf(
    tests.MYSQL_EXTERNAL_SERVER,
    "Test not available for external MySQL servers",
)
@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 12), "XPlugin not compatible")
class MySQLxClientPoolingTests(tests.MySQLxTests):
    def setUp(self):
        settings = tests.get_mysqlx_config()
        self.schema_name = settings["schema"]
        self.host = settings["host"]
        self.user = "router_user"
        self.password = "passpool"
        self.hosts = "127.0.0.1", "localhost"

        try:
            self.session = mysqlx.get_session(settings)
        except mysqlx.Error as err:
            self.fail("{0}".format(err))

        for host in self.hosts:
            self.session.sql(DROP_USER.format(user=self.user, host=host)).execute()
        for host in self.hosts:
            self.session.sql(
                CREATE_USER.format(user=self.user, host=host, password=self.password)
            ).execute()
            # Grant all to new user on database
            self.session.sql(
                GRANT_USER.format(
                    database=self.schema_name,
                    user=self.user,
                    host=host,
                    password=self.password,
                )
            ).execute()

    def tearDown(self):
        session = mysqlx.get_session(tests.get_mysqlx_config())
        for host in self.hosts:
            session.sql(DROP_USER.format(user=self.user, host=host)).execute()

    def test_routing(self):
        settings = tests.get_mysqlx_config()
        pooling_dict = {"enabled": True}
        cnx_options = {"pooling": pooling_dict}
        uri = (
            "mysqlx://{user}:{pwd}@[(address=1.0.0.2:{port}, priority=30),"
            " (address=1.0.0.1:{port}, priority=40),"
            " (address=127.0.0.1:{port}, priority=20),"
            " (address=localhost:{port}, priority=50)]"
            "".format(
                user=settings["user"],
                pwd=settings["password"],
                port=settings["port"],
            )
        )
        client = mysqlx.get_client(uri, cnx_options)
        # Getting a session must success, the higher priority will cause to a
        # valid address to be used
        client.get_session()
        client.close()

    def test_pools_are_not_shared(self):
        settings = tests.get_mysqlx_config()
        pooling_dict = {
            "max_size": 1,
            "max_idle_time": 10000,
        }
        cnx_options = {"pooling": pooling_dict}
        uri = (
            "mysqlx://{user}:{pwd}@["
            " (address={host}:{port}, priority=50)]"
            "".format(
                user=self.user,
                pwd=self.password,
                host=settings["host"],
                port=settings["port"],
            )
        )
        client1 = mysqlx.get_client(uri, cnx_options)

        # Getting a session from client1
        session1 = client1.get_session()
        session1.sql("SELECT 1").execute()
        session1.get_schema(settings["schema"])

        # Getting a session from client2
        client2 = mysqlx.get_client(uri, cnx_options)
        # Getting a session must success, the higher priority will cause to a
        # valid address to be used
        session2 = client2.get_session()
        session2.sql("SELECT 2").execute()

        # Verify the server connections
        connections = get_current_connections(self.session)
        open_connections = connections.get(self.user, -1)
        self.assertEqual(len(open_connections), 2)

        # Closing pools in client1 must not close connections in client2
        client1.close()
        with self.assertRaises((mysqlx.errors.OperationalError, InterfaceError)):
            session1.sql("SELECT 1").execute()
        session2.sql("SELECT 2").execute()
        session2.get_schema(settings["schema"])

        client2.close()

    def test_routing_random(self):
        settings = tests.get_mysqlx_config()
        pooling_dict = {"enabled": True, "max_size": 5, "queue_timeout": 1000}
        cnx_options = {"pooling": pooling_dict}
        uri = (
            "mysqlx://{user}:{pwd}@["
            "(address=1.0.0.1:{port}, priority=20),"
            " (address=1.0.0.2:{port}, priority=20),"
            " (address=127.0.0.1:{port}, priority=30),"
            " (address=localhost:{port}, priority=30),"
            " (address=1.0.0.3:{port}, priority=60),"
            " (address=1.0.0.4:{port}, priority=60)]"
            "".format(
                user=settings["user"],
                pwd=settings["password"],
                port=settings["port"],
            )
        )
        client = mysqlx.get_client(uri, cnx_options)
        # Getting a session must success, the higher priority will cause to a
        # valid address to be used
        sessions = []
        for _ in range(10):
            session = client.get_session()
            sessions.append(session)
        client.close()

        # Test routers without priority
        settings = tests.get_mysqlx_config()
        pooling_dict = {"enabled": True, "max_size": 5, "queue_timeout": 1000}
        cnx_options = {"pooling": pooling_dict}
        uri = (
            "mysqlx://{user}:{pwd}@["
            "(address=1.0.0.2:{port}),"
            " (address=1.0.0.1:{port}),"
            " (address=127.0.0.1:{port}),"
            " (address=localhost:{port}),"
            " (address=127.0.0.1:{port}),"
            " (address=localhost:{port})]"
            "".format(
                user=settings["user"],
                pwd=settings["password"],
                port=settings["port"],
            )
        )
        client = mysqlx.get_client(uri, cnx_options)
        # Getting the total of 10 sessions must success
        sessions = []
        for _ in range(10):
            session = client.get_session()
            sessions.append(session)

        # verify error is thrown when the total sum of the pool sizes is reached
        with self.assertRaises(mysqlx.errors.PoolError) as context:
            _ = client.get_session()
        self.assertTrue(
            ("pool max size has been reached" in context.exception.msg),
            "Unexpected exception message found: {}".format(context.exception.msg),
        )
        client.close()

        # Verify "Unable to connect to any of the target hosts" error message
        settings = tests.get_mysqlx_config()
        pooling_dict = {"enabled": True, "max_size": 5, "queue_timeout": 50}
        cnx_options = {"pooling": pooling_dict}
        uri = (
            "mysqlx://{user}:{pwd}@["
            "(address=1.0.0.1:{port}),"
            " (address=1.0.0.2:{port}),"
            " (address=1.0.0.3:{port}),"
            " (address=1.0.0.4:{port}),"
            " (address=1.0.0.5:{port}),"
            " (address=1.0.0.6:{port}),"
            " (address=1.0.0.7:{port}),"
            " (address=1.0.0.8:{port}),"
            " (address=1.0.0.9:{port}),"
            " (address=1.0.0.10:{port})]?connect-timeout=500"
            "".format(
                user=settings["user"],
                pwd=settings["password"] + "$%^",
                port=settings["port"],
            )
        )
        client = mysqlx.get_client(uri, cnx_options)
        with self.assertRaises(mysqlx.errors.PoolError) as context:
            _ = client.get_session()
        self.assertTrue(
            ("Unable to connect to any of the target hosts" in context.exception.msg),
            "Unexpected exception message found: {}".format(context.exception.msg),
        )

        try:
            client.close()
        except:
            # due to the small connect-timeout closing the client may fail.
            pass
        # Verify random order of the pools
        ip_orders = {}
        attempts = 10
        for _ in range(attempts):
            client = mysqlx.get_client(uri, cnx_options)
            with self.assertRaises(mysqlx.errors.PoolError) as context:
                _ = client.get_session()

            re_ip = re.compile("  pool: 1.0.0.(\d+).*")
            order_list = []
            for line in context.exception.msg.splitlines():
                if "pool:" not in line:
                    continue
                match = re_ip.match(line)
                if match:
                    order_list.append(match.group(1))
            # Verify the 10 pools were verified
            self.assertEqual(
                len(order_list),
                10,
                "10 exception messages were expected but found: {}"
                "".format(context.exception.msg),
            )
            key = ",".join(order_list)
            if not key in ip_orders:
                ip_orders[key] = 1
            else:
                ip_orders[key] = ip_orders[key] + 1
            try:
                client.close()
            except:
                # due to the small connect-timeout closing the client may fail.
                pass
        max_repeated = -1
        for ip_order in ip_orders:
            cur = ip_orders[ip_order]
            max_repeated = cur if cur > max_repeated else max_repeated
        # The possiblility of getting 2 times the same order is : ((1/(10!))^2)*9
        # Getting the same number of different orders than attempts ensures no repetitions.
        self.assertEqual(
            len(ip_orders),
            attempts,
            "Expected less repetions found: {}"
            "".format(
                [
                    "ip_order: {} reps: {}".format(ip_order, ip_orders[ip_order])
                    for ip_order in ip_orders
                ]
            ),
        )


@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 14), "XPlugin not compatible")
class MySQLxConnectionPoolingTests(tests.MySQLxTests):
    def setUp(self):
        settings = tests.get_mysqlx_config()
        self.schema_name = settings["schema"]
        self.host = settings["host"]
        self.user = "pool_user"
        self.password = "passpool"
        self.hosts = "127.0.0.1", "localhost"

        try:
            self.session = mysqlx.get_session(settings)
        except mysqlx.Error as err:
            self.fail("{0}".format(err))

        for host in self.hosts:
            self.session.sql(DROP_USER.format(user=self.user, host=host)).execute()

        for host in self.hosts:
            self.session.sql(
                CREATE_USER.format(user=self.user, host=host, password=self.password)
            ).execute()
            # Grant all to new user on database
            self.session.sql(
                GRANT_USER.format(
                    database=self.schema_name,
                    user=self.user,
                    host=host,
                    password=self.password,
                )
            ).execute()

    def tearDown(self):
        for host in self.hosts:
            self.session.sql(DROP_USER.format(user=self.user, host=host)).execute()

    @unittest.skipIf(
        tests.MYSQL_EXTERNAL_SERVER,
        "Test not available for external MySQL servers",
    )
    def test_pools_recycle(self):
        settings = tests.get_mysqlx_config()
        pooling_dict = {
            "max_size": 1,
            "max_idle_time": 3000,
            "queue_timeout": 1000,
        }
        cnx_options = {"pooling": pooling_dict}
        uri = (
            "mysqlx://{user}:{pwd}@["
            " (address={host}:{port}, priority=50)]?connect_timeout=20000"
            "".format(
                user=self.user,
                pwd=self.password,
                host=settings["host"],
                port=settings["port"],
            )
        )
        client = mysqlx.get_client(uri, cnx_options)

        def thread1(client):
            # Getting a session from client
            session1 = client.get_session()
            session1.sql("SELECT 1").execute()
            sleep(2)
            session1.close()

        def thread2(client):
            # Getting a session from client
            session2 = client.get_session()
            session2.sql("SELECT 2").execute()
            sleep(1)
            session2.close()

        worker1 = Thread(target=thread1, args=[client])
        worker1.start()
        worker1.join()
        sleep(0.5)
        worker2 = Thread(target=thread2, args=[client])
        worker2.start()
        worker2.join()
        # Verify the server connections
        connections = get_current_connections(self.session)
        open_connections = connections.get("unauthenticated user", [])
        if tests.MYSQL_VERSION < (8, 0, 16):
            # Send reset message requires the user to re-authentificate
            # the connection user stays in unauthenticated user
            self.assertTrue(len(open_connections) >= 1)
        else:
            self.assertEqual(len(open_connections), 0)

        client.close()


class MySQLxClientConnectionAttributesTests(tests.MySQLConnectorTests):
    def setUp(self):
        self.connect_kwargs = tests.get_mysqlx_config()
        self.schema_name = self.connect_kwargs["schema"]
        try:
            self.client = mysqlx.get_client(self.connect_kwargs, "{}")
            self.session = self.client.get_session()
        except mysqlx.Error as err:
            self.fail("{0}".format(err))

        if os.name == "nt":
            if "64" in platform.architecture()[0]:
                self.platform_arch = "x86_64"
            elif "32" in platform.architecture()[0]:
                self.platform_arch = "i386"
            else:
                self.platform_arch = platform.architecture()
            self.os_ver = "Windows-{}".format(platform.win32_ver()[1])
        else:
            self.platform_arch = platform.machine()
            if platform.system() == "Darwin":
                self.os_ver = "{}-{}".format("macOS", platform.mac_ver()[0])
            else:
                self.os_ver = "-".join(linux_distribution()[0:2])

        license_chunks = LICENSE.split(" ")
        if license_chunks[0] == "GPLv2":
            self.client_license = "GPL-2.0"
        else:
            self.client_license = "Commercial"

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 16), "XPlugin not compatible")
    def test_connection_attributes(self):
        # Validate an error is raised if URL user defined connection attributes
        # through a connection URL name is duplicate
        connection_attributes = {
            "foo": "bar",
            "repeated": "attribute",
            "baz": "zoom",
        }
        uri = build_uri(
            user=self.connect_kwargs["user"],
            password=self.connect_kwargs["password"],
            host=self.connect_kwargs["host"],
            port=self.connect_kwargs["port"],
            schema=self.connect_kwargs["schema"],
            connection_attributes=connection_attributes,
        )
        uri = "{},repeated=duplicate_attribute]".format(uri[0:-1])

        with self.assertRaises(InterfaceError) as context:
            my_client = mysqlx.get_client(uri, "{}")
            _ = my_client.get_session()

        self.assertTrue(
            "Duplicate key " in context.exception.msg,
            "error found: {}".format(context.exception.msg),
        )

        # Test error is raised for attribute name starting with '_'
        connection_attributes = [
            {"foo": "bar", "_baz": "zoom"},
            {"_baz": "zoom"},
            {"foo": "bar", "_baz": "zoom", "puuuuum": "kaplot"},
        ]
        for conn_attr in connection_attributes:
            connect_kwargs = self.connect_kwargs.copy()
            connect_kwargs["connection_attributes"] = conn_attr
            with self.assertRaises(InterfaceError) as context:
                my_client = mysqlx.get_client(connect_kwargs, "{}")
                _ = my_client.get_session()

            self.assertTrue("connection-attributes" in context.exception.msg)
            self.assertTrue("cannot start with '_'" in context.exception.msg)

        # Test error is raised for attribute name size exceeds 32 characters
        connection_attributes = [
            {"foo": "bar", "p{}w".format("o" * 31): "kaplot"},
            {"p{}w".format("o" * 31): "kaplot"},
            {"baz": "zoom", "p{}w".format("o" * 31): "kaplot", "a": "b"},
        ]
        for conn_attr in connection_attributes:
            connect_kwargs = self.connect_kwargs.copy()
            connect_kwargs["connection_attributes"] = conn_attr
            with self.assertRaises(InterfaceError) as context:
                my_client = mysqlx.get_client(connect_kwargs, "{}")
                _ = my_client.get_session()

            self.assertTrue("exceeds 32 characters limit size" in context.exception.msg)

        # Test error is raised for attribute value size exceeds 1024 characters
        connection_attributes = [
            {"foo": "bar", "pum": "kr{}nk".format("u" * 1024)},
            {"pum": "kr{}nk".format("u" * 1024)},
            {"baz": "zoom", "pum": "kr{}nk".format("u" * 1024), "a": "b"},
        ]
        for conn_attr in connection_attributes:
            connect_kwargs = self.connect_kwargs.copy()
            connect_kwargs["connection-attributes"] = conn_attr
            with self.assertRaises(InterfaceError) as context:
                my_client = mysqlx.get_client(connect_kwargs, "{}")
                _ = my_client.get_session()

            self.assertTrue(
                "exceeds 1024 characters limit size" in context.exception.msg
            )

        # Validate the user defined attributes are created in the server
        # Test user defined connection attributes through a connection URL
        connection_attributes = {
            "foo": "bar",
            "baz": "zoom",
            "quash": "",
            "puuuuum": "kaplot",
        }
        uri = build_uri(
            user=self.connect_kwargs["user"],
            password=self.connect_kwargs["password"],
            host=self.connect_kwargs["host"],
            port=self.connect_kwargs["port"],
            schema=self.connect_kwargs["schema"],
            connection_attributes=connection_attributes,
        )

        # Verify user defined session-connection-attributes are in the server
        my_session = mysqlx.get_session(uri)
        row = (
            my_session.sql('SHOW VARIABLES LIKE "pseudo_thread_id"')
            .execute()
            .fetch_all()[0]
        )
        get_attrs = (
            "SELECT ATTR_NAME, ATTR_VALUE FROM "
            "performance_schema.session_account_connect_attrs "
            'where PROCESSLIST_ID = "{}"'
        )
        rows = (
            my_session.sql(get_attrs.format(row.get_string("Value")))
            .execute()
            .fetch_all()
        )
        expected_attrs = connection_attributes.copy()
        expected_attrs.update(
            {
                "_pid": str(os.getpid()),
                "_platform": self.platform_arch,
                "_source_host": socket.gethostname(),
                "_client_name": "mysql-connector-python",
                "_client_license": self.client_license,
                "_client_version": ".".join([str(x) for x in VERSION[0:3]]),
                "_os": self.os_ver,
            }
        )
        # Note that for an empty string "" value the server stores a Null value
        expected_attrs["quash"] = "None"
        for row in rows:
            attr_name, attr_value = (
                row["ATTR_NAME"].decode(),
                row["ATTR_VALUE"].decode() if row["ATTR_VALUE"] else "None",
            )
            self.assertEqual(
                expected_attrs[attr_name],
                attr_value,
                "Attribute {} with value {} differs of {}".format(
                    attr_name, attr_value, expected_attrs[attr_name]
                ),
            )


@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 14), "XPlugin not compatible")
class MySQLxPoolingSessionTests(tests.MySQLxTests):
    def setUp(self):
        self.connect_kwargs = tests.get_mysqlx_config()
        self.session = mysqlx.get_session(self.connect_kwargs)
        self.session.sql("DROP DATABASE IF EXISTS my_test_schema").execute()

    def test_get_default_schema(self):
        pooling_dict = {
            "max_size": 1,
            "max_idle_time": 3000,
            "queue_timeout": 1000,
        }
        # Test None value is returned if no schema name is specified
        settings = self.connect_kwargs.copy()
        settings.pop("schema")
        client = mysqlx.get_client(settings, pooling_dict)
        session = client.get_session()
        schema = session.get_default_schema()
        self.assertIsNone(schema, "None value was expected but got '{}'".format(schema))
        session.close()

        # Test SQL statements not fully qualified, which must not raise error:
        #     mysqlx.errors.OperationalError: No database selected
        self.session.sql("CREATE DATABASE my_test_schema").execute()
        self.session.sql("CREATE TABLE my_test_schema.pets(name VARCHAR(20))").execute()
        settings = self.connect_kwargs.copy()
        settings["schema"] = "my_test_schema"

        client = mysqlx.get_client(settings, pooling_dict)
        session = client.get_session()
        schema = session.get_default_schema()
        self.assertTrue(schema, mysqlx.Schema)
        self.assertEqual(schema.get_name(), "my_test_schema")
        result = session.sql("SHOW TABLES").execute().fetch_all()
        self.assertEqual("pets", result[0][0])
        self.session.sql("DROP DATABASE my_test_schema").execute()
        self.assertFalse(schema.exists_in_database())
        self.assertRaises(mysqlx.ProgrammingError, session.get_default_schema)
        session.close()
        client.close()

        # Test without default schema configured at connect time (passing None)
        settings = self.connect_kwargs.copy()
        settings["schema"] = None
        client = mysqlx.get_client(settings, pooling_dict)
        session = client.get_session()
        schema = session.get_default_schema()
        self.assertIsNone(schema, "None value was expected but got '{}'".format(schema))
        session.close()
        client.close()

        # Test not existing default schema at get_session raise error
        settings = self.connect_kwargs.copy()
        settings["schema"] = "nonexistent"
        client = mysqlx.get_client(settings, pooling_dict)
        self.assertRaises(InterfaceError, client.get_session)
