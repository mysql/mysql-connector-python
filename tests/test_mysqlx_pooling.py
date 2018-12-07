# -*- coding: utf-8 -*-

# Copyright (c) 2018, 2019, Oracle and/or its affiliates. All rights reserved.
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

import unittest
import tests
import mysqlx
import os
import platform
import socket

from mysqlx.errors import InterfaceError, ProgrammingError
from mysql.connector.version import VERSION, LICENSE
from .test_mysqlx_connection import build_uri
from time import sleep
from threading import Thread

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
        if row.get_string('User') not in connections:
            connections[row.get_string('User')] = [row.get_string('Host')]
        else:
            connections[row.get_string('User')].append(row.get_string('Host'))
    return connections


@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 12), "XPlugin not compatible")
class MySQLxClientTests(tests.MySQLxTests):
    def setUp(self):
        self.connect_kwargs = tests.get_mysqlx_config()
        self.schema_name = self.connect_kwargs["schema"]
        self.host = self.connect_kwargs['host']

        try:
            self.session = mysqlx.get_session(self.connect_kwargs)
        except mysqlx.Error as err:
            self.fail("{0}".format(err))

        self.users = [("client_user", "passclient"), ("max_size", "max_pass")]

        for user, password in self.users:
            self.session.sql(DROP_USER.format(user=user, host=self.host)) \
                        .execute()
            self.session.sql(CREATE_USER.format(user=user, host=self.host,
                                                password=password)).execute()
            # Grant all to new user on database
            self.session.sql(GRANT_USER.format(database=self.schema_name,
                                               user=user, host=self.host)) \
                        .execute()

    def tearDown(self):
        for user, _ in self.users:
            self.session.sql(DROP_USER.format(user=user, host=self.host)) \
                        .execute()

    def test_get_client(self):
        """Test valid and invalid parameters of get_client()."""
        # Test invalid settings
        invalid_params = [(), (""), ({}), ("1", "2", "3"), ({}, {}, {})]
        for params in invalid_params:
            with self.assertRaises(TypeError,
                                   msg="with params {}".format(params)):
                mysqlx.get_client(*params)

        invalid_params = [("", ""), ({}, {}),]
        for params in invalid_params:
            with self.assertRaises(InterfaceError,
                                   msg="with params {}".format(params)):
                mysqlx.get_client(*params)

        settings = self.connect_kwargs.copy()
        # Raise error for invalid values for pooling option
        invalid_values = [False, True, "False", "true", "", -1, 0, 1, "1", None]
        for value in invalid_values:
            cnx_options = {"pooling": value}
            with self.assertRaises(InterfaceError,
                                   msg="with value {}".format(value)):
                mysqlx.get_client(settings, cnx_options)

        # Raise error for unrecognized settings for pooling option
        invalid_values = [{"max_pool_size": 100}, {"min_pool_size": "10"}]
        for value in invalid_values:
            pooling_dict = value
            cnx_options = {"pooling": pooling_dict}
            with self.assertRaises(InterfaceError):
                mysqlx.get_client(settings, cnx_options)

        # Raise error for invalid values for max_size option
        invalid_values = [False, True, "False", "true", "", -1, 0, "1", None,
                          {}, ()]
        for value in invalid_values:
            pooling_dict = {"max_size": value}
            cnx_options = {"pooling": pooling_dict}
            with self.assertRaises(AttributeError,
                                   msg="with value {}".format(value)):
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
        invalid_values = [False, True, "False", "true", "", -1, "1", None, {},
                          ()]
        for value in invalid_values:
            pooling_dict = {"max_idle_time": value}
            cnx_options = {"pooling": pooling_dict}
            with self.assertRaises(AttributeError,
                                   msg="with value {}".format(value)):
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
        invalid_values = [False, True, "False", "true", "", -1, "1", None, {},
                          ()]
        for value in invalid_values:
            pooling_dict = {"queue_timeout": value}
            cnx_options = {"pooling": pooling_dict}
            with self.assertRaises(AttributeError,
                                   msg="with value {}".format(value)):
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
            with self.assertRaises(AttributeError,
                                   msg="with value {}".format(value)):
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

    def test_get_session(self):
        """Test get_session() opens new connections."""
        # Auxiliary session to query server
        old_session = mysqlx.get_session(self.connect_kwargs.copy())
        # Setup a client to get sessions from
        settings = self.connect_kwargs.copy()
        pooling_dict = {"enabled": True, "max_idle_time":3000}
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
        sleep(0.5)
        # Verify the number of connections open in the server
        connections = get_current_connections(old_session)
        self.assertEqual(len(connections[self.users[0][0]]), total_connections)

        # Verify that clossing the session returns the connection
        # to the pool instead of being closed
        sessions[5].close()
        sessions[9].close()
        sleep(0.5)
        connections = get_current_connections(old_session)
        self.assertTrue(len(connections[self.users[0][0]]) >=
                        (total_connections - 2))

        connections = get_current_connections(old_session)
        open_connections = connections.get("unauthenticated user", [])
        if tests.MYSQL_VERSION < (8, 0, 16):
            # Send reset message requires the user to re-authentificate
            # the connection user stays in unauthenticated user
            self.assertEqual(len(open_connections), 2)
        else:
            self.assertEqual(len(open_connections), 0)

        # Connections must be closed when client.close() is invoked
        # check len(pool) == total_connections
        client.close()
        sleep(3)
        # Verify the connections on the pool are closed
        connections = get_current_connections(old_session)
        open_connections = connections.get("self.users[0][0]", 0)
        self.assertEqual(open_connections, 0)

    def test_max_pool_size(self):
        """Test exausted pool behavior"""
        # Initial pool limit size
        pool_limit = 5
        # Setup a client to get sessions from.
        settings = self.connect_kwargs.copy()
        pooling_dict = {
            "enabled": True,
            "max_idle_time": 10000,
            "queue_timeout": 2000,
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
        # so then can be retrieved again.
        last_session.close()
        _ = client.get_session()
        client.close()

        # verify all sessions are closed
        for session in sessions:
            with self.assertRaises(mysqlx.errors.OperationalError):
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
        with self.assertRaises(mysqlx.errors.OperationalError):
            session.sql("SELECT 1").execute()
        client.close()
        # Verify that clossing the client again does not raise eceptions
        client.close()

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 16), "not reset compatible")
    def test_reset_keeps_same_id(self):
        """Test pooled Session keeps the same session id."""
        settings = self.connect_kwargs.copy()
        pooling_dict = {"enabled": True, "max_size":1}
        cnx_options = {"pooling": pooling_dict}
        client = mysqlx.get_client(settings, cnx_options)

        session1 = client.get_session()
        conn_id1 = session1.sql("select connection_id()"
                               ).execute().fetch_all()[0][0]
        session1.close()
        # Verify that new session is has the same id from previous one
        session2 = client.get_session()
        conn_id2 = session2.sql("select connection_id()"
                               ).execute().fetch_all()[0][0]
        self.assertEqual(conn_id1, conn_id2,
                         "The connection id was not the same")
        session2.close()
        client.close()

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 16), "not reset compatible")
    def test_reset_get_new_connection(self):
        """Test connection is closed by max idle time."""
        # Verify max_idle_time closses an idle session and so for the
        # connection id must increase due to the pool which has created a
        # new connection to the server to return a new session.
        settings = self.connect_kwargs.copy()
        pooling_dict = {"enabled": True, "max_idle_time": 2000, "max_size":3,
                        "queue_timeout": 1000,}
        cnx_options = {"pooling": pooling_dict}
        client = mysqlx.get_client(settings, cnx_options)

        # Getting session 0
        session0 = client.get_session()
        conn_id0 = session0.sql("select connection_id()"
                               ).execute().fetch_all()[0][0]
        # Closing session 0
        session0.close()

        # Getting session 1
        session1 = client.get_session()
        conn_id1 = session1.sql("select connection_id()"
                               ).execute().fetch_all()[0][0]
        # Closing session 1
        self.assertEqual(conn_id1, conn_id0,
                         "The connection id was not greater")

        session1.close()
        # Verify that new session does not has the same id from previous one
        # goint to sleep 2 sec just above the max idle time
        sleep(4)
        # Getting session 2
        session2 = client.get_session()
        conn_id2 = session2.sql("select connection_id()"
                               ).execute().fetch_all()[0][0]
        self.assertNotEqual(conn_id0, conn_id2,
                            "The connection id was the same from the old")
        self.assertNotEqual(conn_id1, conn_id2,
                            "The connection id was the same from the old")

        # Verify pool integrity
        # Open the 4th connection in client life time, when max size is 3
        # Getting the max allowed connections
        # Getting session 3
        session3 = client.get_session()
        conn_id3 = session3.sql("select connection_id()"
                               ).execute().fetch_all()[0][0]
        self.assertGreater(conn_id3, conn_id2,
                           "The connection id was not greater")

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
            self.session.sql(DROP_USER.format(user=self.user,
                                              host=host)).execute()
        for host in self.hosts:
            self.session.sql(
                CREATE_USER.format(user=self.user,
                                   host=host,
                                   password=self.password)).execute()
            # Grant all to new user on database
            self.session.sql(
                GRANT_USER.format(database=self.schema_name,
                                  user=self.user, host=host,
                                  password=self.password)).execute()

    def tearDown(self):
        for host in self.hosts:
            self.session.sql(DROP_USER.format(user=self.user,
                                              host=host)).execute()

    def test_routing(self):
        settings = tests.get_mysqlx_config()
        pooling_dict = {"enabled": True}
        cnx_options = {"pooling": pooling_dict}
        uri = ("mysqlx://{user}:{pwd}@[(address=1.0.0.2:{port}, priority=30),"
               " (address=1.0.0.1:{port}, priority=40),"
               " (address=127.0.0.1:{port}, priority=80),"
               " (address=localhost:{port}, priority=50)]"
               "".format(user=settings["user"], pwd=settings["password"],
                         port=settings["port"]))
        client = mysqlx.get_client(uri, cnx_options)
        # Getting a session must success, the higher priority will cause to a
        # valid address to be used
        client.get_session()
        client.close()

    def test_pools_are_not_shared(self):
        settings = tests.get_mysqlx_config()
        pooling_dict = {"max_size": 1, "max_idle_time": 10000,}
        cnx_options = {"pooling": pooling_dict}
        uri = ("mysqlx://{user}:{pwd}@["
               " (address={host}:{port}, priority=50)]"
               "".format(user=self.user, pwd=self.password,
                         host=settings["host"], port=settings["port"]))
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
        with self.assertRaises(mysqlx.errors.OperationalError):
            session1.sql("SELECT 1").execute()
        session2.sql("SELECT 2").execute()
        session2.get_schema(settings["schema"])

        client2.close()


@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 12), "XPlugin not compatible")
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
            self.session.sql(DROP_USER.format(user=self.user,
                                              host=host)).execute()

        for host in self.hosts:
            self.session.sql(
                CREATE_USER.format(user=self.user,
                                   host=host,
                                   password=self.password)).execute()
            # Grant all to new user on database
            self.session.sql(
                GRANT_USER.format(database=self.schema_name,
                                  user=self.user, host=host,
                                  password=self.password)).execute()

    def tearDown(self):
        for host in self.hosts:
            self.session.sql(DROP_USER.format(user=self.user,
                                              host=host)).execute()

    def test_pools_recycle(self):
        settings = tests.get_mysqlx_config()
        pooling_dict = {"max_size": 1, "max_idle_time": 3000,
                        "queue_timeout": 1000}
        cnx_options = {"pooling": pooling_dict}
        uri = ("mysqlx://{user}:{pwd}@["
               " (address={host}:{port}, priority=50)]?connect_timeout=20000"
               "".format(user=self.user, pwd=self.password,
                         host=settings["host"], port=settings["port"]))
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
                self.os_ver = "-".join(platform.linux_distribution()[0:2])

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
        uri = build_uri(user=self.connect_kwargs["user"],
                        password=self.connect_kwargs["password"],
                        host=self.connect_kwargs["host"],
                        port=self.connect_kwargs["port"],
                        schema=self.connect_kwargs["schema"],
                        connection_attributes=connection_attributes)
        uri = "{},repeated=duplicate_attribute]".format(uri[0:-1])

        with self.assertRaises(InterfaceError) as context:
            my_client = mysqlx.get_client(uri, "{}")
            _ = my_client.get_session()

        self.assertTrue('Duplicate key ' in context.exception.msg,
                        "error found: {}".format(context.exception.msg))

        # Test error is raised for attribute name starting with '_'
        connection_attributes = [
            {"foo": "bar", "_baz": "zoom"},
            {"_baz": "zoom"},
            {"foo": "bar", "_baz": "zoom", "puuuuum": "kaplot"}
        ]
        for conn_attr in connection_attributes:
            connect_kwargs = self.connect_kwargs.copy()
            connect_kwargs["connection_attributes"] = conn_attr
            with self.assertRaises(InterfaceError) as context:
                my_client = mysqlx.get_client(connect_kwargs, "{}")
                _ = my_client.get_session()

            self.assertTrue("connection-attributes" in
                            context.exception.msg)
            self.assertTrue("cannot start with '_'" in context.exception.msg)

        # Test error is raised for attribute name size exceeds 32 characters
        connection_attributes = [
            {"foo": "bar", "p{}w".format("o"*31): "kaplot"},
            {"p{}w".format("o"*31): "kaplot"},
            {"baz": "zoom", "p{}w".format("o"*31): "kaplot", "a": "b"}
        ]
        for conn_attr in connection_attributes:
            connect_kwargs = self.connect_kwargs.copy()
            connect_kwargs["connection_attributes"] = conn_attr
            with self.assertRaises(InterfaceError) as context:
                my_client = mysqlx.get_client(connect_kwargs, "{}")
                _ = my_client.get_session()

            self.assertTrue("exceeds 32 characters limit size" in
                            context.exception.msg)

        # Test error is raised for attribute value size exceeds 1024 characters
        connection_attributes = [
            {"foo": "bar", "pum": "kr{}nk".format("u"*1024)},
            {"pum": "kr{}nk".format("u"*1024)},
            {"baz": "zoom", "pum": "kr{}nk".format("u"*1024), "a": "b"}
        ]
        for conn_attr in connection_attributes:
            connect_kwargs = self.connect_kwargs.copy()
            connect_kwargs["connection-attributes"] = conn_attr
            with self.assertRaises(InterfaceError) as context:
                my_client = mysqlx.get_client(connect_kwargs, "{}")
                _ = my_client.get_session()

            self.assertTrue("exceeds 1024 characters limit size" in
                            context.exception.msg)

        # Validate the user defined attributes are created in the server
        # Test user defined connection attributes through a connection URL
        connection_attributes = {
            "foo": "bar",
            "baz": "zoom",
            "quash": "",
            "puuuuum": "kaplot"
        }
        uri = build_uri(user=self.connect_kwargs["user"],
                        password=self.connect_kwargs["password"],
                        host=self.connect_kwargs["host"],
                        port=self.connect_kwargs["port"],
                        schema=self.connect_kwargs["schema"],
                        connection_attributes=connection_attributes)

        # Verify user defined session-connection-attributes are in the server
        my_session = mysqlx.get_session(uri)
        row = my_session.sql("SHOW VARIABLES LIKE \"pseudo_thread_id\"").\
            execute().fetch_all()[0]
        get_attrs = ("SELECT ATTR_NAME, ATTR_VALUE FROM "
                    "performance_schema.session_account_connect_attrs "
                    "where PROCESSLIST_ID = \"{}\"")
        rows = my_session.sql(get_attrs.format(row.get_string('Value'))).\
            execute().fetch_all()
        expected_attrs = connection_attributes.copy()
        expected_attrs.update({
            "_pid": str(os.getpid()),
            "_platform": self.platform_arch,
            "_source_host": socket.gethostname(),
            "_client_name": "mysql-connector-python",
            "_client_license": self.client_license,
            "_client_version": ".".join([str(x) for x in VERSION[0:3]]),
            "_os": self.os_ver
        })
        # Note that for an empty string "" value the server stores a Null value
        expected_attrs["quash"] = "None"
        for row in rows:
            self.assertEqual(expected_attrs[row.get_string('ATTR_NAME')],
                             row.get_string('ATTR_VALUE'),
                             "Attribute {} with value {} differs of {}".format(
                                 row.get_string('ATTR_NAME'),
                                 row.get_string('ATTR_VALUE'),
                                 expected_attrs[row.get_string('ATTR_NAME')]))


@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 12), "XPlugin not compatible")
class MySQLxPoolingSessionTests(tests.MySQLxTests):
    def setUp(self):
        self.connect_kwargs = tests.get_mysqlx_config()
        self.session = mysqlx.get_session(self.connect_kwargs)
        self.session.sql('DROP DATABASE IF EXISTS my_test_schema').execute()

    def test_get_default_schema(self):
        pooling_dict = {"max_size": 1, "max_idle_time": 3000,
                        "queue_timeout": 1000}
        # Test None value is returned if no schema name is specified
        settings = self.connect_kwargs.copy()
        settings.pop("schema")
        client = mysqlx.get_client(settings, pooling_dict)
        session = client.get_session()
        schema = session.get_default_schema()
        self.assertIsNone(schema,
                          "None value was expected but got '{}'".format(schema))
        session.close()

        # Test SQL statements not fully qualified, which must not raise error:
        #     mysqlx.errors.OperationalError: No database selected
        self.session.sql('CREATE DATABASE my_test_schema').execute()
        self.session.sql('CREATE TABLE my_test_schema.pets(name VARCHAR(20))'
                         ).execute()
        settings = self.connect_kwargs.copy()
        settings["schema"] = "my_test_schema"

        client = mysqlx.get_client(settings, pooling_dict)
        session = client.get_session()
        schema = session.get_default_schema()
        self.assertTrue(schema, mysqlx.Schema)
        self.assertEqual(schema.get_name(),
                         "my_test_schema")
        result = session.sql('SHOW TABLES').execute().fetch_all()
        self.assertEqual("pets", result[0][0])
        self.session.sql('DROP DATABASE my_test_schema').execute()
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
        self.assertIsNone(schema,
                          "None value was expected but got '{}'".format(schema))
        session.close()
        client.close()

        # Test not existing default schema at get_session raise error
        settings = self.connect_kwargs.copy()
        settings["schema"] = "nonexistent"
        client = mysqlx.get_client(settings, pooling_dict)
        self.assertRaises(InterfaceError, client.get_session)
