# -*- coding: utf-8 -*-

# Copyright (c) 2018, Oracle and/or its affiliates. All rights reserved.
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

from mysqlx.errors import InterfaceError
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
        pooling_dict = {"enabled": True, "max_idle_time":3}
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
        self.assertTrue(len(connections[self.users[0][0]]) >=
                        (total_connections - 2))

        connections = get_current_connections(old_session)
        # At far the send reset message requires the user to re-authentificate
        # the connection user stays in unauthenticated user
        open_connections = connections.get("unauthenticated user", [])
        self.assertEqual(len(open_connections), 2)

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
            "max_idle_time": 2,
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
        pooling_dict = {"max_size": 1, "max_idle_time": 0,}
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
                        "queue_timeout": 10}
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
        # At far the send reset message requires the user to re-authentificate
        # the connection user stays in unauthenticated user
        open_connections = connections.get("unauthenticated user", [])
        self.assertTrue(len(open_connections) >= 1)

        client.close()
