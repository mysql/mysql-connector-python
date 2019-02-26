# -*- coding: utf-8 -*-

# Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.
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

"""Unittests for mysqlx.connection
"""

import logging
import os
import platform
import unittest
import sys
import tests
import time
import socket
import struct
import mysqlx

from threading import Thread
from time import sleep

from mysqlx.connection import SocketStream
from mysqlx.compat import STRING_TYPES
from mysqlx.errors import InterfaceError, OperationalError, ProgrammingError
from mysqlx.protocol import  Message, MessageReaderWriter, Protocol
from mysqlx.protobuf import HAVE_MYSQLXPB_CEXT, mysqlxpb_enum, Protobuf
from mysql.connector.version import VERSION, LICENSE

if mysqlx.compat.PY3:
    from urllib.parse import quote_plus, quote
else:
    from urllib import quote_plus, quote

from .test_mysqlx_crud import drop_table

LOGGER = logging.getLogger(tests.LOGGER_NAME)

_URI_TEST_RESULTS = (  # (uri, result)
    ("127.0.0.1", None),
    ("localhost", None),
    ("domain.com", None),
    ("user:password@127.0.0.1", {"schema": "", "host": "127.0.0.1",
                                 "password": "password", "port": 33060,
                                 "user": "user"}),
    ("user:password@127.0.0.1:33061", {"schema": "", "host": "127.0.0.1",
                                       "password": "password", "port": 33061,
                                       "user": "user"}),
    ("user:@127.0.0.1", {"schema": "", "host": "127.0.0.1", "password": "",
                         "port": 33060, "user": "user"}),
    ("user:@127.0.0.1/schema", {"schema": "schema", "host": "127.0.0.1",
                                "password": "", "port": 33060,
                                "user": "user"}),
    ("user:@127.0.0.1/schema?use_pure=true", {"schema": "schema",
                                              "host": "127.0.0.1",
                                              "password": "", "port": 33060,
                                              "user": "user",
                                              "use-pure": True}),
    ("user{0}:password{0}@127.0.0.1/schema?use_pure=true"
     "".format(quote("?!@#$%/:")), {"schema": "schema", "host": "127.0.0.1",
                                    "port": 33060, "user": "user?!@#$%/:",
                                    "password": "password?!@#$%/:",
                                    "use-pure": True}),
    ("mysqlx://user:@127.0.0.1", {"schema": "", "host": "127.0.0.1",
                                  "password": "", "port": 33060,
                                  "user": "user"}),
    ("mysqlx://user:@127.0.0.1:33060/schema",
     {"schema": "schema", "host": "127.0.0.1", "password": "", "port": 33060,
      "user": "user"}),
    ("mysqlx://user@[2001:db8:85a3:8d3:1319:8a2e:370:7348]:1", None),
    ("mysqlx://user:password@[2001:db8:85a3:8d3:1319:8a2e:370:7348]:1",
     {"schema": "", "host": "2001:db8:85a3:8d3:1319:8a2e:370:7348",
      "password": "password", "port": 1, "user": "user"}),
    ("mysqlx://user:password@[2001:db8:85a3:8d3:1319:8a2e:370:7348]:1/schema",
     {"schema": "schema", "host": "2001:db8:85a3:8d3:1319:8a2e:370:7348",
      "password": "password", "port": 1, "user": "user"}),
    ("áé'í'óú:unicode@127.0.0.1",
     {"schema": "", "host": "127.0.0.1", "password": "unicode",
      "port": 33060, "user": "áé'í'óú"}),
    ("unicode:áé'í'óú@127.0.0.1",
     {"schema": "", "host": "127.0.0.1", "password": "áé'í'óú",
      "port": 33060, "user": "unicode"}),
    ("root:@[localhost, 127.0.0.1:88, [::]:99, [a1:b1::]]",
     {"routers": [{"host": "localhost", "port": 33060},
                  {"host": "127.0.0.1", "port": 88},
                  {"host": "::", "port": 99},
                  {"host": "a1:b1::", "port": 33060}],
      "user": "root", "password": "", "schema": ""}),
     ("root:@[a1:a2:a3:a4:a5:a6:a7:a8]]",
      {"host": "a1:a2:a3:a4:a5:a6:a7:a8", "schema": "",
              "port": 33060, "user": "root", "password": ""}),
     ("root:@localhost", {"user": "root", "password": "",
      "host": "localhost", "port": 33060, "schema": ""}),
     ("root:@[a1:b1::]", {"user": "root", "password": "",
      "host": "a1:b1::", "port": 33060, "schema": ""}),
     ("root:@[a1:b1::]:88", {"user": "root", "password": "",
      "host": "a1:b1::", "port": 88, "schema": ""}),
     ("root:@[[a1:b1::]:88]", {"user": "root", "password": "",
      "routers": [{"host": "a1:b1::", "port":88}], "schema": ""}),
     ("root:@[(address=localhost:99, priority=99)]",
      {"user": "root", "password": "", "schema": "",
      "routers": [{"host": "localhost", "port": 99, "priority": 99}]})
)


_ROUTER_LIST_RESULTS = (  # (uri, result)
    ("áé'í'óú:unicode@127.0.0.1", {"schema": "", "host": "127.0.0.1",
     "port": 33060, "password": "unicode", "user": "áé'í'óú"}),
    ("unicode:áé'í'óú@127.0.0.1", {"schema": "", "host": "127.0.0.1",
     "port": 33060, "password": "áé'í'óú", "user": "unicode"}),
    ("user:password@[127.0.0.1, localhost]", {"schema": "", "routers":
     [{"host": "127.0.0.1", "port": 33060}, {"host": "localhost", "port":
     33060}], "password": "password", "user": "user"}),
    ("user:password@[(address=127.0.0.1, priority=99), (address=localhost,"
     "priority=98)]", {"schema": "", "routers": [{"host": "127.0.0.1",
     "port": 33060, "priority": 99}, {"host": "localhost", "port": 33060,
     "priority": 98}], "password": "password", "user": "user"}),
)

_PREP_STMT_QUERY = (
    "SELECT p.sql_text, p.count_execute "
    "FROM performance_schema.prepared_statements_instances AS p "
    "JOIN performance_schema.threads AS t ON p.owner_thread_id = t.thread_id "
    "AND t.processlist_id = @@pseudo_thread_id")


def file_uri(path, brackets=True):
    if brackets:
        return "{0}{1}".format(path[0], quote_plus(path[1:]))
    return "({0})".format(path)

def build_uri(**kwargs):
    uri = "mysqlx://{0}:{1}".format(kwargs["user"], kwargs["password"])

    if "host" in kwargs:
        host = "[{0}]".format(kwargs["host"]) \
                if ":" in kwargs["host"] else kwargs["host"]
        uri = "{0}@{1}".format(uri, host)
    elif "routers" in kwargs:
        routers = []
        for router in kwargs["routers"]:
            fmt = "(address={host}{port}, priority={priority})" \
                   if "priority" in router else "{host}{port}"
            host = "[{0}]".format(router["host"]) if ":" in router["host"] \
                    else router["host"]
            port = ":{0}".format(router["port"]) if "port" in router else ""

            routers.append(fmt.format(host=host, port=port,
                                      priority=router.get("priority", None)))

        uri = "{0}@[{1}]".format(uri, ",".join(routers))
    else:
        raise ProgrammingError("host or routers required.")

    if "port" in kwargs:
        uri = "{0}:{1}".format(uri, kwargs["port"])
    if "schema" in kwargs:
        uri = "{0}/{1}".format(uri, kwargs["schema"])

    query = []
    if "ssl_mode" in kwargs:
        query.append("ssl-mode={0}".format(kwargs["ssl_mode"]))
    if "ssl_ca" in kwargs:
        query.append("ssl-ca={0}".format(kwargs["ssl_ca"]))
    if "ssl_cert" in kwargs:
        query.append("ssl-cert={0}".format(kwargs["ssl_cert"]))
    if "ssl_key" in kwargs:
        query.append("ssl-key={0}".format(kwargs["ssl_key"]))
    if "use_pure" in kwargs:
        query.append("use-pure={0}".format(kwargs["use_pure"]))
    if "connect_timeout" in kwargs:
        query.append("connect-timeout={0}".format(kwargs["connect_timeout"]))
    if "connection_attributes" in kwargs:
        conn_attrs = kwargs["connection_attributes"]
        if isinstance(conn_attrs, STRING_TYPES) and \
           not (conn_attrs.startswith("[") and conn_attrs.endswith("[")):
            query.append("connection-attributes={}"
                         "".format(kwargs["connection_attributes"]))
        else:
            attr_list = []
            for key in conn_attrs:
                attr_list.append("{}={}".format(key, conn_attrs[key]))
            query.append("connection-attributes={0}"
                         "".format("[{}]".format(",".join(attr_list))))

    if len(query) > 0:
        uri = "{0}?{1}".format(uri, "&".join(query))

    return uri


class ServerSocketStream(SocketStream):
    def __init__(self):
        self._socket = None

    def start_receive(self, host, port):
        """Opens a sokect to comunicate to the given host, port

        Args:
            host (str): host name.
            port (int): host port.
        Returns:
            address of the communication channel
        """
        my_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        my_sock.bind((host, port))
        # Starting receiving...
        if sys.version_info > (3, 4):
            my_sock.listen()
        else:
            my_sock.listen(1)
        self._socket, addr = my_sock.accept()
        return addr


class ServerProtocol(Protocol):
    def __init__(self, reader_writer):
        super(ServerProtocol, self).__init__(reader_writer)

    def send_auth_continue_server(self, auth_data):
        """Send Server authenticate continue.

        Args:
            auth_data (str): Authentication data.
        """
        msg = Message("Mysqlx.Session.AuthenticateContinue",
                      auth_data=auth_data)
        self._writer.write_message(mysqlxpb_enum(
            "Mysqlx.ServerMessages.Type.SESS_AUTHENTICATE_CONTINUE"), msg)

    def send_auth_ok(self):
        """Send authenticate OK.
        """
        msg = Message("Mysqlx.Session.AuthenticateOk")
        self._writer.write_message(mysqlxpb_enum(
            "Mysqlx.ServerMessages.Type.SESS_AUTHENTICATE_OK"), msg)


@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 12), "XPlugin not compatible")
class MySQLxSessionTests(tests.MySQLxTests):

    def setUp(self):
        self.connect_kwargs = tests.get_mysqlx_config()
        self.schema_name = self.connect_kwargs["schema"]
        try:
            self.session = mysqlx.get_session(self.connect_kwargs)
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

    def test___init__(self):
        bad_config = {
            "host": "bad_host",
            "port": "",
            "username": "root",
            "password": ""
        }
        self.assertRaises(InterfaceError, mysqlx.Session, bad_config)

        host = self.connect_kwargs["host"]
        port = self.connect_kwargs["port"]
        user = self.connect_kwargs["user"]
        password = self.connect_kwargs["password"]

        # Session to a farm using one of many routers (prios)
        # Loop during connect because of network error (succeed)
        routers = [{"host": "bad_host","priority": 100},
                   {"host": host, "port": port, "priority": 98}]
        uri = build_uri(user=user, password=password, routers=routers)
        session = mysqlx.get_session(uri)
        session.close()

        # Session to a farm using one of many routers (incomplete prios)
        routers = [{"host": "bad_host", "priority": 100},
                   {"host": host, "port": port}]
        uri = build_uri(user=user, password=password, routers=routers)
        self.assertRaises(ProgrammingError, mysqlx.get_session, uri)
        try:
            session = mysqlx.get_session(uri)
        except ProgrammingError as err:
            self.assertEqual(4000, err.errno)

        # Session to a farm using invalid priorities (out of range)
        routers = [{"host": "bad_host", "priority": 100},
                   {"host": host, "port": port, "priority": 101}]
        uri = build_uri(user=user, password=password, routers=routers)
        self.assertRaises(ProgrammingError, mysqlx.get_session, uri)
        try:
            session = mysqlx.get_session(uri)
        except ProgrammingError as err:
            self.assertEqual(4007, err.errno)

        # Establish an Session to a farm using one of many routers (no prios)
        routers = [{"host": "bad_host"}, {"host": host, "port": port}]
        uri = build_uri(user=user, password=password, routers=routers)
        session = mysqlx.get_session(uri)
        session.close()

        # Break loop during connect (non-network error)
        uri = build_uri(user=user, password="bad_pass", routers=routers)
        self.assertRaises(InterfaceError, mysqlx.get_session, uri)

        # Break loop during connect (none left)
        uri = "mysqlx://{0}:{1}@[bad_host, another_bad_host]".format(user, password)
        self.assertRaises(InterfaceError, mysqlx.get_session, uri)
        try:
            session = mysqlx.get_session(uri)
        except InterfaceError as err:
            self.assertEqual(4001, err.errno)

        # Invalid option with URI
        uri = "mysqlx://{0}:{1}@{2}:{3}?invalid=option" \
              "".format(user, password, host, port)
        self.assertRaises(ProgrammingError, mysqlx.get_session, uri)

        # Invalid option with dict
        config = {
            "user": user,
            "password": password,
            "host": host,
            "port": port,
            "invalid": "option"
        }
        self.assertRaises(ProgrammingError, mysqlx.get_session, config)

        # Invalid option with kwargs
        self.assertRaises(ProgrammingError, mysqlx.get_session, **config)

        # SocketSteam.is_socket()
        session = mysqlx.get_session(user=user, password=password,
                                     host=host, port=port)
        self.assertFalse(session._connection.stream.is_socket())

    def test_auth(self):
        sess = mysqlx.get_session(self.connect_kwargs)
        sess.sql("CREATE USER 'native'@'%' IDENTIFIED WITH "
                 "mysql_native_password BY 'test'").execute()
        sess.sql("CREATE USER 'sha256'@'%' IDENTIFIED WITH "
                 "sha256_password BY 'sha256'").execute()

        config = {'host': self.connect_kwargs['host'],
                  'port': self.connect_kwargs['port']}

        config['user'] = 'native'
        config['password'] = 'test'
        config['auth'] = 'plain'
        mysqlx.get_session(config)

        config['auth'] = 'mysql41'
        mysqlx.get_session(config)

        config['user'] = 'sha256'
        config['password'] = 'sha256'
        if tests.MYSQL_VERSION >= (8, 0, 1):
            config['auth'] = 'plain'
            mysqlx.get_session(config)

        config['auth'] = 'mysql41'
        self.assertRaises(InterfaceError, mysqlx.get_session, config)

        sess.sql("DROP USER 'native'@'%'").execute()
        sess.sql("DROP USER 'sha256'@'%'").execute()
        sess.close()

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 5),
                     "SHA256_MEMORY authentation mechanism not available")
    def test_auth_sha265_memory(self):
        sess = mysqlx.get_session(self.connect_kwargs)
        sess.sql("CREATE USER 'caching'@'%' IDENTIFIED WITH "
                 "caching_sha2_password BY 'caching'").execute()
        config = {
            "user": "caching",
            "password": "caching",
            "host": self.connect_kwargs["host"],
            "port": self.connect_kwargs["port"]
        }

        # Session creation is not possible with SSL disabled
        config["ssl-mode"] = mysqlx.SSLMode.DISABLED
        self.assertRaises(InterfaceError, mysqlx.get_session, config)
        config["auth"] = mysqlx.Auth.SHA256_MEMORY
        self.assertRaises(InterfaceError, mysqlx.get_session, config)

        # Session creation is possible with SSL enabled
        config["ssl-mode"] = mysqlx.SSLMode.REQUIRED
        config["auth"] = mysqlx.Auth.PLAIN
        mysqlx.get_session(config)

        # Disable SSL
        config["ssl-mode"] = mysqlx.SSLMode.DISABLED

        # Password is in cache will, session creation is possible
        config["auth"] = mysqlx.Auth.SHA256_MEMORY
        mysqlx.get_session(config)

        sess.sql("DROP USER 'caching'@'%'").execute()
        sess.close()

    @unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 15), "--mysqlx-socket option"
                     " tests not available for this MySQL version")
    @unittest.skipIf(os.name == 'nt', "sockets not available"
                     " on windows")
    def test_mysqlx_socket(self):
        # Connect with unix socket
        uri = "mysqlx://{user}:{password}@({socket})".format(
            user=self.connect_kwargs["user"],
            password=self.connect_kwargs["password"],
            socket=self.connect_kwargs["socket"])

        session = mysqlx.get_session(uri)

        # No SSL with Unix Sockets
        res = mysqlx.statement.SqlStatement(session._connection,
            "SHOW STATUS LIKE 'Mysqlx_ssl_active'").execute().fetch_all()
        self.assertEqual("OFF", res[0][1])

        session.close()

        # Socket parsing tests
        conn = mysqlx._get_connection_settings("root:@(/path/to/sock)")
        self.assertEqual("/path/to/sock", conn["socket"])
        self.assertEqual("", conn["schema"])

        conn = mysqlx._get_connection_settings("root:@(/path/to/sock)/schema")
        self.assertEqual("/path/to/sock", conn["socket"])
        self.assertEqual("schema", conn["schema"])

        conn = mysqlx._get_connection_settings("root:@/path%2Fto%2Fsock")
        self.assertEqual("/path/to/sock", conn["socket"])
        self.assertEqual("", conn["schema"])

        conn = mysqlx._get_connection_settings("root:@/path%2Fto%2Fsock/schema")
        self.assertEqual("/path/to/sock", conn["socket"])
        self.assertEqual("schema", conn["schema"])

        conn = mysqlx._get_connection_settings("root:@.%2Fpath%2Fto%2Fsock")
        self.assertEqual("./path/to/sock", conn["socket"])
        self.assertEqual("", conn["schema"])

        conn = mysqlx._get_connection_settings("root:@.%2Fpath%2Fto%2Fsock"
                                               "/schema")
        self.assertEqual("./path/to/sock", conn["socket"])
        self.assertEqual("schema", conn["schema"])

        conn = mysqlx._get_connection_settings("root:@..%2Fpath%2Fto%2Fsock")
        self.assertEqual("../path/to/sock", conn["socket"])
        self.assertEqual("", conn["schema"])

        conn = mysqlx._get_connection_settings("root:@..%2Fpath%2Fto%2Fsock"
                                               "/schema")
        self.assertEqual("../path/to/sock", conn["socket"])
        self.assertEqual("schema", conn["schema"])

    @unittest.skipIf(HAVE_MYSQLXPB_CEXT == False, "C Extension not available")
    def test_connection_uri(self):
        uri = build_uri(user=self.connect_kwargs["user"],
                        password=self.connect_kwargs["password"],
                        host=self.connect_kwargs["host"],
                        port=self.connect_kwargs["port"],
                        schema=self.connect_kwargs["schema"],
                        use_pure=False)
        session = mysqlx.get_session(uri)
        self.assertIsInstance(session, mysqlx.Session)

        # Test URI parser function
        for uri, res in _URI_TEST_RESULTS:
            try:
                settings = mysqlx._get_connection_settings(uri)
                self.assertEqual(res, settings)
            except mysqlx.Error:
                self.assertEqual(res, None)

        # Test URI parser function
        for uri, res in _ROUTER_LIST_RESULTS:
            try:
                settings = mysqlx._get_connection_settings(uri)
                self.assertEqual(res, settings)
            except mysqlx.Error:
                self.assertEqual(res, None)

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 13),
                 "MySQL 8.0.13+ is required for connect timeout")
    def test_connect_timeout(self):
        config = self.connect_kwargs.copy()
        # 0 ms disables timouts on socket connections
        config["connect-timeout"] = 0
        session = mysqlx.get_session(config)
        session.close()

        # 10000 ms should be time enough to connect
        config["connect-timeout"] = 10000
        session = mysqlx.get_session(config)
        session.close()

        # Use connect timeout in URI
        session = mysqlx.get_session(build_uri(**config))
        session.close()

        # Timeout for an unreachable host
        # https://en.wikipedia.org/wiki/IPv4#Special-use_addresses
        hosts = [
            "198.51.100.255",
            "192.0.2.255",
            "10.255.255.1",
            "192.0.2.0",
            "203.0.113.255",
            "10.255.255.255",
            "192.168.255.255",
            "203.0.113.4",
            "192.168.0.0",
            "172.16.0.0",
            "10.255.255.251",
            "172.31.255.255",
            "198.51.100.23",
            "172.16.255.255",
            "198.51.100.8",
            "192.0.2.254",
        ]
        unreach_hosts = []
        config["connect-timeout"] = 2000

        # Find two unreachable hosts for testing
        for host in hosts:
            try:
                config["host"] = host
                mysqlx.get_session(config)
            except mysqlx.TimeoutError:
                unreach_hosts.append(host)
                if len(unreach_hosts) == 2:
                    break  # We just need 2 unreachable hosts
            except:
                pass

        total_unreach_hosts = len(unreach_hosts)
        self.assertEqual(total_unreach_hosts, 2,
                         "Two unreachable hosts are needed, {0} found"
                         "".format(total_unreach_hosts))

        # Multi-host scenarios
        # Connect to a secondary host if the primary fails
        routers = [
            {"host": unreach_hosts[0], "port": config["port"], "priority": 100},
            {"host": "127.0.0.1", "port": config["port"], "priority": 90}
        ]
        uri = build_uri(user=config["user"], password=config["password"],
                        connect_timeout=2000, routers=routers)
        session = mysqlx.get_session(uri)
        session.close()

        # Fail to connect to all hosts
        routers = [
            {"host": unreach_hosts[0], "port": config["port"], "priority": 100},
            {"host": unreach_hosts[1], "port": config["port"], "priority": 90}
        ]
        uri = build_uri(user=config["user"], password=config["password"],
                        connect_timeout=2000, routers=routers)
        try:
            mysqlx.get_session(uri)
            self.fail("It should not connect to any unreachable host")
        except mysqlx.TimeoutError as err:
            self.assertEqual(err.msg,
                             "All server connection attempts were aborted. "
                             "Timeout of 2000 ms was exceeded for each "
                             "selected server")
        except mysqlx.InterfaceError as err:
            self.assertEqual(err.msg, "Failed to connect to any of the routers")

        # Trying to establish a connection with a wrong password should not
        # wait for timeout
        config["host"] = "127.0.0.1"
        config["password"] = "invalid_password"
        config["connect-timeout"] = 2000
        time_start = time.time()
        self.assertRaises(InterfaceError, mysqlx.get_session, config)
        time_elapsed = time.time() - time_start
        session.close()
        if time_elapsed >= config["connect-timeout"]:
            self.fail("Trying to establish a connection with a wrong password "
                      "should not wait for timeout")

        # The connection timeout value must be a positive integer
        config["connect-timeout"] = -1
        self.assertRaises(TypeError, mysqlx.get_session, config)
        config["connect-timeout"] = 10.0983
        self.assertRaises(TypeError, mysqlx.get_session, config)
        config["connect-timeout"] = "abc"
        self.assertRaises(TypeError, mysqlx.get_session, config)

    def test_get_schemas(self):
        schema_name = "test_get_schemas"
        self.session.create_schema(schema_name)
        schemas = self.session.get_schemas()
        self.assertIsInstance(schemas, list)
        self.assertTrue(schema_name in schemas)
        self.session.drop_schema(schema_name)

    def test_get_schema(self):
        schema = self.session.get_schema(self.schema_name)
        self.assertTrue(schema, mysqlx.Schema)
        self.assertEqual(schema.get_name(), self.schema_name)

    def test_get_default_schema(self):
        schema = self.session.get_default_schema()
        self.assertTrue(schema, mysqlx.Schema)
        self.assertEqual(schema.get_name(), self.connect_kwargs["schema"])
        self.assertTrue(schema.exists_in_database())

        # Test None value is returned if no schema name is specified
        settings = self.connect_kwargs.copy()
        settings.pop("schema")
        session = mysqlx.get_session(settings)
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
        session = mysqlx.get_session(settings)
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

        # Test without default schema configured at connect time (passing None)
        settings = self.connect_kwargs.copy()
        settings["schema"] = None
        build_uri(**settings)
        session = mysqlx.get_session(settings)
        schema = session.get_default_schema()
        self.assertIsNone(schema,
                          "None value was expected but got '{}'".format(schema))
        session.close()

        # Test not existing default schema at get_session raise error
        settings = self.connect_kwargs.copy()
        settings["schema"] = "nonexistent"
        self.assertRaises(InterfaceError, mysqlx.get_session, settings)

        # Test BUG#28942938: 'ACCESS DENIED' error for unauthorized user tries
        # to use the default schema if not exists at get_session
        self.session.sql("DROP USER IF EXISTS 'def_schema'@'%'").execute()
        self.session.sql("CREATE USER 'def_schema'@'%' IDENTIFIED WITH "
                         "mysql_native_password BY 'test'").execute()
        settings = self.connect_kwargs.copy()
        settings['user'] = 'def_schema'
        settings['password'] = 'test'
        settings["schema"] = "nonexistent"
        # a) Test with no Granted privileges
        with self.assertRaises(InterfaceError) as context:
            _ = mysqlx.get_session(settings)
        # Access denied for this user
        self.assertEqual(1044, context.exception.errno)

        # Grant privilege to one unrelated schema
        self.session.sql("GRANT ALL PRIVILEGES ON nonexistent.* TO "
                         "'def_schema'@'%'").execute()
        with self.assertRaises(InterfaceError) as context:
            _ = mysqlx.get_session(settings)
        # Schema does not exist
        self.assertNotEqual(1044, context.exception.errno)

    def test_drop_schema(self):
        test_schema = 'mysql_session_test_drop_schema'
        schema = self.session.create_schema(test_schema)

        self.session.drop_schema(test_schema)
        self.assertFalse(schema.exists_in_database())

    def test_create_schema(self):
        schema = self.session.create_schema(self.schema_name)
        self.assertTrue(schema.exists_in_database())

    def test_sql(self):
        statement = self.session.sql("SELECT VERSION()")
        self.assertTrue(isinstance(statement, mysqlx.Statement))
        # SQL statements should be strings
        statement = self.session.sql(123)
        self.assertRaises(mysqlx.ProgrammingError, statement.execute)
        # Test unicode statements
        statement = self.session.sql(u"SELECT VERSION()").execute()
        self.assertTrue(isinstance(statement, mysqlx.SqlResult))

    def test_rollback(self):
        table_name = "t2"
        schema = self.session.get_schema(self.schema_name)

        if not schema.exists_in_database():
            self.session.create_schema(self.schema_name)

        stmt = "CREATE TABLE {0}.{1}(_id INT)"
        self.session.sql(stmt.format(self.schema_name, table_name)).execute()
        table = schema.get_table(table_name)

        self.session.start_transaction()

        table.insert("_id").values(1).execute()
        self.assertEqual(table.count(), 1)

        self.session.rollback()
        self.assertEqual(table.count(), 0)

        drop_table(schema, table_name)

    def test_commit(self):
        table_name = "t2"
        schema = self.session.get_schema(self.schema_name)

        if not schema.exists_in_database():
            self.session.create_schema(self.schema_name)

        stmt = "CREATE TABLE {0}.{1}(_id INT)"
        self.session.sql(stmt.format(self.schema_name, table_name)).execute()
        table = schema.get_table(table_name)

        self.session.start_transaction()

        table.insert("_id").values(1).execute()
        self.assertEqual(table.count(), 1)

        self.session.commit()
        self.assertEqual(table.count(), 1)

        drop_table(schema, table_name)

    def test_savepoint(self):
        collection_name = "collection_test"
        schema = self.session.get_schema(self.schema_name)

        # The savepoint name should be a valid string
        self.assertRaises(mysqlx.errors.ProgrammingError,
                          self.session.set_savepoint, 123)

        # The savepoint name should not be an empty string
        self.assertRaises(mysqlx.errors.ProgrammingError,
                          self.session.set_savepoint, "")

        # The savepoint name should not be a white space
        self.assertRaises(mysqlx.errors.ProgrammingError,
                          self.session.set_savepoint, " ")

        # Invalid rollback savepoint without a started transaction
        sp1 = self.session.set_savepoint("sp1")
        self.assertRaises(mysqlx.errors.OperationalError,
                          self.session.rollback_to, sp1)

        collection = schema.create_collection(collection_name)

        self.session.start_transaction()

        collection.add({"_id": "1", "name": "Fred", "age": 21}).execute()
        self.assertEqual(1, collection.count())

        # Create a savepoint named 'sp2'
        sp2 = self.session.set_savepoint("sp2")
        self.assertEqual(sp2, "sp2")

        collection.add({"_id": "2", "name": "Wilma", "age": 33}).execute()
        self.assertEqual(2, collection.count())

        # Create a savepoint named 'sp3'
        sp3 = self.session.set_savepoint("sp3")

        collection.add({"_id": "3", "name": "Betty", "age": 67}).execute()
        self.assertEqual(3, collection.count())

        # Rollback to 'sp3' savepoint
        self.session.rollback_to(sp3)
        self.assertEqual(2, collection.count())

        # Rollback to 'sp2' savepoint
        self.session.rollback_to(sp2)
        self.assertEqual(1, collection.count())

        # The 'sp3' savepoint should not exist at this point
        self.assertRaises(mysqlx.errors.OperationalError,
                          self.session.rollback_to, sp3)

        collection.add({"_id": "4", "name": "Barney", "age": 42}).execute()
        self.assertEqual(2, collection.count())

        # Create an unnamed savepoint
        sp4 = self.session.set_savepoint()

        collection.add({"_id": "3", "name": "Wilma", "age": 33}).execute()
        self.assertEqual(3, collection.count())

        # Release unnamed savepoint
        self.session.release_savepoint(sp4)
        self.assertEqual(3, collection.count())

        # The 'sp4' savepoint should not exist at this point
        self.assertRaises(mysqlx.errors.OperationalError,
                          self.session.rollback_to, sp4)

        self.session.commit()
        schema.drop_collection(collection_name)

    def test_close(self):
        session = mysqlx.get_session(self.connect_kwargs)
        schema = session.get_schema(self.schema_name)

        session.close()
        self.assertRaises(mysqlx.OperationalError, schema.exists_in_database)

    @unittest.skipIf(sys.version_info < (2, 7, 9), "The support for SSL is "
                     "not available for Python versions < 2.7.9.")
    def test_ssl_connection(self):
        config = {}
        config.update(self.connect_kwargs)
        socket = config.pop("socket")

        # Secure by default
        session = mysqlx.get_session(config)

        res = mysqlx.statement.SqlStatement(session._connection,
            "SHOW STATUS LIKE 'Mysqlx_ssl_active'").execute().fetch_all()
        self.assertEqual("ON", res[0][1])

        res = mysqlx.statement.SqlStatement(session._connection,
            "SHOW STATUS LIKE 'Mysqlx_ssl_version'").execute().fetch_all()
        self.assertTrue("TLS" in res[0][1])

        session.close()

        # Error on setting Client key without Client Certificate
        config["ssl-key"] = tests.SSL_KEY
        self.assertRaises(InterfaceError, mysqlx.get_session, config)

        # Error on settings CRL without setting CA Certificate
        config["ssl-crl"] = "/dummy/path"
        self.assertRaises(InterfaceError, mysqlx.get_session, config)
        config.pop("ssl-crl")

        # Error on setting SSL Mode to disabled with any SSL option
        config["ssl-mode"] = "disabled"
        self.assertRaises(InterfaceError, mysqlx.get_session, config)

        # Error on setting SSL Mode to verify_* without ssl_ca
        config["ssl-mode"] = "verify_ca"
        self.assertRaises(InterfaceError, mysqlx.get_session, config)

        config["ssl-mode"] = "verify_identity"
        self.assertRaises(InterfaceError, mysqlx.get_session, config)

        # Error on SSL Mode set to required with CA set
        config["ssl-ca"] = tests.SSL_CA
        config["ssl-cert"] = tests.SSL_CERT
        config["ssl-mode"] = "required"
        self.assertRaises(InterfaceError, mysqlx.get_session, config)

        # Connection with ssl parameters
        config["ssl-mode"] = "verify_identity"
        session = mysqlx.get_session(config)

        res = mysqlx.statement.SqlStatement(session._connection,
            "SHOW STATUS LIKE 'Mysqlx_ssl_active'").execute().fetch_all()
        self.assertEqual("ON", res[0][1])

        res = mysqlx.statement.SqlStatement(session._connection,
            "SHOW STATUS LIKE 'Mysqlx_ssl_version'").execute().fetch_all()
        self.assertTrue("TLS" in res[0][1])

        session.close()

        # Error if ssl-mode=disabled and ssl_* set
        extra = [("ssl_mode", "disabled"),
                 ("ssl_ca", "({0})".format(tests.SSL_CA))]
        uri = build_uri(**dict(list(self.connect_kwargs.items()) + extra))
        self.assertRaises(InterfaceError, mysqlx.get_session, uri)

        # Error if invalid ssl-mode
        extra = [("ssl_mode", "invalid")]
        uri = build_uri(**dict(list(self.connect_kwargs.items()) + extra))
        self.assertRaises(InterfaceError, mysqlx.get_session, uri)

        # Parsing SSL Certificates
        extra = [("ssl_mode", "verify_ca"),
                 ("ssl_ca", file_uri(tests.SSL_CA, False)),
                 ("ssl_key", file_uri(tests.SSL_KEY, False)),
                 ("ssl_cert", file_uri(tests.SSL_CERT, False))]
        uri = build_uri(**dict(list(self.connect_kwargs.items()) + extra))
        session = mysqlx.get_session(uri)

        extra = [("ssl_mode", "verify_ca"),
                 ("ssl_ca", file_uri(tests.SSL_CA)),
                 ("ssl_key", file_uri(tests.SSL_KEY)),
                 ("ssl_cert", file_uri(tests.SSL_CERT))]
        uri = build_uri(**dict(list(self.connect_kwargs.items()) + extra))
        session = mysqlx.get_session(uri)

    def test_disabled_x_protocol(self):
        session = mysqlx.get_session(self.connect_kwargs)
        res = session.sql("SHOW VARIABLES WHERE Variable_name = 'port'") \
                     .execute().fetch_all()
        settings = self.connect_kwargs.copy()
        settings["port"] = res[0][1]  # Lets use the MySQL classic port
        session.close()
        self.assertRaises(ProgrammingError, mysqlx.get_session, settings)

    @unittest.skipIf(HAVE_MYSQLXPB_CEXT == False, "C Extension not available")
    def test_use_pure(self):
        settings = self.connect_kwargs.copy()
        settings["use-pure"] = False
        session = mysqlx.get_session(settings)
        self.assertFalse(session.use_pure)
        self.assertEqual(Protobuf.mysqlxpb.__name__, "_mysqlxpb")
        session.use_pure = True
        self.assertTrue(session.use_pure)
        self.assertEqual(Protobuf.mysqlxpb.__name__, "_mysqlxpb_pure")
        # 'use_pure' should be a bool type
        self.assertRaises(ProgrammingError, setattr, session, "use_pure", -1)
        session.close()

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 16), "XPlugin not compatible")
    def test_connection_attributes(self):
        # Validate an error is raised if URL user defined connection attributes
        # given in a list are invalid
        invalid_conn_attrs = [2, 1.2, "[_='13']", '[_="1"]', '[_=23]', "[_2.3]",
                              "[_invalid]", "[valid=0,_]", "[valid=0,_nvalid]",
                              "[_invalid,valid=0]"]
        uri = build_uri(user=self.connect_kwargs["user"],
                            password=self.connect_kwargs["password"],
                            host=self.connect_kwargs["host"],
                            port=self.connect_kwargs["port"],
                            schema=self.connect_kwargs["schema"])
        for invalid_attr in invalid_conn_attrs:
            uri_test = "{}?connection_attributes={}".format(uri, invalid_attr)
            with self.assertRaises(InterfaceError) as _:
                mysqlx.get_session(uri_test)
                LOGGER.error("InterfaceError not raised while testing "
                             "invalid attribute: {}".format(invalid_attr))

        # Validate an error is raised if URL user defined connection attributes
        # are not a list or a bool type
        invalid_conn_attrs = ["[incompleteL", "incompleteL]", "A", "invalid",
                              "_invalid", "2", "2.3", "{}", "{invalid=0}",
                              "{[invalid=0]}", "_", 2, 0.2]

        for invalid_attr in invalid_conn_attrs:
            uri_test = "{}?connection_attributes={}".format(uri, invalid_attr)
            with self.assertRaises(InterfaceError) as _:
                mysqlx.get_session(uri_test)
                LOGGER.error("InterfaceError not raised while testing "
                             "invalid attribute: {}".format(invalid_attr))

        # Validate an error is raised if URL user defined connection attributes
        # through a connection URL when a name is duplicated
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
            mysqlx.get_session(uri)
            LOGGER.error("InterfaceError not raised while testing "
                             "uri: {}".format(uri))

        self.assertTrue("Duplicate key 'repeated' used in "
                        "connection-attributes" in context.exception.msg)

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
                mysqlx.get_session(connect_kwargs)
                LOGGER.error("InterfaceError not raised while testing "
                             "connect_kwargs: {}".format(connect_kwargs))

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
                mysqlx.get_session(connect_kwargs)
                LOGGER.error("InterfaceError not raised while testing "
                             "connection_attributes: {}".format(conn_attr))

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
                mysqlx.get_session(connect_kwargs)
                LOGGER.error("InterfaceError not raised while testing "
                             "connection_attributes: {}".format(conn_attr))

            self.assertTrue("exceeds 1024 characters limit size" in
                            context.exception.msg)

        # Test valid generic values for the connection-attributes on URI
        valid_conn_attrs = ["[]", "False", "True", "false", "true", "[valid]",
                            "[valid=0]", "[valid,valid2=0]", '["_valid=0]',
                            "[valid2='0']", "[valid=,valid2=0]", "['_valid=0]",
                            "[[_valid=0]]"]
        uri = build_uri(user=self.connect_kwargs["user"],
                        password=self.connect_kwargs["password"],
                        host=self.connect_kwargs["host"],
                        port=self.connect_kwargs["port"],
                        schema=self.connect_kwargs["schema"])
        for valid_attr in valid_conn_attrs:
            uri_test = "{}?connection_attributes={}".format(uri, valid_attr)
            mysqlx.get_session(uri_test)

        # Test valid generic values when passing a dict with connection data
        valid_conn_attrs = [{}, "False", "True", "false", "true", {"valid": ""},
                            {"valid": None}, {"valid1": 1}, True, False, 1, 0,
                            [], ['a1=2', 'a3'], {"valid"}, {"foo", "bar"}]
        for conn_attr in valid_conn_attrs:
            connect_kwargs = self.connect_kwargs.copy()
            connect_kwargs["connection_attributes"] = conn_attr
            mysqlx.get_session(connect_kwargs)

        # Test invalid generic values when passing a dict with connection data
        invalid_conn_attrs = [{1:"1"}, {1:2}, {"_invalid":""}, {"_": ""},
                              123, 123.456, None, {"_invalid"}, ['_a1=2',]]
        for conn_attr in invalid_conn_attrs:
            connect_kwargs = self.connect_kwargs.copy()
            connect_kwargs["connection_attributes"] = conn_attr
            with self.assertRaises(InterfaceError) as context:
                mysqlx.get_session(connect_kwargs)
                LOGGER.error("InterfaceError not raised while testing "
                             "connection_attributes: {}".format(conn_attr))

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

        # Verify connection-attributes can be skiped to be set on server
        # by URI as "connection_attributes"=false
        uri = build_uri(user=self.connect_kwargs["user"],
                        password=self.connect_kwargs["password"],
                        host=self.connect_kwargs["host"],
                        port=self.connect_kwargs["port"],
                        schema=self.connect_kwargs["schema"],
                        connection_attributes="false")
        my_session = mysqlx.get_session(uri)
        row = my_session.sql("SHOW VARIABLES LIKE \"pseudo_thread_id\"").\
            execute().fetch_all()[0]
        get_attrs = ("SELECT ATTR_NAME, ATTR_VALUE FROM "
                    "performance_schema.session_account_connect_attrs "
                    "where PROCESSLIST_ID = \"{}\"")
        rows = my_session.sql(get_attrs.format(row.get_string('Value'))).\
            execute().fetch_all()
        self.assertEqual(len(rows), 0, "connection attributes where created "
                         "while was specified to not do so: {}".format(rows))


@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 12), "XPlugin not compatible")
class MySQLxInnitialNoticeTests(tests.MySQLxTests):

    def setUp(self):
        self.connect_kwargs = tests.get_mysqlx_config()
        self.settings = {
            "user": "root",
            "password": "",
            "host": "localhost",
            "ssl-mode": "disabled",
            "use_pure": True
        }

    def _server_thread(self, host="localhost", port=33061, notice=1):
            stream = ServerSocketStream()
            stream.start_receive(host, port)
            reader_writer = MessageReaderWriter(stream)
            protocol = ServerProtocol(reader_writer)
            # Read message header
            hdr = stream.read(5)
            msg_len, msg_type = struct.unpack("<LB", hdr)
            self.assertEqual(msg_type, 4)
            # Read payload
            _ = stream.read(msg_len - 1)
            # send handshake
            if notice == 1:
                # send empty notice
                stream.sendall(b"\x01\x00\x00\x00\x0b")
            else:
                # send notice frame with explicit default
                stream.sendall(b"\x03\x00\x00\x00\x0b\x08\x01")
                #stream.sendall(b"\x01\x00\x00\x00\x0b")
            # send auth start")
            protocol.send_auth_continue_server("00000000000000000000")
            # Capabilities are not check for ssl-mode: disabled
            # Reading auth_continue from client
            hdr = stream.read(5)
            msg_len, msg_type = struct.unpack("<LB", hdr)
            self.assertEqual(msg_type, 5)
            # Read payload
            _ = stream.read(msg_len - 1)
            # Send auth_ok
            protocol.send_auth_ok()

            # Read message
            hdr = stream.read(5)
            msg_len, msg_type = struct.unpack("<LB", hdr)
            self.assertEqual(msg_type, 12)
            # Read payload
            _ = stream.read(msg_len - 1)
            # send empty notice
            if notice == 1:
                # send empty notice
                stream.sendall(b"\x01\x00\x00\x00\x0b")
            else:
                # send notice frame with explicit default
                stream.sendall(b"\x03\x00\x00\x00\x0b\x08\x01")

            # msg_type: 12 Mysqlx.Resultset.ColumnMetaData
            stream.sendall(b"\x31\x00\x00\x00\x0c"
                           b"\x08\x07\x12\x08\x44\x61\x74\x61\x62\x61\x73"
                           b"\x65\x1a\x08\x44\x61\x74\x61\x62\x61\x73\x65"
                           b"\x22\x08\x53\x43\x48\x45\x4d\x41\x54\x41\x2a"
                           b"\x00\x32\x00\x3a\x03\x64\x65\x66\x40\x4c\x50"
                           b"\xc0\x01\x58\x10")

            # send unexpected notice
            if notice == 1:
                # send empty notice
                stream.sendall(b"\x01\x00\x00\x00\x0b")
            else:
                # send notice frame with explicit default
                stream.sendall(b"\x03\x00\x00\x00\x0b\x08\x01")

            # msg_type: 13 Mysqlx.Resultset.Row
            stream.sendall(b"\x16\x00\x00\x00\x0d"
                           b"\x0a\x13\x69\x6e\x66\x6f\x72\x6d\x61\x74\x69"
                           b"\x6f\x6e\x5f\x73\x63\x68\x65\x6d\x61\x00")
            # msg_type: 14 Mysqlx.Resultset.FetchDone
            stream.sendall(b"\x01\x00\x00\x00\x0e")
            # msg_type: 11 Mysqlx.Notice.Frame
            stream.sendall(b"\x0f\x00\x00\x00\x0b\x08\x03\x10\x02\x1a\x08\x08"
                           b"\x04\x12\x04\x08\x02\x18\x00")

            # send unexpected notice
            if notice == 1:
                # send empty notice
                stream.sendall(b"\x01\x00\x00\x00\x0b")
            else:
                # send notice frame with explicit default
                stream.sendall(b"\x03\x00\x00\x00\x0b\x08\x01")

            # msg_type: 17 Mysqlx.Sql.StmtExecuteOk
            stream.sendall(b"\x01\x00\x00\x00\x11")

            # Read message
            hdr = stream.read(5)
            msg_len, msg_type = struct.unpack("<LB", hdr)
            self.assertEqual(msg_type, 7)
            # Read payload
            _ = stream.read(msg_len - 1)
            stream.sendall(b"\x07\x00\x00\x00\x00\n\x04bye!")

            # Close socket
            stream.close()

    @unittest.skipIf(HAVE_MYSQLXPB_CEXT == False, "C Extension not available")
    def test_initial_empty_notice_cext(self):
        connect_kwargs = self.connect_kwargs.copy()
        host = "localhost"
        port = connect_kwargs["port"] + 10
        worker1 = Thread(target=self._server_thread, args=[host, port, 1])
        worker1.daemon = True
        worker1.start()
        sleep(1)
        settings = self.settings.copy()
        settings["port"] = port
        settings["use_pure"] = False
        session = mysqlx.get_session(settings)
        rows = session.sql("show databases").execute().fetch_all()
        self.assertEqual(rows[0][0], "information_schema")
        session.close()

    def test_initial_empty_notice_pure(self):
        connect_kwargs = self.connect_kwargs.copy()
        host = "localhost"
        port = connect_kwargs["port"] + 20
        worker2 = Thread(target=self._server_thread, args=[host, port, 1])
        worker2.daemon = True
        worker2.start()
        sleep(2)
        settings = self.settings.copy()
        settings["port"] = port
        settings["use_pure"] = True
        session = mysqlx.get_session(settings)
        rows = session.sql("show databases").execute().fetch_all()
        self.assertEqual(rows[0][0], "information_schema")
        session.close()

    @unittest.skipIf(HAVE_MYSQLXPB_CEXT == False, "C Extension not available")
    def test_initial_notice_cext(self):
        connect_kwargs = self.connect_kwargs.copy()
        host = "localhost"
        port = connect_kwargs["port"] + 11
        worker1 = Thread(target=self._server_thread, args=[host, port, 2])
        worker1.daemon = True
        worker1.start()
        sleep(1)
        settings = self.settings.copy()
        settings["port"] = port
        settings["use_pure"] = False
        session = mysqlx.get_session(settings)
        rows = session.sql("show databases").execute().fetch_all()
        self.assertEqual(rows[0][0], "information_schema")
        session.close()

    def test_initial_notice_pure(self):
        connect_kwargs = self.connect_kwargs.copy()
        host = "localhost"
        port = connect_kwargs["port"] + 21
        worker2 = Thread(target=self._server_thread, args=[host, port, 2])
        worker2.daemon = True
        worker2.start()
        sleep(2)
        settings = self.settings.copy()
        settings["port"] = port
        settings["use_pure"] = True
        session = mysqlx.get_session(settings)
        rows = session.sql("show databases").execute().fetch_all()
        self.assertEqual(rows[0][0], "information_schema")
        session.close()
