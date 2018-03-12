# -*- coding: utf-8 -*-

# Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
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
import unittest
import sys
import tests
import mysqlx

from mysqlx.errors import InterfaceError, ProgrammingError
from mysqlx.protobuf import Protobuf

if mysqlx.compat.PY3:
    from urllib.parse import quote_plus
else:
    from urllib import quote_plus

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

    if len(query) > 0:
        uri = "{0}?{1}".format(uri, "&".join(query))

    return uri


@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 12), "XPlugin not compatible")
class MySQLxSessionTests(tests.MySQLxTests):

    def setUp(self):
        self.connect_kwargs = tests.get_mysqlx_config()
        self.schema_name = self.connect_kwargs["schema"]
        try:
            self.session = mysqlx.get_session(self.connect_kwargs)
        except mysqlx.Error as err:
            self.fail("{0}".format(err))

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

    def test_get_schema(self):
        schema = self.session.get_schema(self.schema_name)
        self.assertTrue(schema, mysqlx.Schema)
        self.assertEqual(schema.get_name(), self.schema_name)

    def test_get_default_schema(self):
        schema = self.session.get_default_schema()
        self.assertTrue(schema, mysqlx.Schema)
        self.assertEqual(schema.get_name(), self.connect_kwargs["schema"])

        # Test without default schema configured at connect time
        settings = self.connect_kwargs.copy()
        settings["schema"] = None
        session = mysqlx.get_session(settings)
        self.assertRaises(mysqlx.ProgrammingError, session.get_default_schema)
        session.close()

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
        self.assertTrue(isinstance(statement, mysqlx.statement.Statement))
        # SQL statements should be strings
        statement = self.session.sql(123)
        self.assertRaises(mysqlx.ProgrammingError, statement.execute)
        # Test unicode statements
        statement = self.session.sql(u"SELECT VERSION()").execute()
        self.assertTrue(isinstance(statement, mysqlx.statement.SqlResult))

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
