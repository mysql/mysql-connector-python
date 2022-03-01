# Copyright (c) 2021, 2022, Oracle and/or its affiliates.
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

"""Tests for connection session reset."""

import time
import unittest

import mysqlx
import tests


@unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 25), "XPlugin not compatible")
class SessionResetTests(tests.MySQLxTests):
    """Tests for session reset tests."""

    def test_session_reset(self):
        """Test that when max_idle_time is reached, session is
        re-authenticated."""
        config = tests.get_mysqlx_config()
        connection_string = {
            "user": config["user"],
            "password": config["password"],
            "host": config["host"],
            "port": config["port"],
        }
        client_options = {
            "pooling": {
                "max_idle_time": 1000,
                "max_size": 1,
                "queue_timeout": 1000,
            }
        }
        client = mysqlx.get_client(connection_string, client_options)
        session1 = client.get_session()
        conn_id1 = (
            session1.sql("select connection_id();").execute().fetch_all()[0][0]
        )
        session1.drop_schema("test")
        session1.create_schema("test")
        schema = session1.get_schema("test")
        collection = schema.get_collection("mycoll1")
        if collection.exists_in_database():
            schema.drop_collection("mycoll1")
        collection = schema.create_collection("mycoll1")
        collection.add(
            {"_id": 1, "name": "a"}, {"_id": 2, "name": "b"}
        ).execute()
        self.assertEqual(collection.count(), 2)
        schema.drop_collection("mycoll1")
        session1.close()
        time.sleep(10)
        session2 = client.get_session()
        conn_id2 = (
            session2.sql("select connection_id();").execute().fetch_all()[0][0]
        )
        self.assertNotEqual(conn_id1, conn_id2)
        session2.close()
        client.close()

    def test_session_reset2(self):
        """Test that when the session is closed and reopend again, the reopend
        session will use the same connection without re-authenticating .
        """
        config = tests.get_mysqlx_config()
        connection_string = {
            "user": config["user"],
            "password": config["password"],
            "host": config["host"],
            "port": config["port"],
        }
        client_options = {"pooling": {"max_size": 1, "queue_timeout": 1000}}
        client = mysqlx.get_client(connection_string, client_options)
        session1 = client.get_session()
        conn_id1 = (
            session1.sql("select connection_id();").execute().fetch_all()[0][0]
        )
        session1.drop_schema("test")
        session1.create_schema("test")
        schema = session1.get_schema("test")
        collection = schema.get_collection("mycoll1")
        if collection.exists_in_database():
            schema.drop_collection("mycoll1")
        collection = schema.create_collection(
            "mycoll1",
        )
        collection.add(
            {"_id": 1, "name": "a"}, {"_id": 2, "name": "b"}
        ).execute()
        self.assertEqual(collection.count(), 2)
        session1.sql("SET @a = 1000").execute()
        session1.sql("USE test").execute()
        session1.sql(
            "CREATE TEMPORARY TABLE temp_tbl(a int, b char(20));"
        ).execute()
        session1.close()
        time.sleep(10)
        session2 = client.get_session()
        conn_id2 = (
            session2.sql("select connection_id();").execute().fetch_all()[0][0]
        )
        a = session2.sql("SELECT @a").execute().fetch_one()[0]
        self.assertIsNone(a)
        self.assertEqual(conn_id1, conn_id2)
        schema.drop_collection("mycoll1")
        session2.close()
        client.close()

    def test_session_reset3(self):
        """Test that connection_id is reused."""
        config = tests.get_mysqlx_config()
        connection_string = {
            "user": config["user"],
            "password": config["password"],
            "host": config["host"],
            "port": config["port"],
        }
        client_options = {"pooling": {"enabled": True, "max_size": 2}}
        client = mysqlx.get_client(connection_string, client_options)
        for _ in range(1, 100):
            session1 = client.get_session()
            conn_id1 = (
                session1.sql("select connection_id();")
                .execute()
                .fetch_all()[0][0]
            )
            session1.close()
            session2 = client.get_session()
            conn_id2 = (
                session2.sql("select connection_id();")
                .execute()
                .fetch_all()[0][0]
            )
            session2.close()
            assert conn_id1 == conn_id2
