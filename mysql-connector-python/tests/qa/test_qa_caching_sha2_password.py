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

import unittest

import mysql.connector
import tests


@unittest.skipIf(
    tests.MYSQL_EXTERNAL_SERVER,
    "Test not available for external MySQL servers",
)
@unittest.skipIf(not tests.SSL_AVAILABLE, "Python has no SSL support")
@unittest.skipIf(
    tests.MYSQL_VERSION < (8, 0, 3),
    "caching_sha2_password plugin not supported by server",
)
class CachingSha2PasswordTests(tests.MySQLConnectorTests):
    """Testing the caching_sha2_password plugin."""

    def test_caching_sha2_password_test1(self):
        """Test FULL authentication with SSL."""
        for use_pure in self.use_pure_options:
            config = self.get_clean_mysql_config()
            config["use_pure"] = use_pure

            with mysql.connector.connect(**config) as cnx:
                cnx.cmd_query("DROP USER IF EXISTS 'sham'@'%'")
                cnx.cmd_query(
                    "CREATE USER 'sham'@'%' IDENTIFIED "
                    "WITH caching_sha2_password BY 'shapass'"
                )
                cnx.cmd_query("GRANT ALL ON *.* TO 'sham'@'%'")

            config["user"] = "sham"
            config["password"] = "shapass"

            with mysql.connector.connect(**config) as cnx:
                with cnx.cursor() as cur:
                    cur.execute(
                        "SELECT CONNECTION_TYPE FROM "
                        "performance_schema.threads "
                        "WHERE processlist_command='Query'"
                    )
                    res = cur.fetchone()
                    # Verifying that the connection is secured
                    self.assertEqual(res[0], "SSL/TLS")

                    cur.execute("DROP TABLE IF EXISTS t1")
                    cur.execute("CREATE TABLE t1(j1 int)")
                    cur.execute("INSERT INTO t1 VALUES ('1')")
                    cur.execute("SELECT * FROM t1")
                    self.assertEqual(1, len(cur.fetchone()))
                    cur.execute("DROP TABLE IF EXISTS t1")
                cnx.cmd_query("DROP USER IF EXISTS 'sham'@'%'")

    def test_caching_sha2_password_test3(self):
        """Test full authentication with SSL after create user,
        flushing privileges, altering user, setting new password."""
        for use_pure in self.use_pure_options:
            config = self.get_clean_mysql_config()
            config["use_pure"] = use_pure

            with mysql.connector.connect(**config) as cnx:
                cnx.cmd_query("DROP USER IF EXISTS 'sham'@'%'")
                cnx.cmd_query(
                    "CREATE USER 'sham'@'%' IDENTIFIED "
                    "WITH caching_sha2_password BY 'shapass'"
                )
                cnx.cmd_query("GRANT ALL ON *.* TO 'sham'@'%'")
                cnx.cmd_query("FLUSH PRIVILEGES")

            config["user"] = "sham"
            config["password"] = "shapass"

            with mysql.connector.connect(**config) as cnx:
                with cnx.cursor() as cur:
                    cur.execute(
                        "SELECT CONNECTION_TYPE "
                        "FROM performance_schema.threads "
                        "WHERE processlist_command='Query'"
                    )
                    res = cur.fetchone()
                    # Verifying that the connection is secured
                    self.assertEqual(res[0], "SSL/TLS")

                    cur.execute("DROP TABLE IF EXISTS t2")
                    cur.execute("CREATE TABLE t2(j1 int);")
                    cur.execute("INSERT INTO t2 VALUES ('1');")
                    cur.execute("SELECT * FROM t2")
                    self.assertEqual(1, len(cur.fetchone()))
                    cur.execute("DROP TABLE IF EXISTS t2")
                    cur.execute("SET PASSWORD FOR 'sham'@'%'='newshapass'")
                    config["password"] = "newshapass"
                    mysql.connector.connect(**config)
                cnx.cmd_query("DROP USER IF EXISTS 'sham'@'%'")
