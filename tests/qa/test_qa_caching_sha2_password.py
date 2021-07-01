# Copyright (c) 2021, Oracle and/or its affiliates.
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

import mysql.connector
import tests
import unittest


@unittest.skipIf(not tests.SSL_AVAILABLE, "Python has no SSL support")
@unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 3),
                 "caching_sha2_password plugin not supported by server")
class CachingSha2PasswordTests(tests.MySQLConnectorTests):
    """Testing the caching_sha2_password plugin."""

    # def setUp(self):
    #     self.config = self.get_clean_mysql_config()
    #     self.config["user"] = "sham"
    #     self.config["password"] = "shapass"
    #     self.user = "sham@{}".format(self.config["host"])
    #     with mysql.connector.connect(**tests.get_mysql_config()) as cnx:
    #         cnx.cmd_query("DROP USER IF EXISTS {}".format(self.user))
    #         cnx.cmd_query(
    #             "CREATE USER {} IDENTIFIED "
    #             "WITH caching_sha2_password BY 'shapass'".format(self.user)
    #         )
    #         cnx.cmd_query("GRANT ALL ON *.* to {}".format(self.user))
    #         cnx.cmd_query("FLUSH PRIVILEGES")
    #         cnx.cmd_query("DROP TABLE IF EXISTS t1"
    #         cnx.cmd_query("CREATE TABLE t1(j1 int)")
    #         cnx.cmd_query("INSERT INTO t1 VALUES ('1')")

    # def tearDown(self):
    #     with mysql.connector.connect(**tests.get_mysql_config()) as cnx:
    #         cnx.cmd_query("DROP USER IF EXISTS {}".format(self.user))
    #         cnx.cmd_query("DROP TABLE IF EXISTS t1")

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

    # def test_caching_sha2_password_test4(self):
    #     """Testing fast authentication without SSL."""
    #     for use_pure in self.use_pure_options:
    #         config = self.get_clean_mysql_config()
    #         config["use_pure"] = use_pure

    #         config["ssl_disabled"] = True
    #         with mysql.connector.connect(**config) as cnx:
    #             cnx.cmd_query("DROP USER IF EXISTS 'sham'@'%'")
    #             cnx.cmd_query(
    #                 "CREATE USER 'sham'@'%' IDENTIFIED "
    #                 "WITH caching_sha2_password BY 'shapass'"
    #             )
    #             cnx.cmd_query("GRANT ALL ON *.* TO 'sham'@'%'")
    #             cnx.cmd_query("FLUSH PRIVILEGES")

    #         config["user"] = "sham"
    #         config["password"] = "newshapass"

    #         import pprint as pp; pp.pprint(config)

    #         with mysql.connector.connect(**config) as cnx:
    #             with cnx.cursor() as cur:
    #                 cur.execute("set password for sham@localhost='newshapass'")
    #                 cur.execute(
    #                     "SELECT CONNECTION_TYPE FROM "
    #                     "performance_schema.threads "
    #                     "WHERE processlist_command='Query'"
    #                 )
    #                 res = cur.fetchone()
    #                 # Verifying that the connection is non-ssl
    #                 self.assertEqual(res[0], "TCP/IP")
    #             cnx.cmd_query("DROP USER IF EXISTS 'sham'@'%'")

# # If flush privileges is performed, it is expected to performs full authentication
# def caching_sha2_password_test5():
#     """Authenticate with the MySQL server using caching_sha2_password"""
#     try:
#         cnx = mysql.connector.connect(
#             user="root",
#             password="",
#             host=results.hostname,
#             port=results.portvalue,
#             database="test",
#         )
#         #        cnx = mysql.connector.connect(user='sham', password='newshapass',database='test',host=results.hostname,port=results.portvalue, ssl_disabled=True)
#         cursor = cnx.cursor()
#         cursor.execute("flush privileges")
#         cnx = mysql.connector.connect(
#             user="sham",
#             password="newshapass",
#             database="test",
#             host=results.hostname,
#             port=results.portvalue,
#             ssl_disabled=True,
#         )
#         cursor = cnx.cursor()
#         cursor.execute(
#             "SELECT CONNECTION_TYPE from performance_schema.threads where processlist_command='Query';"
#         )
#         res = cursor.fetchone()
#         print(res[0])
#         assert res[0] == "SSL/TLS"  # verifying that the connection is secured
#         print("caching_sha2_password_test5 IS NOT OK")
#     except mysql.connector.errors.InterfaceError as e:
#         print(e)
#         print("caching_sha2_password_test5 IS OK")
#     except Exception as err:
#         print(err)
#         print("caching_sha2_password_test5 IS NOT OK")
#         traceback.print_exc()


# # Testing full authentication without SSL - should fail
# def caching_sha2_password_test6():
#     """Authenticate with the MySQL server using caching_sha2_password"""
#     try:
#         cnx = mysql.connector.connect(
#             user="root",
#             password="",
#             host=results.hostname,
#             port=results.portvalue,
#             database="test",
#         )
#         cursor = cnx.cursor()
#         drop_user("sham", cursor)
#         cursor.execute(
#             "create user sham@localhost identified with caching_sha2_password by 'shapass'"
#         )
#         cursor.execute("grant all on *.* to sham@localhost")
#         cnx = mysql.connector.connect(
#             user="sham",
#             password="shapass",
#             database="test",
#             host=results.hostname,
#             port=results.portvalue,
#             ssl_disabled=True,
#         )
#         cursor = cnx.cursor()
#         cursor.execute(
#             "SELECT CONNECTION_TYPE from performance_schema.threads where processlist_command='Query';"
#         )
#         res = cursor.fetchone()
#         print(res[0])
#         assert res[0] == "SSL/TLS"  # verifying that the connection is secured
#         print("caching_sha2_password_test6 IS NOT OK")
#     except mysql.connector.errors.InterfaceError as e:
#         print(e)
#         print("caching_sha2_password_test6 IS OK")
#     except Exception as err:
#         print(err)
#         print("caching_sha2_password_test6 IS NOT OK")
#         traceback.print_exc()


# # Test with empty password - authentication should fail
# def caching_sha2_password_test7():
#     """Authenticate with the MySQL server using caching_sha2_password"""
#     try:
#         cnx = mysql.connector.connect(
#             user="sham",
#             password="",
#             database="test",
#             host=results.hostname,
#             port=results.portvalue,
#             ssl_disabled=True,
#         )
#         cursor = cnx.cursor()
#         cursor.execute(
#             "SELECT CONNECTION_TYPE from performance_schema.threads where processlist_command='Query';"
#         )
#         res = cursor.fetchone()
#         print(res[0])
#         assert res[0] == "SSL/TLS"  # verifying that the connection is secured
#         print("caching_sha2_password_test7 IS NOT OK")
#     except mysql.connector.errors.ProgrammingError as e:
#         print(e)
#         print("caching_sha2_password_test7 IS OK")
#     except Exception as err:
#         print(err)
#         print("caching_sha2_password_test7 IS NOT OK")
#         traceback.print_exc()


# # Create user with empty password, Testing authentication with empty password - should succeed both fast and full
# def caching_sha2_password_test8():
#     """Authenticate with the MySQL server using caching_sha2_password"""
#     try:
#         cnx = mysql.connector.connect(
#             user="root",
#             password="",
#             host=results.hostname,
#             port=results.portvalue,
#             database="test",
#         )
#         cursor = cnx.cursor()
#         drop_user("sham", cursor)
#         cursor.execute(
#             "create user sham@localhost identified with caching_sha2_password by ''"
#         )
#         cursor.execute("grant all on *.* to sham@localhost")
#         cursor.execute("flush privileges")
#         #        cnx = mysql.connector.connect(user='sham', password='',database='test',host=results.hostname,port=results.portvalue)
#         #        cursor=cnx.cursor()
#         #        cursor.execute("SELECT CONNECTION_TYPE from performance_schema.threads where processlist_command='Query';")
#         #        print(cursor.fetchone())
#         cnx = mysql.connector.connect(
#             user="sham",
#             password="",
#             database="test",
#             host=results.hostname,
#             port=results.portvalue,
#             ssl_disabled=True,
#         )
#         cursor = cnx.cursor()
#         cursor.execute(
#             "SELECT CONNECTION_TYPE from performance_schema.threads where processlist_command='Query';"
#         )
#         res = cursor.fetchone()
#         print(res[0])
#         assert res[0] == "TCP/IP"  # verifying that the connection is non-ssl
#         print("caching_sha2_password_test8 IS OK")
#     except mysql.connector.errors.InterfaceError as e:
#         print(e)
#         print("caching_sha2_password_test8 IS NOT OK")
#     except Exception as err:
#         print(err)
#         print("caching_sha2_password_test8 IS NOT OK")
#         traceback.print_exc()


# def test_shutdown():
#     if cursor is not None:
#         cursor.close()
#     if cnx is not None:
#         cnx.close()
#     print("test_shutdown IS OK")


# def drop_user(str, cur):
#     try:
#         cur.execute("drop user " + str + "@localhost")
#     except mysql.connector.Error as err:
#         print("Ignore Error: {0}".format(err))

