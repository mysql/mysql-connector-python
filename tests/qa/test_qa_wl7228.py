# Copyright (c) 2014, 2021, Oracle and/or its affiliates.
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
import os.path
import tests


OPTION_FILES_PATH = os.path.join("tests", "data", "qa", "option")


class WL7228Tests(tests.MySQLConnectorTests):
    """Testing the new connection arguments option_files."""

    def setUp(self):
        config = tests.get_mysql_config()
        with mysql.connector.connect(**config) as cnx:
            with cnx.cursor() as cur:
                cur.execute("drop database if exists cli_data")
                cur.execute("drop database if exists cpy_data")
                cur.execute("drop database if exists ex_data")
                cur.execute("drop database if exists mygroup_data")
                cur.execute("create database cli_data")
                cur.execute("create database cpy_data")
                cur.execute("create database ex_data")
                cur.execute("create database mygroup_data")
                cnx.commit()

    def tearDown(self):
        config = tests.get_mysql_config()
        with mysql.connector.connect(**config) as cnx:
            with cnx.cursor() as cur:
                cur.execute("drop database if exists cli_data")
                cur.execute("drop database if exists cpy_data")
                cur.execute("drop database if exists ex_data")
                cur.execute("drop database if exists mygroup_data")

    def get_clean_mysql_config(self):
        config = super().get_clean_mysql_config()
        del config["database"]
        return config

    def test_option_file(self):
        """Checking the basic functionality."""
        config = self.get_clean_mysql_config()
        config["option_files"] = os.path.join(
            OPTION_FILES_PATH, "my_option.cnf"
        )
        for use_pure in self.use_pure_options:
            config["use_pure"] = use_pure
            with mysql.connector.connect(**config) as cnx:
                with cnx.cursor() as _:
                    self.assertEqual(cnx._user, "root")
                    self.assertEqual(cnx._password, "")
                    self.assertEqual(cnx._database, "cpy_data")
                    self.assertEqual(cnx._use_unicode, True)
                    self.assertEqual(cnx._autocommit, True)
                    self.assertEqual(cnx._sql_mode, "ANSI")
                    self.assertEqual(cnx._get_warnings, True)
                    self.assertEqual(cnx._raise_on_warnings, True)
                    self.assertEqual(cnx._connection_timeout, 10)
                    self.assertEqual(cnx._buffered, True)
                    self.assertEqual(cnx._raw, True)
                    self.assertEqual(cnx._force_ipv6, False)

    def test_explicit_arg(self):
        """Setting the connection arguments explicitely."""
        config = self.get_clean_mysql_config()
        config["database"] = "ex_data"
        config["buffered"] = False
        config["force_ipv6"] = True
        config["option_files"] = os.path.join(
            OPTION_FILES_PATH, "my_option.cnf"
        )
        config["option_groups"] = "my_explicit"
        for cls in self.all_cnx_classes:
            with cls(**config) as cnx:
                with cnx.cursor() as _:
                    self.assertEqual(cnx._user, "root")
                    self.assertEqual(cnx._password, "")
                    self.assertEqual(cnx._database, "ex_data")
                    self.assertEqual(cnx._buffered, False)
                    self.assertEqual(cnx._force_ipv6, True)

    def test_invalid_option_file(self):
        """Checking with the option file which doesnt exist."""
        config = self.get_clean_mysql_config()
        config["option_files"] = os.path.join(
            OPTION_FILES_PATH, "my_invalid.cnf"
        )
        for cls in self.all_cnx_classes:
            self.assertRaises(ValueError, cls, **config)

    def test_duplicate_option_groups(self):
        """Checking the behaviour with duplicate groups."""
        config = self.get_clean_mysql_config()
        config["option_files"] = os.path.join(
            OPTION_FILES_PATH, "my_duplicate.cnf"
        )
        for cls in self.all_cnx_classes:
            with cls(**config) as cnx:
                with cnx.cursor() as _:
                    self.assertEqual(cnx._database, "mygroup_data")

    def test_preced_of_groups(self):
        """Checking the precedence of the option groups."""
        config = self.get_clean_mysql_config()
        config["option_files"] = os.path.join(
            OPTION_FILES_PATH, "my_option.cnf"
        )
        for use_pure in self.use_pure_options:
            config["use_pure"] = use_pure
            with mysql.connector.connect(**config) as cnx:
                with cnx.cursor() as _:
                    self.assertEqual(cnx._database, "cpy_data")

    def test_custom_option(self):
        """Checking the behaviour with custom option group."""
        config = self.get_clean_mysql_config()
        config["database"] = "mygroup_data"
        config["option_files"] = os.path.join(
            OPTION_FILES_PATH, "my_option.cnf"
        )
        config["option_groups"] = "my_group"
        for cls in self.all_cnx_classes:
            with cls(**config) as cnx:
                with cnx.cursor() as _:
                    self.assertEqual(cnx._database, "mygroup_data")

    def test_duplicate_files(self):
        """Checking behaviour with duplicate option files."""
        config = self.get_clean_mysql_config()
        config["option_files"] = [
            os.path.join(OPTION_FILES_PATH, "my_option.cnf"),
            os.path.join(OPTION_FILES_PATH, "my_option.cnf"),
        ]
        for cls in self.all_cnx_classes:
            self.assertRaises(ValueError, cls, **config)

    def test_incorrect_perm_file(self):
        """Checking with option file having invalid permission."""
        config = self.get_clean_mysql_config()
        config["option_files"] = os.path.join(
            OPTION_FILES_PATH, "my_option.cnf"
        )
        for use_pure in self.use_pure_options:
            config["use_pure"] = use_pure
            with mysql.connector.connect(**config) as cnx:
                with cnx.cursor() as cur:
                    cur.execute("SELECT DATABASE()")
                    row = cur.fetchone()
                    self.assertNotEqual(row[0], "my_data")

    def test_without_cpy(self):
        """Checking the behaviour with client option group."""
        config = self.get_clean_mysql_config()
        config["option_files"] = os.path.join(OPTION_FILES_PATH, "my_cli.cnf")
        for cls in self.all_cnx_classes:
            with cls(**config) as cnx:
                with cnx.cursor() as _:
                    self.assertEqual(cnx._database, "cli_data")
