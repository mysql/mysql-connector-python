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

import unittest

import mysql.connector
import tests

from mysql.connector.errors import DatabaseError, PoolError


class WL6080Tests(tests.MySQLConnectorTests):
    """Test for WL6080."""

    def _create_pool(self, pool_name):
        """Invoked by a thread from test_creat_multipools."""
        config = self.get_clean_mysql_config()
        config["pool_name"] = pool_name
        config["pool_size"] = 5
        with mysql.connector.connect(**config) as _:
            with mysql.connector.connect(pool_name=pool_name) as _:
                with mysql.connector.connect(pool_name=pool_name) as cnx:
                    cur = cnx.cursor()
                    cur.close()

    def test_correctpool(self):
        """Test Valid connection pool scenarios."""
        config = self.get_clean_mysql_config()
        config["pool_name"] = "testpool"
        config["pool_size"] = 5
        for use_pure in self.use_pure_options:
            config["use_pure"] = use_pure
            with mysql.connector.connect(**config) as _:
                with mysql.connector.connect(pool_name="testpool") as cnx:
                    with cnx.cursor() as _:
                        self.assertEqual(cnx._database, config["database"])

    def test_invalid_poolsize(self):
        """Test connection pool with 0 as pool size.
        Bug 17401406
        """
        config = self.get_clean_mysql_config()
        config["pool_name"] = "testpool"
        config["pool_size"] = 0
        for use_pure in self.use_pure_options:
            config["use_pure"] = use_pure
            self.assertRaises(
                mysql.connector.errors.PoolError,
                mysql.connector.connect,
                **config,
            )

    def test_negative_poolsize(self):
        """Test connection pool with -1 as pool size.
        Bug 17401406
        """
        config = self.get_clean_mysql_config()
        config["pool_name"] = "testpool"
        config["pool_size"] = -1
        for use_pure in self.use_pure_options:
            config["use_pure"] = use_pure
            self.assertRaises(
                mysql.connector.errors.PoolError,
                mysql.connector.connect,
                **config,
            )

    def test_spchar_name(self):
        """Test with pool name containing only special characters."""
        config = self.get_clean_mysql_config()
        config["pool_name"] = "&^%$!#@^*((!&#^$^@((!(&!&^^()__)(*!^^"
        config["pool_size"] = 5
        for use_pure in self.use_pure_options:
            config["use_pure"] = use_pure
            self.assertRaises(
                AttributeError,
                mysql.connector.connect,
                pool_name="&^%$!#@^*((!&#^$^@((!(&!&^^()__)(*!^^",
            )

    def test_diffchar_name(self):
        """Test with pool name "containing special character."""
        config = self.get_clean_mysql_config()
        config["pool_name"] = "testpool*$#_"
        config["pool_size"] = 5
        for use_pure in self.use_pure_options:
            config["use_pure"] = use_pure
            with mysql.connector.connect(**config) as _:
                with mysql.connector.connect(pool_name="testpool*$#_") as cnx:
                    with cnx.cursor() as _:
                        self.assertEqual(cnx._database, config["database"])

    def test_long_name(self):
        """Test with pool name "really really long"."""
        config = self.get_clean_mysql_config()
        config[
            "pool_name"
        ] = "THIS POOL NAME NEEDS TO BE REALLY REALLY LONG SUCH THAT THIS SHOULD PROBABLY RAISE AN ERROR  THIS POOL NAME NEEDS TO BE REALLY REALLY LONG SUCH THAT THIS SHOULD PROBABLY RAISE AN ERROR THIS POOL NAME NEEDS TO BE REALLY REALLY LONG SUCH THAT THIS SHOULD PROBABLY RAISE AN ERROR"
        config["pool_size"] = 5
        for use_pure in self.use_pure_options:
            config["use_pure"] = use_pure
            self.assertRaises(
                AttributeError, mysql.connector.connect, **config
            )

    def test_numbers_name(self):
        """Test with pool name having only numbers."""
        config = self.get_clean_mysql_config()
        config["pool_name"] = "1234567890"
        config["pool_size"] = 5
        for use_pure in self.use_pure_options:
            config["use_pure"] = use_pure
            with mysql.connector.connect(**config) as _:
                with mysql.connector.connect(pool_name="1234567890") as cnx:
                    with cnx.cursor() as _:
                        self.assertEqual(cnx._database, config["database"])

    def test_spaceonly_name(self):
        """Test with pool name "having only spaces."""
        config = self.get_clean_mysql_config()
        config["pool_name"] = "                                             "
        config["pool_size"] = 5
        for use_pure in self.use_pure_options:
            config["use_pure"] = use_pure
            self.assertRaises(
                AttributeError, mysql.connector.connect, **config
            )

    def test_multipool_samename(self):
        """Test with multiple pool name having same name.
        This currently works as against the specification in WL6080.
        """
        config = self.get_clean_mysql_config()
        config["pool_name"] = "testpool"
        config["pool_size"] = 5
        with mysql.connector.connect(**config) as _:
            with mysql.connector.connect(pool_name="testpool") as cnx:
                cur = cnx.cursor()
                cur.close()
                with mysql.connector.connect(**config) as _:
                    with mysql.connector.connect(pool_name="testpool") as cnx:
                        cur = cnx.cursor()
                        cur.close()

    def test_create_multipools(self):
        """For i in 1 to 100 run the thread with create_pool(poolname+1)."""
        config = self.get_clean_mysql_config()
        config["database"] = "information_schema"
        with mysql.connector.connect(**config) as cnx:
            cur = cnx.cursor()
            cur.execute("SHOW VARIABLES LIKE 'max_connections'")
            max_connections = int(cur.fetchone()[1])

        cnx_list = []
        for idx in range(1, max_connections):
            config["pool_name"] = "newname{}".format(idx)
            config["pool_size"] = 5
            try:
                cnx_list.append(
                    mysql.connector.connect(pool_name=config["pool_name"])
                )
            except DatabaseError as err:
                self.assertEqual(err.errno, 1040)
            except PoolError:
                pass
        for cnx in cnx_list:
            cnx.close()
