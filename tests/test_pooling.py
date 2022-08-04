# Copyright (c) 2013, 2022, Oracle and/or its affiliates. All rights reserved.
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

"""Unittests for mysql.connector.pooling
"""

import unittest
import uuid

from queue import Queue

try:
    from mysql.connector.connection_cext import CMySQLConnection
except ImportError:
    CMySQLConnection = None

import mysql.connector
import tests

from mysql.connector import errors, pooling
from mysql.connector.connection import MySQLConnection
from mysql.connector.constants import ClientFlag

MYSQL_CNX_CLASS = (
    (MySQLConnection)
    if CMySQLConnection is None
    else (MySQLConnection, CMySQLConnection)
)


class PoolingTests(tests.MySQLConnectorTests):
    def tearDown(self):
        mysql.connector.pooling._CONNECTION_POOLS = {}

    def test_generate_pool_name(self):
        self.assertRaises(errors.PoolError, pooling.generate_pool_name)

        config = {"host": "ham", "database": "spam"}
        self.assertEqual("ham_spam", pooling.generate_pool_name(**config))

        config = {"database": "spam", "port": 3377, "host": "example.com"}
        self.assertEqual("example.com_3377_spam", pooling.generate_pool_name(**config))

        config = {
            "user": "ham",
            "database": "spam",
            "port": 3377,
            "host": "example.com",
        }
        self.assertEqual(
            "example.com_3377_ham_spam", pooling.generate_pool_name(**config)
        )


class PooledMySQLConnectionTests(tests.MySQLConnectorTests):
    def tearDown(self):
        mysql.connector.pooling._CONNECTION_POOLS = {}

    def test___init__(self):
        dbconfig = tests.get_mysql_config()
        if tests.MYSQL_VERSION < (5, 7):
            dbconfig["client_flags"] = [-ClientFlag.CONNECT_ARGS]
        cnxpool = pooling.MySQLConnectionPool(pool_size=1, **dbconfig)
        self.assertRaises(TypeError, pooling.PooledMySQLConnection)
        cnx = MySQLConnection(**dbconfig)
        pcnx = pooling.PooledMySQLConnection(cnxpool, cnx)
        self.assertEqual(cnxpool, pcnx._cnx_pool)
        self.assertEqual(cnx, pcnx._cnx)

        self.assertRaises(AttributeError, pooling.PooledMySQLConnection, None, None)
        self.assertRaises(AttributeError, pooling.PooledMySQLConnection, cnxpool, None)

    def test___getattr__(self):
        dbconfig = tests.get_mysql_config()
        if tests.MYSQL_VERSION < (5, 7):
            dbconfig["client_flags"] = [-ClientFlag.CONNECT_ARGS]
        cnxpool = pooling.MySQLConnectionPool(pool_size=1, pool_name="test")
        cnx = MySQLConnection(**dbconfig)
        pcnx = pooling.PooledMySQLConnection(cnxpool, cnx)

        exp_attrs = {
            "_connection_timeout": dbconfig["connection_timeout"],
            "_database": dbconfig["database"],
            "_host": dbconfig["host"],
            "_password": dbconfig["password"],
            "_port": dbconfig["port"],
            "_unix_socket": dbconfig["unix_socket"],
        }
        for attr, value in exp_attrs.items():
            self.assertEqual(
                value,
                getattr(pcnx, attr),
                "Attribute {0} of reference connection not correct".format(attr),
            )

        self.assertEqual(pcnx.connect, cnx.connect)

    def test_close(self):
        dbconfig = tests.get_mysql_config()
        if tests.MYSQL_VERSION < (5, 7):
            dbconfig["client_flags"] = [-ClientFlag.CONNECT_ARGS]
        cnxpool = pooling.MySQLConnectionPool(pool_size=1, **dbconfig)

        cnxpool._original_cnx = None

        def dummy_add_connection(self, cnx=None):
            self._original_cnx = cnx

        cnxpool.add_connection = dummy_add_connection.__get__(
            cnxpool, pooling.MySQLConnectionPool
        )
        if tests.MYSQL_VERSION < (5, 7):
            dbconfig["client_flags"] = [-ClientFlag.CONNECT_ARGS]
        pcnx = pooling.PooledMySQLConnection(cnxpool, MySQLConnection(**dbconfig))

        cnx = pcnx._cnx
        pcnx.close()
        self.assertEqual(cnx, cnxpool._original_cnx)

    def test_config(self):
        dbconfig = tests.get_mysql_config()
        cnxpool = pooling.MySQLConnectionPool(pool_size=1, **dbconfig)
        cnx = cnxpool.get_connection()

        self.assertRaises(errors.PoolError, cnx.config, user="spam")


class MySQLConnectionPoolTests(tests.MySQLConnectorTests):
    def tearDown(self):
        mysql.connector.pooling._CONNECTION_POOLS = {}

    def test___init__(self):
        dbconfig = tests.get_mysql_config()
        if tests.MYSQL_VERSION < (5, 7):
            dbconfig["client_flags"] = [-ClientFlag.CONNECT_ARGS]
        self.assertRaises(errors.PoolError, pooling.MySQLConnectionPool)

        self.assertRaises(
            AttributeError,
            pooling.MySQLConnectionPool,
            pool_name="test",
            pool_size=-1,
        )
        self.assertRaises(
            AttributeError,
            pooling.MySQLConnectionPool,
            pool_name="test",
            pool_size=0,
        )
        self.assertRaises(
            AttributeError,
            pooling.MySQLConnectionPool,
            pool_name="test",
            pool_size=(pooling.CNX_POOL_MAXSIZE + 1),
        )

        cnxpool = pooling.MySQLConnectionPool(pool_name="test")
        self.assertEqual(5, cnxpool._pool_size)
        self.assertEqual("test", cnxpool._pool_name)
        self.assertEqual({}, cnxpool._cnx_config)
        self.assertTrue(isinstance(cnxpool._cnx_queue, Queue))
        self.assertTrue(isinstance(cnxpool._config_version, uuid.UUID))
        self.assertTrue(True, cnxpool._reset_session)

        cnxpool = pooling.MySQLConnectionPool(pool_size=10, pool_name="test")
        self.assertEqual(10, cnxpool._pool_size)

        cnxpool = pooling.MySQLConnectionPool(pool_size=10, **dbconfig)
        self.assertEqual(
            dbconfig,
            cnxpool._cnx_config,
            "Connection configuration not saved correctly",
        )
        self.assertEqual(10, cnxpool._cnx_queue.qsize())
        self.assertTrue(isinstance(cnxpool._config_version, uuid.UUID))

        cnxpool = pooling.MySQLConnectionPool(
            pool_size=1, pool_name="test", pool_reset_session=False
        )
        self.assertFalse(cnxpool._reset_session)

    def test_pool_name(self):
        """Test MySQLConnectionPool.pool_name property"""
        pool_name = "ham"
        cnxpool = pooling.MySQLConnectionPool(pool_name=pool_name)
        self.assertEqual(pool_name, cnxpool.pool_name)

    def test_pool_size(self):
        """Test MySQLConnectionPool.pool_size property"""
        pool_size = 4
        cnxpool = pooling.MySQLConnectionPool(pool_name="test", pool_size=pool_size)
        self.assertEqual(pool_size, cnxpool.pool_size)

    def test_reset_session(self):
        """Test MySQLConnectionPool.reset_session property"""
        cnxpool = pooling.MySQLConnectionPool(
            pool_name="test", pool_reset_session=False
        )
        self.assertFalse(cnxpool.reset_session)
        cnxpool._reset_session = True
        self.assertTrue(cnxpool.reset_session)

    def test__set_pool_size(self):
        cnxpool = pooling.MySQLConnectionPool(pool_name="test")
        self.assertRaises(AttributeError, cnxpool._set_pool_size, -1)
        self.assertRaises(AttributeError, cnxpool._set_pool_size, 0)
        self.assertRaises(
            AttributeError,
            cnxpool._set_pool_size,
            pooling.CNX_POOL_MAXSIZE + 1,
        )

        cnxpool._set_pool_size(pooling.CNX_POOL_MAXSIZE - 1)
        self.assertEqual(pooling.CNX_POOL_MAXSIZE - 1, cnxpool._pool_size)

    def test__set_pool_name(self):
        cnxpool = pooling.MySQLConnectionPool(pool_name="test")

        self.assertRaises(AttributeError, cnxpool._set_pool_name, "pool name")
        self.assertRaises(AttributeError, cnxpool._set_pool_name, "pool%%name")
        self.assertRaises(
            AttributeError,
            cnxpool._set_pool_name,
            "long_pool_name" * pooling.CNX_POOL_MAXNAMESIZE,
        )

    def test_add_connection(self):
        cnxpool = pooling.MySQLConnectionPool(pool_name="test")
        self.assertRaises(errors.PoolError, cnxpool.add_connection)

        dbconfig = tests.get_mysql_config()
        if tests.MYSQL_VERSION < (5, 7):
            dbconfig["client_flags"] = [-ClientFlag.CONNECT_ARGS]
        cnxpool = pooling.MySQLConnectionPool(pool_size=2, pool_name="test")
        cnxpool.set_config(**dbconfig)

        cnxpool.add_connection()
        pcnx = pooling.PooledMySQLConnection(
            cnxpool, cnxpool._cnx_queue.get(block=False)
        )
        self.assertTrue(isinstance(pcnx._cnx, MYSQL_CNX_CLASS))
        self.assertEqual(cnxpool, pcnx._cnx_pool)
        self.assertEqual(cnxpool._config_version, pcnx._cnx._pool_config_version)

        cnx = pcnx._cnx
        pcnx.close()
        # We should get the same connection back
        self.assertEqual(cnx, cnxpool._cnx_queue.get(block=False))
        cnxpool.add_connection(cnx)

        # reach max connections
        cnxpool.add_connection()
        self.assertRaises(errors.PoolError, cnxpool.add_connection)

        # fail connecting
        cnxpool._remove_connections()
        cnxpool._cnx_config["port"] = 9999999
        cnxpool._cnx_config["unix_socket"] = "/ham/spam/foobar.socket"
        self.assertRaises(errors.Error, cnxpool.add_connection)

        self.assertRaises(errors.PoolError, cnxpool.add_connection, cnx=str)

    def test_set_config(self):
        dbconfig = tests.get_mysql_config()
        if tests.MYSQL_VERSION < (5, 7):
            dbconfig["client_flags"] = [-ClientFlag.CONNECT_ARGS]
        cnxpool = pooling.MySQLConnectionPool(pool_name="test")

        # No configuration changes
        config_version = cnxpool._config_version
        cnxpool.set_config()
        self.assertEqual(config_version, cnxpool._config_version)
        self.assertEqual({}, cnxpool._cnx_config)

        # Valid configuration changes
        config_version = cnxpool._config_version
        cnxpool.set_config(**dbconfig)
        self.assertEqual(dbconfig, cnxpool._cnx_config)
        self.assertNotEqual(config_version, cnxpool._config_version)

        # Invalid configuration changes
        config_version = cnxpool._config_version
        wrong_dbconfig = dbconfig.copy()
        wrong_dbconfig["spam"] = "ham"
        self.assertRaises(errors.PoolError, cnxpool.set_config, **wrong_dbconfig)
        self.assertEqual(dbconfig, cnxpool._cnx_config)
        self.assertEqual(config_version, cnxpool._config_version)

    def test_get_connection(self):
        dbconfig = tests.get_mysql_config()
        if tests.MYSQL_VERSION < (5, 7):
            dbconfig["client_flags"] = [-ClientFlag.CONNECT_ARGS]
        cnxpool = pooling.MySQLConnectionPool(pool_size=2, pool_name="test")

        self.assertRaises(errors.PoolError, cnxpool.get_connection)

        cnxpool = pooling.MySQLConnectionPool(pool_size=1, **dbconfig)

        # Get connection from pool
        pcnx = cnxpool.get_connection()
        self.assertTrue(isinstance(pcnx, pooling.PooledMySQLConnection))
        self.assertRaises(errors.PoolError, cnxpool.get_connection)
        self.assertEqual(pcnx._cnx._pool_config_version, cnxpool._config_version)
        prev_config_version = pcnx._pool_config_version
        prev_thread_id = pcnx.connection_id
        pcnx.close()

        # Change configuration
        config_version = cnxpool._config_version
        cnxpool.set_config(autocommit=True)
        self.assertNotEqual(config_version, cnxpool._config_version)

        pcnx = cnxpool.get_connection()
        self.assertNotEqual(pcnx._cnx._pool_config_version, prev_config_version)
        self.assertNotEqual(prev_thread_id, pcnx.connection_id)
        self.assertEqual(1, pcnx.autocommit)
        pcnx.close()

        # Get connection from pool using a context manager
        with cnxpool.get_connection() as pcnx:
            self.assertTrue(isinstance(pcnx, pooling.PooledMySQLConnection))

    def test__remove_connections(self):
        dbconfig = tests.get_mysql_config()
        if tests.MYSQL_VERSION < (5, 7):
            dbconfig["client_flags"] = [-ClientFlag.CONNECT_ARGS]
        cnxpool = pooling.MySQLConnectionPool(pool_size=2, pool_name="test", **dbconfig)
        pcnx = cnxpool.get_connection()
        self.assertEqual(1, cnxpool._remove_connections())
        pcnx.close()
        self.assertEqual(1, cnxpool._remove_connections())
        self.assertEqual(0, cnxpool._remove_connections())

        self.assertRaises(errors.PoolError, cnxpool.get_connection)


class ModuleConnectorPoolingTests(tests.MySQLConnectorTests):

    """Testing MySQL Connector module pooling functionality"""

    def tearDown(self):
        mysql.connector.pooling._CONNECTION_POOLS = {}

    def test__connection_pools(self):
        self.assertEqual(mysql.connector.pooling._CONNECTION_POOLS, {})

    def test__get_pooled_connection(self):
        dbconfig = tests.get_mysql_config()
        mysql.connector.pooling._CONNECTION_POOLS.update({"spam": "ham"})
        self.assertRaises(
            errors.InterfaceError, mysql.connector.connect, pool_name="spam"
        )

        mysql.connector.pooling._CONNECTION_POOLS = {}

        mysql.connector.connect(pool_name="ham", **dbconfig)
        self.assertTrue("ham" in mysql.connector.pooling._CONNECTION_POOLS)
        cnxpool = mysql.connector.pooling._CONNECTION_POOLS["ham"]
        self.assertTrue(isinstance(cnxpool, pooling.MySQLConnectionPool))
        self.assertEqual("ham", cnxpool.pool_name)

        mysql.connector.connect(pool_size=5, **dbconfig)
        pool_name = pooling.generate_pool_name(**dbconfig)
        self.assertTrue(pool_name in mysql.connector.pooling._CONNECTION_POOLS)

    def test_connect_pure(self):
        dbconfig = tests.get_mysql_config()
        dbconfig["use_pure"] = True
        cnx = mysql.connector.connect(pool_size=1, pool_name="ham", **dbconfig)
        self.assertIsInstance(
            cnx._cnx,
            MySQLConnection,
            "{} type was expected".format(MySQLConnection),
        )
        exp = cnx.connection_id
        cnx.close()
        self.assertEqual(
            exp,
            mysql.connector.pooling._get_pooled_connection(
                pool_name="ham"
            ).connection_id,
        )

    @unittest.skipIf(not CMySQLConnection, "cext is required for this test")
    def test_connect_cext(self):
        config = tests.get_mysql_config()
        config["use_pure"] = False
        cnx = mysql.connector.connect(pool_size=1, pool_name="ham", **config)
        self.assertIsInstance(
            cnx._cnx,
            CMySQLConnection,
            "{} type was expected".format(CMySQLConnection),
        )
        exp = cnx.connection_id
        cnx.close()
        self.assertEqual(
            exp,
            mysql.connector.pooling._get_pooled_connection(
                pool_name="ham"
            ).connection_id,
        )

    @unittest.skipIf(CMySQLConnection, "Test requires cext to be absent")
    def test_connect_cext_error(self):
        config = tests.get_mysql_config()
        config["use_pure"] = False
        config["pool_size"] = 1
        config["pool_name"] = "ham"
        self.assertRaises(ImportError, mysql.connector.connect, **config)
