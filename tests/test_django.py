# Copyright (c) 2014, 2017, Oracle and/or its affiliates. All rights reserved.
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

"""Unittests for mysql.connector.django
"""

import datetime
import unittest
import sys
import unittest

import tests

# Load 3rd party _after_ loading tests
try:
    from django.conf import settings
except ImportError:
    DJANGO_AVAILABLE = False
else:
    DJANGO_AVAILABLE = True

# Have to setup Django before loading anything else
if DJANGO_AVAILABLE:
    try:
        settings.configure()
    except RuntimeError as exc:
        if not 'already configured' in str(exc):
            raise
    DBCONFIG = tests.get_mysql_config()

    settings.DATABASES = {
        'default': {
            'ENGINE': 'mysql.connector.django',
            'NAME': DBCONFIG['database'],
            'USER': 'root',
            'PASSWORD': '',
            'HOST': DBCONFIG['host'],
            'PORT': DBCONFIG['port'],
            'TEST_CHARSET': 'utf8',
            'TEST_COLLATION': 'utf8_general_ci',
            'CONN_MAX_AGE': 0,
            'AUTOCOMMIT': True,
            'TIME_ZONE': None,
        },
    }
    settings.SECRET_KEY = "django_tests_secret_key"
    settings.TIME_ZONE = 'UTC'
    settings.USE_TZ = False
    settings.SOUTH_TESTS_MIGRATE = False
    settings.DEBUG = False

TABLES = {}
TABLES['django_t1'] = """
CREATE TABLE {table_name} (
id INT NOT NULL AUTO_INCREMENT,
c1 INT,
c2 VARCHAR(20),
INDEX (c1),
UNIQUE INDEX (c2),
PRIMARY KEY (id)
) ENGINE=InnoDB
"""

TABLES['django_t2'] = """
CREATE TABLE {table_name} (
id INT NOT NULL AUTO_INCREMENT,
id_t1 INT NOT NULL,
INDEX (id_t1),
PRIMARY KEY (id),
FOREIGN KEY (id_t1) REFERENCES django_t1(id) ON DELETE CASCADE
) ENGINE=InnoDB
"""

# Have to load django.db to make importing db backend work for Django < 1.6
import django.db  # pylint: disable=W0611
from django.db.backends.signals import connection_created
from django.utils.safestring import SafeBytes, SafeText

import mysql.connector
from mysql.connector.django.introspection import FieldInfo

if DJANGO_AVAILABLE:
    from mysql.connector.django.base import (
        DatabaseWrapper, DatabaseOperations, DjangoMySQLConverter)
    from mysql.connector.django.introspection import DatabaseIntrospection


@unittest.skipIf(not DJANGO_AVAILABLE, "Django not available")
class DjangoIntrospection(tests.MySQLConnectorTests):

    """Test the Django introspection module"""

    cnx = None
    introspect = None

    def setUp(self):
        # Python 2.6 has no setUpClass, we run it here, once.
        if sys.version_info < (2, 7) and not self.__class__.cnx:
            self.__class__.setUpClass()

    @classmethod
    def setUpClass(cls):
        dbconfig = tests.get_mysql_config()
        cls.cnx = DatabaseWrapper(settings.DATABASES['default'])
        cls.introspect = DatabaseIntrospection(cls.cnx)

        cur = cls.cnx.cursor()

        for table_name, sql in TABLES.items():
            cur.execute("SET foreign_key_checks = 0")
            cur.execute("DROP TABLE IF EXISTS {table_name}".format(
                table_name=table_name))
            cur.execute(sql.format(table_name=table_name))
        cur.execute("SET foreign_key_checks = 1")

    @classmethod
    def tearDownClass(cls):
        cur = cls.cnx.cursor()
        cur.execute("SET foreign_key_checks = 0")
        for table_name, sql in TABLES.items():
            cur.execute("DROP TABLE IF EXISTS {table_name}".format(
                table_name=table_name))
        cur.execute("SET foreign_key_checks = 1")

    def test_get_table_list(self):
        cur = self.cnx.cursor()
        for exp in TABLES.keys():
            if sys.version_info < (2, 7):
                self.assertTrue(exp in self.introspect.get_table_list(cur))
            else:
                if tests.DJANGO_VERSION < (1, 8):
                    res = any(table == exp
                              for table in self.introspect.get_table_list(cur))
                else:
                    res = any(table.name == exp
                              for table in self.introspect.get_table_list(cur))
                self.assertTrue(res, "Table {table_name} not in table list"
                                     "".format(table_name=exp))

    def test_get_table_description(self):
        cur = self.cnx.cursor()

        if tests.DJANGO_VERSION < (1, 6):
            exp = [
                ('id', 3, None, None, None, None, 0, 16899),
                ('c1', 3, None, None, None, None, 1, 16392),
                ('c2', 253, None, 20, None, None, 1, 16388)
            ]
        elif tests.DJANGO_VERSION < (1, 8):
            exp = [
                FieldInfo(name=u'id', type_code=3, display_size=None,
                          internal_size=None, precision=None, scale=None,
                          null_ok=0),
                FieldInfo(name=u'c1', type_code=3, display_size=None,
                          internal_size=None, precision=None, scale=None,
                          null_ok=1),
                FieldInfo(name=u'c2', type_code=253, display_size=None,
                          internal_size=20, precision=None, scale=None,
                          null_ok=1)
            ]
        elif tests.DJANGO_VERSION < (1, 11):
            exp = [
                FieldInfo(name=u'id', type_code=3, display_size=None,
                          internal_size=None, precision=10, scale=None,
                          null_ok=0, extra=u'auto_increment'),
                FieldInfo(name=u'c1', type_code=3, display_size=None,
                          internal_size=None, precision=10, scale=None,
                          null_ok=1, extra=u''),
                FieldInfo(name=u'c2', type_code=253, display_size=None,
                          internal_size=20, precision=None, scale=None,
                          null_ok=1, extra=u'')
            ]
        else:
            exp = [
                FieldInfo(name=u'id', type_code=3, display_size=None,
                          internal_size=None, precision=10, scale=None,
                          null_ok=0, default=None, extra=u'auto_increment'),
                FieldInfo(name=u'c1', type_code=3, display_size=None,
                          internal_size=None, precision=10, scale=None,
                          null_ok=1, default=None, extra=u''),
                FieldInfo(name=u'c2', type_code=253, display_size=None,
                          internal_size=20, precision=None, scale=None,
                          null_ok=1, default=None, extra=u'')
            ]
        res = self.introspect.get_table_description(cur, 'django_t1')
        self.assertEqual(exp, res)

    def test_get_relations(self):
        cur = self.cnx.cursor()
        if tests.DJANGO_VERSION < (1, 8):
            exp = {1: (0, 'django_t1')}
        else:
            exp = {u'id_t1': (u'id', u'django_t1')}
        self.assertEqual(exp, self.introspect.get_relations(cur, 'django_t2'))

    def test_get_key_columns(self):
        cur = self.cnx.cursor()
        exp = [('id_t1', 'django_t1', 'id')]
        self.assertEqual(exp, self.introspect.get_key_columns(cur, 'django_t2'))

    def test_get_indexes(self):
        cur = self.cnx.cursor()
        exp = {
            'c1': {'primary_key': False, 'unique': False},
            'id': {'primary_key': True, 'unique': True},
            'c2': {'primary_key': False, 'unique': True}
        }
        self.assertEqual(exp, self.introspect.get_indexes(cur, 'django_t1'))

    def test_get_primary_key_column(self):
        cur = self.cnx.cursor()
        res = self.introspect.get_primary_key_column(cur, 'django_t1')
        self.assertEqual('id', res)

    def test_get_constraints(self):
        cur = self.cnx.cursor()
        exp = {
            'PRIMARY': {'check': False,
                        'columns': ['id'],
                        'foreign_key': None,
                        'index': True,
                        'primary_key': True,
                        'unique': True},
            'django_t2_ibfk_1': {'check': False,
                                 'columns': ['id_t1'],
                                 'foreign_key': ('django_t1', 'id'),
                                 'index': False,
                                 'primary_key': False,
                                 'unique': False},
            'id_t1': {'check': False,
                      'columns': ['id_t1'],
                      'foreign_key': None,
                      'index': True,
                      'primary_key': False,
                      'unique': False}
        }
        self.assertEqual(
            exp, self.introspect.get_constraints(cur, 'django_t2'))

@unittest.skipIf(not DJANGO_AVAILABLE, "Django not available")
class DjangoDatabaseWrapper(tests.MySQLConnectorTests):

    """Test the Django base.DatabaseWrapper class"""

    def setUp(self):
        dbconfig = tests.get_mysql_config()
        self.conn = mysql.connector.connect(**dbconfig)
        self.cnx = DatabaseWrapper(settings.DATABASES['default'])

    def test__init__(self):
        exp = self.conn.get_server_version()
        self.assertEqual(exp, self.cnx.mysql_version)

        value = datetime.time(2, 5, 7)
        exp = self.conn.converter._time_to_mysql(value)
        self.assertEqual(exp, self.cnx.ops.value_to_db_time(value))

        self.cnx.connection = None
        value = datetime.time(2, 5, 7)
        exp = self.conn.converter._time_to_mysql(value)
        self.assertEqual(exp, self.cnx.ops.value_to_db_time(value))



    def test_signal(self):
        from django.db import connection

        def conn_setup(*args, **kwargs):
            conn = kwargs['connection']
            settings.DEBUG = True
            cur = conn.cursor()
            settings.DEBUG = False
            cur.execute("SET @xyz=10")
            cur.close()

        connection_created.connect(conn_setup)
        cursor = connection.cursor()
        cursor.execute("SELECT @xyz")

        self.assertEqual((10,), cursor.fetchone())
        cursor.close()
        self.cnx.close()

    def count_conn(self, *args, **kwargs):
        try:
            self.connections += 1
        except AttributeError:
            self.connection = 1

    def test_connections(self):
        connection_created.connect(self.count_conn)
        self.connections = 0

        # Checking if DatabaseWrapper object creates a connection by default
        conn = DatabaseWrapper(settings.DATABASES['default'])
        dbo = DatabaseOperations(conn)
        dbo.value_to_db_time(datetime.time(3, 3, 3))
        self.assertEqual(self.connections, 0)


class DjangoDatabaseOperations(tests.MySQLConnectorTests):

    """Test the Django base.DatabaseOperations class"""

    def setUp(self):
        dbconfig = tests.get_mysql_config()
        self.conn = mysql.connector.connect(**dbconfig)
        self.cnx = DatabaseWrapper(settings.DATABASES['default'])
        self.dbo = DatabaseOperations(self.cnx)

    def test_value_to_db_time(self):
        if tests.DJANGO_VERSION < (1, 9):
            value_to_db_time = self.dbo.value_to_db_time
        else:
            value_to_db_time = self.dbo.adapt_timefield_value

        self.assertEqual(None, value_to_db_time(None))

        value = datetime.time(0, 0, 0)
        exp = self.conn.converter._time_to_mysql(value)
        self.assertEqual(exp, value_to_db_time(value))

        value = datetime.time(2, 5, 7)
        exp = self.conn.converter._time_to_mysql(value)
        self.assertEqual(exp, value_to_db_time(value))

    def test_value_to_db_datetime(self):
        if tests.DJANGO_VERSION < (1, 9):
            value_to_db_datetime = self.dbo.value_to_db_datetime
        else:
            value_to_db_datetime = self.dbo.adapt_datetimefield_value

        self.assertEqual(None, value_to_db_datetime(None))

        value = datetime.datetime(1, 1, 1)
        exp = self.conn.converter._datetime_to_mysql(value)
        self.assertEqual(exp, value_to_db_datetime(value))

        value = datetime.datetime(2, 5, 7, 10, 10)
        exp = self.conn.converter._datetime_to_mysql(value)
        self.assertEqual(exp, value_to_db_datetime(value))

    def test_bulk_insert_sql(self):
        num_values = 5
        fields = ["col1", "col2", "col3"]
        placeholder_rows = [["%s"] * len(fields) for _ in range(num_values)]
        exp = "VALUES {0}".format(", ".join(
            ["({0})".format(", ".join(["%s"] * len(fields)))] * num_values))
        if tests.DJANGO_VERSION < (1, 9):
            self.assertEqual(
                exp, self.dbo.bulk_insert_sql(fields, num_values))
        else:
            self.assertEqual(
                exp, self.dbo.bulk_insert_sql(fields, placeholder_rows))


class DjangoMySQLConverterTests(tests.MySQLConnectorTests):
    """Test the Django base.DjangoMySQLConverter class"""
    def test__TIME_to_python(self):
        value = b'10:11:12'
        django_converter = DjangoMySQLConverter()
        self.assertEqual(datetime.time(10, 11, 12),
                         django_converter._TIME_to_python(value, dsc=None))

    def test__DATETIME_to_python(self):
        value = b'1990-11-12 00:00:00'
        django_converter = DjangoMySQLConverter()
        self.assertEqual(datetime.datetime(1990, 11, 12, 0, 0, 0),
                         django_converter._DATETIME_to_python(value, dsc=None))

        settings.USE_TZ = True
        value = b'0000-00-00 00:00:00'
        django_converter = DjangoMySQLConverter()
        self.assertEqual(None,
                         django_converter._DATETIME_to_python(value, dsc=None))
        settings.USE_TZ = False


class BugOra20106629(tests.MySQLConnectorTests):
    """CONNECTOR/PYTHON DJANGO BACKEND DOESN'T SUPPORT SAFETEXT"""
    def setUp(self):
        dbconfig = tests.get_mysql_config()
        self.conn = mysql.connector.connect(**dbconfig)
        self.cnx = DatabaseWrapper(settings.DATABASES['default'])
        self.cur = self.cnx.cursor()
        self.tbl = "BugOra20106629"
        self.cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl), ())
        self.cur.execute("CREATE TABLE {0}(col1 TEXT, col2 BLOB)".format(self.tbl), ())

    def teardown(self):
        self.cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl), ())

    def test_safe_string(self):
        safe_text = SafeText("dummy & safe data <html> ")
        safe_bytes = SafeBytes(b"\x00\x00\x4c\x6e\x67\x39")
        self.cur.execute("INSERT INTO {0} VALUES(%s, %s)".format(self.tbl), (safe_text, safe_bytes))
        self.cur.execute("SELECT * FROM {0}".format(self.tbl), ())
        self.assertEqual(self.cur.fetchall(), [(safe_text, safe_bytes)])
