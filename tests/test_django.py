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

"""Unittests for mysql.connector.django
"""

import datetime
import unittest

import tests

# Load 3rd party _after_ loading tests
try:
    from django import VERSION as DJANGO_VERSION
    from django.conf import settings
except ImportError:
    DJANGO_AVAILABLE = False
    DJANGO_VERSION = (0, 0, 0)
else:
    DJANGO_AVAILABLE = True

# Have to setup Django before loading anything else
if DJANGO_AVAILABLE:
    try:
        settings.configure()
    except RuntimeError as exc:
        if "already configured" not in str(exc):
            raise
    DBCONFIG = tests.get_mysql_config()

    settings.DATABASES = {
        "default": {
            "ENGINE": "mysql.connector.django",
            "NAME": DBCONFIG["database"],
            "USER": "root",
            "PASSWORD": "",
            "HOST": DBCONFIG["host"],
            "PORT": DBCONFIG["port"],
            "TEST_CHARSET": "utf8",
            "TEST_COLLATION": "utf8_general_ci",
            "CONN_MAX_AGE": 0,
            "AUTOCOMMIT": True,
            "TIME_ZONE": None,
        },
    }
    settings.SECRET_KEY = "django_tests_secret_key"
    settings.TIME_ZONE = "UTC"
    settings.USE_TZ = False
    settings.SOUTH_TESTS_MIGRATE = False
    settings.DEBUG = False

TABLES = {}
TABLES[
    "django_t1"
] = """
CREATE TABLE {table_name} (
id INT NOT NULL AUTO_INCREMENT,
c1 INT,
c2 VARCHAR(20),
INDEX (c1),
UNIQUE INDEX (c2),
PRIMARY KEY (id)
) ENGINE=InnoDB
"""

TABLES[
    "django_t2"
] = """
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

from django.core.exceptions import ImproperlyConfigured
from django.db import connection
from django.db.backends.signals import connection_created
from django.db.utils import DEFAULT_DB_ALIAS, load_backend
from django.utils.safestring import SafeText

import mysql.connector

from mysql.connector.conversion import MySQLConverter
from mysql.connector.django.introspection import FieldInfo
from mysql.connector.errors import ProgrammingError

if DJANGO_AVAILABLE:
    from mysql.connector.django.base import (
        DatabaseOperations,
        DatabaseWrapper,
        DjangoMySQLConverter,
    )
    from mysql.connector.django.introspection import DatabaseIntrospection


@unittest.skipIf(not DJANGO_AVAILABLE, "Django not available")
class DjangoSettings(tests.MySQLConnectorTests):
    """Test the Django settings."""

    def test_get_connection_params(self):
        config = tests.get_mysql_config()
        settings_dict = connection.settings_dict.copy()

        # The default isolation_level should be None
        database_wrapper = DatabaseWrapper(settings_dict)
        self.assertIsNone(database_wrapper.isolation_level)

        # An invalid isolation_level should raise ImproperlyConfigured
        settings_dict["OPTIONS"]["isolation_level"] = "invalid_level"
        with self.assertRaises(ImproperlyConfigured):
            _ = DatabaseWrapper(settings_dict).get_connection_params()

        # Test a valid isolation_level
        settings_dict["OPTIONS"]["isolation_level"] = "read committed"
        database_wrapper = DatabaseWrapper(settings_dict)
        connection_params = database_wrapper.get_connection_params()
        self.assertEqual(database_wrapper.isolation_level, "read committed")
        self.assertEqual(connection_params["database"], config["database"])

        # Test session isolation level integration
        with database_wrapper.cursor() as cur:
            cur.execute("SELECT @@transaction_isolation")
            res = cur.fetchall()
            self.assertEqual(res[0][0], "READ-COMMITTED")


@unittest.skipIf(not DJANGO_AVAILABLE, "Django not available")
class DjangoIntrospection(tests.MySQLConnectorTests):
    """Test the Django introspection module"""

    cnx = None
    introspect = None

    @classmethod
    def setUpClass(cls):
        cls.cnx = DatabaseWrapper(settings.DATABASES["default"])
        cls.introspect = DatabaseIntrospection(cls.cnx)

        cur = cls.cnx.cursor()

        for table_name, sql in TABLES.items():
            cur.execute("SET foreign_key_checks = 0")
            cur.execute(
                "DROP TABLE IF EXISTS {table_name}".format(table_name=table_name)
            )
            cur.execute(sql.format(table_name=table_name))
        cur.execute("SET foreign_key_checks = 1")

    @classmethod
    def tearDownClass(cls):
        cur = cls.cnx.cursor()
        cur.execute("SET foreign_key_checks = 0")
        for table_name, sql in TABLES.items():
            cur.execute(
                "DROP TABLE IF EXISTS {table_name}".format(table_name=table_name)
            )
        cur.execute("SET foreign_key_checks = 1")

    def test_get_table_list(self):
        cur = self.cnx.cursor()
        for exp in TABLES.keys():
            res = any(
                table.name == exp for table in self.introspect.get_table_list(cur)
            )
            self.assertTrue(
                res,
                "Table {table_name} not in table list".format(table_name=exp),
            )

    def test_get_table_description(self):
        cur = self.cnx.cursor()
        if DJANGO_VERSION < (3, 2, 0):
            exp = [
                FieldInfo(
                    name="id",
                    type_code=3,
                    display_size=None,
                    internal_size=None,
                    precision=10,
                    scale=None,
                    null_ok=0,
                    default=None,
                    extra="auto_increment",
                    is_unsigned=0,
                    has_json_constraint=False,
                ),
                FieldInfo(
                    name="c1",
                    type_code=3,
                    display_size=None,
                    internal_size=None,
                    precision=10,
                    scale=None,
                    null_ok=1,
                    default=None,
                    extra="",
                    is_unsigned=0,
                    has_json_constraint=False,
                ),
                FieldInfo(
                    name="c2",
                    type_code=253,
                    display_size=None,
                    internal_size=20,
                    precision=None,
                    scale=None,
                    null_ok=1,
                    default=None,
                    extra="",
                    is_unsigned=0,
                    has_json_constraint=False,
                ),
            ]
        else:
            exp = [
                FieldInfo(
                    name="id",
                    type_code=3,
                    display_size=None,
                    internal_size=None,
                    precision=10,
                    scale=None,
                    null_ok=0,
                    default=None,
                    collation=None,
                    extra="auto_increment",
                    is_unsigned=0,
                    has_json_constraint=False,
                ),
                FieldInfo(
                    name="c1",
                    type_code=3,
                    display_size=None,
                    internal_size=None,
                    precision=10,
                    scale=None,
                    null_ok=1,
                    default=None,
                    collation=None,
                    extra="",
                    is_unsigned=0,
                    has_json_constraint=False,
                ),
                FieldInfo(
                    name="c2",
                    type_code=253,
                    display_size=None,
                    internal_size=20,
                    precision=None,
                    scale=None,
                    null_ok=1,
                    default=None,
                    collation=None,
                    extra="",
                    is_unsigned=0,
                    has_json_constraint=False,
                ),
            ]
        res = self.introspect.get_table_description(cur, "django_t1")
        self.assertEqual(exp, res)

    def test_get_relations(self):
        cur = self.cnx.cursor()
        exp = {"id_t1": ("id", "django_t1")}
        self.assertEqual(exp, self.introspect.get_relations(cur, "django_t2"))

    def test_get_key_columns(self):
        cur = self.cnx.cursor()
        exp = [("id_t1", "django_t1", "id")]
        self.assertEqual(exp, self.introspect.get_key_columns(cur, "django_t2"))

    def test_get_indexes(self):
        cur = self.cnx.cursor()
        exp = {
            "c1": {"primary_key": False, "unique": False},
            "id": {"primary_key": True, "unique": True},
            "c2": {"primary_key": False, "unique": True},
        }
        self.assertEqual(exp, self.introspect.get_indexes(cur, "django_t1"))

    def test_get_primary_key_column(self):
        cur = self.cnx.cursor()
        res = self.introspect.get_primary_key_column(cur, "django_t1")
        self.assertEqual("id", res)

    def test_get_constraints(self):
        cur = self.cnx.cursor()
        exp = {
            "PRIMARY": {
                "check": False,
                "columns": ["id"],
                "foreign_key": None,
                "index": True,
                "primary_key": True,
                "type": "idx",
                "unique": True,
            },
            "django_t2_ibfk_1": {
                "check": False,
                "columns": ["id_t1"],
                "foreign_key": ("django_t1", "id"),
                "index": False,
                "primary_key": False,
                "unique": False,
            },
            "id_t1": {
                "check": False,
                "columns": ["id_t1"],
                "foreign_key": None,
                "index": True,
                "primary_key": False,
                "type": "idx",
                "unique": False,
            },
        }
        if DJANGO_VERSION >= (3, 2, 0):
            exp["PRIMARY"]["orders"] = ["ASC"]
            exp["django_t2_ibfk_1"]["orders"] = []
            exp["id_t1"]["orders"] = ["ASC"]
        self.assertEqual(exp, self.introspect.get_constraints(cur, "django_t2"))


@unittest.skipIf(not DJANGO_AVAILABLE, "Django not available")
class DjangoDatabaseWrapper(tests.MySQLConnectorTests):
    """Test the Django base.DatabaseWrapper class"""

    def setUp(self):
        dbconfig = tests.get_mysql_config()
        self.conn = mysql.connector.connect(**dbconfig)
        self.cnx = DatabaseWrapper(settings.DATABASES["default"])

    def test__init__(self):
        exp = self.conn.get_server_version()
        self.assertEqual(exp, self.cnx.mysql_version)

    def test_signal(self):
        from django.db import connection

        def conn_setup(*args, **kwargs):
            conn = kwargs["connection"]
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
        conn = DatabaseWrapper(settings.DATABASES["default"])
        dbo = DatabaseOperations(conn)
        dbo.adapt_timefield_value(datetime.time(3, 3, 3))
        self.assertEqual(self.connections, 0)


class DjangoDatabaseOperations(tests.MySQLConnectorTests):
    """Test the Django base.DatabaseOperations class"""

    def setUp(self):
        dbconfig = tests.get_mysql_config()
        dbconfig["use_pure"] = True
        self.conn = mysql.connector.connect(**dbconfig)
        self.cnx = DatabaseWrapper(settings.DATABASES["default"])
        self.dbo = DatabaseOperations(self.cnx)

    def test_value_to_db_time(self):
        value_to_db_time = self.dbo.adapt_timefield_value
        self.assertEqual(None, value_to_db_time(None))

        value = datetime.time(0, 0, 0)
        exp = self.conn.converter._time_to_mysql(value)
        self.assertEqual(exp, value_to_db_time(value))

        value = datetime.time(2, 5, 7)
        exp = self.conn.converter._time_to_mysql(value)
        self.assertEqual(exp, value_to_db_time(value))

    def test_value_to_db_datetime(self):
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
        exp = "VALUES {0}".format(
            ", ".join(["({0})".format(", ".join(["%s"] * len(fields)))] * num_values)
        )
        self.assertEqual(exp, self.dbo.bulk_insert_sql(fields, placeholder_rows))


class DjangoMySQLConverterTests(tests.MySQLConnectorTests):
    """Test the Django base.DjangoMySQLConverter class"""

    def test__time_to_python(self):
        value = b"10:11:12"
        django_converter = DjangoMySQLConverter()
        self.assertEqual(
            datetime.time(10, 11, 12),
            django_converter._time_to_python(value, dsc=None),
        )

    def test__datetime_to_python(self):
        value = b"1990-11-12 00:00:00"
        django_converter = DjangoMySQLConverter()
        self.assertEqual(
            datetime.datetime(1990, 11, 12, 0, 0, 0),
            django_converter._datetime_to_python(value, dsc=None),
        )

        settings.USE_TZ = True
        value = b"0000-00-00 00:00:00"
        django_converter = DjangoMySQLConverter()
        self.assertEqual(None, django_converter._datetime_to_python(value, dsc=None))
        settings.USE_TZ = False


class CustomDjangoMySQLConverter(DjangoMySQLConverter):
    """A custom Django MySQL converter."""

    def _customtype_to_python(self, value):
        try:
            return int(value)
        except ValueError:
            raise ValueError("Invalid value for customtype conversion")


class CustomDjangoMySQLConverterTests(tests.MySQLConnectorTests):
    """Test the Django custom MySQL converter class."""

    @staticmethod
    def create_connection(alias=DEFAULT_DB_ALIAS):
        django.db.connections.ensure_defaults(alias)
        django.db.connections.prepare_test_settings(alias)
        db = django.db.connections.databases[alias]
        backend = load_backend(db["ENGINE"])
        return backend.DatabaseWrapper(db, alias)

    def test__time_to_python(self):
        value = b"10:11:12"
        django_converter = CustomDjangoMySQLConverter()
        self.assertEqual(
            datetime.time(10, 11, 12),
            django_converter._time_to_python(value, dsc=None),
        )

    def test__datetime_to_python(self):
        value = b"1990-11-12 00:00:00"
        django_converter = CustomDjangoMySQLConverter()
        self.assertEqual(
            datetime.datetime(1990, 11, 12, 0, 0, 0),
            django_converter._datetime_to_python(value, dsc=None),
        )

        settings.USE_TZ = True
        value = b"0000-00-00 00:00:00"
        django_converter = DjangoMySQLConverter()
        self.assertEqual(
            None,
            django_converter._datetime_to_python(value, dsc=None),
        )
        settings.USE_TZ = False

    def test__customtype_to_python(self):
        value = b"2021"
        django_converter = CustomDjangoMySQLConverter()
        self.assertEqual(
            2021,
            django_converter._customtype_to_python(value),
        )

    def test_invalid__customtype_to_python(self):
        value = b"abc"
        django_converter = CustomDjangoMySQLConverter()
        self.assertRaises(
            ValueError,
            django_converter._customtype_to_python,
            value,
        )

    def test_invalid_converter_class(self):
        settings.DATABASES["default"]["OPTIONS"] = {
            "converter_class": MySQLConverter,
        }
        self.assertRaises(ProgrammingError, self.create_connection)
        del settings.DATABASES["default"]["OPTIONS"]

    def test_converter_class(self):
        settings.DATABASES["default"]["OPTIONS"] = {
            "converter_class": CustomDjangoMySQLConverter,
        }
        cnx = self.create_connection()
        cnx.close()
        del settings.DATABASES["default"]["OPTIONS"]


class BugOra20106629(tests.MySQLConnectorTests):
    """CONNECTOR/PYTHON DJANGO BACKEND DOESN'T SUPPORT SAFETEXT"""

    def setUp(self):
        dbconfig = tests.get_mysql_config()
        self.conn = mysql.connector.connect(**dbconfig)
        self.cnx = DatabaseWrapper(settings.DATABASES["default"])
        self.cur = self.cnx.cursor()
        self.tbl = "BugOra20106629"
        self.cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl), ())
        self.cur.execute("CREATE TABLE {0}(col1 TEXT, col2 BLOB)".format(self.tbl), ())

    def tearDown(self):
        self.cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl), ())

    def test_safe_string(self):
        safe_text = SafeText("dummy & safe data <html> ")
        safe_bytes = SafeText("dummy & safe data <html> ")
        self.cur.execute(
            "INSERT INTO {0} VALUES(%s, %s)".format(self.tbl),
            (safe_text, safe_bytes),
        )
        self.cur.execute("SELECT * FROM {0}".format(self.tbl), ())
        self.assertEqual(self.cur.fetchall(), [(safe_text, safe_bytes.encode())])
