# MySQL Connector/Python - MySQL driver written in Python.

"""Django database Backend using MySQL Connector/Python

This Django database backend is heavily based on the MySQL backend coming
with Django.

Changes include:
* Support for microseconds (MySQL 5.6.3 and later)
* Using INFORMATION_SCHEMA where possible
* Using new defaults for, for example SQL_AUTO_IS_NULL

Requires and comes with MySQL Connector/Python v1.1 and later:
    http://dev.mysql.com/downloads/connector/python/
"""


from __future__ import unicode_literals

import sys

import django
from django.utils.functional import cached_property

try:
    import mysql.connector
    from mysql.connector.conversion import MySQLConverter
except ImportError as err:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured(
        "Error loading mysql.connector module: {0}".format(err))

try:
    version = mysql.connector.__version_info__[0:3]
except AttributeError:
    from mysql.connector.version import VERSION
    version = VERSION[0:3]

if version < (1, 1):
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured(
        "MySQL Connector/Python v1.1.0 or newer "
        "is required; you have %s" % mysql.connector.__version__)

from django.db import utils
if django.VERSION < (1, 7):
    from django.db.backends import util
else:
    from django.db.backends import utils as backend_utils
from django.db.backends import (BaseDatabaseFeatures, BaseDatabaseOperations,
                                BaseDatabaseWrapper)
from django.db.backends.signals import connection_created
from django.utils import (six, timezone, dateparse)
from django.conf import settings

from mysql.connector.django.client import DatabaseClient
from mysql.connector.django.creation import DatabaseCreation
from mysql.connector.django.introspection import DatabaseIntrospection
from mysql.connector.django.validation import DatabaseValidation
if django.VERSION >= (1, 7):
    from mysql.connector.django.schema import DatabaseSchemaEditor

try:
    import pytz
    HAVE_PYTZ = True
except ImportError:
    HAVE_PYTZ = False

DatabaseError = mysql.connector.DatabaseError
IntegrityError = mysql.connector.IntegrityError
NotSupportedError = mysql.connector.NotSupportedError


class DjangoMySQLConverter(MySQLConverter):
    """Custom converter for Django"""
    def _TIME_to_python(self, value, dsc=None):
        """Return MySQL TIME data type as datetime.time()

        Returns datetime.time()
        """
        return dateparse.parse_time(value.decode('utf-8'))

    def _DATETIME_to_python(self, value, dsc=None):
        """Connector/Python always returns naive datetime.datetime

        Connector/Python always returns naive timestamps since MySQL has
        no time zone support. Since Django needs non-naive, we need to add
        the UTC time zone.

        Returns datetime.datetime()
        """
        if not value:
            return None
        dt = MySQLConverter._DATETIME_to_python(self, value)
        if dt is None:
            return None
        if settings.USE_TZ and timezone.is_naive(dt):
            dt = dt.replace(tzinfo=timezone.utc)
        return dt


class CursorWrapper(object):
    """Wrapper around MySQL Connector/Python's cursor class.

    The cursor class is defined by the options passed to MySQL
    Connector/Python. If buffered option is True in those options,
    MySQLCursorBuffered will be used.
    """
    def __init__(self, cursor):
        self.cursor = cursor

    def _execute_wrapper(self, method, query, args):
        """Wrapper around execute() and executemany()"""
        try:
            return method(query, args)
        except (mysql.connector.ProgrammingError) as err:
            six.reraise(utils.ProgrammingError,
                        utils.ProgrammingError(err.msg), sys.exc_info()[2])
        except (mysql.connector.IntegrityError) as err:
            six.reraise(utils.IntegrityError,
                        utils.IntegrityError(err.msg), sys.exc_info()[2])
        except mysql.connector.OperationalError as err:
            six.reraise(utils.DatabaseError,
                        utils.DatabaseError(err.msg), sys.exc_info()[2])
        except mysql.connector.DatabaseError as err:
            six.reraise(utils.DatabaseError,
                        utils.DatabaseError(err.msg), sys.exc_info()[2])

    def execute(self, query, args=None):
        """Executes the given operation

        This wrapper method around the execute()-method of the cursor is
        mainly needed to re-raise using different exceptions.
        """
        return self._execute_wrapper(self.cursor.execute, query, args)

    def executemany(self, query, args):
        """Executes the given operation

        This wrapper method around the executemany()-method of the cursor is
        mainly needed to re-raise using different exceptions.
        """
        return self._execute_wrapper(self.cursor.executemany, query, args)

    def __getattr__(self, attr):
        """Return attribute of wrapped cursor"""
        return getattr(self.cursor, attr)

    def __iter__(self):
        """Returns iterator over wrapped cursor"""
        return iter(self.cursor)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()


class DatabaseFeatures(BaseDatabaseFeatures):
    """Features specific to MySQL

    Microsecond precision is supported since MySQL 5.6.3 and turned on
    by default.
    """
    empty_fetchmany_value = []
    update_can_self_select = False
    allows_group_by_pk = True
    related_fields_match_type = True
    allow_sliced_subqueries = False
    has_bulk_insert = True
    has_select_for_update = True
    has_select_for_update_nowait = False
    supports_forward_references = False
    supports_long_model_names = False
    supports_binary_field = six.PY2
    supports_microsecond_precision = False  # toggled in __init__()
    supports_regex_backreferencing = False
    supports_date_lookup_using_string = False
    can_introspect_binary_field = False
    can_introspect_boolean_field = False
    supports_timezones = False
    requires_explicit_null_ordering_when_grouping = True
    allows_auto_pk_0 = False
    allows_primary_key_0 = False
    uses_savepoints = True
    atomic_transactions = False
    supports_column_check_constraints = False

    def __init__(self, connection):
        super(DatabaseFeatures, self).__init__(connection)
        self.supports_microsecond_precision = self._microseconds_precision()

    def _microseconds_precision(self):
        if self.connection.mysql_version >= (5, 6, 3):
            return True
        return False

    @cached_property
    def _mysql_storage_engine(self):
        """Get default storage engine of MySQL

        This method creates a table without ENGINE table option and inspects
        which engine was used.

        Used by Django tests.
        """
        tblname = 'INTROSPECT_TEST'

        droptable = 'DROP TABLE IF EXISTS {table}'.format(table=tblname)
        with self.connection.cursor() as cursor:
            cursor.execute(droptable)
            cursor.execute('CREATE TABLE {table} (X INT)'.format(table=tblname))

            if self.connection.mysql_version >= (5, 0, 0):
                cursor.execute(
                    "SELECT ENGINE FROM INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                    (self.connection.settings_dict['NAME'], tblname))
                engine = cursor.fetchone()[0]
            else:
                # Very old MySQL servers..
                cursor.execute("SHOW TABLE STATUS WHERE Name='{table}'".format(
                    table=tblname))
                engine = cursor.fetchone()[1]
            cursor.execute(droptable)

        self._cached_storage_engine = engine
        return engine

    @cached_property
    def can_introspect_foreign_keys(self):
        """Confirm support for introspected foreign keys

        Only the InnoDB storage engine supports Foreigen Key (not taking
        into account MySQL Cluster here).
        """
        return self._mysql_storage_engine == 'InnoDB'

    @cached_property
    def has_zoneinfo_database(self):
        """Tests if the time zone definitions are installed

        MySQL accepts full time zones names (eg. Africa/Nairobi) but rejects
        abbreviations (eg. EAT). When pytz isn't installed and the current
        time zone is LocalTimezone (the only sensible value in this context),
        the current time zone name will be an abbreviation. As a consequence,
        MySQL cannot perform time zone conversions reliably.
        """
        # Django 1.6
        if not HAVE_PYTZ:
            return False

        with self.connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM mysql.time_zone LIMIT 1")
            return cursor.fetchall() != []


class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = "mysql.connector.django.compiler"

    # MySQL stores positive fields as UNSIGNED ints.
    if django.VERSION >= (1, 7):
        integer_field_ranges = dict(BaseDatabaseOperations.integer_field_ranges,
                                    PositiveSmallIntegerField=(0, 4294967295),
                                    PositiveIntegerField=(
                                        0, 18446744073709551615),)

    def date_extract_sql(self, lookup_type, field_name):
        # http://dev.mysql.com/doc/mysql/en/date-and-time-functions.html
        if lookup_type == 'week_day':
            # DAYOFWEEK() returns an integer, 1-7, Sunday=1.
            # Note: WEEKDAY() returns 0-6, Monday=0.
            return "DAYOFWEEK({0})".format(field_name)
        else:
            return "EXTRACT({0} FROM {1})".format(
                lookup_type.upper(), field_name)

    def date_trunc_sql(self, lookup_type, field_name):
        """Returns SQL simulating DATE_TRUNC

        This function uses MySQL functions DATE_FORMAT and CAST to
        simulate DATE_TRUNC.

        The field_name is returned when lookup_type is not supported.
        """
        fields = ['year', 'month', 'day', 'hour', 'minute', 'second']
        format = ('%Y-', '%m', '-%d', ' %H:', '%i', ':%S')
        format_def = ('0000-', '01', '-01', ' 00:', '00', ':00')
        try:
            i = fields.index(lookup_type) + 1
        except ValueError:
            # Wrong lookup type, just return the value from MySQL as-is
            sql = field_name
        else:
            format_str = ''.join([f for f in format[:i]] +
                                 [f for f in format_def[i:]])
            sql = "CAST(DATE_FORMAT({0}, '{1}') AS DATETIME)".format(
                field_name, format_str)
        return sql

    def datetime_extract_sql(self, lookup_type, field_name, tzname):
        # Django 1.6
        if settings.USE_TZ:
            field_name = "CONVERT_TZ({0}, 'UTC', %s)".format(field_name)
            params = [tzname]
        else:
            params = []

        # http://dev.mysql.com/doc/mysql/en/date-and-time-functions.html
        if lookup_type == 'week_day':
            # DAYOFWEEK() returns an integer, 1-7, Sunday=1.
            # Note: WEEKDAY() returns 0-6, Monday=0.
            sql = "DAYOFWEEK({0})".format(field_name)
        else:
            sql = "EXTRACT({0} FROM {1})".format(lookup_type.upper(),
                                                 field_name)
        return sql, params

    def datetime_trunc_sql(self, lookup_type, field_name, tzname):
        # Django 1.6
        if settings.USE_TZ:
            field_name = "CONVERT_TZ({0}, 'UTC', %s)".format(field_name)
            params = [tzname]
        else:
            params = []
        fields = ['year', 'month', 'day', 'hour', 'minute', 'second']
        format_ = ('%Y-', '%m', '-%d', ' %H:', '%i', ':%S')
        format_def = ('0000-', '01', '-01', ' 00:', '00', ':00')
        try:
            i = fields.index(lookup_type) + 1
        except ValueError:
            sql = field_name
        else:
            format_str = ''.join([f for f in format_[:i]] +
                                 [f for f in format_def[i:]])
            sql = "CAST(DATE_FORMAT({0}, '{1}') AS DATETIME)".format(
                field_name, format_str)
        return sql, params

    def date_interval_sql(self, sql, connector, timedelta):
        """Returns SQL for calculating date/time intervals
        """
        fmt = (
            "({sql} {connector} INTERVAL '{days} "
            "0:0:{secs}:{msecs}' DAY_MICROSECOND)"
        )
        return fmt.format(
            sql=sql,
            connector=connector,
            days=timedelta.days,
            secs=timedelta.seconds,
            msecs=timedelta.microseconds
        )

    def drop_foreignkey_sql(self):
        return "DROP FOREIGN KEY"

    def force_no_ordering(self):
        """
        "ORDER BY NULL" prevents MySQL from implicitly ordering by grouped
        columns. If no ordering would otherwise be applied, we don't want any
        implicit sorting going on.
        """
        return ["NULL"]

    def fulltext_search_sql(self, field_name):
        return 'MATCH ({0}) AGAINST (%s IN BOOLEAN MODE)'.format(field_name)

    def last_executed_query(self, cursor, sql, params):
        return cursor.statement

    def no_limit_value(self):
        # 2**64 - 1, as recommended by the MySQL documentation
        return 18446744073709551615

    def quote_name(self, name):
        if name.startswith("`") and name.endswith("`"):
            return name  # Quoting once is enough.
        return "`{0}`".format(name)

    def random_function_sql(self):
        return 'RAND()'

    def sql_flush(self, style, tables, sequences, allow_cascade=False):
        if tables:
            sql = ['SET FOREIGN_KEY_CHECKS = 0;']
            for table in tables:
                sql.append('{keyword} {table};'.format(
                    keyword=style.SQL_KEYWORD('TRUNCATE'),
                    table=style.SQL_FIELD(self.quote_name(table))))
            sql.append('SET FOREIGN_KEY_CHECKS = 1;')
            sql.extend(self.sequence_reset_by_name_sql(style, sequences))
            return sql
        else:
            return []

    def sequence_reset_by_name_sql(self, style, sequences):
        # Truncate already resets the AUTO_INCREMENT field from
        # MySQL version 5.0.13 onwards. Refs #16961.
        res = []
        if self.connection.mysql_version < (5, 0, 13):
            fmt = "{alter} {table} {{tablename}} {auto_inc} {field};".format(
                alter=style.SQL_KEYWORD('ALTER'),
                table=style.SQL_KEYWORD('TABLE'),
                auto_inc=style.SQL_KEYWORD('AUTO_INCREMENT'),
                field=style.SQL_FIELD('= 1')
            )
            for sequence in sequences:
                tablename = style.SQL_TABLE(self.quote_name(sequence['table']))
                res.append(fmt.format(tablename=tablename))
            return res
        return res

    def validate_autopk_value(self, value):
        # MySQLism: zero in AUTO_INCREMENT field does not work. Refs #17653.
        if value == 0:
            raise ValueError('The database backend does not accept 0 as a '
                             'value for AutoField.')
        return value

    def value_to_db_datetime(self, value):
        if value is None:
            return None
        # MySQL doesn't support tz-aware times
        if timezone.is_aware(value):
            if settings.USE_TZ:
                value = value.astimezone(timezone.utc).replace(tzinfo=None)
            else:
                raise ValueError(
                    "MySQL backend does not support timezone-aware times."
                )

        return self.connection.converter.to_mysql(value)

    def value_to_db_time(self, value):
        if value is None:
            return None

        # MySQL doesn't support tz-aware times
        if timezone.is_aware(value):
            raise ValueError("MySQL backend does not support timezone-aware "
                             "times.")

        return self.connection.converter.to_mysql(value)

    def year_lookup_bounds(self, value):
        # Again, no microseconds
        first = '{0}-01-01 00:00:00'
        second = '{0}-12-31 23:59:59.999999'
        return [first.format(value), second.format(value)]

    def year_lookup_bounds_for_datetime_field(self, value):
        # Django 1.6
        # Again, no microseconds
        first, second = super(DatabaseOperations,
            self).year_lookup_bounds_for_datetime_field(value)
        if self.connection.mysql_version >= (5, 6, 4):
            return [first.replace(microsecond=0), second]
        else:
            return [first.replace(microsecond=0),
                second.replace(microsecond=0)]

    def max_name_length(self):
        return 64

    def bulk_insert_sql(self, fields, num_values):
        items_sql = "({0})".format(", ".join(["%s"] * len(fields)))
        return "VALUES " + ", ".join([items_sql] * num_values)

    def savepoint_create_sql(self, sid):
        return "SAVEPOINT {0}".format(sid)

    def savepoint_commit_sql(self, sid):
        return "RELEASE SAVEPOINT {0}".format(sid)

    def savepoint_rollback_sql(self, sid):
        return "ROLLBACK TO SAVEPOINT {0}".format(sid)

    def combine_expression(self, connector, sub_expressions):
        """
        MySQL requires special cases for ^ operators in query expressions
        """
        if connector == '^':
            return 'POW(%s)' % ','.join(sub_expressions)
        return super(DatabaseOperations, self).combine_expression(
            connector, sub_expressions)


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'mysql'
    operators = {
        'exact': '= %s',
        'iexact': 'LIKE %s',
        'contains': 'LIKE BINARY %s',
        'icontains': 'LIKE %s',
        'regex': 'REGEXP BINARY %s',
        'iregex': 'REGEXP %s',
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': 'LIKE BINARY %s',
        'endswith': 'LIKE BINARY %s',
        'istartswith': 'LIKE %s',
        'iendswith': 'LIKE %s',
    }

    Database = mysql.connector

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)

        self.converter = DjangoMySQLConverter()
        self.ops = DatabaseOperations(self)
        self.features = DatabaseFeatures(self)
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        self.validation = DatabaseValidation(self)

    def _valid_connection(self):
        if self.connection:
            return self.connection.is_connected()
        return False

    def get_connection_params(self):
        # Django 1.6
        kwargs = {
            'charset': 'utf8',
            'use_unicode': True,
            'buffered': True,
        }

        settings_dict = self.settings_dict

        if settings_dict['USER']:
            kwargs['user'] = settings_dict['USER']
        if settings_dict['NAME']:
            kwargs['database'] = settings_dict['NAME']
        if settings_dict['PASSWORD']:
            kwargs['passwd'] = settings_dict['PASSWORD']
        if settings_dict['HOST'].startswith('/'):
            kwargs['unix_socket'] = settings_dict['HOST']
        elif settings_dict['HOST']:
            kwargs['host'] = settings_dict['HOST']
        if settings_dict['PORT']:
            kwargs['port'] = int(settings_dict['PORT'])

        # Raise exceptions for database warnings if DEBUG is on
        kwargs['raise_on_warnings'] = settings.DEBUG

        kwargs['client_flags'] = [
            # Need potentially affected rows on UPDATE
            mysql.connector.constants.ClientFlag.FOUND_ROWS,
        ]
        try:
            kwargs.update(settings_dict['OPTIONS'])
        except KeyError:
            # OPTIONS missing is OK
            pass

        return kwargs

    def get_new_connection(self, conn_params):
        # Django 1.6
        cnx = mysql.connector.connect(**conn_params)
        cnx.set_converter_class(DjangoMySQLConverter)

        return cnx

    def init_connection_state(self):
        # Django 1.6
        if self.mysql_version < (5, 5, 3):
            # See sysvar_sql_auto_is_null in MySQL Reference manual
            self.connection.cmd_query("SET SQL_AUTO_IS_NULL = 0")

        if 'AUTOCOMMIT' in self.settings_dict:
            try:
                # Django 1.6
                self.set_autocommit(self.settings_dict['AUTOCOMMIT'])
            except AttributeError:
                self._set_autocommit(self.settings_dict['AUTOCOMMIT'])

    def create_cursor(self):
        # Django 1.6
        cursor = self.connection.cursor()
        return CursorWrapper(cursor)

    def _connect(self):
        """Setup the connection with MySQL"""
        self.connection = self.get_new_connection(self.get_connection_params())
        connection_created.send(sender=self.__class__, connection=self)
        self.init_connection_state()

    def _cursor(self):
        """Return a CursorWrapper object

        Returns a CursorWrapper
        """
        try:
            # Django 1.6
            return super(DatabaseWrapper, self)._cursor()
        except AttributeError:
            if not self.connection:
                self._connect()
            return self.create_cursor()

    def get_server_version(self):
        """Returns the MySQL server version of current connection

        Returns a tuple
        """
        try:
            # Django 1.6
            self.ensure_connection()
        except AttributeError:
            if not self.connection:
                self._connect()

        return self.connection.get_server_version()

    def disable_constraint_checking(self):
        """Disables foreign key checks

        Disables foreign key checks, primarily for use in adding rows with
        forward references. Always returns True,
        to indicate constraint checks need to be re-enabled.

        Returns True
        """
        self.cursor().execute('SET @@session.foreign_key_checks = 0')
        return True

    def enable_constraint_checking(self):
        """Re-enable foreign key checks

        Re-enable foreign key checks after they have been disabled.
        """
        # Override needs_rollback in case constraint_checks_disabled is
        # nested inside transaction.atomic.
        if django.VERSION >= (1, 6):
            self.needs_rollback, needs_rollback = False, self.needs_rollback
        try:
            self.cursor().execute('SET @@session.foreign_key_checks = 1')
        finally:
            if django.VERSION >= (1, 6):
                self.needs_rollback = needs_rollback

    def check_constraints(self, table_names=None):
        """Check rows in tables for invalid foreign key references

        Checks each table name in `table_names` for rows with invalid foreign
        key references. This method is intended to be used in conjunction with
        `disable_constraint_checking()` and `enable_constraint_checking()`, to
        determine if rows with invalid references were entered while
        constraint checks were off.

        Raises an IntegrityError on the first invalid foreign key reference
        encountered (if any) and provides detailed information about the
        invalid reference in the error message.

        Backends can override this method if they can more directly apply
        constraint checking (e.g. via "SET CONSTRAINTS ALL IMMEDIATE")
        """
        ref_query = """
            SELECT REFERRING.`{0}`, REFERRING.`{1}` FROM `{2}` as REFERRING
            LEFT JOIN `{3}` as REFERRED
            ON (REFERRING.`{4}` = REFERRED.`{5}`)
            WHERE REFERRING.`{6}` IS NOT NULL AND REFERRED.`{7}` IS NULL"""
        cursor = self.cursor()
        if table_names is None:
            table_names = self.introspection.table_names(cursor)
        for table_name in table_names:
            primary_key_column_name = \
                self.introspection.get_primary_key_column(cursor, table_name)
            if not primary_key_column_name:
                continue
            key_columns = self.introspection.get_key_columns(cursor,
                                                             table_name)
            for column_name, referenced_table_name, referenced_column_name \
                    in key_columns:
                cursor.execute(ref_query.format(primary_key_column_name,
                                                column_name, table_name,
                                                referenced_table_name,
                                                column_name,
                                                referenced_column_name,
                                                column_name,
                                                referenced_column_name))
                for bad_row in cursor.fetchall():
                    msg = ("The row in table '{0}' with primary key '{1}' has "
                           "an invalid foreign key: {2}.{3} contains a value "
                           "'{4}' that does not have a corresponding value in "
                           "{5}.{6}.".format(table_name, bad_row[0],
                                             table_name, column_name,
                                             bad_row[1], referenced_table_name,
                                             referenced_column_name))
                    raise utils.IntegrityError(msg)

    def _rollback(self):
        try:
            BaseDatabaseWrapper._rollback(self)
        except NotSupportedError:
            pass

    def _set_autocommit(self, autocommit):
        # Django 1.6
        with self.wrap_database_errors:
            self.connection.autocommit = autocommit

    def schema_editor(self, *args, **kwargs):
        """Returns a new instance of this backend's SchemaEditor"""
        # Django 1.7
        return DatabaseSchemaEditor(self, *args, **kwargs)

    def is_usable(self):
        # Django 1.6
        return self.connection.is_connected()

    @cached_property
    def mysql_version(self):
        config = self.get_connection_params()
        temp_conn = mysql.connector.connect(**config)
        server_version = temp_conn.get_server_version()
        temp_conn.close()

        return server_version
