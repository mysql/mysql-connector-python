# Copyright (c) 2020, Oracle and/or its affiliates.
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

from django.db.backends.mysql.operations import DatabaseOperations as MySQLDatabaseOperations
from django.conf import settings
from django.utils import timezone

try:
    from _mysql_connector import datetime_to_mysql, time_to_mysql
except ImportError:
    HAVE_CEXT = False
else:
    HAVE_CEXT = True


class DatabaseOperations(MySQLDatabaseOperations):
    compiler_module = "mysql.connector.django.compiler"

    def date_trunc_sql(self, lookup_type, field_name):
        fields = {
            'year': '%Y-01-01',
            'month': '%Y-%m-01',
        }  # Use double percents to escape.
        if lookup_type in fields:
            format_str = fields[lookup_type]
            return "CAST(DATE_FORMAT(%s, '%s') AS DATE)" % (field_name, format_str)
        elif lookup_type == 'quarter':
            return "MAKEDATE(YEAR(%s), 1) + INTERVAL QUARTER(%s) QUARTER - INTERVAL 1 QUARTER" % (
                field_name, field_name
            )
        elif lookup_type == 'week':
            return "DATE_SUB(%s, INTERVAL WEEKDAY(%s) DAY)" % (
                field_name, field_name
            )
        else:
            return "DATE(%s)" % (field_name)

    def datetime_trunc_sql(self, lookup_type, field_name, tzname):
        field_name = self._convert_field_to_tz(field_name, tzname)
        fields = ['year', 'month', 'day', 'hour', 'minute', 'second']
        format = ('%Y-', '%m', '-%d', ' %H:', '%i', ':%s')
        format_def = ('0000-', '01', '-01', ' 00:', '00', ':00')
        if lookup_type == 'quarter':
            return (
                "CAST(DATE_FORMAT(MAKEDATE(YEAR({field_name}), 1) + "
                "INTERVAL QUARTER({field_name}) QUARTER - " +
                "INTERVAL 1 QUARTER, '%Y-%m-01 00:00:00') AS DATETIME)"
            ).format(field_name=field_name)
        if lookup_type == 'week':
            return (
                "CAST(DATE_FORMAT(DATE_SUB({field_name}, "
                "INTERVAL WEEKDAY({field_name}) DAY), "
                "'%Y-%m-%d 00:00:00') AS DATETIME)"
            ).format(field_name=field_name)
        try:
            i = fields.index(lookup_type) + 1
        except ValueError:
            sql = field_name
        else:
            format_str = ''.join(format[:i] + format_def[i:])
            sql = "CAST(DATE_FORMAT(%s, '%s') AS DATETIME)" % (field_name, format_str)
        return sql

    def time_trunc_sql(self, lookup_type, field_name):
        fields = {
            'hour': '%H:00:00',
            'minute': '%H:%i:00',
            'second': '%H:%i:%s',
        }  # Use double percents to escape.
        if lookup_type in fields:
            format_str = fields[lookup_type]
            return "CAST(DATE_FORMAT(%s, '%s') AS TIME)" % (field_name, format_str)
        else:
            return "TIME(%s)" % (field_name)

    def regex_lookup(self, lookup_type):
        if self.connection.mysql_version < (8, 0, 0):
            if lookup_type == 'regex':
                return '%s REGEXP BINARY %s'
            return '%s REGEXP %s'

        match_option = 'c' if lookup_type == 'regex' else 'i'
        return "REGEXP_LIKE(%s, %s, '%s')" % match_option

    def adapt_datetimefield_value(self, value):
        return self.value_to_db_datetime(value)

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
        if not self.connection.features.supports_microsecond_precision:
            value = value.replace(microsecond=0)
        if not self.connection.use_pure:
            return datetime_to_mysql(value)
        return self.connection.converter.to_mysql(value)

    def adapt_timefield_value(self, value):
        return self.value_to_db_time(value)

    def value_to_db_time(self, value):
        if value is None:
            return None

        # MySQL doesn't support tz-aware times
        if timezone.is_aware(value):
            raise ValueError("MySQL backend does not support timezone-aware "
                             "times.")

        if not self.connection.use_pure:
            return time_to_mysql(value)
        return self.connection.converter.to_mysql(value)
