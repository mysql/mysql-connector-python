# MySQL Connector/Python - MySQL driver written in Python.

from __future__ import unicode_literals

from django.conf import settings
from django.db.backends.mysql.operations import DatabaseOperations as MySQLDatabaseOperations
from django.utils import timezone


try:
    from _mysql_connector import datetime_to_mysql, time_to_mysql
except ImportError:
    HAVE_CEXT = False
else:
    HAVE_CEXT = True


class DatabaseOperations(MySQLDatabaseOperations):
    compiler_module = "mysql.connector.django.compiler"

    def adapt_datetimefield_value(self, value):
        if value is None:
            return None

        # MySQL doesn't support tz-aware times
        if timezone.is_aware(value):
            if settings.USE_TZ:
                value = timezone.make_naive(value, self.connection.timezone)
            else:
                raise ValueError("MySQL backend does not support timezone-aware datetimes when USE_TZ is False.")

        if not self.connection.features.supports_microsecond_precision:
            value = value.replace(microsecond=0)
        if not self.connection.use_pure:
            return datetime_to_mysql(value)

        return self.connection.converter.to_mysql(value)

    def adapt_timefield_value(self, value):
        if value is None:
            return None

        # MySQL doesn't support tz-aware times
        if timezone.is_aware(value):
            raise ValueError("MySQL backend does not support timezone-aware times.")

        if not self.connection.use_pure:
            return time_to_mysql(value)

        return self.connection.converter.to_mysql(value)

    def convert_datetimefield_value(self, value, expression, connection):
        if value is not None:
            if not timezone.is_aware(value):
                value = timezone.make_aware(value, self.connection.timezone)
        return value
