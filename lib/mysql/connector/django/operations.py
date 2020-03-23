# MySQL Connector/Python - MySQL driver written in Python.

from __future__ import unicode_literals

from django.db.backends.mysql.operations import DatabaseOperations as MySQLDatabaseOperations


try:
    from _mysql_connector import datetime_to_mysql, time_to_mysql
except ImportError:
    HAVE_CEXT = False
else:
    HAVE_CEXT = True


class DatabaseOperations(MySQLDatabaseOperations):
    compiler_module = "mysql.connector.django.compiler"
