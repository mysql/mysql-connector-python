# Copyright (c) 2009, 2017, Oracle and/or its affiliates. All rights reserved.
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

"""Unittests for MySQL data types
"""

from decimal import Decimal
import time
import datetime

from mysql.connector import connection, errors
import tests
from tests import foreach_cnx, cnx_config

try:
    from mysql.connector.connection_cext import CMySQLConnection
except ImportError:
    # Test without C Extension
    CMySQLConnection = None


def _get_insert_stmt(tbl, cols):
    insert = "INSERT INTO {table} ({columns}) values ({values})".format(
        table=tbl,
        columns=','.join(cols),
        values=','.join(['%s'] * len(cols))
        )
    return insert


def _get_select_stmt(tbl, cols):
    select = "SELECT {columns} FROM {table} ORDER BY id".format(
        columns=','.join(cols),
        table=tbl
        )
    return select


class TestsDataTypes(tests.MySQLConnectorTests):

    tables = {
        'bit': 'myconnpy_mysql_bit',
        'int': 'myconnpy_mysql_int',
        'bool': 'myconnpy_mysql_bool',
        'float': 'myconnpy_mysql_float',
        'decimal': 'myconnpy_mysql_decimal',
        'temporal': 'myconnpy_mysql_temporal',
        'temporal_year': 'myconnpy_mysql_temporal_year',
        'set': 'myconnpy_mysql_set',
    }

    def compare(self, name, val1, val2):
        self.assertEqual(val1, val2, "%s  %s != %s" % (name, val1, val2))

    def drop_tables(self, cnx):
        cur = cnx.cursor()
        table_names = self.tables.values()
        cur.execute("DROP TABLE IF EXISTS {tables}".format(
            tables=','.join(table_names))
            )
        cur.close()


class TestsCursor(TestsDataTypes):

    def setUp(self):
        self.config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**self.config)
        self.drop_tables(cnx)

    def tearDown(self):
        cnx = connection.MySQLConnection(**self.config)
        self.drop_tables(cnx)
        cnx.close()

    @foreach_cnx()
    def test_numeric_int(self):
        tbl = self.tables['int']
        self.cnx.cmd_query("DROP TABLE IF EXISTS {0}".format(tbl))
        cur = self.cnx.cursor()
        columns = [
            'tinyint_signed',
            'tinyint_unsigned',
            'bool_signed',
            'smallint_signed',
            'smallint_unsigned',
            'mediumint_signed',
            'mediumint_unsigned',
            'int_signed',
            'int_unsigned',
            'bigint_signed',
            'bigint_unsigned',
        ]
        cur.execute((
            "CREATE TABLE {table} ("
            "`id` TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,"
            "`tinyint_signed` TINYINT SIGNED,"
            "`tinyint_unsigned` TINYINT UNSIGNED,"
            "`bool_signed` BOOL,"
            "`smallint_signed` SMALLINT SIGNED,"
            "`smallint_unsigned` SMALLINT UNSIGNED,"
            "`mediumint_signed` MEDIUMINT SIGNED,"
            "`mediumint_unsigned` MEDIUMINT UNSIGNED,"
            "`int_signed` INT SIGNED,"
            "`int_unsigned` INT UNSIGNED,"
            "`bigint_signed` BIGINT SIGNED,"
            "`bigint_unsigned` BIGINT UNSIGNED,"
            "PRIMARY KEY (id))"
          ).format(table=tbl)
        )

        data = [
            (
                -128,  # tinyint signed
                0,  # tinyint unsigned
                0,  # boolean
                -32768,  # smallint signed
                0,  # smallint unsigned
                -8388608,  # mediumint signed
                0,  # mediumint unsigned
                -2147483648,  # int signed
                0,  # int unsigned
                -9223372036854775808,  # big signed
                0,  # big unsigned
            ),
            (
                127,  # tinyint signed
                255,  # tinyint unsigned
                127,  # boolean
                32767,  # smallint signed
                65535,  # smallint unsigned
                8388607,  # mediumint signed
                16777215,  # mediumint unsigned
                2147483647,  # int signed
                4294967295,  # int unsigned
                9223372036854775807,  # big signed
                18446744073709551615,  # big unsigned
            )
        ]

        insert = _get_insert_stmt(tbl, columns)
        select = _get_select_stmt(tbl, columns)

        cur.executemany(insert, data)
        cur.execute(select)
        rows = cur.fetchall()

        for i, col in enumerate(columns):
            self.compare(col, data[0][i], rows[0][i])
            self.compare(col, data[1][i], rows[1][i])

        cur.close()

    @foreach_cnx()
    def test_numeric_bit(self):
        tbl = self.tables['bit']
        self.cnx.cmd_query("DROP TABLE IF EXISTS {0}".format(tbl))
        cur = self.cnx.cursor()
        columns = [
            'c8', 'c16', 'c24', 'c32',
            'c40', 'c48', 'c56', 'c63',
            'c64']
        cur.execute((
            "CREATE TABLE {table} ("
            "`id` int NOT NULL AUTO_INCREMENT,"
            "`c8` bit(8) DEFAULT NULL,"
            "`c16` bit(16) DEFAULT NULL,"
            "`c24` bit(24) DEFAULT NULL,"
            "`c32` bit(32) DEFAULT NULL,"
            "`c40` bit(40) DEFAULT NULL,"
            "`c48` bit(48) DEFAULT NULL,"
            "`c56` bit(56) DEFAULT NULL,"
            "`c63` bit(63) DEFAULT NULL,"
            "`c64` bit(64) DEFAULT NULL,"
            "PRIMARY KEY (id))"
            ).format(table=tbl)
        )

        insert = _get_insert_stmt(tbl, columns)
        select = _get_select_stmt(tbl, columns)

        data = list()
        data.append(tuple([0] * len(columns)))

        values = list()
        for col in columns:
            values.append((1 << int(col.replace('c', ''))) - 1)
        data.append(tuple(values))

        values = list()
        for col in columns:
            bits = int(col.replace('c', ''))
            values.append((1 << bits) - 1)
        data.append(tuple(values))

        cur.executemany(insert, data)
        cur.execute(select)
        rows = cur.fetchall()

        self.assertEqual(rows, data)
        cur.close()

    @foreach_cnx()
    def test_numeric_float(self):
        tbl = self.tables['float']
        self.cnx.cmd_query("DROP TABLE IF EXISTS {0}".format(tbl))
        cur = self.cnx.cursor()
        columns = [
            'float_signed',
            'float_unsigned',
            'double_signed',
            'double_unsigned',
        ]
        cur.execute((
            "CREATE TABLE {table} ("
            "`id` int NOT NULL AUTO_INCREMENT,"
            "`float_signed` FLOAT(6,5) SIGNED,"
            "`float_unsigned` FLOAT(6,5) UNSIGNED,"
            "`double_signed` DOUBLE(15,10) SIGNED,"
            "`double_unsigned` DOUBLE(15,10) UNSIGNED,"
            "PRIMARY KEY (id))"
            ).format(table=tbl)
        )

        insert = _get_insert_stmt(tbl, columns)
        select = _get_select_stmt(tbl, columns)

        data = [
            (-3.402823466, 0, -1.7976931348623157, 0,),
            (-1.175494351, 3.402823466,
             1.7976931348623157, 2.2250738585072014),
            (-1.23455678, 2.999999, -1.3999999999999999, 1.9999999999999999),
        ]
        cur.executemany(insert, data)
        cur.execute(select)
        rows = cur.fetchall()

        for j in range(0, len(data)):
            for i, col in enumerate(columns[0:2]):
                self.compare(col, round(data[j][i], 5), rows[j][i])
            for i, col in enumerate(columns[2:2]):
                self.compare(col, round(data[j][i], 10), rows[j][i])
        cur.close()

    @foreach_cnx()
    def test_numeric_decimal(self):
        tbl = self.tables['decimal']
        self.cnx.cmd_query("DROP TABLE IF EXISTS {0}".format(tbl))
        cur = self.cnx.cursor()
        columns = [
            'decimal_signed',
            'decimal_unsigned',
        ]
        cur.execute((
            "CREATE TABLE {table} ("
            "`id` int NOT NULL AUTO_INCREMENT,"
            "`decimal_signed` DECIMAL(65,30) SIGNED,"
            "`decimal_unsigned` DECIMAL(65,30) UNSIGNED,"
            "PRIMARY KEY (id))"
            ).format(table=tbl)
        )

        insert = _get_insert_stmt(tbl, columns)
        select = _get_select_stmt(tbl, columns)

        data = [
            (Decimal(
                '-9999999999999999999999999.999999999999999999999999999999'),
             Decimal(
                 '+9999999999999999999999999.999999999999999999999999999999')),
            (Decimal('-1234567.1234'),
             Decimal('+123456789012345.123456789012345678901')),
            (Decimal(
                '-1234567890123456789012345.123456789012345678901234567890'),
             Decimal(
                 '+1234567890123456789012345.123456789012345678901234567890')),
        ]
        cur.executemany(insert, data)
        cur.execute(select)
        rows = cur.fetchall()

        self.assertEqual(data, rows)

        cur.close()

    @foreach_cnx()
    def test_temporal_datetime(self):
        tbl = self.tables['temporal']
        self.cnx.cmd_query("DROP TABLE IF EXISTS {0}".format(tbl))
        cur = self.cnx.cursor()
        cur.execute("SET SESSION time_zone = '+00:00'")
        columns = [
            't_date',
            't_datetime',
            't_time',
            't_timestamp',
            't_year_4',
        ]
        cur.execute((
            "CREATE TABLE {table} ("
            "`id` int NOT NULL AUTO_INCREMENT,"
            "`t_date` DATE,"
            "`t_datetime` DATETIME,"
            "`t_time` TIME,"
            "`t_timestamp` TIMESTAMP DEFAULT 0,"
            "`t_year_4` YEAR(4),"
            "PRIMARY KEY (id))"
            ).format(table=tbl)
        )

        insert = _get_insert_stmt(tbl, columns)
        select = _get_select_stmt(tbl, columns)

        data = [
            (datetime.date(2010, 1, 17),
             datetime.datetime(2010, 1, 17, 19, 31, 12),
             datetime.timedelta(hours=43, minutes=32, seconds=21),
             datetime.datetime(2010, 1, 17, 19, 31, 12),
             0),
            (datetime.date(1000, 1, 1),
             datetime.datetime(1000, 1, 1, 0, 0, 0),
             datetime.timedelta(hours=-838, minutes=59, seconds=59),
             datetime.datetime(*time.gmtime(1)[:6]),
             1901),
            (datetime.date(9999, 12, 31),
             datetime.datetime(9999, 12, 31, 23, 59, 59),
             datetime.timedelta(hours=838, minutes=59, seconds=59),
             datetime.datetime(2038, 1, 19, 3, 14, 7),
             2155),
        ]

        cur.executemany(insert, data)
        cur.execute(select)
        rows = cur.fetchall()

        for j in (range(0, len(data))):
            for i, col in enumerate(columns):
                self.compare("{column} (data[{count}])".format(
                    column=col, count=j), data[j][i], rows[j][i])

        # Testing YEAR(2), which is now obsolete since MySQL 5.6.6
        tblname = self.tables['temporal_year']
        cur.execute("DROP TABLE IF EXISTS {0}".format(tblname))
        stmt = (
            "CREATE TABLE {table} ("
            "`id` int NOT NULL AUTO_INCREMENT KEY, "
            "`t_year_2` YEAR(2))".format(table=tblname)
        )
        if tests.MYSQL_VERSION >= (5, 7, 5):
            # Support for YEAR(2) removed in MySQL 5.7.5
            self.assertRaises(errors.DatabaseError, cur.execute, stmt)
        else:
            cur.execute(stmt)
            cur.execute(_get_insert_stmt(tblname, ['t_year_2']), (10,))
            cur.execute(_get_select_stmt(tblname, ['t_year_2']))
            row = cur.fetchone()

            if tests.MYSQL_VERSION >= (5, 6, 6):
                self.assertEqual(2010, row[0])
            else:
                self.assertEqual(10, row[0])

        cur.close()

    @cnx_config(consume_results=True)
    @foreach_cnx()
    def test_set(self):
        tbl = self.tables['temporal']
        self.cnx.cmd_query("DROP TABLE IF EXISTS {0}".format(tbl))
        cur = self.cnx.cursor()

        cur.execute((
            "CREATE TABLE {table} ("
            "`id` int NOT NULL AUTO_INCREMENT,"
            "c1 SET ('a', 'b', 'c'),"
            "c2 SET ('1', '2', '3'),"
            "c3 SET ('ham', 'spam'),"
            "PRIMARY KEY (id))"
            ).format(table=tbl)
        )

        insert = (
            "INSERT INTO {table} (c1, c2, c3) VALUES "
            "('a,c', '1,3', 'spam'), ('b', '3,2', 'spam,spam,ham')"
        ).format(table=tbl)
        cur.execute(insert)
        cur.execute("SELECT * FROM {table}".format(table=tbl))

        exp = [
            (1, set([u'a', u'c']), set([u'1', u'3']), set([u'spam'])),
            (2, set([u'b']), set([u'3', u'2']), set([u'ham', u'spam']))
        ]
        self.assertEqual(exp, cur.fetchall())

        cur.close()
