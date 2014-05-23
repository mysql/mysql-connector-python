# -*- coding: utf-8 -*-
# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2009, 2014, Oracle and/or its affiliates. All rights reserved.

# MySQL Connector/Python is licensed under the terms of the GPLv2
# <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most
# MySQL Connectors. There are special exceptions to the terms and
# conditions of the GPLv2 as it is applied to this software, see the
# FOSS License Exception
# <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA

"""Unittests for mysql.connector.conversion
"""

from decimal import Decimal
import datetime
import time
import uuid

import tests

from mysql.connector import conversion, constants


class MySQLConverterBaseTests(tests.MySQLConnectorTests):

    def test_init(self):
        cnv = conversion.MySQLConverterBase()

        self.assertEqual('utf8', cnv.charset)
        self.assertEqual(True, cnv.use_unicode)

    def test_init2(self):
        cnv = conversion.MySQLConverterBase(charset='latin1',
                                            use_unicode=False)

        self.assertEqual('latin1', cnv.charset)
        self.assertEqual(False, cnv.use_unicode)

    def test_set_charset(self):
        cnv = conversion.MySQLConverterBase()
        cnv.set_charset('latin2')

        self.assertEqual('latin2', cnv.charset)

    def test_set_useunicode(self):
        cnv = conversion.MySQLConverterBase()
        cnv.set_unicode(False)

        self.assertEqual(False, cnv.use_unicode)

    def test_to_mysql(self):
        cnv = conversion.MySQLConverterBase()

        self.assertEqual('a value', cnv.to_mysql('a value'))

    def test_to_python(self):
        cnv = conversion.MySQLConverterBase()

        self.assertEqual('a value', cnv.to_python('nevermind', 'a value'))

    def test_escape(self):
        cnv = conversion.MySQLConverterBase()

        self.assertEqual("'a value'", cnv.escape("'a value'"))

    def test_quote(self):
        cnv = conversion.MySQLConverterBase()

        self.assertEqual("'a value'", cnv.escape("'a value'"))


class MySQLConverterTests(tests.MySQLConnectorTests):

    def setUp(self):
        self.cnv = conversion.MySQLConverter()

    def tearDown(self):
        pass

    def test_init(self):
        pass

    def test_escape(self):
        """Making strings ready for MySQL operations"""
        data = (
            None,               # should stay the same
            int(128),           # should stay the same
            long(1281288),      # should stay the same
            float(3.14),        # should stay the same
            Decimal('3.14'),    # should stay a Decimal
            r'back\slash',
            'newline\n',
            'return\r',
            "'single'",
            '"double"',
            'windows\032',
        )
        exp = (
            None,
            128,
            1281288,
            float(3.14),
            Decimal("3.14"),
            'back\\\\slash',
            'newline\\n',
            'return\\r',
            "\\'single\\'",
            '\\"double\\"',
            'windows\\\x1a'
        )

        res = tuple([self.cnv.escape(v) for v in data])
        self.assertEqual(res, exp)

    def test_quote(self):
        """Quote values making them ready for MySQL operations."""

        data = (
            None,
            int(128),
            long(1281288),
            float(3.14),
            Decimal('3.14'),
            'string A',
            "string B",
        )
        exp = (
            'NULL',
            '128',
            '1281288',
            '3.14',
            '3.14',
            "'string A'",
            "'string B'",
        )

        res = tuple([self.cnv.quote(value) for value in data])
        self.assertEqual(res, exp)

    def test_to_mysql(self):
        """Convert Python types to MySQL types using helper method"""
        st_now = time.localtime()
        data = (
            128,            # int
            1281288,        # long
            float(3.14),    # float
            str("Strings are sexy"),
            u'\u82b1',
            None,
            datetime.datetime(2008, 5, 7, 20, 0o1, 23),
            datetime.date(2008, 5, 7),
            datetime.time(20, 0o3, 23),
            st_now,
            datetime.timedelta(hours=40, minutes=30, seconds=12),
            Decimal('3.14'),
        )
        exp = (
            data[0],
            data[1],
            data[2],
            self.cnv._str_to_mysql(data[3]),
            self.cnv._unicode_to_mysql(data[4]),
            None,
            '2008-05-07 20:01:23',
            '2008-05-07',
            '20:03:23',
            time.strftime('%Y-%m-%d %H:%M:%S', st_now),
            '40:30:12',
            '3.14',
        )

        res = tuple([self.cnv.to_mysql(value) for value in data])
        self.assertEqual(res, exp)
        self.assertRaises(TypeError, self.cnv.to_mysql, uuid.uuid4())

    def test__str_to_mysql(self):
        """A Python string is a MySQL string."""
        data = str("Strings are sexy")
        res = self.cnv._str_to_mysql(data)

        self.assertEqual(data, res)

    def test__unicode_to_mysql(self):
        """Python unicode strings become encoded strings."""
        data = u'\u82b1'
        res = self.cnv._unicode_to_mysql(data)
        exp = data.encode(self.cnv.charset)

        self.assertEqual(exp, res)

    def test__nonetype_to_mysql(self):
        """Python None stays None for MySQL."""
        data = None
        res = self.cnv._nonetype_to_mysql(data)

        self.assertEqual(data, res)

    def test__datetime_to_mysql(self):
        """A datetime.datetime becomes formatted like Y-m-d H:M:S.f"""
        cases = [
            (datetime.datetime(2008, 5, 7, 20, 1, 23),
             '2008-05-07 20:01:23'),
            (datetime.datetime(2012, 5, 2, 20, 1, 23, 10101),
             '2012-05-02 20:01:23.010101')
        ]
        for data, exp in cases:
            self.assertEqual(exp, self.cnv._datetime_to_mysql(data))

    def test__date_to_mysql(self):
        """A datetime.date becomes formatted like Y-m-d"""
        data = datetime.date(2008, 5, 7)
        res = self.cnv._date_to_mysql(data)
        exp = data.strftime('%Y-%m-%d')

        self.assertEqual(exp, res)

    def test__time_to_mysql(self):
        """A datetime.time becomes formatted like Y-m-d H:M:S[.f]"""
        cases = [
            (datetime.time(20, 3, 23), '20:03:23'),
            (datetime.time(20, 3, 23, 10101), '20:03:23.010101'),
        ]
        for data, exp in cases:
            self.assertEqual(exp, self.cnv._time_to_mysql(data))

    def test__struct_time_to_mysql(self):
        """A time.struct_time becomes formatted like Y-m-d H:M:S[.f]"""
        data = time.localtime()
        res = self.cnv._struct_time_to_mysql(data)
        exp = time.strftime('%Y-%m-%d %H:%M:%S', data)

        self.assertEqual(exp, res)

    def test__timedelta_to_mysql(self):
        """A datetime.timedelta becomes format like 'H:M:S[.f]'"""
        cases = [
            (datetime.timedelta(hours=40, minutes=30, seconds=12),
             '40:30:12'),
            (datetime.timedelta(hours=-40, minutes=30, seconds=12),
             '-39:29:48'),
            (datetime.timedelta(hours=40, minutes=-1, seconds=12),
             '39:59:12'),
            (datetime.timedelta(hours=-40, minutes=60, seconds=12),
             '-38:59:48'),
            (datetime.timedelta(hours=40, minutes=30, seconds=12,
             microseconds=10101),
             '40:30:12.010101'),
            (datetime.timedelta(hours=-40, minutes=30, seconds=12,
             microseconds=10101),
             '-39:29:47.989899'),
            (datetime.timedelta(hours=40, minutes=-1, seconds=12,
             microseconds=10101),
             '39:59:12.010101'),
            (datetime.timedelta(hours=-40, minutes=60, seconds=12,
             microseconds=10101),
             '-38:59:47.989899'),
        ]
        for i, case in enumerate(cases):
            data, exp = case
            self.assertEqual(exp, self.cnv._timedelta_to_mysql(data),
                             "Case {0} failed: {1}; got {2}".format(
                                 i + 1, repr(data),
                                 self.cnv._timedelta_to_mysql(data)))

    def test__decimal_to_mysql(self):
        """A decimal.Decimal becomes a string."""
        data = Decimal('3.14')
        self.assertEqual('3.14', self.cnv._decimal_to_mysql(data))

    def test_to_python(self):
        """Convert MySQL data to Python types using helper method"""
        data = (
            #, None, None, None, None, True, constants.FieldFlag.NUM),
            ('3.14', ('float', constants.FieldType.FLOAT)),
            ('128', ('int', constants.FieldType.TINY)),
            ('1281288', ('long', constants.FieldType.LONG)),
            ('3.14', ('decimal', constants.FieldType.DECIMAL)),
            ('2008-05-07', ('date', constants.FieldType.DATE)),
            ('45:34:10', ('time', constants.FieldType.TIME)),
            ('2008-05-07 22:34:10',
             ('datetime', constants.FieldType.DATETIME)),
            ('val1,val2', ('set', constants.FieldType.SET, None,
             None, None, None, True, constants.FieldFlag.SET)),
            ('\xc3\xa4 utf8 string',
             ('utf8', constants.FieldType.STRING,
              None, None, None, None, True, 0)),
            ('2008', ('year', constants.FieldType.YEAR)),
            ('\x80\x00\x00\x00', ('bit', constants.FieldType.BIT)),
        )
        exp = (
            float(data[0][0]),
            int(data[1][0]),
            long(data[2][0]),
            Decimal(data[3][0]),
            datetime.date(2008, 5, 7),
            datetime.timedelta(hours=45, minutes=34, seconds=10),
            datetime.datetime(2008, 5, 7, 22, 34, 10),
            set(['val1', 'val2']),
            unicode(data[8][0], 'utf8'),
            int(data[9][0]),
            2147483648,
        )

        res = tuple([self.cnv.to_python(v[1], v[0]) for v in data])
        self.assertEqual(res, exp)

    def test__FLOAT_to_python(self):
        """Convert a MySQL FLOAT/DOUBLE to a Python float type"""
        data = '3.14'
        exp = float(data)
        res = self.cnv._FLOAT_to_python(data)

        self.assertEqual(exp, res)

        self.assertEqual(self.cnv._FLOAT_to_python,
                         self.cnv._DOUBLE_to_python)

    def test__INT_to_python(self):
        """Convert a MySQL TINY/SHORT/INT24/INT to a Python int type"""
        data = '128'
        exp = int(data)
        res = self.cnv._INT_to_python(data)

        self.assertEqual(exp, res)

        self.assertEqual(self.cnv._INT_to_python, self.cnv._TINY_to_python)
        self.assertEqual(self.cnv._INT_to_python, self.cnv._SHORT_to_python)
        self.assertEqual(self.cnv._INT_to_python, self.cnv._INT24_to_python)

    def test__LONG_to_python(self):
        """Convert a MySQL LONG/LONGLONG to a Python long type"""
        data = '1281288'
        exp = long(data)
        res = self.cnv._LONG_to_python(data)

        self.assertEqual(exp, res)

        self.assertEqual(self.cnv._LONG_to_python,
                         self.cnv._LONGLONG_to_python)

    def test__DECIMAL_to_python(self):
        """Convert a MySQL DECIMAL to a Python decimal.Decimal type"""
        data = '3.14'
        exp = Decimal(data)
        res = self.cnv._DECIMAL_to_python(data)

        self.assertEqual(exp, res)

        self.assertEqual(self.cnv._DECIMAL_to_python,
                         self.cnv._NEWDECIMAL_to_python)

    def test__BIT_to_python(self):
        """Convert a MySQL BIT to Python int"""
        data = [
            '\x80',
            '\x80\x00',
            '\x80\x00\x00',
            '\x80\x00\x00\x00',
            '\x80\x00\x00\x00\x00',
            '\x80\x00\x00\x00\x00\x00',
            '\x80\x00\x00\x00\x00\x00\x00',
            '\x80\x00\x00\x00\x00\x00\x00\x00',
        ]
        exp = [128, 32768, 8388608, 2147483648, 549755813888,
               140737488355328, 36028797018963968, 9223372036854775808]

        res = map(self.cnv._BIT_to_python, data)
        self.assertEqual(exp, res)

    def test__DATE_to_python(self):
        """Convert a MySQL DATE to a Python datetime.date type"""
        data = '2008-05-07'
        exp = datetime.date(2008, 5, 7)
        res = self.cnv._DATE_to_python(data)

        self.assertEqual(exp, res)

        res = self.cnv._DATE_to_python('0000-00-00')
        self.assertEqual(None, res)
        res = self.cnv._DATE_to_python('1000-00-00')
        self.assertEqual(None, res)

    def test__TIME_to_python(self):
        """Convert a MySQL TIME to a Python datetime.time type"""
        cases = [
            ('45:34:10',
             datetime.timedelta(hours=45, minutes=34, seconds=10)),
            ('-45:34:10',
             datetime.timedelta(-2, 8750)),
            ('45:34:10.010101',
             datetime.timedelta(hours=45, minutes=34, seconds=10,
                                microseconds=10101)),
            (b'-45:34:10.010101',
             datetime.timedelta(-2, 8749, 989899)),
        ]
        for data, exp in cases:
            self.assertEqual(exp, self.cnv._TIME_to_python(data))

    def test__DATETIME_to_python(self):
        """Convert a MySQL DATETIME to a Python datetime.datetime type"""
        cases = [
            ('2008-05-07 22:34:10',
             datetime.datetime(2008, 5, 7, 22, 34, 10)),
            ('2008-05-07 22:34:10.010101',
             datetime.datetime(2008, 5, 7, 22, 34, 10, 10101)),
            ('0000-00-00 00:00:00', None),
            ('1000-00-00 00:00:00', None),
        ]
        for data, exp in cases:
            self.assertEqual(exp, self.cnv._DATETIME_to_python(data))

    def test__YEAR_to_python(self):
        """Convert a MySQL YEAR to Python int"""
        data = '2008'
        exp = 2008

        self.assertEqual(exp, self.cnv._YEAR_to_python(data))
        data = 'foobar'
        self.assertRaises(ValueError, self.cnv._YEAR_to_python, data)

    def test__SET_to_python(self):
        """Convert a MySQL SET type to a Python sequence

        This actually calls hte _STRING_to_python() method since a SET is
        returned as string by MySQL. However, the description of the field
        has in it's field flags that the string is a SET.
        """
        data = 'val1,val2'
        exp = set(['val1', 'val2'])
        desc = ('foo', constants.FieldType.STRING,
                2, 3, 4, 5, 6, constants.FieldFlag.SET)
        res = self.cnv._STRING_to_python(data, desc)

        self.assertEqual(exp, res)

    def test__STRING_to_python_utf8(self):
        """Convert a UTF-8 MySQL STRING/VAR_STRING to a Python Unicode type"""
        self.cnv.set_charset('utf8')  # default
        data = '\xc3\xa4 utf8 string'
        exp = unicode(data, 'utf8')
        res = self.cnv._STRING_to_python(data)

        self.assertEqual(exp, res)

    def test__STRING_to_python_latin1(self):
        """Convert a ISO-8859-1 MySQL STRING/VAR_STRING to a Python str"""
        self.cnv.set_charset('latin1')
        self.cnv.set_unicode(False)
        data = '\xe4 latin string'
        exp = data
        res = self.cnv._STRING_to_python(data)

        self.assertEqual(exp, res)
        self.cnv.set_charset('utf8')
        self.cnv.set_unicode(True)

    def test__STRING_to_python_binary(self):
        """Convert a STRING BINARY to Python bytes type"""
        data = '\x33\xfd\x34\xed'
        desc = ('foo', constants.FieldType.STRING,
                2, 3, 4, 5, 6, constants.FieldFlag.BINARY)
        res = self.cnv._STRING_to_python(data, desc)

        self.assertEqual(data, res)

    def test__BLOB_to_python_binary(self):
        """Convert a BLOB BINARY to Python bytes type"""
        data = '\x33\xfd\x34\xed'
        desc = ('foo', constants.FieldType.BLOB,
                2, 3, 4, 5, 6, constants.FieldFlag.BINARY)
        res = self.cnv._BLOB_to_python(data, desc)

        self.assertEqual(data, res)
