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

"""Unittests for mysql.connector.constants
"""

import tests
from mysql.connector import constants, errors


class Helpers(tests.MySQLConnectorTests):

    def test_flag_is_set(self):
        """Check if a particular flag/bit is set"""

        data = [
            1 << 3,
            1 << 5,
            1 << 7,
        ]
        flags = 0
        for flag in data:
            flags |= flag

        for flag in data:
            self.assertTrue(constants.flag_is_set(flag, flags))

        self.assertFalse(constants.flag_is_set(1 << 4, flags))

    def test_MAX_PACKET_LENGTH(self):
        """Check MAX_PACKET_LENGTH"""
        self.assertEqual(16777215, constants.MAX_PACKET_LENGTH)

    def test_NET_BUFFER_LENGTH(self):
        """Check NET_BUFFER_LENGTH"""
        self.assertEqual(8192, constants.NET_BUFFER_LENGTH)


class FieldTypeTests(tests.MySQLConnectorTests):

    desc = {
        'DECIMAL':       (0x00, 'DECIMAL'),
        'TINY':          (0x01, 'TINY'),
        'SHORT':         (0x02, 'SHORT'),
        'LONG':          (0x03, 'LONG'),
        'FLOAT':         (0x04, 'FLOAT'),
        'DOUBLE':        (0x05, 'DOUBLE'),
        'NULL':          (0x06, 'NULL'),
        'TIMESTAMP':     (0x07, 'TIMESTAMP'),
        'LONGLONG':      (0x08, 'LONGLONG'),
        'INT24':         (0x09, 'INT24'),
        'DATE':          (0x0a, 'DATE'),
        'TIME':          (0x0b, 'TIME'),
        'DATETIME':      (0x0c, 'DATETIME'),
        'YEAR':          (0x0d, 'YEAR'),
        'NEWDATE':       (0x0e, 'NEWDATE'),
        'VARCHAR':       (0x0f, 'VARCHAR'),
        'BIT':           (0x10, 'BIT'),
        'NEWDECIMAL':    (0xf6, 'NEWDECIMAL'),
        'ENUM':          (0xf7, 'ENUM'),
        'SET':           (0xf8, 'SET'),
        'TINY_BLOB':     (0xf9, 'TINY_BLOB'),
        'MEDIUM_BLOB':   (0xfa, 'MEDIUM_BLOB'),
        'LONG_BLOB':     (0xfb, 'LONG_BLOB'),
        'BLOB':          (0xfc, 'BLOB'),
        'VAR_STRING':    (0xfd, 'VAR_STRING'),
        'STRING':        (0xfe, 'STRING'),
        'GEOMETRY':      (0xff, 'GEOMETRY'),
    }

    type_groups = {
        'string': [
            constants.FieldType.VARCHAR,
            constants.FieldType.ENUM,
            constants.FieldType.VAR_STRING, constants.FieldType.STRING,
        ],
        'binary': [
            constants.FieldType.TINY_BLOB, constants.FieldType.MEDIUM_BLOB,
            constants.FieldType.LONG_BLOB, constants.FieldType.BLOB,
        ],
        'number': [
            constants.FieldType.DECIMAL, constants.FieldType.NEWDECIMAL,
            constants.FieldType.TINY, constants.FieldType.SHORT,
            constants.FieldType.LONG,
            constants.FieldType.FLOAT, constants.FieldType.DOUBLE,
            constants.FieldType.LONGLONG, constants.FieldType.INT24,
            constants.FieldType.BIT,
            constants.FieldType.YEAR,
        ],
        'datetime': [
            constants.FieldType.DATETIME, constants.FieldType.TIMESTAMP,
        ],
    }

    def test_attributes(self):
        """Check attributes for FieldType"""

        self.assertEqual('FIELD_TYPE_', constants.FieldType.prefix)

        for key, value in self.desc.items():
            self.assertTrue(key in constants.FieldType.__dict__,
                            '{0} is not an attribute of FieldType'.format(key))
            self.assertEqual(
                value[0], constants.FieldType.__dict__[key],
                '{0} attribute of FieldType has wrong value'.format(key))

    def test_get_desc(self):
        """Get field type by name"""

        for key, value in self.desc.items():
            exp = value[1]
            res = constants.FieldType.get_desc(key)
            self.assertEqual(exp, res)

        self.assertEqual(None, constants.FieldType.get_desc('FooBar'))

    def test_get_info(self):
        """Get field type by id"""

        for _, value in self.desc.items():
            exp = value[1]
            res = constants.FieldType.get_info(value[0])
            self.assertEqual(exp, res)

        self.assertEqual(None, constants.FieldType.get_info(999999999))

    def test_get_string_types(self):
        """DBAPI string types"""
        self.assertEqual(self.type_groups['string'],
                         constants.FieldType.get_string_types())

    def test_get_binary_types(self):
        """DBAPI string types"""
        self.assertEqual(self.type_groups['binary'],
                         constants.FieldType.get_binary_types())

    def test_get_number_types(self):
        """DBAPI number types"""
        self.assertEqual(self.type_groups['number'],
                         constants.FieldType.get_number_types())

    def test_get_timestamp_types(self):
        """DBAPI datetime types"""
        self.assertEqual(self.type_groups['datetime'],
                         constants.FieldType.get_timestamp_types())


class FieldFlagTests(tests.MySQLConnectorTests):

    desc = {
        'NOT_NULL': (1 << 0, "Field can't be NULL"),
        'PRI_KEY': (1 << 1, "Field is part of a primary key"),
        'UNIQUE_KEY': (1 << 2, "Field is part of a unique key"),
        'MULTIPLE_KEY': (1 << 3, "Field is part of a key"),
        'BLOB': (1 << 4, "Field is a blob"),
        'UNSIGNED': (1 << 5, "Field is unsigned"),
        'ZEROFILL': (1 << 6, "Field is zerofill"),
        'BINARY': (1 << 7, "Field is binary  "),
        'ENUM': (1 << 8, "field is an enum"),
        'AUTO_INCREMENT': (1 << 9, "field is a autoincrement field"),
        'TIMESTAMP': (1 << 10, "Field is a timestamp"),
        'SET': (1 << 11, "field is a set"),
        'NO_DEFAULT_VALUE': (1 << 12, "Field doesn't have default value"),
        'ON_UPDATE_NOW': (1 << 13, "Field is set to NOW on UPDATE"),
        'NUM': (1 << 14, "Field is num (for clients)"),

        'PART_KEY': (1 << 15, "Intern; Part of some key"),
        'GROUP': (1 << 14, "Intern: Group field"),   # Same as NUM
        'UNIQUE': (1 << 16, "Intern: Used by sql_yacc"),
        'BINCMP': (1 << 17, "Intern: Used by sql_yacc"),
        'GET_FIXED_FIELDS': (1 << 18, "Used to get fields in item tree"),
        'FIELD_IN_PART_FUNC': (1 << 19, "Field part of partition func"),
        'FIELD_IN_ADD_INDEX': (1 << 20, "Intern: Field used in ADD INDEX"),
        'FIELD_IS_RENAMED': (1 << 21, "Intern: Field is being renamed"),
    }

    def test_attributes(self):
        """Check attributes for FieldFlag"""

        self.assertEqual('', constants.FieldFlag._prefix)

        for key, value in self.desc.items():
            self.assertTrue(key in constants.FieldFlag.__dict__,
                            '{0} is not an attribute of FieldFlag'.format(key))
            self.assertEqual(
                value[0], constants.FieldFlag.__dict__[key],
                '{0} attribute of FieldFlag has wrong value'.format(key))

    def test_get_desc(self):
        """Get field flag by name"""

        for key, value in self.desc.items():
            exp = value[1]
            res = constants.FieldFlag.get_desc(key)
            self.assertEqual(exp, res)

    def test_get_info(self):
        """Get field flag by id"""

        for exp, info in self.desc.items():
            # Ignore the NUM/GROUP (bug in MySQL source code)
            if info[0] == 1 << 14:
                break
            res = constants.FieldFlag.get_info(info[0])
            self.assertEqual(exp, res)

    def test_get_bit_info(self):
        """Get names of the set flags"""

        data = 0
        data |= constants.FieldFlag.BLOB
        data |= constants.FieldFlag.BINARY
        exp = ['BINARY', 'BLOB'].sort()
        self.assertEqual(exp, constants.FieldFlag.get_bit_info(data).sort())


class ClientFlagTests(tests.MySQLConnectorTests):

    desc = {
        'LONG_PASSWD': (1 << 0, 'New more secure passwords'),
        'FOUND_ROWS': (1 << 1, 'Found instead of affected rows'),
        'LONG_FLAG': (1 << 2, 'Get all column flags'),
        'CONNECT_WITH_DB': (1 << 3, 'One can specify db on connect'),
        'NO_SCHEMA': (1 << 4, "Don't allow database.table.column"),
        'COMPRESS': (1 << 5, 'Can use compression protocol'),
        'ODBC': (1 << 6, 'ODBC client'),
        'LOCAL_FILES': (1 << 7, 'Can use LOAD DATA LOCAL'),
        'IGNORE_SPACE': (1 << 8, "Ignore spaces before ''"),
        'PROTOCOL_41': (1 << 9, 'New 4.1 protocol'),
        'INTERACTIVE': (1 << 10, 'This is an interactive client'),
        'SSL': (1 << 11, 'Switch to SSL after handshake'),
        'IGNORE_SIGPIPE': (1 << 12, 'IGNORE sigpipes'),
        'TRANSACTIONS': (1 << 13, 'Client knows about transactions'),
        'RESERVED': (1 << 14, 'Old flag for 4.1 protocol'),
        'SECURE_CONNECTION': (1 << 15, 'New 4.1 authentication'),
        'MULTI_STATEMENTS': (1 << 16, 'Enable/disable multi-stmt support'),
        'MULTI_RESULTS': (1 << 17, 'Enable/disable multi-results'),
        'SSL_VERIFY_SERVER_CERT': (1 << 30, ''),
        'REMEMBER_OPTIONS': (1 << 31, ''),
    }

    def test_attributes(self):
        """Check attributes for ClientFlag"""
        for key, value in self.desc.items():
            self.assertTrue(key in constants.ClientFlag.__dict__,
                            '{0} is not an attribute of FieldFlag'.format(key))
            self.assertEqual(
                value[0], constants.ClientFlag.__dict__[key],
                '{0} attribute of FieldFlag has wrong value'.format(key))

    def test_get_desc(self):
        """Get client flag by name"""
        for key, value in self.desc.items():
            exp = value[1]
            res = constants.ClientFlag.get_desc(key)
            self.assertEqual(exp, res)

    def test_get_info(self):
        """Get client flag by id"""
        for exp, info in self.desc.items():
            res = constants.ClientFlag.get_info(info[0])
            self.assertEqual(exp, res)

    def test_get_bit_info(self):
        """Get names of the set flags"""
        data = 0
        data |= constants.ClientFlag.LONG_FLAG
        data |= constants.ClientFlag.LOCAL_FILES
        exp = ['LONG_FLAG', 'LOCAL_FILES'].sort()
        self.assertEqual(exp, constants.ClientFlag.get_bit_info(data).sort())

    def test_get_default(self):
        """Get client flags which are set by default.
        """
        data = [
            constants.ClientFlag.LONG_PASSWD,
            constants.ClientFlag.LONG_FLAG,
            constants.ClientFlag.CONNECT_WITH_DB,
            constants.ClientFlag.PROTOCOL_41,
            constants.ClientFlag.TRANSACTIONS,
            constants.ClientFlag.SECURE_CONNECTION,
            constants.ClientFlag.MULTI_STATEMENTS,
            constants.ClientFlag.MULTI_RESULTS,
            constants.ClientFlag.LOCAL_FILES,
        ]
        exp = 0
        for option in data:
            exp |= option

        self.assertEqual(exp, constants.ClientFlag.get_default())


class CharacterSetTests(tests.MySQLConnectorTests):

    """Tests for constants.CharacterSet"""

    def test_get_info(self):
        """Get info about charset using MySQL ID"""
        exp = ('utf8', 'utf8_general_ci')
        data = 33
        self.assertEqual(exp, constants.CharacterSet.get_info(data))

        exception = errors.ProgrammingError
        data = 50000
        self.assertRaises(exception, constants.CharacterSet.get_info, data)

    def test_get_desc(self):
        """Get info about charset using MySQL ID as string"""
        exp = 'utf8/utf8_general_ci'
        data = 33
        self.assertEqual(exp, constants.CharacterSet.get_desc(data))

        exception = errors.ProgrammingError
        data = 50000
        self.assertRaises(exception, constants.CharacterSet.get_desc, data)

    def test_get_default_collation(self):
        """Get default collation for a given Character Set"""
        func = constants.CharacterSet.get_default_collation
        data = 'sjis'
        exp = ('sjis_japanese_ci', data, 13)
        self.assertEqual(exp, func(data))
        self.assertEqual(exp, func(exp[2]))

        exception = errors.ProgrammingError
        data = 'foobar'
        self.assertRaises(exception, func, data)

    def test_get_charset_info(self):
        """Get info about charset by name and collation"""
        func = constants.CharacterSet.get_charset_info
        exp = (209, 'utf8', 'utf8_esperanto_ci')
        data = exp[1:]

        self.assertEqual(exp, func(data[0], data[1]))
        self.assertEqual(exp, func(None, data[1]))
        self.assertEqual(exp, func(exp[0]))

        self.assertRaises(errors.ProgrammingError,
                          constants.CharacterSet.get_charset_info, 666)
        self.assertRaises(
            errors.ProgrammingError,
            constants.CharacterSet.get_charset_info, charset='utf8',
            collation='utf8_spam_ci')
        self.assertRaises(
            errors.ProgrammingError,
            constants.CharacterSet.get_charset_info,
            collation='utf8_spam_ci')

    def test_get_supported(self):
        """Get list of all supported character sets"""
        exp = (
            'big5', 'latin2', 'dec8', 'cp850', 'latin1', 'hp8', 'koi8r',
            'swe7', 'ascii', 'ujis', 'sjis', 'cp1251', 'hebrew', 'tis620',
            'euckr', 'latin7', 'koi8u', 'gb2312', 'greek', 'cp1250', 'gbk',
            'cp1257', 'latin5', 'armscii8', 'utf8', 'ucs2', 'cp866', 'keybcs2',
            'macce', 'macroman', 'cp852', 'utf8mb4','utf16', 'utf16le',
            'cp1256', 'utf32', 'binary', 'geostd8', 'cp932', 'eucjpms',
            'gb18030',
        )

        self.assertEqual(exp, constants.CharacterSet.get_supported())


class SQLModesTests(tests.MySQLConnectorTests):

    modes = (
        'REAL_AS_FLOAT',
        'PIPES_AS_CONCAT',
        'ANSI_QUOTES',
        'IGNORE_SPACE',
        'NOT_USED',
        'ONLY_FULL_GROUP_BY',
        'NO_UNSIGNED_SUBTRACTION',
        'NO_DIR_IN_CREATE',
        'POSTGRESQL',
        'ORACLE',
        'MSSQL',
        'DB2',
        'MAXDB',
        'NO_KEY_OPTIONS',
        'NO_TABLE_OPTIONS',
        'NO_FIELD_OPTIONS',
        'MYSQL323',
        'MYSQL40',
        'ANSI',
        'NO_AUTO_VALUE_ON_ZERO',
        'NO_BACKSLASH_ESCAPES',
        'STRICT_TRANS_TABLES',
        'STRICT_ALL_TABLES',
        'NO_ZERO_IN_DATE',
        'NO_ZERO_DATE',
        'INVALID_DATES',
        'ERROR_FOR_DIVISION_BY_ZERO',
        'TRADITIONAL',
        'NO_AUTO_CREATE_USER',
        'HIGH_NOT_PRECEDENCE',
        'NO_ENGINE_SUBSTITUTION',
        'PAD_CHAR_TO_FULL_LENGTH',
    )

    def test_get_info(self):
        for mode in SQLModesTests.modes:
            self.assertEqual(mode, getattr(constants.SQLMode, mode),
                             'Wrong info for SQL Mode {0}'.format(mode))

    def test_get_full_info(self):
        modes = tuple(sorted(SQLModesTests.modes))
        self.assertEqual(modes,
                         constants.SQLMode.get_full_info())


class ShutdownTypeTests(tests.MySQLConnectorTests):

    """Test COM_SHUTDOWN types"""
    desc = {
        'SHUTDOWN_DEFAULT': (
            0,
            "defaults to SHUTDOWN_WAIT_ALL_BUFFERS"
        ),
        'SHUTDOWN_WAIT_CONNECTIONS': (
            1,
            "wait for existing connections to finish"
        ),
        'SHUTDOWN_WAIT_TRANSACTIONS': (
            2,
            "wait for existing trans to finish"
        ),
        'SHUTDOWN_WAIT_UPDATES': (
            8,
            "wait for existing updates to finish"
        ),
        'SHUTDOWN_WAIT_ALL_BUFFERS': (
            16,
            "flush InnoDB and other storage engine buffers"
        ),
        'SHUTDOWN_WAIT_CRITICAL_BUFFERS': (
            17,
            "don't flush InnoDB buffers, flush other storage engines' buffers"
        ),
        'KILL_QUERY': (
            254, "(no description)"
        ),
        'KILL_CONNECTION': (
            255, "(no description)"
        ),
    }

    def test_attributes(self):
        """Check attributes for FieldType"""
        self.assertEqual('', constants.ShutdownType.prefix)

        for key, value in self.desc.items():
            self.assertTrue(key in constants.ShutdownType.__dict__,
                            '{0} is not an attribute of FieldType'.format(key))
            self.assertEqual(
                value[0], constants.ShutdownType.__dict__[key],
                '{0} attribute of ShutdownType has wrong value'.format(key))

    def test_get_desc(self):
        """Get field flag by name"""
        for key, value in self.desc.items():
            exp = value[1]
            res = constants.ShutdownType.get_desc(key)
            self.assertEqual(exp, res)
