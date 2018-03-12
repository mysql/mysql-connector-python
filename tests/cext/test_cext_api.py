# -*- coding: utf-8 -*-

# Copyright (c) 2014, 2018, Oracle and/or its affiliates. All rights reserved.
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

"""Testing the C Extension MySQL C API
"""

import logging
import os
import re
import unittest

import tests

from mysql.connector.constants import ServerFlag, ClientFlag
try:
    from _mysql_connector import MySQL, MySQLError, MySQLInterfaceError
except ImportError:
    CEXT_MYSQL_AVAILABLE = False
else:
    CEXT_MYSQL_AVAILABLE = True

LOGGER = logging.getLogger(tests.LOGGER_NAME)


def get_variables(cnx, pattern=None, variables=None, global_vars=False):
    """Get session or global system variables

    We use the MySQL connection cnx to query the INFORMATION_SCHEMA
    table SESSION_VARIABLES or, when global_vars is True, the table
    GLOBAL_VARIABLES.

    :param cnx: Valid MySQL connection
    :param pattern: Pattern to use (used for LIKE)
    :param variables: Variables to query for
    :return: Dictionary containing variables with values or empty dict
    :rtype : dict
    """

    format_vars = {
        'where_clause': '',
        'where': '',
    }
    ver = cnx.get_server_version()
    if ver >= (5, 7, 6):
        table_global_vars = 'global_variables'
        table_session_vars = 'session_variables'
        format_vars['schema'] = 'performance_schema'
    else:
        table_global_vars = 'GLOBAL_VARIABLES'
        table_session_vars = 'SESSION_VARIABLES'
        format_vars['schema'] = 'INFORMATION_SCHEMA'

    if global_vars is True:
        format_vars['table'] = table_global_vars
    else:
        format_vars['table'] = table_session_vars

    query = "SELECT * FROM {schema}.{table} {where} {where_clause}"

    where = []
    if pattern:
        where.append('VARIABLE_NAME LIKE "{0}"'.format(pattern))
    if variables:
        where.append('VARIABLE_NAME IN ({0})'.format(
            ','.join([ "'{0}'".format(name) for name in variables ])
        ))

    if where:
        format_vars['where'] = 'WHERE'
        format_vars['where_clause'] = ' OR '.join(where)

    cnx.query(query.format(**format_vars))
    result = {}

    row = cnx.fetch_row()
    while row:
        result[row[0].lower()] = row[1]
        row = cnx.fetch_row()

    cnx.free_result()
    return result

def fetch_rows(cnx, query=None):
    """Execute query and fetch first result set

    This function will use connection cnx and execute the query. All
    rows are then returned as a list of tuples.

    :param cnx: Valid MySQL connection
    :param query: SQL statement to execute
    :return: List of tuples
    :rtype: list
    """
    rows = []
    if query:
        cnx.query(query)

    if cnx.have_result_set:
        row = cnx.fetch_row()
        while row:
            rows.append(row)
            row = cnx.fetch_row()

    if cnx.next_result():
        raise Exception("fetch_rows does not work with multi results")

    cnx.free_result()
    return rows

@unittest.skipIf(CEXT_MYSQL_AVAILABLE == False, "C Extension not available")
class CExtMySQLTests(tests.MySQLConnectorTests):
    """Test the MySQL class in the C Extension"""

    def setUp(self):
        self.config = tests.get_mysql_config()

        connect_args = [
            "host", "user", "password", "database",
            "port", "unix_socket", "client_flags"
        ]
        self.connect_kwargs = {}
        for key, value in self.config.items():
            if key in connect_args:
                self.connect_kwargs[key] = value

        if 'client_flags' not in self.connect_kwargs:
            self.connect_kwargs['client_flags'] = ClientFlag.get_default()

    def test___init__(self):
        cmy = MySQL()
        self.assertEqual(False, cmy.buffered())
        self.assertEqual(False, cmy.raw())

        cmy = MySQL(buffered=True, raw=True)
        self.assertEqual(True, cmy.buffered())
        self.assertEqual(True, cmy.raw())

        exp = 'gbk'
        cmy = MySQL(charset_name=exp)
        cmy.connect(**self.connect_kwargs)
        self.assertEqual(exp, cmy.character_set_name())

    def test_buffered(self):
        cmy = MySQL()
        self.assertEqual(False, cmy.buffered())
        cmy.buffered(True)
        self.assertEqual(True, cmy.buffered())
        cmy.buffered(False)
        self.assertEqual(False, cmy.buffered())

        self.assertRaises(TypeError, cmy.buffered, 'a')

    def test_raw(self):
        cmy = MySQL()
        self.assertEqual(False, cmy.raw())
        cmy.raw(True)
        self.assertEqual(True, cmy.raw())
        cmy.raw(False)
        self.assertEqual(False, cmy.raw())

        self.assertRaises(TypeError, cmy.raw, 'a')

    def test_connected(self):
        config = self.connect_kwargs.copy()
        cmy = MySQL()
        self.assertFalse(cmy.connected())
        cmy.connect(**config)
        self.assertTrue(cmy.connected())
        cmy.close()
        self.assertFalse(cmy.connected())

    def test_connect(self):
        config = self.connect_kwargs.copy()
        cmy = MySQL()

        self.assertFalse(cmy.ping())

        # Using Unix Socket
        cmy.connect(**config)
        self.assertTrue(cmy.ping())

        # Using TCP
        config['unix_socket'] = None
        cmy.connect(**config)
        self.assertTrue(cmy.ping())

        self.assertEqual(None, cmy.close())
        self.assertFalse(cmy.ping())
        self.assertEqual(None, cmy.close())

    def test_close(self):
        """
        MySQL_close() is being tested in test_connected

        Unless something needs to be tested additionally, leave this
        test case as placeholder.
        """
        pass

    def test_ping(self):
        """
        MySQL_ping() is being tested in test_connected

        Unless something needs to be tested additionally, leave this
        test case as placeholder.
        """
        pass

    def test_escape_string(self):
        cases = [
            ('new\nline', b'new\\nline'),
            ('carriage\rreturn', b'carriage\\rreturn'),
            ('control\x1aZ', b'control\\ZZ'),
            ("single'quote", b"single\\'quote"),
            ('double"quote', b'double\\"quote'),
            ('back\slash', b'back\\\\slash'),
            ('nul\0char', b'nul\\0char'),
            (u"Kangxi⽃\0⽇", b'Kangxi\xe2\xbd\x83\\0\xe2\xbd\x87'),
            (b'bytes\0ob\'j\n"ct\x1a', b'bytes\\0ob\\\'j\\n\\"ct\\Z'),
        ]

        cmy = MySQL()
        cmy.connect(**self.connect_kwargs)

        unicode_string = u"Kangxi⽃\0⽇"
        self.assertRaises(UnicodeEncodeError, cmy.escape_string, unicode_string)

        cmy.set_character_set("UTF8")

        for value, exp in cases:
            self.assertEqual(exp, cmy.escape_string(value))

        self.assertRaises(TypeError, cmy.escape_string, 1234);

    def test_get_character_set_info(self):
        cmy = MySQL()
        self.assertRaises(MySQLInterfaceError, cmy.get_character_set_info)
        cmy.connect(**self.connect_kwargs)

        # We go by the default of MySQL, which is latin1/swedish_ci
        exp = {'comment': '', 'name': 'latin1_swedish_ci',
               'csname': 'latin1', 'mbmaxlen': 1, 'number': 8, 'mbminlen': 1}
        result = cmy.get_character_set_info()
        # make 'comment' deterministic
        result['comment'] = ''
        self.assertEqual(exp, result)

        cmy.query("SET NAMES utf8")
        cmy.set_character_set('utf8')

        exp = {'comment': '', 'name': 'utf8_general_ci',
               'csname': 'utf8', 'mbmaxlen': 3, 'number': 33, 'mbminlen': 1}
        result = cmy.get_character_set_info()
        # make 'comment' deterministic
        result['comment'] = ''
        self.assertEqual(exp, result)

    def test_get_proto_info(self):
        cmy = MySQL()
        self.assertRaises(MySQLInterfaceError, cmy.get_proto_info)

        cmy.connect(**self.connect_kwargs)
        self.assertEqual(10, cmy.get_proto_info())

    def test_get_server_info(self):
        cmy = MySQL()
        self.assertRaises(MySQLInterfaceError, cmy.get_server_info)

        cmy.connect(**self.connect_kwargs)
        version = cmy.get_server_version()
        info = cmy.get_server_info()
        self.assertIsInstance(info, str)
        self.assertTrue(info.startswith('.'.join([str(v) for v in version])))

    def test_get_server_version(self):
        cmy = MySQL()
        self.assertRaises(MySQLInterfaceError, cmy.get_server_version)

        cmy.connect(**self.connect_kwargs)
        version = cmy.get_server_version()
        self.assertIsInstance(version, tuple)
        self.assertEqual(3, len(version))
        self.assertTrue(all([isinstance(v, int) for v in version]))

        self.assertTrue(3 < version[0] < 9)
        self.assertTrue(0 <= version[1] < 20)
        self.assertTrue(0 < version[2] < 99)

    def test_thread_id(self):
        cmy = MySQL()
        self.assertRaises(MySQLInterfaceError, cmy.thread_id)

        cmy.connect(**self.connect_kwargs)

        if tests.PY2:
            self.assertIsInstance(cmy.thread_id(), long)
        else:
            self.assertIsInstance(cmy.thread_id(), int)
        self.assertGreater(cmy.thread_id(), 0)
        thread_id = cmy.thread_id()
        cmy.close()

        self.assertRaises(MySQLError, cmy.thread_id)

    def test_select_db(self):
        cmy = MySQL(buffered=True)
        cmy.connect(**self.connect_kwargs)

        cmy.select_db('mysql')
        cmy.query("SELECT DATABASE()")
        self.assertEqual(b'mysql', cmy.fetch_row()[0])
        cmy.free_result()

        cmy.select_db('myconnpy')
        cmy.query("SELECT DATABASE()")
        self.assertEqual(b'myconnpy', cmy.fetch_row()[0])
        cmy.free_result()

    def test_affected_rows(self):
        cmy = MySQL(buffered=True)
        cmy.connect(**self.connect_kwargs)

        table = "affected_rows"

        cmy.select_db('myconnpy')
        cmy.query("DROP TABLE IF EXISTS {0}".format(table))
        cmy.query("CREATE TABLE {0} (c1 INT, c2 INT)".format(table))

        cmy.query("INSERT INTO {0} (c1, c2) VALUES "
                  "(1, 10), (2, 20), (3, 30)".format(table))
        self.assertEqual(3, cmy.affected_rows())

        cmy.query("UPDATE {0} SET c2 = c2 + 1 WHERE c1 < 3".format(table))
        self.assertEqual(2, cmy.affected_rows())

        cmy.query("DELETE FROM {0} WHERE c1 IN (1, 2, 3)".format(table))
        self.assertEqual(3, cmy.affected_rows())

        cmy.query("DROP TABLE IF EXISTS {0}".format(table))

    def test_field_count(self):
        cmy = MySQL(buffered=True)
        cmy.connect(**self.connect_kwargs)

        table = "field_count"

        cmy.select_db('myconnpy')
        cmy.query("DROP TABLE IF EXISTS {0}".format(table))
        cmy.query("CREATE TABLE {0} (c1 INT, c2 INT, c3 INT)".format(table))

        cmy.query("SELECT * FROM {0}".format(table))
        self.assertEqual(3, cmy.field_count())
        cmy.free_result()

        cmy.query("INSERT INTO {0} (c1, c2, c3) VALUES "
                  "(1, 10, 100)".format(table))
        cmy.commit()

        cmy.query("SELECT * FROM {0}".format(table))
        self.assertEqual(3, cmy.field_count())
        cmy.free_result()

        cmy.query("DROP TABLE IF EXISTS {0}".format(table))

    def test_autocommit(self):
        cmy1 = MySQL(buffered=True)
        cmy1.connect(**self.connect_kwargs)
        cmy2 = MySQL(buffered=True)
        cmy2.connect(**self.connect_kwargs)

        self.assertRaises(ValueError, cmy1.autocommit, 'ham')
        self.assertRaises(ValueError, cmy1.autocommit, 1)
        self.assertRaises(ValueError, cmy1.autocommit, None)

        table = "autocommit_test"

        # For the test we start off by making sure the autocommit is off
        # for both sessions
        cmy1.query("SELECT @@global.autocommit")
        if cmy1.fetch_row()[0] != 1:
            cmy1.query("SET @@session.autocommit = 0")
            cmy2.query("SET @@session.autocommit = 0")

        cmy1.query("DROP TABLE IF EXISTS {0}".format(table))
        cmy1.query("CREATE TABLE {0} (c1 INT)".format(table))

        # Turn AUTOCOMMIT on
        cmy1.autocommit(True)
        cmy1.query("INSERT INTO {0} (c1) VALUES "
                   "(1), (2), (3)".format(table))

        cmy2.query("SELECT * FROM {0}".format(table))
        self.assertEqual(3, cmy2.num_rows())
        rows = fetch_rows(cmy2)

        # Turn AUTOCOMMIT off
        cmy1.autocommit(False)
        cmy1.query("INSERT INTO {0} (c1) VALUES "
                   "(4), (5), (6)".format(table))

        cmy2.query("SELECT * FROM {0} WHERE c1 > 3".format(table))
        self.assertEqual([], fetch_rows(cmy2))

        cmy1.commit()
        cmy2.query("SELECT * FROM {0} WHERE c1 > 3".format(table))
        self.assertEqual([(4,), (5,), (6,)], fetch_rows(cmy2))

        cmy1.query("DROP TABLE IF EXISTS {0}".format(table))
        cmy1.close()
        cmy2.close()

    def test_commit(self):
        cmy1 = MySQL(buffered=True)
        cmy1.connect(**self.connect_kwargs)
        cmy2 = MySQL(buffered=True)
        cmy2.connect(**self.connect_kwargs)

        table = "commit_test"

        cmy1.query("DROP TABLE IF EXISTS {0}".format(table))
        cmy1.query("CREATE TABLE {0} (c1 INT)".format(table))

        cmy1.query("START TRANSACTION")
        cmy1.query("INSERT INTO {0} (c1) VALUES "
                   "(1), (2), (3)".format(table))

        cmy2.query("SELECT * FROM {0}".format(table))
        self.assertEqual([], fetch_rows(cmy2))

        cmy1.commit()

        cmy2.query("SELECT * FROM {0}".format(table))
        self.assertEqual([(1,), (2,), (3,)], fetch_rows(cmy2))

        cmy1.query("DROP TABLE IF EXISTS {0}".format(table))

    @unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 0), "Plugin unavailable.")
    def test_change_user(self):
        connect_kwargs = self.connect_kwargs.copy()
        connect_kwargs['unix_socket'] = None
        connect_kwargs['ssl_disabled'] = False
        cmy1 = MySQL(buffered=True)
        cmy1.connect(**connect_kwargs)
        cmy2 = MySQL(buffered=True)
        cmy2.connect(**connect_kwargs)

        new_user = {
            'user': 'cextuser',
            'host': self.config['host'],
            'database': self.connect_kwargs['database'],
            'password': 'connc',
        }

        try:
            cmy1.query("DROP USER '{user}'@'{host}'".format(**new_user))
        except MySQLInterfaceError:
            # Probably not created
            pass

        stmt = ("CREATE USER '{user}'@'{host}' IDENTIFIED WITH "
                "caching_sha2_password").format(**new_user)
        cmy1.query(stmt)
        if tests.MYSQL_VERSION < (8, 0, 5):
            cmy1.query("SET old_passwords = 0")
            res = cmy1.query("SET PASSWORD FOR '{user}'@'{host}' = "
                             "PASSWORD('{password}')".format(**new_user))
        else:
            res = cmy1.query("ALTER USER '{user}'@'{host}' IDENTIFIED BY "
                             "'{password}'".format(**new_user))
        cmy1.query("GRANT ALL ON {database}.* "
                   "TO '{user}'@'{host}'".format(**new_user))

        cmy2.query("SHOW GRANTS FOR {user}@{host}".format(**new_user))
        cmy2.query("SELECT USER()")
        orig_user = cmy2.fetch_row()[0]
        cmy2.free_result()
        cmy2.change_user(user=new_user['user'], password=new_user['password'],
                         database=new_user['database'])

        cmy2.query("SELECT USER()")
        current_user = cmy2.fetch_row()[0]
        self.assertNotEqual(orig_user, current_user)
        self.assertTrue(
            u"{user}@".format(**new_user) in current_user.decode('utf8'))
        cmy2.free_result()

    def test_character_set_name(self):
        cmy1 = MySQL(buffered=True)
        self.assertRaises(MySQLInterfaceError, cmy1.character_set_name)

        cmy1.connect(**self.connect_kwargs)

        self.assertEqual('latin1', cmy1.character_set_name())

    def test_set_character_set(self):
        cmy1 = MySQL(buffered=True)
        self.assertRaises(MySQLInterfaceError, cmy1.set_character_set, 'latin2')

        cmy1.connect(**self.connect_kwargs)
        orig = cmy1.character_set_name()

        cmy1.set_character_set('utf8')
        charset = cmy1.character_set_name()
        self.assertNotEqual(orig, charset)
        self.assertEqual('utf8', charset)

        self.assertRaises(MySQLInterfaceError,
                          cmy1.set_character_set, 'ham_spam')

        variables = ('character_set_connection',)
        exp = {b'character_set_connection': b'utf8',}
        self.assertEqual(exp, get_variables(cmy1, variables=variables))

        exp = {b'character_set_connection': b'big5',}
        cmy1.set_character_set('big5')
        self.assertEqual(exp, get_variables(cmy1, variables=variables))

    @unittest.skipIf(tests.MYSQL_VERSION == (5, 7, 4),
                     "test_get_ssl_cipher not tested with MySQL version 5.7.4")
    def test_get_ssl_cipher(self):
        cmy1 = MySQL(buffered=True)
        self.assertRaises(MySQLInterfaceError, cmy1.get_ssl_cipher)

        cmy1.connect(**self.connect_kwargs)
        self.assertEqual(None, cmy1.get_ssl_cipher())

    def test_hex_string(self):
        config = self.connect_kwargs.copy()
        cmy = MySQL(buffered=True)

        table = "hex_string"

        cases = {
            'utf8': [
                (u'ham', b"X'68616D'"),
            ],
            'big5': [
                (u'\u5C62', b"X'B9F0'")
            ],
            'sjis': [
                (u'\u005c', b"X'5C'"),
            ],
            'gbk': [
                (u'赵孟頫', b"X'D5D4C3CFEE5C'"),
                (u'赵\孟\頫\\', b"X'D5D45CC3CF5CEE5C5C'"),
                (u'遜', b"X'DF64'")
            ],
            'ascii': [
                ('\x5c\x00\x5c', b"X'5C005C'"),
            ],
        }

        cmy.connect(**config)

        def create_table(charset):
            cmy.query("DROP TABLE IF EXISTS {0}".format(table))
            cmy.query("CREATE TABLE {0} (id INT, "
                      "c1 VARCHAR(400)) CHARACTER SET {1}".format(
                table, charset))

        insert = "INSERT INTO {0} (id, c1) VALUES ({{id}}, {{hex}})".format(
            table)
        select = "SELECT c1 FROM {0} WHERE id = {{id}}".format(table)

        for encoding, data in cases.items():
            create_table(encoding)
            for i, info in enumerate(data):
                case, exp = info
                cmy.set_character_set(encoding)
                hexed = cmy.hex_string(case.encode(encoding))
                self.assertEqual(exp, hexed)
                cmy.query(insert.format(id=i, hex=hexed.decode()))
                cmy.query(select.format(id=i))
                try:
                    fetched = fetch_rows(cmy)[0][0]
                except UnicodeEncodeError:
                    self.fail("Could not encode {0}".format(encoding))
                self.assertEqual(case, fetched.decode(encoding),
                                 "Failed with case {0}/{1}".format(i, encoding))

        cmy.query("DROP TABLE IF EXISTS {0}".format(table))

    def test_insert_id(self):
        cmy = MySQL(buffered=True)
        cmy.connect(**self.connect_kwargs)

        table = "insert_id_test"
        cmy.query("DROP TABLE IF EXISTS {0}".format(table))
        cmy.query("CREATE TABLE {0} (id INT AUTO_INCREMENT KEY)".format(table))

        self.assertEqual(0, cmy.insert_id())

        cmy.query("INSERT INTO {0} VALUES ()".format(table))
        self.assertEqual(1, cmy.insert_id())

        # Multiple-row
        cmy.query("INSERT INTO {0} VALUES (), ()".format(table))
        self.assertEqual(2, cmy.insert_id())

        cmy.query("DROP TABLE IF EXISTS {0}".format(table))

    def test_warning_count(self):
        cmy = MySQL()
        cmy.connect(**self.connect_kwargs)

        cmy.query("SELECT 'a' + 'b'", buffered=False)
        fetch_rows(cmy)
        self.assertEqual(2, cmy.warning_count())

        cmy.query("SELECT 1 + 1", buffered=True)
        self.assertEqual(0, cmy.warning_count())
        fetch_rows(cmy)

    def test_get_client_info(self):
        cmy = MySQL(buffered=True)

        match = re.match(r"(\d+\.\d+.\d+)(.*)", cmy.get_client_info())
        self.assertNotEqual(None, match)

    def test_get_client_version(self):
        cmy = MySQL(buffered=True)

        version = cmy.get_client_version()
        self.assertTrue(isinstance(version, tuple))
        self.assertTrue(all([ isinstance(v, int) for v in version]))

    def test_get_host_info(self):
        config = self.connect_kwargs.copy()
        cmy = MySQL(buffered=True)
        self.assertRaises(MySQLInterfaceError, cmy.get_host_info)

        cmy.connect(**config)

        if os.name == 'posix':
            # On POSIX systems we would be connected by UNIX socket
            self.assertTrue('via UNIX socket' in cmy.get_host_info())

        # Connect using TCP/IP
        config['unix_socket'] = None
        cmy.connect(**config)
        self.assertTrue('via TCP/IP' in cmy.get_host_info())

    def test_query(self):
        config = self.connect_kwargs.copy()
        cmy = MySQL(buffered=True)
        self.assertRaises(MySQLInterfaceError, cmy.query)

        cmy.connect(**config)

        self.assertRaises(MySQLInterfaceError, cmy.query, "SELECT spam")


        self.assertTrue(cmy.query("SET @ham = 4"))
        self.assertEqual(None, cmy.num_fields())
        self.assertEqual(0, cmy.field_count())


        self.assertTrue(cmy.query("SELECT @ham"))
        self.assertEqual(4, cmy.fetch_row()[0])
        self.assertEqual(None, cmy.fetch_row())
        cmy.free_result()

        self.assertTrue(cmy.query("SELECT 'ham', 'spam', 5", raw=True))
        row = cmy.fetch_row()
        self.assertTrue(isinstance(row[0], bytearray))
        self.assertEqual(bytearray(b'spam'), row[1])
        self.assertEqual(None, cmy.fetch_row())
        cmy.free_result()

    def test_st_server_status(self):
        config = self.connect_kwargs.copy()
        cmy = MySQL(buffered=True)

        self.assertEqual(0, cmy.st_server_status())

        cmy.connect(**config)
        self.assertTrue(
            cmy.st_server_status() & ServerFlag.STATUS_AUTOCOMMIT)
        cmy.autocommit(False)
        self.assertFalse(
            cmy.st_server_status() & ServerFlag.STATUS_AUTOCOMMIT)

        cmy.query("START TRANSACTION")
        self.assertTrue(
            cmy.st_server_status() & ServerFlag.STATUS_IN_TRANS)
        cmy.query("ROLLBACK")
        self.assertFalse(
            cmy.st_server_status() & ServerFlag.STATUS_IN_TRANS)

    def test_rollback(self):
        cmy1 = MySQL(buffered=True)
        cmy1.connect(**self.connect_kwargs)
        cmy2 = MySQL(buffered=True)
        cmy2.connect(**self.connect_kwargs)

        table = "commit_test"

        cmy1.query("DROP TABLE IF EXISTS {0}".format(table))
        cmy1.query("CREATE TABLE {0} (c1 INT)".format(table))

        cmy1.query("START TRANSACTION")
        cmy1.query("INSERT INTO {0} (c1) VALUES "
                   "(1), (2), (3)".format(table))
        cmy1.commit()

        cmy2.query("SELECT * FROM {0}".format(table))
        self.assertEqual([(1,), (2,), (3,)], fetch_rows(cmy2))

        cmy1.query("START TRANSACTION")
        cmy1.query("INSERT INTO {0} (c1) VALUES "
                   "(4), (5), (6)".format(table))
        cmy1.rollback()

        cmy2.query("SELECT * FROM {0}".format(table))
        self.assertEqual(3, cmy2.num_rows())

        cmy1.query("DROP TABLE IF EXISTS {0}".format(table))

    def test_next_result(self):
        cmy = MySQL()
        cmy.connect(**self.connect_kwargs)

        table = "next_result_test"

        cmy.query("DROP TABLE IF EXISTS {0}".format(table))
        cmy.query("CREATE TABLE {0} (c1 INT AUTO_INCREMENT KEY)".format(table))

        var_names = ('"HAVE_CRYPT"', '"CHARACTER_SET_CONNECTION"')
        queries = (
            "SELECT 'HAM'",
            "INSERT INTO {0} () VALUES ()".format(table),
            "SELECT 'SPAM'",
        )
        exp = [
            [(b'HAM',)],
            {'insert_id': 1, 'affected': 1},
            [(b'SPAM',)]
        ]

        result = []
        have_more = cmy.query(';'.join(queries))
        self.assertTrue(have_more)
        while have_more:
            if cmy.have_result_set:
                rows = []
                row = cmy.fetch_row()
                while row:
                    rows.append(row)
                    row = cmy.fetch_row()
                result.append(rows)
            else:
                result.append({
                    "affected": cmy.affected_rows(),
                    "insert_id": cmy.insert_id()
                })
            have_more = cmy.next_result()

        self.assertEqual(exp, result)
        cmy.query("DROP TABLE IF EXISTS {0}".format(table))
