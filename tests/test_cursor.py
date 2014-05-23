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
# Foundation, Incur., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA

"""Test module for bugs

Bug test cases specific to a particular Python (major) version are loaded
from py2.bugs or py3.bugs.

This module was originally located in python2/tests and python3/tests. It
should contain bug test cases which work for both Python v2 and v3.

Whenever a bug is bout to a specific Python version, put the test cases
in tests/py2/bugs.py or tests/py3/bugs.py. It might be that these files need
to be created first.
"""

import os
import re
import sys

import tests

if sys.version_info[0] == 2:
    from .py2.cursor import *
else:
    from .py3.cursor import *


from mysql.connector import (connection, cursor, conversion,
                             protocol, utils, errors, constants)


class CursorModule(tests.MySQLConnectorTests):

    """
    Tests for the cursor module functions and attributes
    """

    def test_RE_SQL_INSERT_VALUES(self):
        regex = cursor.RE_SQL_INSERT_VALUES

        cases = [
            ("(%s, %s)",
             "INSERT INTO t1 VALUES (%s, %s)"),
            ("( %s, \n  %s   )",
             "INSERT INTO t1 VALUES  ( %s, \n  %s   )"),
            ("(%(c1)s, %(c2)s)",
             "INSERT INTO t1 VALUES (%(c1)s, %(c2)s)"),
            ("(\n%(c1)s\n, \n%(c2)s\n)",
             "INSERT INTO t1 VALUES \n(\n%(c1)s\n, \n%(c2)s\n)"),
            ("(  %(c1)s  ,  %(c2)s  )",
             "INSERT INTO t1 VALUES   (  %(c1)s  ,  %(c2)s  ) ON DUPLICATE"),
            ("(%s, %s, NOW())",
             "INSERT INTO t1 VALUES (%s, %s, NOW())"),
            ("(%s, CONCAT('a', 'b'), %s, NOW())",
             "INSERT INTO t1 VALUES (%s, CONCAT('a', 'b'), %s, NOW())"),
            ("( NOW(),  %s, \n, CONCAT('a', 'b'), %s   )",
             "INSERT INTO t1 VALUES "
             " ( NOW(),  %s, \n, CONCAT('a', 'b'), %s   )"),
            ("(%(c1)s, NOW(6), %(c2)s)",
             "INSERT INTO t1 VALUES (%(c1)s, NOW(6), %(c2)s)"),
            ("(\n%(c1)s\n, \n%(c2)s, REPEAT('a', 20)\n)",
             "INSERT INTO t1 VALUES "
             "\n(\n%(c1)s\n, \n%(c2)s, REPEAT('a', 20)\n)"),
            ("(  %(c1)s  ,NOW(),REPEAT('a', 20)\n),  %(c2)s  )",
             "INSERT INTO t1 VALUES "
             " (  %(c1)s  ,NOW(),REPEAT('a', 20)\n),  %(c2)s  ) ON DUPLICATE"),
            ("(  %(c1)s, %(c2)s  )",
             "INSERT INTO `values` VALUES "
             "  (  %(c1)s, %(c2)s  ) ON DUPLICATE"),
        ]

        for exp, stmt in cases:
            self.assertEqual(exp, re.search(regex, stmt).group(1))


class CursorBaseTests(tests.MySQLConnectorTests):

    def setUp(self):
        self.cur = cursor.CursorBase()

    def test___init__(self):
        exp = {
            '_description': None,
            '_rowcount': -1,
            'arraysize': 1,
        }

        for key, value in exp.items():
            self.assertEqual(value, getattr(self.cur, key),
                             msg="Default for '%s' did not match." % key)

    def test_callproc(self):
        """CursorBase object callproc()-method"""
        self.check_method(self.cur, 'callproc')

        try:
            self.cur.callproc('foo', args=(1, 2, 3))
        except (SyntaxError, TypeError):
            self.fail("Cursor callproc(): wrong arguments")

    def test_close(self):
        """CursorBase object close()-method"""
        self.check_method(self.cur, 'close')

    def test_execute(self):
        """CursorBase object execute()-method"""
        self.check_method(self.cur, 'execute')

        try:
            self.cur.execute('select', params=(1, 2, 3))
        except (SyntaxError, TypeError):
            self.fail("Cursor execute(): wrong arguments")

    def test_executemany(self):
        """CursorBase object executemany()-method"""
        self.check_method(self.cur, 'executemany')

        try:
            self.cur.executemany('select', [()])
        except (SyntaxError, TypeError):
            self.fail("Cursor executemany(): wrong arguments")

    def test_fetchone(self):
        """CursorBase object fetchone()-method"""
        self.check_method(self.cur, 'fetchone')

    def test_fetchmany(self):
        """CursorBase object fetchmany()-method"""
        self.check_method(self.cur, 'fetchmany')

        try:
            self.cur.fetchmany(size=1)
        except (SyntaxError, TypeError):
            self.fail("Cursor fetchmany(): wrong arguments")

    def test_fetchall(self):
        """CursorBase object fetchall()-method"""
        self.check_method(self.cur, 'fetchall')

    def test_nextset(self):
        """CursorBase object nextset()-method"""
        self.check_method(self.cur, 'nextset')

    def test_setinputsizes(self):
        """CursorBase object setinputsizes()-method"""
        self.check_method(self.cur, 'setinputsizes')

        try:
            self.cur.setinputsizes((1,))
        except (SyntaxError, TypeError):
            self.fail("CursorBase setinputsizes(): wrong arguments")

    def test_setoutputsize(self):
        """CursorBase object setoutputsize()-method"""
        self.check_method(self.cur, 'setoutputsize')

        try:
            self.cur.setoutputsize(1, column=None)
        except (SyntaxError, TypeError):
            self.fail("CursorBase setoutputsize(): wrong arguments")

    def test_description(self):
        self.assertEqual(None, self.cur.description)
        self.assertEqual(self.cur._description, self.cur.description)
        self.cur._description = 'ham'
        self.assertEqual('ham', self.cur.description)
        if tests.OLD_UNITTEST:
            try:
                self.cur.description = 'spam'
            except AttributeError as err:
                # Exception should be raised
                pass
            else:
                self.fail("AttributeError was not raised")
        else:
            with self.assertRaises(AttributeError):
                self.cur.description = 'spam'

    def test_rowcount(self):
        self.assertEqual(-1, self.cur.rowcount)
        self.assertEqual(self.cur._rowcount, self.cur.rowcount)
        self.cur._rowcount = 2
        self.assertEqual(2, self.cur.rowcount)
        if tests.OLD_UNITTEST:
            try:
                self.cur.description = 'spam'
            except AttributeError as err:
                # Exception should be raised
                pass
            else:
                self.fail("AttributeError was not raised")
        else:
            with self.assertRaises(AttributeError):
                self.cur.rowcount = 3

    def test_last_insert_id(self):
        self.assertEqual(None, self.cur.lastrowid)
        self.assertEqual(self.cur._last_insert_id, self.cur.lastrowid)
        self.cur._last_insert_id = 2
        self.assertEqual(2, self.cur.lastrowid)
        if tests.OLD_UNITTEST:
            try:
                self.cur.description = 'spam'
            except AttributeError as err:
                # Exception should be raised
                pass
            else:
                self.fail("AttributeError was not raised")
        else:
            with self.assertRaises(AttributeError):
                self.cur.lastrowid = 3
