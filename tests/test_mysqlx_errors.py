# Copyright (c) 2016, 2017, Oracle and/or its affiliates. All rights reserved.
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

"""Unittests for mysqlx.errors
"""

import unittest

import tests

from mysqlx import errors


@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 12), "XPlugin not compatible")
class ErrorsTests(tests.MySQLxTests):

    def test_get_mysql_exception(self):
        tests = {
            errors.ProgrammingError: (
                "24", "25", "26", "27", "28", "2A", "2C",
                "34", "35", "37", "3C", "3D", "3F", "42"),
            errors.DataError: ("02", "21", "22"),
            errors.NotSupportedError: ("0A",),
            errors.IntegrityError: ("23", "XA"),
            errors.InternalError: ("40", "44"),
            errors.OperationalError: ("08", "HZ", "0K"),
            errors.DatabaseError: ("07", "2B", "2D", "2E", "33", "ZZ", "HY"),
        }

        msg = "Ham"
        for exp, errlist in tests.items():
            for sqlstate in errlist:
                errno = 1000
                res = errors.get_mysql_exception(errno, msg, sqlstate)
                self.assertTrue(isinstance(res, exp),
                                "SQLState {0} should be {1}".format(
                                    sqlstate, exp.__name__))
                self.assertEqual(sqlstate, res.sqlstate)
                self.assertEqual("{0} ({1}): {2}".format(errno, sqlstate, msg),
                                 str(res))

        errno = 1064
        sqlstate = "42000"
        msg = "You have an error in your SQL syntax"
        exp = "1064 (42000): You have an error in your SQL syntax"
        err = errors.get_mysql_exception(errno, msg, sqlstate)
        self.assertEqual(exp, str(err))

        # Hardcoded exceptions
        self.assertTrue(isinstance(errors._ERROR_EXCEPTIONS, dict))
        self.assertTrue(
            isinstance(errors.get_mysql_exception(1243, None, None),
                       errors.ProgrammingError))

    def test_get_exception(self):
        ok_packet = bytearray(b"\x07\x00\x00\x01\x00\x01\x00\x00\x00\x01\x00")
        err_packet = bytearray(
            b"\x47\x00\x00\x02\xff\x15\x04\x23\x32\x38\x30\x30\x30"
            b"\x41\x63\x63\x65\x73\x73\x20\x64\x65\x6e\x69\x65\x64"
            b"\x20\x66\x6f\x72\x20\x75\x73\x65\x72\x20\x27\x68\x61"
            b"\x6d\x27\x40\x27\x6c\x6f\x63\x61\x6c\x68\x6f\x73\x74"
            b"\x27\x20\x28\x75\x73\x69\x6e\x67\x20\x70\x61\x73\x73"
            b"\x77\x6f\x72\x64\x3a\x20\x59\x45\x53\x29")
        self.assertTrue(isinstance(errors.get_exception(err_packet),
                                   errors.ProgrammingError))

        self.assertRaises(ValueError, errors.get_exception, ok_packet)

        res = errors.get_exception(bytearray(b"\x47\x00\x00\x02\xff\x15"))
        self.assertTrue(isinstance(res, errors.InterfaceError))


class ErrorTest(tests.MySQLxTests):

    def test___init__(self):
        self.assertTrue(issubclass(errors.Error, Exception))

        err = errors.Error()
        self.assertEqual(-1, err.errno)
        self.assertEqual("Unknown error", err.msg)
        self.assertEqual(None, err.sqlstate)

        msg = "Ham"
        err = errors.Error(msg, errno=1)
        self.assertEqual(1, err.errno)
        self.assertEqual("1: {0}".format(msg), err._full_msg)
        self.assertEqual(msg, err.msg)

        err = errors.Error("Ham", errno=1, sqlstate="SPAM")
        self.assertEqual(1, err.errno)
        self.assertEqual("1 (SPAM): Ham", err._full_msg)
        self.assertEqual("1 (SPAM): Ham", str(err))

        err = errors.Error(errno=2000)
        self.assertEqual("Unknown MySQL error", err.msg)
        self.assertEqual("2000: Unknown MySQL error", err._full_msg)

        err = errors.Error(errno=2003, values=("/path/to/ham", 2))
        self.assertEqual(
            "2003: Can't connect to MySQL server on '/path/to/ham' (2)",
            err._full_msg)
        self.assertEqual(
            "Can't connect to MySQL server on '/path/to/ham' (2)",
            err.msg)

        err = errors.Error(errno=2001, values=("ham",))
        if "(Warning:" in str(err):
            self.fail("Found %d in error message.")

        err = errors.Error(errno=2003, values=("ham",))
        self.assertEqual(
            "2003: Can't connect to MySQL server on '%-.100s' (%s) "
            "(Warning: not enough arguments for format string)",
            err._full_msg)

    def test___str__(self):
        msg = "Spam"
        self.assertEqual("Spam", str(errors.Error(msg)))
        self.assertEqual("1: Spam", str(errors.Error(msg, 1)))
        self.assertEqual("1 (XYZ): Spam",
                         str(errors.Error(msg, 1, sqlstate="XYZ")))


class InterfaceErrorTests(tests.MySQLxTests):

    def test___init__(self):
        self.assertTrue(issubclass(errors.InterfaceError, errors.Error))


class DatabaseErrorTests(tests.MySQLxTests):

    def test___init__(self):
        self.assertTrue(issubclass(errors.DatabaseError, errors.Error))


class InternalErrorTests(tests.MySQLxTests):

    def test___init__(self):
        self.assertTrue(issubclass(errors.InternalError,
                                   errors.DatabaseError))


class OperationalErrorTests(tests.MySQLxTests):

    def test___init__(self):
        self.assertTrue(issubclass(errors.OperationalError,
                                   errors.DatabaseError))


class ProgrammingErrorTests(tests.MySQLxTests):

    def test___init__(self):
        self.assertTrue(issubclass(errors.ProgrammingError,
                                   errors.DatabaseError))


class IntegrityErrorTests(tests.MySQLxTests):

    def test___init__(self):
        self.assertTrue(issubclass(errors.IntegrityError,
                                   errors.DatabaseError))


class DataErrorTests(tests.MySQLxTests):

    def test___init__(self):
        self.assertTrue(issubclass(errors.DataError, errors.DatabaseError))


class NotSupportedErrorTests(tests.MySQLxTests):

    def test___init__(self):
        self.assertTrue(issubclass(errors.NotSupportedError,
                                   errors.DatabaseError))


class PoolErrorTests(tests.MySQLxTests):

    def test___init__(self):
        self.assertTrue(issubclass(errors.PoolError, errors.Error))
