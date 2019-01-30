# -*- coding: utf-8 -*-

# Copyright (c) 2009, 2018, Oracle and/or its affiliates. All rights reserved.
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

"""Unittests for examples
"""

from hashlib import md5
import sys
import unittest
import tests

from . import PY2
import mysql.connector

try:
    from mysql.connector.connection_cext import CMySQLConnection
except ImportError:
    # Test without C Extension
    CMySQLConnection = None


class TestExamples(tests.MySQLConnectorTests):

    def setUp(self):
        self.config = tests.get_mysql_config()
        self.config['use_pure'] = True
        self.cnx = mysql.connector.connect(**self.config)

    def tearDown(self):
        self.cnx.close()

    def _exec_main(self, example, exp=None):
        try:
            result = example.main(self.config)
            if not exp:
                return result
        except Exception as err:
            self.fail(err)

        md5_result = md5()
        output = u'\n'.join(result)
        md5_result.update(output.encode('utf-8'))

        self.assertEqual(exp, md5_result.hexdigest(),
                         'Output was not correct')

    def test_dates(self):
        """examples/dates.py"""
        try:
            import examples.dates as example
        except Exception as err:
            self.fail(err)
        output = example.main(self.config)
        exp = ['  1 | 1977-06-14 | 1977-06-14 21:10:00 | 21:10:00 |',
               '  2 |       None |                None |  0:00:00 |',
               '  3 |       None |                None |  0:00:00 |']
        self.assertEqual(output, exp)

        example.DATA.append(('0000-00-00', None, '00:00:00'),)
        self.assertRaises(mysql.connector.errors.IntegrityError,
                          example.main, self.config)

        sys.modules.pop('examples.dates', None)

    def test_engines(self):
        """examples/engines.py"""
        try:
            import examples.engines as example
        except:
            self.fail()
        output = self._exec_main(example)

        # Can't check output as it might be different per MySQL instance
        # We check only if MyISAM is present
        found = False
        for line in output:
            if line.find('MyISAM') > -1:
                found = True
                break

        self.assertTrue(found, 'MyISAM engine not found in output')

        sys.modules.pop('examples.engine', None)

    def test_inserts(self):
        """examples/inserts.py"""
        try:
            import examples.inserts as example
        except Exception as err:
            self.fail(err)
        exp = '077dcd0139015c0aa6fb82ed932f053e'
        self._exec_main(example, exp)

        sys.modules.pop('examples.inserts', None)

    def test_transactions(self):
        """examples/transactions.py"""
        db = mysql.connector.connect(**self.config)
        r = tests.have_engine(db, 'InnoDB')
        db.close()
        if not r:
            return

        try:
            import examples.transaction as example
        except Exception as e:
            self.fail(e)
        exp = '3bd75261ffeb5624cdd754a43e2fd938'
        self._exec_main(example, exp)

        sys.modules.pop('examples.transaction', None)

    def test_unicode(self):
        """examples/unicode.py"""
        try:
            import examples.unicode as example
        except Exception as e:
            self.fail(e)
        output = self._exec_main(example)
        if PY2:
            exp = [u'Unicode string: ¿Habla español?',
                   u'Unicode string coming from db: ¿Habla español?']
        else:
            exp = ['Unicode string: ¿Habla español?',
                   'Unicode string coming from db: ¿Habla español?']
        self.assertEqual(output, exp)

        sys.modules.pop('examples.unicode', None)

    def test_warnings(self):
        """examples/warnings.py"""
        try:
            import examples.warnings as example
        except Exception as e:
            self.fail(e)
        output = self._exec_main(example)
        exp = ["Executing 'SELECT 'abc'+1'",
               "1292: Truncated incorrect DOUBLE value: 'abc'"]
        self.assertEqual(output, exp, 'Output was not correct')

        example.STMT = "SELECT 'abc'"
        self.assertRaises(Exception, example.main, self.config)

        sys.modules.pop('examples.warnings', None)

    def test_multi_resultsets(self):
        """examples/multi_resultsets.py"""
        try:
            import examples.multi_resultsets as example
        except Exception as e:
            self.fail(e)
        output = self._exec_main(example)
        exp = ['Inserted 1 row', 'Number of rows: 1', 'Inserted 2 rows',
               'Names in table: Geert Jan Michel']
        self.assertEqual(output, exp, 'Output was not correct')

        sys.modules.pop('examples.resultsets', None)

    def test_microseconds(self):
        """examples/microseconds.py"""
        try:
            import examples.microseconds as example
        except Exception as e:
            self.fail(e)
        output = self._exec_main(example)

        if self.cnx.get_server_version() < (5, 6, 4):
            exp = "does not support fractional precision for timestamps."
            self.assertTrue(output[0].endswith(exp))
        else:
            exp = [
                ' 1 |  1 | 0:00:47.510000 | 2009-06-07 09:15:02.000234',
                ' 1 |  2 | 0:00:47.020000 | 2009-06-07 09:30:05.102345',
                ' 1 |  3 | 0:00:47.650000 | 2009-06-07 09:50:23.002300',
                ' 1 |  4 | 0:00:46.060000 | 2009-06-07 10:30:56.000001',
            ]
            self.assertEqual(output, exp)

        sys.modules.pop('examples.microseconds', None)

    def test_prepared_statements(self):
        """examples/prepared_statements.py"""
        try:
            import examples.prepared_statements as example
        except Exception as e:
            self.fail(e)
        output = self._exec_main(example)

        exp = [
            'Inserted data',
            '1 | Geert',
            '2 | Jan',
            '3 | Michel',
        ]
        self.assertEqual(output, exp, 'Output was not correct')

        sys.modules.pop('examples.prepared_statements', None)


@unittest.skipIf(not CMySQLConnection, "C Extension not available")
class TestExamplesCExt(TestExamples):

    def setUp(self):
        self.config = tests.get_mysql_config()
        self.config['use_pure'] = False
        self.cnx = mysql.connector.connect(**self.config)

    def tearDown(self):
        self.cnx.close()

    def test_prepared_statements(self):
        pass
