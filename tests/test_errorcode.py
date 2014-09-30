# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2012, 2014, Oracle and/or its affiliates. All rights reserved.

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

"""Unittests for mysql.connector.errorcode
"""

from datetime import datetime
import tests
from mysql.connector import errorcode


class ErrorCodeTests(tests.MySQLConnectorTests):

    def test__MYSQL_VERSION(self):
        minimum = (5, 6, 6)
        self.assertTrue(isinstance(errorcode._MYSQL_VERSION, tuple))
        self.assertTrue(len(errorcode._MYSQL_VERSION) == 3)
        self.assertTrue(errorcode._MYSQL_VERSION >= minimum)

    def _check_code(self, code, num):
        try:
            self.assertEqual(getattr(errorcode, code), num)
        except AttributeError as err:
            self.fail(err)

    def test_server_error_codes(self):
        cases = {
            'ER_HASHCHK': 1000,
            'ER_TRG_INVALID_CREATION_CTX': 1604,
            'ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION': 1792,
        }

        for code, num in cases.items():
            self._check_code(code, num)

    def test_client_error_codes(self):
        cases = {
            'CR_UNKNOWN_ERROR': 2000,
            'CR_PROBE_SLAVE_STATUS': 2022,
            'CR_AUTH_PLUGIN_CANNOT_LOAD': 2059,
        }

        for code, num in cases.items():
            self._check_code(code, num)
