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

"""Unittests for mysql.connector.locales
"""

from datetime import datetime

import tests
from . import PY2
from mysql.connector import errorcode, locales


def _get_client_errors():
    errors = {}
    for name in dir(errorcode):
        if name.startswith('CR_'):
            errors[name] = getattr(errorcode, name)
    return errors


class LocalesModulesTests(tests.MySQLConnectorTests):

    def test_defaults(self):
        # There should always be 'eng'
        try:
            from mysql.connector.locales import eng  # pylint: disable=W0612
        except ImportError:
            self.fail("locales.eng could not be imported")

        # There should always be 'eng.client_error'
        some_error = None
        try:
            from mysql.connector.locales.eng import client_error
            some_error = client_error.CR_UNKNOWN_ERROR
        except ImportError:
            self.fail("locales.eng.client_error could not be imported")
        some_error = some_error + ''  # fool pylint

    def test_get_client_error(self):
        try:
            locales.get_client_error(2000, language='spam')
        except ImportError as err:
            self.assertEqual("No localization support for language 'spam'",
                             str(err))
        else:
            self.fail("ImportError not raised")

        exp = "Unknown MySQL error"
        self.assertEqual(exp, locales.get_client_error(2000))
        self.assertEqual(exp, locales.get_client_error('CR_UNKNOWN_ERROR'))

        try:
            locales.get_client_error(tuple())
        except ValueError as err:
            self.assertEqual(
                "error argument needs to be either an integer or string",
                str(err))
        else:
            self.fail("ValueError not raised")


class LocalesEngClientErrorTests(tests.MySQLConnectorTests):

    """Testing locales.eng.client_error"""

    def test__MYSQL_VERSION(self):
        try:
            from mysql.connector.locales.eng import client_error
        except ImportError:
            self.fail("locales.eng.client_error could not be imported")

        minimum = (5, 6, 6)
        self.assertTrue(isinstance(client_error._MYSQL_VERSION, tuple))
        self.assertTrue(len(client_error._MYSQL_VERSION) == 3)
        self.assertTrue(client_error._MYSQL_VERSION >= minimum)

    def test_messages(self):
        try:
            from mysql.connector.locales.eng import client_error
        except ImportError:
            self.fail("locales.eng.client_error could not be imported")

        errors = _get_client_errors()

        count = 0
        for name in dir(client_error):
            if name.startswith('CR_'):
                count += 1
        self.assertEqual(len(errors), count)

        if PY2:
            strtype = unicode  # pylint: disable=E0602
        else:
            strtype = str

        for name in errors.keys():
            self.assertTrue(isinstance(getattr(client_error, name), strtype))
