# Copyright (c) 2013, 2018, Oracle and/or its affiliates. All rights reserved.
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

"""Unit tests for the setup script of Connector/Python
"""

import sys
import tests
import imp

import setupinfo


class VersionTests(tests.MySQLConnectorTests):

    """Testing the version of Connector/Python"""

    def test_version(self):
        """Test validity of version"""
        vs = setupinfo.VERSION
        self.assertTrue(all(
            [isinstance(vs[0], int),
             isinstance(vs[1], int),
             isinstance(vs[2], int),
             isinstance(vs[3], str),
             isinstance(vs[4], int)]))

    def test___version__(self):
        """Test module __version__ and __version_info__"""
        import mysql.connector
        self.assertTrue(hasattr(mysql.connector, '__version__'))
        self.assertTrue(hasattr(mysql.connector, '__version_info__'))
        self.assertTrue(isinstance(mysql.connector.__version__, str))
        self.assertTrue(isinstance(mysql.connector.__version_info__, tuple))
        self.assertEqual(setupinfo.VERSION_TEXT, mysql.connector.__version__)
        self.assertEqual(setupinfo.VERSION, mysql.connector.__version_info__)


class SetupInfoTests(tests.MySQLConnectorTests):

    """Testing meta setup information

    We are importing the setupinfo module insite the unit tests
    to be able to actually do tests.
    """

    def setUp(self):
        # we temper with version_info, play safe, keep copy
        self._sys_version_info = sys.version_info

    def tearDown(self):
        # we temper with version_info, play safe, restore copy
        sys.version_info = self._sys_version_info

    def test_name(self):
        """Test the name of Connector/Python"""
        import setupinfo
        self.assertEqual('mysql-connector-python', setupinfo.name)

    def test_dev_statuses(self):
        """Test the development statuses"""
        import setupinfo
        exp = {
            'a': '3 - Alpha',
            'b': '4 - Beta',
            'rc': '4 - Beta',
            '': '5 - Production/Stable'
        }
        self.assertEqual(exp, setupinfo.DEVELOPMENT_STATUSES)

    def test_package_dir(self):
        """Test the package directory"""
        import setupinfo
        exp = {
            '': 'lib',
        }
        self.assertEqual(exp, setupinfo.package_dir)

    def test_unsupported_python(self):
        """Test if old Python version are unsupported"""
        import setupinfo
        tmp = sys.version_info
        sys.version_info = (3, 0, 0, 'final', 0)
        try:
            imp.reload(setupinfo)
        except RuntimeError:
            pass
        else:
            self.fail("RuntimeError not raised with unsupported Python")
        sys.version_info = tmp

    def test_version(self):
        """Test the imported version information"""
        import setupinfo
        ver = setupinfo.VERSION
        exp = '{0}.{1}.{2}'.format(*ver[0:3])
        self.assertEqual(exp, setupinfo.version)

    def test_misc_meta(self):
        """Test miscellaneous data such as URLs"""
        import setupinfo
        self.assertEqual(
            'http://dev.mysql.com/doc/connector-python/en/index.html',
            setupinfo.url)
        self.assertEqual(
            'http://dev.mysql.com/downloads/connector/python/',
            setupinfo.download_url)

    def test_classifiers(self):
        """Test Trove classifiers"""
        import setupinfo
        for clsfr in setupinfo.classifiers:
            if 'Programming Language :: Python' in clsfr:
                ver = clsfr.replace('Programming Language :: Python :: ', '')
                if ver not in ('2.6', '2.7', '3', '3.1', '3.2', '3.3', '3.4',
                               '3.5', '3.6', '3.7'):
                    self.fail('Unsupported version in classifiers')
            if 'Development Status ::' in clsfr:
                status = clsfr.replace('Development Status :: ', '')
                self.assertEqual(
                    setupinfo.DEVELOPMENT_STATUSES[setupinfo.VERSION[3]],
                    status)
