# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2014, Oracle and/or its affiliates. All rights reserved.

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

import logging
import os
import tests

from mysql.connector import connect
from mysql.connector.optionfiles import MySQLOptionsParser, read_option_files

LOGGER = logging.getLogger(tests.LOGGER_NAME)


class MySQLOptionsParserTests(tests.MySQLConnectorTests):

    """Class checking MySQLOptionsParser"""

    def setUp(self):
        self.option_file_dir = os.path.join('tests', 'data', 'option_files')
        self.option_file_parser = MySQLOptionsParser(files=os.path.join(
            self.option_file_dir, 'my.cnf'))

    def test___init__(self):
        self.assertRaises(ValueError, MySQLOptionsParser)
        option_file_parser = MySQLOptionsParser(files=os.path.join(
            self.option_file_dir, 'my.cnf'))
        self.assertEqual(option_file_parser.files, [os.path.join(
            self.option_file_dir, 'my.cnf')])

    def test_optionxform(self):
        """Converts option strings

        Converts option strings to lower case and replaces dashes(-) with
        underscores(_) if keep_dashes variable is set.
        """
        self.assertEqual('ham', self.option_file_parser.optionxform('HAM'))
        self.assertEqual('ham-spam', self.option_file_parser.optionxform(
            'HAM-SPAM'))

        self.option_file_parser.keep_dashes = False
        self.assertEqual('ham_spam', self.option_file_parser.optionxform(
            'HAM-SPAM'))

    def test__parse_options(self):
        files = [
            os.path.join(self.option_file_dir, 'include_files', '1.cnf'),
            os.path.join(self.option_file_dir, 'include_files', '2.cnf'),
        ]
        self.option_file_parser = MySQLOptionsParser(files)
        self.assertRaises(ValueError, self.option_file_parser._parse_options,
                          'dummy_file.cnf')
        self.option_file_parser._parse_options(files)
        exp = {
            'option1': '15',
            'option2': '20'
        }
        self.assertEqual(exp, self.option_file_parser.get_groups('group2',
                                                                 'group1'))

        exp = {
            'option3': '200'
        }
        self.assertEqual(exp, self.option_file_parser.get_groups('group3',
                                                                 'group4'))
        self.assertEqual(exp, self.option_file_parser.get_groups('group4',
                                                                 'group3'))

    def test_read(self,):
        filename = os.path.join( self.option_file_dir, 'my.cnf')
        self.assertEqual([filename], self.option_file_parser.read(filename))

        filenames = [
            os.path.join(self.option_file_dir, 'include_files', '1.cnf'),
            os.path.join(self.option_file_dir, 'include_files', '2.cnf'),
        ]
        self.assertEqual(filenames, self.option_file_parser.read(filenames))

        self.assertEqual([], self.option_file_parser.read('dummy-file.cnf'))

    def test_get_groups(self):
        exp = {
            'password': '12345',
            'port': '1001',
            'socket': '/var/run/mysqld/mysqld2.sock',
            'ssl-ca': 'dummyCA',
            'ssl-cert': 'dummyCert',
            'ssl-key': 'dummyKey',
            'nice': '0',
            'user': 'mysql',
            'pid-file': '/var/run/mysqld/mysqld.pid',
            'basedir': '/usr',
            'datadir': '/var/lib/mysql',
            'tmpdir': '/tmp',
            'lc-messages-dir': '/usr/share/mysql',
            'skip-external-locking': '',
            'bind-address': '127.0.0.1',
            'log_error': '/var/log/mysql/error.log',
        }
        self.assertEqual(exp, self.option_file_parser.get_groups('client',
                                                                 'mysqld_safe',
                                                                 'mysqld'))

    def test_get_groups_as_dict(self):
        exp = dict([
            ('client', {'port': '1000',
                        'password': '12345',
                        'socket': '/var/run/mysqld/mysqld.sock',
                        'ssl-ca': 'dummyCA',
                        'ssl-cert': 'dummyCert',
                        'ssl-key': 'dummyKey'}),
            ('mysqld_safe', {'socket': '/var/run/mysqld/mysqld1.sock',
                            'nice': '0'}),
            ('mysqld', {'user': 'mysql',
                       'pid-file': '/var/run/mysqld/mysqld.pid',
                       'socket': '/var/run/mysqld/mysqld2.sock',
                       'port': '1001', 'basedir': '/usr',
                       'datadir': '/var/lib/mysql', 'tmpdir': '/tmp',
                       'lc-messages-dir': '/usr/share/mysql',
                       'skip-external-locking': '',
                       'bind-address': '127.0.0.1',
                       'log_error': '/var/log/mysql/error.log'}),
        ])
        self.assertEqual(exp, self.option_file_parser.get_groups_as_dict())

    def test_get_groups_as_dict_with_priority(self):
        files = [
            os.path.join(self.option_file_dir, 'include_files', '1.cnf'),
            os.path.join(self.option_file_dir, 'include_files', '2.cnf'),
        ]
        self.option_file_parser = MySQLOptionsParser(files)

        exp = dict([
            ('group1', {'option1': ('15', 1),
                        'option2': ('20', 1)}),
            ('group2', {'option1': ('20', 1),
                        'option2': ('30', 1)}),
            ('group3', {'option3': ('100', 0)}),
            ('group4', {'option3': ('200', 1)}),
            ('mysql', {'user': ('ham', 0)}),
            ('client', {'user': ('spam', 1)})
        ])
        self.assertEqual(
            exp, self.option_file_parser.get_groups_as_dict_with_priority())

    def test_read_option_files(self):

        self.assertRaises(ValueError, read_option_files,
                          option_files='dummy_file.cnf')

        option_file_dir = os.path.join('tests', 'data', 'option_files')
        exp = {
            'password': '12345',
            'port': 1000,
            'unix_socket': '/var/run/mysqld/mysqld.sock',
            'ssl_ca': 'dummyCA',
            'ssl_cert': 'dummyCert',
            'ssl_key': 'dummyKey',
        }
        result = read_option_files(option_files=os.path.join(
            option_file_dir, 'my.cnf'))
        self.assertEqual(exp, result)
        exp = {
            'password': '12345',
            'port': 1001,
            'unix_socket': '/var/run/mysqld/mysqld2.sock',
            'ssl_ca': 'dummyCA',
            'ssl_cert': 'dummyCert',
            'ssl_key': 'dummyKey',
            'user': 'mysql',
        }
        result = read_option_files(option_files=os.path.join(
            option_file_dir, 'my.cnf'), option_groups=['client', 'mysqld'])
        self.assertEqual(exp, result)

        option_file_dir = os.path.join('tests', 'data', 'option_files')
        files = [
            os.path.join(option_file_dir, 'include_files', '1.cnf'),
            os.path.join(option_file_dir, 'include_files', '2.cnf'),
        ]
        exp = {
            'user': 'spam'
        }
        result = read_option_files(option_files=files,
                                   option_groups=['client', 'mysql'])
        self.assertEqual(exp, result)

        self.assertRaises(ValueError, connect, option_files='dummy_file.cnf')
