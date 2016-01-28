# -*- coding: utf-8 -*-
# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

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

""" BUG21879914 Fix using C/Extension with only CA given
"""

import os.path
import unittest

import mysql.connector
from tests import foreach_cnx, cnx_config
import tests

try:
    from mysql.connector.connection_cext import CMySQLConnection
except ImportError:
    # Test without C Extension
    CMySQLConnection = None

TEST_SSL = {
    'ca': os.path.join(tests.SSL_DIR, 'tests_CA_cert.pem'),
    'cert': os.path.join(tests.SSL_DIR, 'tests_client_cert.pem'),
    'key': os.path.join(tests.SSL_DIR, 'tests_client_key.pem'),
}

OPTION_FILE = os.path.join('tests', 'data', 'option_files', 'my.cnf')

class Bug21879914(tests.MySQLConnectorTests):

    def test_ssl_cipher_in_option_file(self):
        config = tests.get_mysql_config()
        config['ssl_ca'] = TEST_SSL['ca']
        config['use_pure'] = False

        cnx = mysql.connector.connect(**config)
        cnx.cmd_query("SHOW STATUS LIKE 'Ssl_cipher'")
        self.assertNotEqual(cnx.get_row()[1], '')  # Ssl_cipher must have a value
