# -*- coding: utf-8 -*-

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

""" BUG22545879 Fix reading and using ssl-cipher MySQL option
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

class Bug21449996(tests.MySQLConnectorTests):

    @cnx_config(ssl_ca=TEST_SSL['ca'], ssl_cert=TEST_SSL['cert'], ssl_key=TEST_SSL['key'],
        ssl_cipher="AES256-SHA:CAMELLIA256-SHA")
    @foreach_cnx()
    def test_cnx_argument_ssl_cipher(self):
        self.assertIn('cipher', self.cnx._ssl)
        self.assertEqual("AES256-SHA:CAMELLIA256-SHA", self.cnx._ssl['cipher'])

    def test_ssl_cipher_in_option_file(self):
        config = tests.get_mysql_config()
        config['option_files'] = [OPTION_FILE]

        cnx = mysql.connector.MySQLConnection()
        cnx.config(**config)
        self.assertIn('cipher', cnx._ssl)
        self.assertEqual("AES256-SHA:CAMELLIA256-SHA", cnx._ssl['cipher'])

        if CMySQLConnection:
            cnx = CMySQLConnection()
            cnx.config(**config)
            self.assertIn('cipher', cnx._ssl)
            self.assertEqual("AES256-SHA:CAMELLIA256-SHA", cnx._ssl['cipher'])
