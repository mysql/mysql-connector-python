# -*- coding: utf-8 -*-
# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2013, Oracle and/or its affiliates. All rights reserved.

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
"""Unittests for bugs using __future__
"""

from __future__ import unicode_literals
import tests

from mysql.connector import (connection, errors)


class BugOra16655208(tests.MySQLConnectorTests):
    """BUG#16655208:UNICODE DATABASE NAMES FAILS WHEN USING UNICODE_LITERALS
    """
    def test_unicode_database(self):
        config = tests.get_mysql_config()
        config['database'] = 'データベース'
        self.assertRaises(errors.DatabaseError,
                          connection.MySQLConnection, **config)

