# -*- coding: utf-8 -*-

# Copyright (c) 2015, 2022, Oracle and/or its affiliates. All rights reserved.
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

import unittest

import mysql.connector
import tests

from tests import foreach_cnx

# using "/" (slash) to avoid windows scape characters
DATA_FILE = "/".join(["tests", "data", "random_big_bin.csv"])


@unittest.skipIf(
    tests.MYSQL_EXTERNAL_SERVER,
    "Test not available for external MySQL servers",
)
class Bug21449996(tests.MySQLConnectorTests):
    def setUp(self):
        self.table_name = "Bug21449996"
        cnx = mysql.connector.connect(**tests.get_mysql_config())
        cnx.cmd_query("DROP TABLE IF EXISTS %s" % self.table_name)
        cnx.cmd_query(
            "CREATE TABLE {0} (c1 BLOB) DEFAULT CHARSET=latin1"
            "".format(self.table_name)
        )
        cnx.close()

    def tearDown(self):
        cnx = mysql.connector.connect(**tests.get_mysql_config())
        cnx.cmd_query("DROP TABLE IF EXISTS %s" % self.table_name)
        cnx.close()

    @foreach_cnx(allow_local_infile=True)
    def test_load_data_compressed(self):
        try:
            cur = self.cnx.cursor()
            sql = (
                "LOAD DATA LOCAL INFILE '{0}' INTO TABLE {1} CHARACTER "
                "SET latin1".format(DATA_FILE, self.table_name)
            )
            cur.execute(sql)
        except mysql.connector.errors.InterfaceError as exc:
            raise
            self.fail(exc)

        cur.execute("SELECT COUNT(*) FROM %s" % self.table_name)
        self.assertEqual(11486, cur.fetchone()[0])
