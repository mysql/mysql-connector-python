# Copyright (c) 2013, 2024, Oracle and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0, as
# published by the Free Software Foundation.
#
# This program is designed to work with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation. The authors of MySQL hereby grant you an
# additional permission to link the program and your derivative works
# with the separately licensed software that they have either included with
# the program or referenced in the documentation.
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

from mysql.connector.errors import ConnectionTimeoutError
import tests

import mysql.connector


class WL6351Tests(tests.MySQLConnectorTests):
    """Test to check for Return error codes."""

    def test_host(self):
        """Try to open a database connection with wrong ip should throw
        an error.
        """
        config = self.get_clean_mysql_config()
        config["host"] = "1.3.5.1"
        config["connect_timeout"] = 1
        for cls in self.all_cnx_classes:
            self.assertRaises(
                (
                    mysql.connector.errors.ConnectionTimeoutError,
                    mysql.connector.errors.InterfaceError,
                    mysql.connector.errors.DatabaseError,
                ),
                cls,
                **config,
            )

    @tests.foreach_cnx()
    def test_db(self):
        """Try to open a database connection and use non existing database."""
        with self.cnx.cursor() as cur:
            with self.assertRaises(mysql.connector.errors.ProgrammingError) as context:
                cur.execute("use unknowndb")
            self.assertEqual(context.exception.errno, 1049)

    @tests.foreach_cnx()
    def test_table(self):
        """Execute the SQL query using execute() method."""
        with self.cnx.cursor() as cur:
            with self.assertRaises(mysql.connector.errors.ProgrammingError) as context:
                cur.execute("SELECT * FROM unknowntable")
            self.assertEqual(context.exception.errno, 1146)
