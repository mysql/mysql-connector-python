# Copyright (c) 2014, 2021, Oracle and/or its affiliates.
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

import mysql.connector
import tests


class WL7542Tests(tests.MySQLConnectorTests):
    """Testing the reset_session API."""

    @tests.foreach_cnx()
    def test_reset_session_with_user_param(self):
        """Setting the user defined param while resetting session."""
        with self.cnx.cursor() as cur:
            cur.execute("SET @t1=0")
            self.cnx.reset_session({"t1": 1})
            cur.execute("SELECT @t1")
            exp = 1
            out = cur.fetchone()[0]
            self.assertEqual(exp, out)

    @tests.foreach_cnx()
    def test_reset_session_without_param(self):
        """Resetting the sesion without any setting any parameter."""
        self.cnx.reset_session()

    @tests.foreach_cnx()
    def test_reset_session_with_system_param(self):
        """Setting the system variable while resetting the session."""
        with self.cnx.cursor() as cur:
            self.cnx.reset_session({}, {"max_error_count": 5})
            operation = "SHOW VARIABLES LIKE 'max_error_count'"
            cur.execute(operation)
            exp = "5"
            out = cur.fetchone()[1]
            self.assertEqual(exp, out)

    @tests.foreach_cnx()
    def test_reset_session_with_invalid_param(self):
        """Observing the behaviour with invalid parameters."""
        self.assertRaises(
            mysql.connector.errors.DatabaseError,
            self.cnx.reset_session,
            {},
            {"max_conn": 5},
        )

    @tests.foreach_cnx()
    def test_delete_cached_trans(self):
        """Test to verify the deletion of cached transactions."""
        with self.cnx.cursor() as cur:
            cur.execute("drop table if exists test7542")
            cur.execute("create table test7542 (t int)")
            cur.execute("insert into test7542 values(10)")
            cur.execute("SELECT t from test7542")
            out = cur.fetchone()[0]
            self.assertEqual(10, out)
            self.cnx.reset_session()
            self.cnx.autocommit = True
            cur.execute("SELECT t from test7542")
            out = cur.fetchone()
            self.assertNotEqual(10, out)
