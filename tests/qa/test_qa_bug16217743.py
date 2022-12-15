# Copyright (c) 2013, 2022, Oracle and/or its affiliates.
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

import datetime

import mysql.connector
import tests


class BUG1617743(tests.MySQLConnectorTests):
    """Test for fix for Bug 16217743."""

    @tests.foreach_cnx()
    def test_bug(self):
        with self.cnx.cursor() as cur:
            cur.execute("drop table if exists varTable")
            cur.execute("create table varTable(f1 varchar(255))")
            # Create Procedure
            cur.execute("drop procedure if exists varProc")
            cur.execute(
                "create procedure varProc(v1 varchar(255)) "
                "begin insert into varTable values(v1); end"
            )
            cur.execute("drop procedure if exists dateProc")
            cur.execute(
                "create procedure dateProc(v1 DATE) "
                "begin insert into varTable values(v1); end"
            )
            cur.execute("drop procedure if exists timestampProc")
            cur.execute(
                "create procedure timestampProc(v1 TIMESTAMP) "
                "begin insert into varTable values(v1); end"
            )

            # Create a table with VARCHAR column type
            exp = (
                "&^$%J()@%EW*##^@!!!!*~*#&$*****#&@(!",
                "Derek O'Brien",
                "Readable Data",
            )
            for value in exp:
                cur.callproc("varProc", (value,))

            cur.execute("select f1 from varTable")
            for row in cur.fetchall():
                self.assertIn(row[0], exp)

            # Create a table with DATE column type
            cur.execute("drop table if exists varTable")
            cur.execute("create table varTable(f1 DATE)")
            cur.callproc("dateProc", ("1978-10-18",))
            cur.execute("select f1 from varTable")
            res = cur.fetchone()
            self.assertEqual(res[0], datetime.date(1978, 10, 18))

            # Create a table with TIMESTAMP column type
            cur.execute("drop table if exists varTable")
            cur.execute("create table varTable(f1 TIMESTAMP)")
            cur.callproc("timestampProc", ("2013-01-01 00:00:01",))
            cur.execute("select f1 from varTable")
            res = cur.fetchone()
            self.assertEqual(res[0], datetime.datetime(2013, 1, 1, 0, 0, 1))

            cur.execute("drop procedure if exists varProc")
            cur.execute("drop procedure if exists dateProc")
            cur.execute("drop procedure if exists timestampProc")
            self.cnx.commit()
