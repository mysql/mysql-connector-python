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
import unittest


@unittest.skipIf(
    tests.MYSQL_VERSION < (5, 7, 3), "Not supported for MySQL <5.7.3 versions"
)
class WL6936Tests(tests.MySQLConnectorTests):
    """Testing the reset_session API."""

    @tests.foreach_cnx()
    def test_insert_point(self):
        with self.cnx.cursor() as cur:
            # creating the table
            cur.execute("create table gm1 (i int , g geometry )engine=innodb")
            stmt = "INSERT INTO gm1 VALUES(%s,ST_GeomFromText(%s))"
            cur.execute(stmt, (1, "POINT(1 2)"))
            cur.execute("SELECT ST_X(g) FROM gm1 WHERE i=1;")
            out = cur.fetchone()[0]
            self.assertEqual(1, out)
            cur.execute("drop table if exists gm1")

    @tests.foreach_cnx()
    def test_fetch_index(self):
        with self.cnx.cursor() as cur:
            # creating the table
            cur.execute(
                "create table gm2 "
                "(g geometry not null, spatial index(g))engine=innodb"
            )
            stmt = "INSERT INTO gm2 VALUES(ST_GeomFromText(%s))"
            cur.execute(stmt, ("POINT(3 0)",))
            cur.execute(stmt, ("POINT(1 1)",))
            cur.execute("SELECT ST_X(g) FROM gm2 WHERE g=POINT(3,0)")
            out = cur.fetchone()[0]
            self.assertEqual(3, out)
            cur.execute("drop table if exists gm2")

    @tests.foreach_cnx()
    def test_polygon(self):
        with self.cnx.cursor() as cur:
            # creating the table
            cur.execute("create table gm3 (g geometry )engine=innodb")
            cur.execute(
                "set @g = "
                "'POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7,5 5))'"
            )
            cur.execute("INSERT INTO gm3 VALUES (ST_GeomFromText(@g))")
            exp = "POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7,5 5))"
            cur.execute("SELECT ST_AsText(g) FROM gm3")
            out = cur.fetchone()[0]
            self.assertEqual(exp, out)
            cur.execute("drop table if exists gm3")

    @tests.foreach_cnx()
    def test_linestring(self):
        with self.cnx.cursor() as cur:
            # creating the table
            cur.execute("create table gm4 (g geometry )engine=innodb")
            cur.execute("set @g ='LINESTRING(0 0,1 1,2 2)'")
            cur.execute("INSERT INTO gm4 VALUES (ST_GeomFromText(@g))")
            exp = "LINESTRING(0 0,1 1,2 2)"
            cur.execute("SELECT ST_AsText(g) FROM gm4")
            out = cur.fetchone()[0]
            self.assertEqual(exp, out)
            cur.execute("drop table if exists gm4")
