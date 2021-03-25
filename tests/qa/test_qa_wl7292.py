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


class WL7292Tests(tests.MySQLConnectorTests):
    """Testing the resultset retrieved namedtuples and dictionaries."""

    def setUp(self):
        config = tests.get_mysql_config()
        with mysql.connector.connect(**config) as cnx:
            with cnx.cursor() as cur:
                cur.execute("drop table if exists wl7292")
                cur.execute(
                    "create table wl7292(id int, name varchar(5), "
                    "dept varchar(3))"
                )
                cur.execute("insert into wl7292 values(1, 'abc', 'cs')")
                cur.execute("insert into wl7292 values(2, 'def', 'is')")
                cur.execute("insert into wl7292 values(3, 'ghi', 'cs')")
                cur.execute("insert into wl7292 values(4, 'jkl', 'it')")
                cnx.commit()

    def tearDown(self):
        config = tests.get_mysql_config()
        with mysql.connector.connect(**config) as cnx:
            with cnx.cursor() as cur:
                cur.execute("drop table if exists wl7292")
            cnx.commit()

    @tests.foreach_cnx()
    def test_curdict(self):
        """Retrieving the resultset as dictionary."""
        test_cd = 0
        with self.cnx.cursor(dictionary=True) as curdict:
            curdict.execute("select * from wl7292 where dept='cs'")
            for row in curdict:
                if (row["id"] == 1 and row["name"] == "abc") or (
                    row["id"] == 3 and row["name"] == "ghi"
                ):
                    test_cd = 1
            self.assertEqual(1, test_cd)

    @tests.foreach_cnx()
    def test_curdict_buff(self):
        """Buffered retrieval of resultset."""
        test_cdb = 0
        with self.cnx.cursor(dictionary=True, buffered=True) as curdict_buff:
            curdict_buff.execute("select * from wl7292 where dept='cs'")
            for row in curdict_buff:
                if (row["id"] == 1 and row["name"] == "abc") or (
                    row["id"] == 3 and row["name"] == "ghi"
                ):
                    test_cdb = 1
            self.assertEqual(1, test_cdb)

    @tests.foreach_cnx()
    def test_curnamed(self):
        """Retrieving the resultset as namedtuple."""
        test_cn = 0
        with self.cnx.cursor(named_tuple=True) as curnam:
            curnam.execute("select * from wl7292 where dept='cs'")
            for row in curnam:
                if (row.id == 1 and row.name == "abc") or (
                    row.id == 3 and row.name == "ghi"
                ):
                    test_cn = 1
            self.assertEqual(1, test_cn)

    @tests.foreach_cnx()
    def test_curnamed_buff(self):
        """Buffered retrieval of resultset."""
        test_cnb = 0
        with self.cnx.cursor(named_tuple=True, buffered=True) as curnam_buff:
            curnam_buff.execute("select * from wl7292 where dept='cs'")
            for row in curnam_buff:
                if (row.id == 1 and row.name == "abc") or (
                    row.id == 3 and row.name == "ghi"
                ):
                    test_cnb = 1
            self.assertEqual(1, test_cnb)
