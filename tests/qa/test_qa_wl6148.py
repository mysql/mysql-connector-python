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

import mysql.connector
import tests


class WL6148Tests(tests.MySQLConnectorTests):
    """Test for WL6148."""

    def setUp(self):
        config = tests.get_mysql_config()
        with mysql.connector.connect(**config) as cnx:
            cnx.cmd_query("DROP TABLE IF EXISTS customer")
            cnx.cmd_query("CREATE TABLE customer(i INT PRIMARY KEY,name varchar(10))")
            cnx.cmd_query(
                "INSERT INTO customer(i,name) VALUES "
                "(10,'Joshi'),(20,'Kiran'),(30,'Raja')"
            )
            cnx.commit()

    def tearDown(self):
        config = tests.get_mysql_config()
        with mysql.connector.connection.MySQLConnection(**config) as cnx:
            cnx.cmd_query("DROP TABLE IF EXISTS customer")

    @tests.foreach_cnx()
    def test_prepared_true(self):
        """TC1.
        - Open a connection with prepared=True
        - Check if the server side preparation works fine.
        """
        # Open cursor with prepared=True
        cursor = self.cnx.cursor(prepared=True)
        # Issue Select SQL with one column preparation
        pStmt = "SELECT i FROM customer WHERE name = ?"
        cursor.execute(pStmt, ("Joshi",))
        exp = 10
        out = cursor.fetchone()[0]
        self.assertEqual(exp, out)
        cursor.execute(pStmt, ("Kiran",))
        exp = 20
        out = cursor.fetchone()[0]
        self.assertEqual(exp, out)
        cursor.close()

    @tests.foreach_cnx()
    def test_no_prepared(self):
        """
        - Open a connection with prepared=False
        - Check if the server side preparation does not work as expected

        TC3:
        - Do not specify any prepared parameter in the connection
        - Test the resultant behavior. Expect a error.

        Both TC2 and TC3 are covered in test_no_prepared().
        """
        # Open without cursor_class=MySQLCursorPrepared
        # This is same as False
        cursor = self.cnx.cursor()
        # Issue Select SQL with one column preparation
        pStmt = "SELECT i FROM customer WHERE name = ?"
        self.assertRaises(
            mysql.connector.errors.ProgrammingError,
            cursor.execute,
            pStmt,
            ("Joshi",),
        )
        cursor.close()

    @tests.foreach_cnx()
    def test_invalid_params(self):
        """TC4.
        - Specify a invalid prepared parameter in the connection
        - Test the resultant behavior
        """
        # Issue Select SQL for multiple column preparation
        # Open cursor with prepared=True
        cursor = self.cnx.cursor(prepared=True)
        pStmt = "SELECT i FROM customer WHERE i = ? and name= ?"
        cursor.execute(
            pStmt,
            (
                10,
                "Joshi",
            ),
        )
        exp = 10
        out = cursor.fetchone()[0]
        self.assertEqual(exp, out)

        # Interchange Parameters and expect empty recordset
        cursor.execute(
            pStmt,
            (
                "Joshi",
                10,
            ),
        )
        rows = cursor.fetchall()
        exp = "[]"
        out = str(rows)
        self.assertEqual(exp, out)

        # Try passing ? ? together and see the behavior
        pStmt = "SELECT * FROM customer WHERE i = ? ?"
        self.assertRaises(
            (
                mysql.connector.errors.InterfaceError,
                mysql.connector.errors.ProgrammingError,
            ),
            cursor.execute,
            pStmt,
            (10, "Joshi"),
        )
        cursor.close()

    @tests.foreach_cnx()
    def test_multiple_columns(self):
        """
        TC5:
        - Open a connection with prepared=True
        - Create a table with multiple columns,
        - Create a prepared statement stmt_all  for all the columns
        - Create a prepared statement stmt_part for few of the columns
        - Pass correct params and verify contents of the table
        - Pass wrong parameters, say int instead of char and see the resultant behavior
        - Pass incorrect number of parameters and verify the resultant behavior

        TC6:
        - Open a connection with prepared=True
        - Create a table with multiple columns,
        - Delete, alter update table SQL with prepared statements. Check the behavior
        TC5 and TC6 are covered in test_MultipleColumns
        """
        cursor = self.cnx.cursor(prepared=True)
        cursor.execute("DROP TABLE IF EXISTS customer")
        cursor.execute(
            "CREATE TABLE customer2 (i INT PRIMARY KEY,name varchar(10),age int)"
        )
        cursor.execute(
            "INSERT INTO customer2 (i,name,age) VALUES"
            "(10,'Joshi',30),(20,'Kiran',30),(30,'Raja',30)"
        )
        # Open cursor with prepared=True
        cursor = self.cnx.cursor(prepared=True)
        pStmt = "SELECT age FROM customer2 WHERE i = ? and name= ?"
        cursor.execute(
            pStmt,
            (
                10,
                "Joshi",
            ),
        )
        exp = 30
        out = cursor.fetchone()[0]
        self.assertEqual(exp, out)
        # Update the table data
        altStmt = "UPDATE customer2 set name='same name' where i = ? and age = ?"
        cursor.execute(
            altStmt,
            (
                10,
                30,
            ),
        )
        # Select the updated data
        cursor.execute(
            pStmt,
            (
                10,
                "same name",
            ),
        )
        exp = 30
        out = cursor.fetchone()[0]
        self.assertEqual(exp, out)
        cursor.execute("DROP TABLE IF EXISTS customer2")
        cursor.close()
