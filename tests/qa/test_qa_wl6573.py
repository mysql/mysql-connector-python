# Copyright (c) 2013, 2021, Oracle and/or its affiliates.
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


class WL6936Tests(tests.MySQLConnectorTests):
    """Test for WL6573."""

    @tests.foreach_cnx(autocommit=False)
    def test_tran_close_wo_commit(self):
        """TC1:
        - Start the transaction with start_transaction() with autocommit OFF
        - Start few transaction, but do not commit
        - Close the connection and verify resultant behavior
        """
        with self.cnx.cursor() as cur:
            self.cnx.start_transaction()
            cur.execute("DROP TABLE IF EXISTS customer")
            cur.execute("CREATE TABLE customer(i INT,name varchar(10))")
            cur.execute(
                "INSERT INTO customer(i,name) "
                "values (10,'Joshi'),(20,'Kiran'),(30,'Raja')"
            )
            self.assertRaises(
                mysql.connector.errors.ProgrammingError,
                self.cnx.start_transaction,
            )

    @tests.foreach_cnx(autocommit=False)
    def handle_close_wo_commit(self):
        """TC2:
        - Start the transaction with start_transaction() with autocommit OFF
        - Start few transaction, but do not commit
        - Close the connection and verify resultant behavior
        - After ProgrammingError is caught, issue the commit
        - Verify contents of the table
        """
        with self.cnx.cursor() as cur:
            self.cnx.start_transaction()
            cur.execute("DROP TABLE IF EXISTS customer")
            cur.execute("CREATE TABLE customer(i INT,name varchar(10))")
            cur.execute(
                "INSERT INTO customer(i,name) "
                "values (10,'Joshi'),(20,'Kiran'),(30,'Raja')"
            )
            self.assertRaises(
                mysql.connector.errors.ProgrammingError,
                self.cnx.start_transaction,
            )
            self.cnx.commit()
            cur.execute("SELECT * FROM customer")
            _ = cur.fetchall()
            self.assertEqual(cur.rowcount, 3)

    @tests.foreach_cnx()
    def test_invalid_isolation(self):
        """TC3:
        - Start a transaction using start_transaction() with a particular
          ISOLATION LEVEL
        - Create a table and insert few records via a particular thread
        - In another thread while the transaction in previous step runs change
          the ISOLATION LEVEL and check the resultant behavior
        - After ProgrammingError is caught, issue the commit
        - Verify contents of the table

        This test case is not completely valid since the 'threadsafety' for
        Connector/Python is 1, which means sharing connection betwwen thwo
        threads is not ideal.

        Test invalidIsolation Level.
        """
        self.assertRaises(
            ValueError,
            self.cnx.start_transaction,
            isolation_level="NONEXISTISOLATION",
        )

    @tests.foreach_cnx()
    def test_tran_mul_commit_validate(self):
        """TC4:
        - Start the transaction with start_transaction()
        - Create a table and insert few records
        - Issue Commit
        - insert few more records
        - Issue Commit
        - Verify table contents
        """
        with self.cnx.cursor() as cur:
            self.cnx.start_transaction()
            cur.execute("DROP TABLE IF EXISTS customer")
            cur.execute("CREATE TABLE customer(i INT,name varchar(10))")
            cur.execute(
                "INSERT INTO customer(i,name) "
                "values (10,'Joshi'),(20,'Kiran'),(30,'Raja')"
            )
            cur.execute("SELECT * FROM customer")
            _ = cur.fetchall()
            self.cnx.commit()
            self.assertEqual(cur.rowcount, 3)

            # Insert some more data
            self.cnx.start_transaction()
            cur.execute(
                "INSERT INTO customer(i,name) "
                "values (10,'Joshi'),(20,'Kiran'),(30,'Raja')"
            )
            self.cnx.commit()
            cur.execute("SELECT * FROM customer")
            _ = cur.fetchall()
            self.assertEqual(cur.rowcount, 6)

    @tests.foreach_cnx()
    def test_nested_tran(self):
        """TC5:
        - Start the transaction with start_transaction()
        - Create a table and insert few records
        - Start a new transaction using start_transaction() inside the above transaction
        - Insert some more data
        - Commit and verify the cotents of the table
        """
        with self.cnx.cursor() as cur:
            self.cnx.start_transaction()
            cur.execute("DROP TABLE IF EXISTS customer")
            cur.execute("CREATE TABLE customer(i INT,name varchar(10))")
            cur.execute(
                "INSERT INTO customer(i,name) "
                "values (10,'Joshi'),(20,'Kiran'),(30,'Raja')"
            )
            # W/O commit/rollback start another transaction
            # Check the resultant behavior
            self.assertRaises(
                mysql.connector.errors.ProgrammingError,
                self.cnx.start_transaction,
            )

    @tests.foreach_cnx()
    def test_tran_in_valid_sql(self):
        """TC6:
        - Start the transaction with start_transaction()
        - Perform a invalid SQL execution inside the transaction
        - Validate the resultant error
        """
        with self.cnx.cursor() as cur:
            self.cnx.start_transaction()
            with self.assertRaises(
                mysql.connector.errors.ProgrammingError
            ) as context:
                cur.execute("Invalid SQL Statement")
                _ = cur.fetchall()
            self.assertEqual(context.exception.errno, 1064)
            self.cnx.commit()
