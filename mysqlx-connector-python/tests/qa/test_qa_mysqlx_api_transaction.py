# Copyright (c) 2023, Oracle and/or its affiliates.
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

"""MySQL X API transaction tests."""

import unittest

import mysqlx

import tests


@unittest.skipIf(
    tests.MYSQL_VERSION < (8, 0, 6),
    "Transaction savepoints not supported",
)
class APIResultTests(tests.MySQLxTests):
    """API transaction tests."""

    def _drop_collection_if_exists(self, name):
        collection = self.schema.get_collection(name)
        if collection.exists_in_database():
            self.schema.drop_collection(name)

    @tests.foreach_session()
    def test_transaction_savepoint_test1(self):
        """Create a savepoint without specifying a name, verify that savepoint
        has a name, rollback to the savepoint."""
        self._drop_collection_if_exists("mycoll1")
        collection = self.schema.create_collection("mycoll1")
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp = self.session.set_savepoint()
        collection.add(
            {"name": "Robb Stark", "age": 25},
            {"name": "Brandon Stark", "age": 15},
        ).execute()
        self.assertEqual(collection.count(), 4)
        self.session.rollback_to(sp)
        self.assertEqual(collection.count(), 2)
        self.session.commit()
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_transaction_savepoint_test2(self):
        """Create a savepoint without specifying a name, verify that savepoint
        has a name, release the savepoint."""
        self._drop_collection_if_exists("mycoll2")
        collection = self.schema.create_collection("mycoll2")
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp = self.session.set_savepoint()
        collection.add(
            {"name": "Robb Stark", "age": 25},
            {"name": "Brandon Stark", "age": 15},
        ).execute()
        self.assertEqual(collection.count(), 4)
        self.session.release_savepoint(sp)
        self.assertEqual(collection.count(), 4)
        # Should give error as the savepoint is already released and doesn't
        # exist anymore
        self.assertRaises(mysqlx.OperationalError, self.session.release_savepoint, sp)
        self.session.commit()
        self.schema.drop_collection("mycoll2")

    @tests.foreach_session()
    def test_transaction_savepoint_test3(self):
        """Create a savepoint with a custom name, verify that savepoint
        has that name, rollback to the savepoint."""
        self._drop_collection_if_exists("mycoll13")
        collection = self.schema.create_collection("mycoll13")
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp = self.session.set_savepoint("SavedPoint")
        self.assertEqual(sp, "SavedPoint")
        collection.add(
            {"name": "Robb Stark", "age": 25},
            {"name": "Brandon Stark", "age": 15},
        ).execute()
        self.assertEqual(collection.count(), 4)
        self.session.rollback_to("SavedPoint")
        self.assertEqual(collection.count(), 2)
        self.session.commit()
        self.schema.drop_collection("mycoll13")

    @tests.foreach_session()
    def test_transaction_savepoint_test4(self):
        """Create a savepoint with a custom name, verify that savepoint
        has the same name, release the savepoint."""
        self._drop_collection_if_exists("mycoll4")
        collection = self.schema.create_collection("mycoll4")
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp = self.session.set_savepoint("SavedPoint")
        self.assertEqual(sp, "SavedPoint")
        collection.add(
            {"name": "Robb Stark", "age": 25},
            {"name": "Brandon Stark", "age": 15},
        ).execute()
        self.assertEqual(collection.count(), 4)
        self.session.release_savepoint("SavedPoint")
        self.assertEqual(collection.count(), 4)
        self.assertRaises(mysqlx.OperationalError, self.session.release_savepoint, sp)
        self.session.commit()
        self.schema.drop_collection("mycoll4")

    @tests.foreach_session()
    def test_transaction_savepoint_test5(self):
        """set_savepoint with a duplicate name, should succeed and the old
        one will be over written by new one."""
        self._drop_collection_if_exists("mycoll5")
        collection = self.schema.create_collection("mycoll5")
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp = self.session.set_savepoint("SavedPoint")
        self.assertEqual(sp, "SavedPoint")
        collection.add(
            {"name": "Robb Stark", "age": 25},
            {"name": "Brandon Stark", "age": 15},
        ).execute()
        self.assertEqual(collection.count(), 4)
        sp = self.session.set_savepoint("SavedPoint")
        collection.add({"name": "John Snow", "age": 25}).execute()
        self.assertEqual(collection.count(), 5)
        self.session.rollback_to(sp)
        self.assertEqual(collection.count(), 4)
        self.session.release_savepoint("SavedPoint")
        self.session.commit()
        self.schema.drop_collection("mycoll5")

    @tests.foreach_session()
    def test_transaction_savepoint_test6(self):
        """Set two savepoints A and then B, Test rollback_to B after rolling
        back to A - should give error."""
        self._drop_collection_if_exists("mycoll6")
        collection = self.schema.create_collection("mycoll6")
        self.session.start_transaction()
        collection.add(
            {"name": "Robert", "age": 54}, {"name": "Renly", "age": 40}
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp1 = self.session.set_savepoint("SavedPoint1")
        collection.add({"name": "Stannis", "age": 45}).execute()
        self.assertEqual(collection.count(), 3)
        sp2 = self.session.set_savepoint("SavedPoint2")
        collection.add({"name": "Joffrey", "age": 23}).execute()
        self.assertEqual(collection.count(), 4)
        self.session.rollback_to(sp1)
        self.assertEqual(collection.count(), 2)
        self.assertRaises(mysqlx.OperationalError, self.session.release_savepoint, sp2)
        self.session.commit()
        self.schema.drop_collection("mycoll6")

    @tests.foreach_session()
    def test_transaction_savepoint_test7(self):
        """Create multiple valid savepoint with/without specifying name and
        rollback to the same using the names generated in order."""
        self._drop_collection_if_exists("mycoll7")
        collection = self.schema.create_collection("mycoll7")
        self.session.start_transaction()
        collection.add(
            {"name": "Robert", "age": 54}, {"name": "Renly", "age": 40}
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp1 = self.session.set_savepoint("SavedPoint1")
        collection.add({"name": "Stannis", "age": 45}).execute()
        self.assertEqual(collection.count(), 3)
        sp2 = self.session.set_savepoint()
        collection.add({"name": "Joffrey", "age": 23}).execute()
        self.assertEqual(collection.count(), 4)
        sp3 = self.session.set_savepoint("SavedPoint3")
        collection.add({"name": "Cersei", "age": 45}).execute()
        self.assertEqual(collection.count(), 5)
        sp4 = self.session.set_savepoint("SavedPoint4")
        collection.add({"name": "Jaimie", "age": 45}).execute()
        self.assertEqual(collection.count(), 6)
        sp5 = self.session.set_savepoint("SavedPoint5")
        collection.add({"name": "Tyrian", "age": 40}).execute()
        self.assertEqual(collection.count(), 7)
        sp6 = self.session.set_savepoint()
        self.session.rollback_to(sp6)
        self.assertEqual(collection.count(), 7)
        self.session.rollback_to(sp5)
        self.assertEqual(collection.count(), 6)
        self.session.rollback_to("SavedPoint4")
        self.assertEqual(collection.count(), 5)
        self.session.rollback_to(sp3)
        self.assertEqual(collection.count(), 4)
        self.session.rollback_to(sp2)
        self.assertEqual(collection.count(), 3)
        self.session.rollback_to(sp1)
        self.assertEqual(collection.count(), 2)
        self.session.commit()
        self.schema.drop_collection("mycoll7")

    @tests.foreach_session()
    def test_transaction_savepoint_test8(self):
        """Create savepoint without starting a transaction and try to
        rollback - should give error."""
        self._drop_collection_if_exists("mycoll8")
        collection = self.schema.create_collection("mycoll8")
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp = self.session.set_savepoint()
        collection.add({"name": "Robb", "age": 25}).execute()
        self.assertEqual(collection.count(), 3)
        self.assertRaises(mysqlx.OperationalError, self.session.release_savepoint, sp)
        self.schema.drop_collection("mycoll8")

    @tests.foreach_session()
    def test_transaction_savepoint_test9(self):
        """Create savepoint without starting a transaction and try to
        release - should give error."""
        self._drop_collection_if_exists("mycoll9")
        collection = self.schema.create_collection("mycoll9")
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp = self.session.set_savepoint()
        collection.add({"name": "Robb", "age": 25}).execute()
        self.assertEqual(collection.count(), 3)
        self.assertRaises(mysqlx.OperationalError, self.session.release_savepoint, sp)
        # self.assertEqual(collection.count(), 2)
        self.schema.drop_collection("mycoll9")

    @tests.foreach_session()
    def test_transaction_savepoint_test10(self):
        """Create a savepoint with invalid names."""
        self._drop_collection_if_exists("mycoll10")
        collection = self.schema.create_collection("mycoll10")
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        self.assertRaises(mysqlx.ProgrammingError, self.session.set_savepoint, "")
        self.schema.drop_collection("mycoll10")

    @tests.foreach_session()
    def test_transaction_savepoint_test11(self):
        self._drop_collection_if_exists("mycoll11")
        collection = self.schema.create_collection("mycoll11")
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp1 = self.session.set_savepoint("_")
        self.assertEqual(sp1, "_")
        collection.add({"name": "Robb", "age": 25}).execute()
        self.assertEqual(collection.count(), 3)
        sp2 = self.session.set_savepoint("123456")
        self.assertEqual(sp2, "123456")
        collection.add({"name": "Catelyn", "age": 45}).execute()
        self.assertEqual(collection.count(), 4)
        sp3 = self.session.set_savepoint("-")
        self.assertEqual(sp3, "-")
        collection.add({"name": "Ned Stark", "age": 50}).execute()
        self.assertEqual(collection.count(), 5)
        self.session.rollback_to(sp3)
        self.assertEqual(collection.count(), 4)
        self.session.rollback_to(sp2)
        self.assertEqual(collection.count(), 3)
        self.session.rollback_to(sp1)
        self.assertEqual(collection.count(), 2)
        self.session.commit()
        self.schema.drop_collection("mycoll11")

    @tests.foreach_session()
    def test_transaction_savepoint_test12(self):
        """Create savepoint name as None, it should autogenerate the savepoint
        name."""
        self._drop_collection_if_exists("mycoll12")
        collection = self.schema.create_collection("mycoll12")
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp = self.session.set_savepoint(None)
        collection.add({"name": "Robb", "age": 25}).execute()
        self.assertEqual(collection.count(), 3)
        self.session.rollback_to(sp)
        self.assertEqual(collection.count(), 2)
        self.session.release_savepoint(sp)
        self.session.commit()
        self.schema.drop_collection("mycoll12")

    @tests.foreach_session()
    def test_transaction_savepoint_test13(self):
        """Create a savepoint several times with the same name and the
        savepoint must be overwritten."""
        self._drop_collection_if_exists("mycoll13")
        collection = self.schema.create_collection("mycoll13")
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp = self.session.set_savepoint("SavedPoint")
        collection.add({"name": "Robb Stark", "age": 25}).execute()
        self.assertEqual(collection.count(), 3)
        sp = self.session.set_savepoint("SavedPoint")
        collection.add({"name": "John Snow", "age": 25}).execute()
        self.assertEqual(collection.count(), 4)
        sp = self.session.set_savepoint("SavedPoint")
        collection.add({"name": "Brandon Stark", "age": 15}).execute()
        self.assertEqual(collection.count(), 5)
        self.session.rollback_to(sp)
        self.assertEqual(collection.count(), 4)
        self.session.release_savepoint("SavedPoint")
        self.session.commit()
        self.schema.drop_collection("mycoll13")

    @tests.foreach_session()
    def test_transaction_savepoint_test14(self):
        """Rollback to the same savepoint multiple times."""
        self._drop_collection_if_exists("mycoll14")
        collection = self.schema.create_collection("mycoll14")
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp = self.session.set_savepoint("SavedPoint")
        collection.add({"name": "Robb Stark", "age": 25}).execute()
        self.assertEqual(collection.count(), 3)
        self.session.rollback_to(sp)
        self.assertEqual(collection.count(), 2)
        collection.add({"name": "John Snow", "age": 25}).execute()
        self.assertEqual(collection.count(), 3)
        self.session.rollback_to(sp)
        self.assertEqual(collection.count(), 2)
        collection.add({"name": "Brandon Stark", "age": 15}).execute()
        self.assertEqual(collection.count(), 3)
        self.session.rollback_to(sp)
        self.assertEqual(collection.count(), 2)
        self.session.commit()
        self.schema.drop_collection("mycoll14")

    @tests.foreach_session()
    def test_transaction_savepoint_test15(self):
        """Test releasing the non-existing savepoint - should throw
        exception."""
        self._drop_collection_if_exists("mycoll15")
        collection = self.schema.create_collection("mycoll15")
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        self.assertRaises(
            mysqlx.OperationalError,
            self.session.release_savepoint,
            "SavedPoint",
        )
        self.session.commit()
        self.schema.drop_collection("mycoll15")

    @tests.foreach_session()
    def test_transaction_savepoint_test16(self):
        """Rollback to the non-existing savepoint - should throw exception."""
        self._drop_collection_if_exists("mycoll16")
        collection = self.schema.create_collection("mycoll16")
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        self.assertRaises(
            mysqlx.OperationalError, self.session.rollback_to, "SavedPoint"
        )
        self.session.commit()
        self.schema.drop_collection("mycoll16")

    @tests.foreach_session()
    def test_transaction_savepoint_test17(self):
        """Test Rollback and release of savepoint after transaction commit."""
        self._drop_collection_if_exists("mycoll17")
        collection = self.schema.create_collection("mycoll17")
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp = self.session.set_savepoint("SavedPoint")
        collection.add({"name": "Robb Stark", "age": 25}).execute()
        self.assertEqual(collection.count(), 3)
        self.session.commit()
        self.assertRaises(
            mysqlx.OperationalError, self.session.rollback_to, "SavedPoint"
        )
        self.schema.drop_collection("mycoll17")

    @tests.foreach_session()
    def test_transaction_savepoint_test18(self):
        self._drop_collection_if_exists("mycoll18")
        collection = self.schema.create_collection("mycoll18")
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp = self.session.set_savepoint("SavedPoint")
        collection.add({"name": "Robb Stark", "age": 25}).execute()
        self.assertEqual(collection.count(), 3)
        self.session.commit()
        self.assertRaises(
            mysqlx.OperationalError,
            self.session.release_savepoint,
            "SavedPoint",
        )
        self.schema.drop_collection("mycoll18")

    @tests.foreach_session()
    def test_transaction_savepoint_test19(self):
        """Rollback and Release a savepoint after a transaction rollback,
        error must be thrown."""
        self._drop_collection_if_exists("mycoll19")
        collection = self.schema.create_collection(
            "mycoll19",
        )
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp = self.session.set_savepoint("SavedPoint")
        collection.add({"name": "Robb Stark", "age": 25}).execute()
        self.assertEqual(collection.count(), 3)
        self.session.rollback()
        self.assertRaises(
            mysqlx.OperationalError, self.session.rollback_to, "SavedPoint"
        )
        self.schema.drop_collection("mycoll19")

    @tests.foreach_session()
    def test_transaction_savepoint_test20(self):
        self._drop_collection_if_exists("mycoll20")
        collection = self.schema.create_collection("mycoll20")
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp = self.session.set_savepoint("SavedPoint")
        collection.add({"name": "Robb Stark", "age": 25}).execute()
        self.assertEqual(collection.count(), 3)
        self.session.rollback()
        self.assertRaises(
            mysqlx.OperationalError,
            self.session.release_savepoint,
            "SavedPoint",
        )
        self.schema.drop_collection("mycoll20")

    @tests.foreach_session()
    def test_transaction_savepoint_test21(self):
        """Test rollback to the savepoint after it is release - should give
        error."""
        self._drop_collection_if_exists("mycoll21")
        collection = self.schema.create_collection("mycoll21")
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp = self.session.set_savepoint("SavedPoint")
        collection.add({"name": "Robb Stark", "age": 25}).execute()
        self.assertEqual(collection.count(), 3)
        self.session.release_savepoint("SavedPoint")
        self.assertRaises(
            mysqlx.OperationalError, self.session.rollback_to, "SavedPoint"
        )
        self.session.commit()
        self.schema.drop_collection("mycoll21")

    @tests.foreach_session()
    def test_transaction_savepoint_test22(self):
        """Release same savepoint multiple times - should give error."""
        self._drop_collection_if_exists("mycoll22")
        collection = self.schema.create_collection("mycoll22")
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp = self.session.set_savepoint("SavedPoint")
        collection.add({"name": "Robb Stark", "age": 25}).execute()
        self.assertEqual(collection.count(), 3)
        self.session.release_savepoint("SavedPoint")
        self.assertRaises(
            mysqlx.OperationalError,
            self.session.release_savepoint,
            "SavedPoint",
        )
        self.session.commit()
        self.schema.drop_collection("mycoll22")

    @tests.foreach_session()
    def test_transaction_savepoint_test23(self):
        """Rollback to empty savepoint."""
        self._drop_collection_if_exists("mycoll23")
        collection = self.schema.create_collection("mycoll23")
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp = self.session.set_savepoint("SavedPoint")
        collection.add({"name": "Robb Stark", "age": 25}).execute()
        self.assertEqual(collection.count(), 3)
        self.assertRaises(mysqlx.ProgrammingError, self.session.rollback_to, "")
        self.session.commit()
        self.schema.drop_collection("mycoll23")

    @tests.foreach_session()
    def test_transaction_savepoint_test24(self):
        """Release empty savepoint."""
        self._drop_collection_if_exists("mycoll24")
        collection = self.schema.create_collection("mycoll24")
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        sp = self.session.set_savepoint("SavedPoint")
        collection.add({"name": "Robb Stark", "age": 25}).execute()
        self.assertEqual(collection.count(), 3)
        self.assertRaises(mysqlx.ProgrammingError, self.session.release_savepoint, "")
        self.session.commit()
        self.schema.drop_collection("mycoll24")

    @tests.foreach_session()
    def test_transaction_savepoint_test25(self):
        """Create a savepoint with empty space."""
        self._drop_collection_if_exists("mycoll25")
        collection = self.schema.create_collection("mycoll25")
        self.session.start_transaction()
        collection.add(
            {"name": "Arya Stark", "age": 14},
            {"name": "Sansa Stark", "age": 20},
        ).execute()
        self.assertEqual(collection.count(), 2)
        self.assertRaises(mysqlx.ProgrammingError, self.session.set_savepoint, " ")
        collection.add({"name": "Robb", "age": 25}).execute()
        self.session.commit()
        self.schema.drop_collection("mycoll25")

    @tests.foreach_session()
    def test_transaction_test1(self):
        """Testing the MCPY-343 commit."""
        config = tests.get_mysqlx_config()
        session1 = mysqlx.get_session(config)
        schema1 = session1.get_default_schema()
        self.session.sql("drop table if exists t2").execute()
        self.session.sql("create table t2(_id int primary key)").execute()
        table = self.schema.get_table("t2")
        table1 = schema1.get_table("t2")
        self.session.start_transaction()
        table.insert("_id").values(1).execute()
        self.assertEqual(table.count(), 1)
        self.assertEqual(table1.count(), 0)
        self.session.commit()
        self.assertEqual(table.count(), 1)
        self.assertEqual(table1.count(), 1)
        self.session.sql("drop table if exists t2").execute()

    @tests.foreach_session()
    def test_transaction_test2(self):
        """Testing the MCPY-343 commit."""
        config = tests.get_mysqlx_config()
        session1 = mysqlx.get_session(config)
        schema1 = session1.get_default_schema()
        self.session.sql("drop table if exists t3").execute()
        self.session.sql("create table t3(_id int primary key)").execute()
        table = self.schema.get_table("t3")
        table1 = schema1.get_table("t3")
        self.session.start_transaction()
        table.insert("_id").values(1).execute()
        self.assertEqual(table.count(), 1)
        self.assertEqual(table1.count(), 0)
        self.session.rollback()
        self.assertEqual(table.count(), 0)
        self.assertEqual(table1.count(), 0)
        self.session.sql("drop table if exists t3").execute()
