# Copyright (c) 2009, 2018, Oracle and/or its affiliates. All rights reserved.
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

"""Unittests for PEP-249

Rewritten from scratch. Found Ian Bicking's test suite and shamelessly
stole few of his ideas. (Geert)
"""

import datetime
import time
import inspect

import tests
import mysql.connector as myconn


class PEP249Base(tests.MySQLConnectorTests):

    def db_connect(self):
        return myconn.connect(use_pure=True, **tests.get_mysql_config())

    def get_connection_id(self, cursor):
        cid = None
        try:
            cursor.execute("SELECT CONNECTION_ID()")
            cid = cursor.fetchone()[0]
        except myconn.errors.Error as err:
            self.fail("Failed getting connection id; {0}".format(str(err)))

        return cid

    def setUp(self):
        self.cnx = self.db_connect()

    def tearDown(self):
        self.cnx.close()


class PEP249ModuleTests(PEP249Base):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_connect(self):
        """Interface exports the connect()-function"""
        self.assertTrue(inspect.isfunction(myconn.connect),
                        "Module does not export the connect()-function")
        cnx = myconn.connect(use_pure=True, **tests.get_mysql_config())
        self.assertTrue(isinstance(cnx, myconn.connection.MySQLConnection),
                        "connect() not returning by default pure "
                        "MySQLConnection object")

        if tests.MYSQL_CAPI:
            # By default use_pure=False
            cnx = myconn.connect(**tests.get_mysql_config())
            self.assertTrue(isinstance(cnx,
                                       myconn.connection_cext.CMySQLConnection),
                            "The connect()-method returns incorrect instance")

    def test_apilevel(self):
        """Interface sets the API level"""
        self.assertTrue(hasattr(myconn, 'apilevel'),
                        "API level is not defined")
        self.assertEqual('2.0', myconn.apilevel,
                         "API Level should be '2.0'")

    def test_threadsafety(self):
        """Interface defines thread safety"""
        self.assertTrue(myconn.threadsafety in (0, 1, 2, 3))
        self.assertEqual(1, myconn.threadsafety)

    def test_paramstyle(self):
        """Interface sets the parameter style"""
        self.assertTrue(myconn.paramstyle in
                       ('qmark', 'numeric', 'named', 'format', 'pyformat'),
                        "paramstyle was assigned an unsupported value")
        self.assertEqual('pyformat', myconn.paramstyle,
                         "paramstyle should be 'pyformat'")


class PEP249ErrorsTests(PEP249Base):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_Warning(self):
        """Interface exports the Warning-exception"""
        self.assertTrue(issubclass(myconn.errors.Warning, Exception),
                        "Warning is not subclass of Exception")

    def test_Error(self):
        """Interface exports the Error-exception"""
        self.assertTrue(issubclass(myconn.errors.Error, Exception),
                        "Error is not subclass of Exception")

    def test_InterfaceError(self):
        """Interface exports the InterfaceError-exception"""
        self.assertTrue(issubclass(myconn.errors.InterfaceError,
                                   myconn.errors.Error),
                        "InterfaceError is not subclass of errors.Error")

    def test_DatabaseError(self):
        """Interface exports the DatabaseError-exception"""
        self.assertTrue(issubclass(myconn.errors.DatabaseError,
                                   myconn.errors.Error),
                        "DatabaseError is not subclass of errors.Error")

    def test_DataError(self):
        """Interface exports the DataError-exception"""
        self.assertTrue(issubclass(myconn.errors.DataError,
                                   myconn.errors.DatabaseError),
                        "DataError is not subclass of errors.DatabaseError")

    def test_OperationalError(self):
        """Interface exports the OperationalError-exception"""
        self.assertTrue(
            issubclass(myconn.errors.OperationalError,
                       myconn.errors.DatabaseError),
            "OperationalError is not subclass of errors.DatabaseError")

    def test_IntegrityError(self):
        """Interface exports the IntegrityError-exception"""
        self.assertTrue(
            issubclass(myconn.errors.IntegrityError,
                       myconn.errors.DatabaseError),
            "IntegrityError is not subclass of errors.DatabaseError")

    def test_InternalError(self):
        """Interface exports the InternalError-exception"""
        self.assertTrue(issubclass(myconn.errors.InternalError,
                                   myconn.errors.DatabaseError),
                        "InternalError is not subclass of errors.DatabaseError")

    def test_ProgrammingError(self):
        """Interface exports the ProgrammingError-exception"""
        self.assertTrue(
            issubclass(myconn.errors.ProgrammingError,
                       myconn.errors.DatabaseError),
            "ProgrammingError is not subclass of errors.DatabaseError")

    def test_NotSupportedError(self):
        """Interface exports the NotSupportedError-exception"""
        self.assertTrue(
            issubclass(myconn.errors.NotSupportedError,
                       myconn.errors.DatabaseError),
            "NotSupportedError is not subclass of errors.DatabaseError")


class PEP249ConnectionTests(PEP249Base):

    def test_close(self):
        """Connection object has close()-method"""
        self.assertTrue(hasattr(self.cnx, 'close'),
                        "Interface connection has no close()-method")
        self.assertTrue(
            inspect.ismethod(self.cnx.close),
            "Interface connection defines connect, but is not a method")

    def test_commit(self):
        """Connection object has commit()-method"""
        self.assertTrue(hasattr(self.cnx, 'commit'),
                        "Interface connection has no commit()-method")
        self.assertTrue(
            inspect.ismethod(self.cnx.commit),
            "Interface connection defines commit, but is not a method")

    def test_rollback(self):
        """Connection object has rollback()-method"""
        self.assertTrue(hasattr(self.cnx, 'rollback'),
                        "Interface connection has no rollback()-method")
        self.assertTrue(
            inspect.ismethod(self.cnx.rollback),
            "Interface connection defines rollback, but is not a method")

    def test_cursor(self):
        """Connection object has cursor()-method"""
        self.assertTrue(hasattr(self.cnx, 'cursor'),
                        "Interface connection has no cursor()-method")
        self.assertTrue(
            inspect.ismethod(self.cnx.cursor),
            "Interface connection defines cursor, but is not a method")
        self.assertTrue(
            isinstance(self.cnx.cursor(), myconn.cursor.MySQLCursor),
            "Interface connection cursor()-method does not return a cursor")


class PEP249CursorTests(PEP249Base):

    def setUp(self):
        self.cnx = self.db_connect()
        self.cur = self.cnx.cursor()

    def test_description(self):
        """Cursor object has description-attribute"""
        self.assertTrue(hasattr(self.cur, 'description'),
                        "Cursor object has no description-attribute")
        self.assertEqual(None, self.cur.description,
                         "Cursor object's description should default ot None")

    def test_rowcount(self):
        """Cursor object has rowcount-attribute"""
        self.assertTrue(hasattr(self.cur, 'rowcount'),
                        "Cursor object has no rowcount-attribute")
        self.assertEqual(-1, self.cur.rowcount,
                         "Cursor object's rowcount should default to -1")

    def test_lastrowid(self):
        """Cursor object has lastrowid-attribute"""
        self.assertTrue(hasattr(self.cur, 'lastrowid'),
                        "Cursor object has no lastrowid-attribute")
        self.assertEqual(None, self.cur.lastrowid,
                         "Cursor object's lastrowid should default to None")

    def test_callproc(self):
        """Cursor object has callproc()-method"""
        self.assertTrue(hasattr(self.cur, 'callproc'),
                        "Cursor object has no callproc()-method")
        self.assertTrue(inspect.ismethod(self.cur.callproc),
                        "Cursor object defines callproc, but is not a method")

    def test_close(self):
        """Cursor object has close()-method"""
        self.assertTrue(hasattr(self.cur, 'close'),
                        "Cursor object has no close()-method")
        self.assertTrue(inspect.ismethod(self.cur.close),
                        "Cursor object defines close, but is not a method")

    def test_execute(self):
        """Cursor object has execute()-method"""
        self.assertTrue(hasattr(self.cur, 'execute'),
                        "Cursor object has no execute()-method")
        self.assertTrue(inspect.ismethod(self.cur.execute),
                        "Cursor object defines execute, but is not a method")

    def test_executemany(self):
        """Cursor object has executemany()-method"""
        self.assertTrue(hasattr(self.cur, 'executemany'),
                        "Cursor object has no executemany()-method")
        self.assertTrue(
            inspect.ismethod(self.cur.executemany),
            "Cursor object defines executemany, but is not a method")

    def test_fetchone(self):
        """Cursor object has fetchone()-method"""
        self.assertTrue(hasattr(self.cur, 'fetchone'),
                        "Cursor object has no fetchone()-method")
        self.assertTrue(inspect.ismethod(self.cur.fetchone),
                        "Cursor object defines fetchone, but is not a method")

    def test_fetchmany(self):
        """Cursor object has fetchmany()-method"""
        self.assertTrue(hasattr(self.cur, 'execute'),
                        "Cursor object has no fetchmany()-method")
        self.assertTrue(inspect.ismethod(self.cur.fetchmany),
                        "Cursor object defines fetchmany, but is not a method")

    def test_fetchall(self):
        """Cursor object has fetchall()-method"""
        self.assertTrue(hasattr(self.cur, 'fetchall'),
                        "Cursor object has no fetchall()-method")
        self.assertTrue(inspect.ismethod(self.cur.fetchall),
                        "Cursor object defines fetchall, but is not a method")

    def test_nextset(self):
        """Cursor object has nextset()-method"""
        self.assertTrue(hasattr(self.cur, 'nextset'),
                        "Cursor object has no nextset()-method")
        self.assertTrue(inspect.ismethod(self.cur.nextset),
                        "Cursor object defines nextset, but is not a method")

    def test_arraysize(self):
        """Cursor object has arraysize-attribute"""
        self.assertTrue(hasattr(self.cur, 'arraysize'),
                        "Cursor object has no arraysize-attribute")
        self.assertEqual(1, self.cur.arraysize,
                         "Cursor object's arraysize should default to 1")

    def test_setinputsizes(self):
        """Cursor object has setinputsizes()-method"""
        self.assertTrue(hasattr(self.cur, 'setinputsizes'),
                        "Cursor object has no setinputsizes()-method")
        self.assertTrue(inspect.ismethod(self.cur.setinputsizes),
                        "Cursor object's setinputsizes should default to 1")

    def test_setoutputsize(self):
        """Cursor object has setoutputsize()-method"""
        self.assertTrue(hasattr(self.cur, 'setoutputsize'),
                        "Cursor object has no setoutputsize()-method")
        self.assertTrue(inspect.ismethod(self.cur.setoutputsize),
                        "Cursor object's setoutputsize should default to 1")

    def _isolation_setup(self, drop, create):
        cursor = self.cnx.cursor()
        try:
            cursor.execute(drop)
            cursor.execute(create)
        except myconn.errors.Error as err:
            self.fail("Failed setting up test table; {0}".format(err))
        cursor.close()

    def _isolation_connection_equal(self, cnx1, cnx2):
        cid1 = self.get_connection_id(cnx1)
        cid2 = self.get_connection_id(cnx2)
        return (cid1 == cid2)

    def _isolation_cleanup(self, drop):
        cursor = self.cnx.cursor()
        try:
            cursor.execute(drop)
        except myconn.errors.Error as err:
            self.fail("Failed cleaning up; {0}".format(err))
        cursor.close()

    def _isolation_test(self, cnx1, cnx2, engine='MyISAM'):

        cur1 = cnx1.cursor()
        cur2 = cnx2.cursor()
        data = (1, 'myconnpy')
        tbl = 'myconnpy_cursor_isolation'

        stmt_create = (
            "CREATE TABLE {table} "
            "(col1 INT, col2 VARCHAR(30), PRIMARY KEY (col1)) "
            "ENGINE={engine}"
        ).format(table=tbl, engine=engine)
        stmt_drop = "DROP TABLE IF EXISTS {table}".format(table=tbl)
        stmt_insert = (
            "INSERT INTO {table} (col1,col2) "
            "VALUES (%s,%s)"
        ).format(table=tbl)
        stmt_select = "SELECT col1,col2 FROM {table}".format(table=tbl)

        # Setup
        cur1.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        cur2.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        self._isolation_setup(stmt_drop, stmt_create)
        conn_equal = self._isolation_connection_equal(cur1, cur2)
        if cnx1 == cnx2 and not conn_equal:
            self.fail("Cursors should have same connection ID")
        elif cnx1 != cnx2 and conn_equal:
            self.fail("Cursors should have different connection ID")

        # Insert data
        try:
            cur1.execute(stmt_insert, data)
        except myconn.errors.Error as err:
            self.fail("Failed inserting test data; {0}".format(str(err)))

        # Query for data
        result = None
        try:
            cur2.execute(stmt_select)
            result = cur2.fetchone()
        except myconn.errors.InterfaceError:
            pass
        except myconn.errors.Error as err:
            self.fail("Failed querying for test data; {0}".format(str(err)))

        if conn_equal:
            self.assertEqual(data, result)
        elif not conn_equal and engine.lower() == 'innodb':
            self.assertEqual(None, result)

        # Clean up
        self._isolation_cleanup(stmt_drop)

        cur1.close()
        cur2.close()

    def test_isolation1(self):
        """Cursor isolation between 2 cursor on same connection"""
        self._isolation_test(self.cnx, self.cnx, 'MyISAM')

    def test_isolation2(self):
        """Cursor isolation with 2 cursors, different connections, trans."""
        db2 = self.db_connect()
        if tests.have_engine(db2, 'InnoDB'):
            self._isolation_test(self.cnx, db2, 'InnoDB')


class PEP249TypeObjConstructorsTests(PEP249Base):

    def test_Date(self):
        """Interface exports Date"""
        exp = datetime.date(1977, 6, 14)
        self.assertEqual(myconn.Date(1977, 6, 14), exp,
                         "Interface Date should return a datetime.date")

    def test_Time(self):
        """Interface exports Time"""
        exp = datetime.time(23, 56, 13)
        self.assertEqual(myconn.Time(23, 56, 13), exp,
                         "Interface Time should return a datetime.time")

    def test_Timestamp(self):
        """Interface exports Timestamp"""
        adate = (1977, 6, 14, 21, 54, 23)
        exp = datetime.datetime(*adate)
        self.assertEqual(
            myconn.Timestamp(*adate), exp,
            "Interface Timestamp should return a datetime.datetime")

    def test_DateFromTicks(self):
        """Interface exports DateFromTicks"""
        ticks = 1
        exp = datetime.date(*time.localtime(ticks)[:3])
        self.assertEqual(
            myconn.DateFromTicks(ticks), exp,
            "Interface DateFromTicks should return a datetime.date")

    def test_TimeFromTicks(self):
        """Interface exports TimeFromTicks"""
        ticks = 1
        exp = datetime.time(*time.localtime(ticks)[3:6])
        self.assertEqual(
            myconn.TimeFromTicks(ticks), exp,
            "Interface TimeFromTicks should return a datetime.time")

    def test_TimestampFromTicks(self):
        """Interface exports TimestampFromTicks"""
        ticks = 1
        exp = datetime.datetime(*time.localtime(ticks)[:6])
        self.assertEqual(
            myconn.TimestampFromTicks(ticks), exp,
            "Interface TimestampFromTicks should return a datetime.datetime")

    def test_Binary(self):
        """Interface exports Binary"""
        exp = r'\u82b1'.encode('utf-8')
        self.assertEqual(
            myconn.Binary(r'\u82b1'.encode('utf-8')), exp,
            "Interface Binary should return a str")

    def test_STRING(self):
        """Interface exports STRING"""
        self.assertTrue(hasattr(myconn, 'STRING'))
        self.assertTrue(
            isinstance(myconn.STRING, myconn.dbapi._DBAPITypeObject),
            "Interface STRING should return a _DBAPITypeObject")

    def test_BINARY(self):
        """Interface exports BINARY"""
        self.assertTrue(hasattr(myconn, 'BINARY'))
        self.assertTrue(
            isinstance(myconn.BINARY, myconn.dbapi._DBAPITypeObject),
            "Interface BINARY should return a _DBAPITypeObject")

    def test_NUMBER(self):
        """Interface exports NUMBER"""
        self.assertTrue(hasattr(myconn, 'NUMBER'))
        self.assertTrue(
            isinstance(myconn.NUMBER, myconn.dbapi._DBAPITypeObject),
            "Interface NUMBER should return a _DBAPITypeObject")

    def test_DATETIME(self):
        """Interface exports DATETIME"""
        self.assertTrue(hasattr(myconn, 'DATETIME'))
        self.assertTrue(
            isinstance(myconn.DATETIME, myconn.dbapi._DBAPITypeObject),
            "Interface DATETIME should return a _DBAPITypeObject")

    def test_ROWID(self):
        """Interface exports ROWID"""
        self.assertTrue(hasattr(myconn, 'ROWID'))
        self.assertTrue(
            isinstance(myconn.ROWID, myconn.dbapi._DBAPITypeObject),
            "Interface ROWID should return a _DBAPITypeObject")
