# -*- coding: utf-8 -*-
# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2009, 2015, Oracle and/or its affiliates. All rights reserved.

# MySQL Connector/Python is licensed under the terms of the GPLv2
# <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most
# MySQL Connectors. There are special exceptions to the terms and
# conditions of the GPLv2 as it is applied to this software, see the
# FOSS License Exception
# <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Incur., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA

"""Test module for bugs

Bug test cases specific to a particular Python (major) version are loaded
from py2.bugs or py3.bugs.

This module was originally located in python2/tests and python3/tests. It
should contain bug test cases which work for both Python v2 and v3.

Whenever a bug is bout to a specific Python version, put the test cases
in tests/py2/bugs.py or tests/py3/bugs.py. It might be that these files need
to be created first.
"""

import io
import os
import gc
import tempfile
from datetime import datetime, timedelta, time
from threading import Thread
import traceback
import time
import unittest

import tests
from tests import foreach_cnx, cnx_config
from . import PY2
from mysql.connector import (connection, cursor, conversion, protocol,
                             errors, constants, pooling)
from mysql.connector.optionfiles import read_option_files
import mysql.connector

try:
    from mysql.connector.connection_cext import CMySQLConnection
except ImportError:
    # Test without C Extension
    CMySQLConnection = None

ERR_NO_CEXT = "C Extension not available"


class Bug328998Tests(tests.MySQLConnectorTests):
    """Tests where connection timeout has been set"""

    @cnx_config(connection_timeout=1)
    @foreach_cnx()
    def test_set_connection_timeout(self):
        cur = self.cnx.cursor()
        self.assertRaises(
            errors.OperationalError,
            cur.execute,
            "SELECT SLEEP({0})".format(self.config['connection_timeout'] + 4)
        )


class Bug437972Tests(tests.MySQLConnectorTests):
    def test_windows_tcp_connection(self):
        """lp:437972 TCP connection to Windows"""
        if os.name != 'nt':
            pass

        cnx = None
        try:
            cnx = connection.MySQLConnection(**tests.get_mysql_config())
        except errors.InterfaceError:
            self.fail()

        if cnx:
            cnx.close()


class Bug441430Tests(tests.MySQLConnectorTests):

    @foreach_cnx()
    def test_execute_return(self):
        """lp:441430 cursor.execute*() should return the cursor.rowcount"""

        cur = self.cnx.cursor()
        tbl = "buglp44130"
        cur.execute("DROP TABLE IF EXISTS %s" % tbl)
        cur.execute("CREATE TABLE %s (id INT)" % tbl)
        cur.execute("INSERT INTO %s VALUES (%%s),(%%s)" % tbl, (1, 2,))
        self.assertEqual(2, cur.rowcount)
        stmt = "INSERT INTO %s VALUES (%%s)" % tbl
        res = cur.executemany(stmt, [(3,), (4,), (5,), (6,), (7,), (8,)])
        self.assertEqual(6, cur.rowcount)
        res = cur.execute("UPDATE %s SET id = id + %%s" % tbl, (10,))
        self.assertEqual(8, cur.rowcount)
        cur.close()
        self.cnx.close()


class Bug454782(tests.MySQLConnectorTests):

    """lp:454782 fetchone() does not follow pep-0249"""

    @foreach_cnx()
    def test_fetch_retun_values(self):
        cur = self.cnx.cursor()
        self.assertEqual(None, cur.fetchone())
        self.assertEqual([], cur.fetchmany())
        self.assertRaises(errors.InterfaceError, cur.fetchall)
        cur.close()
        self.cnx.close()


class Bug454790(tests.MySQLConnectorTests):

    """lp:454790 pyformat / other named parameters broken"""

    @foreach_cnx()
    def test_pyformat(self):
        cur = self.cnx.cursor()

        data = {'name': 'Geert', 'year': 1977}
        cur.execute("SELECT %(name)s,%(year)s", data)
        self.assertEqual(('Geert', 1977), cur.fetchone())

        data = [
            {'name': 'Geert', 'year': 1977},
            {'name': 'Marta', 'year': 1980}
        ]
        cur.executemany("SELECT %(name)s,%(year)s", data)
        self.assertEqual(2, cur.rowcount)
        cur.close()
        self.cnx.close()


class Bug480360(tests.MySQLConnectorTests):

    """lp:480360: fetchall() should return [] when no result"""

    @foreach_cnx()
    def test_fetchall(self):
        cur = self.cnx.cursor()

        # Trick to get empty result not needing any table
        cur.execute("SELECT * FROM (SELECT 1) AS t WHERE 0 = 1")
        self.assertEqual([], cur.fetchall())
        cur.close()
        self.cnx.close()

@unittest.skipIf(tests.MYSQL_VERSION >= (5, 6, 6),
                 "Bug380528 not tested with MySQL version >= 5.6.6")
class Bug380528(tests.MySQLConnectorTests):

    """lp:380528: we do not support old passwords"""

    @foreach_cnx()
    def test_old_password(self):
        cur = self.cnx.cursor()

        if self.config['unix_socket'] and os.name != 'nt':
            user = "'myconnpy'@'localhost'"
        else:
            user = "'myconnpy'@'%s'" % (config['host'])

        try:
            cur.execute("GRANT SELECT ON %s.* TO %s" %
                        (self.config['database'], user))
            cur.execute("SET PASSWORD FOR %s = OLD_PASSWORD('fubar')" % (user))
        except:
            self.fail("Failed executing grant.")
        cur.close()

        # Test using the newly created user
        test_config = self.config.copy()
        test_config['user'] = 'myconnpy'
        test_config['password'] = 'fubar'

        self.assertRaises(errors.NotSupportedError,
                          self.cnx.__class__, **test_config)

        self.cnx = self.cnx.__class__(**self.config)
        cur = self.cnx.cursor()
        try:
            cur.execute("REVOKE SELECT ON %s.* FROM %s" %
                        (self.config['database'], user))
            cur.execute("DROP USER %s" % (user))
        except mysql.connector.Error as exc:
            self.fail("Failed cleaning up user {0}: {1}".format(user, exc))
        cur.close()


class Bug499362(tests.MySQLConnectorTests):

    """lp:499362 Setting character set at connection fails"""

    @cnx_config(charset='latin1')
    @foreach_cnx()
    def test_charset(self):
        cur = self.cnx.cursor()

        ver = self.cnx.get_server_version()
        if ver < (5, 1, 12):
            exp1 = [('character_set_client', 'latin1'),
                    ('character_set_connection', 'latin1'),
                    ('character_set_database', 'utf8'),
                    ('character_set_filesystem', 'binary'),
                    ('character_set_results', 'latin1'),
                    ('character_set_server', 'utf8'),
                    ('character_set_system', 'utf8')]
            exp2 = [('character_set_client', 'latin2'),
                    ('character_set_connection', 'latin2'),
                    ('character_set_database', 'utf8'),
                    ('character_set_filesystem', 'binary'),
                    ('character_set_results', 'latin2'),
                    ('character_set_server', 'utf8'),
                    ('character_set_system', 'utf8')]
            varlst = []
            stmt = r"SHOW SESSION VARIABLES LIKE 'character\_set\_%%'"
        else:
            exp1 = [('CHARACTER_SET_CONNECTION', 'latin1'),
                    ('CHARACTER_SET_CLIENT', 'latin1'),
                    ('CHARACTER_SET_RESULTS', 'latin1')]
            exp2 = [('CHARACTER_SET_CONNECTION', 'latin2'),
                    ('CHARACTER_SET_CLIENT', 'latin2'),
                    ('CHARACTER_SET_RESULTS', 'latin2')]

            varlst = ['character_set_client', 'character_set_connection',
                      'character_set_results']
            stmt = """SELECT * FROM INFORMATION_SCHEMA.SESSION_VARIABLES
                WHERE VARIABLE_NAME IN (%s,%s,%s)"""

        cur.execute(stmt, varlst)
        res1 = cur.fetchall()

        self.cnx.set_charset_collation('latin2')
        cur.execute(stmt, varlst)
        res2 = cur.fetchall()

        cur.close()
        self.cnx.close()

        self.assertTrue(tests.cmp_result(exp1, res1))
        self.assertTrue(tests.cmp_result(exp2, res2))


class Bug501290(tests.MySQLConnectorTests):

    """lp:501290 Client flags are set to None when connecting"""

    def _setup(self):
        self.capabilities = self.cnx._handshake['capabilities']

        self.default_flags = constants.ClientFlag.get_default()
        if self.capabilities & constants.ClientFlag.PLUGIN_AUTH:
            self.default_flags |= constants.ClientFlag.PLUGIN_AUTH

    @foreach_cnx()
    def test_default(self):
        self._setup()
        for flag in constants.ClientFlag.default:
            self.assertTrue(self.cnx._client_flags & flag)

    @foreach_cnx()
    def test_set_unset(self):
        self._setup()
        orig = self.cnx._client_flags

        exp = self.default_flags | constants.ClientFlag.COMPRESS
        self.cnx.set_client_flags([constants.ClientFlag.COMPRESS])
        for flag in constants.ClientFlag.default:
            self.assertTrue(self.cnx._client_flags & flag)

        self.cnx.set_client_flags([-constants.ClientFlag.COMPRESS])
        self.assertEqual(self.cnx._client_flags, orig)

    @foreach_cnx()
    def test_isset_client_flag(self):
        self._setup()
        flag = constants.ClientFlag.COMPRESS
        data = self.default_flags | flag

        self.cnx._client_flags = data
        self.assertEqual(True, self.cnx.isset_client_flag(flag))


class Bug507466(tests.MySQLConnectorTests):

    """lp:507466 BIT values are not converted correctly to Python"""

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        try:
            cur = cnx.cursor()
            cur.execute("DROP TABLE IF EXISTS myconnpy_bits")
        except:
            pass
        cnx.close()

    @foreach_cnx()
    def test_bits(self):
        cur = self.cnx.cursor()

        cur.execute("DROP TABLE IF EXISTS myconnpy_bits")
        cur.execute("""CREATE TABLE `myconnpy_bits` (
          `id` int NOT NULL AUTO_INCREMENT,
          `c1` bit(8) DEFAULT NULL,
          `c2` bit(16) DEFAULT NULL,
          `c3` bit(24) DEFAULT NULL,
          `c4` bit(32) DEFAULT NULL,
          `c5` bit(40) DEFAULT NULL,
          `c6` bit(48) DEFAULT NULL,
          `c7` bit(56) DEFAULT NULL,
          `c8` bit(64) DEFAULT NULL,
          PRIMARY KEY (id)
        )
        """)

        insert = """insert into myconnpy_bits (c1,c2,c3,c4,c5,c6,c7,c8)
            values (%s,%s,%s,%s,%s,%s,%s,%s)"""
        select = "SELECT c1,c2,c3,c4,c5,c6,c7,c8 FROM myconnpy_bits ORDER BY id"

        data = []
        data.append((0, 0, 0, 0, 0, 0, 0, 0))
        data.append((
            1 << 7, 1 << 15, 1 << 23, 1 << 31,
            1 << 39, 1 << 47, 1 << 55, (1 << 63)-1,
        ))
        cur.executemany(insert, data)
        cur.execute(select)
        rows = cur.fetchall()

        self.assertEqual(rows, data)

        self.cnx.close()


class Bug519301(tests.MySQLConnectorTests):

    """lp:519301 Temporary connection failures with 2 exceptions"""

    @foreach_cnx()
    def test_auth(self):
        config = self.config.copy()
        config['user'] = 'ham'
        config['password'] = 'spam'

        for _ in range(1, 100):
            try:
                cnx = self.cnx.__class__(**config)
            except errors.ProgrammingError:
                pass
            except errors.Error as err:
                self.fail("Failing authenticating: {0}".format(str(err)))
            except:
                raise
            else:
                cnx.close()


class Bug524668(tests.MySQLConnectorTests):

    """lp:524668 Error in server handshake with latest code"""

    def test_handshake(self):
        handshake = bytearray(
            b'\x47\x00\x00\x00\x0a\x35\x2e\x30\x2e\x33\x30\x2d\x65'
            b'\x6e\x74\x65\x72\x70\x72\x69\x73\x65\x2d\x67\x70\x6c'
            b'\x2d\x6c\x6f'
            b'\x67\x00\x09\x01\x00\x00\x68\x34\x69\x36\x6f\x50\x21\x4f\x00'
            b'\x2c\xa2\x08\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
            b'\x00'
            b'\x00\x00\x4c\x6e\x67\x39\x26\x50\x44\x40\x57\x72\x59\x48\x00'
        )

        prtcl = protocol.MySQLProtocol()
        try:
            prtcl.parse_handshake(handshake)
        except:
            self.fail("Failed handling handshake")


class Bug571201(tests.MySQLConnectorTests):
    """lp:571201 Problem with more than one statement at a time"""

    def setUp(self):
        self.tbl = 'Bug571201'

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        try:
            cur = cnx.cursor()
            cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))
        except:
            pass
        cnx.close()

    @foreach_cnx()
    def test_multistmts(self):
        cur = self.cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))
        cur.execute(("CREATE TABLE {0} ( "
                     "id INT AUTO_INCREMENT KEY, "
                     "c1 INT)").format(self.tbl))

        stmts = [
            "SELECT * FROM %s" % (self.tbl),
            "INSERT INTO %s (c1) VALUES (10),(20)" % (self.tbl),
            "SELECT * FROM %s" % (self.tbl),
        ]
        result_iter = cur.execute(';'.join(stmts), multi=True)

        self.assertEqual(None, next(result_iter).fetchone())
        self.assertEqual(2, next(result_iter).rowcount)
        exp = [(1, 10), (2, 20)]
        self.assertEqual(exp, next(result_iter).fetchall())
        self.assertRaises(StopIteration, next, result_iter)

        self.cnx.close()


class Bug551533and586003(tests.MySQLConnectorTests):

    """lp: 551533 as 586003: impossible to retrieve big result sets"""

    def setUp(self):
        self.tbl = 'Bug551533'

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cnx.cmd_query("DROP TABLE IF EXISTS {table}".format(table=self.tbl))
        cnx.close()

    @cnx_config(connection_timeout=10)
    @foreach_cnx()
    def test_select(self):
        cur = self.cnx.cursor()

        cur.execute("DROP TABLE IF EXISTS {table}".format(table=self.tbl))
        cur.execute(
            ("CREATE TABLE {table} (id INT AUTO_INCREMENT KEY, "
             "c1 VARCHAR(100) DEFAULT 'abcabcabcabcabcabcabcabcabcabc') "
             "ENGINE=INNODB").format(table=self.tbl)
        )

        insert = "INSERT INTO {table} (id) VALUES (%s)".format(table=self.tbl)
        exp = 20000
        cur.executemany(insert, [(None,)] * exp)
        self.cnx.commit()

        cur.execute(
            'SELECT * FROM {table} LIMIT 20000'.format(table=self.tbl))
        try:
            rows = cur.fetchall()
        except errors.Error as err:
            self.fail("Failed retrieving big result set: {0}".format(err))
        else:
            self.assertEqual(exp, cur.rowcount)
            self.assertEqual(exp, len(rows))


class Bug675425(tests.MySQLConnectorTests):

    """lp: 675425: Problems with apostrophe"""

    def setUp(self):
        self.tbl = 'Bug675425'

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))
        cnx.close()

    @foreach_cnx()
    def test_executemany_escape(self):
        cur = self.cnx.cursor()

        cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))
        cur.execute("CREATE TABLE {0} (c1 VARCHAR(30),"
                    " c2 VARCHAR(30))".format(self.tbl))

        data = [
            ("ham", "spam",),
            ("spam", "ham",),
            ("ham \\' spam", "spam ' ham",)
        ]
        sql = "INSERT INTO {0} VALUES (%s, %s)".format(self.tbl)
        try:
            cur.executemany(sql, data)
        except Exception as exc:
            self.fail(str(exc))

        self.cnx.close()


class Bug695514(tests.MySQLConnectorTests):

    """lp: 695514: Infinite recursion when setting connection client_flags"""

    @foreach_cnx()
    def test_client_flags(self):
        try:
            config = tests.get_mysql_config()
            config['connection_timeout'] = 2
            config['client_flags'] = constants.ClientFlag.get_default()
            self.cnx = self.cnx.__class__(**config)
        except:
            self.fail("Failed setting client_flags using integer")


class Bug809033(tests.MySQLConnectorTests):

    """lp: 809033: Lost connection causes infinite loop"""

    def setUp(self):
        self.table_name = 'Bug809033'

    def _setup(self):
        self.cnx.cmd_query("DROP TABLE IF EXISTS {0}".format(self.table_name))
        table = (
            "CREATE TABLE {table} ("
            " id INT UNSIGNED NOT NULL AUTO_INCREMENT,"
            " c1 VARCHAR(255) DEFAULT '{default}',"
            " PRIMARY KEY (id)"
            ")"
        ).format(table=self.table_name, default='a' * 255)
        self.cnx.cmd_query(table)

        stmt = "INSERT INTO {table} (id) VALUES {values}".format(
            table=self.table_name,
            values=','.join(['(NULL)'] * 1024)
        )
        self.cnx.cmd_query(stmt)

    def tearDown(self):
        try:
            cnx = connection.MySQLConnection(**tests.get_mysql_config())
            cnx.cmd_query(
                "DROP TABLE IF EXISTS {0}".format(self.table_name))
            cnx.close()
        except:
            pass

    @foreach_cnx()
    def test_lost_connection(self):
        self._setup()

        def kill(connection_id):
            """Kill connection using separate connection"""
            killer = connection.MySQLConnection(**tests.get_mysql_config())
            time.sleep(1)
            killer.cmd_query("KILL {0}".format(connection_id))
            killer.close()

        def sleepy_select(cnx):
            """Execute a SELECT statement which takes a while to complete"""
            cur = cnx.cursor()
            # Ugly query ahead!
            stmt = "SELECT x1.*, x2.* from {table} as x1, {table} as x2".format(
                table=self.table_name)
            cur.execute(stmt)
            # Save the error so we can check in the calling thread
            cnx.test_error = None
            try:
                cur.fetchall()
            except errors.Error as err:
                cnx.test_error = err

        worker = Thread(target=sleepy_select, args=[self.cnx])
        killer = Thread(target=kill, args=[self.cnx.connection_id])
        worker.start()
        killer.start()
        worker.join()
        killer.join()

        self.assertTrue(
            isinstance(self.cnx.test_error,
                       (errors.InterfaceError, errors.OperationalError))
        )

        self.cnx.close()


class Bug865859(tests.MySQLConnectorTests):

    """lp: 865859: sock.recv fails to return in some cases (infinite wait)"""

    def setUp(self):
        self.table_name = 'Bug865859'

    @cnx_config(connection_timeout=1)
    @foreach_cnx()
    def test_reassign_connection(self):
        cur = self.cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS {0}".format(self.table_name))
        cur.execute("CREATE TABLE {0} (c1 INT)".format(self.table_name))
        cur.execute("INSERT INTO {0} (c1) VALUES (1)".format(self.table_name))

        try:
            # We create a new cnx, replacing current
            self.cnx = self.cnx.__class__(**self.config)
            cur = self.cnx.cursor()
            cur.execute("DROP TABLE IF EXISTS {0}".format(self.table_name))
        except errors.InterfaceError as err:
            self.fail(
                "Connection was not closed, we got timeout: {0}".format(err))
        else:
            cur.close()
            self.cnx.close()


class BugOra13395083(tests.MySQLConnectorTests):

    """BUG#13395083: Using time zones"""

    def setUp(self):
        self.table_name = 'BugOra13395083'

    @cnx_config(time_zone="+00:00")
    @foreach_cnx()
    def test_time_zone(self):
        utc = tests.UTCTimeZone()
        testzone = tests.TestTimeZone(+2)

        # Store a datetime in UTC into a TIMESTAMP column
        now_utc = datetime.utcnow().replace(microsecond=0, tzinfo=utc)

        cur = self.cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS {0}".format(self.table_name))
        cur.execute("CREATE TABLE {0} (c1 TIMESTAMP)".format(self.table_name))
        cur.execute(
            "INSERT INTO {0} (c1) VALUES (%s)".format(self.table_name),
            (now_utc,))
        self.cnx.commit()

        cur.execute("SELECT c1 FROM {0}".format(self.table_name))
        row = cur.fetchone()
        self.assertEqual(now_utc, row[0].replace(tzinfo=utc))

        self.cnx.time_zone = "+02:00"
        cur.execute("SELECT c1 FROM {0}".format(self.table_name))
        row = cur.fetchone()
        self.assertEqual(now_utc.astimezone(testzone),
                         row[0].replace(tzinfo=testzone))

        self.cnx.close()


class BugOra13392739(tests.MySQLConnectorTests):

    """BUG#13392739: MySQLConnection.ping()"""

    @cnx_config(connection_timeout=2, unix_socket=None)
    @foreach_cnx()
    def test_ping(self):
        cnx = self.cnx.__class__()
        self.assertRaises(errors.InterfaceError, cnx.ping)

        try:
            self.cnx.ping()
        except Exception as e:
            self.fail("Error raised although connection should be "
                      "available (%s)." % e)

        self.cnx.close()
        self.assertRaises(errors.InterfaceError, self.cnx.ping)

        try:
            self.cnx.ping(reconnect=True)
        except Exception as e:
            self.fail("Error raised although ping should reconnect. (%s)" % e)

        # Temper with the host to which we reconnect to simulate the
        # MySQL not being available.
        self.cnx.disconnect()
        self.cnx._host = 'some-unknown-host-somwhere-on.mars'
        self.assertRaises(errors.InterfaceError, self.cnx.ping, reconnect=True)

    @cnx_config(connection_timeout=2, unix_socket=None)
    @foreach_cnx()
    def test_reconnect(self):
        self.cnx.disconnect()
        self.assertRaises(errors.InterfaceError, self.cnx.ping)
        try:
            self.cnx.reconnect()
        except:
            self.fail("Errors raised although connection should have been "
                      "reconnected.")

        self.cnx.disconnect()
        # Temper with the host to which we reconnect to simulate the
        # MySQL not being available.
        self.cnx._host = 'some-unknown-host-somwhere-on-mars.example.com'
        self.assertRaises(errors.InterfaceError, self.cnx.reconnect)
        try:
            self.cnx.reconnect(attempts=3)
        except errors.InterfaceError as exc:
            self.assertTrue('3 attempt(s)' in str(exc))


class BugOra13435186(tests.MySQLConnectorTests):
    def setUp(self):
        self.sample_size = 100
        self.tolerate = 5
        self._reset_samples()
        self.samples = [0, ] * self.sample_size
        gc.collect()

    def _reset_samples(self):
        self.samples = [0, ] * self.sample_size

    def _assert_flat_line(self, samples):
        counters = {}
        for value in samples:
            try:
                counters[value] = counters[value] + 1
            except KeyError:
                counters[value] = 1

        if len(counters) > self.tolerate:
            self.fail("Counters of collected object higher than tolerated.")

    def test_converter(self):
        for i in range(0, self.sample_size):
            conversion.MySQLConverter()
            self.samples[i] = len(gc.get_objects())

        self._assert_flat_line(self.samples)

    @foreach_cnx()
    def test_connection(self):
        # Create a connection and close using close()-method
        for i in range(0, self.sample_size):
            cnx = self.cnx.__class__(**self.config)
            cnx.close()
            self.samples[i] = len(gc.get_objects())

        self._assert_flat_line(self.samples)

        self._reset_samples()
        # Create a connection and rely on destructor to close
        for i in range(0, self.sample_size):
            cnx = self.cnx.__class__(**self.config)
            self.samples[i] = len(gc.get_objects())

        self._assert_flat_line(self.samples)

    @foreach_cnx()
    def test_cursor(self):
        # Create a cursor and close using close()-method
        for i in range(0, self.sample_size):
            cursor = self.cnx.cursor()
            cursor.close()
            self.samples[i] = len(gc.get_objects())

        self._assert_flat_line(self.samples)

        self._reset_samples()
        # Create a cursor and rely on destructor to close
        for i in range(0, self.sample_size):
            cursor = self.cnx.cursor()
            self.samples[i] = len(gc.get_objects())

        self._assert_flat_line(self.samples)


class BugOra14184643(tests.MySQLConnectorTests):

    """BUG#14184643: cmd_query() disregards waiting results"""

    @foreach_cnx()
    def test_cmd_query(self):
        self.cnx.cmd_query('SELECT 1')
        self.assertRaises(errors.InternalError, self.cnx.cmd_query,
                          'SELECT 2')

    @foreach_cnx(connection.MySQLConnection)
    def test_get_rows(self):
        self.cnx.cmd_query('SELECT 1')
        self.cnx.get_rows()
        self.assertRaises(errors.InternalError, self.cnx.get_rows)

        self.cnx.cmd_query('SELECT 1')
        self.cnx.get_row()
        self.assertEqual(None, self.cnx.get_row()[0])
        self.assertRaises(errors.InternalError, self.cnx.get_row)

    @unittest.skipIf(not CMySQLConnection, ERR_NO_CEXT)
    @foreach_cnx(CMySQLConnection)
    def test_get_rows(self):
        self.cnx.cmd_query('SELECT 1')
        while True:
            self.cnx.get_rows()
            if not self.cnx.next_result():
                break
            else:
                self.fail("Found multiple results where only 1 was expected")
        self.assertRaises(errors.InternalError, self.cnx.get_rows)

    @foreach_cnx()
    def test_cmd_statistics(self):
        self.cnx.cmd_query('SELECT 1')
        self.assertRaises(errors.InternalError, self.cnx.cmd_statistics)
        self.cnx.get_rows()


class BugOra14208326(tests.MySQLConnectorTests):

    """BUG#14208326: cmd_query() does not handle multiple statements"""

    def setUp(self):
        self.table = "BugOra14208326"

    def _setup(self):
        self.cnx.cmd_query("DROP TABLE IF EXISTS %s" % self.table)
        self.cnx.cmd_query("CREATE TABLE %s (id INT)" % self.table)

    @foreach_cnx(connection.MySQLConnection)
    def test_cmd_query(self):
        self._setup()
        self.assertRaises(errors.InterfaceError,
                          self.cnx.cmd_query, 'SELECT 1; SELECT 2')

    @unittest.skipIf(not CMySQLConnection, ERR_NO_CEXT)
    @foreach_cnx(CMySQLConnection)
    def test_cmd_query_iter(self):
        self._setup()
        stmt = 'SELECT 1; INSERT INTO %s VALUES (1),(2); SELECT 3'
        results = []
        try:
            for result in self.cnx.cmd_query_iter(stmt % self.table):
                results.append(result)
                if 'columns' in result:
                    results.append(self.cnx.get_rows())
        except NotImplementedError:
            # Some cnx are not implementing this
            if not isinstance(self.cnx, CMySQLConnection):
                raise


class BugOra14201459(tests.MySQLConnectorTests):

    """BUG#14201459: Server error 1426 should raise ProgrammingError"""

    def setUp(self):
        self.tbl = 'Bug14201459'

    def tearDown(self):
        config = tests.get_mysql_config()
        self.cnx = connection.MySQLConnection(**config)
        self._setup()

    def _setup(self):
        self.cnx.cmd_query("DROP TABLE IF EXISTS %s" % (self.tbl))

    @foreach_cnx()
    def test_error1426(self):
        cur = self.cnx.cursor()
        self._setup()
        create = "CREATE TABLE %s (c1 TIME(7))" % self.tbl
        try:
            cur.execute(create)
        except errors.ProgrammingError as exception:
            if tests.MYSQL_VERSION < (5, 6, 4) and exception.errno != 1064:
                self.fail("ProgrammingError is not Error 1064")
            elif tests.MYSQL_VERSION >= (5, 6, 4) and exception.errno != 1426:
                self.fail("ProgrammingError is not Error 1426")
        else:
            self.fail("ProgrammingError not raised")


class BugOra14231160(tests.MySQLConnectorTests):

    """BUG#14231160: lastrowid, description and rowcount read-only"""

    @foreach_cnx()
    def test_readonly_properties(self):
        cur = self.cnx.cursor()
        for attr in ('description', 'rowcount', 'lastrowid'):
            try:
                setattr(cur, attr, 'spam')
            except AttributeError:
                # It's readonly, that's OK
                pass
            else:
                self.fail('Need read-only property: {0}'.format(attr))


class BugOra14259954(tests.MySQLConnectorTests):

    """BUG#14259954: ON DUPLICATE KEY UPDATE VALUE FAILS REGEX"""

    def setUp(self):
        self.tbl = 'Bug14259954'

    def _setup(self):
        cur = self.cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS %s" % (self.tbl))
        create = ("CREATE TABLE %s ( "
                  "`id` int(11) NOT NULL AUTO_INCREMENT, "
                  "`c1` int(11) NOT NULL DEFAULT '0', "
                  "PRIMARY KEY (`id`,`c1`))" % (self.tbl))
        cur.execute(create)

    @foreach_cnx()
    def test_executemany(self):
        self._setup()
        cur = self.cnx.cursor()
        query = ("INSERT INTO %s (id,c1) VALUES (%%s,%%s) "
                 "ON DUPLICATE KEY UPDATE c1=VALUES(c1)") % self.tbl
        try:
            cur.executemany(query, [(1, 1), (2, 2)])
        except errors.ProgrammingError as err:
            self.fail("Regular expression fails with executemany(): %s" %
                      err)


class BugOra14548043(tests.MySQLConnectorTests):

    """BUG#14548043: ERROR MESSAGE SHOULD BE IMPROVED TO DIAGNOSE THE PROBLEM
    """

    @foreach_cnx()
    def test_unix_socket(self):
        config = self.config.copy()
        config['unix_socket'] = os.path.join(
            tempfile.gettempdir(), 'a' * 100 + 'myconnpy_bug14548043.test')

        try:
            cnx = self.cnx.__class__(**config)
        except errors.InterfaceError as exc:
            self.assertEqual(2002, exc.errno)


class BugOra14754894(tests.MySQLConnectorTests):
    """
    """

    def setUp(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        self.tbl = 'BugOra14754894'
        cnx.cmd_query("DROP TABLE IF EXISTS {0}".format(self.tbl))
        cnx.cmd_query("CREATE TABLE {0} (c1 INT)".format(self.tbl))

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cnx.cmd_query("DROP TABLE IF EXISTS %s" % (self.tbl))

    @foreach_cnx()
    def test_executemany(self):
        self.cnx.cmd_query("TRUNCATE TABLE {0}".format(self.tbl))
        cur = self.cnx.cursor()

        insert = "INSERT INTO {0} (c1) VALUES (%(c1)s)".format(self.tbl)
        data = [{'c1': 1}]

        try:
            cur.executemany(insert, [{'c1': 1}])
        except ValueError as err:
            self.fail(err)

        cur.execute("SELECT c1 FROM %s" % self.tbl)
        self.assertEqual(data[0]['c1'], cur.fetchone()[0])

        cur.close()

@unittest.skipIf(not tests.IPV6_AVAILABLE, "IPv6 testing disabled")
class BugOra15876886(tests.MySQLConnectorTests):

    """BUG#15876886: CONNECTOR/PYTHON CAN NOT CONNECT TO MYSQL THROUGH IPV6
    """

    @foreach_cnx()
    def test_ipv6(self):
        config = self.config.copy()
        config['host'] = '::1'
        config['unix_socket'] = None
        try:
            cnx = self.cnx.__class__(**config)
        except errors.InterfaceError as err:
            self.fail("Can not connect using IPv6: {0}".format(str(err)))
        else:
            cnx.close()


class BugOra15915243(tests.MySQLConnectorTests):

    """BUG#15915243: PING COMMAND ALWAYS RECONNECTS TO THE DATABASE
    """

    @foreach_cnx()
    def test_ping(self):
        cid = self.cnx.connection_id
        self.cnx.ping()
        # Do not reconnect
        self.assertEqual(cid, self.cnx.connection_id)
        self.cnx.close()
        # Do not reconnect
        self.assertRaises(errors.InterfaceError, self.cnx.ping)
        # Do reconnect
        self.cnx.ping(reconnect=True)
        self.assertNotEqual(cid, self.cnx.connection_id)
        self.cnx.close()


class BugOra15916486(tests.MySQLConnectorTests):
    """BUG#15916486: RESULTS AFTER STORED PROCEDURE WITH ARGUMENTS ARE NOT KEPT
    """

    def setUp(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()

        cur.execute("DROP PROCEDURE IF EXISTS sp1")
        cur.execute("DROP PROCEDURE IF EXISTS sp2")
        sp1 = ("CREATE PROCEDURE sp1(IN pIn INT, OUT pOut INT)"
               " BEGIN SELECT 1; SET pOut := pIn; SELECT 2; END")
        sp2 = ("CREATE PROCEDURE sp2 ()"
               " BEGIN SELECT 1; SELECT 2; END")

        cur.execute(sp1)
        cur.execute(sp2)
        cnx.close()

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()
        try:
            cur.execute("DROP PROCEDURE IF EXISTS sp1")
            cur.execute("DROP PROCEDURE IF EXISTS sp2")
        except:
            pass  # Clean up fail is acceptable for this test

        cnx.close()

    @foreach_cnx()
    def test_callproc_with_args(self):
        cur = self.cnx.cursor()
        exp = (5, 5)
        self.assertEqual(exp, cur.callproc('sp1', (5, 0)))

        exp = [[(1,)], [(2,)]]
        results = []
        for result in cur.stored_results():
            results.append(result.fetchall())
        self.assertEqual(exp, results)

    @foreach_cnx()
    def test_callproc_without_args(self):
        cur = self.cnx.cursor()
        exp = ()
        self.assertEqual(exp, cur.callproc('sp2'))

        exp = [[(1,)], [(2,)]]
        results = []
        for result in cur.stored_results():
            results.append(result.fetchall())
        self.assertEqual(exp, results)

@unittest.skipIf(os.name == 'nt',
                 "Cannot test error handling when doing handshake on Windows")
class BugOra15836979(tests.MySQLConnectorTests):

    """BUG#15836979: UNCLEAR ERROR MESSAGE CONNECTING USING UNALLOWED IP ADDRESS
    """

    def setUp(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cnx.cmd_query("DROP USER 'root'@'127.0.0.1'")
        try:
            cnx.cmd_query("DROP USER 'root'@'::1'")
        except errors.DatabaseError:
            # Some MySQL servers have no IPv6 entry
            pass
        cnx.close()

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cnx.cmd_query(
            "GRANT ALL PRIVILEGES ON *.* TO 'root'@'127.0.0.1' "
            "WITH GRANT OPTION")
        cnx.cmd_query(
            "GRANT ALL PRIVILEGES ON *.* TO 'root'@'::1' "
            "WITH GRANT OPTION")
        cnx.close()

    @foreach_cnx()
    def test_handshake(self):
        config = self.config.copy()
        config['host'] = '127.0.0.1'
        config['unix_socket'] = None
        try:
            self.cnx.__class__(**config)
        except errors.Error as exc:
            self.assertTrue(
                'Access denied' in str(exc) or 'not allowed' in str(exc),
                'Wrong error message, was: {0}'.format(str(exc)))


class BugOra16217743(tests.MySQLConnectorTests):

    """BUG#16217743: CALLPROC FUNCTION WITH STRING PARAMETERS
    """

    def setUp(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)

        cnx.cmd_query("DROP TABLE IF EXISTS bug16217743")
        cnx.cmd_query("DROP PROCEDURE IF EXISTS sp_bug16217743")
        cnx.cmd_query("CREATE TABLE bug16217743 (c1 VARCHAR(20), c2 INT)")
        cnx.cmd_query(
            "CREATE PROCEDURE sp_bug16217743 (p1 VARCHAR(20), p2 INT) "
            "BEGIN INSERT INTO bug16217743 (c1, c2) "
            "VALUES (p1, p2); END;")

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cnx.cmd_query("DROP TABLE IF EXISTS bug16217743")
        cnx.cmd_query("DROP PROCEDURE IF EXISTS sp_bug16217743")

    @foreach_cnx()
    def test_procedure(self):
        exp = ('ham', 42)
        cur = self.cnx.cursor()
        cur.callproc('sp_bug16217743', ('ham', 42))
        cur.execute("SELECT c1, c2 FROM bug16217743")
        self.assertEqual(exp, cur.fetchone())


@unittest.skipIf(not tests.SSL_AVAILABLE,
                 "BugOra16217667 test failed. Python lacks SSL support.")
class BugOra16217667(tests.MySQLConnectorTests):

    """BUG#16217667: PYTHON CONNECTOR 3.2 SSL CONNECTION FAILS
    """

    def setUp(self):
        config = tests.get_mysql_config()
        self.admin_cnx = connection.MySQLConnection(**config)

        self.admin_cnx.cmd_query(
            "GRANT ALL ON {db}.* TO 'ssluser'@'{host}' REQUIRE X509".format(
                db=config['database'], host=tests.get_mysql_config()['host']))

    def tearDown(self):
        self.admin_cnx.cmd_query("DROP USER 'ssluser'@'{0}'".format(
            tests.get_mysql_config()['host']))

    @foreach_cnx()
    def test_sslauth(self):
        config = self.config.copy()
        config['user'] = 'ssluser'
        config['password'] = ''
        config['unix_socket']= None
        config['ssl_verify_cert'] = True
        config.update({
            'ssl_ca': os.path.abspath(
                os.path.join(tests.SSL_DIR, 'tests_CA_cert.pem')),
            'ssl_cert': os.path.abspath(
                os.path.join(tests.SSL_DIR, 'tests_client_cert.pem')),
            'ssl_key': os.path.abspath(
                os.path.join(tests.SSL_DIR, 'tests_client_key.pem')),
        })

        try:
            self.cnx = self.cnx.__class__(**config)
        except errors.Error as exc:
            self.assertTrue('ssl' in str(exc).lower(), str(exc))

        self.cnx.cmd_query("SHOW STATUS LIKE 'Ssl_cipher'")
        self.assertTrue(self.cnx.get_rows()[0][0] != '')


@unittest.skipIf(not tests.SSL_AVAILABLE,
                 "BugOra16316049 test failed. Python lacks SSL support.")
class BugOra16316049(tests.MySQLConnectorTests):

    """ SSL ERROR: [SSL: TLSV1_ALERT_UNKNOWN_CA] AFTER FIX 6217667"""

    def setUp(self):
        config = tests.get_mysql_config()
        self.host = config['host']
        cnx = connection.MySQLConnection(**config)
        cnx.cmd_query(
            "GRANT ALL ON {db}.* TO 'ssluser'@'{host}' REQUIRE SSL".format(
                db=config['database'], host=self.host))
        cnx.close()

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cnx.cmd_query("DROP USER 'ssluser'@'{host}'".format(host=self.host))
        cnx.close()

    @foreach_cnx()
    def test_ssl(self):
        ssl_ca = os.path.abspath(
            os.path.join(tests.SSL_DIR, 'tests_CA_cert.pem'))
        ssl_cert = os.path.abspath(
            os.path.join(tests.SSL_DIR, 'tests_client_cert.pem'))
        ssl_key = os.path.abspath(
            os.path.join(tests.SSL_DIR, 'tests_client_key.pem'))

        config = self.config.copy()
        config.update({
            'ssl_ca': None,
            'ssl_cert': None,
            'ssl_key': None,
        })

        # Use wrong value for ssl_ca
        config['user'] = 'ssluser'
        config['password'] = ''
        config['unix_socket']= None
        config['ssl_ca'] = os.path.abspath(
            os.path.join(tests.SSL_DIR, 'tests_casdfasdfdsaa_cert.pem'))
        config['ssl_cert'] = ssl_cert
        config['ssl_key'] = ssl_key
        config['ssl_verify_cert'] = True

        # An Exception should be raised
        try:
            self.cnx.__class__(**config)
        except errors.Error as exc:
            exc_str = str(exc).lower()
            self.assertTrue('ssl' in exc_str or 'no such file' in exc_str)

        # Use correct value
        config['ssl_ca'] = ssl_ca
        config['host'] = 'localhost'  # common name must be equal
        try:
            self.cnx = self.cnx.__class__(**config)
        except errors.Error as exc:
            if exc.errno == 1045 and ':' not in self.host:
                # For IPv4
                self.fail("Auth failed:" + str(exc))

        if ':' in self.host:
            # Special case for IPv6
            config['ssl_verify_cert'] = False
            config['host'] = self.host
            try:
                self.cnx = self.cnx.__class__(**config)
            except errors.Error as exc:
                if exc.errno == 1045 and not tests.IPV6_AVAILABLE:
                    self.fail("Auth failed:" + str(exc))

        self.cnx.cmd_query("SHOW STATUS LIKE 'Ssl_cipher'")
        self.assertTrue(self.cnx.get_rows()[0][0] != '')


class BugOra16662920(tests.MySQLConnectorTests):
    """BUG#16662920: FETCHALL() IGNORES NEXT_ROW FOR BUFFERED CURSORS
    """
    def setUp(self):
        self.tbl = 'BugOra16662920'
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()

        cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))
        cur.execute(
            "CREATE TABLE {0} (id INT AUTO_INCREMENT, c1 VARCHAR(20), "
            "PRIMARY KEY (id)) ENGINE=InnoDB".format(self.tbl)
        )

        data = [('a',), ('c',), ('e',), ('d',), ('g',), ('f',)]
        cur.executemany("INSERT INTO {0} (c1) VALUES (%s)".format(self.tbl),
                        data)
        cur.close()
        cnx.commit()

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))
        cnx.close()

    @foreach_cnx()
    def test_buffered(self):
        cur = self.cnx.cursor(buffered=True)
        cur.execute("SELECT * FROM {0} ORDER BY c1".format(self.tbl))
        self.assertEqual((1, 'a'), cur.fetchone())
        exp = [(2, 'c'), (4, 'd'), (3, 'e')]
        self.assertEqual(exp, cur.fetchmany(3))
        exp = [(6, 'f'), (5, 'g')]
        self.assertEqual(exp, cur.fetchall())
        cur.close()

    @foreach_cnx()
    def test_buffered_raw(self):
        cur = self.cnx.cursor(buffered=True, raw=True)
        cur.execute("SELECT * FROM {0} ORDER BY c1".format(self.tbl))
        exp_one = (b'1', b'a')
        exp_many = [(b'2', b'c'), (b'4', b'd'), (b'3', b'e')]
        exp_all = [(b'6', b'f'), (b'5', b'g')]

        self.assertEqual(exp_one, cur.fetchone())
        self.assertEqual(exp_many, cur.fetchmany(3))
        self.assertEqual(exp_all, cur.fetchall())
        cur.close()


class BugOra17041412(tests.MySQLConnectorTests):
    """BUG#17041412: FETCHALL() DOES NOT RETURN SELF._NEXTROW IF AVAILABLE
    """

    def setUp(self):
        self.table_name = 'BugOra17041412'
        self.data = [(1,), (2,), (3,)]
        self.data_raw = [(b'1',), (b'2',), (b'3',)]

    def _setup(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS %s" % self.table_name)
        cur.execute("CREATE TABLE %s (c1 INT)" % self.table_name)
        cur.executemany(
            "INSERT INTO %s (c1) VALUES (%%s)" % self.table_name,
            self.data)
        cnx.commit()

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS %s" % self.table_name)

    @foreach_cnx()
    def test_one_all(self):
        self._setup()
        cur = self.cnx.cursor()
        cur.execute("SELECT * FROM %s ORDER BY c1" % self.table_name)
        self.assertEqual(self.data[0], cur.fetchone())
        self.assertEqual(1, cur.rowcount)
        self.assertEqual(self.data[1:], cur.fetchall())
        self.assertEqual(3, cur.rowcount)

    @foreach_cnx()
    def test_many_all(self):
        self._setup()
        cur = self.cnx.cursor()
        cur.execute("SELECT * FROM %s ORDER BY c1" % self.table_name)
        self.assertEqual(self.data[0:2], cur.fetchmany(2))
        self.assertEqual(2, cur.rowcount)
        self.assertEqual(self.data[2:], cur.fetchall())
        self.assertEqual(3, cur.rowcount)

    @foreach_cnx()
    def test_many(self):
        self._setup()
        cur = self.cnx.cursor()
        cur.execute("SELECT * FROM %s ORDER BY c1" % self.table_name)
        self.assertEqual(self.data, cur.fetchall())
        self.assertEqual(3, cur.rowcount)

        cur.execute("SELECT * FROM %s WHERE c1 > %%s" % self.table_name,
                    (self.data[-1][0] + 100,))
        self.assertEqual([], cur.fetchall())

    @foreach_cnx()
    def test_raw_one_all(self):
        self._setup()
        cur = self.cnx.cursor(raw=True)
        cur.execute("SELECT * FROM %s ORDER BY c1" % self.table_name)
        self.assertEqual(self.data_raw[0], cur.fetchone())
        self.assertEqual(1, cur.rowcount)
        self.assertEqual(self.data_raw[1:], cur.fetchall())
        self.assertEqual(3, cur.rowcount)

    @foreach_cnx()
    def test_raw_many_all(self):
        self._setup()
        cur = self.cnx.cursor(raw=True)
        cur.execute("SELECT * FROM %s ORDER BY c1" % self.table_name)
        self.assertEqual(self.data_raw[0:2], cur.fetchmany(2))
        self.assertEqual(2, cur.rowcount)
        self.assertEqual(self.data_raw[2:], cur.fetchall())
        self.assertEqual(3, cur.rowcount)

    @foreach_cnx()
    def test_raw_many(self):
        self._setup()
        cur = self.cnx.cursor(raw=True)
        cur.execute("SELECT * FROM %s ORDER BY c1" % self.table_name)
        self.assertEqual(self.data_raw, cur.fetchall())
        self.assertEqual(3, cur.rowcount)

        cur.execute("SELECT * FROM %s WHERE c1 > 1000" % self.table_name)
        self.assertEqual([], cur.fetchall())


class BugOra16819486(tests.MySQLConnectorTests):
    """BUG#16819486: ERROR 1210 TO BE HANDLED
    """

    def setUp(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS BugOra16819486")
        cur.execute("CREATE TABLE BugOra16819486 (c1 INT, c2 INT)")
        cur.executemany("INSERT INTO BugOra16819486 VALUES (%s, %s)",
                             [(1, 10), (2, 20), (3, 30)])
        cnx.commit()

    def tearDown(self):
        cnx = connection.MySQLConnection(**tests.get_mysql_config())
        cur = cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS BugOra16819486")
        cnx.close()

    @foreach_cnx(connection.MySQLConnection)
    def test_error1210(self):
        cur = self.cnx.cursor(prepared=True)
        prep_stmt = "SELECT * FROM BugOra16819486 WHERE c1 = %s AND c2 = %s"
        self.assertRaises(mysql.connector.ProgrammingError,
                          cur.execute, prep_stmt, (1,))

        prep_stmt = "SELECT * FROM BugOra16819486 WHERE c1 = %s AND c2 = %s"
        exp = [(1, 10)]
        cur.execute(prep_stmt, (1, 10))
        self.assertEqual(exp, cur.fetchall())

    @unittest.skipIf(not CMySQLConnection, ERR_NO_CEXT)
    @foreach_cnx(CMySQLConnection)
    def test_prepared_argument(self):
        self.assertRaises(NotImplementedError, self.cnx.cursor, prepared=True)


class BugOra16656621(tests.MySQLConnectorTests):
    """BUG#16656621: IMPOSSIBLE TO ROLLBACK WITH UNREAD RESULTS
    """

    def setUp(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()

        cur.execute("DROP TABLE IF EXISTS BugOra16656621")
        cur.execute(
            "CREATE TABLE BugOra16656621 "
            "(id INT AUTO_INCREMENT, c1 VARCHAR(20), "
            "PRIMARY KEY (id)) ENGINE=InnoDB")

    def tearDown(self):
        cnx = connection.MySQLConnection(**tests.get_mysql_config())
        cur = cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS BugOra16656621")

    @foreach_cnx()
    def test_rollback(self):
        cur = self.cnx.cursor()
        cur.execute(
            "INSERT INTO BugOra16656621 (c1) VALUES ('a'),('b'),('c')")
        self.cnx.commit()

        cur.execute("SELECT * FROM BugOra16656621")
        try:
            self.cnx.rollback()
        except mysql.connector.InternalError:
            self.fail("Rollback not possible with unread results")


class BugOra16660356(tests.MySQLConnectorTests):
    """BUG#16660356: USING EXECUTEMANY WITH EMPTY DATA SHOULD DO NOTHING
    """

    def setUp(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()

        cur.execute("DROP TABLE IF EXISTS bug16660356")
        cur.execute(
            "CREATE TABLE bug16660356 (id INT AUTO_INCREMENT, c1 VARCHAR(20), "
            "PRIMARY KEY (id)) ENGINE=InnoDB"
        )
        cnx.close()

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cnx.cmd_query("DROP TABLE IF EXISTS bug16660356")
        cnx.close()

    @foreach_cnx()
    def test_executemany(self):
        cur = self.cnx.cursor()
        try:
            cur.executemany(
                "INSERT INTO bug16660356 (c1) VALUES (%s)", []
            )
        except mysql.connector.ProgrammingError:
            self.fail("executemany raise ProgrammingError with empty data")


class BugOra17041240(tests.MySQLConnectorTests):
    """BUG#17041240: UNCLEAR ERROR CLOSING CURSOR WITH UNREAD RESULTS
    """

    def setUp(self):
        self.table_name = 'BugOra17041240'
        self.data = [(1,), (2,), (3,)]

    def _setup(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS {table}".format(
            table=self.table_name))
        cur.execute("CREATE TABLE {table} (c1 INT)".format(
            table=self.table_name))
        cur.executemany(
            "INSERT INTO {table} (c1) VALUES (%s)".format(
                table=self.table_name),
            self.data)
        cnx.commit()
        cnx.close()

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS {table}".format(
            table=self.table_name))
        cnx.close()

    @foreach_cnx()
    def test_cursor_close(self):
        self._setup()
        cur = self.cnx.cursor()
        cur.execute("SELECT * FROM {table} ORDER BY c1".format(
            table=self.table_name))
        self.assertEqual(self.data[0], cur.fetchone())
        self.assertEqual(self.data[1], cur.fetchone())
        self.assertRaises(mysql.connector.InternalError, cur.close)
        self.assertEqual(self.data[2], cur.fetchone())

    @foreach_cnx()
    def test_cursor_new(self):
        self._setup()
        cur = self.cnx.cursor()
        cur.execute("SELECT * FROM {table} ORDER BY c1".format(
            table=self.table_name))
        self.assertEqual(self.data[0], cur.fetchone())
        self.assertEqual(self.data[1], cur.fetchone())
        self.assertRaises(mysql.connector.InternalError, self.cnx.cursor)
        self.assertEqual(self.data[2], cur.fetchone())


class BugOra17065366(tests.MySQLConnectorTests):
    """BUG#17065366: EXECUTEMANY FAILS USING MYSQL FUNCTION FOR INSERTS
    """

    def _setup(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()
        self.table_name = 'BugOra17065366'
        cur.execute(
            "DROP TABLE IF EXISTS {table}".format(table=self.table_name))
        cur.execute(
            "CREATE TABLE {table} ( "
            "id INT UNSIGNED NOT NULL AUTO_INCREMENT KEY, "
            "c1 INT, c2 DATETIME) ENGINE=INNODB".format(table=self.table_name))
        cnx.close()

    def tearDown(self):
        cnx = connection.MySQLConnection(**tests.get_mysql_config())
        cur = cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS {table}".format(
            table=self.table_name))
        cnx.close()

    @foreach_cnx()
    def test_executemany(self):
        self._setup()
        cur = self.cnx.cursor()


        adate = datetime(2012, 9, 30)
        stmt = (
            "INSERT INTO {table} (id, c1, c2) "
            "VALUES (%s, %s, DATE('{date} 13:07:00'))"
            "/* Using DATE() */ ON DUPLICATE KEY UPDATE c1 = id"
        ).format(table=self.table_name, date=adate.strftime('%Y-%m-%d'))

        exp = [
            (1, 0, datetime(2012, 9, 30, 0, 0)),
            (2, 0, datetime(2012, 9, 30, 0, 0))
        ]
        cur.executemany(stmt, [(None, 0), (None, 0)])
        self.cnx.commit()
        cur.execute("SELECT * FROM {table}".format(table=self.table_name))
        rows = cur.fetchall()
        self.assertEqual(exp, rows)

        exp = [
            (1, 1, datetime(2012, 9, 30, 0, 0)),
            (2, 2, datetime(2012, 9, 30, 0, 0))
        ]
        cur.executemany(stmt, [(1, 1), (2, 2)])
        self.cnx.commit()
        cur.execute("SELECT * FROM {table}".format(table=self.table_name))
        rows = cur.fetchall()
        self.assertEqual(exp, rows)


class BugOra16933795(tests.MySQLConnectorTests):
    """BUG#16933795: ERROR.MSG ATTRIBUTE DOES NOT CONTAIN CORRECT VALUE
    """

    def test_error(self):
        exp = "Some error message"
        error = mysql.connector.Error(msg=exp, errno=-1024)
        self.assertEqual(exp, error.msg)

        exp = "Unknown MySQL error"
        error = mysql.connector.Error(errno=2000)
        self.assertEqual(exp, error.msg)
        self.assertEqual("2000: " + exp, str(error))


class BugOra17022399(tests.MySQLConnectorTests):
    """BUG#17022399: EXECUTING AFTER CONNECTION CLOSED GIVES UNCLEAR ERROR
    """

    @foreach_cnx()
    def test_execute(self):
        cur = self.cnx.cursor()
        self.cnx.close()
        try:
            cur.execute("SELECT 1")
        except mysql.connector.OperationalError as exc:
            self.assertEqual(2055, exc.errno, 'Was: ' + str(exc))

    @cnx_config(client_flags=[constants.ClientFlag.COMPRESS])
    @foreach_cnx()
    def test_execute_compressed(self):
        cur = self.cnx.cursor()
        self.cnx.close()
        try:
            cur.execute("SELECT 1")
        except mysql.connector.OperationalError as exc:
            self.assertEqual(2055, exc.errno, 'Was: ' + str(exc))


class BugOra16369511(tests.MySQLConnectorTests):
    """BUG#16369511: LOAD DATA LOCAL INFILE IS MISSING
    """

    def setUp(self):
        self.data_file = os.path.join('tests', 'data', 'local_data.csv')

    def _setup(self):
        cnx = connection.MySQLConnection(**tests.get_mysql_config())
        self.cnx.cmd_query("DROP TABLE IF EXISTS local_data")
        self.cnx.cmd_query(
            "CREATE TABLE local_data (id int, c1 VARCHAR(6), c2 VARCHAR(6))")
        cnx.close()

    def tearDown(self):
        cnx = connection.MySQLConnection(**tests.get_mysql_config())
        cnx.cmd_query("DROP TABLE IF EXISTS local_data")
        cnx.close()

    @foreach_cnx()
    def test_load_csv(self):
        self._setup()
        cur = self.cnx.cursor()
        sql = "LOAD DATA LOCAL INFILE %s INTO TABLE local_data"
        cur.execute(sql, (self.data_file,))
        cur.execute("SELECT * FROM local_data")

        exp = [
            (1, 'c1_1', 'c2_1'), (2, 'c1_2', 'c2_2'),
            (3, 'c1_3', 'c2_3'), (4, 'c1_4', 'c2_4'),
            (5, 'c1_5', 'c2_5'), (6, 'c1_6', 'c2_6')]
        self.assertEqual(exp, cur.fetchall())

    @foreach_cnx()
    def test_filenotfound(self):
        self._setup()
        cur = self.cnx.cursor()
        sql = "LOAD DATA LOCAL INFILE %s INTO TABLE local_data"
        try:
            cur.execute(sql, (self.data_file + '_spam',))
        except (errors.InterfaceError, errors.DatabaseError) as exc:
            self.assertTrue(
                'not found' in str(exc) or 'could not be read' in str(exc),
                'Was: ' + str(exc))


class BugOra17002411(tests.MySQLConnectorTests):
    """BUG#17002411: LOAD DATA LOCAL INFILE FAILS WITH BIGGER FILES
    """

    def setUp(self):
        self.data_file = os.path.join('tests', 'data', 'local_data_big.csv')
        self.exp_rows = 33000

        with open(self.data_file, 'w') as fp:
            i = 0
            while i < self.exp_rows:
                fp.write("{0}\t{1}\n".format('a' * 255, 'b' * 255))
                i += 1

    def _setup(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = self.cnx.cursor()

        cur.execute("DROP TABLE IF EXISTS local_data")
        cur.execute(
            "CREATE TABLE local_data ("
            "id INT AUTO_INCREMENT KEY, "
            "c1 VARCHAR(255), c2 VARCHAR(255))"
        )

    def tearDown(self):
        cnx = connection.MySQLConnection(**tests.get_mysql_config())
        cnx.cmd_query("DROP TABLE IF EXISTS local_data")
        os.unlink(self.data_file)
        cnx.close()

    @foreach_cnx()
    def test_load_csv(self):
        self._setup()
        cur = self.cnx.cursor()
        sql = "LOAD DATA LOCAL INFILE %s INTO TABLE local_data (c1, c2)"
        cur.execute(sql, (self.data_file,))
        cur.execute("SELECT COUNT(*) FROM local_data")
        self.assertEqual(self.exp_rows, cur.fetchone()[0])


class BugOra17422299(tests.MySQLConnectorTests):
    """BUG#17422299: cmd_shutdown fails with malformed connection packet
    """

    def setUp(self):
        self.config = tests.get_mysql_config()
        self.mysql_server = tests.MYSQL_SERVERS[0]

    def tearDown(self):
        self.ensure_up()

    def ensure_up(self):
        # Start the MySQL server again
        if not self.mysql_server.check_running():
            self.mysql_server.start()

            if not self.mysql_server.wait_up():
                self.fail("Failed restarting MySQL server after test")

    def test_shutdown(self):
        for cnx_class in self.all_cnx_classes:
            self.ensure_up()
            cnx = cnx_class(**self.config)
            try:
                cnx.cmd_shutdown()
            except mysql.connector.DatabaseError as err:
                self.fail("COM_SHUTDOWN failed: {0}".format(err))

            if not self.mysql_server.wait_down():
                self.fail("MySQL not shut down after COM_SHUTDOWN")

    def test_shutdown__with_type(self):
        for cnx_class in self.all_cnx_classes:
            self.ensure_up()
            cnx = cnx_class(**self.config)
            try:
                cnx.cmd_shutdown(
                    constants.ShutdownType.SHUTDOWN_WAIT_ALL_BUFFERS)
            except mysql.connector.DatabaseError as err:
                self.fail("COM_SHUTDOWN failed: {0}".format(err))

            if not self.mysql_server.wait_down():
                self.fail("MySQL not shut down after COM_SHUTDOWN")


class BugOra17215197(tests.MySQLConnectorTests):
    """BUG#17215197: MYSQLCONNECTION.CURSOR(PREPARED=TRUE) NOT POSSIBLE
    """

    def _setup(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS BugOra17215197")
        cur.execute("CREATE TABLE BugOra17215197 (c1 INT, c2 INT)")
        cur.executemany("INSERT INTO BugOra17215197 VALUES (%s, %s)",
                             [(1, 10), (2, 20), (3, 30)])
        cnx.commit()

    def tearDown(self):
        cnx = connection.MySQLConnection(**tests.get_mysql_config())
        cnx.cmd_query("DROP TABLE IF EXISTS BugOra17215197")

    @foreach_cnx(connection.MySQLConnection)
    def test_prepared_argument(self):
        self._setup()
        cur = self.cnx.cursor(prepared=True)
        prep_stmt = "SELECT * FROM BugOra17215197 WHERE c1 = %s AND c2 = %s"
        exp = [(1, 10)]
        cur.execute(prep_stmt, (1, 10))
        self.assertEqual(exp, cur.fetchall())

    @unittest.skipIf(not CMySQLConnection, ERR_NO_CEXT)
    @foreach_cnx(CMySQLConnection)
    def test_prepared_argument(self):
        self.assertRaises(NotImplementedError, self.cnx.cursor, prepared=True)


class BugOra17414258(tests.MySQLConnectorTests):
    """BUG#17414258: IT IS ALLOWED TO CHANGE SIZE OF ACTIVE POOL
    """

    def setUp(self):
        self.config = tests.get_mysql_config()
        self.config['pool_name'] = 'test'
        self.config['pool_size'] = 3

    def tearDown(self):
        # Remove pools created by test
        del mysql.connector._CONNECTION_POOLS[self.config['pool_name']]

    def test_poolsize(self):
        cnx = mysql.connector.connect(**self.config)
        cnx.close()

        newconfig = self.config.copy()
        newconfig['pool_size'] = self.config['pool_size'] + 1
        self.assertRaises(mysql.connector.PoolError,
                          mysql.connector.connect, **newconfig)


class Bug17578937(tests.MySQLConnectorTests):
    """CONNECTION POOL DOES NOT HANDLE A NOT AVAILABLE MYSQL SERVER"""

    def setUp(self):
        self.mysql_server = tests.MYSQL_SERVERS[0]

    def tearDown(self):
        # Start the MySQL server again
        if not self.mysql_server.check_running():
            self.mysql_server.start()

            if not self.mysql_server.wait_up():
                self.fail("Failed restarting MySQL server after test")

    def test_get_connection(self):
        """Test reconnect once MySQL server is back

        To make the test case simpler, we create a pool which only has
        one connection in the queue. This way we can similuate getting a
        connection from a pool for which the MySQL server is not running.
        """
        config = tests.get_mysql_config().copy()
        config['connection_timeout'] = 2
        cnxpool = pooling.MySQLConnectionPool(
            pool_name='test', pool_size=1, **config)

        pcnx = cnxpool.get_connection()
        self.assertTrue(isinstance(pcnx, pooling.PooledMySQLConnection))
        pcnx.close()
        self.mysql_server.stop()
        if not self.mysql_server.wait_down():
            self.fail("MySQL not shut down; can not continue test")
        self.assertRaises(errors.InterfaceError, cnxpool.get_connection)

        self.mysql_server.start()
        if not self.mysql_server.wait_up():
            self.fail("MySQL started; can not continue test")
        pcnx = cnxpool.get_connection()
        pcnx.close()


class BugOra17079344(tests.MySQLConnectorTests):
    """BUG#17079344: ERROR WITH GBK STRING WITH CHARACTERS ENCODED AS BACKSLASH
    """

    def setUp(self):
        cnx = connection.MySQLConnection(**tests.get_mysql_config())
        cur = cnx.cursor()
        for charset in ('gbk', 'sjis', 'big5'):
            tablename = charset + 'test'
            cur.execute("DROP TABLE IF EXISTS {0}".format(tablename))
            table = (
                "CREATE TABLE {table} ("
                "id INT AUTO_INCREMENT KEY, "
                "c1 VARCHAR(40)"
                ") CHARACTER SET '{charset}'"
            ).format(table=tablename, charset=charset)
            cur.execute(table)
            cnx.commit()
        cur.close()
        cnx.close()

    def tearDown(self):
        cnx = connection.MySQLConnection(**tests.get_mysql_config())
        for charset in ('gbk', 'sjis', 'big5'):
            tablename = charset + 'test'
            cnx.cmd_query("DROP TABLE IF EXISTS {0}".format(tablename))
        cnx.close()

    def _test_charset(self, charset, data):
        config = tests.get_mysql_config()
        config['charset'] = charset
        config['use_unicode'] = True
        self.cnx = self.cnx.__class__(**config)
        tablename = charset + 'test'
        cur = self.cnx.cursor()
        cur.execute("TRUNCATE {0}".format(tablename))
        self.cnx.commit()

        insert = "INSERT INTO {0} (c1) VALUES (%s)".format(tablename)
        for value in data:
            cur.execute(insert, (value,))
        self.cnx.commit()

        cur.execute("SELECT id, c1 FROM {0} ORDER BY id".format(tablename))
        for row in cur:
            self.assertEqual(data[row[0] - 1], row[1])

        cur.close()
        self.cnx.close()

    @foreach_cnx()
    def test_gbk(self):
        self._test_charset('gbk', [u'赵孟頫', u'赵\孟\頫\\', u'遜', ])

    @foreach_cnx()
    def test_sjis(self):
        self._test_charset('sjis', ['\u005c'])

    @foreach_cnx()
    def test_big5(self):
        self._test_charset('big5', ['\u5C62'])


class BugOra17780576(tests.MySQLConnectorTests):
    """BUG#17780576: CHARACTER SET 'UTF8MB4' UNSUPPORTED
    """

    def tearDown(self):
        cnx = connection.MySQLConnection(**tests.get_mysql_config())
        cur = cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS utf8mb4test")
        cur.close()
        cnx.close()

    @foreach_cnx()
    def test_utf8mb4(self):
        if tests.MYSQL_VERSION < (5, 5, 0):
            # Test only valid for MySQL 5.5.0 and later.
            return

        config = tests.get_mysql_config()
        tablename = 'utf8mb4test'
        self.cnx.set_charset_collation('utf8mb4')
        cur = self.cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS {0}".format(tablename))

        table = (
            "CREATE TABLE {table} ("
            "id INT AUTO_INCREMENT KEY, "
            "c1 VARCHAR(40) CHARACTER SET 'utf8mb4'"
            ") CHARACTER SET 'utf8mb4'"
        ).format(table=tablename)
        cur.execute(table)

        insert = "INSERT INTO {0} (c1) VALUES (%s)".format(tablename)
        data = [u'😉😍', u'😃😊', u'😄😘😚', ]
        for value in data:
            cur.execute(insert, (value,))

        cur.execute("SELECT id, c1 FROM {0} ORDER BY id".format(tablename))
        for row in cur:
            self.assertEqual(data[row[0] - 1], row[1])

        cur.close()
        self.cnx.close()


class BugOra17573172(tests.MySQLConnectorTests):
    """BUG#17573172: MISSING SUPPORT FOR READ-ONLY TRANSACTIONS
    """

    def setUp(self):
        config = tests.get_mysql_config()
        self.cnx = connection.MySQLConnection(**config)
        self.cur = self.cnx.cursor()
        self.cur.execute("DROP TABLE IF EXISTS BugOra17573172")
        self.cur.execute("CREATE TABLE BugOra17573172 (c1 INT, c2 INT)")
        self.cur.executemany("INSERT INTO BugOra17573172 VALUES (%s, %s)",
                             [(1, 10), (2, 20), (3, 30)])
        self.cnx.commit()

    def test_read_only(self):
        if self.cnx.get_server_version() < (5, 6, 5):
            self.assertRaises(ValueError, self.cnx.start_transaction,
                              readonly=True)
        else:
            self.cnx.start_transaction(readonly=True)
            self.assertTrue(self.cnx.in_transaction)
            self.assertRaises(errors.ProgrammingError,
                              self.cnx.start_transaction)

            query = "INSERT INTO BugOra17573172 VALUES(4, 40)"
            self.assertRaises(errors.ProgrammingError, self.cur.execute, query)
            self.cnx.rollback()

    def tearDown(self):
        self.cur.execute("DROP TABLE IF EXISTS BugOra17573172")
        self.cur.close()


class BugOra17826833(tests.MySQLConnectorTests):
    """BUG#17826833: EXECUTEMANY() FOR INSERTS W/O VALUES
    """

    def setUp(self):
        config = tests.get_mysql_config()
        self.cnx = connection.MySQLConnection(**config)
        self.cursor = self.cnx.cursor()

        self.emp_tbl = 'Bug17826833_emp'
        self.cursor.execute("DROP TABLE IF EXISTS %s" % (self.emp_tbl))

        self.city_tbl = 'Bug17826833_city'
        self.cursor.execute("DROP TABLE IF EXISTS %s" % (self.city_tbl))

        create = ("CREATE TABLE %s ( "
                  "`id` int(11) NOT NULL, "
                  "`name` varchar(20) NOT NULL , "
                  "`phone` varchar(20), "
                  "PRIMARY KEY (`id`))" % (self.emp_tbl))
        self.cursor.execute(create)

        create = ("CREATE TABLE %s ( "
                  "`id` int(11) NOT NULL, "
                  "`name` varchar(20) NOT NULL, "
                  "PRIMARY KEY (`id`))" % (self.city_tbl))
        self.cursor.execute(create)

    def test_executemany(self):
        stmt = "INSERT INTO {0} (id,name) VALUES (%s,%s)".format(
            self.city_tbl)
        self.cursor.executemany(stmt, [(1, 'ABC'), (2, 'CDE'), (3, 'XYZ')])

        query = ("INSERT INTO %s (id, name, phone)"
                 "SELECT id,name,%%s FROM %s WHERE name=%%s") % (self.emp_tbl,
                                                                 self.city_tbl)
        try:
            self.cursor.executemany(query, [('4567', 'CDE'), ('1234', 'XYZ')])
            stmt = "SELECT * FROM {0}".format(self.emp_tbl)
            self.cursor.execute(stmt)
            self.assertEqual([(2, 'CDE', '4567'), (3, 'XYZ', '1234')],
                             self.cursor.fetchall(), "INSERT ... SELECT failed")
        except errors.ProgrammingError as err:
            self.fail("Regular expression fails with executemany(): %s" %
                      err)


class BugOra18040042(tests.MySQLConnectorTests):
    """BUG#18040042: Reset session closing pooled Connection"""

    def test_clear_session(self):
        cnxpool = pooling.MySQLConnectionPool(
            pool_name='test', pool_size=1, **tests.get_mysql_config())

        pcnx = cnxpool.get_connection()
        exp_session_id = pcnx.connection_id
        pcnx.cmd_query("SET @ham = 2")
        pcnx.close()

        pcnx = cnxpool.get_connection()
        pcnx.cmd_query("SELECT @ham")
        self.assertEqual(exp_session_id, pcnx.connection_id)
        self.assertNotEqual(('2',), pcnx.get_rows()[0][0])

    def test_do_not_clear_session(self):
        cnxpool = pooling.MySQLConnectionPool(
            pool_name='test', pool_size=1, pool_reset_session=False,
            **tests.get_mysql_config())

        pcnx = cnxpool.get_connection()
        exp_session_id = pcnx.connection_id
        pcnx.cmd_query("SET @ham = 2")
        pcnx.close()

        pcnx = cnxpool.get_connection()
        pcnx.cmd_query("SELECT @ham")
        self.assertEqual(exp_session_id, pcnx.connection_id)
        if PY2:
            self.assertEqual(('2',), pcnx.get_rows()[0][0])
        else:
            self.assertEqual((b'2',), pcnx.get_rows()[0][0])


class BugOra17965619(tests.MySQLConnectorTests):
    """BUG#17965619: CALLPROC FUNCTION WITH BYTES PARAMETERS
    """

    def setUp(self):
        self.cnx = connection.MySQLConnection(**tests.get_mysql_config())
        procedure = ("DROP PROCEDURE IF EXISTS `proce_with_binary`")
        self.cnx.cmd_query(procedure)

        procedure = ("CREATE PROCEDURE `proce_with_binary` "
                     "(data VARBINARY(512)) BEGIN END;")
        self.cnx.cmd_query(procedure)

    def tearDown(self):
        procedure = ("DROP PROCEDURE IF EXISTS `proce_with_binary`")
        self.cnx.cmd_query(procedure)
        self.cnx.close()

    def test_callproc(self):
        cur = self.cnx.cursor()

        data = b'\xf0\xf1\xf2'
        output = cur.callproc('proce_with_binary', ((data, 'BINARY'),))
        self.assertEqual((data,), output)

        cur.close()


class BugOra17054848(tests.MySQLConnectorTests):
    """BUG#17054848: USE OF SSL SHOULD NOT REQUIRE SSL_CERT AND SSL_KEY
    """

    def setUp(self):
        config = tests.get_mysql_config()
        self.admin_cnx = connection.MySQLConnection(**config)

        self.admin_cnx.cmd_query(
            "GRANT ALL ON %s.* TO 'ssluser'@'%s' REQUIRE SSL" % (
                config['database'], config['host']))

    def tearDown(self):
        config = tests.get_mysql_config()
        self.admin_cnx.cmd_query("DROP USER 'ssluser'@'%s'" % (
            config['host']))

    def test_ssl(self):
        if not tests.SSL_AVAILABLE:
            tests.MESSAGES['WARNINGS'].append(
                "BugOra16217667 test failed. Python lacks SSL support.")
            return

        ssl_ca = os.path.abspath(
            os.path.join(tests.SSL_DIR, 'tests_CA_cert.pem'))
        ssl_key = os.path.abspath(
            os.path.join(tests.SSL_DIR, 'tests_client_key.pem'))

        config = tests.get_mysql_config()
        config['user'] = 'ssluser'
        config['password'] = ''
        config['unix_socket'] = None
        config['ssl_verify_cert'] = False
        config.update({
            'ssl_ca': ssl_ca,
        })

        try:
            cnx = connection.MySQLConnection(**config)
        except errors.ProgrammingError:
            self.fail("Failed authentication with SSL")

        cnx.cmd_query("SHOW STATUS LIKE 'Ssl_cipher'")
        res = cnx.get_rows()[0][0]
        self.assertTrue(res != '')


@unittest.skipIf(tests.MYSQL_VERSION < (5, 5, 7),
                 "BugOra16217765 not tested with MySQL version < 5.5.7")
class BugOra16217765(tests.MySQLConnectorTests):
    """BUG#16217765: Fix authentication plugin support
    """

    users = {
        'sha256user': {
            'username': 'sha256user',
            'password': 'sha256P@ss',
            'auth_plugin': 'sha256_password',
        },
        'nativeuser': {
            'username': 'nativeuser',
            'password': 'nativeP@ss',
            'auth_plugin': 'mysql_native_password',
        },
        'sha256user_np': {
            'username': 'sha256user_np',
            'password': '',
            'auth_plugin': 'sha256_password',
        },
        'nativeuser_np': {
            'username': 'nativeuser_np',
            'password': '',
            'auth_plugin': 'mysql_native_password',
        },
    }

    def _create_user(self, cnx, user, password, host, database,
                     plugin):

        self._drop_user(cnx, user, host)
        create_user = ("CREATE USER '{user}'@'{host}' "
                       "IDENTIFIED WITH {plugin}")
        cnx.cmd_query(create_user.format(user=user, host=host, plugin=plugin))

        if plugin == 'sha256_password':
            cnx.cmd_query("SET old_passwords = 2")
        else:
            cnx.cmd_query("SET old_passwords = 0")

        if tests.MYSQL_VERSION < (5, 7, 5):
            passwd = ("SET PASSWORD FOR '{user}'@'{host}' = "
                      "PASSWORD('{password}')").format(user=user, host=host,
                                                       password=password)
        else:
            passwd = ("ALTER USER '{user}'@'{host}' IDENTIFIED BY "
                      "'{password}'").format(user=user, host=host,
                                             password=password)
        cnx.cmd_query(passwd)

        grant = "GRANT ALL ON {database}.* TO '{user}'@'{host}'"
        cnx.cmd_query(grant.format(database=database, user=user, host=host))

    def _drop_user(self, cnx, user, host):
        try:
            self.admin_cnx.cmd_query("DROP USER '{user}'@'{host}'".format(
                host=host,
                user=user))
        except errors.DatabaseError:
            # It's OK when drop fails
            pass

    def setUp(self):
        config = tests.get_mysql_config()
        self.host = config['host']
        self.admin_cnx = connection.MySQLConnection(**config)

        for key, user in self.users.items():
            self._create_user(self.admin_cnx, user['username'],
                              user['password'],
                              self.host,
                              config['database'],
                              plugin=user['auth_plugin'])

    def tearDown(self):
        for key, user in self.users.items():
            self._drop_user(self.admin_cnx, user['username'], self.host)

    @unittest.skipIf(tests.MYSQL_VERSION < (5, 6, 6),
                     "MySQL {0} does not support sha256_password auth".format(
                         tests.MYSQL_VERSION_TXT))
    @unittest.skipIf(
        not tests.SSL_AVAILABLE,
        "BugOra16217765.test_sha256 test skipped: SSL support not available")
    def test_sha256(self):
        config = tests.get_mysql_config()
        config['unix_socket'] = None
        config.update({
            'ssl_ca': tests.SSL_CA,
            'ssl_cert': tests.SSL_CERT,
            'ssl_key': tests.SSL_KEY,
        })

        user = self.users['sha256user']
        config['user'] = user['username']
        config['password'] = user['password']
        config['client_flags'] = [constants.ClientFlag.PLUGIN_AUTH]
        config['auth_plugin'] = user['auth_plugin']

        try:
            cnx = connection.MySQLConnection(**config)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            self.fail(self.errmsg.format(config['auth_plugin'], exc))

        try:
            cnx.cmd_change_user(config['user'], config['password'])
        except:
            self.fail("Changing user using sha256_password auth failed "
                      "with pure Python connector")

        if CMySQLConnection:
            try:
                cnx = CMySQLConnection(**config)
            except Exception as exc:
                import traceback
                traceback.print_exc()
                self.fail(self.errmsg.format(config['auth_plugin'], exc))
            try:
                cnx.cmd_change_user(config['user'], config['password'])
            except:
                self.fail("Changing user using sha256_password auth failed "
                          "with CExtension")

    @unittest.skipIf(tests.MYSQL_VERSION < (5, 6, 6),
                     "MySQL {0} does not support sha256_password auth".format(
                         tests.MYSQL_VERSION_TXT))
    def test_sha256_nonssl(self):
        config = tests.get_mysql_config()
        config['unix_socket'] = None
        config['client_flags'] = [constants.ClientFlag.PLUGIN_AUTH]

        user = self.users['sha256user']
        config['user'] = user['username']
        config['password'] = user['password']
        self.assertRaises(errors.InterfaceError, connection.MySQLConnection,
                          **config)
        if CMySQLConnection:
            self.assertRaises(errors.InterfaceError, CMySQLConnection, **config)

    @unittest.skipIf(tests.MYSQL_VERSION < (5, 5, 7),
                     "MySQL {0} does not support authentication plugins".format(
                         tests.MYSQL_VERSION_TXT))
    def test_native(self):
        config = tests.get_mysql_config()
        config['unix_socket'] = None

        user = self.users['nativeuser']
        config['user'] = user['username']
        config['password'] = user['password']
        config['client_flags'] = [constants.ClientFlag.PLUGIN_AUTH]
        config['auth_plugin'] = user['auth_plugin']
        try:
            cnx = connection.MySQLConnection(**config)
        except Exception as exc:
                self.fail(self.errmsg.format(config['auth_plugin'], exc))

        if CMySQLConnection:
            try:
                cnx = CMySQLConnection(**config)
            except Exception as exc:
                self.fail(self.errmsg.format(config['auth_plugin'], exc))


class BugOra18144971(tests.MySQLConnectorTests):

    """BUG#18144971 ERROR WHEN USING UNICODE ARGUMENTS IN PREPARED STATEMENT"""

    def setUp(self):
        self.table = 'Bug18144971'
        self.table_cp1251 = 'Bug18144971_cp1251'

    def _setup(self):
        config = tests.get_mysql_config()
        config['use_unicode'] = True
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()

        cur.execute("DROP TABLE IF EXISTS {0}".format(self.table))

        create = ("CREATE TABLE {0} ( "
                  "`id` int(11) NOT NULL, "
                  "`name` varchar(40) NOT NULL , "
                  "`phone` varchar(40), "
                  "PRIMARY KEY (`id`))"
                  " CHARACTER SET 'utf8'".format(self.table))

        cur.execute(create)

        cur.execute(
            "DROP TABLE IF EXISTS {0}".format(self.table_cp1251)
        )

        create = ("CREATE TABLE {0} ( "
                  "`id` int(11) NOT NULL, "
                  "`name` varchar(40) NOT NULL , "
                  "`phone` varchar(40), "
                  "PRIMARY KEY (`id`))"
                  " CHARACTER SET 'cp1251'".format(self.table_cp1251))
        cur.execute(create)
        cnx.close()

    @cnx_config(use_unicode=True)
    @foreach_cnx(connection.MySQLConnection)
    def test_prepared_statement(self):
        self._setup()
        cur = self.cnx.cursor(prepared=True)
        stmt = "INSERT INTO {0} VALUES (?,?,?)".format(
            self.table)
        data = [(1, b'bytes', '1234'), (2, u'aaaаффф', '1111')]
        exp = [(1, b'bytes', b'1234'),
               (2, u'aaaаффф'.encode('cp1251'), b'1111')]
        cur.execute(stmt, data[0])
        self.cnx.commit()
        cur.execute("SELECT * FROM {0}".format(self.table))
        self.assertEqual(cur.fetchall(), [exp[0]])

        config = tests.get_mysql_config()
        config['charset'] = 'cp1251'
        self.cnx = self.cnx.__class__(**config)
        cur = self.cnx.cursor(prepared=True)
        stmt = "INSERT INTO {0} VALUES (?,?,?)".format(
            self.table_cp1251)
        cur.execute(stmt, data[1])
        self.cnx.commit()
        cur.execute("SELECT * FROM {0}".format(self.table_cp1251))
        self.assertEqual(cur.fetchall(), [exp[1]])

    @unittest.skipIf(not CMySQLConnection, ERR_NO_CEXT)
    @cnx_config(use_unicode=True)
    @foreach_cnx(CMySQLConnection)
    def test_prepared_argument(self):
        self.assertRaises(NotImplementedError, self.cnx.cursor, prepared=True)


class BugOra18389196(tests.MySQLConnectorTests):
    """BUG#18389196:  INSERTING PARAMETER MULTIPLE TIMES IN STATEMENT
    """

    def setUp(self):
        self.tbl = 'Bug18389196'

        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()

        cur.execute("DROP TABLE IF EXISTS %s" % self.tbl)

        create = ("CREATE TABLE %s ( "
                  "`id` int(11) NOT NULL, "
                  "`col1` varchar(20) NOT NULL, "
                  "`col2` varchar(20) NOT NULL, "
                  "PRIMARY KEY (`id`))" % self.tbl)
        cur.execute(create)
        cnx.close()

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cnx.cmd_query("DROP TABLE IF EXISTS %s" % self.tbl)
        cnx.close()

    @foreach_cnx()
    def test_parameters(self):
        self.cnx.cmd_query("TRUNCATE {0}".format(self.tbl))
        cur = self.cnx.cursor()
        stmt = ("INSERT INTO {0} (id,col1,col2) VALUES "
                "(%(id)s,%(name)s,%(name)s)".format(
            self.tbl))
        try:
            cur.execute(stmt, {'id': 1, 'name': 'ABC'})
        except errors.ProgrammingError as err:
            self.fail("Inserting parameter multiple times in a statement "
                      "failed: %s" % err)

        cur.close()

@unittest.skipIf(tests.MYSQL_VERSION >= (5, 7, 5),
                 "MySQL {0} does not support old password auth".format(
                     tests.MYSQL_VERSION_TXT))
class BugOra18415927(tests.MySQLConnectorTests):
    """BUG#18415927: AUTH_RESPONSE VARIABLE INCREMENTED WITHOUT BEING DEFINED
    """

    user = {
        'username': 'nativeuser',
        'password': 'nativeP@ss',
    }

    def setUp(self):
        config = tests.get_mysql_config()
        host = config['host']
        database = config['database']
        cnx = connection.MySQLConnection(**config)
        try:
            cnx.cmd_query("DROP USER '{user}'@'{host}'".format(
                host=host,
                user=self.user['username']))
        except:
            pass

        create_user = "CREATE USER '{user}'@'{host}' "
        cnx.cmd_query(create_user.format(user=self.user['username'],
                                         host=host))

        passwd = ("SET PASSWORD FOR '{user}'@'{host}' = "
                  "PASSWORD('{password}')").format(
            user=self.user['username'], host=host,
            password=self.user['password'])

        cnx.cmd_query(passwd)

        grant = "GRANT ALL ON {database}.* TO '{user}'@'{host}'"
        cnx.cmd_query(grant.format(database=database,
                                   user=self.user['username'],
                                   host=host))

    def tearDown(self):
        config = tests.get_mysql_config()
        host = config['host']
        cnx = connection.MySQLConnection(**config)
        cnx.cmd_query("DROP USER '{user}'@'{host}'".format(
            host=host,
            user=self.user['username']))

    def test_auth_response(self):
        config = tests.get_mysql_config()
        config['unix_socket'] = None
        config['user'] = self.user['username']
        config['password'] = self.user['password']
        config['client_flags'] = [-constants.ClientFlag.SECURE_CONNECTION]
        try:
            cnx = connection.MySQLConnection(**config)
        except Exception as exc:
            self.fail("Connection failed: {0}".format(exc))


class BugOra18527437(tests.MySQLConnectorTests):
    """BUG#18527437: UNITTESTS FAILING WHEN --host=::1 IS PASSED AS ARGUMENT
    """

    def test_poolname(self):
        config = tests.get_mysql_config()
        config['host'] = '::1'
        config['pool_size'] = 3

        exp = '{0}_{1}_{2}_{3}'.format(config['host'], config['port'],
                                       config['user'], config['database'])
        self.assertEqual(exp, pooling.generate_pool_name(**config))

    def test_custom_poolname(self):
        cnxpool = pooling.MySQLConnectionPool(pool_name='ham:spam',
                                              **tests.get_mysql_config())
        self.assertEqual('ham:spam', cnxpool._pool_name)
        cnxpool._remove_connections()


class BugOra18694096(tests.MySQLConnectorTests):
    """
    BUG#18694096: INCORRECT CONVERSION OF NEGATIVE TIMEDELTA
    """

    cases = [
        (timedelta(hours=0, minutes=0, seconds=1, microseconds=0),
         '00:00:01',),
        (timedelta(hours=0, minutes=0, seconds=-1, microseconds=0),
         '-00:00:01'),
        (timedelta(hours=0, minutes=1, seconds=1, microseconds=0),
         '00:01:01'),
        (timedelta(hours=0, minutes=-1, seconds=-1, microseconds=0),
         '-00:01:01'),
        (timedelta(hours=1, minutes=1, seconds=1, microseconds=0),
         '01:01:01'),
        (timedelta(hours=-1, minutes=-1, seconds=-1, microseconds=0),
         '-01:01:01'),
        (timedelta(days=3, seconds=86401),
         '96:00:01'),
        (timedelta(days=-3, seconds=86401),
         '-47:59:59'),
    ]

    # Cases for MySQL 5.6.4 and higher
    cases_564 = [
        (timedelta(hours=0, minutes=0, seconds=0, microseconds=1),
         '00:00:00.000001'),
        (timedelta(hours=0, minutes=0, seconds=0, microseconds=-1),
         '-00:00:00.000001'),
        (timedelta(days=2, hours=0, microseconds=1),
         '48:00:00.000001'),
        (timedelta(days=-3, seconds=86399, microseconds=999999),
         '-48:00:00.000001'),
    ]

    def setUp(self):
        config = tests.get_mysql_config()
        self.cnx = mysql.connector.connect(**config)

        self.tbl = 'times'
        self.cnx.cmd_query("DROP TABLE IF EXISTS {0}".format(self.tbl))
        if tests.MYSQL_VERSION >= (5, 6, 4):
            create = "CREATE TABLE {0} (c1 TIME(6))".format(self.tbl)
            self.cases += self.cases_564
        else:
            create = "CREATE TABLE {0} (c1 TIME)".format(self.tbl)
        self.cnx.cmd_query(create)

    def tearDown(self):
        return
        if self.cnx:
            self.cnx.cmd_query("DROP TABLE IF EXISTS {0}".format(self.tbl))

    def test_timedelta(self):
        # Note that both _timedelta_to_mysql and _TIME_to_python are
        # tested
        cur = self.cnx.cursor()

        # Following uses _timedelta_to_mysql to insert data
        data = [(case[0],) for case in self.cases]
        cur.executemany("INSERT INTO {0} (c1) VALUES (%s)".format(self.tbl),
                        data)
        self.cnx.commit()

        # We use _TIME_to_python to convert back to Python
        cur.execute("SELECT c1 FROM {0}".format(self.tbl))
        for i, row in enumerate(cur.fetchall()):
            self.assertEqual(self.cases[i][0], row[0],
                             "Incorrect timedelta for {0}, was {1!r}".format(
                                 self.cases[i][1], row[0]))


class BugOra18220593(tests.MySQLConnectorTests):
    """BUG#18220593 MYSQLCURSOR.EXECUTEMANY() DOESN'T LIKE UNICODE OPERATIONS
    """

    def setUp(self):
        config = tests.get_mysql_config()
        self.cnx = connection.MySQLConnection(**config)
        self.cur = self.cnx.cursor()
        self.table = u"⽃⽄⽅⽆⽇⽈⽉⽊"
        self.cur.execute(u"DROP TABLE IF EXISTS {0}".format(self.table))
        self.cur.execute(u"CREATE TABLE {0} (c1 VARCHAR(100)) "
                         u"CHARACTER SET 'utf8'".format(self.table))

    def test_unicode_operation(self):
        data = [('database',), (u'データベース',), (u'데이터베이스',)]
        self.cur.executemany(u"INSERT INTO {0} VALUES (%s)".format(
            self.table), data)
        self.cnx.commit()
        self.cur.execute(u"SELECT c1 FROM {0}".format(self.table))

        self.assertEqual(self.cur.fetchall(), data)

    def tearDown(self):
        self.cur.execute(u"DROP TABLE IF EXISTS {0}".format(self.table))
        self.cur.close()
        self.cnx.close()


class BugOra14843456(tests.MySQLConnectorTests):
    """BUG#14843456: UNICODE USERNAME AND/OR PASSWORD FAILS
    """

    def setUp(self):
        config = tests.get_mysql_config()
        self.cnx = connection.MySQLConnection(**config)
        self.cursor = self.cnx.cursor()

        if config['unix_socket'] and os.name != 'nt':
            self.host = 'localhost'
        else:
            self.host = config['host']

        grant = u"CREATE USER '{user}'@'{host}' IDENTIFIED BY '{password}'"

        self._credentials = [
            (u'Herne', u'Herne'),
            (u'\u0141owicz', u'\u0141owicz'),
        ]
        for user, password in self._credentials:
            self.cursor.execute(grant.format(
                user=user, host=self.host, password=password))

    def tearDown(self):
        for user, password in self._credentials:
            self.cursor.execute(u"DROP USER '{user}'@'{host}'".format(
                user=user, host=self.host))

    def test_unicode_credentials(self):
        config = tests.get_mysql_config()
        for user, password in self._credentials:
            config['user'] = user
            config['password'] = password
            config['database'] = None
            try:
                cnx = connection.MySQLConnection(**config)
            except (UnicodeDecodeError, errors.InterfaceError):
                self.fail('Failed using unicode username or password')
            else:
                cnx.close()


class Bug499410(tests.MySQLConnectorTests):

    """lp:499410 Disabling unicode does not work"""

    def test_use_unicode(self):
        config = tests.get_mysql_config()
        config['use_unicode'] = False
        cnx = connection.MySQLConnection(**config)

        self.assertEqual(False, cnx._use_unicode)
        cnx.close()

    @cnx_config(use_unicode=False, charset='greek')
    @foreach_cnx()
    def test_charset(self):
        charset = 'greek'
        cur = self.cnx.cursor()

        data = [b'\xe1\xed\xf4\xdf\xef']  # Bye in Greek
        exp_unicode = [(u'\u03b1\u03bd\u03c4\u03af\u03bf',), ]
        exp_nonunicode = [(data[0],)]

        tbl = '{0}test'.format(charset)
        try:
            cur.execute("DROP TABLE IF EXISTS {0}".format(tbl))
            cur.execute(
                "CREATE TABLE {0} (c1 VARCHAR(60)) charset={1}".format(
                    tbl, charset))
        except:
            self.fail("Failed creating test table.")

        stmt = u'INSERT INTO {0} VALUES (%s)'.format(tbl)
        try:
            for line in data:
                cur.execute(stmt, (line,))
        except Exception as exc:
            self.fail("Failed populating test table: {0}".format(str(exc)))

        cur.execute("SELECT * FROM {0}".format(tbl))
        res_nonunicode = cur.fetchall()
        self.cnx.set_unicode(True)
        cur.execute("SELECT * FROM {0}".format(tbl))
        res_unicode = cur.fetchall()

        try:
            cur.execute('DROP TABLE IF EXISTS {0}'.format(tbl))
        except:
            self.fail("Failed cleaning up test table.")

        self.assertEqual(exp_nonunicode, res_nonunicode)
        self.assertEqual(exp_unicode, res_unicode)


class BugOra18742429(tests.MySQLConnectorTests):
    """BUG#18742429:  CPY FAILS WHEN QUERYING LARGE NUMBER OF COLUMNS
    """
    def setUp(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)

        self.tbl = 'Bug18742429'
        cnx.cmd_query("DROP TABLE IF EXISTS %s" % self.tbl)

        create = 'CREATE TABLE {0}({1})'.format(self.tbl, ','.join(
            ['col'+str(i)+' INT(10)' for i in range(1000)]))

        cnx.cmd_query(create)
        cnx.close()

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cnx.cmd_query("DROP TABLE IF EXISTS {0}".format(self.tbl))
        cnx.close()

    @foreach_cnx(connection.MySQLConnection)
    def test_columns(self):
        cur = self.cnx.cursor()

        cur.execute('TRUNCATE TABLE {0}'.format(self.tbl))

        stmt = "INSERT INTO {0} VALUES({1})".format(self.tbl, ','.join(
            [str(i) if i%2==0 else 'NULL' for i in range(1000)]
        ))
        exp = tuple(i if i%2==0 else None for i in range(1000))
        cur.execute(stmt)

        cur = self.cnx.cursor(prepared=True)
        stmt = 'SELECT * FROM {0} WHERE col0=?'.format(self.tbl)
        cur.execute(stmt, (0,))
        self.assertEqual(exp, cur.fetchone())


class BugOra19164627(tests.MySQLConnectorTests):
    """BUG#19164627: Cursor tries to decode LINESTRING data as utf-8
    """
    def setUp(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)

        self.tbl = 'BugOra19164627'
        cnx.cmd_query("DROP TABLE IF EXISTS %s" % self.tbl)

        cnx.cmd_query("CREATE TABLE {0} ( "
                      "id SERIAL PRIMARY KEY AUTO_INCREMENT NOT NULL, "
                      "line LINESTRING NOT NULL "
                      ") DEFAULT CHARSET=ascii".format(self.tbl))
        cnx.close()

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cnx.cmd_query("DROP TABLE IF EXISTS {0}".format(self.tbl))
        cnx.close()

    @foreach_cnx()
    def test_linestring(self):
        cur = self.cnx.cursor()

        cur.execute('TRUNCATE TABLE {0}'.format(self.tbl))

        cur.execute('INSERT IGNORE INTO {0} (id, line) '
                    'VALUES (0,LINESTRING(POINT(0, 0), POINT(0, 1)))'.format(
            self.tbl
        ))

        cur.execute("SELECT * FROM {0} LIMIT 1".format(self.tbl))
        self.assertEqual(cur.fetchone(), (1, b'\x00\x00\x00\x00\x01\x02\x00\x00'
                                             b'\x00\x02\x00\x00\x00\x00\x00\x00'
                                             b'\x00\x00\x00\x00\x00\x00\x00\x00'
                                             b'\x00\x00\x00\x00\x00\x00\x00\x00'
                                             b'\x00\x00\x00\x00\x00\x00\x00\x00'
                                             b'\x00\x00\x00\xf0?', ))
        cur.close()


class BugOra19225481(tests.MySQLConnectorTests):
    """BUG#19225481: FLOATING POINT INACCURACY WITH PYTHON v2
    """
    def setUp(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)

        self.tbl = 'Bug19225481'
        cnx.cmd_query("DROP TABLE IF EXISTS {0}".format(self.tbl))

        create = 'CREATE TABLE {0} (col1 DOUBLE)'.format(self.tbl)
        cnx.cmd_query(create)
        cnx.close()

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cnx.cmd_query("DROP TABLE IF EXISTS {0}".format(self.tbl))
        cnx.close()

    @foreach_cnx()
    def test_columns(self):
        self.cnx.cmd_query("TRUNCATE {0}".format(self.tbl))
        cur = self.cnx.cursor()
        values = [
            (123.123456789987,),
            (234.234,),
            (12.12,),
            (111.331,),
            (0.0,),
            (-99.99999900099,)
        ]
        stmt = "INSERT INTO {0} VALUES(%s)".format(self.tbl)
        cur.executemany(stmt, values)

        stmt = "SELECT * FROM {0}".format(self.tbl)
        cur.execute(stmt)
        self.assertEqual(values, cur.fetchall())


class BugOra19169990(tests.MySQLConnectorTests):
    """BUG#19169990: Issue with compressed cnx using Python 2
    """

    @cnx_config(compress=True)
    @foreach_cnx()
    def test_compress(self):
        for charset in ('utf8', 'latin1', 'latin7'):
            self.config['charset'] = charset
            try:
                self.cnx = self.cnx.__class__(**self.config)
                cur = self.cnx.cursor()
                cur.execute("SELECT %s", ('mysql'*10000,))
            except TypeError:
                traceback.print_exc()
                self.fail("Failed setting up compressed cnx using {0}".format(
                    charset
                ))
            except errors.Error:
                self.fail("Failed sending/retrieving compressed data")

            self.cnx.close()

class BugOra19184025(tests.MySQLConnectorTests):
    """BUG#19184025: FIRST NULL IN ROW RETURNS REST OF ROW AS NONE
    """
    def setUp(self):
        self.tbl = 'Bug19184025'
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)

        cnx.cmd_query("DROP TABLE IF EXISTS {0}".format(self.tbl))
        create = "CREATE TABLE {0} (c1 INT, c2 INT NOT NULL DEFAULT 2)".format(
            self.tbl
        )
        cnx.cmd_query(create)

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cnx.cmd_query("DROP TABLE IF EXISTS {0}".format(self.tbl))
        cnx.close()

    @foreach_cnx()
    def test_row_to_python(self):
        self.cnx.cmd_query("TRUNCATE {0}".format(self.tbl))
        cur = self.cnx.cursor()
        cur.execute("INSERT INTO {0} (c1) VALUES (NULL)".format(self.tbl))
        cur.execute("SELECT * FROM {0}".format(self.tbl))
        self.assertEqual((None, 2), cur.fetchone())
        cur.close()


class BugOra19170287(tests.MySQLConnectorTests):
    """BUG#19170287: DUPLICATE OPTION_GROUPS RAISING ERROR WITH PYTHON 3
    """
    def test_duplicate_groups(self):
        option_file_dir = os.path.join('tests', 'data', 'option_files')
        opt_file = os.path.join(option_file_dir, 'dup_groups.cnf')

        exp = {
            u'password': u'mypass',
            u'user': u'mysql',
            u'database': u'duplicate_data',
            u'port': 10000
        }
        self.assertEqual(exp, read_option_files(option_files=opt_file))


class BugOra19169143(tests.MySQLConnectorTests):
    """BUG#19169143: FAILURE IN RAISING ERROR WITH DUPLICATE OPTION_FILES
    """
    def test_duplicate_optionfiles(self):
        option_file_dir = os.path.join('tests', 'data', 'option_files')
        files = [
            os.path.join(option_file_dir, 'include_files', '1.cnf'),
            os.path.join(option_file_dir, 'include_files', '2.cnf'),
            os.path.join(option_file_dir, 'include_files', '1.cnf'),
        ]
        self.assertRaises(ValueError, mysql.connector.connect,
                          option_files=files)


class BugOra19282158(tests.MySQLConnectorTests):
    """BUG#19282158: NULL values with prepared statements
    """
    def setUp(self):
        config = tests.get_mysql_config()
        self.cnx = connection.MySQLConnection(**config)
        self.cursor = self.cnx.cursor()

        self.tbl = 'Bug19282158'
        self.cursor.execute("DROP TABLE IF EXISTS %s" % self.tbl)

        create = ('CREATE TABLE {0}(col1 INT NOT NULL, col2 INT NULL, '
                  'col3 VARCHAR(10), col4 DECIMAL(4,2) NULL, '
                  'col5 DATETIME NULL, col6 INT NOT NULL, col7 VARCHAR(10), '
                  'PRIMARY KEY(col1))'.format(self.tbl))

        self.cursor.execute(create)

    def tearDown(self):
        self.cursor.execute("DROP TABLE IF EXISTS %s" % self.tbl)
        self.cursor.close()
        self.cnx.close()

    def test_null(self):
        cur = self.cnx.cursor(prepared=True)
        sql = ("INSERT INTO {0}(col1, col2, col3, col4, col5, col6, col7) "
               "VALUES (?, ?, ?, ?, ?, ?, ?)".format(self.tbl))
        params = (100, None, 'foo', None, datetime(2014, 8, 4, 9, 11, 14),
                  10, 'bar')
        exp = (100, None, bytearray(b'foo'), None,
               datetime(2014, 8, 4, 9, 11, 14), 10, bytearray(b'bar'))
        cur.execute(sql, params)

        sql = "SELECT * FROM {0}".format(self.tbl)
        cur.execute(sql)
        self.assertEqual(exp, cur.fetchone())
        cur.close()


class BugOra19168737(tests.MySQLConnectorTests):
    """BUG#19168737: UNSUPPORTED CONNECTION ARGUMENTS WHILE USING OPTION_FILES
    """
    def test_unsupported_arguments(self):
        option_file_dir = os.path.join('tests', 'data', 'option_files')
        opt_file = os.path.join(option_file_dir, 'pool.cnf')
        config = tests.get_mysql_config()

        conn = mysql.connector.connect(option_files=opt_file,
                                       option_groups=['pooling'], **config)
        self.assertEqual('my_pool', conn.pool_name)
        mysql.connector._CONNECTION_POOLS = {}
        conn.close()

        new_config = read_option_files(option_files=opt_file,
                                       option_groups=['fabric'], **config)

        exp = {
            'fabric': {
                'connect_delay': 3,
                'host': 'fabric.example.com',
                'password': 'foo',
                'ssl_ca': '/path/to/ssl'
           }
        }
        exp.update(config)

        self.assertEqual(exp, new_config)

        new_config = read_option_files(option_files=opt_file,
                                       option_groups=['failover'], **config)

        exp = {
            'failover': ({'pool_name': 'failA', 'port': 3306},
                         {'pool_name': 'failB', 'port': 3307})
        }
        exp.update(config)

        self.assertEqual(exp, new_config)


class BugOra19481761(tests.MySQLConnectorTests):
    """BUG#19481761: OPTION_FILES + !INCLUDE FAILS WITH TRAILING NEWLINE
    """
    def test_option_files_with_include(self):
        temp_cnf_file = os.path.join(os.getcwd(), 'temp.cnf')
        temp_include_file = os.path.join(os.getcwd(), 'include.cnf')

        cnf_file = open(temp_cnf_file, "w+")
        include_file = open(temp_include_file, "w+")

        config = tests.get_mysql_config()

        cnf = "[connector_python]\n"
        cnf += '\n'.join(['{0} = {1}'.format(key, value)
                         for key, value in config.items()])

        include_file.write(cnf)
        cnf_file.write("!include {0}\n".format(temp_include_file))

        cnf_file.close()
        include_file.close()

        try:
            conn = mysql.connector.connect(option_files=temp_cnf_file)
        except:
            self.fail("Connection failed with option_files argument.")

        self.assertEqual(config, read_option_files(option_files=temp_cnf_file))

        os.remove(temp_cnf_file)
        os.remove(temp_include_file)


class BugOra19584051(tests.MySQLConnectorTests):
    """BUG#19584051: TYPE_CODE DOES NOT COMPARE EQUAL
    """
    def setUp(self):
        config = tests.get_mysql_config()
        self.cnx = connection.MySQLConnection(**config)
        self.cursor = self.cnx.cursor()

        self.tbl = 'Bug19584051'
        self.cursor.execute("DROP TABLE IF EXISTS %s" % self.tbl)

        create = ('CREATE TABLE {0}(col1 INT NOT NULL, col2 BLOB, '
                  'col3 VARCHAR(10), col4 DECIMAL(4,2), '
                  'col5 DATETIME , col6 YEAR, '
                  'PRIMARY KEY(col1))'.format(self.tbl))

        self.cursor.execute(create)

    def tearDown(self):
        self.cursor.execute("DROP TABLE IF EXISTS %s" % self.tbl)
        self.cursor.close()
        self.cnx.close()

    def test_dbapi(self):
        cur = self.cnx.cursor()
        sql = ("INSERT INTO {0}(col1, col2, col3, col4, col5, col6) "
               "VALUES (%s, %s, %s, %s, %s, %s)".format(self.tbl))
        params = (100, 'blob-data', 'foo', 1.2, datetime(2014, 8, 4, 9, 11, 14),
                  2014)

        exp = [
            mysql.connector.NUMBER,
            mysql.connector.BINARY,
            mysql.connector.STRING,
            mysql.connector.NUMBER,
            mysql.connector.DATETIME,
            mysql.connector.NUMBER,
        ]
        cur.execute(sql, params)

        sql = "SELECT * FROM {0}".format(self.tbl)
        cur.execute(sql)
        temp = cur.fetchone()
        type_codes = [row[1] for row in cur.description]
        self.assertEqual(exp, type_codes)
        cur.close()


class BugOra19522948(tests.MySQLConnectorTests):
    """BUG#19522948: DATA CORRUPTION WITH TEXT FIELDS
    """
    def setUp(self):
        config = tests.get_mysql_config()
        self.cnx = connection.MySQLConnection(**config)
        self.cur = self.cnx.cursor()

        self.tbl = 'Bug19522948'
        self.cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))

        create = "CREATE TABLE {0} (c1 LONGTEXT NOT NULL)".format(
            self.tbl
        )
        self.cur.execute(create)

    def tearDown(self):
        self.cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))
        self.cur.close()
        self.cnx.close()

    def test_row_to_python(self):
        cur = self.cnx.cursor(prepared=True)

        data = "test_data"*10
        cur.execute("INSERT INTO {0} (c1) VALUES (?)".format(self.tbl), (data,))
        self.cur.execute("SELECT * FROM {0}".format(self.tbl))
        self.assertEqual((data,), self.cur.fetchone())
        self.cur.execute("TRUNCATE TABLE {0}".format(self.tbl))

        data = "test_data"*1000
        cur.execute("INSERT INTO {0} (c1) VALUES (?)".format(self.tbl), (data,))
        self.cur.execute("SELECT * FROM {0}".format(self.tbl))
        self.assertEqual((data,), self.cur.fetchone())
        self.cur.execute("TRUNCATE TABLE {0}".format(self.tbl))

        data = "test_data"*10000
        cur.execute("INSERT INTO {0} (c1) VALUES (?)".format(self.tbl), (data,))
        self.cur.execute("SELECT * FROM {0}".format(self.tbl))
        self.assertEqual((data,), self.cur.fetchone())


class BugOra19500097(tests.MySQLConnectorTests):
    """BUG#19500097: BETTER SUPPORT FOR RAW/BINARY DATA
    """
    def setUp(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()

        self.tbl = 'Bug19500097'
        cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))

        create = ("CREATE TABLE {0} (col1 VARCHAR(10), col2 INT) "
                  "DEFAULT CHARSET latin1".format(self.tbl))
        cur.execute(create)
        cur.close()
        cnx.close()

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))
        cur.close()
        cnx.close()

    @foreach_cnx()
    def test_binary_charset(self):

        sql = "INSERT INTO {0} VALUES(%s, %s)".format(self.tbl)
        cur = self.cnx.cursor()
        cur.execute(sql, ('foo', 1))
        cur.execute(sql, ('ëëë', 2))
        cur.execute(sql, (u'ááá', 5))

        self.cnx.set_charset_collation('binary')
        cur.execute(sql, ('bar', 3))
        cur.execute(sql, ('ëëë', 4))
        cur.execute(sql, (u'ááá', 6))

        exp = [
            (bytearray(b'foo'), 1),
            (bytearray(b'\xeb\xeb\xeb'), 2),
            (bytearray(b'\xe1\xe1\xe1'), 5),
            (bytearray(b'bar'), 3),
            (bytearray(b'\xc3\xab\xc3\xab\xc3\xab'), 4),
            (bytearray(b'\xc3\xa1\xc3\xa1\xc3\xa1'), 6)
        ]
        cur.execute("SELECT * FROM {0}".format(self.tbl))
        self.assertEqual(exp, cur.fetchall())


@unittest.skipIf(tests.MYSQL_VERSION < (5, 7, 3),
                 "MySQL {0} does not support COM_RESET_CONNECTION".format(
                 tests.MYSQL_VERSION_TXT))
class BugOra19549363(tests.MySQLConnectorTests):
    """BUG#19549363: Compression does not work with Change User
    """
    def test_compress(self):
        config = tests.get_mysql_config()
        config['compress'] = True

        mysql.connector._CONNECTION_POOLS = {}
        config['pool_name'] = 'mypool'
        config['pool_size'] = 3
        config['pool_reset_session'] = True
        cnx1 = mysql.connector.connect(**config)

        try:
            cnx1.close()
        except:
            self.fail("Reset session with compression test failed.")
        finally:
            mysql.connector._CONNECTION_POOLS = {}


class BugOra19803702(tests.MySQLConnectorTests):
    """BUG#19803702: CAN'T REPORT ERRORS THAT HAVE NON-ASCII CHARACTERS
    """
    def test_errors(self):
        config = tests.get_mysql_config()
        self.cnx = connection.MySQLConnection(**config)
        self.cur = self.cnx.cursor()

        self.tbl = 'áááëëëááá'
        self.cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))

        create = ("CREATE TABLE {0} (col1 VARCHAR(10), col2 INT) "
                  "DEFAULT CHARSET latin1".format(self.tbl))

        self.cur.execute(create)
        self.assertRaises(errors.DatabaseError, self.cur.execute, create)

    def tearDown(self):
        self.cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))
        self.cur.close()
        self.cnx.close()


class BugOra19777815(tests.MySQLConnectorTests):
    """BUG#19777815:  CALLPROC() DOES NOT SUPPORT WARNINGS
    """
    def setUp(self):
        config = tests.get_mysql_config()
        config['get_warnings'] = True
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()
        self.sp1 = 'BUG19777815'
        self.sp2 = 'BUG19777815_with_result'
        create1 = (
            "CREATE PROCEDURE {0}() BEGIN SIGNAL SQLSTATE '01000' "
            "SET MESSAGE_TEXT = 'TEST WARNING'; END;".format(self.sp1)
        )
        create2 = (
            "CREATE PROCEDURE {0}() BEGIN SELECT 1; SIGNAL SQLSTATE '01000' "
            "SET MESSAGE_TEXT = 'TEST WARNING'; END;".format(self.sp2)
        )

        cur.execute("DROP PROCEDURE IF EXISTS {0}".format(self.sp1))
        cur.execute("DROP PROCEDURE IF EXISTS {0}".format(self.sp2))
        cur.execute(create1)
        cur.execute(create2)
        cur.close()
        cnx.close()

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()
        cur.execute("DROP PROCEDURE IF EXISTS {0}".format(self.sp1))
        cur.execute("DROP PROCEDURE IF EXISTS {0}".format(self.sp2))
        cur.close()
        cnx.close()

    @foreach_cnx(get_warnings=True)
    def test_warning(self):
        cur = self.cnx.cursor()
        cur.callproc(self.sp1)
        exp = [(u'Warning', 1642, u'TEST WARNING')]
        self.assertEqual(exp, cur.fetchwarnings())

    @foreach_cnx(get_warnings=True)
    def test_warning_with_rows(self):
        cur = self.cnx.cursor()
        cur.callproc(self.sp2)

        exp = [(1,)]
        if PY2:
            self.assertEqual(exp, cur.stored_results().next().fetchall())
        else:
            self.assertEqual(exp, next(cur.stored_results()).fetchall())

        exp = [(u'Warning', 1642, u'TEST WARNING')]
        self.assertEqual(exp, cur.fetchwarnings())


class BugOra20407036(tests.MySQLConnectorTests):
    """BUG#20407036:  INCORRECT ARGUMENTS TO MYSQLD_STMT_EXECUTE ERROR
    """
    def setUp(self):
        config = tests.get_mysql_config()
        self.cnx = connection.MySQLConnection(**config)
        self.cur = self.cnx.cursor()

        self.tbl = 'Bug20407036'
        self.cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))

        create = ("CREATE TABLE {0} ( id int(10) unsigned NOT NULL, "
                  "text VARCHAR(70000) CHARACTER SET utf8 NOT NULL, "
                  "rooms tinyint(3) unsigned NOT NULL) "
                  "ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 "
                  "COLLATE=utf8_unicode_ci".format(self.tbl))
        self.cur.execute(create)

    def tearDown(self):
        self.cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))
        self.cur.close()
        self.cnx.close()

    def test_binary_charset(self):
        cur = self.cnx.cursor(prepared=True)
        sql = "INSERT INTO {0}(text, rooms) VALUES(%s, %s)".format(self.tbl)
        cur.execute(sql, ('a'*252, 1))
        cur.execute(sql, ('a'*253, 2))
        cur.execute(sql, ('a'*255, 3))
        cur.execute(sql, ('a'*251, 4))
        cur.execute(sql, ('a'*65535, 5))

        exp = [
            (0, 'a'*252, 1),
            (0, 'a'*253, 2),
            (0, 'a'*255, 3),
            (0, 'a'*251, 4),
            (0, 'a'*65535, 5),
        ]

        self.cur.execute("SELECT * FROM {0}".format(self.tbl))
        self.assertEqual(exp, self.cur.fetchall())


class BugOra20301989(tests.MySQLConnectorTests):
    """BUG#20301989: SET DATA TYPE NOT TRANSLATED CORRECTLY WHEN EMPTY
    """
    def setUp(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()

        self.tbl = 'Bug20301989'
        cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))

        create = ("CREATE TABLE {0} (col1 SET('val1', 'val2')) "
                  "DEFAULT CHARSET latin1".format(self.tbl))
        cur.execute(create)
        cur.close()
        cnx.close()

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))
        cur.close()
        cnx.close()

    @foreach_cnx()
    def test_set(self):
        cur = self.cnx.cursor()
        sql = "INSERT INTO {0} VALUES(%s)".format(self.tbl)
        cur.execute(sql, ('val1,val2',))
        cur.execute(sql, ('val1',))
        cur.execute(sql, ('',))
        cur.execute(sql, (None,))

        exp = [
            (set([u'val1', u'val2']),),
            (set([u'val1']),),
            (set([]),),
            (None,)
        ]

        cur.execute("SELECT * FROM {0}".format(self.tbl))
        self.assertEqual(exp, cur.fetchall())


class BugOra20462427(tests.MySQLConnectorTests):
    """BUG#20462427: BYTEARRAY INDEX OUT OF RANGE
    """
    def setUp(self):
        config = tests.get_mysql_config()
        config['autocommit'] = True
        config['connection_timeout'] = 100
        self.cnx = connection.MySQLConnection(**config)
        self.cur = self.cnx.cursor()

        self.tbl = 'BugOra20462427'
        self.cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))

        create = ("CREATE TABLE {0} ("
                  "id INT PRIMARY KEY, "
                  "a LONGTEXT "
                  ") ENGINE=Innodb DEFAULT CHARSET utf8".format(self.tbl))

        self.cur.execute(create)

    def tearDown(self):
        self.cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))
        self.cur.close()
        self.cnx.close()

    def test_bigdata(self):
        temp = 'a'*16777210
        insert = "INSERT INTO {0} (a) VALUES ('{1}')".format(self.tbl, temp)

        self.cur.execute(insert)
        self.cur.execute("SELECT a FROM {0}".format(self.tbl))
        res = self.cur.fetchall()
        self.assertEqual(16777210, len(res[0][0]))

        self.cur.execute("UPDATE {0} SET a = concat(a, 'a')".format(self.tbl))
        self.cur.execute("SELECT a FROM {0}".format(self.tbl))
        res = self.cur.fetchall()
        self.assertEqual(16777211, len(res[0][0]))

        self.cur.execute("UPDATE {0} SET a = concat(a, 'a')".format(self.tbl))
        self.cur.execute("SELECT a FROM {0}".format(self.tbl))
        res = self.cur.fetchall()
        self.assertEqual(16777212, len(res[0][0]))

        self.cur.execute("UPDATE {0} SET a = concat(a, 'a')".format(self.tbl))
        self.cur.execute("SELECT a FROM {0}".format(self.tbl))
        res = self.cur.fetchall()
        self.assertEqual(16777213, len(res[0][0]))


class BugOra20811802(tests.MySQLConnectorTests):
    """BUG#20811802:  ISSUES WHILE USING BUFFERED=TRUE OPTION WITH CPY CEXT
    """
    def setUp(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()

        self.tbl = 'Bug20811802'
        cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))

        create = ("CREATE TABLE {0} (id INT, name VARCHAR(5), dept VARCHAR(5)) "
                  "DEFAULT CHARSET latin1".format(self.tbl))
        cur.execute(create)
        cur.close()
        cnx.close()

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))
        cur.close()
        cnx.close()

    @foreach_cnx()
    def test_set(self):
        cur = self.cnx.cursor()
        sql = "INSERT INTO {0} VALUES(%s, %s, %s)".format(self.tbl)

        data = [
            (1, 'abc', 'cs'),
            (2, 'def', 'is'),
            (3, 'ghi', 'cs'),
            (4, 'jkl', 'it'),
        ]
        cur.executemany(sql, data)
        cur.close()

        cur = self.cnx.cursor(named_tuple=True, buffered=True)
        cur.execute("SELECT * FROM {0}".format(self.tbl))
        i = 0
        for row in cur:
            self.assertEqual((row.id, row.name, row.dept), data[i])
            i += 1
        cur.close()

        cur = self.cnx.cursor(dictionary=True, buffered=True)
        cur.execute("SELECT * FROM {0}".format(self.tbl))
        i = 0
        for row in cur:
            self.assertEqual(row, dict(zip(('id', 'name', 'dept'), data[i])))
            i += 1

        cur = self.cnx.cursor(named_tuple=True, buffered=False)
        cur.execute("SELECT * FROM {0}".format(self.tbl))
        i = 0
        for row in cur:
            self.assertEqual((row.id, row.name, row.dept), data[i])
            i += 1
        cur.close()

        cur = self.cnx.cursor(dictionary=True, buffered=False)
        cur.execute("SELECT * FROM {0}".format(self.tbl))
        i = 0
        for row in cur:
            self.assertEqual(row, dict(zip(('id', 'name', 'dept'), data[i])))
            i += 1


class BugOra20834643(tests.MySQLConnectorTests):
    """BUG#20834643: ATTRIBUTE ERROR NOTICED WHILE TRYING TO PROMOTE SERVERS
    """
    def setUp(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()

        self.tbl = 'Bug20834643'
        cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))

        create = ("CREATE TABLE {0} (id INT, name VARCHAR(5), dept VARCHAR(5)) "
                  "DEFAULT CHARSET latin1".format(self.tbl))
        cur.execute(create)
        cur.close()
        cnx.close()

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))
        cur.close()
        cnx.close()

    @foreach_cnx()
    def test_set(self):
        cur = self.cnx.cursor()
        sql = "INSERT INTO {0} VALUES(%s, %s, %s)".format(self.tbl)

        data = [
            (1, 'abc', 'cs'),
            (2, 'def', 'is'),
            (3, 'ghi', 'cs'),
            (4, 'jkl', 'it'),
        ]
        cur.executemany(sql, data)
        cur.close()

        cur = self.cnx.cursor(named_tuple=True)
        cur.execute("SELECT * FROM {0}".format(self.tbl))

        res = cur.fetchone()
        self.assertEqual(data[0], (res.id, res.name, res.dept))
        res = cur.fetchall()
        exp = []
        for row in res:
            exp.append((row.id, row.name, row.dept))
        self.assertEqual(exp, data[1:])
        cur.close()

        cur = self.cnx.cursor(named_tuple=True, buffered=True)
        cur.execute("SELECT * FROM {0}".format(self.tbl))
        res = cur.fetchone()
        self.assertEqual(data[0], (res.id, res.name, res.dept))
        res = cur.fetchall()
        exp = []
        for row in res:
            exp.append((row.id, row.name, row.dept))
        self.assertEqual(exp, data[1:])
        cur.close()

        cur = self.cnx.cursor(named_tuple=True, buffered=False)
        cur.execute("SELECT * FROM {0}".format(self.tbl))
        res = cur.fetchone()
        self.assertEqual(data[0], (res.id, res.name, res.dept))
        res = cur.fetchall()
        exp = []
        for row in res:
            exp.append((row.id, row.name, row.dept))
        self.assertEqual(exp, data[1:])
        cur.close()


class BugOra20653441(tests.MySQLConnectorTests):

    """BUG#20653441: PYTHON CONNECTOR HANGS IF A QUERY IS KILLED (ERROR 1317)"""

    def setUp(self):
        self.table_name = 'Bug20653441'
        self._setup()

    def _setup(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cnx.cmd_query("DROP TABLE IF EXISTS {0}".format(self.table_name))
        table = (
            "CREATE TABLE {table} ("
            " id INT UNSIGNED NOT NULL AUTO_INCREMENT,"
            " c1 VARCHAR(255) DEFAULT '{default}',"
            " PRIMARY KEY (id)"
            ")"
        ).format(table=self.table_name, default='a' * 255)
        cnx.cmd_query(table)

        stmt = "INSERT INTO {table} (id) VALUES {values}".format(
            table=self.table_name,
            values=','.join(['(NULL)'] * 1024)
        )
        cnx.cmd_query(stmt)
        cnx.commit()
        cnx.close()

    def tearDown(self):
        try:
            cnx = connection.MySQLConnection(**tests.get_mysql_config())
            cnx.cmd_query(
                "DROP TABLE IF EXISTS {0}".format(self.table_name))
            cnx.close()
        except:
            pass

    @foreach_cnx()
    def test_kill_query(self):

        def kill(connection_id):
            """Kill query using separate connection"""
            killer_cnx = connection.MySQLConnection(**tests.get_mysql_config())
            time.sleep(1)
            killer_cnx.cmd_query("KILL QUERY {0}".format(connection_id))
            killer_cnx.close()

        def sleepy_select(cnx):
            """Execute a SELECT statement which takes a while to complete"""
            cur = cnx.cursor()
            # Ugly query ahead!
            stmt = "SELECT x1.*, x2.* from {table} as x1, {table} as x2".format(
                table=self.table_name)
            cur.execute(stmt)
            # Save the error so we can check in the calling thread
            cnx.test_error = None

            try:
                cur.fetchall()
            except errors.Error as err:
                cnx.test_error = err
                cur.close()

        worker = Thread(target=sleepy_select, args=[self.cnx])
        killer = Thread(target=kill, args=[self.cnx.connection_id])
        worker.start()
        killer.start()
        worker.join()
        killer.join()

        self.cnx.close()

        self.assertTrue(isinstance(self.cnx.test_error, errors.DatabaseError))
        self.assertEqual(str(self.cnx.test_error),
                         "1317 (70100): Query execution was interrupted")


class BugOra21535573(tests.MySQLConnectorTests):
    """BUG#21535573:  SEGFAULT WHEN TRY TO SELECT GBK DATA WITH C-EXTENSION
    """
    def tearDown(self):
        cnx = connection.MySQLConnection(**tests.get_mysql_config())
        for charset in ('gbk', 'sjis', 'big5'):
            tablename = charset + 'test'
            cnx.cmd_query("DROP TABLE IF EXISTS {0}".format(tablename))
        cnx.close()

    def _test_charset(self, charset, data):
        config = tests.get_mysql_config()
        config['charset'] = charset
        config['use_unicode'] = True
        self.cnx = self.cnx.__class__(**config)
        tablename = charset + 'test'
        cur = self.cnx.cursor()

        cur.execute("DROP TABLE IF EXISTS {0}".format(tablename))
        if PY2:
            column = data.encode(charset)
        else:
            column = data
        table = (
            "CREATE TABLE {table} ("
            " {col} INT AUTO_INCREMENT KEY, "
            "c1 VARCHAR(40)"
            ") CHARACTER SET '{charset}'"
        ).format(table=tablename, charset=charset, col=column)
        cur.execute(table)
        self.cnx.commit()

        cur.execute("TRUNCATE {0}".format(tablename))
        self.cnx.commit()

        insert = "INSERT INTO {0} (c1) VALUES (%s)".format(tablename)
        cur.execute(insert, (data,))
        self.cnx.commit()

        cur.execute("SELECT * FROM {0}".format(tablename))
        for row in cur:
            self.assertEqual(data, row[1])

        cur.close()
        self.cnx.close()

    @foreach_cnx()
    def test_gbk(self):
        self._test_charset('gbk', u'海豚')

    @foreach_cnx()
    def test_sjis(self):
        self._test_charset('sjis', u'シイラ')

    @foreach_cnx()
    def test_big5(self):
        self._test_charset('big5', u'皿')


class BugOra21536507(tests.MySQLConnectorTests):
    """BUG#21536507:C/PYTHON BEHAVIOR NOT PROPER WHEN RAISE_ON_WARNINGS=TRUE
    """
    @cnx_config(raw=True, get_warnings=True, raise_on_warnings=True)
    @foreach_cnx()
    def test_with_raw(self):
        cur = self.cnx.cursor()
        drop_stmt = "DROP TABLE IF EXISTS unknown"
        self.assertRaises(errors.DatabaseError, cur.execute, drop_stmt)
        exp = [('Note', 1051, "Unknown table 'myconnpy.unknown'")]
        self.assertEqual(exp, cur.fetchwarnings())

        select_stmt = "SELECT 'a'+'b'"
        cur.execute(select_stmt)
        self.assertRaises(errors.DatabaseError, cur.fetchall)
        exp = [
            ('Warning', 1292, "Truncated incorrect DOUBLE value: 'a'"),
            ('Warning', 1292, "Truncated incorrect DOUBLE value: 'b'"),
        ]
        self.assertEqual(exp, cur.fetchwarnings())
        try:
            cur.close()
        except errors.InternalError as exc:
            self.fail("Closing cursor failed with: %s" % str(exc))


class BugOra21420633(tests.MySQLConnectorTests):
    """BUG#21420633: CEXTENSION CRASHES WHILE FETCHING LOTS OF NULL VALUES
    """
    def setUp(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()

        self.tbl = 'Bug21420633'
        cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))

        create = ("CREATE TABLE {0} (id INT, dept VARCHAR(5)) "
                  "DEFAULT CHARSET latin1".format(self.tbl))
        cur.execute(create)
        cur.close()
        cnx.close()

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS {0}".format(self.tbl))
        cur.close()
        cnx.close()

    @foreach_cnx()
    def test_null(self):
        cur = self.cnx.cursor()
        sql = "INSERT INTO {0} VALUES(%s, %s)".format(self.tbl)

        data = [(i, None) for i in range(10000)]

        cur.executemany(sql, data)
        cur.close()

        cur = self.cnx.cursor(named_tuple=True)
        cur.execute("SELECT * FROM {0}".format(self.tbl))

        res = cur.fetchall()
        cur.close()


class BugOra21492428(tests.MySQLConnectorTests):
    """BUG#21492428: CONNECT FAILS WHEN PASSWORD STARTS OR ENDS WITH SPACES
    """

    def setUp(self):
        config = tests.get_mysql_config()
        self.cnx = connection.MySQLConnection(**config)
        self.cursor = self.cnx.cursor()

        if config['unix_socket'] and os.name != 'nt':
            self.host = 'localhost'
        else:
            self.host = config['host']

        grant = u"CREATE USER '{user}'@'{host}' IDENTIFIED BY '{password}'"

        self._credentials = [
            ('ABCD', ' XYZ'),
            (' PQRS', ' 1 2 3 '),
            ('XYZ1 ', 'XYZ123    '),
            (' A B C D ', '    ppppp    '),
        ]
        for user, password in self._credentials:
            self.cursor.execute(grant.format(
                user=user, host=self.host, password=password))

    def tearDown(self):
        for user, password in self._credentials:
            self.cursor.execute(u"DROP USER '{user}'@'{host}'".format(
                user=user, host=self.host))

    def test_password_with_spaces(self):
        config = tests.get_mysql_config()
        for user, password in self._credentials:
            config['user'] = user
            config['password'] = password
            config['database'] = None
            try:
                cnx = connection.MySQLConnection(**config)
            except errors.ProgrammingError:
                self.fail('Failed using password with spaces')
            else:
                cnx.close()


class BugOra21492815(tests.MySQLConnectorTests):
    """BUG#21492815: CALLPROC() HANGS WHEN CONSUME_RESULTS=TRUE
    """
    def setUp(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()

        self.proc1 = 'Bug20834643'
        self.proc2 = 'Bug20834643_1'
        cur.execute("DROP PROCEDURE IF EXISTS {0}".format(self.proc1))

        create = ("CREATE PROCEDURE {0}() BEGIN SELECT 1234; "
                  "END".format(self.proc1))
        cur.execute(create)
        cur.execute("DROP PROCEDURE IF EXISTS {0}".format(self.proc2))

        create = ("CREATE PROCEDURE {0}() BEGIN SELECT 9876; "
                  "SELECT CONCAT('','abcd'); END".format(self.proc2))
        cur.execute(create)
        cur.close()
        cnx.close()

    def tearDown(self):
        config = tests.get_mysql_config()
        cnx = connection.MySQLConnection(**config)
        cur = cnx.cursor()
        cur.execute("DROP PROCEDURE IF EXISTS {0}".format(self.proc1))
        cur.execute("DROP PROCEDURE IF EXISTS {0}".format(self.proc2))
        cur.close()
        cnx.close()

    @cnx_config(consume_results=True, raw=True)
    @foreach_cnx()
    def test_set(self):
        cur = self.cnx.cursor()
        cur.callproc(self.proc1)
        self.assertEqual((bytearray(b'1234'),),
                          next(cur.stored_results()).fetchone())

        cur.callproc(self.proc2)
        exp = [[(bytearray(b'9876'),)], [(bytearray(b'abcd'),)]]
        results = []
        for result in cur.stored_results():
            results.append(result.fetchall())
        self.assertEqual(exp, results)
        cur.close()

    @cnx_config(consume_results=True, raw=False)
    @foreach_cnx()
    def test_set(self):
        cur = self.cnx.cursor()
        cur.callproc(self.proc1)
        self.assertEqual((1234,),
                          next(cur.stored_results()).fetchone())

        cur.callproc(self.proc2)
        exp = [[(9876,)], [('abcd',)]]
        results = []
        for result in cur.stored_results():
            results.append(result.fetchall())
        self.assertEqual(exp, results)
        cur.close()
