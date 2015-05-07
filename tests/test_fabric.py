# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2013, 2015, Oracle and/or its affiliates. All rights reserved.

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
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA

"""Unittests for mysql.connector.fabric
"""

import datetime
from decimal import Decimal
import time
import unittest
import uuid

try:
    from xmlrpclib import Fault, ServerProxy
except ImportError:
    # Python v3
    from xmlrpc.client import Fault, ServerProxy  # pylint: disable=F0401

import tests
import mysql.connector
from mysql.connector import fabric, errorcode
from mysql.connector.fabric import connection, balancing
from mysql.connector.catch23 import UNICODE_TYPES, PY2
from mysql.connector.pooling import PooledMySQLConnection

ERR_NO_FABRIC_CONFIG = "Fabric configuration not available"


def wait_for_gtid(cur, gtid):
    cur.execute("SELECT WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS(%s, 2)", (gtid,))
    cur.fetchall()


class FabricModuleTests(tests.MySQLConnectorTests):
    """Testing mysql.connector.fabric module"""

    def test___all___(self):
        attrs = [
            'MODE_READWRITE',
            'MODE_READONLY',
            'STATUS_PRIMARY',
            'STATUS_SECONDARY',
            'SCOPE_GLOBAL',
            'SCOPE_LOCAL',
            'FabricMySQLServer',
            'FabricShard',
            'connect',
            'Fabric',
            'FabricConnection',
            'MySQLFabricConnection',
        ]

        for attr in attrs:
            try:
                getattr(fabric, attr)
            except AttributeError:
                self.fail("Attribute '{0}' not in fabric.__all__".format(attr))

    def test_fabricmyqlserver(self):
        attrs = ['uuid', 'group', 'host', 'port', 'mode', 'status', 'weight']
        try:
            nmdtpl = fabric.FabricMySQLServer(*([''] * len(attrs)))
        except TypeError:
            self.fail("Fail creating namedtuple FabricMySQLServer")

        self.check_namedtuple(nmdtpl, attrs)

    def test_fabricshard(self):
        attrs = [
            'database', 'table', 'column', 'key', 'shard', 'shard_type',
            'group', 'global_group'
        ]
        try:
            nmdtpl = fabric.FabricShard(*([''] * len(attrs)))
        except TypeError:
            self.fail("Fail creating namedtuple FabricShard")

        self.check_namedtuple(nmdtpl, attrs)

    def test_connect(self):

        class FakeConnection(object):
            def __init__(self, *args, **kwargs):
                pass

        orig = fabric.MySQLFabricConnection
        fabric.MySQLFabricConnection = FakeConnection

        self.assertTrue(isinstance(fabric.connect(), FakeConnection))
        fabric.MySQLFabricConnection = orig


class ConnectionModuleTests(tests.MySQLConnectorTests):
    """Testing mysql.connector.fabric.connection module"""

    def test_module_variables(self):
        error_codes = (
            errorcode.CR_SERVER_LOST,
            errorcode.ER_OPTION_PREVENTS_STATEMENT,
        )
        self.assertEqual(error_codes, connection.RESET_CACHE_ON_ERROR)

        modvars = {
            'MYSQL_FABRIC_PORT': {
                'xmlrpc': 32274, 'mysql': 32275
            },
            'DEFAULT_FABRIC_PROTOCOL': 'xmlrpc',
            'FABRICS': {},
            '_CNX_ATTEMPT_DELAY': 1,
            '_CNX_ATTEMPT_MAX': 3,
            '_GETCNX_ATTEMPT_DELAY': 1,
            '_GETCNX_ATTEMPT_MAX': 3,
            'MODE_READONLY': 1,
            'MODE_WRITEONLY': 2,
            'MODE_READWRITE': 3,
            'STATUS_FAULTY': 0,
            'STATUS_SPARE': 1,
            'STATUS_SECONDARY': 2,
            'STATUS_PRIMARY': 3,
            'SCOPE_GLOBAL': 'GLOBAL',
            'SCOPE_LOCAL': 'LOCAL',
            '_SERVER_STATUS_FAULTY': 'FAULTY',
        }

        for modvar, value in modvars.items():
            try:
                self.assertEqual(value, getattr(connection, modvar))
            except AttributeError:
                self.fail("Module variable connection.{0} not found".format(
                    modvar))

    def test_cnx_properties(self):
        cnxprops = {
            # name: (valid_types, description, default)
            'group': ((str,), "Name of group of servers", None),
            'key': (tuple([int, str, datetime.datetime,
                     datetime.date] + list(UNICODE_TYPES)),
                    "Sharding key", None),
            'tables': ((tuple, list), "List of tables in query", None),
            'mode': ((int,), "Read-Only, Write-Only or Read-Write",
                     connection.MODE_READWRITE),
            'shard': ((str,), "Identity of the shard for direct connection",
                      None),
            'mapping': ((str,), "", None),
            'scope': ((str,), "GLOBAL for accessing Global Group, or LOCAL",
                      connection.SCOPE_LOCAL),
            'attempts': ((int,), "Attempts for getting connection",
                         connection._CNX_ATTEMPT_MAX),
            'attempt_delay': ((int,), "Seconds to wait between each attempt",
                              connection._CNX_ATTEMPT_DELAY),
        }

        for prop, desc in cnxprops.items():
            try:
                self.assertEqual(desc, connection._CNX_PROPERTIES[prop])
            except KeyError:
                self.fail("Connection property '{0}'' not available".format(
                    prop))

        self.assertEqual(len(cnxprops), len(connection._CNX_PROPERTIES))

    def test__fabric_xmlrpc_uri(self):
        data = ('example.com', 32274)
        exp = 'http://{host}:{port}'.format(host=data[0], port=data[1])
        self.assertEqual(exp, connection._fabric_xmlrpc_uri(*data))

    def test__fabric_server_uuid(self):
        data = ('example.com', 32274)
        url = 'http://{host}:{port}'.format(host=data[0], port=data[1])
        exp = uuid.uuid3(uuid.NAMESPACE_URL, url)
        self.assertEqual(exp, connection._fabric_server_uuid(*data))

    def test__validate_ssl_args(self):
        func = connection._validate_ssl_args
        kwargs = dict(ssl_ca=None, ssl_key=None, ssl_cert=None)
        self.assertEqual(None, func(**kwargs))

        kwargs = dict(ssl_ca=None, ssl_key='/path/to/key',
                      ssl_cert=None)
        self.assertRaises(AttributeError, func, **kwargs)

        kwargs = dict(ssl_ca='/path/to/ca', ssl_key='/path/to/key',
                      ssl_cert=None)
        self.assertRaises(AttributeError, func, **kwargs)

        exp = {
            'ca': '/path/to/ca',
            'key': None,
            'cert': None,
        }
        kwargs = dict(ssl_ca='/path/to/ca', ssl_key=None, ssl_cert=None)
        self.assertEqual(exp, func(**kwargs))

        exp = {
            'ca': '/path/to/ca',
            'key': '/path/to/key',
            'cert': '/path/to/cert',
        }
        res = func(ssl_ca=exp['ca'], ssl_cert=exp['cert'], ssl_key=exp['key'])
        self.assertEqual(exp, res)

    def test_extra_failure_report(self):
        func = connection.extra_failure_report
        func([])
        self.assertEqual([], connection.REPORT_ERRORS_EXTRA)

        self.assertRaises(AttributeError, func, 1)
        self.assertRaises(AttributeError, func, [1])

        exp = [2222]
        func(exp)
        self.assertEqual(exp, connection.REPORT_ERRORS_EXTRA)


class FabricBalancingBaseScheduling(tests.MySQLConnectorTests):

    """Test fabric.balancing.BaseScheduling"""

    def setUp(self):
        self.obj = balancing.BaseScheduling()

    def test___init__(self):
        self.assertEqual([], self.obj._members)
        self.assertEqual([], self.obj._ratios)

    def test_set_members(self):
        self.assertRaises(NotImplementedError, self.obj.set_members, 'spam')

    def test_get_next(self):
        self.assertRaises(NotImplementedError, self.obj.get_next)


class FabricBalancingWeightedRoundRobin(tests.MySQLConnectorTests):

    """Test fabric.balancing.WeightedRoundRobin"""

    def test___init__(self):
        balancer = balancing.WeightedRoundRobin()
        self.assertEqual([], balancer._members)
        self.assertEqual([], balancer._ratios)
        self.assertEqual([], balancer._load)

        # init with args
        class FakeWRR(balancing.WeightedRoundRobin):
            def set_members(self, *args):
                self.set_members_called = True
        balancer = FakeWRR('ham', 'spam')
        self.assertTrue(balancer.set_members_called)

    def test_members(self):
        balancer = balancing.WeightedRoundRobin()
        self.assertEqual([], balancer.members)
        balancer._members = ['ham']
        self.assertEqual(['ham'], balancer.members)

    def test_ratios(self):
        balancer = balancing.WeightedRoundRobin()
        self.assertEqual([], balancer.ratios)
        balancer._ratios = ['ham']
        self.assertEqual(['ham'], balancer.ratios)

    def test_load(self):
        balancer = balancing.WeightedRoundRobin()
        self.assertEqual([], balancer.load)
        balancer._load = ['ham']
        self.assertEqual(['ham'], balancer.load)

    def test_set_members(self):
        balancer = balancing.WeightedRoundRobin()
        balancer._members = ['ham']
        balancer.set_members()
        self.assertEqual([], balancer.members)

        servers = [('ham1', 0.2), ('ham2', 0.8)]

        balancer.set_members(*servers)
        exp = [('ham2', Decimal('0.8')), ('ham1', Decimal('0.2'))]
        self.assertEqual(exp, balancer.members)
        self.assertEqual([400, 100], balancer.ratios)
        self.assertEqual([0, 0], balancer.load)

    def test_reset_load(self):
        balancer = balancing.WeightedRoundRobin(*[('ham1', 0.2), ('ham2', 0.8)])
        balancer._load = [5, 6]
        balancer.reset()
        self.assertEqual([0, 0], balancer.load)

    def test_get_next(self):
        servers = [('ham1', 0.2), ('ham2', 0.8)]
        balancer = balancing.WeightedRoundRobin(*servers)
        self.assertEqual(('ham2', Decimal('0.8')), balancer.get_next())
        self.assertEqual([1, 0], balancer.load)
        balancer._load = [80, 0]
        self.assertEqual(('ham1', Decimal('0.2')), balancer.get_next())
        self.assertEqual([80, 1], balancer.load)
        balancer._load = [80, 20]
        self.assertEqual(('ham2', Decimal('0.8')), balancer.get_next())
        self.assertEqual([81, 20], balancer.load)

        servers = [('ham1', 0.1), ('ham2', 0.2), ('ham3', 0.7)]
        balancer = balancing.WeightedRoundRobin(*servers)
        exp_sum = count = 101
        while count > 0:
            count -= 1
            _ = balancer.get_next()
        self.assertEqual(exp_sum, sum(balancer.load))
        self.assertEqual([34, 34, 33], balancer.load)

        servers = [('ham1', 0.2), ('ham2', 0.2), ('ham3', 0.7)]
        balancer = balancing.WeightedRoundRobin(*servers)
        exp_sum = count = 101
        while count > 0:
            count -= 1
            _ = balancer.get_next()
        self.assertEqual(exp_sum, sum(balancer.load))
        self.assertEqual([34, 34, 33], balancer.load)

        servers = [('ham1', 0.25), ('ham2', 0.25),
                   ('ham3', 0.25), ('ham4', 0.25)]
        balancer = balancing.WeightedRoundRobin(*servers)
        exp_sum = count = 101
        while count > 0:
            count -= 1
            _ = balancer.get_next()
        self.assertEqual(exp_sum, sum(balancer.load))
        self.assertEqual([26, 25, 25, 25], balancer.load)

        servers = [('ham1', 0.5), ('ham2', 0.5)]
        balancer = balancing.WeightedRoundRobin(*servers)
        count = 201
        while count > 0:
            count -= 1
            _ = balancer.get_next()
        self.assertEqual(1, sum(balancer.load))
        self.assertEqual([1, 0], balancer.load)

    def test___repr__(self):
        balancer = balancing.WeightedRoundRobin(*[('ham1', 0.2), ('ham2', 0.8)])
        exp = ("<class 'mysql.connector.fabric.balancing.WeightedRoundRobin'>"
               "(load=[0, 0], ratios=[400, 100])")
        self.assertEqual(exp, repr(balancer))

    def test___eq__(self):
        servers = [('ham1', 0.2), ('ham2', 0.8)]
        balancer1 = balancing.WeightedRoundRobin(*servers)
        balancer2 = balancing.WeightedRoundRobin(*servers)
        self.assertTrue(balancer1 == balancer2)

        servers = [('ham1', 0.2), ('ham2', 0.3), ('ham3', 0.5)]
        balancer3 = balancing.WeightedRoundRobin(*servers)
        self.assertFalse(balancer1 == balancer3)

@unittest.skipIf(not tests.FABRIC_CONFIG, ERR_NO_FABRIC_CONFIG)
class FabricShardingTests(tests.MySQLConnectorTests):

    """Test Fabric's sharding"""

    emp_data = {
        1985: [
            (10001, datetime.date(1953, 9, 2), u'Georgi', u'Facello', u'M',
             datetime.date(1986, 6, 26)),
            (10002, datetime.date(1964, 6, 2), u'Bezalel', u'Simmel', u'F',
             datetime.date(1985, 11, 21)),
        ],
        2000: [
            (47291, datetime.date(1960, 9, 9), u'Ulf', u'Flexer', u'M',
             datetime.date(2000, 1, 12)),
            (60134, datetime.date(1964, 4, 21), u'Seshu', u'Rathonyi', u'F',
             datetime.date(2000, 1, 2)),
        ]
    }

    def setUp(self):
        self.cnx = mysql.connector.connect(
            fabric=tests.FABRIC_CONFIG, user='root', database='employees'
        )

    def _check_table(self, table, shard_type):
        fabric = self.cnx._fabric
        fab_set = fabric.execute("sharding", "lookup_table", table)
        found = False
        if fab_set.rowcount:
            for row in fab_set.rows():
                if (row.table_name == table and row.type_name == shard_type):
                    found = True
                    break

        if found == False:
            raise ValueError(
                "Table {table} not found or wrong sharding type".format(
                    table=table))

        return True

    def _populate(self, cnx, wait_gtid, table, insert, data, shard_key_index):
        for employee in data:
            cnx.set_property(tables=["employees." + table],
                             key=employee[shard_key_index],
                             scope=fabric.SCOPE_LOCAL,
                             mode=fabric.MODE_READWRITE)
            cur = cnx.cursor()
            wait_for_gtid(cur, wait_gtid)
            cur.execute(insert, employee)
            cnx.commit()

    def _truncate(self, cur, table):
        cur.execute("TRUNCATE TABLE {0}".format(table))
        cur.execute("SELECT @@global.gtid_executed")
        return cur.fetchone()[0]

    def test_range(self):
        self.assertTrue(self._check_table("employees.employees_range", 'RANGE'))
        tbl_name = "employees_range"

        tables = ["employees.{0}".format(tbl_name)]

        self.cnx.set_property(tables=tables,
                              scope=fabric.SCOPE_GLOBAL,
                              mode=fabric.MODE_READWRITE)
        cur = self.cnx.cursor()
        gtid_executed = self._truncate(cur, tbl_name)
        self.cnx.commit()

        insert = ("INSERT INTO {0} "
                  "VALUES (%s, %s, %s, %s, %s, %s)").format(tbl_name)

        self._populate(self.cnx, gtid_executed, tbl_name, insert,
                       self.emp_data[1985] + self.emp_data[2000], 0)

        time.sleep(2)

        # Year is key of self.emp_data, second value is emp_no for RANGE key
        exp_keys = [(1985, 10002), (2000, 47291)]
        for year, emp_no in exp_keys:
            self.cnx.set_property(tables=tables,
                                  scope=fabric.SCOPE_LOCAL,
                                  key=emp_no, mode=fabric.MODE_READONLY)
            cur = self.cnx.cursor()
            cur.execute("SELECT * FROM {0}".format(tbl_name))
            rows = cur.fetchall()
            self.assertEqual(rows, self.emp_data[year])

        self.cnx.set_property(tables=tables,
                              key='spam', mode=fabric.MODE_READONLY)
        self.assertRaises(ValueError, self.cnx.cursor)


    def test_range_datetime(self):
        self.assertTrue(self._check_table(
            "employees.employees_range_datetime", 'RANGE_DATETIME'))
        tbl_name = "employees_range_datetime"

        tables = ["employees.{0}".format(tbl_name)]

        self.cnx.set_property(tables=tables,
                              scope=fabric.SCOPE_GLOBAL,
                              mode=fabric.MODE_READWRITE)
        cur = self.cnx.cursor()
        gtid_executed = self._truncate(cur, tbl_name)
        self.cnx.commit()

        insert = ("INSERT INTO {0} "
                  "VALUES (%s, %s, %s, %s, %s, %s)").format(tbl_name)

        self._populate(self.cnx, gtid_executed, tbl_name, insert,
                       self.emp_data[1985] + self.emp_data[2000], 5)

        time.sleep(2)

        hire_dates = [datetime.date(1985, 1, 1), datetime.date(2000, 1, 1)]
        for hire_date in hire_dates:
            self.cnx.set_property(tables=tables,
                                  key=hire_date, mode=fabric.MODE_READONLY)
            cur = self.cnx.cursor()
            cur.execute("SELECT * FROM {0}".format(tbl_name))
            rows = cur.fetchall()
            self.assertEqual(rows, self.emp_data[hire_date.year])

        self.cnx.set_property(tables=tables,
                              key='2014-01-02', mode=fabric.MODE_READONLY)
        self.assertRaises(ValueError, self.cnx.cursor)

    def test_range_string(self):
        self.assertTrue(self._check_table(
            "employees.employees_range_string", 'RANGE_STRING'))
        tbl_name = "employees_range_string"

        tables = ["employees.{0}".format(tbl_name)]

        self.cnx.set_property(tables=tables,
                              scope=fabric.SCOPE_GLOBAL,
                              mode=fabric.MODE_READWRITE)
        cur = self.cnx.cursor()
        gtid_executed = self._truncate(cur, tbl_name)
        self.cnx.commit()

        insert = ("INSERT INTO {0} "
                  "VALUES (%s, %s, %s, %s, %s, %s)").format(tbl_name)

        self._populate(self.cnx, gtid_executed, tbl_name, insert,
                       self.emp_data[1985] + self.emp_data[2000], 3)

        time.sleep(2)

        emp_exp_range_string = {
            'A': [self.emp_data[1985][0],
                  self.emp_data[2000][0]],
            'M': [self.emp_data[1985][1],
                  self.emp_data[2000][1]],
        }

        str_keys = [u'A', u'M']
        for str_key in str_keys:
            self.cnx.set_property(tables=tables,
                                  key=str_key, mode=fabric.MODE_READONLY)
            cur = self.cnx.cursor()
            cur.execute("SELECT * FROM {0}".format(tbl_name))
            rows = cur.fetchall()
            self.assertEqual(rows, emp_exp_range_string[str_key])

        self.cnx.set_property(tables=tables,
                              key=b'not unicode str', mode=fabric.MODE_READONLY)
        self.assertRaises(ValueError, self.cnx.cursor)

        self.cnx.set_property(tables=tables,
                              key=12345, mode=fabric.MODE_READONLY)
        self.assertRaises(ValueError, self.cnx.cursor)

        if PY2:
            self.cnx.set_property(tables=tables,
                                  key='not unicode str',
                                  mode=fabric.MODE_READONLY)
            self.assertRaises(ValueError, self.cnx.cursor)

    def test_bug19642249(self):
        self.assertTrue(self._check_table(
            "employees.employees_range_string", 'RANGE_STRING'))

        # Invalid key for RANGE_STRING
        tbl_name = "employees_range_string"
        tables = ["employees.{0}".format(tbl_name)]

        self.cnx.set_property(tables=tables,
                              key=u'1', mode=fabric.MODE_READONLY)
        try:
            cur = self.cnx.cursor()
        except ValueError as exc:
            self.assertEqual("Key invalid; was '1'", str(exc))
        else:
            self.fail("ValueError not raised")

        # Invalid key for RANGE_DATETIME
        tbl_name = "employees_range_datetime"
        tables = ["employees.{0}".format(tbl_name)]

        self.cnx.set_property(tables=tables,
                              key=datetime.date(1977, 1, 1),
                              mode=fabric.MODE_READONLY)
        try:
            cur = self.cnx.cursor()
        except ValueError as exc:
            self.assertEqual("Key invalid; was '1977-01-01'", str(exc))
        else:
            self.fail("ValueError not raised")

    def test_bug19331658(self):
        """Pooling not working with fabric
        """
        self.assertRaises(
            AttributeError, mysql.connector.connect,
            fabric=tests.FABRIC_CONFIG, user='root', database='employees',
            pool_name='mypool')

        pool_size = 2
        cnx = mysql.connector.connect(
            fabric=tests.FABRIC_CONFIG, user='root', database='employees',
            pool_size=pool_size, pool_reset_session=False
        )
        tbl_name = "employees_range"

        tables = ["employees.{0}".format(tbl_name)]

        cnx.set_property(tables=tables,
                         scope=fabric.SCOPE_GLOBAL,
                         mode=fabric.MODE_READWRITE)
        cnx.cursor()
        self.assertTrue(isinstance(cnx._mysql_cnx, PooledMySQLConnection))

        data = self.emp_data[1985]
        for emp in data:
            cnx.set_property(tables=tables,
                             key=emp[0],
                             scope=fabric.SCOPE_LOCAL,
                             mode=fabric.MODE_READWRITE)
            cnx.cursor()
            mysqlserver = cnx._fabric_mysql_server
            config = cnx._mysql_config
            self.assertEqual(
                cnx._mysql_cnx.pool_name, "{0}_{1}_{2}_{3}".format(
                    mysqlserver.host, mysqlserver.port, config['user'],
                    config['database'])
            )

    def test_range_hash(self):
        self.assertTrue(self._check_table(
            "employees.employees_hash", 'HASH'))
        tbl_name = "employees_hash"

        tables = ["employees.{0}".format(tbl_name)]

        self.cnx.set_property(tables=tables,
                              scope=fabric.SCOPE_GLOBAL,
                              mode=fabric.MODE_READWRITE)

        cur = self.cnx.cursor()
        gtid_executed = self._truncate(cur, tbl_name)
        self.cnx.commit()

        insert = ("INSERT INTO {0} "
                  "VALUES (%s, %s, %s, %s, %s, %s)").format(tbl_name)

        self._populate(self.cnx, gtid_executed, tbl_name, insert,
                       self.emp_data[1985] + self.emp_data[2000], 3)

        time.sleep(2)

        emp_exp_hash = self.emp_data[1985] + self.emp_data[2000]

        rows = []
        self.cnx.reset_properties()
        str_keys = ['group1', 'group2']
        for str_key in str_keys:
            self.cnx.set_property(group=str_key, mode=fabric.MODE_READONLY)
            cur = self.cnx.cursor()
            cur.execute("SELECT * FROM {0}".format(tbl_name))
            rows += cur.fetchall()

        self.assertEqual(rows, emp_exp_hash)
