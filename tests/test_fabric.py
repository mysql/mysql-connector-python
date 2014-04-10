# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved.

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

from decimal import Decimal
import sys
import uuid

try:
    from xmlrpclib import Fault, ServerProxy
except ImportError:
    # Python v3
    from xmlrpc.client import Fault, ServerProxy  # pylint: disable=F0401

import tests
import mysql.connector
from mysql.connector import fabric, errorcode, errors
from mysql.connector.fabric import connection, caching, balancing

_HOST = 'tests.example.com'
_PORT = 1234


class _MockupXMLProxy(object):
    """Mock-up of XMLProxy simulating Fabric XMLRPC

    This class can be used as a mockup for xmlrpclib.ServerProxy
    """

    fabric_servers = ['tests.example.com:1234']
    fabric_uuid = 'c4563310-f742-4d24-87ec-930088d892ff'
    version_token = 1
    ttl = 1 * 60

    groups = {
        'testgroup1': [
            ['a6ac2895-574f-11e3-bc32-bcaec56cc4a7', 'testgroup1',
             _HOST, '3372', 1, 2, 1.0],
            ['af1cb1e4-574f-11e3-bc33-bcaec56cc4a7', 'testgroup1',
             _HOST, '3373', 3, 3, 1.0],
        ],
        'testgroup2': [
            ['b99bf2f3-574f-11e3-bc33-bcaec56cc4a7', 'testgroup2',
             _HOST, '3374', 3, 3, 1.0],
            ['c3afdef6-574f-11e3-bc33-bcaec56cc4a7', 'testgroup2',
             _HOST, '3375', 1, 2, 1.0],
        ],
        'testglobalgroup': [
            ['91f7090e-574f-11e3-bc32-bcaec56cc4a7', 'testglobalgroup',
             _HOST, '3370', 3, 3, 1.0],
            ['9c09932d-574f-11e3-bc32-bcaec56cc4a7', 'testglobalgroup',
             _HOST, '3371', 1, 2, 1.0],
        ],
        'onlysecondary': [
            ['b99bf2f3-574f-11e3-bc33-bcaec56cc4a7', 'onlysecondary',
             _HOST, '3374', 1, 2, 1.0],
            ['c3afdef6-574f-11e3-bc33-bcaec56cc4a7', 'onlysecondary',
             _HOST, '3375', 1, 2, 1.0],
        ],
        'onlyprimary': [
            ['af1cb1e4-574f-11e3-bc33-bcaec56cc4a7', 'onlyprimary',
             _HOST, '3373', 3, 3, 1.0],
        ],
        'onlyspare': [
            ['b99bf2f3-574f-11e3-bc33-bcaec56cc4a7', 'onlyspare',
             _HOST, '3374', 1, 1, 1.0],
            ['c3afdef6-574f-11e3-bc33-bcaec56cc4a7', 'onlyspare',
             _HOST, '3375', 1, 1, 1.0],
        ],
        'emptygroup': [],
    }

    sharding_information = {
        'shardtype.range': [
            ['shardtype', 'range', 'id', '1', '1',
             'RANGE', 'testgroup1', 'testglobalgroup'],
            ['shardtype', 'range', 'id', '21', '2',
             'RANGE', 'testgroup2', 'testglobalgroup'],
        ],
        'shardtype.hash': [
            ['shardtype', 'hash', 'name',
             '513772EE53011AD9F4DC374B2D34D0E9', '1',
             'HASH', 'testgroup1', 'testglobalgroup'],
            ['shardtype', 'hash', 'name',
             'F617868BD8C41043DC4BEBC7952C7024', '2',
             'HASH', 'testgroup2', 'testglobalgroup'],
        ],
        'shardtype.spam': [
            ['shardtype', 'spam', 'emp_no', '1', '1',
             'SPAM', 'testgroup1', 'testglobalgroup'],
            ['shardtype', 'spam', 'emp_no', '21', '2',
             'SPAM', 'testgroup2', 'testglobalgroup'],
        ],
    }

    @staticmethod
    def wrap_response(data):
        return (
            _MockupXMLProxy.fabric_uuid,
            _MockupXMLProxy.version_token,  # version
            _MockupXMLProxy.ttl,  # ttl
            data,
        )

    @property
    def server(self):
        class Server(object):
            @staticmethod
            def set_status(server_uuid, status):
                return server_uuid, status

        return Server()

    @property
    def threat(self):
        class Threat(object):
            @staticmethod
            def report_failure(server_uuid, reporter, status):
                return server_uuid, status

            @staticmethod
            def report_error(server_uuid, reporter, status):
                return server_uuid, status

        return Threat()

    @property
    def dump(self):
        class Dump(object):

            """Mocking Fabric dump commands"""

            @staticmethod
            def fabric_nodes():
                return _MockupXMLProxy.wrap_response(
                    _MockupXMLProxy.fabric_servers)

            @staticmethod
            def servers(version, patterns):
                groups = patterns.split(',')
                data = []
                for group in groups:
                    for server in _MockupXMLProxy.groups[group]:
                        data.append(server)
                return _MockupXMLProxy.wrap_response(data)

            @staticmethod
            def sharding_information(version, patterns):
                tables = patterns.split(',')
                shards = _MockupXMLProxy.sharding_information
                data = []
                for table in tables:
                    try:
                        data.extend(shards[table])
                    except KeyError:
                        pass
                return _MockupXMLProxy.wrap_response(data)

        return Dump()

    def __init__(self, *args, **kwargs):
        """Initializing"""
        self._uri = kwargs.get('uri', None)
        self._allow_none = kwargs.get('allow_none', None)

    @staticmethod
    def _some_nonexisting_method():
        """A non-existing method raising Fault"""
        raise Fault(0, 'Testing')


class _MockupFabric(fabric.Fabric):
    """Mock-up of fabric.Fabric

    This class is similar to fabric.Fabric except that it does not
    create a connection with MySQL Fabric. It is used to be able to
    unit tests without the need of having to run a complete Fabric
    setup.
    """

    _cnx_class = None

    def seed(self, host=None, port=None):
        if _HOST in (host, self._init_host):
            self._cnx_class = _MockupFabricConnection
        super(_MockupFabric, self).seed(host, port)


class _MockupFabricConnection(fabric.FabricConnection):
    """Mock-up of fabric.FabricConnection"""

    def _xmlrpc_get_proxy(self):
        return _MockupXMLProxy()


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
            'MYSQL_FABRIC_PORT': 32274,
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
            'key': ((int, str), "Sharding key", None),
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
        data = ('example.com', _PORT)
        exp = 'http://{host}:{port}'.format(host=data[0], port=data[1])
        self.assertEqual(exp, connection._fabric_xmlrpc_uri(*data))

    def test__fabric_server_uuid(self):
        data = ('example.com', _PORT)
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


class FabricTests(tests.MySQLConnectorTests):
    """Testing mysql.connector.fabric.Fabric class"""

    def setUp(self):
        self._orig_fabric_connection_class = connection.FabricConnection
        self._orig_fabric_servers = _MockupXMLProxy.fabric_servers

        connection.FabricConnection = _MockupFabricConnection

    def tearDown(self):
        connection.FabricConnection = self._orig_fabric_connection_class
        _MockupXMLProxy.fabric_servers = self._orig_fabric_servers

    def test___init__(self):
        fab = fabric.Fabric(_HOST, port=_PORT)
        attrs = {
            '_fabric_instances': {},
            '_fabric_uuid': None,
            '_ttl': 1 * 60,
            '_version_token': None,
            '_connect_attempts': connection._CNX_ATTEMPT_MAX,
            '_connect_delay': connection._CNX_ATTEMPT_DELAY,
            '_cache': None,
            '_group_balancers': {},
            '_init_host': _HOST,
            '_init_port': _PORT,
            '_ssl': None,
            '_username': None,
            '_password': None,
            '_report_errors': False,
        }

        for attr, default in attrs.items():
            if attr in ('_cache', '_fabric_instances'):
                # Tested later
                continue
            try:
                self.assertEqual(default, getattr(fab, attr))
            except AttributeError:
                self.fail("Fabric instance has no attribute '{0}'".format(
                    attr))

        self.assertTrue(isinstance(fab._cache, caching.FabricCache))
        self.assertEqual(fab._fabric_instances, {})

        # SSL
        exp = {
            'ca': '/path/to/ca',
            'key': '/path/to/key',
            'cert': '/path/to/cert',
        }
        fab = fabric.Fabric(_HOST, port=_PORT,
                            ssl_ca=exp['ca'], ssl_cert=exp['cert'],
                            ssl_key=exp['key'])

        self.assertEqual(exp, fab._ssl)

        # Check user/username
        self.assertRaises(ValueError, fabric.Fabric, _HOST, username='ham',
                          user='spam')
        fab = fabric.Fabric(_HOST, username='spam')
        self.assertEqual('spam', fab._username)
        fab = fabric.Fabric(_HOST, user='ham')
        self.assertEqual('ham', fab._username)


    def test_seed(self):
        fab = _MockupFabric(_HOST, _PORT)

        # Empty server list results in InterfaceError
        _MockupXMLProxy.fabric_servers = None
        self.assertRaises(errors.InterfaceError, fab.seed)
        _MockupXMLProxy.fabric_servers = self._orig_fabric_servers

        exp_server_uuid = uuid.UUID(_MockupXMLProxy.fabric_uuid)
        exp_version = _MockupXMLProxy.version_token
        exp_ttl = _MockupXMLProxy.ttl
        fabrics = [
            {'host': _HOST, 'port': _PORT}
        ]

        # Normal operations
        fab.seed()
        self.assertEqual(exp_server_uuid, fab._fabric_uuid)
        self.assertEqual(exp_version, fab._version_token)
        self.assertEqual(exp_ttl, fab._ttl)

        exp_fabinst_uuid = connection._fabric_server_uuid(
            fabrics[0]['host'], fabrics[0]['port'])

        self.assertTrue(exp_fabinst_uuid in fab._fabric_instances)
        fabinst = fab._fabric_instances[exp_fabinst_uuid]
        self.assertEqual(fabrics[0]['host'], fabinst.host)
        self.assertEqual(fabrics[0]['port'], fabinst.port)

        # Don't change anything when version did not change
        exp_ttl = 10
        fab.seed()
        self.assertNotEqual(exp_ttl, fab._ttl)

    def test_reset_cache(self):
        class FabricNoServersLookup(_MockupFabric):
            def get_group_servers(self, group, use_cache=True):
                self.test_group = group

        fab = FabricNoServersLookup(_HOST)
        first_cache = fab._cache
        fab.reset_cache()
        self.assertNotEqual(first_cache, fab._cache)

        exp = 'testgroup'
        fab.reset_cache(exp)
        self.assertEqual(exp, fab.test_group)

    def test_get_instance(self):
        fab = _MockupFabric(_HOST, _PORT)
        self.assertRaises(errors.InterfaceError, fab.get_instance)

        fab.seed()
        if sys.version_info[0] == 2:
            instance_list = fab._fabric_instances.keys()
            exp = fab._fabric_instances[instance_list[0]]
        else:
            exp = fab._fabric_instances[list(fab._fabric_instances)[0]]
        self.assertEqual(exp, fab.get_instance())

    def test_report_failure(self):
        fab = _MockupFabric(_HOST, _PORT)

        fabinst = connection.FabricConnection(fab, _HOST, _PORT)
        fabinst._proxy = _MockupXMLProxy()
        fab._fabric_instances[fabinst.uuid] = fabinst

        fab.report_failure(uuid.uuid4(), connection.REPORT_ERRORS[0])

    def test_get_fabric_servers(self):
        fab = _MockupFabric(_HOST, _PORT)
        fab.seed()

        exp = (
            uuid.UUID('{' + _MockupXMLProxy.fabric_uuid + '}'),
            _MockupXMLProxy.version_token,
            _MockupXMLProxy.ttl,
            [{'host': _HOST, 'port': _PORT}]
        )

        self.assertEqual(exp, fab.get_fabric_servers())

        # No instances available
        fabinst = _MockupFabricConnection(fab, _HOST, _PORT)
        fab._fabric_instances = {}
        self.assertRaises(errors.InterfaceError,
                          fab.get_fabric_servers)

        fabinst.connect()
        self.assertEqual(exp, fab.get_fabric_servers(fabinst))

        fab.seed()

    def test_get_group_servers(self):
        fab = _MockupFabric(_HOST, _PORT)
        fab.seed()

        exp = [
            # Secondary
            fabric.FabricMySQLServer(
                uuid='a6ac2895-574f-11e3-bc32-bcaec56cc4a7',
                group='testgroup1',
                host='tests.example.com', port=3372,
                mode=1, status=2, weight=1.0),
            # Primary
            fabric.FabricMySQLServer(
                uuid='af1cb1e4-574f-11e3-bc33-bcaec56cc4a7',
                group='testgroup1',
                host='tests.example.com', port=3373,
                mode=3, status=3, weight=1.0),
        ]

        self.assertEqual(exp, fab.get_group_servers('testgroup1'))
        self.assertEqual(exp,
                         fab.get_group_servers('testgroup1', use_cache=False))

        exp_balancers = {
            'testgroup1': balancing.WeightedRoundRobin(
                (exp[0].uuid, exp[0].weight))
        }
        self.assertEqual(exp_balancers, fab._group_balancers)

        # No instances available, checking cache
        fab._fabric_instances = {}
        fab.get_group_servers('testgroup1')
        self.assertEqual(exp, fab.get_group_servers('testgroup1'))

        # Force lookup
        self.assertRaises(errors.InterfaceError,
                          fab.get_group_servers, 'testgroup1', use_cache=False)

    def test_get_group_server(self):
        fab = _MockupFabric(_HOST, _PORT)
        fab.seed()

        self.assertRaises(ValueError, fab.get_group_server,
                          'testgroup1', mode=1, status=1)

        self.assertRaises(errors.InterfaceError, fab.get_group_server,
                          'emptygroup')

        # Request PRIMARY (master)
        exp = fab.get_group_servers('testgroup1')[1]
        self.assertEqual(
            exp,
            fab.get_group_server('testgroup1', status=fabric.STATUS_PRIMARY)
        )
        self.assertEqual(
            exp,
            fab.get_group_server('testgroup1', mode=fabric.MODE_READWRITE)
        )

        # Request PRIMARY, but non available
        self.assertRaises(errors.InterfaceError,
                          fab.get_group_server,
                          'onlysecondary', status=fabric.STATUS_PRIMARY)
        self.assertRaises(errors.InterfaceError,
                          fab.get_group_server,
                          'onlysecondary', mode=fabric.MODE_READWRITE)

        # Request SECONDARY, but non available, returns primary
        exp = fab.get_group_servers('onlyprimary')[0]
        self.assertEqual(
            exp,
            fab.get_group_server('onlyprimary', mode=fabric.MODE_READONLY)
        )

        # Request SECONDARY
        exp = fab.get_group_servers('testgroup1')[0]
        self.assertEqual(
            exp,
            fab.get_group_server('testgroup1', status=fabric.STATUS_SECONDARY)
        )
        self.assertEqual(
            exp,
            fab.get_group_server('testgroup1', mode=fabric.MODE_READONLY)
        )

        # No Primary or Secondary
        self.assertRaises(errors.InterfaceError,
                          fab.get_group_server, 'onlyspare',
                          status=fabric.STATUS_SECONDARY)

    def test_get_sharding_information(self):
        fab = _MockupFabric(_HOST, _PORT)
        fab.seed()

        self.assertRaises(ValueError, fab.get_sharding_information,
                          'notlist')

        table = ('range', 'shardtype')  # table name, database name

        exp = {
            1: {'group': 'testgroup1'},
            21: {'group': 'testgroup2'}
        }

        fab.get_sharding_information([table])
        entry = fab._cache.sharding_search(table[1], table[0])
        self.assertEqual(exp, entry.partitioning)

        fab.get_sharding_information([table[0]], 'shardtype')
        entry = fab._cache.sharding_search(table[1], table[0])
        self.assertEqual(exp, entry.partitioning)

    def test_get_shard_server(self):
        fab = _MockupFabric(_HOST, _PORT)
        fab.seed()

        self.assertRaises(ValueError, fab.get_shard_server, 'notlist', 1)
        self.assertRaises(ValueError, fab.get_shard_server, ['not_list'], 1)

        exp_local = [
            # Secondary
            fabric.FabricMySQLServer(
                uuid='a6ac2895-574f-11e3-bc32-bcaec56cc4a7',
                group='testgroup1',
                host='tests.example.com', port=3372,
                mode=1, status=2, weight=1.0),
            # Primary
            fabric.FabricMySQLServer(
                uuid='af1cb1e4-574f-11e3-bc33-bcaec56cc4a7',
                group='testgroup1',
                host='tests.example.com', port=3373,
                mode=3, status=3, weight=1.0),
        ]

        exp_global = [
            fabric.FabricMySQLServer(
                uuid='91f7090e-574f-11e3-bc32-bcaec56cc4a7',
                group='testglobalgroup',
                host='tests.example.com', port=3370,
                mode=3, status=3, weight=1.0),
            fabric.FabricMySQLServer(
                uuid='9c09932d-574f-11e3-bc32-bcaec56cc4a7',
                group='testglobalgroup',
                host='tests.example.com', port=3371,
                mode=1, status=2, weight=1.0),
        ]

        # scope=SCOPE_LOCAL, mode=None
        self.assertEqual(
            exp_local[0],
            fab.get_shard_server(['shardtype.range'], 1)
        )

        # scope=SCOPE_GLOBAL, read-only and read-write
        self.assertEqual(
            exp_global[0],
            fab.get_shard_server(['shardtype.range'], 1,
                                 scope=fabric.SCOPE_GLOBAL,
                                 mode=fabric.MODE_READWRITE)
        )
        self.assertEqual(
            exp_global[1],
            fab.get_shard_server(['shardtype.range'], 1,
                                 scope=fabric.SCOPE_GLOBAL,
                                 mode=fabric.MODE_READONLY)
        )

        self.assertRaises(errors.InterfaceError,
                          fab.get_shard_server, ['shardtype.spam'], 1)

        self.assertRaises(errors.DatabaseError,
                          fab.get_shard_server, ['shartype.unknowntable'], 1)


class FabricConnectionTests(tests.MySQLConnectorTests):
    """Testing mysql.connector.fabric.FabricConnection class"""

    def setUp(self):
        self.fab = connection.Fabric(_HOST, port=_PORT)
        self.fabcnx = connection.FabricConnection(self.fab, _HOST, port=_PORT)

    def tearDown(self):
        connection.ServerProxy = ServerProxy

    def test___init___(self):
        self.assertRaises(ValueError,
                          connection.FabricConnection, None, _HOST, port=_PORT)

        attrs = {
            '_fabric': self.fab,
            '_host': _HOST,
            '_port': _PORT,
            '_proxy': None,
            '_connect_attempts': connection._CNX_ATTEMPT_MAX,
            '_connect_delay': connection._CNX_ATTEMPT_DELAY,
        }

        for attr, default in attrs.items():
            try:
                self.assertEqual(default, getattr(self.fabcnx, attr))
            except AttributeError:
                self.fail("FabricConnection instance has no "
                          "attribute '{0}'".format(attr))

    def test_host(self):
        self.assertEqual(_HOST, self.fabcnx.host)

    def test_port(self):
        fabcnx = connection.FabricConnection(self.fab, _HOST, port=_PORT)
        self.assertEqual(_PORT, self.fabcnx.port)

    def test_uri(self):
        self.assertEqual(connection._fabric_xmlrpc_uri(_HOST, _PORT),
                         self.fabcnx.uri)

    def test_proxy(self):
        # We did not yet connect
        self.assertEqual(None, self.fabcnx.proxy)

    def test__xmlrpc_get_proxy(self):
        # Try connection, which fails
        self.fabcnx._connect_attempts = 1  # Make it fail quicker
        self.assertRaises(errors.InterfaceError,
                          self.fabcnx._xmlrpc_get_proxy)

        # Using mock-up
        connection.ServerProxy = _MockupXMLProxy
        self.assertTrue(isinstance(self.fabcnx._xmlrpc_get_proxy(),
                                   _MockupXMLProxy))

    def test_connect(self):
        # Try connection, which fails
        self.fabcnx._connect_attempts = 1  # Make it fail quicker
        self.assertRaises(errors.InterfaceError, self.fabcnx.connect)

        # Using mock-up
        connection.ServerProxy = _MockupXMLProxy
        self.fabcnx.connect()
        self.assertTrue(isinstance(self.fabcnx.proxy, _MockupXMLProxy))

    def test_is_connected(self):
        self.assertFalse(self.fabcnx.is_connected)
        self.fabcnx._proxy = 'spam'
        self.assertFalse(self.fabcnx.is_connected)

        # Using mock-up
        connection.ServerProxy = _MockupXMLProxy
        self.fabcnx.connect()
        self.assertTrue(self.fabcnx.is_connected)


class MySQLFabricConnectionTests(tests.MySQLConnectorTests):
    """Testing mysql.connector.fabric.FabricConnection class"""

    def setUp(self):
        # Mock-up: we don't actually connect to Fabric
        connection.ServerProxy = _MockupXMLProxy
        self.fabric_config = {
            'host': _HOST,
            'port': _PORT,
        }
        config = {'fabric': self.fabric_config}
        self.cnx = connection.MySQLFabricConnection(**config)

    def tearDown(self):
        connection.ServerProxy = ServerProxy

    def _get_default_properties(self):
        result = {}
        for key, attr in connection._CNX_PROPERTIES.items():
            result[key] = attr[2]
        return result

    def test___init__(self):
        # Missing 'fabric' argument
        self.assertRaises(ValueError,
                          connection.MySQLFabricConnection)

        attrs = {
            '_mysql_cnx': None,
            '_fabric': None,
            '_fabric_mysql_server': None,
            '_mysql_config': {},
            '_cnx_properties': {},
        }

        for attr, default in attrs.items():
            if attr in ('_cnx_properties', '_fabric'):
                continue
            try:
                self.assertEqual(default, getattr(self.cnx, attr),
                                 "Wrong init for {0}".format(attr))
            except AttributeError:
                self.fail("MySQLFabricConnection instance has no "
                          "attribute '{0}'".format(attr))

        self.assertEqual(self._get_default_properties(),
                         self.cnx._cnx_properties)

    def test___getattr__(self):
        none_supported_attrs = [
            'cmd_refresh',
            'cmd_quit',
            'cmd_shutdown',
            'cmd_statistics',
            'cmd_process_info',
            'cmd_process_kill',
            'cmd_debug',
            'cmd_ping',
            'cmd_change_user',
            'cmd_stmt_prepare',
            'cmd_stmt_execute',
            'cmd_stmt_close',
            'cmd_stmt_send_long_data',
            'cmd_stmt_reset',
        ]
        for attr in none_supported_attrs:
            self.assertRaises(errors.NotSupportedError,
                              getattr, self.cnx, attr)

    def test_fabric_uuid(self):
        self.cnx._fabric_mysql_server = fabric.FabricMySQLServer(
            uuid='af1cb1e4-574f-11e3-bc33-bcaec56cc4a7',
            group='testgroup1',
            host='tests.example.com', port=3373,
            mode=3, status=3, weight=1.0
        )
        exp = 'af1cb1e4-574f-11e3-bc33-bcaec56cc4a7'
        self.assertEqual(exp, self.cnx.fabric_uuid)

    def test_properties(self):
        self.assertEqual(self.cnx._cnx_properties, self.cnx.properties)

    def test_reset_cache(self):
        self.cnx._fabric._cache.cache_group('spam', None)
        self.cnx.reset_cache()
        self.assertEqual({}, self.cnx._fabric._cache._groups)

    def test_is_connected(self):
        self.assertFalse(self.cnx.is_connected())
        self.cnx._mysql_cnx = 'spam'
        self.assertTrue(self.cnx.is_connected())

    def test_reset_properties(self):
        exp = self.cnx._cnx_properties
        self.cnx._cnx_properties = {'spam': 'ham'}
        self.cnx.reset_properties()
        self.assertEqual(exp, self.cnx._cnx_properties)
        self.assertEqual(connection._GETCNX_ATTEMPT_DELAY,
                         self.cnx._cnx_properties['attempt_delay'])
        self.assertEqual(connection._GETCNX_ATTEMPT_MAX,
                         self.cnx._cnx_properties['attempts'])

    def test_set_property__errors(self):
        self.assertRaises(ValueError, self.cnx.set_property, unknown='Spam')

        # Can't use 'group' when 'key' was set
        self.cnx._cnx_properties = {'key': 42}
        self.assertRaises(ValueError, self.cnx.set_property, group='spam')
        self.cnx.reset_properties()

        # Can't use 'key' when 'group' was set
        self.cnx._cnx_properties = {'group': 'ham'}
        self.assertRaises(ValueError, self.cnx.set_property, key=42)
        self.cnx.reset_properties()

        # Invalid scope and mode
        self.assertRaises(ValueError, self.cnx.set_property, scope='SPAM')
        self.assertRaises(ValueError, self.cnx.set_property, mode=99999)

        # Invalid types
        self.assertRaises(TypeError, self.cnx.set_property, mode='SPAM')
        self.assertRaises(TypeError, self.cnx.set_property, tables='SPAM')
        self.assertRaises(TypeError, self.cnx.set_property, key=('1',))

    def test_set_property(self):
        self.cnx._cnx_properties = {'key': 42}
        self.cnx.set_property(key=None)
        self.assertEqual(None, self.cnx._cnx_properties['key'])
        self.cnx.reset_properties()

        exp = 'ham'
        self.cnx.set_property(group='ham')
        self.assertEqual(exp, self.cnx._cnx_properties['group'])

        self.cnx.set_property(attempts=None)
        self.assertEqual(connection._GETCNX_ATTEMPT_MAX,
                         self.cnx._cnx_properties['attempts'])


class FabricConnectorPythonTests(tests.MySQLConnectorTests):

    """Testing mysql.connector.connect()"""

    def setUp(self):
        # Mock-up: we don't actually connect to Fabric
        connection.ServerProxy = _MockupXMLProxy
        self.fabric_config = {
            'host': _HOST,
            'port': _PORT,
        }
        self.config = {'fabric': self.fabric_config}

    def tearDown(self):
        connection.ServerProxy = ServerProxy

    def test_connect(self):
        self.assertTrue(isinstance(
            mysql.connector.connect(**self.config),
            connection.MySQLFabricConnection
        ))


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
