# Copyright (c) 2013, 2018, Oracle and/or its affiliates. All rights reserved.
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

"""Unittests
"""

import os
import sys
import re
import socket
import datetime
import inspect
import platform
import unittest
import logging
import shutil
import subprocess
import errno
import traceback
from imp import load_source
from functools import wraps
from pkgutil import walk_packages

LOGGER_NAME = "myconnpy_tests"
LOGGER = logging.getLogger(LOGGER_NAME)
PY2 = sys.version_info[0] == 2
_CACHED_TESTCASES = []

try:
    from unittest.util import strclass
except ImportError:
    # Python v2
    from unittest import _strclass as strclass  # pylint: disable=E0611

try:
    from unittest.case import SkipTest
except ImportError:
    if sys.version_info[0:2] == (3, 1):
        from unittest import SkipTest
    elif sys.version_info[0:2] == (2, 6):
        # Support skipping tests for Python v2.6
        from tests.py26 import test_skip, test_skip_if, SkipTest
        unittest.skip = test_skip
        unittest.skipIf = test_skip_if
    else:
        LOGGER.error("Could not initialize Python's unittest module")
        sys.exit(1)

from lib.cpy_distutils import get_mysql_config_info

SSL_AVAILABLE = True
try:
    import ssl
except ImportError:
    SSL_AVAILABLE = False

# Note that IPv6 support for Python is checked here, but it can be disabled
# when the bind_address of MySQL was not set to '::1'.
IPV6_AVAILABLE = socket.has_ipv6

OLD_UNITTEST = sys.version_info[0:2] in [(2, 6)]

if os.name == 'nt':
    WINDOWS_VERSION = platform.win32_ver()[1]
    WINDOWS_VERSION_INFO = [0] * 2
    for i, value in enumerate(WINDOWS_VERSION.split('.')[0:2]):
        WINDOWS_VERSION_INFO[i] = int(value)
    WINDOWS_VERSION_INFO = tuple(WINDOWS_VERSION_INFO)
else:
    WINDOWS_VERSION = None
    WINDOWS_VERSION_INFO = ()

# Following dictionary holds messages which were added by test cases
# but only logged at the end.
MESSAGES = {
    'WARNINGS': [],
    'INFO': [],
    'SKIPPED': [],
}

OPTIONS_INIT = False

MYSQL_SERVERS_NEEDED = 1
MYSQL_SERVERS = []
MYSQL_VERSION = ()
MYSQL_VERSION_TXT = ''
MYSQL_DUMMY = None
MYSQL_DUMMY_THREAD = None
SSL_DIR = os.path.join('tests', 'data', 'ssl')
SSL_CA = os.path.abspath(os.path.join(SSL_DIR, 'tests_CA_cert.pem'))
SSL_CERT = os.path.abspath(os.path.join(SSL_DIR, 'tests_client_cert.pem'))
SSL_KEY = os.path.abspath(os.path.join(SSL_DIR, 'tests_client_key.pem'))
TEST_BUILD_DIR = None
MYSQL_CAPI = None

DJANGO_VERSION = None

__all__ = [
    'MySQLConnectorTests',
    'MySQLxTests',
    'get_test_names', 'printmsg',
    'LOGGER_NAME',
    'DummySocket',
    'SSL_DIR',
    'get_test_modules',
    'MESSAGES',
    'setup_logger',
    'install_connector',
    'TEST_BUILD_DIR',
]


class DummySocket(object):

    """Dummy socket class

    This class helps to test socket connection without actually making any
    network activity. It is a proxy class using socket.socket.
    """

    def __init__(self, *args):
        self._socket = socket.socket(*args)
        self._server_replies = bytearray(b'')
        self._client_sends = []
        self._raise_socket_error = 0

    def __getattr__(self, attr):
        return getattr(self._socket, attr)

    def raise_socket_error(self, err=errno.EPERM):
        self._raise_socket_error = err

    def recv(self, bufsize=4096, flags=0):
        if self._raise_socket_error:
            raise socket.error(self._raise_socket_error)
        res = self._server_replies[0:bufsize]
        self._server_replies = self._server_replies[bufsize:]
        return res

    def recv_into(self, buffer_, nbytes=0, flags=0):
        if self._raise_socket_error:
            raise socket.error(self._raise_socket_error)
        if nbytes == 0:
            nbytes = len(buffer_)
        try:
            buffer_[0:nbytes] = self._server_replies[0:nbytes]
        except (IndexError, TypeError) as err:
            return 0
        except ValueError:
            pass
        self._server_replies = self._server_replies[nbytes:]
        return len(buffer_)

    def send(self, string, flags=0):
        if self._raise_socket_error:
            raise socket.error(self._raise_socket_error)
        self._client_sends.append(bytearray(string))
        return len(string)

    def sendall(self, string, flags=0):
        self._client_sends.append(bytearray(string))
        return None

    def add_packet(self, packet):
        self._server_replies += packet

    def add_packets(self, packets):
        for packet in packets:
            self._server_replies += packet

    def reset(self):
        self._raise_socket_error = 0
        self._server_replies = bytearray(b'')
        self._client_sends = []

    def get_address(self):
        return 'dummy'


def get_test_modules():
    """Get list of Python modules containing tests

    This function scans the tests/ folder for Python modules which name
    start with 'test_'. It will return the dotted name of the module with
    submodules together with the first line of the doc string found in
    the module.

    The result is a sorted list of tuples and each tuple is
        (name, module_dotted_path, description)

    For example:
        ('cext_connection', 'tests.cext.cext_connection', 'This module..')

    Returns a list of tuples.
    """
    global _CACHED_TESTCASES

    if _CACHED_TESTCASES:
        return _CACHED_TESTCASES
    testcases = []

    pattern = re.compile('.*test_(.*)')
    for finder, name, is_pkg in walk_packages(__path__, prefix=__name__+'.'):
        if ('.test_' not in name or
                ('django' in name and not DJANGO_VERSION) or
                ('cext' in name and not MYSQL_CAPI)):
            continue

        module_path = os.path.join(finder.path, name.split('.')[-1] + '.py')
        dsc = '(description not available)'
        try:
            mod = load_source(name, module_path)
        except IOError as exc:
            # Not Python source files
            continue
        except ImportError as exc:
            check_c_extension(exc)
        else:
            try:
                dsc = mod.__doc__.splitlines()[0]
            except AttributeError:
                # No description available
                pass

        testcases.append((pattern.match(name).group(1), name, dsc))

    testcases.sort(key=lambda x: x[0], reverse=False)

    # 'Unimport' modules so they can be correctly imported when tests run
    for _, module, _ in testcases:
        sys.modules.pop(module, None)

    _CACHED_TESTCASES = testcases
    return testcases


def get_test_names():
    """Get test names

    This functions gets the names of Python modules containing tests. The
    name is parsed from files prefixed with 'test_'. For example,
    'test_cursor.py' has name 'cursor'.

    Returns a list of strings.
    """
    pattern = re.compile('.*test_(.*)')
    return [mod[0] for mod in get_test_modules()]


def set_nr_mysql_servers(number):
    """Set the number of MySQL servers needed

    This functions sets how much MySQL servers are needed for running the
    unit tests. The number argument should be a integer between 1 and
    16 (16 being the hard limit).

    The set_nr_mysql_servers() function is used in test modules, usually at
    the very top (after imports).

    Raises AttributeError on errors.
    """
    global MYSQL_SERVERS_NEEDED  # pylint: disable=W0603
    if not isinstance(number, int) or (number < 1 or number > 16):
        raise AttributeError(
            "number of MySQL servers should be a value between 1 and 16")
    if number > MYSQL_SERVERS_NEEDED:
        MYSQL_SERVERS_NEEDED = number


def fake_hostname():
    """Return a fake hostname

    This function returns a string which can be used in the creation of
    fake hostname. Note that we do not add a domain name.

    Returns a string.
    """
    if PY2:
        return ''.join(["%02x" % ord(c) for c in os.urandom(4)])
    else:
        return ''.join(["%02x" % c for c in os.urandom(4)])


def get_mysqlx_config(name=None, index=None):
    """Get MySQLx enabled server configuration for running MySQL server

    If no name is given, then we will return the configuration of the
    first added.
    """
    if not name and not index:
        return MYSQL_SERVERS[0].xplugin_config.copy()

    if name:
        for server in MYSQL_SERVERS:
            if server.name == name:
                return server.xplugin_config.copy()
    elif index:
        return MYSQL_SERVERS[index].xplugin_config.copy()

    return None


def get_mysql_config(name=None, index=None):
    """Get MySQL server configuration for running MySQL server

    If no name is given, then we will return the configuration of the
    first added.
    """
    if not name and not index:
        return MYSQL_SERVERS[0].client_config.copy()

    if name:
        for server in MYSQL_SERVERS:
            if server.name == name:
                return server.client_config.copy()
    elif index:
        return MYSQL_SERVERS[index].client_config.copy()

    return None


def have_engine(cnx, engine):
    """Check support for given storage engine

    This function checks if the MySQL server accessed through cnx has
    support for the storage engine.

    Returns True or False.
    """
    have = False
    engine = engine.lower()

    cur = cnx.cursor()
    # Should use INFORMATION_SCHEMA, but play nice with v4.1
    cur.execute("SHOW ENGINES")
    rows = cur.fetchall()
    for row in rows:
        if row[0].lower() == engine:
            if row[1].lower() == 'yes':
                have = True
            break

    cur.close()
    return have


def cmp_result(result1, result2):
    """Compare results (list of tuples) coming from MySQL

    For certain results, like SHOW VARIABLES or SHOW WARNINGS, the
    order is unpredictable. To check if what is expected in the
    tests, we need to compare each row.

    Returns True or False.
    """
    try:
        if len(result1) != len(result2):
            return False

        for row in result1:
            if row not in result2:
                return False
    except:
        return False

    return True


class UTCTimeZone(datetime.tzinfo):

    """UTC"""

    def __init__(self):
        pass

    def utcoffset(self, dt):
        return datetime.timedelta(0)

    def dst(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        return 'UTC'


class TestTimeZone(datetime.tzinfo):

    """Test time zone"""

    def __init__(self, hours=0):
        self._offset = datetime.timedelta(hours=hours)

    def utcoffset(self, dt):
        return self._offset

    def dst(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        return 'TestZone'


def cnx_config(**extra_config):
    def _cnx_config(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not hasattr(self, 'config'):
                self.config = get_mysql_config()
            if extra_config:
                for key, value in extra_config.items():
                    self.config[key] = value
            func(self, *args, **kwargs)
        return wrapper
    return _cnx_config


def foreach_cnx(*cnx_classes, **extra_config):
    def _use_cnx(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not hasattr(self, 'config'):
                self.config = get_mysql_config()
            if extra_config:
                for key, value in extra_config.items():
                    self.config[key] = value
            for cnx_class in cnx_classes or self.all_cnx_classes:
                try:
                    self.cnx = cnx_class(**self.config)
                    self._testMethodName = "{0} (using {1})".format(
                        func.__name__, cnx_class.__name__)
                except Exception as exc:
                    if hasattr(self, 'cnx'):
                        # We will rollback/close later
                        pass
                    else:
                        traceback.print_exc(file=sys.stdout)
                        raise exc
                try:
                    func(self, *args, **kwargs)
                except Exception as exc:
                    traceback.print_exc(file=sys.stdout)
                    raise exc
                finally:
                    try:
                        self.cnx.rollback()
                        self.cnx.close()
                    except:
                        # Might already be closed.
                        pass
        return wrapper
    return _use_cnx


class MySQLConnectorTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        from mysql.connector import connection
        self.all_cnx_classes = [connection.MySQLConnection]
        self.maxDiff = 64
        try:
            import _mysql_connector
            from mysql.connector import connection_cext
        except ImportError:
            self.have_cext = False
        else:
            self.have_cext = True
            self.all_cnx_classes.append(connection_cext.CMySQLConnection)
        super(MySQLConnectorTests, self).__init__(methodName=methodName)

    def __str__(self):
        classname = strclass(self.__class__)
        return "{classname}.{method}".format(
            method=self._testMethodName,
            classname=re.sub(r"tests\d*.test_", "", classname)
        )

    def check_attr(self, obj, attrname, default):
        cls_name = obj.__class__.__name__
        self.assertTrue(
            hasattr(obj, attrname),
            "{name} object has no '{attr}' attribute".format(
                name=cls_name, attr=attrname))
        self.assertEqual(
            default,
            getattr(obj, attrname),
            "{name} object's '{attr}' should "
            "default to {type_} '{default}'".format(
                name=cls_name,
                attr=attrname,
                type_=type(default).__name__,
                default=default))

    def check_method(self, obj, method):
        cls_name = obj.__class__.__name__
        self.assertTrue(
            hasattr(obj, method),
            "{0} object has no '{1}' method".format(cls_name, method))
        self.assertTrue(
            inspect.ismethod(getattr(obj, method)),
            "{0} object defines {1}, but is not a method".format(
                cls_name, method))

    def check_args(self, function, supported_arguments):
        argspec = inspect.getargspec(function)
        function_arguments = dict(zip(argspec[0][1:], argspec[3]))
        for argument, default in function_arguments.items():
            try:
                self.assertEqual(
                    supported_arguments[argument],
                    default,
                    msg="Argument '{0}' has wrong default".format(argument))
            except KeyError:
                self.fail("Found unsupported or new argument '%s'" % argument)
        for argument, default in supported_arguments.items():
            if not argument in function_arguments:
                self.fail("Supported argument '{0}' fails".format(argument))

    if sys.version_info[0:2] >= (3, 4):
        def _addSkip(self, result, test_case, reason):
            add_skip = getattr(result, 'addSkip', None)
            if add_skip:
                add_skip(test_case, self._testMethodName + ': ' + reason)
    else:
        def _addSkip(self, result, reason):
            add_skip = getattr(result, 'addSkip', None)
            if add_skip:
                add_skip(self, self._testMethodName + ': ' + reason)

    if sys.version_info[0:2] == (2, 6):
        # Backport handy asserts from 2.7
        def assertIsInstance(self, obj, cls, msg=None):
            if not isinstance(obj, cls):
                msg = "{0} is not an instance of {1}".format(
                    unittest.util.safe_repr(obj), unittest.util.repr(cls))
                self.fail(self._formatMessage(msg, msg))

        def assertGreater(self, a, b, msg=None):
            if not a > b:
                msg = "{0} not greater than {1}".format(
                    unittest.util.safe_repr(a), unittest.util.safe_repr(b))
                self.fail(self._formatMessage(msg, msg))

    def run(self, result=None):
        if sys.version_info[0:2] == (2, 6):
            test_method = getattr(self, self._testMethodName)
            if (getattr(self.__class__, "__unittest_skip__", False) or
                    getattr(test_method, "__unittest_skip__", False)):
                # We skipped a class
                try:
                    why = (
                        getattr(self.__class__, '__unittest_skip_why__', '')
                        or
                        getattr(test_method, '__unittest_skip_why__', '')
                    )
                    self._addSkip(result, why)
                finally:
                    result.stopTest(self)
                return

        if PY2:
            return super(MySQLConnectorTests, self).run(result)
        else:
            return super().run(result)

    def check_namedtuple(self, tocheck, attrs):
        for attr in attrs:
            try:
                getattr(tocheck, attr)
            except AttributeError:
                self.fail("Attribute '{0}' not part of namedtuple {1}".format(
                    attr, tocheck))


class TestsCursor(MySQLConnectorTests):

    def _test_execute_setup(self, cnx, tbl="myconnpy_cursor", engine="MyISAM"):

        self._test_execute_cleanup(cnx, tbl)
        stmt_create = (
            "CREATE TABLE {table} "
            "(col1 INT, col2 VARCHAR(30), PRIMARY KEY (col1))"
            "ENGINE={engine}").format(
            table=tbl, engine=engine)

        try:
            cur = cnx.cursor()
            cur.execute(stmt_create)
        except Exception as err:  # pylint: disable=W0703
            self.fail("Failed setting up test table; {0}".format(err))
        cur.close()

    def _test_execute_cleanup(self, cnx, tbl="myconnpy_cursor"):

        stmt_drop = "DROP TABLE IF EXISTS {table}".format(table=tbl)

        try:
            cur = cnx.cursor()
            cur.execute(stmt_drop)
        except Exception as err:  # pylint: disable=W0703
            self.fail("Failed cleaning up test table; {0}".format(err))
        cur.close()


class CMySQLConnectorTests(MySQLConnectorTests):

    def connc_connect_args(self, recache=False):
        """Get connection arguments for the MySQL C API

        Get the connection arguments suitable for the MySQL C API
        from the Connector/Python arguments. This method sets the member
        variable connc_kwargs as well as returning a copy of connc_kwargs.

        If recache is True, the information stored in connc_kwargs will
        be refreshed.

        :return: Dictionary containing connection arguments.
        :rtype: dict
        """
        self.config = get_mysql_config().copy()

        if not self.hasattr('connc_kwargs') or recache is True:
            connect_args = [
                "host", "user", "password", "database",
                "port", "unix_socket", "client_flags"
            ]
            self.connc_kwargs = {}
            for key, value in self.config.items():
                if key in connect_args:
                    self.connect_kwargs[key] = value
        return self.connc_kwargs.copy()


class CMySQLCursorTests(CMySQLConnectorTests):

    _cleanup_tables = []

    def setUp(self):
        self.config = get_mysql_config()
        # Import here allowed
        from mysql.connector.connection_cext import CMySQLConnection
        self.cnx = CMySQLConnection(**self.config)

    def tearDown(self):
        self.cleanup_tables(self.cnx)
        self.cnx.close()

    def setup_table(self, cnx, tbl="myconnpy_cursor", engine="InnoDB"):

        self.cleanup_table(cnx, tbl)
        stmt_create = (
            "CREATE TABLE {table} "
            "(col1 INT AUTO_INCREMENT, "
            "col2 VARCHAR(30), "
            "col3 INT NOT NULL DEFAULT 0, "
            "PRIMARY KEY (col1))"
            "ENGINE={engine}").format(
            table=tbl, engine=engine)

        try:
            cnx.cmd_query(stmt_create)
        except Exception as err:  # pylint: disable=W0703
            cnx.rollback()
            self.fail("Failed setting up test table; {0}".format(err))
        else:
            cnx.commit()

        self._cleanup_tables.append(tbl)

    def cleanup_table(self, cnx, tbl="myconnpy_cursor"):

        stmt_drop = "DROP TABLE IF EXISTS {table}".format(table=tbl)

        # Explicit rollback: uncommited changes could otherwise block
        cnx.rollback()

        try:
            cnx.cmd_query(stmt_drop)
        except Exception as err:  # pylint: disable=W0703
            self.fail("Failed cleaning up test table; {0}".format(err))

        if tbl in self._cleanup_tables:
            self._cleanup_tables.remove(tbl)

    def cleanup_tables(self, cnx):
        for tbl in self._cleanup_tables:
            self.cleanup_table(cnx, tbl)


class MySQLxTests(MySQLConnectorTests):

    def __init__(self, methodName="runTest"):
        super(MySQLxTests, self).__init__(methodName=methodName)

    def run(self, result=None):
        if sys.version_info[0:2] == (2, 6):
            test_method = getattr(self, self._testMethodName)
            if (getattr(self.__class__, "__unittest_skip__", False) or
                    getattr(test_method, "__unittest_skip__", False)):
                # We skipped a class
                try:
                    why = (
                        getattr(self.__class__, '__unittest_skip_why__', '')
                        or
                        getattr(test_method, '__unittest_skip_why__', '')
                    )
                    self._addSkip(result, why)
                finally:
                    result.stopTest(self)
                return

        if PY2:
            return super(MySQLxTests, self).run(result)
        else:
            return super().run(result)


def printmsg(msg=None):
    if msg is not None:
        print(msg)


class SkipTest(Exception):

    """Exception compatible with SkipTest of Python v2.7 and later"""


def _id(obj):
    """Function defined in unittest.case which is needed for decorators"""
    return obj


def test_skip(reason):
    """Skip test

    This decorator is used by Python v2.6 code to keep compatible with
    Python v2.7 (and later) unittest.skip.
    """
    def decorator(test):
        if not isinstance(test, (type, types.ClassType)):
            @wraps(test)
            def wrapper(*args, **kwargs):
                raise SkipTest(reason)
            test = wrapper

        test.__unittest_skip__ = True
        test.__unittest_skip_why__ = reason
        return test
    return decorator


def test_skip_if(condition, reason):
    """Skip test if condition is true

    This decorator is used by Python v2.6 code to keep compatible with
    Python v2.7 (and later) unittest.skipIf.
    """
    if condition:
        return test_skip(reason)
    return _id


def setup_logger(logger, debug=False, logfile=None):
    """Setting up the logger"""
    formatter = logging.Formatter(
        "%(asctime)s [%(name)s:%(levelname)s] %(message)s")
    handler = None
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    LOGGER.handlers = []  # We only need one handler
    LOGGER.addHandler(handler)


def install_connector(root_dir, install_dir, protobuf_include_dir,
                      protobuf_lib_dir, protoc, connc_location=None,
                      extra_compile_args=None, extra_link_args=None,
                      debug=False):
    """Install Connector/Python in working directory
    """
    logfile = 'myconnpy_install.log'
    LOGGER.info("Installing Connector/Python in {0}".format(install_dir))

    try:
        # clean up previous run
        if os.path.exists(logfile):
            os.unlink(logfile)
        shutil.rmtree(install_dir)
    except OSError:
        pass

    cmd = [
        sys.executable,
        'setup.py',
        'clean', '--all',  # necessary for removing the build/
    ]

    cmd.extend([
        'install',
        '--root', install_dir,
        '--install-lib', '.',
        '--static',
    ])

    if any((protobuf_include_dir, protobuf_lib_dir, protoc)):
        cmd.extend([
            '--with-protobuf-include-dir', protobuf_include_dir,
            '--with-protobuf-lib-dir', protobuf_lib_dir,
            '--with-protoc', protoc,
        ])

    if connc_location:
        cmd.extend(['--with-mysql-capi', connc_location])

    if extra_compile_args:
        cmd.extend(['--extra-compile-args', extra_compile_args])

    if extra_link_args:
        cmd.extend(['--extra-link-args', extra_link_args])

    prc = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                           stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                           cwd=root_dir)
    stdout = prc.communicate()[0]
    if prc.returncode is not 0:
        with open(logfile, 'wb') as logfp:
            logfp.write(stdout)
        LOGGER.error("Failed installing Connector/Python, see {log}".format(
            log=logfile))
        if debug:
            with open(logfile) as logfr:
                print(logfr.read())
        sys.exit(1)


def check_c_extension(exc=None):
    """Check whether we can load the C Extension

    This function needs the location of the mysql_config tool to
    figure out the location of the MySQL Connector/C libraries. On
    Windows it would be the installation location of Connector/C.

    :param mysql_config: Location of the mysql_config tool
    :param exc: An ImportError exception
    """
    if not MYSQL_CAPI:
        return

    if platform.system() == "Darwin":
        libpath_var = 'DYLD_LIBRARY_PATH'
    elif platform.system() == "Windows":
        libpath_var = 'PATH'
    else:
        libpath_var = 'LD_LIBRARY_PATH'

    if not os.path.exists(MYSQL_CAPI):
        LOGGER.error("MySQL Connector/C not available using '%s'", MYSQL_CAPI)

    if os.name == 'posix':
        if os.path.isdir(MYSQL_CAPI):
            mysql_config = os.path.join(MYSQL_CAPI, 'bin', 'mysql_config')
        else:
            mysql_config = MYSQL_CAPI
        lib_dir = get_mysql_config_info(mysql_config)['lib_dir']
    elif os.path.isdir(MYSQL_CAPI):
        lib_dir = os.path.join(MYSQL_CAPI, 'lib')
    else:
        LOGGER.error("C Extension not supported on %s", os.name)
        sys.exit(1)

    error_msg = ''
    if not exc:
        try:
            import _mysql_connector
        except ImportError as exc:
            error_msg = str(exc).strip()
    else:
        assert(isinstance(exc, ImportError))
        error_msg = str(exc).strip()

    if not error_msg:
        # Nothing to do
        return

    match = re.match('.*Library not loaded:\s(.+)\n.*', error_msg)
    if match:
        lib_name = match.group(1)
        LOGGER.error(
            "MySQL Client library not loaded. Make sure the shared library "
            "'%s' can be loaded by Python. Tip: Add folder '%s' to "
            "environment variable '%s'.",
            lib_name, lib_dir, libpath_var)
        sys.exit(1)
    else:
        LOGGER.error("C Extension not available: %s", error_msg)
        sys.exit(1)
