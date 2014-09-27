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
import glob
import logging
import shutil
import subprocess
import errno


LOGGER_NAME = "myconnpy_tests"
LOGGER = logging.getLogger(LOGGER_NAME)
PY2 = sys.version_info[0] == 2

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
MYSQL_VERSION = None
MYSQL_VERSION_TXT = ''
MYSQL_DUMMY = None
MYSQL_DUMMY_THREAD = None
SSL_DIR = os.path.join('tests', 'data', 'ssl')
SSL_CA = os.path.abspath(os.path.join(SSL_DIR, 'tests_CA_cert.pem'))
SSL_CERT = os.path.abspath(os.path.join(SSL_DIR, 'tests_client_cert.pem'))
SSL_KEY = os.path.abspath(os.path.join(SSL_DIR, 'tests_client_key.pem'))
TEST_BUILD_DIR = None

DJANGO_VERSION = None
FABRIC_CONFIG = None

__all__ = [
    'MySQLConnectorTests',
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

    def recv_into(self, buffer, nbytes=0, flags=0):
        if self._raise_socket_error:
            raise socket.error(self._raise_socket_error)
        if nbytes == 0:
            nbytes = len(buffer)
        try:
            buffer[0:nbytes] = self._server_replies[0:nbytes]
        except (IndexError, TypeError, ValueError) as err:
            return 0
        self._server_replies = self._server_replies[nbytes:]
        return len(buffer)

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
    start with 'test_'.

    Returns a list of strings.
    """
    testcases = []

    # For all python version
    for file_ in glob.glob(os.path.join('tests', 'test_*.py')):
        module = os.path.splitext(os.path.basename(file_))[0]
        if OPTIONS_INIT and not DJANGO_VERSION and 'django' in module:
            # Skip django testing completely when Django is not available.
            LOGGER.warning("Django tests will not run: Django not available")
            continue
        testcases.append(
            'tests.{module}'.format(module=module))
        LOGGER.debug('Added tests.{module}'.format(module=module))

    _CACHED_TESTCASES = testcases
    return testcases


def get_test_names():
    """Get test names

    This functions gets the names of Python modules containing tests. The
    name is parsed from files prefixed with 'test_'. For example,
    'test_cursor.py' has name 'cursor'.

    Returns a list of strings.
    """
    pattern = re.compile('.*test_')
    return [pattern.sub('', s) for s in get_test_modules()]


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


class MySQLConnectorTests(unittest.TestCase):

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


def install_connector(root_dir, install_dir):
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
        'install',
        '--root', install_dir,
        '--install-lib', '.'
    ]

    prc = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                           stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                           cwd=root_dir)
    stdout = prc.communicate()[0]
    if prc.returncode is not 0:
        with open(logfile, 'w') as logfp:
            logfp.write(stdout.decode('utf8'))
        LOGGER.error("Failed installing Connector/Python, see {log}".format(
            log=logfile))
        sys.exit(1)
