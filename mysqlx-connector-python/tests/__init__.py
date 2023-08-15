# Copyright (c) 2013, 2023, Oracle and/or its affiliates.
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

import datetime
import errno
import importlib
import inspect
import logging
import os
import platform
import re
import shutil
import socket
import struct
import subprocess
import sys
import traceback
import unittest

from distutils.dist import Distribution
from functools import wraps
from pkgutil import walk_packages
from unittest.case import SkipTest
from unittest.util import strclass

ARCH_64BIT = struct.calcsize("P") * 8 == 64
LOGGER_NAME = "myconnpy_tests"
LOGGER = logging.getLogger(LOGGER_NAME)
_CACHED_TESTCASES = []


def load_source(name, path):
    loader = importlib.machinery.SourceFileLoader(name, path)
    spec = importlib.util.spec_from_loader(name, loader)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    loader.exec_module(module)


SSL_AVAILABLE = True
try:
    import ssl
except ImportError:
    SSL_AVAILABLE = False

# Note that IPv6 support for Python is checked here, but it can be disabled
# when the bind_address of MySQL was not set to '::1'.
IPV6_AVAILABLE = socket.has_ipv6

OLD_UNITTEST = sys.version_info[0:2] in [(2, 6)]

if os.name == "nt":
    WINDOWS_VERSION = platform.win32_ver()[1]
    WINDOWS_VERSION_INFO = [0] * 2
    for i, value in enumerate(WINDOWS_VERSION.split(".")[0:2]):
        WINDOWS_VERSION_INFO[i] = int(value)
    WINDOWS_VERSION_INFO = tuple(WINDOWS_VERSION_INFO)
else:
    WINDOWS_VERSION = None
    WINDOWS_VERSION_INFO = ()

# Following dictionary holds messages which were added by test cases
# but only logged at the end.
MESSAGES = {
    "WARNINGS": [],
    "INFO": [],
    "SKIPPED": [],
}

OPTIONS_INIT = False

MYSQL_EXTERNAL_SERVER = False
MYSQL_SERVERS_NEEDED = 1
MYSQL_SERVERS = []
MYSQL_VERSION = ()
MYSQL_LICENSE = ""
MYSQL_VERSION_TXT = ""
MYSQL_DUMMY = None
MYSQL_DUMMY_THREAD = None
SSL_DIR = os.path.join("tests", "data", "ssl")
SSL_CA = os.path.abspath(os.path.join(SSL_DIR, "tests_CA_cert.pem"))
SSL_CERT = os.path.abspath(os.path.join(SSL_DIR, "tests_client_cert.pem"))
SSL_KEY = os.path.abspath(os.path.join(SSL_DIR, "tests_client_key.pem"))
TEST_BUILD_DIR = None

__all__ = [
    "MySQLConnectorTests",
    "MySQLxTests",
    "get_test_names",
    "printmsg",
    "LOGGER_NAME",
    "DummySocket",
    "SSL_DIR",
    "get_test_modules",
    "MESSAGES",
    "setup_logger",
    "install_connector",
    "TEST_BUILD_DIR",
]


class DummySocket:

    """Dummy socket class

    This class helps to test socket connection without actually making any
    network activity. It is a proxy class using socket.socket.
    """

    def __init__(self, *args):
        self._socket = socket.socket(*args)
        self._server_replies = bytearray(b"")
        self._client_sends = []
        self._raise_socket_error = 0

    def __getattr__(self, attr):
        return getattr(self._socket, attr)

    def raise_socket_error(self, err=errno.EPERM):
        self._raise_socket_error = err

    def recv(self, bufsize=4096, flags=0):
        if self._raise_socket_error:
            raise OSError(self._raise_socket_error)
        res = self._server_replies[0:bufsize]
        self._server_replies = self._server_replies[bufsize:]
        return res

    def recv_into(self, buffer_, nbytes=0, flags=0):
        if self._raise_socket_error:
            raise OSError(self._raise_socket_error)
        if nbytes == 0:
            nbytes = len(buffer_)
        try:
            if isinstance(buffer_, memoryview):
                # return the number of bytes received,
                # not the length of the memoryview
                len_ = 0
                for x in self._server_replies[0:nbytes]:
                    buffer_[len_] = x
                    len_ += 1
            else:
                buffer_[0:nbytes] = self._server_replies[0:nbytes]
        except (IndexError, TypeError) as err:
            return 0
        except ValueError:
            pass
        self._server_replies = self._server_replies[nbytes:]
        return len(buffer_) if not isinstance(buffer_, memoryview) else len_

    def send(self, string, flags=0):
        if self._raise_socket_error:
            raise OSError(self._raise_socket_error)
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
        self._server_replies = bytearray(b"")
        self._client_sends = []

    def get_address(self):
        return "dummy"


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

    pattern = re.compile(".*test_(.*)")
    for finder, name, is_pkg in walk_packages(__path__, prefix=__name__ + "."):
        if ".test_" not in name or ("django" in name):
            continue

        module_path = os.path.join(finder.path, name.split(".")[-1] + ".py")
        dsc = "(description not available)"

        testing_dir = os.path.dirname(os.path.realpath(__file__))
        sys.path.append(os.path.join(testing_dir, "..", "build", "testing"))

        try:
            mod = load_source(name, module_path)
        except IOError:
            # Not Python source files
            continue
        except ImportError as exc:
            continue
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
    pattern = re.compile(".*test_(.*)")
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
            "number of MySQL servers should be a value between 1 and 16"
        )
    if number > MYSQL_SERVERS_NEEDED:
        MYSQL_SERVERS_NEEDED = number


def fake_hostname():
    """Return a fake hostname

    This function returns a string which can be used in the creation of
    fake hostname. Note that we do not add a domain name.

    Returns a string.
    """
    return "".join(["%02x" % c for c in os.urandom(4)])


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
        return "UTC"


class TestTimeZone(datetime.tzinfo):

    """Test time zone"""

    def __init__(self, hours=0):
        self._offset = datetime.timedelta(hours=hours)

    def utcoffset(self, dt):
        return self._offset

    def dst(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        return "TestZone"


def foreach_session(**extra_config):
    def _use_cnx(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            import mysqlx

            if not hasattr(self, "config"):
                self.config = get_mysqlx_config()
            if extra_config:
                for key, value in extra_config.items():
                    self.config[key] = value
            for use_pure in self.use_pure_options:
                config = self.config.copy()
                config["use_pure"] = use_pure
                self.session = mysqlx.get_session(config)
                self.schema = self.session.get_default_schema()
                try:
                    func(self, *args, **kwargs)
                except Exception as exc:
                    traceback.print_exc(file=sys.stdout)
                    raise exc
                finally:
                    self.session.close()

        return wrapper

    return _use_cnx


class MySQLConnectorTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(MySQLConnectorTests, self).__init__(methodName=methodName)

    def __str__(self):
        classname = strclass(self.__class__)
        return "{classname}.{method}".format(
            method=self._testMethodName,
            classname=re.sub(r"tests\d*.test_", "", classname),
        )

    def check_attr(self, obj, attrname, default):
        cls_name = obj.__class__.__name__
        self.assertTrue(
            hasattr(obj, attrname),
            "{name} object has no '{attr}' attribute".format(
                name=cls_name, attr=attrname
            ),
        )
        self.assertEqual(
            default,
            getattr(obj, attrname),
            "{name} object's '{attr}' should "
            "default to {type_} '{default}'".format(
                name=cls_name,
                attr=attrname,
                type_=type(default).__name__,
                default=default,
            ),
        )

    def check_method(self, obj, method):
        cls_name = obj.__class__.__name__
        self.assertTrue(
            hasattr(obj, method),
            "{0} object has no '{1}' method".format(cls_name, method),
        )
        self.assertTrue(
            inspect.ismethod(getattr(obj, method)),
            "{0} object defines {1}, but is not a method".format(cls_name, method),
        )

    def check_args(self, function, supported_arguments):
        argspec = inspect.getfullargspec(function)
        function_arguments = dict(zip(argspec[0][1:], argspec[3]))
        for argument, default in function_arguments.items():
            try:
                self.assertEqual(
                    supported_arguments[argument],
                    default,
                    msg="Argument '{0}' has wrong default".format(argument),
                )
            except KeyError:
                self.fail("Found unsupported or new argument '%s'" % argument)
        for argument, default in supported_arguments.items():
            if not argument in function_arguments:
                self.fail("Supported argument '{0}' fails".format(argument))

    if sys.version_info[0:2] >= (3, 4):

        def _addSkip(self, result, test_case, reason):
            add_skip = getattr(result, "addSkip", None)
            if add_skip:
                add_skip(test_case, self._testMethodName + ": " + reason)

    else:

        def _addSkip(self, result, reason):
            add_skip = getattr(result, "addSkip", None)
            if add_skip:
                add_skip(self, self._testMethodName + ": " + reason)

    if sys.version_info[0:2] == (2, 6):
        # Backport handy asserts from 2.7
        def assertIsInstance(self, obj, cls, msg=None):
            if not isinstance(obj, cls):
                msg = "{0} is not an instance of {1}".format(
                    unittest.util.safe_repr(obj), unittest.util.repr(cls)
                )
                self.fail(self._formatMessage(msg, msg))

        def assertGreater(self, a, b, msg=None):
            if not a > b:
                msg = "{0} not greater than {1}".format(
                    unittest.util.safe_repr(a), unittest.util.safe_repr(b)
                )
                self.fail(self._formatMessage(msg, msg))

    def run(self, result=None):
        if sys.version_info[0:2] == (2, 6):
            test_method = getattr(self, self._testMethodName)
            if getattr(self.__class__, "__unittest_skip__", False) or getattr(
                test_method, "__unittest_skip__", False
            ):
                # We skipped a class
                try:
                    why = getattr(
                        self.__class__, "__unittest_skip_why__", ""
                    ) or getattr(test_method, "__unittest_skip_why__", "")
                    self._addSkip(result, why)
                finally:
                    result.stopTest(self)
                return
        return super().run(result)

    def check_namedtuple(self, tocheck, attrs):
        for attr in attrs:
            try:
                getattr(tocheck, attr)
            except AttributeError:
                self.fail(
                    "Attribute '{0}' not part of namedtuple {1}".format(attr, tocheck)
                )

    def get_clean_mysql_config(self):
        config = get_mysql_config()
        return {
            opt: config[opt] for opt in ["host", "port", "user", "password", "database"]
        }


class MySQLxTests(MySQLConnectorTests):
    def __init__(self, methodName="runTest"):
        super(MySQLxTests, self).__init__(methodName=methodName)
        from mysqlx.protobuf import HAVE_MYSQLXPB_CEXT

        self.use_pure_options = [True, False] if HAVE_MYSQLXPB_CEXT else [True]

    def run(self, result=None):
        if sys.version_info[0:2] == (2, 6):
            test_method = getattr(self, self._testMethodName)
            if getattr(self.__class__, "__unittest_skip__", False) or getattr(
                test_method, "__unittest_skip__", False
            ):
                # We skipped a class
                try:
                    why = getattr(
                        self.__class__, "__unittest_skip_why__", ""
                    ) or getattr(test_method, "__unittest_skip_why__", "")
                    self._addSkip(result, why)
                finally:
                    result.stopTest(self)
                return
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
        if not isinstance(test, (type)):

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


def setup_logger(logger, debug=False, logfile=None, filter=None):
    """Setting up the logger"""
    formatter = logging.Formatter("%(asctime)s [%(name)s:%(levelname)s] %(message)s")
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
    if filter:
        logger.addFilter(filter)
    LOGGER.handlers = []  # We only need one handler
    LOGGER.addHandler(handler)


def install_connector(
    root_dir,
    install_dir,
    protobuf_include_dir,
    protobuf_lib_dir,
    protoc,
    extra_compile_args=None,
    extra_link_args=None,
    debug=False,
):
    """Install Connector/Python in working directory"""
    logfile = "myconnpy_install.log"
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
        "setup.py",
        "clean",
        "--all",  # necessary for removing the build/
    ]

    dist = Distribution()
    cmd_build = dist.get_command_obj("build")
    cmd_build.ensure_finalized()

    cmd.extend(
        [
            "install",
            "--root",
            install_dir,
            "--install-lib",
            ".",
        ]
    )
    if os.name == "nt":
        cmd.extend(["--install-data", cmd_build.build_platlib])

    if any((protobuf_include_dir, protobuf_lib_dir, protoc)):
        cmd.extend(
            [
                "--with-protobuf-include-dir",
                protobuf_include_dir,
                "--with-protobuf-lib-dir",
                protobuf_lib_dir,
                "--with-protoc",
                protoc,
            ]
        )

    if extra_compile_args:
        cmd.extend(["--extra-compile-args", extra_compile_args])

    if extra_link_args:
        cmd.extend(["--extra-link-args", extra_link_args])

    if debug:
        cmd.append("--debug")

    LOGGER.debug("Installing command: {0}".format(cmd))
    prc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
        cwd=root_dir,
    )
    stdout = prc.communicate()[0]
    if prc.returncode != 0:
        with open(logfile, "wb") as logfp:
            logfp.write(stdout)
        LOGGER.error(
            "Failed installing Connector/Python, see {log}".format(log=logfile)
        )
        if debug:
            with open(logfile) as logfr:
                print(logfr.read())
        sys.exit(1)


def check_tls_versions_support(tls_versions):
    """Check whether we can connect with given TLS version

    Attempts a connection to a server using a specific TLS version but does not verify
    which TLS version was used on the connection.

    :param: List of TLS versions to test.
    :return: List of supported TLS versions.
    :rtype: list
    """
    settings = get_mysql_config()
    if "socket" in settings:
        settings.pop("socket")
    if "unix_socket" in settings:
        settings.pop("unix_socket")
    supported_tls = []
    try:
        from mysql.connector import MySQLConnection

        for tls_v in tls_versions:
            try:
                settings["tls_versions"] = [tls_v]
                cnx = MySQLConnection(**settings)
                cnx.close()
                supported_tls.append(tls_v)
            except:
                pass
    except ImportError:
        pass
    return supported_tls


def product_of(old_list, new_list):
    product_res = []
    if not old_list:
        for new_element in new_list:
            if isinstance(new_element, tuple):
                product_res.append(list(new_element))
            else:
                product_res.append([new_element])
    else:
        for old_element in old_list:
            for new_element in new_list:
                if isinstance(new_element, tuple):
                    product_res.append(old_element + list(new_element))
                else:
                    product_res.append(old_element + [new_element])

    return product_res


def get_scenarios_matrix(scenarios_lists):
    res_matrix = []
    for scenarios in scenarios_lists:
        res_matrix = product_of(res_matrix, scenarios)
    return res_matrix
