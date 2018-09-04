#!/usr/bin/env python
# -*- coding: utf-8 -*-

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

"""Script for running unittests

unittests.py launches all or selected unit tests. For more information and
options, simply do:
 shell> python unittests.py --help

The unittest.py script will check for tests in Python source files prefixed
with 'test_' in the folder tests/.

Examples:
 Running unit tests using MySQL installed under /opt
 shell> python unittests.py --with-mysql=/opt/mysql/mysql-5.7

 Executing unit tests for cursor module
 shell> python unittests.py -t cursor

 Keep the MySQL server(s) running; speeds up multiple runs:
 shell> python unittests.py --keep

 Force shutting down of still running MySQL servers, and bootstrap again:
 shell> python unittests.py --force

 Show a more verbose and comprehensive output of tests (see --help to safe
 information to a database):
 shell> python unittests.py --keep --stats

 Run tests using IPv6:
 shell> python unittests.py --ipv6

unittests.py has exit status 0 when tests were ran successfully, 1 otherwise.

"""
import os
import sys
import time
import unittest
try:
    from urlparse import urlsplit
except ImportError:
    # Python 3
    from urllib.parse import urlsplit
import logging

try:
    from argparse import ArgumentParser
except ImportError:
    # Python v2.6
    from optparse import OptionParser

try:
    from unittest import TextTestResult
except ImportError:
    # Compatibility with Python v2.6
    from unittest import _TextTestResult as TextTestResult

import tests
from tests import mysqld

_TOPDIR = os.path.dirname(os.path.realpath(__file__))
LOGGER = logging.getLogger(tests.LOGGER_NAME)
tests.setup_logger(LOGGER)

# Only run for supported Python Versions
if not (((2, 6) <= sys.version_info < (3, 0)) or sys.version_info >= (3, 3)):
    LOGGER.error("Python v%d.%d is not supported",
                 sys.version_info[0], sys.version_info[1])
    sys.exit(1)
else:
    sys.path.insert(0, os.path.join(_TOPDIR, 'lib'))
    sys.path.insert(0, os.path.join(_TOPDIR))
    tests.TEST_BUILD_DIR = os.path.join(_TOPDIR, 'build', 'testing')
    sys.path.insert(0, tests.TEST_BUILD_DIR)

# MySQL option file template. Platform specifics dynamically added later.
MY_CNF = """
# MySQL option file for MySQL Connector/Python tests
[mysqld-8.0]
plugin-load={mysqlx_plugin}
loose_mysqlx_port={mysqlx_port}
{mysqlx_bind_address}

[mysqld-5.7]
plugin-load={mysqlx_plugin}
loose_mysqlx_port={mysqlx_port}
{mysqlx_bind_address}

[mysqld-5.6]
innodb_compression_level = 0
innodb_compression_failure_threshold_pct = 0
lc_messages_dir = {lc_messages_dir}
lc_messages = en_US
general_log = ON

[mysqld-5.5]
lc_messages_dir = {lc_messages_dir}
lc_messages = en_US
general_log = ON

[mysqld-5.1]
language = {lc_messages_dir}/english
general_log = ON

[mysqld-5.0]
language = {lc_messages_dir}/english

[mysqld]
max_allowed_packet=26777216
basedir = {basedir}
datadir = {datadir}
tmpdir = {tmpdir}
port = {port}
socket = {unix_socket}
bind_address = {bind_address}
pid-file = {pid_file}
skip_name_resolve
server_id = {serverid}
sql_mode = ""
default_time_zone = +00:00
log-error = mysqld_{name}.err
log-bin = mysqld_{name}_bin
local_infile = 1
innodb_flush_log_at_trx_commit = 2
innodb_log_file_size = 1Gb
general_log_file = general_{name}.log
{secure_file_priv}
"""

# Platform specifics
if os.name == 'nt':
    MY_CNF += '\n'.join((
        "ssl-ca = {ssl_ca}",
        "ssl-cert = {ssl_cert}",
        "ssl-key = {ssl_key}",
    ))
    MYSQL_DEFAULT_BASE = os.path.join(
        "C:/", "Program Files", "MySQL", "MySQL Server 5.6")
else:
    MY_CNF += '\n'.join((
        "ssl-ca = {ssl_ca}",
        "ssl-cert = {ssl_cert}",
        "ssl-key = {ssl_key}",
        "innodb_flush_method = O_DIRECT",
    ))
    MYSQL_DEFAULT_BASE = os.path.join('/', 'usr', 'local', 'mysql')

MY_CNF += "\nssl={ssl}"

MYSQL_DEFAULT_TOPDIR = _TOPDIR

_UNITTESTS_CMD_ARGS = {
    ('-T', '--one-test'): {
        'dest': 'onetest', 'metavar': 'NAME',
        'help': (
            'Particular test to execute, format: '
            '<module>[.<class>[.<method>]]. For example, to run a particular '
            'test BugOra13392739.test_reconnect() from the tests.test_bugs '
            'module, use following value for the -T option: '
            ' tests.test_bugs.BugOra13392739.test_reconnect')
    },

    ('-t', '--test'): {
        'dest': 'testcase', 'metavar': 'NAME',
        'help': 'Tests to execute, see --help-tests for more information'
    },

    ('-l', '--log'): {
        'dest': 'logfile', 'metavar': 'NAME', 'default': None,
        'help': 'Log file location (if not given, logging is disabled)'
    },

    ('', '--force'): {
        'dest': 'force', 'action': 'store_true', 'default': False,
        'help': 'Remove previous MySQL test installation.'
    },

    ('', '--keep'): {
        'dest': 'keep', 'action': "store_true", 'default': False,
        'help': 'Keep MySQL installation (i.e. for debugging)'
    },

    ('', '--debug'): {
        'dest': 'debug', 'action': 'store_true', 'default': False,
        'help': 'Show/Log debugging messages'
    },

    ('', '--verbosity'): {
        'dest': 'verbosity', 'metavar': 'NUMBER', 'default': 0, 'type': int,
        'help': 'Verbosity of unittests (default 0)',
        'type_optparse': 'int'
    },

    ('', '--stats'): {
        'dest': 'stats', 'default': False, 'action': 'store_true',
        'help': "Show timings of each individual test."
    },

    ('', '--stats-host'): {
        'dest': 'stats_host', 'default': None, 'metavar': 'NAME',
        'help': (
            "MySQL server for saving unittest statistics. Specify this option "
            "to start saving results to a database. Implies --stats.")
    },

    ('', '--stats-port'): {
        'dest': 'stats_port', 'default': 3306, 'metavar': 'PORT',
        'help': (
            "TCP/IP port of the MySQL server for saving unittest statistics. "
            "Implies --stats. (default 3306)")
    },

    ('', '--stats-user'): {
        'dest': 'stats_user', 'default': 'root', 'metavar': 'NAME',
        'help': (
            "User for connecting with the MySQL server for saving unittest "
            "statistics. Implies --stats. (default root)")
    },

    ('', '--stats-password'): {
        'dest': 'stats_password', 'default': '', 'metavar': 'PASSWORD',
        'help': (
            "Password for connecting with the MySQL server for saving unittest "
            "statistics. Implies --stats. (default to no password)")
    },

    ('', '--stats-db'): {
        'dest': 'stats_db', 'default': 'test', 'metavar': 'NAME',
        'help': (
            "Database name for saving unittest statistics. "
            "Implies --stats. (default test)")
    },

    ('', '--with-mysql'): {
        'dest': 'mysql_basedir', 'metavar': 'NAME',
        'default': MYSQL_DEFAULT_BASE,
        'help': (
            "Installation folder of the MySQL server. "
            "(default {default})").format(default=MYSQL_DEFAULT_BASE)
    },

    ('', '--with-mysql-share'): {
        'dest': 'mysql_sharedir', 'metavar': 'NAME',
        'default': None,
        'help': (
            "share folder of the MySQL server (default <basedir>/share)")
    },

    ('', '--mysql-topdir'): {
        'dest': 'mysql_topdir', 'metavar': 'NAME',
        'default': MYSQL_DEFAULT_TOPDIR,
        'help': (
            "Where to bootstrap the new MySQL instances for testing. "
            "(default {default})").format(default=MYSQL_DEFAULT_TOPDIR)
    },

    ('', '--secure-file-priv'): {
        'dest': 'secure_file_priv', 'metavar': 'DIRECTORY',
        'default': None,
        'help': (
            "MySQL server option, can be empty to disable")
    },

    ('', '--bind-address'): {
        'dest': 'bind_address', 'metavar': 'NAME', 'default': '127.0.0.1',
        'help': 'IP address to bind to'
    },

    ('-H', '--host'): {
        'dest': 'host', 'metavar': 'NAME', 'default': '127.0.0.1',
        'help': 'Hostname or IP address for TCP/IP connections.'
    },

    ('-P', '--port'): {
        'dest': 'port', 'metavar': 'NUMBER', 'default': 33770, 'type': int,
        'help': 'First TCP/IP port to use.',
        'type_optparse': int,
    },

    ('', '--mysqlx-port'): {
        'dest': 'mysqlx_port', 'metavar': 'NUMBER', 'default': 33060,
        'type': int, 'help': 'First TCP/IP port to use for mysqlx protocol.',
        'type_optparse': int,
    },

    ('', '--unix-socket'): {
        'dest': 'unix_socket_folder', 'metavar': 'NAME',
        'help': 'Folder where UNIX Sockets will be created'
    },

    ('', '--ipv6'): {
        'dest': 'ipv6', 'action': 'store_true', 'default': False,
        'help': (
            'Use IPv6 to run tests. This sets --bind-address=:: --host=::1.'
        ),
    },

    ('', '--with-django'): {
        'dest': 'django_path', 'metavar': 'NAME',
        'default': None,
        'help': ("Location of Django (none installed source)")
    },

    ('', '--help-tests'): {
        'dest': 'show_tests', 'action': 'store_true',
        'help': ("Show extra information about test groups")
    },

    ('', '--skip-install'): {
        'dest': 'skip_install', 'action': 'store_true', 'default': False,
        'help': (
            'Skip installation of Connector/Python, reuse previous.'
        ),
    },

    ('', '--with-mysql-capi'): {
        'dest': 'mysql_capi', 'metavar': 'NAME',
        'default': None,
        'help': ("Location of MySQL C API installation "
                 "or full path to mysql_config")
    },

    ('', '--with-protobuf-include-dir'): {
        'dest': 'protobuf_include_dir', 'metavar': 'NAME',
        'default': None,
        'help': ("Location of Protobuf include directory")
    },

    ('', '--with-protobuf-lib-dir'): {
        'dest': 'protobuf_lib_dir', 'metavar': 'NAME',
        'default': None,
        'help': ("Location of Protobuf library directory")
    },

    ('', '--with-protoc'): {
        'dest': 'protoc', 'metavar': 'NAME',
        'default': None,
        'help': ("Location of Protobuf protoc binary")
    },

    ('', '--extra-compile-args'): {
        'dest': 'extra_compile_args', 'metavar': 'NAME',
        'default': None,
        'help': ("Extra compile args for the C extension")
    },

    ('', '--extra-link-args'): {
        'dest': 'extra_link_args', 'metavar': 'NAME',
        'default': None,
        'help': ("Extra link args for the C extension")
    },
}


def _get_arg_parser():
    """Parse command line ArgumentParser

    This function parses the command line arguments and returns the parser.

    It works with both optparse and argparse where available.
    """

    def _clean_optparse(adict):
        """Remove items from dictionary ending with _optparse"""
        new_dict = {}
        for key in adict.keys():
            if not key.endswith('_optparse'):
                new_dict[key] = adict[key]
        return new_dict

    new = True
    try:
        parser = ArgumentParser()
        add = parser.add_argument
    except NameError:
        # Fallback to old optparse
        new = False
        parser = OptionParser()
        add = parser.add_option

    for flags, params in _UNITTESTS_CMD_ARGS.items():
        if new:
            flags = [i for i in flags if i]
        add(*flags, **_clean_optparse(params))

    return parser


def _show_help(msg=None, parser=None, exit_code=0):
    """Show the help of the given parser and exits

    If exit_code is -1, this function will not call sys.exit().
    """
    tests.printmsg(msg)
    if parser is not None:
        parser.print_help()
    if exit_code > -1:
        sys.exit(exit_code)


def get_stats_tablename():
    return "myconnpy_{version}".format(
        version='_'.join(
            [str(i) for i in mysql.connector.__version_info__[0:3]]
        )
    )


def get_stats_field(pyver=None, myver=None):
    if not pyver:
        pyver = '.'.join([str(i) for i in sys.version_info[0:2]])
    if not myver:
        myver = '.'.join([str(i) for i in tests.MYSQL_SERVERS[0].version[0:2]])
    return "py{python}my{mysql}".format(
        python=pyver.replace('.', ''), mysql=myver.replace('.', ''))


class StatsTestResult(TextTestResult):
    """Store test results in a database"""
    separator1 = '=' * 78
    separator2 = '-' * 78

    def __init__(self, stream, descriptions, verbosity, dbcnx=None):
        super(StatsTestResult, self).__init__(stream, descriptions, verbosity)
        self.stream = stream
        self.showAll = 0
        self.dots = 0
        self.descriptions = descriptions
        self._start_time = None
        self._stop_time = None
        self.elapsed_time = None
        self._dbcnx = dbcnx
        self._name = None

    @staticmethod
    def get_description(test):  # pylint: disable=R0201
        """Return test description, if needed truncated to 60 characters
        """
        return "{0:.<60s} ".format(str(test)[0:58])

    def startTest(self, test):
        super(StatsTestResult, self).startTest(test)
        self.stream.write(self.get_description(test))
        self.stream.flush()
        self._start_time = time.time()

    def addSuccess(self, test):
        super(StatsTestResult, self).addSuccess(test)
        self._stop_time = time.time()
        self.elapsed_time = self._stop_time - self._start_time
        fmt = "{timing:>8.3f}s {state:<20s}"
        self.stream.writeln(fmt.format(state="ok", timing=self.elapsed_time))
        if self._dbcnx:
            cur = self._dbcnx.cursor()
            stmt = (
                "INSERT INTO {table} (test_case, {field}) "
                "VALUES (%s, %s) ON DUPLICATE KEY UPDATE {field} = %s"
            ).format(table=get_stats_tablename(),
                     field=get_stats_field())
            cur.execute(stmt,
                        (str(test), self.elapsed_time, self.elapsed_time)
            )
            cur.close()

    def _save_not_ok(self, test):
        cur = self._dbcnx.cursor()
        stmt = (
            "INSERT INTO {table} (test_case, {field}) "
            "VALUES (%s, %s) ON DUPLICATE KEY UPDATE {field} = %s"
        ).format(table=get_stats_tablename(),
                 field=get_stats_field())
        cur.execute(stmt, (str(test), -1, -1))
        cur.close()

    def addError(self, test, err):
        super(StatsTestResult, self).addError(test, err)
        self.stream.writeln("ERROR")
        if self._dbcnx:
            self._save_not_ok(test)

    def addFailure(self, test, err):
        super(StatsTestResult, self).addFailure(test, err)
        self.stream.writeln("FAIL")
        if self._dbcnx:
            self._save_not_ok(test)

    def addSkip(self, test, reason):
        try:
            super(StatsTestResult, self).addSkip(test, reason)
        except AttributeError:
            # We are using Python v2.6/v3.1
            pass
        self.stream.writeln("skipped")
        if self._dbcnx:
            self._save_not_ok(test)

    def addExpectedFailure(self, test, err):
        super(StatsTestResult, self).addExpectedFailure(test, err)
        self.stream.writeln("expected failure")
        if self._dbcnx:
            self._save_not_ok(test)

    def addUnexpectedSuccess(self, test):
        super(StatsTestResult, self).addUnexpectedSuccess(test)
        self.stream.writeln("unexpected success")
        if self._dbcnx:
            self._save_not_ok(test)


class StatsTestRunner(unittest.TextTestRunner):
    """Committing results test results"""
    resultclass = StatsTestResult

    def __init__(self, stream=sys.stderr, descriptions=True, verbosity=1,
                 failfast=False, buffer=False, resultclass=None, dbcnx=None):
        try:
            super(StatsTestRunner, self).__init__(
                stream=sys.stderr, descriptions=True, verbosity=1,
                failfast=False, buffer=False)
        except TypeError:
            # Compatibility with Python v2.6
            super(StatsTestRunner, self).__init__(
                stream=sys.stderr, descriptions=True, verbosity=1)
        self._dbcnx = dbcnx

    def _makeResult(self):
        return self.resultclass(self.stream, self.descriptions,
                                self.verbosity, dbcnx=self._dbcnx)

    def run(self, test):
        result = super(StatsTestRunner, self).run(test)
        if self._dbcnx:
            self._dbcnx.commit()
        return result


class BasicTestResult(TextTestResult):
    """Basic test result"""

    def addSkip(self, test, reason):
        """Save skipped reasons"""
        if self.showAll:
            self.stream.writeln("skipped")
        elif self.dots:
            self.stream.write("s")
            self.stream.flush()

        tests.MESSAGES['SKIPPED'].append(reason)


class BasicTestRunner(unittest.TextTestRunner):
    """Basic test runner"""
    resultclass = BasicTestResult

    def __init__(self, stream=sys.stderr, descriptions=True, verbosity=1,
                 failfast=False, buffer=False, warnings='ignore'):
        try:
            super(BasicTestRunner, self).__init__(
                stream=stream, descriptions=descriptions,
                verbosity=verbosity, failfast=failfast, buffer=buffer,
                warnings=warnings)
        except TypeError:
            # Python v3.1
            super(BasicTestRunner, self).__init__(
                stream=stream, descriptions=descriptions, verbosity=verbosity)


class Python26TestRunner(unittest.TextTestRunner):
    """Python v2.6/3.1 Test Runner backporting needed functionality"""

    def __init__(self, stream=sys.stderr, descriptions=True, verbosity=1,
                 failfast=False, buffer=False):
        super(Python26TestRunner, self).__init__(
            stream=stream, descriptions=descriptions, verbosity=verbosity)

    def _makeResult(self):
        return BasicTestResult(self.stream, self.descriptions, self.verbosity)


def setup_stats_db(cnx):
    """Setup the database for storing statistics"""
    cur = cnx.cursor()

    supported_python = ('2.6', '2.7', '3.1', '3.2', '3.3', '3.4')
    supported_mysql = ('5.1', '5.5', '5.6', '5.7')

    columns = []
    for pyver in supported_python:
        for myver in supported_mysql:
            columns.append(
                " py{python}my{mysql} DECIMAL(8,4) DEFAULT -1".format(
                    python=pyver.replace('.', ''),
                    mysql=myver.replace('.', ''))
            )

    create_table = (
        "CREATE TABLE {table} ( "
        " test_case VARCHAR(100) NOT NULL,"
        " {pymycols}, "
        " PRIMARY KEY (test_case)"
        ") ENGINE=InnoDB"
    ).format(table=get_stats_tablename(),
             pymycols=', '.join(columns))

    try:
        cur.execute(create_table)
    except mysql.connector.ProgrammingError as err:
        if err.errno != 1050:
            raise
        LOGGER.info("Using exists table '{0}' for saving statistics".format(
            get_stats_tablename()))
    else:
        LOGGER.info("Created table '{0}' for saving statistics".format(
            get_stats_tablename()))
    cur.close()


def init_mysql_server(port, options):
    """Initialize a MySQL Server"""
    name = 'server{0}'.format(len(tests.MYSQL_SERVERS) + 1)
    extra_args = [{
        "version": (5, 7, 17),
        "options": {"mysqlx_bind_address": "mysqlx_bind_address={0}".format("::"
                    if tests.IPV6_AVAILABLE else "0.0.0.0")}
    }]

    if options.secure_file_priv is not None:
        extra_args += [{
            "version": (5, 5, 53),
            "options": {"secure_file_priv": "secure_file_priv = %s" % options.secure_file_priv}
        }]
    else:
        extra_args += [{
            "version": (5, 5, 53),
            "options": {"secure_file_priv": ""}
        }]

    try:
        mysql_server = mysqld.MySQLServer(
            basedir=options.mysql_basedir,
            topdir=os.path.join(options.mysql_topdir, 'cpy_' + name),
            cnf=MY_CNF,
            bind_address=options.bind_address,
            port=port,
            mysqlx_port=options.mysqlx_port,
            unix_socket_folder=options.unix_socket_folder,
            ssl_folder=os.path.abspath(tests.SSL_DIR),
            ssl_ca="tests_CA_cert.pem",
            ssl_cert="tests_server_cert.pem",
            ssl_key="tests_server_key.pem",
            name=name,
            extra_args=extra_args,
            sharedir=options.mysql_sharedir)
    except tests.mysqld.MySQLBootstrapError as err:
        LOGGER.error("Failed initializing MySQL server "
                     "'{name}': {error}".format(
            name=name, error=str(err)))
        sys.exit(1)

    if len(mysql_server.unix_socket) > 103:
        LOGGER.error("Unix socket file is to long for mysqld (>103). "
                     "Consider using --unix-socket")
        sys.exit(1)

    mysql_server._debug = options.debug

    have_to_bootstrap = True
    if options.force:
        # Force removal of previous test data
        if mysql_server.check_running():
            mysql_server.stop()
            if not mysql_server.wait_down():
                LOGGER.error(
                    "Failed shutting down the MySQL server '{name}'".format(
                        name=name))
                sys.exit(1)
        mysql_server.remove()
    else:
        if mysql_server.check_running():
            LOGGER.info(
                "Reusing previously bootstrapped MySQL server '{name}'".format(
                    name=name))
            have_to_bootstrap = False
        else:
            LOGGER.warning(
                "Can not connect to previously bootstrapped "
                "MySQL Server '{name}'; forcing bootstrapping".format(
                    name=name))
            mysql_server.remove()

    tests.MYSQL_VERSION = mysql_server.version
    tests.MYSQL_VERSION_TXT = '.'.join([str(i) for i in mysql_server.version])
    tests.MYSQL_SERVERS.append(mysql_server)

    mysql_server.client_config = {
        'host': options.host,
        'port': port,
        'unix_socket': mysql_server.unix_socket,
        'user': 'root',
        'password': '',
        'database': 'myconnpy',
        'connection_timeout': 10,
    }

    mysql_server.xplugin_config = {
        'host': options.host,
        'port': options.mysqlx_port,
        'user': 'root',
        'password': '',
        'schema': 'myconnpy'
    }

    if mysql_server.version >= (5, 7, 15):
        mysql_server.xplugin_config["socket"] = mysql_server.mysqlx_unix_socket
        os.environ["MYSQLX_UNIX_PORT"] = mysql_server.mysqlx_unix_socket

    # Bootstrap and start a MySQL server
    if have_to_bootstrap:
        LOGGER.info("Bootstrapping MySQL server '{name}'".format(name=name))
        try:
            mysql_server.bootstrap()
        except tests.mysqld.MySQLBootstrapError as exc:
            LOGGER.error("Failed bootstrapping MySQL server '{name}': "
                         "{error}".format(name=name, error=str(exc)))
            sys.exit(1)
        mysql_server.start()
        if not mysql_server.wait_up():
            LOGGER.error("Failed to start the MySQL server '{name}'. "
                         "Check error log.".format(name=name))
            sys.exit(1)

def main():
    parser = _get_arg_parser()
    options = parser.parse_args()
    tests.OPTIONS_INIT = True

    if isinstance(options, tuple):
        # Fallback to old optparse
        options = options[0]

    if options.show_tests:
        sys.path.insert(0, os.path.join(os.getcwd(), 'lib'))
        for name, _, description in tests.get_test_modules():
            print("{0:22s} {1}".format(name, description))
        sys.exit()

    tests.setup_logger(LOGGER, debug=options.debug, logfile=options.logfile)
    LOGGER.info(
        "MySQL Connector/Python unittest using Python v{0}".format(
            '.'.join([str(v) for v in sys.version_info[0:3]])))

    # Check if we can test IPv6
    if options.ipv6:
        if not tests.IPV6_AVAILABLE:
            LOGGER.error("Can not test IPv6: not available on your system")
            sys.exit(1)
        options.bind_address = '::'
        options.host = '::1'
        LOGGER.info("Testing using IPv6. Binding to :: and using host ::1")
    else:
        tests.IPV6_AVAILABLE = False

    if not options.mysql_sharedir:
        options.mysql_sharedir = os.path.join(options.mysql_basedir, 'share')
        LOGGER.debug("Setting default sharedir: %s", options.mysql_sharedir)
    if options.mysql_topdir != MYSQL_DEFAULT_TOPDIR:
        # Make sure the topdir is absolute
        if not os.path.isabs(options.mysql_topdir):
            options.mysql_topdir = os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                options.mysql_topdir
            )

    # If Django was supplied, add Django to PYTHONPATH
    if options.django_path:
        sys.path.insert(0, options.django_path)
        try:
            import django
            tests.DJANGO_VERSION = django.VERSION[0:3]
        except ImportError:
            msg = "Could not find django package at {0}".format(
                options.django_path)
            LOGGER.error(msg)
            sys.exit(1)

        if sys.version_info[0] == 3 and tests.DJANGO_VERSION < (1, 5):
            LOGGER.error("Django older than v1.5 will not work with Python 3")
            sys.exit(1)

    # We have to at least run 1 MySQL server
    init_mysql_server(port=(options.port), options=options)

    tests.MYSQL_CAPI = options.mysql_capi
    if not options.skip_install:
        protobuf_include_dir = options.protobuf_include_dir or \
            os.environ.get("MYSQLXPB_PROTOBUF_INCLUDE_DIR")
        protobuf_lib_dir = options.protobuf_lib_dir or \
            os.environ.get("MYSQLXPB_PROTOBUF_LIB_DIR")
        protoc = options.protoc or os.environ.get("MYSQLXPB_PROTOC")
        if any((protobuf_include_dir, protobuf_lib_dir, protoc)):
            if not protobuf_include_dir:
                LOGGER.error("Unable to find Protobuf include directory.")
                sys.exit(1)
            if not protobuf_lib_dir:
                LOGGER.error("Unable to find Protobuf library directory.")
                sys.exit(1)
            if not protoc:
                LOGGER.error("Unable to find Protobuf protoc binary.")
                sys.exit(1)
        tests.install_connector(_TOPDIR, tests.TEST_BUILD_DIR,
                                protobuf_include_dir,
                                protobuf_lib_dir,
                                protoc,
                                options.mysql_capi,
                                options.extra_compile_args,
                                options.extra_link_args, options.debug)

    # Which tests cases to run
    testcases = []

    if options.testcase:
        for name, module, _ in tests.get_test_modules():
            if name == options.testcase or module == options.testcase:
                LOGGER.info("Executing tests in module %s", module)
                testcases = [module]
                break
        if not testcases:
            LOGGER.error("Test case not valid; see --help-tests")
            sys.exit(1)
    elif options.onetest:
        LOGGER.info("Executing test: %s", options.onetest)
        testcases = [options.onetest]
    else:
        testcases = [mod[1] for mod in tests.get_test_modules()]


    # Load tests
    test_loader = unittest.TestLoader()
    testsuite = None
    if testcases:
        # Check if we nee to test anything with the C Extension
        if any(['cext' in case for case in testcases]):
            # Try to load the C Extension, and try to load the MySQL library
            tests.check_c_extension()
        testsuite = test_loader.loadTestsFromNames(testcases)
    else:
        LOGGER.error("No test cases loaded.")
        sys.exit(1)

    # Initialize the other MySQL Servers
    for i in range(1, tests.MYSQL_SERVERS_NEEDED):
        init_mysql_server(port=(options.port + i), options=options)

    LOGGER.info("Using MySQL server version %s",
                '.'.join([str(v) for v in tests.MYSQL_VERSION[0:3]]))

    LOGGER.info("Starting unit tests")
    was_successful = False
    try:
        # Run test cases
        if options.stats:
            if options.stats_host:
                stats_db_info = {
                    'host': options.stats_host,
                    'port': options.stats_port,
                    'user': options.stats_user,
                    'password': options.stats_password,
                    'database': options.stats_db,
                }
                cnxstats = mysql.connector.connect(**stats_db_info)
                setup_stats_db(cnxstats)
            else:
                cnxstats = None
            result = StatsTestRunner(
                verbosity=options.verbosity, dbcnx=cnxstats).run(testsuite)
        elif sys.version_info[0:2] == (2, 6):
            result = Python26TestRunner(verbosity=options.verbosity).run(
                testsuite)
        else:
            result = BasicTestRunner(verbosity=options.verbosity).run(testsuite)
        was_successful = result.wasSuccessful()
    except KeyboardInterrupt:
        LOGGER.info("Unittesting was interrupted")
        was_successful = False

    # Log messages added by test cases
    for msg in tests.MESSAGES['WARNINGS']:
        LOGGER.warning(msg)
    for msg in tests.MESSAGES['INFO']:
        LOGGER.info(msg)

    # Show skipped tests
    if len(tests.MESSAGES['SKIPPED']):
        LOGGER.info("Skipped tests: %d", len(tests.MESSAGES['SKIPPED']))
        for msg in tests.MESSAGES['SKIPPED']:
            LOGGER.info("Skipped: " + msg)

    # Clean up
    try:
        tests.MYSQL_DUMMY_THREAD.join()
        tests.MYSQL_DUMMY.shutdown()
        tests.MYSQL_DUMMY.server_close()
    except:
        # Is OK when failed
        pass
    for mysql_server in tests.MYSQL_SERVERS:
        name = mysql_server.name
        if not options.keep:
            mysql_server.stop()
            if not mysql_server.wait_down():
                LOGGER.error("Failed stopping MySQL server '%s'", name)
            else:
                mysql_server.remove()
                LOGGER.info("MySQL server '%s' stopped and cleaned up", name)
        elif not mysql_server.check_running():
            mysql_server.start()
            if not mysql_server.wait_up():
                LOGGER.error("MySQL could not be kept running; "
                             "failed to restart")
        else:
            LOGGER.info("MySQL server kept running on %s:%d",
                        mysql_server.bind_address,
                        mysql_server.port
            )

    # Make sure the DEVNULL file is closed
    try:
        mysqld.DEVNULL.close()
    except:
        pass

    txt = ""
    if not was_successful:
        txt = "not "
    LOGGER.info("MySQL Connector/Python unittests were %ssuccessful", txt)

    # Return result of tests as exit code
    sys.exit(not was_successful)


if __name__ == '__main__':
    main()
