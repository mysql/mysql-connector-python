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

"""Module for managing and running a MySQL server"""

import sys
import os
import signal
import re
from shutil import rmtree
import subprocess
import logging
import time
import ctypes
import socket
import errno
import struct

try:
    from ctypes import wintypes
except (ImportError, ValueError):
    # We are not on Windows
    pass

try:
    from socketserver import (
        ThreadingMixIn, TCPServer, BaseRequestHandler
    )
except ImportError:
    from SocketServer import (
        ThreadingMixIn, TCPServer, BaseRequestHandler
    )
TCPServer.allow_reuse_address = True

import tests

LOGGER = logging.getLogger(tests.LOGGER_NAME)
DEVNULL = open(os.devnull, 'w')


# MySQL Server executable name
if os.name == 'nt':
    EXEC_MYSQLD = 'mysqld.exe'
else:
    EXEC_MYSQLD = 'mysqld'

# MySQL client executable name
if os.name == 'nt':
    EXEC_MYSQL = 'mysql.exe'
else:
    EXEC_MYSQL = 'mysql'

def _convert_forward_slash(path):
    """Convert forward slashes with backslashes

    This function replaces forward slashes with backslashes. This
    is necessary using Microsoft Windows for location of files in
    the option files.

    Returns a string
    """
    if os.name == 'nt':
        nmpath = os.path.normpath(path)
        return nmpath.replace('\\', '\\\\')
    return path


def process_running(pid):
    """Check whether a process is running

    This function takes the process ID or pid and checks whether it is
    running. It works for Windows and UNIX-like systems.

    Return True or False
    """
    if os.name == 'nt':
        # We are on Windows
        process = subprocess.Popen(['tasklist'], stdout=subprocess.PIPE)
        output, _ = process.communicate()
        lines = [line.split(None, 2) for line in output.splitlines() if line]
        for name, apid, _ in lines:
            name = name.decode('utf-8')
            if name == EXEC_MYSQLD and pid == int(apid):
                return True
        return False

    # We are on a UNIX-like system
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def process_terminate(pid):
    """Terminates a process

    This function terminates a running process using it's pid (process
    ID), sending a SIGKILL on Posix systems and using ctypes.windll
    on Windows.

    Raises MySQLServerError on errors.
    """
    if os.name == 'nt':
        winkernel = ctypes.windll.kernel32
        process = winkernel.OpenProcess(0x0001, 0, pid)  # PROCESS_TERMINATE
        winkernel.TerminateProcess(process, 1)
        winkernel.CloseHandle(process)
    else:
        os.kill(pid, signal.SIGTERM)


def get_pid(pid_file):
    """Returns the PID read from the PID file

    Returns None or int.
    """
    try:
        return int(open(pid_file, 'r').readline().strip())
    except IOError as err:
        LOGGER.debug("Failed reading pid file: %s", err)
        return None


class MySQLServerError(Exception):
    """Exception for raising errors when managing a MySQL server"""
    pass


class MySQLBootstrapError(MySQLServerError):
    """Exception for raising errors around bootstrapping a MySQL server"""
    pass


class MySQLServerBase(object):
    """Base for classes managing a MySQL server"""

    def __init__(self, basedir, option_file=None, sharedir=None):
        self._basedir = basedir
        self._sbindir = None
        self._sharedir = sharedir
        self._scriptdir = None
        self._process = None
        self._lc_messages_dir = None
        self._init_mysql_install()
        self._version = self._get_version()

        if option_file and os.access(option_file, 0):
            MySQLBootstrapError("Option file not accessible: {name}".format(
                name=option_file))
        self._option_file = option_file

    def _init_mysql_install(self):
        """Checking MySQL installation

        Check the MySQL installation and set the directories where
        to find binaries and SQL bootstrap scripts.

        Raises MySQLBootstrapError when something fails.
        """

        # Locate mysqld, mysql binaries
        LOGGER.info("Locating mysql binaries (could take a while)")
        files_to_find = [EXEC_MYSQL, EXEC_MYSQLD]
        for root, dirs, files in os.walk(self._basedir):
            if self._sbindir:
                break
            for afile in files:
                if (afile == EXEC_MYSQLD and
                        os.access(os.path.join(root, afile), 0)):
                    self._sbindir = root
                    files_to_find.remove(EXEC_MYSQLD)
                elif (afile == EXEC_MYSQL and
                        os.access(os.path.join(root, afile), 0)):
                    self._bindir = root
                    files_to_find.remove(EXEC_MYSQL)

                if not files_to_find:
                    break


        if not self._sbindir:
            raise MySQLBootstrapError(
                "MySQL binaries not found under {0}".format(self._basedir))

        # Try to locate errmsg.sys and mysql_system_tables.sql
        if not self._sharedir:
            match = self._get_mysqld_help_info(r'^lc-messages-dir\s+(.*)\s*$')
            if match:
                self._sharedir = match[0]
            if not self._sharedir:
                raise MySQLBootstrapError("Failed getting share folder. "
                                          "Use --with-mysql-share.")
        LOGGER.debug("Using share folder: %s", self._sharedir)

        found = False
        for root, dirs, files in os.walk(self._sharedir):
            if found:
                break
            for afile in files:
                if afile == 'errmsg.sys' and 'english' in root:
                    self._lc_messages_dir = os.path.abspath(
                        os.path.join(root, os.pardir)
                    )
                elif afile == 'mysql_system_tables.sql':
                    self._scriptdir = root

        if not self._lc_messages_dir or not self._scriptdir:
            raise MySQLBootstrapError(
                "errmsg.sys and mysql_system_tables.sql not found"
                " under {0}".format(self._sharedir))

        LOGGER.debug("Location of MySQL Server binaries: %s", self._sbindir)

        LOGGER.debug("Error messages: %s", self._lc_messages_dir)
        LOGGER.debug("SQL Script folder: %s", self._scriptdir)

    def _get_cmd(self):
        """Returns command to start MySQL server

        Returns list.
        """
        cmd = [
            os.path.join(self._sbindir, EXEC_MYSQLD),
            "--defaults-file={0}".format(self._option_file),
        ]

        if os.name == 'nt':
            cmd.append('--standalone')

        return cmd

    def _get_mysqld_help_info(self, needle):
        """Get information from the mysqld binary help

        This is basically a grep. Needle is a regular expression which
        will be looked for in each line of the mysqld --help --verbose
        output. We return the first match as a list.
        """
        cmd = [
            os.path.join(self._sbindir, EXEC_MYSQLD),
            '--help', '--verbose'
        ]

        prc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=DEVNULL)
        help_verbose = prc.communicate()[0]
        regex = re.compile(needle)
        for help_line in help_verbose.splitlines():
            help_line = help_line.decode('utf-8').strip()
            match = regex.search(help_line)
            if match:
                return match.groups()

        return []

    def _get_version(self):
        """Get the MySQL server version

        This method executes mysqld with the --version argument. It parses
        the output looking for the version number and returns it as a
        tuple with integer values: (major,minor,patch)

        Returns a tuple.
        """
        cmd = [
            os.path.join(self._sbindir, EXEC_MYSQLD),
            '--version'
        ]

        prc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=DEVNULL)
        verstr = str(prc.communicate()[0])
        matches = re.match(r'.*Ver (\d)\.(\d).(\d{1,2}).*', verstr)
        if matches:
            return tuple([int(v) for v in matches.groups()])
        else:
            raise MySQLServerError(
                'Failed reading version from mysqld --version')

    @property
    def version(self):
        """Returns the MySQL server version

        Returns a tuple.
        """
        return self._version

    def _start_server(self):
        """Start the MySQL server"""
        try:
            cmd = self._get_cmd()
            self._process = subprocess.Popen(cmd, stdout=DEVNULL,
                                             stderr=DEVNULL)
        except (OSError, ValueError) as err:
            raise MySQLServerError(err)

    def _stop_server(self):
        """Stop the MySQL server"""
        if not self._process:
            return False
        try:
            process_terminate(self._process.pid)
        except (OSError, ValueError) as err:
            raise MySQLServerError(err)

        return True

    def get_exec(self, exec_name):
        """Find executable in the MySQL directories

        Returns the the full path to the executable named exec_name or
        None when the executable was not found.

        Return str or None.
        """
        for location in [self._bindir, self._sbindir]:
            exec_path = os.path.join(location, exec_name)
            if os.access(exec_path, 0):
                return exec_path

        return None


class MySQLServer(MySQLServerBase):
    """Class for managing a MySQL server"""

    def __init__(self, basedir, topdir, cnf, bind_address, port, mysqlx_port,
                 name, datadir=None, tmpdir=None, extra_args={},
                 unix_socket_folder=None, ssl_folder=None, ssl_ca=None,
                 ssl_cert=None, ssl_key=None, sharedir=None):
        self._extra_args = extra_args
        self._cnf = cnf
        self._option_file = os.path.join(topdir, 'my.cnf')
        self._bind_address = bind_address
        self._port = port
        self._mysqlx_port = mysqlx_port
        self._topdir = topdir
        self._basedir = basedir
        self._ssldir = ssl_folder or topdir
        self._ssl_ca = os.path.join(self._ssldir, ssl_ca)
        self._ssl_cert = os.path.join(self._ssldir, ssl_cert)
        self._ssl_key = os.path.join(self._ssldir, ssl_key)
        self._datadir = datadir or os.path.join(topdir, 'data')
        self._tmpdir = tmpdir or os.path.join(topdir, 'tmp')
        self._name = name
        self._unix_socket = os.path.join(unix_socket_folder or self._topdir,
                                         'mysql_cpy_' + name + '.sock')
        self._mysqlx_unix_socket = os.path.join(unix_socket_folder \
                or self._topdir, 'mysql_cpy_mysqlx_' + name + '.sock')

        self._pid_file = os.path.join(topdir,
                                      'mysql_cpy_' + name + '.pid')
        self._serverid = port + 100000
        self._install = None
        self._server = None
        self._debug = False
        self._sharedir = sharedir

        self.client_config = {}

        super(MySQLServer, self).__init__(self._basedir,
                                          self._option_file,
                                          sharedir=self._sharedir)
        self._init_sql = os.path.join(self._topdir, 'init.sql')

    def _create_directories(self):
        """Create directory structure for bootstrapping

        Create the directories needed for bootstrapping a MySQL
        installation, i.e. 'mysql' directory.
        The 'test' database is deliberately not created.

        Raises MySQLBootstrapError when something fails.
        """
        dirs = [
            self._topdir,
            os.path.join(self._topdir, 'tmp'),
        ]

        if self._version[0:3] < (8, 0, 1):
            dirs.append(self._datadir)
            if self._version[0:3] < (5, 7, 21):
                dirs.append(os.path.join(self._datadir, 'mysql'))

        for adir in dirs:
            LOGGER.debug("Creating directory %s", adir)
            os.mkdir(adir)

    def _get_bootstrap_cmd(self):
        """Get the command for bootstrapping.

        Get the command which will be used for bootstrapping. This is
        the full path to the mysqld executable and its arguments.

        Returns a list (used with subprocess.Popen)
        """
        cmd = [
            os.path.join(self._sbindir, EXEC_MYSQLD),
            '--no-defaults',
            '--basedir=%s' % self._basedir,
            '--datadir=%s' % self._datadir,
            '--max_allowed_packet=8M',
            '--default-storage-engine=myisam',
            '--net_buffer_length=16K',
            '--tmpdir=%s' % self._tmpdir,
            '--innodb_log_file_size=1Gb',
        ]

        if self._version[0:2] >= (8, 0) or self._version >= (5, 7, 21):
            cmd.append("--initialize-insecure")
            cmd.append("--init-file={0}".format(self._init_sql))
        else:
            cmd.append("--bootstrap")

        if self._version < (8, 0, 3):
            cmd.append('--log-warnings=0')

        if self._version[0:2] < (5, 5):
            cmd.append('--language={0}/english'.format(self._lc_messages_dir))
        else:
            cmd.extend([
                '--lc-messages-dir={0}'.format(self._lc_messages_dir),
                '--lc-messages=en_US'
            ])
        if self._version[0:2] >= (5, 1):
            cmd.append('--loose-skip-ndbcluster')

        return cmd

    def bootstrap(self):
        """Bootstrap a MySQL installation

        Bootstrap a MySQL installation using the mysqld executable
        and the --bootstrap option. Arguments are defined by reading
        the defaults file and options set in the _get_bootstrap_cmd()
        method.

        Raises MySQLBootstrapError when something fails.
        """
        if os.access(self._datadir, 0):
            raise MySQLBootstrapError("Datadir exists, can't bootstrap MySQL")

        # Order is important
        script_files = (
            'mysql_system_tables.sql',
            'mysql_system_tables_data.sql',
            'fill_help_tables.sql',
        )

        # Extra SQL statements to execute after SQL scripts
        extra_sql = [
            "CREATE DATABASE myconnpy;"
        ]

        if self._version < (8, 0, 1) and self._version < (5, 7, 21):
            defaults = ("'root'{0}, "
                        "'Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y',"
                        "'Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y',"
                        "'Y','Y','Y','Y','Y','','','','',0,0,0,0,"
                        "@@default_authentication_plugin,'','N',"
                        "CURRENT_TIMESTAMP,NULL{1}")

            hosts = ["::1", "127.0.0.1", "localhost"]

            insert = "INSERT INTO mysql.user VALUES {0};".format(
                ", ".join("('{0}', {{0}})".format(host) for host in hosts))

            if self._version[0:3] >= (5, 7, 6):
                # No password column, has account_locked column
                defaults = defaults.format("", ", 'N'")
            elif self._version[0:3] >= (5, 7, 5):
                # The password column
                defaults = defaults.format(", ''", "")

            extra_sql.append(insert.format(defaults))
        elif self._version[0:3] >= (5, 7, 21) and self._version < (8, 0, 1):
                LOGGER.info("Appending extra SQL for mysqlx")
                extra_sql.extend([
                    "CREATE USER IF NOT EXISTS 'root'@'localhost';",
                    "GRANT ALL ON *.* TO 'root'@'localhost' WITH GRANT "
                    "OPTION;",
                    "CREATE USER IF NOT EXISTS 'root'@'127.0.0.1';",
                    "GRANT ALL ON *.* TO 'root'@'127.0.0.1' WITH GRANT "
                    "OPTION;",
                    "CREATE USER IF NOT EXISTS 'root'@'::1';",
                    "GRANT ALL ON *.* TO 'root'@'::1' WITH GRANT OPTION;",
                    "CREATE USER IF NOT EXISTS mysqlxsys@localhost IDENTIFIED "
                    "WITH mysql_native_password AS 'password' ACCOUNT LOCK;",
                    "GRANT SELECT ON mysql.user TO mysqlxsys@localhost;",
                    "GRANT SUPER ON *.* TO mysqlxsys@localhost;"
                ])
        else:
            extra_sql.extend([
                "CREATE USER IF NOT EXISTS 'root'@'127.0.0.1';",
                "GRANT ALL ON *.* TO 'root'@'127.0.0.1' WITH GRANT OPTION;",
                "CREATE USER IF NOT EXISTS 'root'@'::1';",
                "GRANT ALL ON *.* TO 'root'@'::1' WITH GRANT OPTION;"
            ])

        bootstrap_log = os.path.join(self._topdir, 'bootstrap.log')
        try:
            self._create_directories()
            cmd = self._get_bootstrap_cmd()
            sql = ["USE mysql;"]

            if self._version[0:2] >= (8, 0) or self._version >= (5, 7, 21):
                test_sql = open(self._init_sql, "w")
                test_sql.write("\n".join(extra_sql))
                test_sql.close()
            else:
                for filename in script_files:
                    full_path = os.path.join(self._scriptdir, filename)
                    LOGGER.debug("Reading SQL from '%s'", full_path)
                    with open(full_path, 'r') as fp:
                        sql.extend([line.strip() for line in fp.readlines()])

            fp_log = open(bootstrap_log, 'w')
            if self._version[0:2] < (8, 0) or self._version < (5, 7, 21):
                sql.extend(extra_sql)
                prc = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                       stderr=subprocess.STDOUT,
                                       stdout=fp_log)
                prc.communicate('\n'.join(sql) if sys.version_info[0] == 2
                                else bytearray('\n'.join(sql), 'utf8'))
            else:
                prc = subprocess.call(cmd, stderr=subprocess.STDOUT,
                                      stdout=fp_log)
            fp_log.close()
        except OSError as err:
            raise MySQLBootstrapError(
                "Error bootstrapping MySQL '{name}': {error}".format(
                    name=self._name, error=str(err)))

        with open(bootstrap_log, 'r') as fp:
            log_lines = fp.readlines()
            for log_line in log_lines:
                if '[ERROR]' in log_line:
                    err_msg = log_line.split('[ERROR]')[1].strip()
                    raise MySQLBootstrapError(
                        "Error bootstrapping MySQL '{name}': {error}".format(
                            name=self._name, error=err_msg))

    @property
    def name(self):
        """Returns the name of this MySQL server"""
        return self._name

    @property
    def port(self):
        """Return TCP/IP port of the server"""
        return self._port

    @property
    def bind_address(self):
        """Return IP address the server is listening on"""
        return self._bind_address

    @property
    def unix_socket(self):
        """Return the unix socket of the server"""
        return self._unix_socket

    @property
    def mysqlx_unix_socket(self):
        return self._mysqlx_unix_socket

    def update_config(self, **kwargs):
        options = {
            'name': self._name,
            'basedir': _convert_forward_slash(self._basedir),
            'datadir': _convert_forward_slash(self._datadir),
            'tmpdir': _convert_forward_slash(self._tmpdir),
            'bind_address': self._bind_address,
            'port': self._port,
            'mysqlx_port': self._mysqlx_port,
            'mysqlx_plugin': 'mysqlx.so' if os.name == 'posix' else 'mysqlx',
            'unix_socket': _convert_forward_slash(self._unix_socket),
            'mysqlx_unix_socket': _convert_forward_slash(
                self._mysqlx_unix_socket),
            'ssl_dir': _convert_forward_slash(self._ssldir),
            'ssl_ca': _convert_forward_slash(self._ssl_ca),
            'ssl_cert': _convert_forward_slash(self._ssl_cert),
            'ssl_key': _convert_forward_slash(self._ssl_key),
            'pid_file': _convert_forward_slash(self._pid_file),
            'serverid': self._serverid,
            'lc_messages_dir': _convert_forward_slash(
                self._lc_messages_dir),
            'ssl': 1,
        }

        for arg in self._extra_args:
            if self._version < arg["version"]:
                options.update(dict([(key, '') for key in
                                     arg["options"].keys()]))
            else:
                options.update(arg["options"])
        options.update(**kwargs)
        try:
            fp = open(self._option_file, 'w')
            fp.write(self._cnf.format(**options))
            fp.close()
        except Exception as ex:
            LOGGER.error("Failed to write config file {0}".format(ex))
            sys.exit(1)

    def start(self, **kwargs):
        if self.check_running():
            LOGGER.error("MySQL server '{name}' already running".format(
                name=self.name))
            return

        self.update_config(**kwargs)
        try:
            self._start_server()
            for i in range(10):
                if self.check_running():
                    break
                time.sleep(5)
        except MySQLServerError as err:
            if self._debug is True:
                raise
            LOGGER.error("Failed starting MySQL server "
                         "'{name}': {error}".format(name=self.name,
                                                    error=str(err)))
            sys.exit(1)
        else:
            pid = get_pid(self._pid_file)
            if not pid:
                LOGGER.error("Failed getting PID of MySQL server "
                             "'{name}' (file {pid_file}".format(
                    name=self._name, pid_file=self._pid_file))
                sys.exit(1)
            LOGGER.debug("MySQL server started '{name}' "
                        "(pid={pid})".format(pid=pid, name=self._name))

    def stop(self):
        """Stop the MySQL server

        Stop the MySQL server and returns whether it was successful or not.

        This method stops the process and exits when it failed to stop the
        server due to an error. When the process was killed, but it the
        process is still found to be running, False is returned. When
        the server was stopped successfully, True is returned.

        Raises MySQLServerError or OSError when debug is enabled.

        Returns True or False.
        """
        pid = get_pid(self._pid_file)
        if not pid:
            return
        try:
            if not self._stop_server():
                process_terminate(pid)
        except (MySQLServerError, OSError) as err:
            if self._debug is True:
                raise
            LOGGER.error("Failed stopping MySQL server '{name}': "
                         "{error}".format(error=str(err), name=self._name))
            sys.exit(1)
        else:
            time.sleep(3)

        if self.check_running(pid):
            LOGGER.debug("MySQL server stopped '{name}' "
                        "(pid={pid})".format(pid=pid, name=self._name))
            return True

        return False

    def remove(self):
        """Remove the topdir of the MySQL server"""
        if not os.path.exists(self._topdir) or self.check_running():
            return
        try:
            rmtree(self._topdir)
        except OSError as err:
            LOGGER.debug("Failed removing %s: %s", self._topdir, err)
            if self._debug is True:
                raise
        else:
            LOGGER.info("Removed {folder}".format(folder=self._topdir))

    def check_running(self, pid=None):
        """Check if MySQL server is running

        Check if the MySQL server is running using the given pid, or when
        not specified, using the PID found in the PID file.

        Returns True or False.
        """
        pid = pid or get_pid(self._pid_file)
        if pid:
            LOGGER.debug("Got PID %d", pid)
            return process_running(pid)

        return False

    def wait_up(self, tries=10, delay=1):
        """Wait until the MySQL server is up

        This method can be used to wait until the MySQL server is started.
        True is returned when the MySQL server is up, False otherwise.

        Return True or False.
        """
        running = self.check_running()
        while not running:
            if tries == 0:
                break
            time.sleep(delay)
            running = self.check_running()
            tries -= 1

        return running

    def wait_down(self, tries=10, delay=1):
        """Wait until the MySQL server is down

        This method can be used to wait until the MySQL server has stopped.
        True is returned when the MySQL server is down, False otherwise.

        Return True or False.
        """
        running = self.check_running()
        while running:
            if tries == 0:
                break
            time.sleep(delay)
            running = self.check_running()
            tries -= 1

        return not running


class DummyMySQLRequestHandler(BaseRequestHandler):
    def __init__(self, request, client_address, server):
        super(DummyMySQLRequestHandler, self).__init__(request, client_address,
                                                       server)

    def read_packet(self):
        """Read a MySQL packet from the socket.

        :return: Tuple with type and payload of packet.
        :rtype: tuple
        """
        header = bytearray(self.request.recv(4))
        if not header:
            return
        length = struct.unpack('<I', header[0:3] + '\x00')[0]
        self._curr_pktnr = struct.unpack('B', header[-1])[0]
        data = self.request.recv(length)
        return header + data

    def handle(self):
        if self.server.sock_error:
            raise socket.error(self.server.socket_error)

        res = self._server_replies[0:bufsize]
        self._server_replies = self._server_replies[bufsize:]
        return res

class DummyMySQLServer(ThreadingMixIn, TCPServer):
    """Class accepting connections for testing MySQL connections"""

    def __init__(self, *args, **kwargs):
        TCPServer.__init__(self, *args, **kwargs)
        self._server_replies = bytearray(b'')
        self._client_sends = []

    def finish_request(self, request, client_address):
        """Finish one request by instantiating RequestHandlerClass."""
        self.RequestHandlerClass(request, client_address, self)

    def raise_socket_error(self, err=errno.EPERM):
        self.socket_error = err

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
