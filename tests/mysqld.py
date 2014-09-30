# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2009, 2014, Oracle and/or its affiliates. All rights reserved.

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


# MySQL Server executable used
if os.name == 'nt':
    EXEC_MYSQLD = 'mysqld.exe'
else:
    EXEC_MYSQLD = 'mysqld'


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
        for root, dirs, files in os.walk(self._basedir):
            if self._sbindir:
                break
            for afile in files:
                if (afile == EXEC_MYSQLD and
                        os.access(os.path.join(root, afile), 0)):
                    self._sbindir = root
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


class MySQLServer(MySQLServerBase):
    """Class for managing a MySQL server"""

    def __init__(self, basedir, topdir, cnf, bind_address, port,
                 name, datadir=None, tmpdir=None,
                 unix_socket_folder=None, ssl_folder=None, sharedir=None):
        self._cnf = cnf
        self._option_file = os.path.join(topdir, 'my.cnf')
        self._bind_address = bind_address
        self._port = port
        self._topdir = topdir
        self._basedir = basedir
        self._ssldir = ssl_folder or topdir
        self._datadir = datadir or os.path.join(topdir, 'data')
        self._tmpdir = tmpdir or os.path.join(topdir, 'tmp')
        self._name = name
        self._unix_socket = os.path.join(unix_socket_folder or self._topdir,
                                         'mysql_cpy_' + name + '.sock')
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
            self._datadir,
            os.path.join(self._datadir, 'mysql')
        ]
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
            '--bootstrap',
            '--basedir=%s' % self._basedir,
            '--datadir=%s' % self._datadir,
            '--log-warnings=0',
            '--max_allowed_packet=8M',
            '--default-storage-engine=myisam',
            '--net_buffer_length=16K',
            '--tmpdir=%s' % self._tmpdir,
        ]
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

        if self._version[0:3] >= (5, 7, 5):
            # MySQL 5.7.5 creates no user while bootstrapping
            extra_sql.append(
                "INSERT INTO mysql.user VALUES ('localhost','root','',"
                "'Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y',"
                "'Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y',"
                "'Y','Y','Y','Y','Y','','','','',0,0,0,0,"
                "@@default_authentication_plugin,'','N',"
                "CURRENT_TIMESTAMP,NULL);"
            )
        if self._version[0:3] >= (5, 7, 4):
            # MySQL 5.7.4 only creates root@localhost
            extra_sql.append(
                "INSERT INTO mysql.user SELECT '127.0.0.1', `User`, `Password`,"
                " `Select_priv`, `Insert_priv`, `Update_priv`, `Delete_priv`,"
                " `Create_priv`, `Drop_priv`, `Reload_priv`, `Shutdown_priv`,"
                " `Process_priv`, `File_priv`, `Grant_priv`, `References_priv`,"
                " `Index_priv`, `Alter_priv`, `Show_db_priv`, `Super_priv`,"
                " `Create_tmp_table_priv`, `Lock_tables_priv`, `Execute_priv`,"
                " `Repl_slave_priv`, `Repl_client_priv`, `Create_view_priv`,"
                " `Show_view_priv`, `Create_routine_priv`, "
                "`Alter_routine_priv`,"
                " `Create_user_priv`, `Event_priv`, `Trigger_priv`, "
                "`Create_tablespace_priv`, `ssl_type`, `ssl_cipher`,"
                "`x509_issuer`, `x509_subject`, `max_questions`, `max_updates`,"
                "`max_connections`, `max_user_connections`, `plugin`,"
                "`authentication_string`, `password_expired`,"
                "`password_last_changed`, `password_lifetime` FROM mysql.user "
                "WHERE `user` = 'root' and `host` = 'localhost';"
            )

        bootstrap_log = os.path.join(self._topdir, 'bootstrap.log')
        try:
            self._create_directories()
            cmd = self._get_bootstrap_cmd()
            sql = ["USE mysql;"]
            for filename in script_files:
                full_path = os.path.join(self._scriptdir, filename)
                LOGGER.debug("Reading SQL from '%s'", full_path)
                with open(full_path, 'r') as fp:
                    sql.extend([line.strip() for line in fp.readlines()])
            sql.extend(extra_sql)
            fp_log = open(bootstrap_log, 'w')
            prc = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, stdout=fp_log)
            if sys.version_info[0] == 2:
                prc.communicate('\n'.join(sql))
            else:
                prc.communicate(bytearray('\n'.join(sql), 'utf8'))
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

    def start(self):
        """Start a MySQL server"""
        if self.check_running():
            LOGGER.error("MySQL server '{name}' already running".format(
                name=self.name))
            return

        options = {
            'name': self._name,
            'basedir': _convert_forward_slash(self._basedir),
            'datadir': _convert_forward_slash(self._datadir),
            'tmpdir': _convert_forward_slash(self._tmpdir),
            'bind_address': self._bind_address,
            'port': self._port,
            'unix_socket': _convert_forward_slash(self._unix_socket),
            'ssl_dir': _convert_forward_slash(self._ssldir),
            'pid_file': _convert_forward_slash(self._pid_file),
            'serverid': self._serverid,
            'lc_messages_dir': _convert_forward_slash(
                self._lc_messages_dir),
        }
        try:
            fp = open(self._option_file, 'w')
            fp.write(self._cnf.format(**options))
            fp.close()
            self._start_server()
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
                             "'{name}'".format(name=self._name))
                sys.exit(1)
            LOGGER.info("MySQL server started '{name}' "
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
            LOGGER.info("MySQL server stopped '{name}' "
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
