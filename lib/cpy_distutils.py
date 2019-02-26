# Copyright (c) 2014, 2019, Oracle and/or its affiliates. All rights reserved.
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

"""Implements the DistUtils command 'build_ext'
"""

from datetime import datetime
from distutils.command.build_ext import build_ext
from distutils.command.install import install
from distutils.command.install_lib import install_lib
from distutils.errors import DistutilsExecError
from distutils.util import get_platform
from distutils.version import LooseVersion
from distutils.dir_util import copy_tree, mkpath
from distutils.spawn import find_executable
from distutils.sysconfig import get_python_lib, get_python_version
from distutils import log
from glob import glob
import os
import shlex
import struct
from subprocess import Popen, PIPE, STDOUT, check_call
import sys
import platform
import shutil
import logging
import re

try:
    from dateutil.tz import tzlocal
    NOW = datetime.now(tzlocal())
except ImportError:
    NOW = datetime.now()

try:
    from urllib.parse import parse_qsl
except ImportError:
    from urlparse import parse_qsl


# Logging
LOGGER = logging.getLogger("mysql_c_api_info")
handler = logging.StreamHandler()
formatter = logging.Formatter("%(levelname)s[%(name)s]: %(message)s")
handler.setFormatter(formatter)
LOGGER.addHandler(handler)
LOGGER.setLevel(logging.DEBUG)

# Import mysql.connector.version
version_py = os.path.join("lib", "mysql", "connector", "version.py")
with open(version_py, "rb") as version_fp:
    exec(compile(version_fp.read(), version_py, "exec"))

ARCH_64BIT = sys.maxsize > 2**32  # Works with Python 2.6 and greater
py_arch = '64-bit' if ARCH_64BIT else '32-bit'

CEXT_OPTIONS = [
    ('with-mysql-capi=', None,
     "Location of MySQL C API installation or path to mysql_config"),
    ('with-protobuf-include-dir=', None,
     "Location of Protobuf include directory"),
    ('with-protobuf-lib-dir=', None,
     "Location of Protobuf library directory"),
    ('with-protoc=', None,
     "Location of Protobuf protoc binary"),
    ('extra-compile-args=', None,
     "Extra compile args"),
    ('extra-link-args=', None,
     "Extra link args")
]

CEXT_STATIC_OPTIONS = [
    ('static', None,
     "Link C libraries statically with the C Extension"),
]

INSTALL_OPTIONS = [
    ('byte-code-only=', None,
     "Remove Python .py files; leave byte code .pyc only"),
    ('is-wheel', None,
     "Install beehaves as wheel package requires"),
]


def win_dll_is64bit(dll_file):
    """Check if a Windows DLL is 64 bit or not

    Returns True if the library dll_file is 64bit.

    Raises ValueError when magic of header is invalid.
    Raises IOError when file could not be read.
    Raises OSError when execute on none-Windows platform.

    Returns True or False.
    """
    if os.name != 'nt':
        raise OSError("win_ddl_is64bit only useful on Windows")

    with open(dll_file, 'rb') as fp:
        # IMAGE_DOS_HEADER
        e_magic = fp.read(2)
        if e_magic != b'MZ':
            raise ValueError("Wrong magic in header")

        fp.seek(60)
        offset = struct.unpack("I", fp.read(4))[0]

        # IMAGE_FILE_HEADER
        fp.seek(offset)
        file_header = fp.read(6)
        (signature, machine) = struct.unpack("<4sH", file_header)
        if machine == 0x014c:  # IMAGE_FILE_MACHINE_I386
            return False
        elif machine in (0x8664, 0x2000):  # IMAGE_FILE_MACHINE_I386/AMD64
            return True


def unix_lib_is64bit(lib_file):
    """Check if a library on UNIX is 64 bit or not

    This function uses the `file` command to check if a library on
    UNIX-like platforms is 32 or 64 bit.

    Returns True if the library is 64bit.

    Raises ValueError when magic of header is invalid.
    Raises IOError when file could not be read.
    Raises OSError when execute on none-Windows platform.

    Returns True or False.
    """
    if os.name != 'posix':
        raise OSError("unix_lib_is64bit only useful on UNIX-like systems")

    if os.isdir(lib_file):
        mysqlclient_libs = []
        for root, _, files in os.walk(lib_file):
            for filename in files:
                filepath = os.path.join(root, filename)
                if filename.startswith('libmysqlclient') and \
                   not os.path.islink(filepath) and \
                   '_r' not in filename and \
                   '.a' not in filename:
                    mysqlclient_libs.append(filepath)
            if mysqlclient_libs:
                break
        # give priority to .so files instead of .a
        mysqlclient_libs.sort()
        lib_file = mysqlclient_libs[-1]

    log.debug("# Using file command to test lib_file {0}".format(lib_file))
    if platform.uname() == 'SunOS':
        cmd_list = ['file', '-L', lib_file]
    else:
        cmd_list = ['file', '-L', lib_file]
    prc = Popen(cmd_list, stdin=PIPE, stderr=STDOUT,
                stdout=PIPE)
    stdout = prc.communicate()[0]
    stdout = stdout.split(':')[1]
    log.debug("# lib_file {0} stdout: {1}".format(lib_file, stdout))
    if 'x86_64' in stdout or 'x86-64' in stdout or '32-bit' not in stdout:
        return True

    return False


def parse_command_line(line, debug = False):
    """Parse a command line.

    This will never be perfect without special knowledge about all possible
    command lines "mysql_config" might output. But it should be close enbough
    for our usage.
    """
    args = shlex.split(line)

    # Find out what kind of argument it is first,
    # if starts with "--", "-" or nothing
    pre_parsed_line = []
    for arg in args:
        re_obj = re.search(r"^(--|-|)(.*)", arg)
        pre_parsed_line.append(re_obj.group(1, 2))

    parsed_line = []

    while pre_parsed_line:
        (type1, opt1) = pre_parsed_line.pop(0)

        if "=" in opt1:
            # One of "--key=val", "-key=val" or "key=val"
            parsed_line.append(tuple(opt1.split("=", 1)))
        elif type1:
            # We have an option that might have a value
            # in the next element in the list
            if pre_parsed_line:
                (type2, opt2) = pre_parsed_line[0]
                if type2 == "" and not "=" in opt2:
                    # Value was in the next list element
                    parsed_line.append((opt1, opt2))
                    pre_parsed_line.pop(0)
                    continue
            if type1 == "--":
                # If "--" and no argument then it is an option like "--fast"
                parsed_line.append(opt1)
            else:
                # If "-" (and no "=" handled above) then it is a
                # traditional one character option name that might
                # have a value
                val = opt1[1:]
                if val:
                    parsed_line.append((opt1[:1], val))
                else:
                    parsed_line.append(opt1)
        else:
            LOGGER.warning("Could not handle '{}' in '{}'".format(opt1, line))

    return parsed_line


def mysql_c_api_info(mysql_config, debug=False):
    """Get MySQL information using mysql_config tool.

    Returns a dict.
    """

    process = Popen([mysql_config], stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    if not stdout:
        raise ValueError("Error executing command: {} ({})".format(cmd, stderr))

    # Parse the output. Try to be future safe in case new options
    # are added. This might of course fail.
    info = {}

    # FIXME: handle "Compiler:" ?
    for line in stdout.splitlines():
        re_obj = re.search(
            r"^\s+(?:--)?(\w+)\s+\[\s*(.*?)\s*\]", line.decode("utf-8"))
        if re_obj:
            mc_key = re_obj.group(1)
            mc_val = re_obj.group(2)

            # We always add the raw output from the different "mysql_config"
            # options. And in some cases, like "port", "socket", that is enough
            # for use from Python.
            info[mc_key] = mc_val
            LOGGER.debug("OPTION: {} = {}".format(mc_key, mc_val))

            if not re.search(r"^-", mc_val) and not "=" in mc_val:
                # Not a Unix command line
                continue

            # In addition form useful information parsed from the
            # above command line
            parsed_line = parse_command_line(mc_val, debug = debug)

            if mc_key == "include":
                # Lets assume all arguments are paths with "-I", "--include", ..
                include_directories = [val for _, val in parsed_line]
                info["include_directories"] = include_directories
                LOGGER.debug("OPTION: include_directories = {}"
                             "".format(" ".join(include_directories)))
            elif mc_key == "libs_r":
                info["link_directories"] = [val for key, val in parsed_line \
                                            if key in ("L", "library-path",)]
                info["libraries"] = [val for key, val in parsed_line \
                                     if key in ("l", "library",)]
                LOGGER.debug("OPTION: link_directories = {}"
                             "".format(" ".join(info["link_directories"])))
                LOGGER.debug("OPTION: libraries = {}"
                             "".format(" ".join(info["libraries"])))

    # Try to figure out the architecture
    info["arch"] = "x86_64" if sys.maxsize > 2**32 else "i386"
    # Return a tuple for version instead of a string
    info["version"] = tuple([int(num) if num.isdigit() else num
                             for num in info["version"].split(".")])

    return info


def get_git_info():
    """Get Git information about the last commit.

    Returns a dict.
    """
    is_git_repo = False
    if find_executable("git") is not None:
        # Check if it's a Git repository
        proc = Popen(["git", "branch"], universal_newlines=True)
        proc.communicate()
        is_git_repo = proc.returncode == 0

    if is_git_repo:
        cmd = ["git", "log", "-n", "1", "--date=iso",
               "--pretty=format:'branch=%D&date=%ad&commit=%H&short=%h'"]
        proc = Popen(cmd, stdout=PIPE, universal_newlines=True)
        stdout, _ = proc.communicate()
        git_info = dict(parse_qsl(stdout.replace("'", "").replace("+", "%2B")
                                  .split(",")[-1:][0].strip()))
        git_info["branch"] = stdout.split(",")[0].split("->")[1].strip()
        return git_info
    else:
        branch_src = os.getenv("BRANCH_SOURCE")
        push_rev = os.getenv("PUSH_REVISION")
        if branch_src and push_rev:
            git_info = {
                "branch": branch_src.split()[-1],
                "date": None,
                "commit": push_rev,
                "short": push_rev[:7]
            }
            return git_info
    return None


def remove_cext(distribution, ext):
    """Remove the C Extension from the distribution

    This function can be useful in Distutils commands for creating
    pure Python modules.
    """
    to_remove = []
    for ext_mod in distribution.ext_modules:
        if ext_mod.name == ext:
            to_remove.append(ext_mod)
    for ext_mod in to_remove:
        distribution.ext_modules.remove(ext_mod)


class BuildExtDynamic(build_ext):

    """Build Connector/Python C Extension"""

    description = "build Connector/Python C Extension"

    user_options = build_ext.user_options + CEXT_OPTIONS

    min_connector_c_version = None
    arch = None
    _mysql_config_info = None

    def initialize_options(self):
        build_ext.initialize_options(self)
        self.extra_compile_args = None
        self.extra_link_args = None
        self.with_mysql_capi = None
        self.with_mysqlxpb_cext = False
        self.with_protobuf_include_dir = None
        self.with_protobuf_lib_dir = None
        self.with_protoc = None

    def _get_posix_openssl_libs(self):
        openssl_libs = []
        try:
            openssl_libs_path = os.path.join(self.with_mysql_capi, "lib")
            openssl_libs.extend([
                os.path.basename(glob(
                    os.path.join(openssl_libs_path, "libssl.*.*.*"))[0]),
                os.path.basename(glob(
                    os.path.join(openssl_libs_path, "libcrypto.*.*.*"))[0])
            ])
        except IndexError:
            log.error("Couldn't find OpenSSL libraries in libmysqlclient")
        return openssl_libs

    def _copy_vendor_libraries(self):
        is_wheel = getattr(self.distribution.get_command_obj("install"),
                           "is_wheel", False)
        if not self.with_mysql_capi or not is_wheel:
            return

        log.info("Copying vendor files")
        data_files = []
        vendor_libs = []
        vendor_folder = ""

        if os.name == "nt":
            mysql_capi = os.path.join(self.with_mysql_capi, "bin")
            vendor_libs.append((mysql_capi, ["ssleay32.dll", "libeay32.dll"]))
            # Bundle libmysql.dll
            src = os.path.join(self.with_mysql_capi, "lib", "libmysql.dll")
            dst = os.getcwd()
            log.info("copying {0} -> {1}".format(src, dst))
            shutil.copy(src, dst)
            data_files.append("libmysql.dll")
        else:
            mysql_config = self.with_mysql_capi \
                if not os.path.isdir(self.with_mysql_capi) \
                else os.path.join(self.with_mysql_capi, "bin", "mysql_config")
            mysql_info = mysql_c_api_info(mysql_config)
            if mysql_info["version"] >= (8, 0, 6):
                mysql_capi = os.path.join(self.with_mysql_capi, "lib")
                vendor_libs.append((mysql_capi, self._get_posix_openssl_libs()))
                vendor_folder = "mysql-vendor"

        if vendor_folder:
            mkpath(os.path.join(os.getcwd(), vendor_folder))

        # Copy vendor libraries to 'mysql-vendor' folder
        log.info("Copying vendor libraries")
        for src_folder, files in vendor_libs:
            for filename in files:
                data_files.append(os.path.join(vendor_folder, filename))
                src = os.path.join(src_folder, filename)
                dst = os.path.join(os.getcwd(), vendor_folder)
                log.info("copying {0} -> {1}".format(src, dst))
                shutil.copy(src, dst)
        # Add data_files to distribution
        self.distribution.data_files = [(vendor_folder, data_files)]

    def _finalize_connector_c(self, connc_loc):
        """Finalize the --with-connector-c command line argument
        """
        platform = get_platform()
        self._mysql_config_info = None
        min_version = BuildExtDynamic.min_connector_c_version

        err_invalid_loc = "MySQL C API location is invalid; was %s"

        mysql_config = None
        err_version = "MySQL C API {0}.{1}.{2} or later required".format(
            *BuildExtDynamic.min_connector_c_version)

        if not os.path.exists(connc_loc):
            log.error(err_invalid_loc, connc_loc)
            sys.exit(1)

        if os.path.isdir(connc_loc):
            # if directory, and no mysql_config is available, figure out the
            # lib/ and include/ folders from the the filesystem
            mysql_config = os.path.join(connc_loc, 'bin', 'mysql_config')
            if os.path.isfile(mysql_config) and \
                    os.access(mysql_config, os.X_OK):
                connc_loc = mysql_config
                log.debug("# connc_loc: {0}".format(connc_loc))
            else:
                # Probably using MS Windows
                myversionh = os.path.join(connc_loc, 'include',
                                          'mysql_version.h')

                if not os.path.exists(myversionh):
                    log.error("MySQL C API installation invalid "
                              "(mysql_version.h not found)")
                    sys.exit(1)
                else:
                    with open(myversionh, 'rb') as fp:
                        for line in fp.readlines():
                            if b'#define LIBMYSQL_VERSION' in line:
                                version = LooseVersion(
                                    line.split()[2].replace(b'"', b'').decode()
                                ).version
                                if tuple(version) < min_version:
                                    log.error(err_version);
                                    sys.exit(1)
                                break

                # On Windows we check libmysql.dll
                if os.name == 'nt':
                    lib = os.path.join(self.with_mysql_capi, 'lib',
                                       'libmysql.dll')
                    connc_64bit = win_dll_is64bit(lib)
                # On OSX we check libmysqlclient.dylib
                elif 'macos' in platform:
                    lib = os.path.join(self.with_mysql_capi, 'lib',
                                       'libmysqlclient.dylib')
                    connc_64bit = unix_lib_is64bit(lib)
                # On other Unices we check libmysqlclient (follow symlinks)
                elif os.name == 'posix':
                    connc_64bit = unix_lib_is64bit(connc_loc)
                else:
                    raise OSError("Unsupported platform: %s" % os.name)

                include_dirs = [os.path.join(connc_loc, 'include')]
                if os.name == 'nt':
                    libraries = ['libmysql']
                else:
                    libraries = ['-lmysqlclient']
                library_dirs = [os.path.join(connc_loc, 'lib')]

                log.debug("# connc_64bit: {0}".format(connc_64bit))
                if connc_64bit:
                    self.arch = 'x86_64'
                else:
                    self.arch = 'i386'

        # We were given the location of the mysql_config tool (not on Windows)
        if not os.name == 'nt' and os.path.isfile(connc_loc) \
                and os.access(connc_loc, os.X_OK):
            mysql_config = connc_loc
            # Check mysql_config
            mysql_info = mysql_c_api_info(mysql_config)
            log.debug("# mysql_info: {0}".format(mysql_info))

            if mysql_info['version'] < min_version:
                log.error(err_version)
                sys.exit(1)

            include_dirs = mysql_info['include_directories']
            libraries = mysql_info['libraries']
            library_dirs = mysql_info['link_directories']
            self._mysql_config_info = mysql_info
            self.arch = self._mysql_config_info['arch']
            connc_64bit = self.arch == 'x86_64'

        for include_dir in include_dirs:
            if not os.path.exists(include_dir):
                log.error(err_invalid_loc, connc_loc)
                sys.exit(1)

        # Set up the build_ext class
        self.include_dirs.extend(include_dirs)
        self.libraries.extend(libraries)
        self.library_dirs.extend(library_dirs)

        # We try to offer a nice message when the architecture of Python
        # is not the same as MySQL Connector/C binaries.
        print("# self.arch: {0}".format(self.arch))
        if ARCH_64BIT != connc_64bit:
            log.error("Python is {0}, but does not "
                      "match MySQL C API {1} architecture, "
                      "type: {2}"
                      "".format(py_arch,
                                '64-bit' if connc_64bit else '32-bit',
                                self.arch))
            sys.exit(1)

    def finalize_options(self):
        self.set_undefined_options(
            'install',
            ('extra_compile_args', 'extra_compile_args'),
            ('extra_link_args', 'extra_link_args'),
            ('with_mysql_capi', 'with_mysql_capi'),
            ('with_protobuf_include_dir', 'with_protobuf_include_dir'),
            ('with_protobuf_lib_dir', 'with_protobuf_lib_dir'),
            ('with_protoc', 'with_protoc'))

        self._copy_vendor_libraries()

        build_ext.finalize_options(self)

        print("# Python architecture: {0}".format(py_arch))
        print("# Python ARCH_64BIT: {0}".format(ARCH_64BIT))

        if self.with_mysql_capi:
            self._finalize_connector_c(self.with_mysql_capi)

        if not self.with_protobuf_include_dir:
            self.with_protobuf_include_dir = \
                os.environ.get("MYSQLXPB_PROTOBUF_INCLUDE_DIR")

        if not self.with_protobuf_lib_dir:
            self.with_protobuf_lib_dir = \
                os.environ.get("MYSQLXPB_PROTOBUF_LIB_DIR")

        if not self.with_protoc:
            self.with_protoc = os.environ.get("MYSQLXPB_PROTOC")

        self.with_mysqlxpb_cext = any((self.with_protobuf_include_dir,
                                       self.with_protobuf_lib_dir,
                                       self.with_protoc))

    def run_protoc(self):
        if self.with_protobuf_include_dir:
            print("# Protobuf include directory: {0}"
                  "".format(self.with_protobuf_include_dir))
        else:
            log.error("Unable to find Protobuf include directory.")
            sys.exit(1)

        if self.with_protobuf_lib_dir:
            print("# Protobuf library directory: {0}"
                  "".format(self.with_protobuf_lib_dir))
        else:
            log.error("Unable to find Protobuf library directory.")
            sys.exit(1)

        if self.with_protoc:
            print("# Protobuf protoc binary: {0}".format(self.with_protoc))
        else:
            log.error("Unable to find Protobuf protoc binary.")
            sys.exit(1)

        base_path = os.path.join(os.getcwd(), "src", "mysqlxpb", "mysqlx")
        command = [self.with_protoc, "-I"]
        if "protobuf-2.6" in self.with_protobuf_include_dir:
            command.extend([self.with_protobuf_include_dir, "-I"])
        command.append(os.path.join(base_path, "protocol"))
        command.extend(glob(os.path.join(base_path, "protocol", "*.proto")))
        command.append("--cpp_out={0}".format(base_path))
        log.info("# Running protoc command: {0}".format(" ".join(command)))
        check_call(command)

    def fix_compiler(self):
        cc = self.compiler
        if not cc:
            return

        if 'macosx-10.9' in get_platform():
            for needle in ['-mno-fused-madd']:
                try:
                    cc.compiler.remove(needle)
                    cc.compiler_so.remove(needle)
                except ValueError:
                    # We are removing, so OK when needle not there
                    pass

        for name, args in cc.__dict__.items():
            if not args or not isinstance(args, list):
                continue

            new_args = []
            enum_args = enumerate(args)
            for i, arg in enum_args:
                if arg == '-arch':
                    # Skip not needed architecture
                    if args[i+1] != self.arch:
                        next(enum_args)
                    else:
                        new_args.append(arg)
                else:
                    new_args.append(arg)

            try:
                cc.setattr(name, new_args)
            except AttributeError:
                # Old class
                cc.__dict__[name] = new_args

        # Add system headers to Extensions extra_compile_args
        sysheaders = [ '-isystem' + dir for dir in cc.include_dirs]
        for ext in self.extensions:
            # Add Protobuf include and library dirs
            if ext.name == "_mysqlxpb" and self.with_mysqlxpb_cext:
                ext.include_dirs.append(self.with_protobuf_include_dir)
                ext.library_dirs.append(self.with_protobuf_lib_dir)
                if os.name == 'nt':
                    ext.libraries.append("libprotobuf")
                else:
                    ext.libraries.append("protobuf")
                # Add -std=c++11 needed for Protobuf 3.6.1
                ext.extra_compile_args.append("-std=c++11")
            # Add extra compile args
            if self.extra_compile_args:
                ext.extra_compile_args.extend(self.extra_compile_args.split())
            # Add extra link args
            if self.extra_link_args and ext.name == "_mysql_connector":
                extra_link_args = self.extra_link_args.split()
                if platform.system() == "Linux":
                    extra_link_args += ["-Wl,-rpath,$ORIGIN/mysql-vendor"]
                ext.extra_link_args.extend(extra_link_args)
            # Add system headers
            for sysheader in sysheaders:
                if sysheader not in ext.extra_compile_args:
                    ext.extra_compile_args.append(sysheader)

        # Stop warnings about unknown pragma
        if os.name != 'nt':
            ext.extra_compile_args.append('-Wno-unknown-pragmas')

    def run(self):
        """Run the command"""
        # Generate docs/INFO_SRC
        git_info = get_git_info()
        if git_info:
            with open(os.path.join("docs", "INFO_SRC"), "w") as info_src:
                info_src.write("version: {}\n".format(VERSION_TEXT))
                if git_info:
                    info_src.write("branch: {}\n".format(git_info["branch"]))
                    if git_info.get("date"):
                        info_src.write("date: {}\n".format(git_info["date"]))
                    info_src.write("commit: {}\n".format(git_info["commit"]))
                    info_src.write("short: {}\n".format(git_info["short"]))

        if not self.with_mysql_capi and not self.with_mysqlxpb_cext:
            return

        if self.with_mysql_capi:
            mysql_version = None
            if os.name != "nt":
                # Get MySQL info
                mysql_capi = self.with_mysql_capi
                mysql_config = os.path.join(mysql_capi, "bin", "mysql_config") \
                    if os.path.isdir(mysql_capi) else mysql_capi
                mysql_info = mysql_c_api_info(mysql_config)
                mysql_version = "{}.{}.{}".format(*mysql_info["version"][:3])

            # Generate docs/INFO_BIN
            now = NOW.strftime("%Y-%m-%d %H:%M:%S %z")
            with open(os.path.join("docs", "INFO_BIN"), "w") as info_bin:
                info_bin.write("build-date: {}\n".format(now))
                info_bin.write("os-info: {}\n".format(platform.platform()))
                if mysql_version:
                    info_bin.write("mysql-version: {}\n".format(mysql_version))

        if os.name == 'nt':
            for ext in self.extensions:
                # Add Protobuf include and library dirs
                if ext.name == "_mysqlxpb" and self.with_mysqlxpb_cext:
                    ext.include_dirs.append(self.with_protobuf_include_dir)
                    ext.library_dirs.append(self.with_protobuf_lib_dir)
                    ext.libraries.append("libprotobuf")
                # Add extra compile args
                if self.extra_compile_args:
                    ext.extra_compile_args.extend(self.extra_compile_args.split())
                # Add extra link args
                if self.extra_link_args and ext.name != "_mysqlxpb":
                    ext.extra_link_args.extend(self.extra_link_args.split())
            if self.with_mysqlxpb_cext:
                self.run_protoc()
            build_ext.run(self)
        else:
            self.real_build_extensions = self.build_extensions
            self.build_extensions = lambda: None
            build_ext.run(self)
            self.fix_compiler()
            if self.with_mysqlxpb_cext:
                self.run_protoc()
            self.real_build_extensions()

            if self.with_mysql_capi:
                copy_openssl = mysql_info["version"] >= (8, 0, 6)
                if platform.system() == "Darwin" and copy_openssl:
                    libssl, libcrypto = self._get_posix_openssl_libs()
                    cmd_libssl = [
                        "install_name_tool", "-change", libssl,
                        "@loader_path/mysql-vendor/{0}".format(libssl),
                        build_ext.get_ext_fullpath(self, "_mysql_connector")
                    ]
                    log.info("Executing: {0}".format(" ".join(cmd_libssl)))
                    proc = Popen(cmd_libssl, stdout=PIPE,
                                 universal_newlines=True)
                    stdout, _ = proc.communicate()

                    cmd_libcrypto = [
                        "install_name_tool", "-change", libcrypto,
                        "@loader_path/mysql-vendor/{0}".format(libcrypto),
                        build_ext.get_ext_fullpath(self, "_mysql_connector")
                    ]
                    log.info("Executing: {0}".format(" ".join(cmd_libcrypto)))
                    proc = Popen(cmd_libcrypto, stdout=PIPE,
                                 universal_newlines=True)
                    stdout, _ = proc.communicate()

        if self.with_mysql_capi and self.compiler:
            # Add compiler information to docs/INFO_BIN
            if hasattr(self.compiler, "compiler_so"):
                compiler = self.compiler.compiler_so[0]
                with open(os.path.join("docs", "INFO_BIN"), "a") as info_bin:
                    info_bin.write("compiler: {}\n".format(compiler))


class BuildExtStatic(BuildExtDynamic):

    """Build and Link libraries statically with the C Extensions"""

    user_options = build_ext.user_options + CEXT_OPTIONS

    def finalize_options(self):
        self._copy_vendor_libraries()

        install_obj = self.distribution.get_command_obj('install')
        install_obj.with_mysql_capi = self.with_mysql_capi
        install_obj.with_protobuf_include_dir = self.with_protobuf_include_dir
        install_obj.with_protobuf_lib_dir = self.with_protobuf_lib_dir
        install_obj.with_protoc = self.with_protoc
        install_obj.extra_compile_args = self.extra_compile_args
        install_obj.extra_link_args = self.extra_link_args
        install_obj.static = True

        options_pairs = []
        if not self.extra_compile_args:
            options_pairs.append(('extra_compile_args', 'extra_compile_args'))
        if not self.extra_link_args:
            options_pairs.append(('extra_link_args', 'extra_link_args'))
        if not self.with_mysql_capi:
            options_pairs.append(('with_mysql_capi', 'with_mysql_capi'))
        if not self.with_protobuf_include_dir:
            options_pairs.append(('with_protobuf_include_dir',
                                  'with_protobuf_include_dir'))
        if not self.with_protobuf_lib_dir:
            options_pairs.append(('with_protobuf_lib_dir',
                                  'with_protobuf_lib_dir'))
        if not self.with_protoc:
            options_pairs.append(('with_protoc', 'with_protoc'))
        if options_pairs:
            self.set_undefined_options('install', *options_pairs)

        build_ext.finalize_options(self)

        print("# Python architecture: {0}".format(py_arch))
        print("# Python ARCH_64BIT: {0}".format(ARCH_64BIT))

        self.connc_lib = os.path.join(self.build_temp, 'connc', 'lib')
        self.connc_include = os.path.join(self.build_temp, 'connc', 'include')
        self.protobuf_lib = os.path.join(self.build_temp, 'protobuf', 'lib')
        self.protobuf_include = os.path.join(self.build_temp, 'protobuf', 'include')

        self.with_mysqlxpb_cext = any((self.with_protobuf_include_dir,
                                       self.with_protobuf_lib_dir,
                                       self.with_protoc))
        if self.with_mysql_capi:
            self._finalize_connector_c(self.with_mysql_capi)

        if self.with_mysqlxpb_cext:
            self._finalize_protobuf()

    def _finalize_connector_c(self, connc_loc):
        if not os.path.isdir(connc_loc):
            log.error("MySQL C API should be a directory")
            sys.exit(1)

        log.info("Copying MySQL libraries")
        copy_tree(os.path.join(connc_loc, 'lib'), self.connc_lib)
        log.info("Copying MySQL header files")
        copy_tree(os.path.join(connc_loc, 'include'), self.connc_include)

        # Remove all but static libraries to force static linking
        if os.name == 'posix':
            log.info("Removing non-static MySQL libraries from %s" % self.connc_lib)
            for lib_file in os.listdir(self.connc_lib):
                lib_file_path = os.path.join(self.connc_lib, lib_file)
                if os.path.isfile(lib_file_path) and not lib_file.endswith('.a'):
                    os.unlink(os.path.join(self.connc_lib, lib_file))
        elif os.name == 'nt':
            self.include_dirs.extend([self.connc_include])
            self.libraries.extend(['libmysql'])
            self.library_dirs.extend([self.connc_lib])

    def _finalize_protobuf(self):
        if not self.with_protobuf_include_dir:
            self.with_protobuf_include_dir = \
                os.environ.get("MYSQLXPB_PROTOBUF_INCLUDE_DIR")

        if not self.with_protobuf_lib_dir:
            self.with_protobuf_lib_dir = \
                os.environ.get("MYSQLXPB_PROTOBUF_LIB_DIR")

        if not self.with_protoc:
            self.with_protoc = os.environ.get("MYSQLXPB_PROTOC")

        if self.with_protobuf_include_dir:
            print("# Protobuf include directory: {0}"
                  "".format(self.with_protobuf_include_dir))
            if not os.path.isdir(self.with_protobuf_include_dir):
                log.error("Protobuf include dir should be a directory")
                sys.exit(1)
        else:
            log.error("Unable to find Protobuf include directory.")
            sys.exit(1)

        if self.with_protobuf_lib_dir:
            print("# Protobuf library directory: {0}"
                  "".format(self.with_protobuf_lib_dir))
            if not os.path.isdir(self.with_protobuf_lib_dir):
                log.error("Protobuf library dir should be a directory")
                sys.exit(1)
        else:
            log.error("Unable to find Protobuf library directory.")
            sys.exit(1)

        if self.with_protoc:
            print("# Protobuf protoc binary: {0}".format(self.with_protoc))
            if not os.path.isfile(self.with_protoc):
                log.error("Protobuf protoc binary is not valid.")
                sys.exit(1)
        else:
            log.error("Unable to find Protobuf protoc binary.")
            sys.exit(1)

        if not os.path.exists(self.protobuf_lib):
            os.makedirs(self.protobuf_lib)

        if not os.path.exists(self.protobuf_include):
            os.makedirs(self.protobuf_include)

        log.info("Copying Protobuf libraries")
        lib_files = glob(os.path.join(self.with_protobuf_lib_dir, "libprotobuf*"))
        for lib_file in lib_files:
            if os.path.isfile(lib_file):
                log.info("copying {0} -> {1}".format(lib_file, self.protobuf_lib))
                shutil.copy2(lib_file, self.protobuf_lib)

        log.info("Copying Protobuf header files")
        copy_tree(self.with_protobuf_include_dir, self.protobuf_include)

        # Remove all but static libraries to force static linking
        if os.name == "posix":
            log.info("Removing non-static Protobuf libraries from {0}"
                     "".format(self.protobuf_lib))
            for lib_file in os.listdir(self.protobuf_lib):
                lib_file_path = os.path.join(self.protobuf_lib, lib_file)
                if os.path.isfile(lib_file_path) and \
                   not lib_file.endswith((".a", ".dylib",)):
                    os.unlink(os.path.join(self.protobuf_lib, lib_file))

    def fix_compiler(self):
        BuildExtDynamic.fix_compiler(self)

        include_dirs = []
        library_dirs = []
        libraries = []

        if os.name == 'posix':
            include_dirs.append(self.connc_include)
            library_dirs.append(self.connc_lib)
            if self.with_mysql_capi:
                libraries.append("mysqlclient")

            # As we statically link and the "libmysqlclient.a" library
            # carry no information what it depends on, we need to
            # manually add library dependencies here.
            if platform.system() not in ["Darwin", "Windows"]:
                libraries.append("rt")

        for ext in self.extensions:
            if ext.name == "_mysql_connector":
                ext.include_dirs.extend(include_dirs)
                ext.library_dirs.extend(library_dirs)
                ext.libraries.extend(libraries)
            elif ext.name == "_mysqlxpb" \
                 and platform.system() not in ["Darwin", "Windows"]:
                ext.libraries.append("rt")
            # Add extra compile args
            if self.extra_compile_args:
                ext.extra_compile_args.extend(self.extra_compile_args.split())
            # Add extra link args
            if self.extra_link_args and ext.name != "_mysqlxpb":
                ext.extra_link_args.extend(self.extra_link_args.split())


class InstallLib(install_lib):

    user_options = install_lib.user_options + CEXT_OPTIONS + INSTALL_OPTIONS

    boolean_options = ['byte-code-only']

    def initialize_options(self):
        install_lib.initialize_options(self)
        self.byte_code_only = None

    def finalize_options(self):
        install_lib.finalize_options(self)
        self.set_undefined_options('install',
                                   ('byte_code_only', 'byte_code_only'))
        self.set_undefined_options('build', ('build_base', 'build_dir'))

    def run(self):
        self.build()
        outfiles = [
            filename for filename in self.install() if filename.endswith(".py")
        ]

        # (Optionally) compile .py to .pyc
        if outfiles is not None and self.distribution.has_pure_modules():
            self.byte_compile(outfiles)

        if self.byte_code_only:
            if get_python_version().startswith("3"):
                for base, _, files in os.walk(self.install_dir):
                    for filename in files:
                        if filename.endswith(".pyc"):
                            new_name = "{0}.pyc".format(filename.split(".")[0])
                            os.rename(os.path.join(base, filename),
                                      os.path.join(base, "..", new_name))
                for base, _, files in os.walk(self.install_dir):
                    if base.endswith("__pycache__"):
                        os.rmdir(base)
            for source_file in outfiles:
                log.info("Removing %s", source_file)
                os.remove(source_file)


class Install(install):

    """Install Connector/Python C Extension"""

    description = "install MySQL Connector/Python"

    user_options = install.user_options + CEXT_OPTIONS + INSTALL_OPTIONS + \
                   CEXT_STATIC_OPTIONS

    boolean_options = ['byte-code-only', 'static', 'is-wheel']
    need_ext = False

    def initialize_options(self):
        install.initialize_options(self)
        self.extra_compile_args = None
        self.extra_link_args = None
        self.with_mysql_capi = None
        self.with_mysqlxpb_cext = False
        self.with_protobuf_include_dir = None
        self.with_protobuf_lib_dir = None
        self.with_protoc = None
        self.byte_code_only = None
        self.static = None

    def finalize_options(self):
        if self.static:
            log.info("Linking C Extension statically with libraries")
            self.distribution.cmdclass['build_ext'] = BuildExtStatic

        if self.byte_code_only is None:
            self.byte_code_only = False

        build_ext_obj = self.distribution.get_command_obj('build_ext')
        build_ext_obj.with_mysql_capi = self.with_mysql_capi
        build_ext_obj.with_protobuf_include_dir = self.with_protobuf_include_dir
        build_ext_obj.with_protobuf_lib_dir = self.with_protobuf_lib_dir
        build_ext_obj.with_protoc = self.with_protoc
        build_ext_obj.extra_compile_args = self.extra_compile_args
        build_ext_obj.extra_link_args = self.extra_link_args
        build_ext_obj.static = self.static

        if self.with_mysql_capi:
            self.need_ext = True

        if not self.need_ext:
            remove_cext(self.distribution, "_mysql_connector")

        self.with_mysqlxpb_cext = all((self.with_protobuf_include_dir,
                                       self.with_protobuf_lib_dir,
                                       self.with_protoc))

        if not self.with_mysqlxpb_cext:
            remove_cext(self.distribution, "_mysqlxpb")

        install.finalize_options(self)

    def run(self):
        if not self.need_ext:
            log.info("Not Installing MySQL C Extension")
        else:
            log.info("Installing MySQL C Extension")
        install.run(self)
