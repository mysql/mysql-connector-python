# Copyright (c) 2014, 2018, Oracle and/or its affiliates. All rights reserved.
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

from distutils.command.build_ext import build_ext
from distutils.command.install import install
from distutils.command.install_lib import install_lib
from distutils.errors import DistutilsExecError
from distutils.util import get_platform
from distutils.version import LooseVersion
from distutils.dir_util import copy_tree, mkpath
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


def parse_mysql_config_info(options, stdout):
    log.debug("# stdout: {0}".format(stdout))
    info = {}
    for option, line in zip(options, stdout.split('\n')):
        log.debug("# option: {0}".format(option))
        log.debug("# line: {0}".format(line))
        info[option] = line.strip()

    ver = info['version']
    if '-' in ver:
        ver, _ = ver.split('-', 2)

    info['version'] = tuple([int(v) for v in ver.split('.')[0:3]])
    libs = shlex.split(info['libs'])
    if ',' in libs[1]:
        libs.pop(1)
    info['lib_dir'] = libs[0].replace('-L', '')
    info['libs'] = [ lib.replace('-l', '') for lib in libs[1:] ]
    if platform.uname()[0] == 'SunOS':
        info['lib_dir'] = info['lib_dir'].replace('-R', '')
        info['libs'] = [lib.replace('-R', '') for lib in info['libs']]
    log.debug("# info['libs']: ")
    for lib in info['libs']:
        log.debug("#   {0}".format(lib))
    libs = shlex.split(info['libs_r'])
    if ',' in libs[1]:
        libs.pop(1)
    info['lib_r_dir'] = libs[0].replace('-L', '')
    info['libs_r'] = [ lib.replace('-l', '') for lib in libs[1:] ]
    info['include'] = [x.strip() for x in info['include'].split('-I')[1:]]

    return info


def get_mysql_config_info(mysql_config):
    """Get MySQL information using mysql_config tool

    Returns a dict.
    """
    options = ['cflags', 'include', 'libs', 'libs_r', 'plugindir', 'version']

    cmd = [mysql_config] + [ "--{0}".format(opt) for opt in options ]

    try:
        proc = Popen(cmd, stdout=PIPE, universal_newlines=True)
        stdout, _ = proc.communicate()
    except OSError as exc:
        raise DistutilsExecError("Failed executing mysql_config: {0}".format(
            str(exc)))

    info = parse_mysql_config_info(options, stdout)

    # Try to figure out the architecture
    info['arch'] = None
    if os.name == 'posix':
        if platform.uname()[0] == 'SunOS':
            print("info['lib_dir']: {0}".format(info['lib_dir']))
            print("info['libs'][0]: {0}".format(info['libs'][0]))
            pathname = os.path.abspath(os.path.join(info['lib_dir'],
                                                    'lib',
                                                    info['libs'][0])) + '/*'
        else:
            pathname = os.path.join(info['lib_dir'],
                                    'lib' + info['libs'][0]) + '*'
        print("# Looking mysqlclient_lib at path: {0}".format(pathname))
        log.debug("# searching mysqlclient_lib at: %s", pathname)
        libs = glob(pathname)
        mysqlclient_libs = []
        for filepath in libs:
            _, filename = os.path.split(filepath)
            log.debug("#  filename {0}".format(filename))
            if filename.startswith('libmysqlclient') and \
               not os.path.islink(filepath) and \
               '_r' not in filename and \
               '.a' not in filename:
                mysqlclient_libs.append(filepath)
        mysqlclient_libs.sort()

        stdout = None
        try:
            log.debug("# mysqlclient_lib: {0}".format(mysqlclient_libs[-1]))
            for mysqlclient_lib in mysqlclient_libs:
                log.debug("#+   {0}".format(mysqlclient_lib))
            log.debug("# tested mysqlclient_lib[-1]: "
                      "{0}".format(mysqlclient_libs[-1]))
            if platform.uname()[0] == 'SunOS':
                print("mysqlclient_lib: {0}".format(mysqlclient_libs[-1]))
                cmd_list = ['file', mysqlclient_libs[-1]]
            else:
                cmd_list = ['file', '-L', mysqlclient_libs[-1]]
            proc = Popen(cmd_list, stdout=PIPE,
                         universal_newlines=True)
            stdout, _ = proc.communicate()
            stdout = stdout.split(':')[1]
        except OSError as exc:
            raise DistutilsExecError(
                "Although the system seems POSIX, the file-command could not "
                "be executed: {0}".format(str(exc)))

        if stdout:
            if '64' in stdout:
                info['arch'] = "x86_64"
            else:
                info['arch'] = "i386"
        else:
            raise DistutilsExecError(
                "Failed getting out put from the file-command"
            )
    else:
        raise DistutilsExecError(
            "Cannot determine architecture on {0} systems".format(os.name))

    return info


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

        data_files = []
        vendor_libs = []

        if os.name == "nt":
            mysql_capi = os.path.join(self.with_mysql_capi, "bin")
            vendor_libs.append((mysql_capi, ["ssleay32.dll", "libeay32.dll"]))
            vendor_folder = ""
            # Bundle libmysql.dll
            src = os.path.join(self.with_mysql_capi, "lib", "libmysql.dll")
            dst = os.getcwd()
            log.info("copying {0} -> {1}".format(src, dst))
            shutil.copy(src, dst)
            data_files.append("libmysql.dll")
        else:
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
                library_dirs = os.path.join(connc_loc, 'lib')

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
            myc_info = get_mysql_config_info(mysql_config)
            log.debug("# myc_info: {0}".format(myc_info))

            if myc_info['version'] < min_version:
                log.error(err_version)
                sys.exit(1)

            include_dirs = myc_info['include']
            libraries = myc_info['libs']
            library_dirs = myc_info['lib_dir']
            self._mysql_config_info = myc_info
            self.arch = self._mysql_config_info['arch']
            connc_64bit = self.arch == 'x86_64'

        for include_dir in include_dirs:
            if not os.path.exists(include_dir):
                log.error(err_invalid_loc, connc_loc)
                sys.exit(1)

        # Set up the build_ext class
        self.include_dirs.extend(include_dirs)
        self.libraries.extend(libraries)
        self.library_dirs.append(library_dirs)

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
        if not self.with_mysql_capi and not self.with_mysqlxpb_cext:
            return
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

            if platform.system() == "Darwin":
                libssl, libcrypto = self._get_posix_openssl_libs()
                cmd_libssl = [
                    "install_name_tool", "-change", libssl,
                    "@loader_path/mysql-vendor/{0}".format(libssl),
                    build_ext.get_ext_fullpath(self, "_mysql_connector")
                ]
                log.info("Executing: {0}".format(" ".join(cmd_libssl)))
                proc = Popen(cmd_libssl, stdout=PIPE, universal_newlines=True)
                stdout, _ = proc.communicate()

                cmd_libcrypto = [
                    "install_name_tool", "-change", libcrypto,
                    "@loader_path/mysql-vendor/{0}".format(libcrypto),
                    build_ext.get_ext_fullpath(self, "_mysql_connector")
                ]
                log.info("Executing: {0}".format(" ".join(cmd_libcrypto)))
                proc = Popen(cmd_libcrypto, stdout=PIPE, universal_newlines=True)
                stdout, _ = proc.communicate()


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

    boolean_options = ['byte-code-only', 'static']
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
