# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2014, Oracle and/or its affiliates. All rights reserved.

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

"""Implements the DistUtils command 'build_ext'
"""

from distutils.command.build_ext import build_ext
from distutils.command.install import install
from distutils.command.install_lib import install_lib
from distutils.errors import DistutilsExecError
from distutils.util import get_platform
from distutils.dir_util import copy_tree
from distutils import log
from glob import glob
import os
import shlex
import struct
from subprocess import Popen, PIPE, STDOUT
import sys

ARCH_64BIT = sys.maxsize > 2**32  # Works with Python 2.6 and greater

CEXT_OPTIONS = [
    ('with-mysql-capi=', None,
     "Location of MySQL C API installation or path to mysql_config"),
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
        mysqlclient_lib = None
        for root, dirs, files in os.walk(lib_file):
            for filename in files:
                filepath = os.path.join(root, filename)
                if filename.startswith('libmysqlclient') and \
                    not os.path.islink(filepath) and \
                    '_r' not in filename:
                    mysqlclient_lib = filepath
                    break
            if mysqlclient_lib:
                break
        lib_file = mysqlclient_lib

    prc = Popen(['file', lib_file], stdin=PIPE, stderr=STDOUT, stdout=PIPE)
    stdout = prc.communicate()[0]

    if 'x86_64' in stdout or 'x86-64' in stdout:
        return True

    return False


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

    info = {}
    for option, line in zip(options, stdout.split('\n')):
        info[option] = line.strip()

    info['version'] = tuple([int(v) for v in info['version'].split('.')[0:3]])
    libs = shlex.split(info['libs'])
    info['lib_dir'] = libs[0].replace('-L', '')
    info['libs'] = [ lib.replace('-l', '') for lib in libs[1:] ]

    libs = shlex.split(info['libs_r'])
    info['lib_r_dir'] = libs[0].replace('-L', '')
    info['libs_r'] = [ lib.replace('-l', '') for lib in libs[1:] ]

    info['include'] = info['include'].replace('-I', '')

    # Try to figure out the architecture
    info['arch'] = None
    if os.name == 'posix':
        pathname = os.path.join(info['lib_dir'], 'lib' + info['libs'][0]) + '*'
        lib = glob(pathname)[0]

        stdout = None
        try:
            proc = Popen(['file', lib], stdout=PIPE, universal_newlines=True)
            stdout, _ = proc.communicate()
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


def remove_cext(distribution):
    """Remove the C Extension from the distribution

    This function can be useful in Distutils commands for creating
    pure Python modules.
    """
    to_remove = []
    for ext_mod in distribution.ext_modules:
        if ext_mod.name == '_mysql_connector':
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
        self.with_mysql_capi = None

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
            else:
                # Probably using MS Windows
                myconfigh = os.path.join(connc_loc, 'include', 'my_config.h')

                if not os.path.exists(myconfigh):
                    log.error("MySQL C API installation invalid "
                              "(my_config.h not found)")
                    sys.exit(1)
                else:
                    with open(myconfigh, 'rb') as fp:
                        for line in fp.readlines():
                            if '#define VERSION' in line:
                                version = tuple([
                                    int(v) for v in
                                    line.split()[2].replace('"', '').split('.')
                                ])
                                if version < min_version:
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

                include_dir = os.path.join(connc_loc, 'include')
                if os.name == 'nt':
                    libraries = ['libmysql']
                else:
                    libraries = ['-lmysqlclient']
                library_dirs = os.path.join(connc_loc, 'lib')

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

            if myc_info['version'] < min_version:
                log.error(err_version)
                sys.exit(1)

            include_dir = myc_info['include']
            libraries = myc_info['libs']
            library_dirs = myc_info['lib_dir']
            self._mysql_config_info = myc_info
            self.arch = self._mysql_config_info['arch']
            connc_64bit = self.arch == 'x86_64'

        if not os.path.exists(include_dir):
            log.error(err_invalid_loc, connc_loc)
            sys.exit(1)

        # Set up the build_ext class
        self.include_dirs.append(include_dir)
        self.libraries.extend(libraries)
        self.library_dirs.append(library_dirs)

        # We try to offer a nice message when the architecture of Python
        # is not the same as MySQL Connector/C binaries.
        py_arch = '64-bit' if ARCH_64BIT else '32-bit'
        if ARCH_64BIT != connc_64bit:
            log.error("Python is {0}, but does not "
                      "match MySQL C API {1} architecture".format(
                py_arch, '64-bit' if connc_64bit else '32-bit'))
            sys.exit(1)

    def finalize_options(self):
        self.set_undefined_options('install',
                                   ('with_mysql_capi', 'with_mysql_capi'))

        build_ext.finalize_options(self)

        if self.with_mysql_capi:
            self._finalize_connector_c(self.with_mysql_capi)

    def fix_compiler(self):
        platform = get_platform()

        cc = self.compiler
        if not cc:
            return

        if 'macosx-10.9' in platform:
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
            for sysheader in sysheaders:
                if sysheader not in ext.extra_compile_args:
                    ext.extra_compile_args.append(sysheader)

        # Stop warnings about unknown pragma
        if os.name != 'nt':
            ext.extra_compile_args.append('-Wno-unknown-pragmas')

    def run(self):
        """Run the command"""
        if not self.with_mysql_capi:
            return

        if os.name == 'nt':
            build_ext.run(self)
        else:
            self.real_build_extensions = self.build_extensions
            self.build_extensions = lambda: None
            build_ext.run(self)
            self.fix_compiler()
            self.real_build_extensions()


class BuildExtStatic(BuildExtDynamic):

    """Build and Link libraries statically with the C Extensions"""

    user_options = build_ext.user_options + CEXT_OPTIONS

    def finalize_options(self):
        self.set_undefined_options('install',
                                   ('with_mysql_capi', 'with_mysql_capi'))

        build_ext.finalize_options(self)
        self.connc_lib = os.path.join(self.build_temp, 'connc', 'lib')
        self.connc_include = os.path.join(self.build_temp, 'connc', 'include')

        if self.with_mysql_capi:
            self._finalize_connector_c(self.with_mysql_capi)

    def _finalize_connector_c(self, connc_loc):
        if not os.path.isdir(connc_loc):
            log.error("MySQL C API should be a directory")
            sys.exit(1)

        copy_tree(os.path.join(connc_loc, 'lib'), self.connc_lib)
        copy_tree(os.path.join(connc_loc, 'include'), self.connc_include)

        for lib_file in os.listdir(self.connc_lib):
            if os.name == 'posix' and not lib_file.endswith('.a'):
                os.unlink(os.path.join(self.connc_lib, lib_file))

    def fix_compiler(self):
        BuildExtDynamic.fix_compiler(self)

        extra_compile_args = []
        extra_link_args = []

        if os.name == 'posix':
            extra_compile_args = [
                '-I%s' % self.connc_include
            ]
            extra_link_args = [
                #'-lstdc++',
                '-L%s' % self.connc_lib,
                '-lmysqlclient',
            ]

        if not extra_compile_args and not extra_link_args:
            return

        for ext in self.extensions:
            if extra_compile_args:
                ext.extra_compile_args.extend(extra_compile_args)
            if extra_link_args:
                ext.extra_link_args.extend(extra_link_args)



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
        outfiles = self.install()

        # (Optionally) compile .py to .pyc
        if outfiles is not None and self.distribution.has_pure_modules():
            self.byte_compile(outfiles)

        if self.byte_code_only:
            for source_file in outfiles:
                if os.path.join('mysql', '__init__.py') in source_file:
                    continue
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
        self.with_mysql_capi = None
        self.byte_code_only = None
        self.static = None

    def finalize_options(self):
        install.finalize_options(self)

        if self.static:
            self.distribution.cmdclass['build_ext'] = BuildExtStatic

        if self.byte_code_only is None:
            self.byte_code_only = False

        if self.with_mysql_capi:
            build_ext = self.distribution.get_command_obj('build_ext')
            build_ext.with_mysql_capi = self.with_mysql_capi
            build = self.distribution.get_command_obj('build_ext')
            build.with_mysql_capi = self.with_mysql_capi
            self.need_ext = True

    def run(self):
        if not self.need_ext:
            remove_cext(self.distribution)
            # We install pure Python code in purelib location when no
            # extension is build
            self.install_lib = self.install_purelib
        else:
            log.info("installing C Extension")
        install.run(self)
