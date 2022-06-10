# Copyright (c) 2020, 2022, Oracle and/or its affiliates.
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

"""Connector/Python packaging system."""

import logging
import os
import platform
import shutil
import sys
import tempfile

from glob import glob
from setuptools.command.build_ext import build_ext
from distutils import log
from distutils.command.install import install
from distutils.command.install_lib import install_lib
from distutils.core import Command
from distutils.dir_util import mkpath, remove_tree
from distutils.sysconfig import get_config_vars, get_python_version
from distutils.version import LooseVersion
from subprocess import check_call, Popen, PIPE

from .utils import (ARCH, ARCH_64BIT, mysql_c_api_info, write_info_src,
                    write_info_bin)


# Load version information
VERSION = [999, 0, 0, "a", 0]
VERSION_TEXT = "999.0.0"
VERSION_EXTRA = ""
EDITION = ""
version_py = os.path.join("lib", "mysql", "connector", "version.py")
with open(version_py, "rb") as fp:
    exec(compile(fp.read(), version_py, "exec"))

if "MACOSX_DEPLOYMENT_TARGET" in get_config_vars():
    get_config_vars()["MACOSX_DEPLOYMENT_TARGET"] = "11.0"

COMMON_USER_OPTIONS = [
    ("byte-code-only", None,
     "remove Python .py files; leave byte code .pyc only"),
    ("edition=", None,
     "Edition added in the package name after the version"),
    ("label=", None,
     "label added in the package name after the name"),
    ("debug", None,
     "turn debugging on"),
    ("keep-temp", "k",
     "keep the pseudo-installation tree around after creating the "
     "distribution archive"),
]

CEXT_OPTIONS = [
    ("with-mysql-capi=", None,
     "location of MySQL C API installation or path to mysql_config"),
    ("with-openssl-include-dir=", None,
     "location of OpenSSL include directory"),
    ("with-openssl-lib-dir=", None,
     "location of OpenSSL library directory"),
    ("with-protobuf-include-dir=", None,
     "location of Protobuf include directory"),
    ("with-protobuf-lib-dir=", None,
     "location of Protobuf library directory"),
    ("with-protoc=", None,
     "location of Protobuf protoc binary"),
    ("extra-compile-args=", None,
     "extra compile args"),
    ("extra-link-args=", None,
     "extra link args"),
    ("skip-vendor", None,
     "Skip bundling vendor libraries"),
]

LOGGER = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(levelname)s[%(name)s]: %(message)s")
)
LOGGER.addHandler(handler)
LOGGER.setLevel(logging.WARNING)


class BaseCommand(Command):
    """Base command class for Connector/Python."""

    user_options = COMMON_USER_OPTIONS + CEXT_OPTIONS
    boolean_options = ["debug", "byte_code_only", "keep_temp", "skip_vendor"]

    with_mysql_capi = None
    with_mysqlxpb_cext = False

    with_openssl_include_dir = None
    with_openssl_lib_dir = None

    with_protobuf_include_dir = None
    with_protobuf_lib_dir = None
    with_protoc = None

    extra_compile_args = None
    extra_link_args = None

    byte_code_only = False
    edition = None
    label = None
    debug = False
    keep_temp = False
    skip_vendor = False
    build_base = None
    log = LOGGER
    vendor_folder = os.path.join("lib", "mysql", "vendor")

    _mysql_info = {}
    _build_mysql_lib_dir = None
    _build_protobuf_lib_dir = None

    def initialize_options(self):
        """Initialize the options."""
        self.with_mysql_capi = None
        self.with_mysqlxpb_cext = False
        self.with_openssl_include_dir = None
        self.with_openssl_lib_dir = None
        self.with_protobuf_include_dir = None
        self.with_protobuf_lib_dir = None
        self.with_protoc = None
        self.extra_compile_args = None
        self.extra_link_args = None
        self.byte_code_only = False
        self.edition = None
        self.label = None
        self.debug = False
        self.keep_temp = False
        self.skip_vendor = False

    def finalize_options(self):
        """Finalize the options."""
        if self.debug:
            self.log.setLevel(logging.DEBUG)
            log.set_threshold(1)  # Set Distutils logging level to DEBUG

        cmd_build_ext = self.distribution.get_command_obj("build_ext")
        cmd_build_ext.with_mysql_capi = (
            self.with_mysql_capi or
            os.environ.get("MYSQL_CAPI")
        )
        cmd_build_ext.with_openssl_include_dir = (
            self.with_openssl_include_dir or
            os.environ.get("OPENSSL_INCLUDE_DIR")
        )
        cmd_build_ext.with_openssl_lib_dir = (
            self.with_openssl_lib_dir or
            os.environ.get("OPENSSL_LIB_DIR")
        )
        cmd_build_ext.with_protobuf_include_dir = (
            self.with_protobuf_include_dir or
            os.environ.get("PROTOBUF_INCLUDE_DIR")
        )
        cmd_build_ext.with_protobuf_lib_dir = (
            self.with_protobuf_lib_dir or
            os.environ.get("PROTOBUF_LIB_DIR")
        )
        cmd_build_ext.with_protoc = (
            self.with_protoc or
            os.environ.get("PROTOC")
        )
        cmd_build_ext.extra_compile_args = (
            self.extra_compile_args or
            os.environ.get("EXTRA_COMPILE_ARGS")
        )
        cmd_build_ext.extra_link_args = (
            self.extra_link_args or
            os.environ.get("EXTRA_LINK_ARGS")
        )
        cmd_build_ext.skip_vendor = (
            self.skip_vendor or
            os.environ.get("SKIP_VENDOR")
        )
        if not cmd_build_ext.skip_vendor:
            self._copy_vendor_libraries()

    def remove_temp(self):
        """Remove temporary build files."""
        if not self.keep_temp:
            cmd_build = self.get_finalized_command("build")
            remove_tree(cmd_build.build_base, dry_run=self.dry_run)
            vendor_folder = os.path.join(os.getcwd(), self.vendor_folder)
            if os.path.exists(vendor_folder):
                remove_tree(vendor_folder)
            elif os.name == "nt":
                if ARCH == "64-bit":
                    libraries = ["libmysql.dll", "libssl-1_1-x64.dll",
                                 "libcrypto-1_1-x64.dll"]
                else:
                    libraries = ["libmysql.dll", "libssl-1_1.dll",
                                 "libcrypto-1_1.dll"]
                for filename in libraries:
                    dll_file = os.path.join(os.getcwd(), filename)
                    if os.path.exists(dll_file):
                        os.unlink(dll_file)

    def _get_openssl_libs(self):
        libssl = glob(os.path.join(
            self.with_openssl_lib_dir, "libssl.*.*.*"))
        libcrypto = glob(os.path.join(
            self.with_openssl_lib_dir, "libcrypto.*.*.*"))
        if not libssl or not libcrypto:
            self.log.error("Unable to find OpenSSL libraries in '%s'",
                           self.with_openssl_lib_dir)
            sys.exit(1)
        return (os.path.basename(libssl[0]), os.path.basename(libcrypto[0]))

    def _copy_vendor_libraries(self):
        vendor_libs = []

        if os.name == "posix":
            # Bundle OpenSSL libs
            if self.with_openssl_lib_dir:
                libssl, libcrypto = self._get_openssl_libs()
                vendor_libs.append(
                    (self.with_openssl_lib_dir, [libssl, libcrypto]))

        # Plugins
        bundle_plugin_libs = False
        if self.with_mysql_capi:
            plugin_ext = "dll" if os.name == "nt" else "so"
            plugin_path = os.path.join(self.with_mysql_capi, "lib", "plugin")
            plugin_list = [
                (
                    "LDAP",
                    "authentication_ldap_sasl_client.{}".format(plugin_ext),
                ),
                (
                    "Kerberos",
                    "authentication_kerberos_client.{}".format(plugin_ext),
                ),
                (
                    "OCI IAM",
                    "authentication_oci_client.{}".format(plugin_ext),
                ),
                (
                    "FIDO",
                    "authentication_fido_client.{}".format(plugin_ext),
                ),
            ]
            for plugin_name, plugin_file in plugin_list:
                plugin_full_path = os.path.join(plugin_path, plugin_file)
                self.log.debug(
                    "%s plugin_path: '%s'", plugin_name, plugin_full_path,
                )
                if os.path.exists(plugin_full_path):
                    bundle_plugin_libs = True
                    vendor_libs.append(
                        (plugin_path, [os.path.join("plugin", plugin_file)])
                    )

            # vendor libraries
            if bundle_plugin_libs and os.name == "nt":
                plugin_libs = []
                libs_path = os.path.join(self.with_mysql_capi, "bin")
                for lib_name in ["libsasl.dll", "saslSCRAM.dll"]:
                    if os.path.exists(os.path.join(libs_path, lib_name)):
                        plugin_libs.append(lib_name)
                if plugin_libs:
                    vendor_libs.append((libs_path, plugin_libs))

                if ARCH_64BIT:
                    openssl_libs = ["libssl-1_1-x64.dll",
                                    "libcrypto-1_1-x64.dll"]
                else:
                    openssl_libs = ["libssl-1_1.dll", "libcrypto-1_1.dll"]
                if self.with_openssl_lib_dir:
                    openssl_libs_path = os.path.abspath(self.with_openssl_lib_dir)
                    if os.path.basename(openssl_libs_path) == "lib":
                        openssl_libs_path = os.path.split(openssl_libs_path)[0]
                    if os.path.exists(openssl_libs_path) and \
                       os.path.exists(os.path.join(openssl_libs_path, "bin")):
                        openssl_libs_path = os.path.join(openssl_libs_path, "bin")
                    self.log.info("# openssl_libs_path: %s", openssl_libs_path)
                else:
                    openssl_libs_path = os.path.join(
                        self.with_mysql_capi, "bin")
                vendor_libs.append((openssl_libs_path, openssl_libs))

        if not vendor_libs:
            return

        self.log.debug("# vendor_libs: %s", vendor_libs)

        # mysql/vendor
        if not os.path.exists(self.vendor_folder):
            mkpath(os.path.join(os.getcwd(), self.vendor_folder))

        # mysql/vendor/plugin
        if not os.path.exists(os.path.join(self.vendor_folder, "plugin")):
            mkpath(os.path.join(os.getcwd(), self.vendor_folder, "plugin"))

        # mysql/vendor/private
        if not os.path.exists(os.path.join(self.vendor_folder, "private")):
            mkpath(os.path.join(os.getcwd(), self.vendor_folder, "private"))

        # Copy vendor libraries to 'mysql/vendor' folder
        self.log.info("Copying vendor libraries")
        for src_folder, files in vendor_libs:
            self.log.info("Copying folder: %s", src_folder)
            for filepath in files:
                dst_folder, filename = os.path.split(filepath)
                src = os.path.join(src_folder, filename)
                dst = os.path.join(os.getcwd(), self.vendor_folder, dst_folder)
                self.log.info("copying %s -> %s", src, dst)
                self.log.info("shutil res: %s", shutil.copy(src, dst))

        if os.name == "nt":
            self.distribution.package_data = {"mysql": ["vendor/plugin/*"]}
            site_packages_files = [
                os.path.join(openssl_libs_path, lib_n) for lib_n in openssl_libs
            ]
            site_packages_files.append(
                os.path.join(self.with_mysql_capi, "lib", "libmysql.dll"))
            self.distribution.data_files = [(
                'lib\\site-packages\\', site_packages_files
            )]
            self.log.debug("# site_packages_files: %s",
                           self.distribution.data_files)
        elif bundle_plugin_libs:
            # Bundle SASL libs
            sasl_libs_path = os.path.join(self.with_mysql_capi, "lib",
                                          "private")
            if not os.path.exists(sasl_libs_path):
                self.log.info("sasl2 llibraries not found at %s",
                              sasl_libs_path)
            sasl_libs = []
            sasl_plugin_libs_w = [
                "libsasl2.*.*", "libgssapi_krb5.*.*", "libgssapi_krb5.*.*",
                "libkrb5.*.*", "libk5crypto.*.*", "libkrb5support.*.*",
                "libcrypto.*.*.*", "libssl.*.*.*", "libcom_err.*.*",
                "libfido2.*.*",
            ]
            sasl_plugin_libs = []
            for sasl_lib in sasl_plugin_libs_w:
                lib_path_entries = glob(os.path.join(
                    sasl_libs_path, sasl_lib))
                for lib_path_entry in lib_path_entries:
                    sasl_plugin_libs.append(os.path.basename(lib_path_entry))
            sasl_libs.append((sasl_libs_path, sasl_plugin_libs))

            # Copy vendor libraries to 'mysql/vendor/private' folder
            self.log.info("Copying vendor libraries")
            for src_folder, files in sasl_libs:
                self.log.info("Copying folder: %s", src_folder)
                for filename in files:
                    src = os.path.join(src_folder, filename)
                    if not os.path.exists(src):
                        self.log.warn("Library not found: %s", src)
                        continue
                    dst = os.path.join(
                        os.getcwd(),
                        self.vendor_folder,
                        "private"
                    )
                    self.log.info("copying %s -> %s", src, dst)
                    shutil.copy(src, dst)

            # include sasl2 libs
            sasl2_libs = []
            sasl2_libs_path = os.path.join(self.with_mysql_capi, "lib",
                                           "private", "sasl2")
            if not os.path.exists(sasl2_libs_path):
                self.log.info("sasl2 llibraries not found at %s",
                              sasl2_libs_path)
            sasl2_libs_w = [
                "libanonymous.*", "libcrammd5.*.*", "libdigestmd5.*.*.*.*",
                "libgssapiv2.*", "libplain.*.*", "libscram.*.*.*.*",
                "libanonymous.*.*", "libcrammd5.*.*.*.*", "libgs2.*",
                "libgssapiv2.*.*", "libplain.*.*.*.*", "libanonymous.*.*.*.*",
                "libdigestmd5.*", "libgs2.*.*", "libgssapiv2.*.*.*.*",
                "libscram.*", "libcrammd5.*", "libdigestmd5.*.*",
                "libgs2.*.*.*.*", "libplain.*", "libscram.*.*"]

            sasl2_scram_libs = []
            for sasl2_lib in sasl2_libs_w:
                lib_path_entries = glob(os.path.join(
                    sasl2_libs_path, sasl2_lib))
                for lib_path_entry in lib_path_entries:
                    sasl2_scram_libs.append(os.path.basename(lib_path_entry))

            sasl2_libs.append((sasl2_libs_path, sasl2_scram_libs))

            sasl2_libs_private_path = os.path.join(
                self.vendor_folder, "private", "sasl2"
            )
            if not os.path.exists(sasl2_libs_private_path):
                mkpath(sasl2_libs_private_path)

            # Copy vendor libraries to 'mysql/vendor/private/sasl2' folder
            self.log.info("Copying vendor libraries")
            for src_folder, files in sasl2_libs:
                self.log.info("Copying folder: %s", src_folder)
                for filename in files:
                    src = os.path.join(src_folder, filename)
                    if not os.path.exists(src):
                        self.log.warning("Library not found: %s", src)
                        continue
                    dst = os.path.join(os.getcwd(), sasl2_libs_private_path)
                    self.log.info("copying %s -> %s", src, dst)
                    shutil.copy(src, dst)

        self.distribution.package_data = {
            "mysql": [
                "vendor/*",
                "vendor/plugin/*",
                "vendor/private/*",
                "vendor/private/sasl2/*"
            ]
        }


class BuildExt(build_ext, BaseCommand):
    """Command class for building the Connector/Python C Extensions."""

    description = "build MySQL Connector/Python C extensions"
    user_options = build_ext.user_options + CEXT_OPTIONS
    boolean_options = build_ext.boolean_options + BaseCommand.boolean_options

    def _get_mysql_version(self):
        if os.name == "posix" and self._mysql_info:
            mysql_version = "{}.{}.{}".format(*self._mysql_info["version"][:3])
        elif os.name == "nt" and os.path.isdir(self.with_mysql_capi):
            mysql_version_h = os.path.join(self.with_mysql_capi,
                                           "include",
                                           "mysql_version.h")
            with open(mysql_version_h, "rb") as fp:
                for line in fp.readlines():
                    if b"#define LIBMYSQL_VERSION" in line:
                        mysql_version = LooseVersion(
                            line.split()[2].replace(b'"', b"").decode()
                        ).version
                        break
        else:
            mysql_version = None
        return mysql_version

    def _finalize_mysql_capi(self):
        self.log.info("Copying MySQL libraries")

        if not os.path.exists(self._build_mysql_lib_dir):
            os.makedirs(self._build_mysql_lib_dir)

        libs = []

        # Add libmysqlclient libraries to be copied
        if "link_dirs" in self._mysql_info:
            libs += glob(os.path.join(
                self._mysql_info["link_dirs"][0], "libmysqlclient*"))

        for lib in libs:
            self.log.info("copying %s -> %s",
                          lib, self._build_mysql_lib_dir)
            shutil.copy(lib, self._build_mysql_lib_dir)

        # Remove all but static libraries to force static linking
        if os.name == "posix":
            self.log.info("Removing non-static MySQL libraries from %s",
                          self._build_mysql_lib_dir)
            for lib in os.listdir(self._build_mysql_lib_dir):
                lib_path = os.path.join(self._build_mysql_lib_dir, lib)
                if os.path.isfile(lib_path) and not lib.endswith(".a"):
                    os.unlink(os.path.join(self._build_mysql_lib_dir, lib))

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
            self.log.info("Protobuf include directory: %s",
                          self.with_protobuf_include_dir)
            if not os.path.isdir(self.with_protobuf_include_dir):
                self.log.error("Protobuf include dir should be a directory")
                sys.exit(1)
        else:
            self.log.error("Unable to find Protobuf include directory")
            sys.exit(1)

        if self.with_protobuf_lib_dir:
            self.log.info("Protobuf library directory: %s",
                          self.with_protobuf_lib_dir)
            if not os.path.isdir(self.with_protobuf_lib_dir):
                self.log.error("Protobuf library dir should be a directory")
                sys.exit(1)
        else:
            self.log.error("Unable to find Protobuf library directory")
            sys.exit(1)

        if self.with_protoc:
            self.log.info("Protobuf protoc binary: %s", self.with_protoc)
            if not os.path.isfile(self.with_protoc):
                self.log.error("Protobuf protoc binary is not valid")
                sys.exit(1)
        else:
            self.log.error("Unable to find Protobuf protoc binary")
            sys.exit(1)

        if not os.path.exists(self._build_protobuf_lib_dir):
            os.makedirs(self._build_protobuf_lib_dir)

        self.log.info("Copying Protobuf libraries")

        libs = glob(os.path.join(self.with_protobuf_lib_dir, "libprotobuf*"))
        for lib in libs:
            if os.path.isfile(lib):
                self.log.info("copying %s -> %s",
                              lib, self._build_protobuf_lib_dir)
                shutil.copy2(lib, self._build_protobuf_lib_dir)

        # Remove all but static libraries to force static linking
        if os.name == "posix":
            self.log.info("Removing non-static Protobuf libraries from %s",
                          self._build_protobuf_lib_dir)
            for lib in os.listdir(self._build_protobuf_lib_dir):
                lib_path = os.path.join(self._build_protobuf_lib_dir, lib)
                if os.path.isfile(lib_path) and \
                   not lib.endswith((".a", ".dylib",)):
                    os.unlink(os.path.join(self._build_protobuf_lib_dir, lib))

    def _run_protoc(self):
        base_path = os.path.join(os.getcwd(), "src", "mysqlxpb", "mysqlx")
        command = [self.with_protoc, "-I"]
        command.append(os.path.join(base_path, "protocol"))
        command.extend(glob(os.path.join(base_path, "protocol", "*.proto")))
        command.append("--cpp_out={0}".format(base_path))
        self.log.info("Running protoc command: %s", " ".join(command))
        check_call(command)

    def initialize_options(self):
        """Initialize the options."""
        build_ext.initialize_options(self)
        BaseCommand.initialize_options(self)

    def finalize_options(self):
        """Finalize the options."""
        build_ext.finalize_options(self)
        BaseCommand.finalize_options(self)

        self.log.info("Python architecture: %s", ARCH)

        self._build_mysql_lib_dir = os.path.join(
            self.build_temp, "capi", "lib")
        self._build_protobuf_lib_dir = os.path.join(
            self.build_temp, "protobuf", "lib")
        if self.with_mysql_capi:
            self._mysql_info = mysql_c_api_info(self.with_mysql_capi)
            self._finalize_mysql_capi()

        self.with_mysqlxpb_cext = any((self.with_protobuf_include_dir,
                                       self.with_protobuf_lib_dir,
                                       self.with_protoc))
        if self.with_mysqlxpb_cext:
            self._finalize_protobuf()

    def run(self):
        """Run the command."""
        # Generate docs/INFO_SRC
        write_info_src(VERSION_TEXT)

        disabled = []  # Extensions to be disabled
        for ext in self.extensions:
            # Add Protobuf include and library dirs
            if ext.name == "_mysqlxpb":
                if not self.with_mysqlxpb_cext:
                    self.log.warning(
                        "The '_mysqlxpb' C extension will not be built")
                    disabled.append(ext)
                    continue
                if platform.system() == "Darwin":
                    symbol_file = tempfile.NamedTemporaryFile()
                    ext.extra_link_args.extend(
                        ["-exported_symbols_list", symbol_file.name]
                    )
                    with open(symbol_file.name, "w") as fp:
                        fp.write("_PyInit__mysqlxpb")
                        fp.write("\n")
                ext.include_dirs.append(self.with_protobuf_include_dir)
                ext.library_dirs.append(self._build_protobuf_lib_dir)
                ext.libraries.append(
                    "libprotobuf" if os.name == "nt" else "protobuf")
                # Add -std=c++11 needed for Protobuf 3.6.1
                ext.extra_compile_args.append("-std=c++11")
                self._run_protoc()
            if ext.name == "_mysql_connector":
                if not self.with_mysql_capi:
                    self.log.warning(
                        "The '_mysql_connector' C extension will not be built")
                    disabled.append(ext)
                    continue
                # Add extra compile args
                if self.extra_compile_args:
                    ext.extra_compile_args.extend(
                        self.extra_compile_args.split())
                # Add extra link args
                if self.extra_link_args:
                    ext.extra_link_args.extend(self.extra_link_args.split())
                # Add -rpath if the platform is Linux
                if platform.system() == "Linux" and not self.skip_vendor:
                    ext.extra_link_args.extend([
                        "-Wl,-rpath,$ORIGIN/mysql/vendor"])
                # Add include dirs
                if self.with_openssl_include_dir:
                    ext.include_dirs.append(self.with_openssl_include_dir)
                if "include_dirs" in self._mysql_info:
                    ext.include_dirs.extend(self._mysql_info["include_dirs"])
                # Add library dirs
                ext.library_dirs.append(self._build_mysql_lib_dir)
                if "library_dirs" in self._mysql_info:
                    ext.library_dirs.extend(self._mysql_info["library_dirs"])
                if self.with_openssl_lib_dir:
                    ext.library_dirs.append(self.with_openssl_lib_dir)
                # Add libraries
                if "libraries" in self._mysql_info:
                    ext.libraries.extend(self._mysql_info["libraries"])
            # Suppress unknown pragmas
            if os.name == "posix":
                ext.extra_compile_args.append("-Wno-unknown-pragmas")

        if os.name != "nt":
            if platform.system() == "Darwin":
                cc = os.environ.get("CC", "clang")
                cxx = os.environ.get("CXX", "clang++")
            else:
                cc = os.environ.get("CC", "gcc")
                cxx = os.environ.get("CXX", "g++")

            cmd_cc_ver = [cc, "-v"]
            self.log.info("Executing: {0}"
                          "".format(" ".join(cmd_cc_ver)))
            proc = Popen(cmd_cc_ver, stdout=PIPE,
                         universal_newlines=True)
            self.log.info(proc.communicate())
            cmd_cxx_ver = [cxx, "-v"]
            self.log.info("Executing: {0}"
                          "".format(" ".join(cmd_cxx_ver)))
            proc = Popen(cmd_cxx_ver, stdout=PIPE,
                         universal_newlines=True)
            self.log.info(proc.communicate())

        # Remove disabled extensions
        for ext in disabled:
            self.extensions.remove(ext)

        build_ext.run(self)

        # Change @loader_path if the platform is MacOS
        if platform.system() == "Darwin" and self.with_openssl_lib_dir:
            for ext in self.extensions:
                if ext.name == "_mysql_connector":
                    libssl, libcrypto = self._get_openssl_libs()
                    cmd_libssl = [
                        "install_name_tool", "-change", libssl,
                        "@loader_path/mysql/vendor/{0}".format(libssl),
                        build_ext.get_ext_fullpath(self, "_mysql_connector")
                    ]
                    self.log.info("Executing: {0}"
                                  "".format(" ".join(cmd_libssl)))
                    proc = Popen(cmd_libssl, stdout=PIPE,
                                 universal_newlines=True)
                    stdout, _ = proc.communicate()
                    cmd_libcrypto = [
                        "install_name_tool", "-change", libcrypto,
                        "@loader_path/mysql/vendor/{0}".format(libcrypto),
                        build_ext.get_ext_fullpath(self, "_mysql_connector")
                    ]
                    self.log.info("Executing: {0}"
                                  "".format(" ".join(cmd_libcrypto)))
                    proc = Popen(cmd_libcrypto, stdout=PIPE,
                                 universal_newlines=True)
                    stdout, _ = proc.communicate()

        # Generate docs/INFO_BIN
        if self.with_mysql_capi:
            mysql_version = self._get_mysql_version()
            compiler = self.compiler.compiler_so[0] \
                if hasattr(self.compiler, "compiler_so") else None
            write_info_bin(mysql_version, compiler)


class InstallLib(install_lib):
    """InstallLib Connector/Python implementation."""

    user_options = install_lib.user_options + [
        ("byte-code-only", None,
         "remove Python .py files; leave byte code .pyc only"),
    ]
    boolean_options = ["byte-code-only"]
    log = LOGGER

    def initialize_options(self):
        """Initialize the options."""
        install_lib.initialize_options(self)
        self.byte_code_only = False

    def finalize_options(self):
        """Finalize the options."""
        install_lib.finalize_options(self)
        self.set_undefined_options("install",
                                   ("byte_code_only", "byte_code_only"))
        self.set_undefined_options("build", ("build_base", "build_dir"))

    def run(self):
        """Run the command."""
        if not os.path.exists(self.build_dir):
            mkpath(self.build_dir)

        self.build()

        outfiles = self.install()

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
                    if base.endswith("__pycache__"):
                        os.rmdir(base)
            for source_file in outfiles:
                if source_file.endswith(".py"):
                    self.log.info("Removing %s", source_file)
                    os.remove(source_file)


class Install(install, BaseCommand):
    """Install Connector/Python implementation."""

    description = "install MySQL Connector/Python"
    user_options = install.user_options + BaseCommand.user_options
    boolean_options = install.boolean_options + BaseCommand.boolean_options

    def initialize_options(self):
        """Initialize the options."""
        install.initialize_options(self)
        BaseCommand.initialize_options(self)

    def finalize_options(self):
        """Finalize the options."""
        BaseCommand.finalize_options(self)
        install.finalize_options(self)
        cmd_install_lib = self.distribution.get_command_obj("install_lib")
        cmd_install_lib.byte_code_only = self.byte_code_only
