# Copyright (c) 2020, 2023, Oracle and/or its affiliates.
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
from pathlib import Path
from subprocess import PIPE, Popen, check_call
from sysconfig import get_config_vars, get_python_version

from setuptools import Command
from setuptools.command.build_ext import build_ext
from setuptools.command.install import install
from setuptools.command.install_lib import install_lib

try:
    from setuptools.logging import set_threshold
except ImportError:
    set_threshold = None

from .utils import (
    ARCH,
    get_openssl_libs,
    mysql_c_api_info,
    parse_loose_version,
    write_info_bin,
    write_info_src,
)

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
    (
        "byte-code-only",
        None,
        "remove Python .py files; leave byte code .pyc only",
    ),
    ("edition=", None, "Edition added in the package name after the version"),
    ("label=", None, "label added in the package name after the name"),
    ("debug", None, "turn debugging on"),
    (
        "keep-temp",
        "k",
        "keep the pseudo-installation tree around after creating the "
        "distribution archive",
    ),
]

CEXT_OPTIONS = [
    (
        "with-mysql-capi=",
        None,
        "location of MySQL C API installation or path to mysql_config",
    ),
    (
        "with-openssl-include-dir=",
        None,
        "location of OpenSSL include directory",
    ),
    ("with-openssl-lib-dir=", None, "location of OpenSSL library directory"),
    ("extra-compile-args=", None, "extra compile args"),
    ("extra-link-args=", None, "extra link args"),
    ("skip-vendor", None, "Skip bundling vendor libraries"),
]

LOGGER = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(levelname)s[%(name)s]: %(message)s"))
LOGGER.addHandler(handler)
LOGGER.setLevel(logging.WARNING)


def get_otel_src_package_data():
    """Get a list including all py.typed and dist-info files corresponding
    to opentelemetry-python (see [1]) located at
    `mysql/opentelemetry`.

    Returns:
        package_data: List[str].

    References:
    [1]: https://github.com/open-telemetry/opentelemetry-python
    """
    path_otel = os.path.join("lib", "mysql", "opentelemetry")

    package_data = []
    for root, dirs, filenames in os.walk(os.path.join(os.getcwd(), path_otel, "")):
        offset = root.replace(os.path.join(os.getcwd(), path_otel, ""), "")
        for _dir in dirs:
            if _dir.endswith(".dist-info"):
                package_data.append(os.path.join(offset, _dir, "*"))
        for filename in filenames:
            if filename == "py.typed":
                package_data.append(os.path.join(offset, filename))
    return package_data


class BaseCommand(Command):
    """Base command class for Connector/Python."""

    user_options = COMMON_USER_OPTIONS + CEXT_OPTIONS
    boolean_options = ["debug", "byte_code_only", "keep_temp", "skip_vendor"]

    with_mysql_capi = None

    with_openssl_include_dir = None
    with_openssl_lib_dir = None

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

    def initialize_options(self):
        """Initialize the options."""
        self.with_mysql_capi = None
        self.with_openssl_include_dir = None
        self.with_openssl_lib_dir = None
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
            if set_threshold:
                # Set setuptools logging level to DEBUG
                try:
                    set_threshold(1)
                except AttributeError:
                    pass

        if not self.with_mysql_capi:
            self.with_mysql_capi = os.environ.get("MYSQL_CAPI")
        if not self.with_openssl_include_dir:
            self.with_openssl_include_dir = os.environ.get("OPENSSL_INCLUDE_DIR")
        if not self.with_openssl_lib_dir:
            self.with_openssl_lib_dir = os.environ.get("OPENSSL_LIB_DIR")
        if not self.extra_compile_args:
            self.extra_compile_args = os.environ.get("EXTRA_COMPILE_ARGS")
        if not self.extra_link_args:
            self.extra_link_args = os.environ.get("EXTRA_LINK_ARGS")
        if not self.skip_vendor:
            self.skip_vendor = os.environ.get("SKIP_VENDOR", False)

        if not self.with_mysql_capi:
            self.skip_vendor = True

        cmd_build_ext = self.distribution.get_command_obj("build_ext")
        cmd_build_ext.with_mysql_capi = self.with_mysql_capi
        cmd_build_ext.with_openssl_include_dir = self.with_openssl_include_dir
        cmd_build_ext.with_openssl_lib_dir = self.with_openssl_lib_dir
        cmd_build_ext.extra_compile_args = self.extra_compile_args
        cmd_build_ext.extra_link_args = self.extra_link_args
        cmd_build_ext.skip_vendor = self.skip_vendor

        install = self.distribution.get_command_obj("install")
        install.with_mysql_capi = self.with_mysql_capi
        install.with_openssl_include_dir = self.with_openssl_include_dir
        install.with_openssl_lib_dir = self.with_openssl_lib_dir
        install.extra_compile_args = self.extra_compile_args
        install.extra_link_args = self.extra_link_args
        install.skip_vendor = self.skip_vendor

        self.distribution.package_data = {
            "mysql.connector": ["py.typed"],
            "mysql.opentelemetry": get_otel_src_package_data(),
        }
        if not cmd_build_ext.skip_vendor:
            self._copy_vendor_libraries()

    def remove_temp(self):
        """Remove temporary build files."""
        if not self.keep_temp:
            cmd_build = self.get_finalized_command("build")
            if not self.dry_run:
                shutil.rmtree(cmd_build.build_base)
            vendor_folder = os.path.join(os.getcwd(), self.vendor_folder)
            if os.path.exists(vendor_folder):
                shutil.rmtree(vendor_folder)
            elif os.name == "nt":
                libssl, libcrypto = self._get_openssl_libs(ext="dll")
                libraries = ["libmysql.dll", libssl, libcrypto]
                for filename in libraries:
                    dll_file = os.path.join(os.getcwd(), filename)
                    if os.path.exists(dll_file):
                        os.unlink(dll_file)

    def _get_openssl_libs(self, lib_dir=None, ext=None):
        if lib_dir is None:
            lib_dir = self.with_openssl_lib_dir
        libssl, libcrypto = get_openssl_libs(lib_dir, ext)
        if not libssl or not libcrypto:
            self.log.error("Unable to find OpenSSL libraries in '%s'", lib_dir)
            sys.exit(1)
        self.log.debug(
            "Found OpenSSL libraries '%s', '%s' in '%s'",
            libssl,
            libcrypto,
            lib_dir,
        )
        return (libssl, libcrypto)

    def _copy_vendor_libraries(self):
        openssl_libs = []
        vendor_libs = []

        if os.name == "posix":
            # Bundle OpenSSL libs
            if self.with_openssl_lib_dir:
                libssl, libcrypto = self._get_openssl_libs()
                vendor_libs.append((self.with_openssl_lib_dir, [libssl, libcrypto]))
                # Copy libssl and libcrypto libraries to 'mysql/vendor/plugin' and
                # libcrypto to 'mysql/vendor/lib' on macOS
                if platform.system() == "Darwin":
                    vendor_libs.append(
                        (
                            self.with_openssl_lib_dir,
                            [
                                Path("plugin", libssl).as_posix(),
                                Path("plugin", libcrypto).as_posix(),
                            ],
                        )
                    )
                    vendor_libs.append(
                        (
                            self.with_openssl_lib_dir,
                            [
                                Path("lib", libcrypto).as_posix(),
                            ],
                        )
                    )

        # Plugins
        bundle_plugin_libs = False
        if self.with_mysql_capi:
            plugin_ext = "dll" if os.name == "nt" else "so"
            plugin_path = os.path.join(self.with_mysql_capi, "lib", "plugin")
            plugin_list = [
                ("LDAP", f"authentication_ldap_sasl_client.{plugin_ext}"),
                ("Kerberos", f"authentication_kerberos_client.{plugin_ext}"),
                ("OCI IAM", f"authentication_oci_client.{plugin_ext}"),
                ("FIDO", f"authentication_fido_client.{plugin_ext}"),
                ("WebAuthn", f"authentication_webauthn_client.{plugin_ext}"),
            ]

            for plugin_name, plugin_file in plugin_list:
                plugin_full_path = os.path.join(plugin_path, plugin_file)
                self.log.debug(
                    "%s plugin_path: '%s'",
                    plugin_name,
                    plugin_full_path,
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
                if self.with_openssl_lib_dir:
                    openssl_libs_path = os.path.abspath(self.with_openssl_lib_dir)
                    if os.path.basename(openssl_libs_path) == "lib":
                        openssl_libs_path = os.path.split(openssl_libs_path)[0]
                    if os.path.exists(openssl_libs_path) and os.path.exists(
                        os.path.join(openssl_libs_path, "bin")
                    ):
                        openssl_libs_path = os.path.join(openssl_libs_path, "bin")
                    self.log.info("# openssl_libs_path: %s", openssl_libs_path)
                else:
                    openssl_libs_path = os.path.join(self.with_mysql_capi, "bin")
                libssl, libcrypto = self._get_openssl_libs(openssl_libs_path, "dll")
                openssl_libs = [libssl, libcrypto]
                vendor_libs.append((openssl_libs_path, openssl_libs))

        if not vendor_libs:
            return

        self.log.debug("# vendor_libs: %s", vendor_libs)

        # mysql/vendor
        if not Path(self.vendor_folder).exists():
            Path(os.getcwd(), self.vendor_folder).mkdir(parents=True, exist_ok=True)

        # mysql/vendor/plugin
        if not Path(self.vendor_folder, "plugin").exists():
            Path(os.getcwd(), self.vendor_folder, "plugin").mkdir(
                parents=True, exist_ok=True
            )

        # mysql/vendor/private
        if not Path(self.vendor_folder, "private").exists():
            Path(os.getcwd(), self.vendor_folder, "private").mkdir(
                parents=True, exist_ok=True
            )

        # mysql/vendor/lib
        if (
            platform.system() == "Darwin"
            and not Path(self.vendor_folder, "lib").exists()
        ):
            Path(os.getcwd(), self.vendor_folder, "lib").mkdir(
                parents=True, exist_ok=True
            )

        # Copy vendor libraries to 'mysql/vendor' folder
        self.log.info("Copying vendor libraries")
        for src_folder, files in vendor_libs:
            self.log.info("Copying folder: %s", src_folder)
            for filepath in files:
                dst_folder, filename = os.path.split(filepath)
                src = Path(src_folder, filename)
                dst = Path(os.getcwd(), self.vendor_folder, dst_folder)
                if not Path(dst, src.name).exists():
                    self.log.debug("copying %s -> %s", src, dst)
                    shutil.copy2(src, dst, follow_symlinks=False)

        if os.name == "nt":
            self.distribution.package_data = {"mysql": ["vendor/plugin/*"]}
            site_packages_files = [
                os.path.join(openssl_libs_path, lib_n) for lib_n in openssl_libs
            ]
            site_packages_files.append(
                os.path.join(self.with_mysql_capi, "lib", "libmysql.dll")
            )
            self.distribution.data_files = [
                ("lib\\site-packages\\", site_packages_files)
            ]
            self.log.debug("# site_packages_files: %s", self.distribution.data_files)
        elif bundle_plugin_libs:
            # Bundle SASL libs
            sasl_libs_path = (
                Path(self.with_mysql_capi, "lib")
                if platform.system() == "Darwin"
                else Path(self.with_mysql_capi, "lib", "private")
            )
            if not os.path.exists(sasl_libs_path):
                self.log.info("sasl2 llibraries not found at %s", sasl_libs_path)
            sasl_libs = []
            sasl_plugin_libs_w = [
                "libsasl2.*.*",
                "libgssapi_krb5.*.*",
                "libgssapi_krb5.*.*",
                "libkrb5.*.*",
                "libk5crypto.*.*",
                "libkrb5support.*.*",
                "libcrypto.*.*.*",
                "libssl.*.*.*",
                "libcom_err.*.*",
                "libfido2.*.*",
            ]
            sasl_plugin_libs = []
            for sasl_lib in sasl_plugin_libs_w:
                lib_path_entries = glob(os.path.join(sasl_libs_path, sasl_lib))
                for lib_path_entry in lib_path_entries:
                    sasl_plugin_libs.append(os.path.basename(lib_path_entry))
            sasl_libs.append((sasl_libs_path, sasl_plugin_libs))

            # Copy vendor libraries to 'mysql/vendor/private' folder
            self.log.info("Copying vendor libraries")
            for src_folder, files in sasl_libs:
                self.log.info("Copying folder: %s", src_folder)
                for filename in files:
                    src = Path(src_folder, filename)
                    if not src.exists():
                        self.log.warn("Library not found: %s", src)
                        continue
                    dst = (
                        Path(os.getcwd(), self.vendor_folder)
                        if platform.system() == "Darwin"
                        else Path(os.getcwd(), self.vendor_folder, "private")
                    )
                    if not Path(dst, src.name).exists():
                        self.log.debug("copying %s -> %s", src, dst)
                        shutil.copy2(src, dst, follow_symlinks=False)

            # include sasl2 libs
            sasl2_libs = []
            sasl2_libs_path = os.path.join(
                self.with_mysql_capi, "lib", "private", "sasl2"
            )
            if not os.path.exists(sasl2_libs_path):
                self.log.info("sasl2 libraries not found at %s", sasl2_libs_path)
            sasl2_libs_w = [
                "libanonymous.*",
                "libcrammd5.*.*",
                "libdigestmd5.*.*.*.*",
                "libgssapiv2.*",
                "libplain.*.*",
                "libscram.*.*.*.*",
                "libanonymous.*.*",
                "libcrammd5.*.*.*.*",
                "libgs2.*",
                "libgssapiv2.*.*",
                "libplain.*.*.*.*",
                "libanonymous.*.*.*.*",
                "libdigestmd5.*",
                "libgs2.*.*",
                "libgssapiv2.*.*.*.*",
                "libscram.*",
                "libcrammd5.*",
                "libdigestmd5.*.*",
                "libgs2.*.*.*.*",
                "libplain.*",
                "libscram.*.*",
            ]

            sasl2_scram_libs = []
            for sasl2_lib in sasl2_libs_w:
                lib_path_entries = glob(os.path.join(sasl2_libs_path, sasl2_lib))
                for lib_path_entry in lib_path_entries:
                    sasl2_scram_libs.append(os.path.basename(lib_path_entry))

            sasl2_libs.append((sasl2_libs_path, sasl2_scram_libs))

            sasl2_libs_private_path = os.path.join(
                self.vendor_folder, "private", "sasl2"
            )
            if not os.path.exists(sasl2_libs_private_path):
                Path(sasl2_libs_private_path).mkdir(parents=True, exist_ok=True)

            # Copy vendor libraries to 'mysql/vendor/private/sasl2' folder
            self.log.info("Copying vendor libraries")
            dst = Path(os.getcwd(), sasl2_libs_private_path)
            for src_folder, files in sasl2_libs:
                self.log.info("Copying folder: %s", src_folder)
                for filename in files:
                    src = Path(src_folder, filename)
                    if not src.exists():
                        self.log.warning("Library not found: %s", src)
                        continue
                    if not Path(dst, filename).exists():
                        self.log.debug("copying %s -> %s", src, dst)
                        shutil.copy2(src, dst, follow_symlinks=False)

            # Copy libfido2 libraries to 'mysql/vendor/plugin' on macOS
            if platform.system() == "Darwin":
                dst = Path(os.getcwd(), self.vendor_folder, "plugin")
                libfido2_files = [
                    Path(filename).name
                    for filename in glob(
                        Path(self.vendor_folder, "libfido2.*.*").as_posix()
                    )
                ]
                for filename in libfido2_files:
                    src = Path(os.getcwd(), self.vendor_folder, filename)
                    if not Path(dst, filename).exists():
                        self.log.debug("copying %s -> %s", src, dst)
                        shutil.copy2(src, dst, follow_symlinks=False)

        self.distribution.package_data = {
            "mysql": [
                "vendor/*",
                "vendor/lib/*",
                "vendor/plugin/*",
                "vendor/private/*",
                "vendor/private/sasl2/*",
            ],
            "mysql.connector": ["py.typed"],
            "mysql.opentelemetry": get_otel_src_package_data(),
        }


class BuildExt(build_ext, BaseCommand):
    """Command class for building the Connector/Python C Extensions."""

    description = "build MySQL Connector/Python C extensions"
    user_options = build_ext.user_options + CEXT_OPTIONS
    boolean_options = build_ext.boolean_options + BaseCommand.boolean_options

    def _get_mysql_version(self):
        if os.name == "posix" and self._mysql_info:
            major, minor, patch = self._mysql_info["version"][:3]
            mysql_version = f"{major}.{minor}.{patch}"
        elif os.name == "nt" and os.path.isdir(self.with_mysql_capi):
            mysql_version_h = os.path.join(
                self.with_mysql_capi, "include", "mysql_version.h"
            )
            with open(mysql_version_h, "rb") as fp:
                for line in fp.readlines():
                    if b"#define LIBMYSQL_VERSION" in line:
                        mysql_version = parse_loose_version(
                            line.split()[2].replace(b'"', b"").decode()
                        )
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
            libs += glob(
                os.path.join(self._mysql_info["link_dirs"][0], "libmysqlclient*")
            )

        for lib in libs:
            self.log.info("copying %s -> %s", lib, self._build_mysql_lib_dir)
            shutil.copy2(lib, self._build_mysql_lib_dir)

        # Remove all but static libraries to force static linking
        if os.name == "posix":
            self.log.info(
                "Removing non-static MySQL libraries from %s",
                self._build_mysql_lib_dir,
            )
            for lib in os.listdir(self._build_mysql_lib_dir):
                lib_path = os.path.join(self._build_mysql_lib_dir, lib)
                if os.path.isfile(lib_path) and not lib.endswith(".a"):
                    os.unlink(os.path.join(self._build_mysql_lib_dir, lib))

    def initialize_options(self):
        """Initialize the options."""
        build_ext.initialize_options(self)
        BaseCommand.initialize_options(self)

    def finalize_options(self):
        """Finalize the options."""
        build_ext.finalize_options(self)
        BaseCommand.finalize_options(self)

        self.log.info("Python architecture: %s", ARCH)

        self._build_mysql_lib_dir = os.path.join(self.build_temp, "capi", "lib")
        if self.with_mysql_capi:
            self._mysql_info = mysql_c_api_info(self.with_mysql_capi)
            self._finalize_mysql_capi()

    def run(self):
        """Run the command."""
        # Generate docs/INFO_SRC
        write_info_src(VERSION_TEXT)

        disabled = []  # Extensions to be disabled
        for ext in self.extensions:
            if ext.name == "_mysql_connector":
                if not self.with_mysql_capi:
                    self.log.warning(
                        "The '_mysql_connector' C extension will not be built"
                    )
                    disabled.append(ext)
                    continue
                # Add extra compile args
                if self.extra_compile_args:
                    ext.extra_compile_args.extend(self.extra_compile_args.split())
                # Add extra link args
                if self.extra_link_args:
                    ext.extra_link_args.extend(self.extra_link_args.split())
                # Add -rpath if the platform is Linux
                if platform.system() == "Linux" and not self.skip_vendor:
                    ext.extra_link_args.extend(["-Wl,-rpath,$ORIGIN/mysql/vendor"])
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
            is_macos = platform.system() == "Darwin"
            cc = os.environ.get("CC", "clang" if is_macos else "gcc")
            cxx = os.environ.get("CXX", "clang++" if is_macos else "g++")
            cmd_cc_ver = [cc, "-v"]
            self.log.info("Executing: %s", " ".join(cmd_cc_ver))
            proc = Popen(cmd_cc_ver, stdout=PIPE, universal_newlines=True)
            self.log.info(proc.communicate())
            cmd_cxx_ver = [cxx, "-v"]
            self.log.info("Executing: %s", " ".join(cmd_cxx_ver))
            proc = Popen(cmd_cxx_ver, stdout=PIPE, universal_newlines=True)
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
                        "install_name_tool",
                        "-change",
                        libssl,
                        f"@loader_path/mysql/vendor/{libssl}",
                        build_ext.get_ext_fullpath(self, "_mysql_connector"),
                    ]
                    self.log.info("Executing: %s", " ".join(cmd_libssl))
                    proc = Popen(cmd_libssl, stdout=PIPE, universal_newlines=True)
                    proc.communicate()
                    cmd_libcrypto = [
                        "install_name_tool",
                        "-change",
                        libcrypto,
                        f"@loader_path/mysql/vendor/{libcrypto}",
                        build_ext.get_ext_fullpath(self, "_mysql_connector"),
                    ]
                    self.log.info("Executing: %s", " ".join(cmd_libcrypto))
                    proc = Popen(cmd_libcrypto, stdout=PIPE, universal_newlines=True)
                    proc.communicate()

        # Generate docs/INFO_BIN
        if self.with_mysql_capi:
            mysql_version = self._get_mysql_version()
            compiler = (
                self.compiler.compiler_so[0]
                if hasattr(self.compiler, "compiler_so")
                else None
            )
            write_info_bin(mysql_version, compiler)


class InstallLib(install_lib):
    """InstallLib Connector/Python implementation."""

    user_options = install_lib.user_options + [
        (
            "byte-code-only",
            None,
            "remove Python .py files; leave byte code .pyc only",
        ),
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
        self.set_undefined_options("install", ("byte_code_only", "byte_code_only"))
        self.set_undefined_options("build", ("build_base", "build_dir"))

    def run(self):
        """Run the command."""
        if not os.path.exists(self.build_dir):
            Path(self.build_dir).mkdir(parents=True, exist_ok=True)

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
                            new_name = f"{filename.split('.')[0]}.pyc"
                            os.rename(
                                os.path.join(base, filename),
                                os.path.join(base, "..", new_name),
                            )
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
