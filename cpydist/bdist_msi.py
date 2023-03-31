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


"""Implements the Distutils commands for creating MSI packages."""

import json
import os
import re
import shutil
import sys
import zipfile

from sysconfig import get_platform, get_python_version

try:
    from setuptools.errors import BaseError, OptionError
except ImportError:
    BaseError = Exception
    OptionError = Exception

from . import EDITION, VERSION, VERSION_EXTRA, VERSION_TEXT, BaseCommand, wix
from .utils import (
    ARCH_64BIT,
    add_arch_dep_elems,
    get_magic_tag,
    get_openssl_libs,
    parse_loose_version,
    write_info_bin,
    write_info_src,
)

MSIDATA_ROOT = os.path.join("cpydist", "data", "msi")
DIST_PATH_FORMAT = "wininst_{}{}"


class DistMSI(BaseCommand):
    """Create a MSI distribution."""

    description = "create a MSI distribution"
    user_options = BaseCommand.user_options + [
        (
            "bdist-dir=",
            "d",
            "temporary directory for creating the distribution",
        ),
        ("dist-dir=", "d", "directory to put final built distributions in"),
        (
            "wix-install=",
            None,
            "location of the Windows Installer XML installation"
            f"(default: {wix.WIX_INSTALL_PATH})",
        ),
        (
            "wix-required-version=",
            None,
            "required version the Windows Installer XML installation"
            f"(default: {wix.WIX_REQUIRED_VERSION})",
        ),
        ("python-version=", None, "target Python version"),
        (
            "prepare-stage",
            "p",
            f"only stage installation for this python {get_python_version()[:3]} "
            "version, used later for a single msi",
        ),
        (
            "combine-stage",
            "c",
            "unify the prepared msi stages to only one single msi",
        ),
    ]
    boolean_options = BaseCommand.user_options + [
        "include-sources",
        "prepare-stage",
        "combine-stage",
    ]
    negative_opt = {}

    _connc_include = None
    _connc_lib = None
    _dist_path = {}
    _fix_txt_files = {}
    _supported_versions = ["3.8", "3.9", "3.10", "3.11"]
    _with_cext = False
    _wxs = None

    def initialize_options(self):
        """Initialize the options."""
        BaseCommand.initialize_options(self)
        self.prefix = None
        self.build_base = None
        self.bdist_dir = None
        self.dist_dir = None
        self.wix_install = wix.WIX_INSTALL_PATH
        self.wix_required_version = wix.WIX_REQUIRED_VERSION
        self.python_version = get_python_version()
        self.prepare_stage = False
        self.combine_stage = False
        self.edition = EDITION

    def finalize_options(self):
        """Finalize the options."""
        BaseCommand.finalize_options(self)
        self.set_undefined_options("build", ("build_base", "build_base"))
        self.set_undefined_options("bdist", ("dist_dir", "dist_dir"))

        if not self.prefix:
            self.prefix = os.path.join(
                self.build_base,
                DIST_PATH_FORMAT.format(
                    self.python_version[0], self.python_version[2:]
                ),
            )

        for py_ver in self._supported_versions:
            self._dist_path[py_ver] = os.path.join(
                self.build_base, DIST_PATH_FORMAT.format(*py_ver.split("."))
            )

        if self.python_version not in self._supported_versions:
            raise OptionError(
                f"The --python-version {self.python_version} should be a supported "
                f"version, one of {','.join(self._supported_versions)}"
            )

        if self.python_version[0] != get_python_version()[0]:
            raise BaseError(
                "Python v3 distributions need to be build with a "
                "supported Python v3 installation."
            )

        self._with_cext = any(
            (
                self.with_mysql_capi,
                self.with_protobuf_include_dir,
                self.with_protobuf_lib_dir,
                self.with_protoc,
            )
        )

        if self._with_cext:
            cmd_build = self.get_finalized_command("build")
            self._connc_lib = os.path.join(cmd_build.build_temp, "connc", "lib")
            self._connc_include = os.path.join(cmd_build.build_temp, "connc", "include")
            self._finalize_connector_c(self.with_mysql_capi)

        self._wxs = self._finalize_msi_descriptor()
        self._fix_txt_files = {
            "README.txt": os.path.join(os.getcwd(), "README.txt"),
            "LICENSE.txt": os.path.join(os.getcwd(), "LICENSE.txt"),
            "CHANGES.txt": os.path.join(os.getcwd(), "CHANGES.txt"),
            "README.rst": os.path.join(os.getcwd(), "README.txt"),
            "CONTRIBUTING.rst": os.path.join(os.getcwd(), "CONTRIBUTING.rst"),
            "INFO_SRC": os.path.join(os.getcwd(), "docs", "INFO_SRC"),
            "INFO_BIN": os.path.join(os.getcwd(), "docs", "INFO_BIN"),
        }

    def _finalize_connector_c(self, connc_loc):
        """Finalize C extensions."""
        if not os.path.isdir(connc_loc):
            self.log.error("MySQL C API should be a directory")
            sys.exit(1)
        self.log.info("# Locating OpeenSSL libraries")
        shutil.copytree(
            os.path.join(connc_loc, "lib"), self._connc_lib, dirs_exist_ok=True
        )
        shutil.copytree(
            os.path.join(connc_loc, "include"), self._connc_include, dirs_exist_ok=True
        )

        self.log.info("# self.with_openssl_lib_dir: %s", self.with_openssl_lib_dir)

        if self.with_openssl_lib_dir:
            openssl_lib_dir = os.path.abspath(self.with_openssl_lib_dir)
            if os.path.basename(openssl_lib_dir) == "lib":
                openssl_dir, _ = os.path.split(openssl_lib_dir)
                if os.path.exists(os.path.join(openssl_dir, "bin")):
                    openssl_lib_dir = os.path.join(openssl_dir, "bin")
        else:
            openssl_lib_dir = os.path.join(connc_loc, "bin")

        self.log.info("# openssl_lib_dir: %s", openssl_lib_dir)

        libssl, libcrypto = get_openssl_libs(openssl_lib_dir, ext="dll")
        if not libssl or not libcrypto:
            self.log.error("Unable to find OpenSSL libraries in '%s'", openssl_lib_dir)
            sys.exit(1)

        for filename in (libssl, libcrypto):
            src = os.path.join(openssl_lib_dir, filename)
            self.log.info("Using %s: located in %s", filename, src)
            dst = self._connc_lib
            self.log.info("copying %s -> %s", src, dst)
            shutil.copy2(src, dst)

        for lib_file in os.listdir(self._connc_lib):
            if os.name == "posix" and not lib_file.endswith(".a"):
                os.unlink(os.path.join(self._connc_lib, lib_file))

    def _finalize_msi_descriptor(self):
        """Return the finalized and customized path of the msi descriptor."""
        base_xml_path = os.path.join(MSIDATA_ROOT, "product.wxs")
        result_xml_path = os.path.join(MSIDATA_ROOT, "cpy_product_desc.wxs")

        if get_platform() == "win32":
            add_arch_dep_elems(
                base_xml_path, result_xml_path, for32=True, add_vs_redist=False
            )
        else:
            add_arch_dep_elems(
                base_xml_path, result_xml_path, add_vs_redist=self._with_cext
            )

        return result_xml_path

    def _get_wixobj_name(self, myc_version=None):
        """Get the name for the wixobj-file."""
        if not myc_version:
            myc_version = self.distribution.get_version()
        label = f"-{self.label}" if self.label else ""
        version_extra = f"-{VERSION_EXTRA}" if VERSION_EXTRA else ""
        arch = "windows-x86-64bit" if ARCH_64BIT else "windows-x86-32bit"
        return (
            f"mysql-connector-python{label}-{myc_version}{version_extra}"
            f"{self.edition}-{arch}.wixobj"
        )

    def _find_bdist_paths(self):
        """Find compressed distribution files or valid distribution paths."""
        bdist_paths = {}
        valid_bdist_paths = {}

        for py_ver in self._supported_versions:
            bdist_paths[py_ver] = os.path.join(
                os.path.abspath(self._dist_path[py_ver]),
                "Lib",
                "site-packages",
            )
            dist_path = DIST_PATH_FORMAT.format(*py_ver.split("."))
            zip_fn = f"{dist_path}.zip"

            self.log.info("Locating zip: %s at %s", zip_fn, os.path.curdir)
            bdist_path = None
            if os.path.exists(zip_fn) and zipfile.is_zipfile(zip_fn):
                with zipfile.ZipFile(zip_fn) as zip_f:
                    zip_f.extractall()
            else:
                self.log.warning("Unable to find zip: %s at %s", zip_fn, os.path.curdir)
            if bdist_path is None:
                bdist_path = bdist_paths[py_ver]
                self.log.info("Checking for extracted distribution at %s", bdist_path)
            if os.path.exists(bdist_path):
                valid_bdist_paths[py_ver] = bdist_path
                self.log.info("Distribution path found at %s", bdist_path)
            else:
                self.log.warning("Unable to find distribution path for %s", py_ver)

        return valid_bdist_paths

    def _create_msi(self, dry_run=0):
        """Create the Windows Installer using WiX.

        Creates the Windows Installer using WiX and returns the name of
        the created MSI file.
        """
        # load the upgrade codes
        with open(os.path.join(MSIDATA_ROOT, "upgrade_codes.json")) as fp:
            upgrade_codes = json.load(fp)

        # version variables for Connector/Python and Python
        mycver = self.distribution.metadata.version
        match = re.match(r"(\d+)\.(\d+).(\d+).*", mycver)
        if not match:
            raise ValueError(f"Failed parsing version from {mycver}")
        (major, minor, patch) = match.groups()
        pyver = self.python_version
        pymajor = pyver[0]
        pyminor = pyver[2]

        # check whether we have an upgrade code
        try:
            upgrade_code = upgrade_codes[mycver[0:3]][pyver]
        except KeyError:
            raise BaseError(
                f"No upgrade code found for version v{mycver}, Python v{pyver}"
            )
        self.log.info(
            f"upgrade code for v%s, Python v%s: %s", mycver, pyver, upgrade_code
        )

        self.pyver_bdist_paths = self._find_bdist_paths()

        # wixobj's basename is the name of the installer
        wixobj = self._get_wixobj_name()
        msi = os.path.abspath(
            os.path.join(self.dist_dir, wixobj.replace(".wixobj", ".msi"))
        )
        wixer = wix.WiX(
            self._wxs,
            out=wixobj,
            msi_out=msi,
            base_path=self.build_base,
            install=self.wix_install,
        )

        # correct newlines and version in text files
        self.log.info("Fixing newlines in text files")
        info_files = []
        for txt_file_dest, txt_file_path in self._fix_txt_files.items():
            txt_fixed = os.path.join(self.build_base, txt_file_dest)
            info_files.append(txt_fixed)
            content = open(txt_file_path, "rb").read()

            if b"\r\n" not in content:
                self.log.info("converting newlines in %s", txt_fixed)
                content = content.replace(b"\n", b"\r\n")
                open(txt_fixed, "wb").write(content)
            else:
                self.log.info("not converting newlines in %s, this is odd", txt_fixed)
                open(txt_fixed, "wb").write(content)

        digit_needle = r"Connector/Python \d{1,2}.\d{1,2}"
        xy_needle = "Connector/Python X.Y"
        xy_sub = "Connector/Python {0}.{1}"
        for info_file in info_files:
            self.log.info("correcting version in %s", info_file)
            with open(info_file, "r+") as fp:
                content = fp.readlines()
                for idx, line in enumerate(content):
                    content[idx] = re.sub(
                        digit_needle, xy_sub.format(*VERSION[0:2]), line
                    )
                    line = content[idx]
                    content[idx] = re.sub(xy_needle, xy_sub.format(*VERSION[0:2]), line)
                fp.seek(0)
                fp.write("".join(content))

        plat_type = "x64" if ARCH_64BIT else "x86"
        win64 = "yes" if ARCH_64BIT else "no"
        pyd_arch = "win_amd64" if ARCH_64BIT else "win32"
        directory_id = "ProgramFiles64Folder" if ARCH_64BIT else "ProgramFilesFolder"

        # For 3.5 the driver names are pretty complex, see
        # https://www.python.org/dev/peps/pep-0425/
        if pymajor == "3" and int(pyminor) >= 5:
            pyd_ext = ".cp%s%s-%s.pyd" % (pyver[0], 5, pyd_arch)
        else:
            pyd_ext = ".pyd"

        if self._connc_lib:
            libssl, libcrypto = get_openssl_libs(self._connc_lib, ext="dll")
            libssl_dll_path = os.path.join(os.path.abspath(self._connc_lib), libssl)
            libcrypto_dll_path = os.path.join(
                os.path.abspath(self._connc_lib), libcrypto
            )
        else:
            libssl_dll_path = ""
            libssl = ""
            libcrypto_dll_path = ""
            libcrypto = ""

        # WiX preprocessor variables
        params = {
            "Version": ".".join([major, minor, patch]),
            "FullVersion": mycver,
            "PythonVersion": pyver,
            "PythonMajor": pymajor,
            "PythonMinor": pyminor,
            "Major_Version": major,
            "Minor_Version": minor,
            "Patch_Version": patch,
            "Platform": plat_type,
            "Directory_Id": directory_id,
            "PythonInstallDir": "Python%s" % pyver.replace(".", ""),
            "PyExt": "pyc" if self.byte_code_only else "py",
            "ManualPDF": os.path.abspath(
                os.path.join("docs", "mysql-connector-python.pdf")
            ),
            "ManualHTML": os.path.abspath(
                os.path.join("docs", "mysql-connector-python.html")
            ),
            "UpgradeCode": upgrade_code,
            "MagicTag": get_magic_tag(),
            "BuildDir": os.path.abspath(self.build_base),
            "LibMySQLDLL": os.path.join(
                os.path.abspath(self._connc_lib), "libmysql.dll"
            )
            if self._connc_lib
            else "",
            "LIBcryptoDLL": libcrypto_dll_path,
            "LIBSSLDLL": libssl_dll_path,
            "LIBcrypto": libcrypto,
            "LIBSSL": libssl,
            "Win64": win64,
            "BitmapDir": os.path.join(os.getcwd(), "cpydist", "data", "msi"),
        }
        self.log.debug("Using WiX preprocessor variables: %s", repr(params))
        for py_ver in self._supported_versions:
            ver = py_ver.split(".")
            params[f"BDist{ver[0]}{ver[1]}"] = ""

            if ver[0] == "3" and int(ver[1]) >= 5:
                pyd_ext = ".cp%s%s-%s.pyd" % (ver[0], ver[1], pyd_arch)
            else:
                pyd_ext = ".pyd"

            params[f"CExtLibName{ver[0]}{ver[1]}"] = f"_mysql_connector{pyd_ext}"
            params[f"CExtXPBName{ver[0]}{ver[1]}"] = f"_mysqlxpb{pyd_ext}"
            params[f"HaveCExt{ver[0]}{ver[1]}"] = 0
            params[f"HaveLdapLibs{ver[0]}{ver[1]}"] = 0
            params[f"HaveKerberosLibs{ver[0]}{ver[1]}"] = 0
            params[f"HaveOCILibs{ver[0]}{ver[1]}"] = 0
            params[f"HavePlugin{ver[0]}{ver[1]}"] = 0

            if py_ver in self.pyver_bdist_paths:
                params[f"BDist{ver[0]}{ver[1]}"] = self.pyver_bdist_paths[py_ver]
                if os.path.exists(
                    os.path.join(
                        self.pyver_bdist_paths[py_ver],
                        params[f"CExtLibName{ver[0]}{ver[1]}"],
                    )
                ):
                    params[f"HaveCExt{ver[0]}{ver[1]}"] = 1
                have_plugins = False
                if os.path.exists(
                    os.path.join(
                        self.pyver_bdist_paths[py_ver],
                        "mysql",
                        "vendor",
                        "plugin",
                        "authentication_ldap_sasl_client.dll",
                    )
                ):
                    params[f"HaveLdapLibs{ver[0]}{ver[1]}"] = 1
                    have_plugins = True
                if os.path.exists(
                    os.path.join(
                        self.pyver_bdist_paths[py_ver],
                        "mysql",
                        "vendor",
                        "plugin",
                        "authentication_kerberos_client.dll",
                    )
                ):
                    params[f"HaveKerberosLibs{ver[0]}{ver[1]}"] = 1
                    have_plugins = True
                if os.path.exists(
                    os.path.join(
                        self.pyver_bdist_paths[py_ver],
                        "mysql",
                        "vendor",
                        "plugin",
                        "authentication_oci_client.dll",
                    )
                ):
                    params[f"HaveOCILibs{ver[0]}{ver[1]}"] = 1
                    have_plugins = True
                if have_plugins:
                    params[f"HavePlugin{ver[0]}{ver[1]}"] = 1

        self.log.info("### wixer params:")
        for param in params:
            self.log.info("  %s: %s", param, params[param])
        wixer.set_parameters(params)

        if not dry_run:
            try:
                wixer.compile()
                wixer.link()
            except BaseError:
                raise

        if not self.keep_temp and not dry_run:
            self.log.info("WiX: cleaning up")
            os.unlink(msi.replace(".msi", ".wixpdb"))

        return msi

    def _rename_pycached_files(self, start_dir):
        self.log.info("Renaming pycached files in %s", start_dir)
        for base, _, files in os.walk(start_dir):
            for filename in files:
                if base.endswith("__pycache__") and filename.endswith(".pyc"):
                    file_path = os.path.join(base, filename)
                    new_name = filename.split(".")[0] + ".pyc"
                    new_name_path = os.path.join(base, "..", new_name)
                    self.log.info("  renaming file: %s to: %s", filename, new_name_path)
                    os.rename(file_path, new_name_path)

        for base, _, _ in os.walk(start_dir):
            if base.endswith("__pycache__"):
                os.rmdir(base)

    def _prepare(self):
        self.log.info("Preparing installation in %s", self.build_base)
        cmd_install = self.reinitialize_command("install", reinit_subcommands=1)
        cmd_install.prefix = self.prefix
        cmd_install.with_mysql_capi = self.with_mysql_capi
        cmd_install.with_protobuf_include_dir = self.with_protobuf_include_dir
        cmd_install.with_protobuf_lib_dir = self.with_protobuf_lib_dir
        cmd_install.with_protoc = self.with_protoc
        cmd_install.with_openssl_lib_dir = self.with_openssl_lib_dir
        cmd_install.with_openssl_include_dir = self.with_openssl_include_dir
        cmd_install.extra_compile_args = self.extra_compile_args
        cmd_install.extra_link_args = self.extra_link_args
        cmd_install.static = False
        cmd_install.ensure_finalized()
        cmd_install.run()

        if self.byte_code_only:
            self._rename_pycached_files(self.prefix)

    def _get_mysql_version(self):
        mysql_version = None
        if self.with_mysql_capi and os.path.isdir(self.with_mysql_capi):
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

        return mysql_version

    def run(self):
        """Run the command."""
        if os.name != "nt":
            self.log.info("This command is only useful on Windows. Forcing dry run.")
            self.dry_run = True

        self.log.info("generating INFO_SRC and INFO_BIN files")
        write_info_src(VERSION_TEXT)
        write_info_bin(self._get_mysql_version())

        if not self.combine_stage:
            self._prepare()

        if self.prepare_stage:
            zip_fn = os.path.join(self.dist_dir, f"{os.path.abspath(self.prefix)}.zip")
            self.log.info("generating stage: %s", zip_fn)
            with zipfile.ZipFile(zip_fn, "w", zipfile.ZIP_DEFLATED) as zip_f:
                # Read all directory, subdirectories and file lists
                for root, _, files in os.walk(self.prefix):
                    for filename in files:
                        # Create the full filepath by using os module.
                        file_path = os.path.join(root, filename)
                        self.log.info("  adding file: %s", file_path)
                        zip_f.write(file_path)
            self.log.info("stage created: %s", zip_fn)
        else:
            wix.check_wix_install(
                wix_install_path=self.wix_install,
                wix_required_version=self.wix_required_version,
                dry_run=self.dry_run,
            )

            # create the Windows Installer
            msi_file = self._create_msi(dry_run=self.dry_run)
            self.log.info("created MSI as %s", msi_file)

        self.remove_temp()
