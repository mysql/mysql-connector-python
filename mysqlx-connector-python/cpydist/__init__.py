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

from .utils import ARCH, write_info_bin, write_info_src

# Load version information
VERSION = [999, 0, 0, "a", 0]
VERSION_TEXT = "999.0.0"
VERSION_EXTRA = ""
EDITION = ""
version_py = os.path.join("lib", "mysqlx", "version.py")
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
        "with-protobuf-include-dir=",
        None,
        "location of Protobuf include directory",
    ),
    ("with-protobuf-lib-dir=", None, "location of Protobuf library directory"),
    ("with-protoc=", None, "location of Protobuf protoc binary"),
    ("extra-compile-args=", None, "extra compile args"),
    ("extra-link-args=", None, "extra link args"),
]

LOGGER = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(levelname)s[%(name)s]: %(message)s"))
LOGGER.addHandler(handler)
LOGGER.setLevel(logging.WARNING)


class BaseCommand(Command):
    """Base command class for Connector/Python."""

    user_options = COMMON_USER_OPTIONS + CEXT_OPTIONS
    boolean_options = ["debug", "byte_code_only", "keep_temp"]

    with_mysqlxpb_cext = False

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
    build_base = None
    log = LOGGER

    _mysql_info = {}
    _build_mysql_lib_dir = None
    _build_protobuf_lib_dir = None

    def initialize_options(self):
        """Initialize the options."""
        self.with_mysqlxpb_cext = False
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

        if not self.with_protobuf_include_dir:
            self.with_protobuf_include_dir = os.environ.get("PROTOBUF_INCLUDE_DIR")
        if not self.with_protobuf_lib_dir:
            self.with_protobuf_lib_dir = os.environ.get("PROTOBUF_LIB_DIR")
        if not self.with_protoc:
            self.with_protoc = os.environ.get("PROTOC")
        if not self.extra_compile_args:
            self.extra_compile_args = os.environ.get("EXTRA_COMPILE_ARGS")
        if not self.extra_link_args:
            self.extra_link_args = os.environ.get("EXTRA_LINK_ARGS")

        cmd_build_ext = self.distribution.get_command_obj("build_ext")
        cmd_build_ext.with_protobuf_include_dir = self.with_protobuf_include_dir
        cmd_build_ext.with_protobuf_lib_dir = self.with_protobuf_lib_dir
        cmd_build_ext.with_protoc = self.with_protoc
        cmd_build_ext.extra_compile_args = self.extra_compile_args
        cmd_build_ext.extra_link_args = self.extra_link_args

        install = self.distribution.get_command_obj("install")
        install.with_protobuf_include_dir = self.with_protobuf_include_dir
        install.with_protobuf_lib_dir = self.with_protobuf_lib_dir
        install.with_protoc = self.with_protoc
        install.extra_compile_args = self.extra_compile_args
        install.extra_link_args = self.extra_link_args

        self.distribution.package_data = {
            "mysqlx": ["py.typed"],
        }

    def remove_temp(self):
        """Remove temporary build files."""
        if not self.keep_temp:
            cmd_build = self.get_finalized_command("build")
            if not self.dry_run:
                shutil.rmtree(cmd_build.build_base)


class BuildExt(build_ext, BaseCommand):
    """Command class for building the Connector/Python C Extensions."""

    description = "build MySQL Connector/Python C extensions"
    user_options = build_ext.user_options + CEXT_OPTIONS
    boolean_options = build_ext.boolean_options + BaseCommand.boolean_options

    def _finalize_protobuf(self):
        if not self.with_protobuf_include_dir:
            self.with_protobuf_include_dir = os.environ.get(
                "MYSQLXPB_PROTOBUF_INCLUDE_DIR"
            )

        if not self.with_protobuf_lib_dir:
            self.with_protobuf_lib_dir = os.environ.get("MYSQLXPB_PROTOBUF_LIB_DIR")

        if not self.with_protoc:
            self.with_protoc = os.environ.get("MYSQLXPB_PROTOC")

        if self.with_protobuf_include_dir:
            self.log.info(
                "Protobuf include directory: %s",
                self.with_protobuf_include_dir,
            )
            if not os.path.isdir(self.with_protobuf_include_dir):
                self.log.error("Protobuf include dir should be a directory")
                sys.exit(1)
        else:
            self.log.error("Unable to find Protobuf include directory")
            sys.exit(1)

        if self.with_protobuf_lib_dir:
            self.log.info("Protobuf library directory: %s", self.with_protobuf_lib_dir)
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
                self.log.info("copying %s -> %s", lib, self._build_protobuf_lib_dir)
                shutil.copy2(lib, self._build_protobuf_lib_dir)

        # Remove all but static libraries to force static linking
        if os.name == "posix":
            self.log.info(
                "Removing non-static Protobuf libraries from %s",
                self._build_protobuf_lib_dir,
            )
            for lib in os.listdir(self._build_protobuf_lib_dir):
                lib_path = os.path.join(self._build_protobuf_lib_dir, lib)
                if os.path.isfile(lib_path) and not lib.endswith((".a", ".dylib")):
                    os.unlink(os.path.join(self._build_protobuf_lib_dir, lib))

    def _run_protoc(self):
        base_path = os.path.join(os.getcwd(), "src", "mysqlxpb", "mysqlx")
        command = [self.with_protoc, "-I"]
        command.append(os.path.join(base_path, "protocol"))
        command.extend(glob(os.path.join(base_path, "protocol", "*.proto")))
        command.append(f"--cpp_out={base_path}")
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

        self._build_mysql_lib_dir = os.path.join(self.build_temp, "capi", "lib")
        self._build_protobuf_lib_dir = os.path.join(self.build_temp, "protobuf", "lib")

        self.with_mysqlxpb_cext = any(
            (
                self.with_protobuf_include_dir,
                self.with_protobuf_lib_dir,
                self.with_protoc,
            )
        )
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
                    self.log.warning("The '_mysqlxpb' C extension will not be built")
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
                ext.libraries.append("libprotobuf" if os.name == "nt" else "protobuf")
                # Add -std=c++11 needed for Protobuf 3.6.1
                ext.extra_compile_args.append("-std=c++11")
                self._run_protoc()

            if ext.name != "_mysqlxpb":
                disabled.append(ext)
                continue

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

        # Generate docs/INFO_BIN
        if self.with_mysqlxpb_cext:
            mysql_version = "N/A"
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
