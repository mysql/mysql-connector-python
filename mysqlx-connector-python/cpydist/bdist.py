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

"""Implements the Distutils command 'bdist'.

Creates a binary distribution.
"""

import logging
import os
import shutil

from pathlib import Path
from sysconfig import get_platform, get_python_version

try:
    from setuptools.logging import set_threshold
except ImportError:
    set_threshold = None

from . import COMMON_USER_OPTIONS, EDITION, LOGGER, VERSION_TEXT, Command
from .utils import add_docs, write_info_bin, write_info_src


class DistBinary(Command):
    """Create a generic binary distribution.

    DistBinary is meant to replace distutils.bdist.
    """

    description = "create a built (binary) distribution"
    user_options = COMMON_USER_OPTIONS + [
        ("bdist-base=", "b", "temporary directory for creating built distributions"),
        (
            "plat-name=",
            "p",
            "platform name to embed in generated filenames "
            "(default: %s)" % get_platform(),
        ),
        (
            "bdist-dir=",
            "d",
            "temporary directory for creating the distribution",
        ),
        (
            "dist-dir=",
            "d",
            "directory to put final built distributions in " "[default: dist]",
        ),
        (
            "owner=",
            "u",
            "Owner name used when creating a tar file" " [default: current user]",
        ),
        (
            "group=",
            "g",
            "Group name used when creating a tar file" " [default: current group]",
        ),
    ]

    boolean_options = ["debug", "byte-code-only", "keep-temp"]
    log = LOGGER

    def initialize_options(self):
        """Initialize the options."""
        self.bdist_dir = None
        self.bdist_base = None
        self.dist_dir = None
        self.plat_name = None
        self.skip_build = 0
        self.group = None
        self.owner = None
        self.byte_code_only = False
        self.label = None
        self.edition = EDITION
        self.debug = False
        self.keep_temp = False

    def finalize_options(self):
        """Finalize the options."""

        if self.plat_name is None:
            self.plat_name = self.get_finalized_command("build").plat_name

        if self.bdist_base is None:
            build_base = self.get_finalized_command("build").build_base
            self.bdist_base = os.path.join(build_base, f"bdist.{self.plat_name}")

        if self.dist_dir is None:
            self.dist_dir = "dist"

        if self.bdist_dir is None:
            self.bdist_dir = os.path.join(self.dist_dir, f"bdist.{self.plat_name}")

        if self.debug:
            self.log.setLevel(logging.DEBUG)
            if set_threshold:
                # Set setuptools logging level to DEBUG
                set_threshold(1)

        def _get_fullname():
            label = f"-{self.label}" if self.label else ""
            python_version = f"-py{get_python_version()}" if self.byte_code_only else ""
            name = self.distribution.get_name()
            version = self.distribution.get_version()
            edition = self.edition or ""
            return f"{name}{label}-{version}{edition}{python_version}"

        self.distribution.get_fullname = _get_fullname

    def _remove_sources(self):
        """Remove Python source files from the build directory."""
        for base, _, files in os.walk(self.bdist_dir):
            for filename in files:
                if filename.endswith(".py"):
                    filepath = os.path.join(base, filename)
                    self.log.info("Removing source '%s'", filepath)
                    os.unlink(filepath)

    def _copy_from_pycache(self, start_dir):
        """Copy .py files from __pycache__."""
        for base, _, files in os.walk(start_dir):
            for filename in files:
                if filename.endswith(".pyc"):
                    filepath = os.path.join(base, filename)
                    new_name = f"{filename.split('.')[0]}.pyc"
                    os.rename(filepath, os.path.join(base, "..", new_name))
        for base, _, files in os.walk(start_dir):
            if base.endswith("__pycache__"):
                os.rmdir(base)

    def run(self):
        """Run the command."""
        self.log.info("Installing library code to %s", self.bdist_dir)
        self.log.info("Generating INFO_SRC and INFO_BIN files")
        write_info_src(VERSION_TEXT)
        write_info_bin()

        dist_name = self.distribution.get_fullname()
        self.dist_target = os.path.join(self.dist_dir, dist_name)
        self.log.info("Distribution will be available as '%s'", self.dist_target)

        # build command: just to get the build_base
        cmdbuild = self.get_finalized_command("build")
        self.build_base = cmdbuild.build_base

        # install command
        install = self.reinitialize_command("install_lib", reinit_subcommands=1)
        install.compile = False
        install.warn_dir = 0
        install.install_dir = self.bdist_dir

        self.log.info("Installing to %s", self.bdist_dir)
        self.run_command("install_lib")

        # install_egg_info command
        cmd_egginfo = self.get_finalized_command("install_egg_info")
        cmd_egginfo.install_dir = self.bdist_dir
        self.run_command("install_egg_info")

        installed_files = install.get_outputs()

        # compile and remove sources
        if self.byte_code_only:
            install.compile = True
            install.byte_compile(installed_files)
            self._remove_sources()
            if get_python_version().startswith("3"):
                self.log.info("Copying byte code from __pycache__")
                self._copy_from_pycache(os.path.join(self.bdist_dir, "mysqlx"))

        # create distribution
        info_files = [
            ("README.txt", "README.txt"),
            ("LICENSE.txt", "LICENSE.txt"),
            ("README.rst", "README.rst"),
            ("CONTRIBUTING.rst", "CONTRIBUTING.rst"),
            ("docs/INFO_SRC", "INFO_SRC"),
            ("docs/INFO_BIN", "INFO_BIN"),
        ]

        shutil.copytree(self.bdist_dir, self.dist_target, dirs_exist_ok=True)
        Path(self.dist_target).mkdir(parents=True, exist_ok=True)
        for src, dst in info_files:
            if dst is None:
                shutil.copyfile(src, self.dist_target)
            else:
                shutil.copyfile(src, os.path.join(self.dist_target, dst))

        add_docs(os.path.join(self.dist_target, "docs"))

        if not self.keep_temp:
            shutil.rmtree(self.build_base)
