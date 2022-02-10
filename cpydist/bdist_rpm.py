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

"""Implements the Distutils commands for creating RPM packages."""

import os
import subprocess

from distutils.dir_util import mkpath
from distutils.errors import DistutilsError
from distutils.file_util import copy_file

from . import BaseCommand, EDITION, VERSION, VERSION_EXTRA
from .utils import linux_distribution


RPM_SPEC = os.path.join("cpydist", "data", "rpm",
                        "mysql-connector-python.spec")
LINUX_DIST = linux_distribution()
VERSION_TEXT_SHORT = "{0}.{1}.{2}".format(*VERSION[0:3])


class DistRPM(BaseCommand):
    """Create a RPM distribution."""

    description = "create a RPM distribution"
    user_options = BaseCommand.user_options + [
        ("build-base=", "d",
         "base directory for build library"),
        ("dist-dir=", "d",
         "directory to put final built distributions in"),
        ('pre-release', None,
         "this is a pre-release (changes RPM release number)"),
        ("rpm-base=", "d",
         "base directory for creating RPMs (default <bdist-dir>/rpm)"),
        ("pre-release", None,
         "this is a pre-release (changes RPM release number)"),
        ("python3-pkgversion=", None,
         "Python 3 PKG version"),
    ]

    build_base = None
    dist_dir = None
    rpm_base = None
    pre_release = None
    python3_pkgversion = None

    _cmd_dist_tarball = "sdist"
    _rpm_dirs = {}

    def finalize_options(self):
        """Finalize the options."""
        BaseCommand.finalize_options(self)
        self.set_undefined_options("build",
                                   ("build_base", "build_base"))
        self.set_undefined_options(self._cmd_dist_tarball,
                                   ("dist_dir", "dist_dir"))

        if not self.rpm_base:
            self.rpm_base = os.path.abspath(
                os.path.join(self.build_base, "rpmbuild"))

        if not self.python3_pkgversion:
            self.python3_pkgversion = os.environ.get("PYTHON3_PKGVERSION")

    def _populate_rpm_topdir(self, rpm_base):
        """Create and populate the RPM topdir."""
        mkpath(rpm_base)
        dirs = ["BUILD", "RPMS", "SOURCES", "SPECS", "SRPMS"]
        self._rpm_dirs = {}
        for dirname in dirs:
            self._rpm_dirs[dirname] = os.path.join(rpm_base, dirname)
            self.mkpath(self._rpm_dirs[dirname])

    def _check_rpmbuild(self):
        """Check if we can run rpmbuild.

        Raises DistutilsError when rpmbuild is not available.
        """
        try:
            devnull = open(os.devnull, "w")
            subprocess.Popen(["rpmbuild", "--version"],
                             stdin=devnull, stdout=devnull, stderr=devnull)
        except OSError:
            raise DistutilsError("Could not execute rpmbuild. Make sure "
                                 "it is installed and in your PATH")

    def _create_rpm(self, rpm_name, spec):
        """Create RPM."""
        self.log.info("Creating RPM using rpmbuild")
        macro_bdist_dir = "bdist_dir {}".format(os.path.join(rpm_name, ""))

        cmd = [
            "rpmbuild",
            "-ba",
            "--define", macro_bdist_dir,
            "--define", "_topdir {}".format(os.path.abspath(self.rpm_base)),
            "--define", "version {}".format(VERSION_TEXT_SHORT),
            spec
        ]

        if not self.verbose:
            cmd.append("--quiet")

        if EDITION:
            cmd.extend(["--define", "edition {}".format(EDITION)])

        if self.label:
            cmd.extend(["--define", "label {}".format(self.label)])

        if self.byte_code_only:
            cmd.extend(["--define", "byte_code_only 1"])
            cmd.extend(["--define", "lic_type Commercial"])

        if self.pre_release:
            cmd.extend(["--define", "pre_release 1"])

        if self.python3_pkgversion:
            cmd.extend(["--define", "python3_pkgversion {}".format(self.python3_pkgversion)])

        if VERSION_EXTRA:
            cmd.extend(["--define", "version_extra {}".format(VERSION_EXTRA)])

        cmd.extend(["--define", "mysql_capi {}".format(self.with_mysql_capi)])
        if self.with_openssl_include_dir:
            cmd.extend(["--define", "openssl_include_dir {}"
                                "".format(self.with_openssl_include_dir)])
            cmd.extend(["--define", "openssl_lib_dir {}"
                                "".format(self.with_openssl_lib_dir)])
        cmd.extend(["--define", "protobuf_include_dir {}"
                                "".format(self.with_protobuf_include_dir)])
        cmd.extend(["--define", "protobuf_lib_dir {}"
                                "".format(self.with_protobuf_lib_dir)])
        cmd.extend(["--define", "protoc {}".format(self.with_protoc)])

        if self.extra_compile_args:
            cmd.extend(["--define", "extra_compile_args '{0}'"
                                    "".format(self.extra_compile_args)])
        if self.extra_link_args:
            cmd.extend(["--define",
                        "extra_link_args '{0}'".format(self.extra_link_args)])

        self.spawn(cmd)

        for base, dirs, files in os.walk(self.rpm_base):
            for filename in files:
                if filename.endswith(".rpm"):
                    filepath = os.path.join(base, filename)
                    copy_file(filepath, self.dist_dir)

    def run(self):
        """Run the command."""
        if not self.dry_run:
            self._check_rpmbuild()

        self.mkpath(self.dist_dir)
        self._populate_rpm_topdir(self.rpm_base)

        cmd_sdist = self.get_finalized_command(self._cmd_dist_tarball)
        cmd_sdist.dist_dir = self._rpm_dirs["SOURCES"]
        cmd_sdist.label = self.label
        cmd_sdist.run()

        rpm_name = "mysql-connector-python-{}".format("-{}".format(self.label)
                                                      if self.label else "")
        self._create_rpm(rpm_name=rpm_name, spec=RPM_SPEC)

        self.remove_temp()
