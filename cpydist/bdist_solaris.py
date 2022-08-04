# Copyright (c) 2019, 2022, Oracle and/or its affiliates.
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

"""Implements the Distutils command for creating Solaris packages."""


import logging
import os
import platform
import subprocess
import time

from distutils import log
from distutils.archive_util import make_tarball
from distutils.command.bdist import bdist
from distutils.errors import DistutilsExecError
from distutils.file_util import copy_file

from . import COMMON_USER_OPTIONS, VERSION_EXTRA, VERSION_TEXT, BaseCommand
from .utils import write_info_bin, write_info_src

SOLARIS_PKGS = {"pure": os.path.join("cpydist", "data", "solaris")}
PKGINFO = (
    'PKG="{pkg}"\n'
    'NAME="MySQL Connector/Python {ver} {lic}, MySQL driver written in '
    'Python"\n'
    'VERSION="{ver}"\n'
    'ARCH="all"\n'
    'CLASSES="none"\n'
    'CATEGORY="application"\n'
    'VENDOR="ORACLE Corporation"\n'
    'PSTAMP="{tstamp}"\n'
    'EMAIL="MySQL Release Engineering <mysql-build@oss.oracle.com>"\n'
    'BASEDIR="/"\n'
)


class DistSolaris(bdist, BaseCommand):
    """Create a Solaris distribution."""

    platf_n = "-solaris"
    platf_v = platform.version().split(".")[0]
    platf_a = "sparc" if platform.processor() == "sparc" else "x86"
    description = "create a Solaris distribution"
    user_options = COMMON_USER_OPTIONS + [
        ("dist-dir=", "d", "directory to put final built distributions in"),
        (
            "platform=",
            "p",
            "name of the platform in resulting file (default '{0}')".format(platf_n),
        ),
        (
            "platform-version=",
            "v",
            "version of the platform in resulting file "
            "(default '{0}')".format(platf_v),
        ),
        (
            "platform-version=",
            "a",
            "architecture, i.e. 'sparc' or 'x86' in the resulting file "
            "(default '{0}')".format(platf_a),
        ),
        (
            "trans",
            "t",
            "transform the package into data stream (default 'False')",
        ),
    ]

    def initialize_options(self):
        """Initialize the options."""
        bdist.initialize_options(self)
        BaseCommand.initialize_options(self)
        self.name = self.distribution.get_name()
        self.version = self.distribution.get_version()
        self.version_extra = "-{0}".format(VERSION_EXTRA) if VERSION_EXTRA else ""
        self.keep_temp = None
        self.create_dmg = False
        self.dist_dir = None
        self.started_dir = os.getcwd()
        self.platform = self.platf_n
        self.platform_version = self.platf_v
        self.architecture = self.platf_a
        self.debug = False
        self.sun_pkg_name = "{0}-{1}{2}.pkg".format(
            self.name, self.version, self.version_extra
        )
        self.dstroot = "dstroot"
        self.sign = False
        self.identity = "MySQL Connector/Python"
        self.trans = False

    def finalize_options(self):
        """Finalize the options."""
        bdist.finalize_options(self)
        BaseCommand.finalize_options(self)
        self.set_undefined_options("bdist", ("dist_dir", "dist_dir"))
        if self.debug:
            self.log.setLevel(logging.DEBUG)
            log.set_threshold(1)  # Set Distutils logging level to DEBUG

    def _prepare_pkg_base(self, template_name, data_dir, root=""):
        """Create and populate the src base directory."""
        self.log.info("-> _prepare_pkg_base()")
        self.log.info("  template_name: {}".format(template_name))
        self.log.info("  data_dir: {}".format(data_dir))
        self.log.info("  root: {}".format(root))

        # copy and create necessary files
        sun_dist_name = template_name.format(self.name, self.version)
        self.sun_pkg_name = "{}.pkg".format(sun_dist_name)
        self.log.info("  sun_pkg_name: {}".format(self.sun_pkg_name))

        sun_path = os.path.join(root, self.dstroot)
        self.log.info("  sun_path: {}".format(sun_path))
        cwd = os.path.join(os.getcwd())
        self.log.info("Current directory: {}".format(cwd))

        copy_file_src_dst = []

        # No special folder for GPL or commercial. Files inside the directory
        # will determine what it is.
        data_path = os.path.join(
            sun_path,
            "usr",
            "share",
            template_name.format(self.name, self.version),
        )
        self.mkpath(data_path)

        lic = "(GPL)"
        sun_pkg_info = os.path.join(sun_path, "pkginfo")
        self.log.info("sun_pkg_info path: {0}".format(sun_pkg_info))
        with open(sun_pkg_info, "w") as f_pkg_info:
            f_pkg_info.write(
                PKGINFO.format(
                    ver=self.version,
                    lic=lic,
                    pkg=self.name,
                    tstamp=time.ctime(),
                )
            )
            f_pkg_info.close()

        data_path = os.path.join(
            sun_path,
            "usr",
            "share",
            template_name.format(self.name, self.version),
        )
        copy_file_src_dst += [
            (
                os.path.join(cwd, "README.txt"),
                os.path.join(data_path, "README.txt"),
            ),
            (
                os.path.join(cwd, "LICENSE.txt"),
                os.path.join(data_path, "LICENSE.txt"),
            ),
            (
                os.path.join(cwd, "CHANGES.txt"),
                os.path.join(data_path, "CHANGES.txt"),
            ),
            (
                os.path.join(cwd, "docs", "INFO_SRC"),
                os.path.join(data_path, "INFO_SRC"),
            ),
            (
                os.path.join(cwd, "docs", "INFO_BIN"),
                os.path.join(data_path, "INFO_BIN"),
            ),
            (
                os.path.join(cwd, "README.rst"),
                os.path.join(data_path, "README.rst"),
            ),
            (
                os.path.join(cwd, "CONTRIBUTING.rst"),
                os.path.join(data_path, "CONTRIBUTING.rst"),
            ),
        ]

        for src, dst in copy_file_src_dst:
            copy_file(src, dst)

    def _create_pkg(self, template_name, dmg=False, sign=False, root="", identity=""):
        """Create the Solaris package using the OS dependent commands."""
        self.log.info("-> _create_pkg()")
        self.log.info("template_name: {}".format(template_name))
        self.log.info("identity: {}".format(identity))

        sun_dist_name = template_name.format(self.name, self.version)
        self.sun_pkg_name = "{}.pkg".format(sun_dist_name)
        sun_pkg_contents = os.path.join(self.sun_pkg_name, "Contents")

        self.log.info("sun_dist_name: {}".format(sun_dist_name))
        self.log.info("sun_pkg_name: {}".format(self.sun_pkg_name))
        self.log.info("sun_pkg_contents: {}".format(sun_pkg_contents))

        sun_path = os.path.join(root, self.dstroot)
        os.chdir(sun_path)
        self.log.info("Root directory for Prototype: {}".format(os.getcwd()))

        # Creating a Prototype file, this contains a table of contents of the
        # Package, that is suitable to be used for the package creation tool.
        self.log.info(
            "Creating Prototype file on {} to describe files to "
            "install".format(self.dstroot)
        )

        prototype_path = "Prototype"
        proto_tmp = "Prototype_temp"

        with open(proto_tmp, "w") as f_out:
            cmd = ["pkgproto", "."]
            pkgp_p = subprocess.Popen(cmd, shell=False, stdout=f_out, stderr=f_out)
            res = pkgp_p.wait()
            if res != 0:
                self.log.error("pkgproto command failed with: {}".format(res))
                raise DistutilsExecError("pkgproto command failed with: {}".format(res))
            f_out.flush()

        # log Prototype contents
        self.log.info("/n>> Prototype_temp contents >>/n")
        with open(proto_tmp, "r") as f_in:
            self.log.info(f_in.readlines())
        self.log.info("/n<< Prototype_temp contents end <</n")

        # Fix Prototype file, insert pkginfo and remove Prototype
        self.log.info("Fixing folder permissions on Prototype contents")
        with open(prototype_path, "w") as f_out:
            with open(proto_tmp, "r") as f_in:
                # Add pkginfo entry at beginning of the Prototype file
                f_out.write("i pkginfo\n")
                f_out.flush()
                for line in f_in:
                    if line.startswith("f none Prototype"):
                        continue
                    elif line.startswith("f none pkginfo"):
                        continue
                    elif line.startswith("d"):
                        tokeep = line.split(" ")[:-3]
                        tokeep.extend(["?", "?", "?", "\n"])
                        f_out.write(" ".join(tokeep))
                    elif line.startswith("f"):
                        tokeep = line.split(" ")[:-2]
                        tokeep.extend(["root", "bin", "\n"])
                        f_out.write(" ".join(tokeep))
                    else:
                        f_out.write(line)
                f_out.flush()

        # log Prototype contents
        self.log.info("/n>> Prototype contents >>/n")
        with open(prototype_path, "r") as f_in:
            self.log.info(f_in.readlines())
        self.log.info("/n<< Prototype contents end <</n")

        # Create Solaris package running the package creation command pkgmk
        self.log.info("Creating package with pkgmk")

        self.log.info("Root directory for pkgmk: {}".format(os.getcwd()))
        self.spawn(["pkgmk", "-o", "-r", ".", "-d", "../", "-f", prototype_path])
        os.chdir("../")
        if self.debug:
            self.log.info("current directory: {}".format(os.getcwd()))

        # gzip the package folder
        self.log.info("creating tarball")

        make_tarball(self.sun_pkg_name, self.name, compress="gzip")

        if self.trans:
            self.log.info("Transforming package into data stream with pkgtrans")
            self.log.info("Current directory: {}".format(os.getcwd()))
            self.spawn(
                [
                    "pkgtrans",
                    "-s",
                    os.getcwd(),
                    os.path.join(os.getcwd(), self.sun_pkg_name),
                    self.name,
                ]
            )

        for base, _, files in os.walk(os.getcwd()):
            for filename in files:
                if filename.endswith(".gz") or filename.endswith(".pkg"):
                    new_name = filename.replace(
                        "{}".format(self.version),
                        "{}{}{}{}-{}".format(
                            self.version,
                            self.version_extra,
                            self.platform,
                            self.platform_version,
                            self.architecture,
                        ),
                    )
                    file_path = os.path.join(base, filename)
                    file_dest = os.path.join(self.started_dir, self.dist_dir, new_name)
                    copy_file(file_path, file_dest)
            break

    def run(self):
        """Run the command."""
        self.mkpath(self.dist_dir)
        self.debug = self.verbose

        self.log.info("Generating INFO_SRC and INFO_BIN files")
        write_info_src(VERSION_TEXT)
        write_info_bin()

        cmd_build = self.get_finalized_command("build")
        build_base = os.path.abspath(cmd_build.build_base)
        metadata_name = self.distribution.metadata.name

        data_dir = SOLARIS_PKGS["pure"]
        sun_root = os.path.join(build_base, "sun_pure")
        cmd_install = self.reinitialize_command("install", reinit_subcommands=1)
        cmd_install.byte_code_only = self.byte_code_only
        cmd_install.compile = self.byte_code_only
        cmd_install.distribution.metadata.name = metadata_name
        cmd_install.with_mysql_capi = None
        cmd_install.root = os.path.join(sun_root, self.dstroot)
        cmd_install.ensure_finalized()
        cmd_install.run()

        template_name = ["{}"]
        if self.label:
            template_name.append("-{}".format(self.label))
        template_name.append("-{}")

        self._prepare_pkg_base("".join(template_name), data_dir, root=sun_root)
        self._create_pkg(
            "".join(template_name),
            dmg=self.create_dmg,
            root=sun_root,
            sign=self.sign,
            identity=self.identity,
        )

        os.chdir(self.started_dir)

        self.remove_temp()
