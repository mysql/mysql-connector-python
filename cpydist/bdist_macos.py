# Copyright (c) 2020, Oracle and/or its affiliates.
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

"""Implements the Distutils commands making packages for macOS."""

import os
import platform
import re
import string

from distutils.command.bdist import bdist
from distutils.dir_util import copy_tree, remove_tree
from distutils.file_util import copy_file

from . import BaseCommand, VERSION, VERSION_EXTRA


MACOS_ROOT = os.path.join("cpydist", "data", "macos")


def _get_platform():
    platf_v = ""
    mac_ver = platform.mac_ver()[0]
    if mac_ver:
        major_minor = mac_ver.split('.', 2)[0:2]
        platf_v_major, platf_v_minor = int(major_minor[0]), int(major_minor[1])
        platf_v = "{}.{}".format(platf_v_major, platf_v_minor)
    return ("-macos", platf_v)


class DistMacOS(bdist, BaseCommand):
    """Create a macOS distribution."""

    description = "create a macOS distribution"
    platf_n, platf_v = _get_platform()
    user_options = BaseCommand.user_options + [
        ("create-dmg", "c",
         "create a dmg image from the resulting package file "
         "(default 'False')"),
        ("sign", "s",
         "signs the package file (default 'False')"),
        ("identity=", "i",
         "identity or name of the certificate to use to sign the package file"
         "(default 'MySQL Connector/Python')"),
        ("dist-dir=", "d",
         "directory to put final built distributions in"),
        ('platform=', 'p',
         "name of the platform in resulting file "
         "(default '{}')".format(platf_n)),
        ("platform-version=", "v",
         "version of the platform in resulting file "
         "(default '{}')".format(platf_v)),
    ]
    boolean_options = BaseCommand.boolean_options + ["create-dmg", "sign"]

    def initialize_options(self):
        """Initialize the options."""
        bdist.initialize_options(self)
        BaseCommand.initialize_options(self)

        self.name = self.distribution.get_name()
        self.version = self.distribution.get_version()
        self.version_extra = "-{0}".format(VERSION_EXTRA) \
                             if VERSION_EXTRA else ""
        self.create_dmg = False
        self.dist_dir = None
        self.started_dir = os.getcwd()
        self.platform = self.platf_n
        self.platform_version = self.platf_v
        self.debug = False
        self.macos_pkg_name = "{0}-{1}{2}.pkg".format(self.name,
                                                      self.version,
                                                      self.version_extra)
        if self.label:
            self.macos_pkg_name = "{0}-{1}-{2}{3}.pkg".format(self.name,
                                                          self.label,
                                                          self.version,
                                                          self.version_extra)

        self.dstroot = "dstroot"
        self.sign = False
        self.identity = "MySQL Connector/Python"

    def finalize_options(self):
        """Finalize the options."""
        bdist.finalize_options(self)
        BaseCommand.finalize_options(self)
        cmd_build = self.get_finalized_command("build")
        self.build_base = cmd_build.build_base
        self.set_undefined_options("bdist", ("dist_dir", "dist_dir"))

    def _prepare_pgk_base(self, macos_dist_name, data_dir, root='', gpl=True):
        """Create and populate the src base directory."""
        # Copy and create necessary files
        macos_pkg_name = "{0}.pkg".format(macos_dist_name)
        macos_pkg_contents = os.path.join(root, macos_pkg_name, "Contents")
        macos_pkg_resrc = os.path.join(macos_pkg_contents, "Resources")
        self.mkpath(macos_pkg_resrc)
        macos_path = os.path.join(root, self.dstroot)

        cwd = os.path.join(os.getcwd())

        copy_file_src_dst = [
            (os.path.join(data_dir, "PkgInfo"),
             os.path.join(macos_pkg_contents, "PkgInfo")),
        ]

        readme_loc = os.path.join(macos_pkg_resrc, "README.txt")
        license_loc = os.path.join(macos_pkg_resrc, "LICENSE.txt")
        changes_loc = os.path.join(macos_pkg_resrc, "CHANGES.txt")

        data_path = os.path.join(
            macos_path, "usr", "local",
            macos_dist_name,
        )

        self.mkpath(data_path)

        copy_file_src_dst += [
            (os.path.join(cwd, "README.txt"), readme_loc),
            (os.path.join(cwd, "LICENSE.txt"), license_loc),
            (os.path.join(cwd, "CHANGES.txt"), changes_loc),
            (os.path.join(cwd, "README.txt"),
             os.path.join(data_path, "README.txt")),
            (os.path.join(cwd, "LICENSE.txt"),
             os.path.join(data_path, "LICENSE.txt")),
            (os.path.join(cwd, "CHANGES.txt"),
             os.path.join(data_path, "CHANGES.txt")),
        ]

        copy_file_src_dst += [
            (os.path.join(cwd, "docs", "INFO_SRC"),
             os.path.join(data_path, "INFO_SRC")),
            (os.path.join(cwd, "docs", "INFO_BIN"),
             os.path.join(data_path, "INFO_BIN")),
            (os.path.join(cwd, "README.rst"),
             os.path.join(data_path, "README.rst")),
            (os.path.join(cwd, "CONTRIBUTING.rst"),
             os.path.join(data_path, "CONTRIBUTING.rst")),
        ]

        pkg_files = [
            (os.path.join(data_dir, "Info.plist"),
             os.path.join(macos_pkg_contents, "Info.plist")),
            (os.path.join(data_dir, "Description.plist"),
             os.path.join(macos_pkg_resrc, "Description.plist")),
            (os.path.join(data_dir, "Welcome.rtf"),
             os.path.join(macos_pkg_resrc, "Welcome.rtf")),
        ]

        major_version = self.version.split(".")[0]
        minor_version = self.version.split(".")[1]

        for pkg_file, dest_file in pkg_files:
            with open(pkg_file) as fp:
                template = string.Template(fp.read())

                content = template.substitute(
                    version=self.version,
                    major=major_version,
                    minor=minor_version
                )

                with open(dest_file, "w") as fp_dest:
                    fp_dest.write(content)

        for src, dst in copy_file_src_dst:
            copy_file(src, dst)

        info_files = [
            license_loc,
            readme_loc,
            os.path.join(data_path, "README.txt"),
            os.path.join(data_path, "LICENSE.txt"),
        ]
        re_needle = r"Connector/Python \d{1,2}.\d{1,2}"
        xy_needle = "Connector/Python X.Y"
        version_fmt = "Connector/Python {0}.{1}"
        for info_file in info_files:
            self.log.info("correcting version in %s", info_file)
            with open(info_file, "r+") as fp:
                content = fp.readlines()
                for i, line in enumerate(content):
                    content[i] = re.sub(
                        re_needle, version_fmt.format(*VERSION[0:2]),
                        line)
                    content[i] = line.replace(
                        xy_needle, version_fmt.format(*VERSION[0:2]))

                fp.seek(0)
                fp.write("".join(content))

    def _create_pkg(self, macos_dist_name, dmg=False, sign=False, root="",
                    identity=''):
        """Create the macOS pkg and a dmg image if it is required."""
        macos_pkg_name = "{0}.pkg".format(macos_dist_name)
        macos_pkg_contents = os.path.join(macos_pkg_name, "Contents")

        os.chdir(root)
        self.log.info("Root directory: {0}".format(os.getcwd()))

        # Create a bom(8) file to tell the installer which files need to be
        # installed
        self.log.info("Creating Archive.bom file, that describe files to "
                      "install")
        self.log.info("dstroot {0}".format(self.dstroot))
        archive_bom_path = os.path.join(macos_pkg_contents, "Archive.bom")
        self.spawn(["mkbom", self.dstroot, archive_bom_path])

        # Create an archive of the files to install
        self.log.info("creating Archive.pax with files to be installed")
        os.chdir(self.dstroot)

        pax_file = "../{NAME}/Contents/Archive.pax".format(NAME=macos_pkg_name)
        self.spawn(["pax", "-w", "-x", "cpio", ".", "-f", pax_file])
        os.chdir("../")

        # Sign the package
        # In Order to be possible the certificates needs to be installed
        if sign:
            self.log.info("Signing the package")
            macos_pkg_name_signed = "{0}_s.pkg".format(macos_dist_name)
            self.spawn(["productsign", "--sign", identity,
                        macos_pkg_name,
                        macos_pkg_name_signed])
            self.spawn(["spctl", "-a", "-v", "--type", "install",
                        macos_pkg_name_signed])
            macos_pkg_name = macos_pkg_name_signed

        # Create a .dmg image
        if dmg:
            self.log.info("Creating dmg file")
            self.spawn(["hdiutil", "create", "-volname", macos_dist_name,
                        "-srcfolder", macos_pkg_name, "-ov", "-format",
                        "UDZO", "{0}.dmg".format(macos_dist_name)])

        self.log.info("Current directory: {0}".format(os.getcwd()))

        for base, dirs, files in os.walk(os.getcwd()):
            for filename in files:
                if filename.endswith(".dmg"):
                    new_name = filename.replace(
                        "{0}".format(self.version),
                        "{0}{1}{2}{3}".format(self.version, self.version_extra,
                                              self.platform,
                                              self.platform_version)
                    )
                    file_path = os.path.join(base, filename)
                    file_dest = os.path.join(self.started_dir,
                                             self.dist_dir, new_name)
                    copy_file(file_path, file_dest)
                    break
            for dir_name in dirs:
                if dir_name.endswith(".pkg"):
                    new_name = dir_name.replace(
                        "{0}".format(self.version),
                        "{0}{1}{2}{3}".format(self.version, self.version_extra,
                                              self.platform,
                                              self.platform_version)
                    )
                    dir_dest = os.path.join(self.started_dir,
                                            self.dist_dir, new_name)
                    copy_tree(dir_name, dir_dest)
                    break
            break

    def run(self):
        """Run the command."""
        self.mkpath(self.dist_dir)
        self.debug = self.verbose

        cmd_install = self.reinitialize_command("install",
                                                reinit_subcommands=1)
        cmd_install.with_mysql_capi = self.with_mysql_capi
        cmd_install.with_openssl_include_dir = self.with_openssl_include_dir
        cmd_install.with_openssl_lib_dir = self.with_openssl_lib_dir
        cmd_install.with_protobuf_include_dir = self.with_protobuf_include_dir
        cmd_install.with_protobuf_lib_dir = self.with_protobuf_lib_dir
        cmd_install.with_protoc = self.with_protoc
        cmd_install.extra_compile_args = self.extra_compile_args
        cmd_install.extra_link_args = self.extra_link_args
        cmd_install.root = os.path.join(self.dist_dir, self.dstroot)
        cmd_install.ensure_finalized()
        cmd_install.run()

        system_dir = os.path.join(self.dist_dir, self.dstroot, "System")
        if os.path.exists(system_dir) and os.path.isdir(system_dir):
            remove_tree(system_dir)

        macos_dist_name = "{0}-{1}".format(self.name, self.version)
        if self.label:
            macos_dist_name = "{0}-{1}-{2}".format(self.name, self.label, self.version)

        self._prepare_pgk_base(macos_dist_name, MACOS_ROOT, root=self.dist_dir)
        self._create_pkg(macos_dist_name,
                         dmg=self.create_dmg,
                         root=self.dist_dir,
                         sign=self.sign,
                         identity=self.identity)

        os.chdir(self.started_dir)

        self.remove_temp()
