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

"""Implements the Distutils commands for creating Debian packages."""

import os
import re
import subprocess
import sys

from datetime import datetime

from distutils.file_util import copy_file, move_file

from . import BaseCommand, EDITION, VERSION, VERSION_EXTRA
from .utils import unarchive_targz, linux_distribution


DEBIAN_ROOT = os.path.join("cpydist", "data", "deb")
DPKG_MAKER = "dpkg-buildpackage"
LINUX_DIST = linux_distribution()
VERSION_TEXT_SHORT = "{0}.{1}.{2}".format(*VERSION[0:3])

GPL_LIC_TEXT = """\
This is a release of MySQL Connector/Python, Oracle's dual-
 license Python Driver for MySQL. For the avoidance of
 doubt, this particular copy of the software is released
 under the version 2 of the GNU General Public License.
 MySQL Connector/Python is brought to you by Oracle.
"""

class DistDeb(BaseCommand):
    """Create a Debian distribution."""

    description = "create a Debian distribution"
    debian_files = [
        "changelog",
        "compat",
        "control",
        "copyright",
        "docs",
        "mysql-connector-python-py3.postinst",
        "mysql-connector-python-py3.postrm",
        "mysql-connector-python.postinst",
        "mysql-connector-python.postrm",
        "rules",
    ]
    user_options = BaseCommand.user_options + [
        ("dist-dir=", "d",
         "directory to put final built distributions in"),
        ("platform=", "p",
         "name of the platform in resulting files "
         "(default '%s')" % LINUX_DIST[0].lower()),
        ("sign", None,
         "sign the Debian package"),
    ]
    boolean_options = BaseCommand.boolean_options + ["sign"]

    dist_dir = None
    with_cext = False

    def initialize_options(self):
        """Initialize the options."""
        BaseCommand.initialize_options(self)

        self.platform = LINUX_DIST[0].lower()
        if "debian" in self.platform:
            # For Debian we only use the first part of the version, Ubuntu two
            self.platform_version = LINUX_DIST[1].split(".", 2)[0]
        else:
            self.platform_version = ".".join(LINUX_DIST[1].split(".", 2)[0:2])
        self.sign = False
        self.debian_support_dir = DEBIAN_ROOT
        self.edition = EDITION
        self.codename = linux_distribution()[2].lower()
        self.version_extra = "-{0}".format(VERSION_EXTRA) \
            if VERSION_EXTRA else ""

    def finalize_options(self):
        """Finalize the options."""
        BaseCommand.finalize_options(self)

        cmd_build = self.get_finalized_command("build")
        self.build_base = cmd_build.build_base
        if not self.dist_dir:
            self.dist_dir = "dist"
        self.with_cext = any((self.with_mysql_capi,
                              self.with_protobuf_include_dir,
                              self.with_protobuf_lib_dir, self.with_protoc))

    @property
    def _have_python3(self):
        """Check whether this distribution has Python 3 support."""
        try:
            devnull = open(os.devnull, "w")
            subprocess.Popen(["py3versions"],
                             stdin=devnull,
                             stdout=devnull,
                             stderr=devnull)
        except OSError:
            return False

        return True

    def _get_orig_name(self):
        """Return name for tarball according to Debian's policies."""
        return "%(name)s%(label)s_%(version)s%(version_extra)s.orig" % {
            "name": self.distribution.get_name(),
            "label": "-{}".format(self.label) if self.label else "",
            "version": self.distribution.get_version(),
            "version_extra": self.version_extra
        }

    def _get_changes(self):
        """Get changes from CHANGES.txt."""
        log_lines = []
        found_version = False
        found_items = False
        with open("CHANGES.txt", "r") as fp:
            for line in fp.readlines():
                line = line.rstrip()
                if line.endswith(VERSION_TEXT_SHORT):
                    found_version = True
                if not line.strip() and found_items:
                    break
                elif found_version and line.startswith("- "):
                    log_lines.append(" " * 2 + "* " + line[2:])
                    found_items = True

        return log_lines

    def _populate_debian(self):
        """Copy and make files ready in the debian/ folder."""
        for afile in self.debian_files:
            copy_file(os.path.join(self.debian_support_dir, afile),
                      self.debian_base)

        copy_file(os.path.join(self.debian_support_dir, "source", "format"),
                  os.path.join(self.debian_base, "source"))

        # Update the version and log in the Debian changelog
        changelog_file = os.path.join(self.debian_base, "changelog")
        with open(changelog_file, "r") as fp:
            changelog = fp.readlines()
        self.log.info("changing changelog '%s' version and log",
                      changelog_file)

        log_lines = self._get_changes()
        if not log_lines:
            self.log.error("Failed reading change history from CHANGES.txt")
            log_lines.append("  * (change history missing)")

        new_changelog = []
        first_line = True
        regex = re.compile(r".*\((\d+\.\d+.\d+-1)\).*")
        for line in changelog:
            line = line.rstrip()
            match = regex.match(line)
            if match:
                version = match.groups()[0]
                line = line.replace(version,
                                    "{0}.{1}.{2}-1".format(*VERSION[0:3]))
            if first_line:
                if self.codename == "":
                    proc = subprocess.Popen(["lsb_release", "-c"],
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.STDOUT)
                    codename = proc.stdout.read().split()[-1]
                    self.codename = codename.decode() \
                        if sys.version_info[0] == 3 else codename
                if self.label:
                    line = line.replace(
                        "mysql-connector-python",
                        "mysql-connector-python-{}".format(self.label))
                line = line.replace("UNRELEASED", self.codename)
                line = line.replace("-1",
                                    "{version_extra}-1{platform}{version}"
                                    .format(platform=self.platform,
                                            version=self.platform_version,
                                            version_extra=self.version_extra))
                first_line = False
            if "* Changes here." in line:
                for change in log_lines:
                    new_changelog.append(change)
            elif line.startswith(" --") and "@" in line:
                utcnow = datetime.utcnow().strftime(
                    "%a, %d %b %Y %H:%M:%S +0000")
                line = re.sub(r"( -- .* <.*@.*>  ).*", r"\1" + utcnow, line)
                new_changelog.append(line + "\n")
            else:
                new_changelog.append(line)

        with open(changelog_file, "w") as changelog:
            changelog.write("\n".join(new_changelog))

        control_file = os.path.join(self.debian_base, "control")
        if self.label:
            # Update the Source, Package and Conflicts fields
            # in control file, if self.label is present
            with open(control_file, "r") as fp:
                control = fp.readlines()

            self.log.info("changing control '%s' Source, Package and Conflicts fields",
                          control_file)

            new_control = []
            add_label_regex = re.compile(r"^((?:Source|Package): mysql-connector-python)")
            remove_label_regex = re.compile("^(Conflicts: .*?)-{}".format(self.label))
            for line in control:
                line = line.rstrip()

                match = add_label_regex.match(line)
                if match:
                    line = add_label_regex.sub(r"\1-{}".format(self.label), line)

                match = remove_label_regex.match(line)
                if match:
                    line = remove_label_regex.sub(r"\1", line)

                new_control.append(line)

            with open(control_file, "w") as fp:
                fp.write("\n".join(new_control))

        lic_text = GPL_LIC_TEXT

        if self.byte_code_only:
            copyright_file = os.path.join(self.debian_base, "copyright")
            self.log.info("Reading license text from copyright file ({})"
                          "".format(copyright_file))
            with open(copyright_file, "r") as fp:
                # Skip to line just before the text we want to copy
                while True:
                    line = fp.readline()
                    if not line:
                        break
                    if line.startswith("License: Commercial"):
                        # Read the rest of the text
                        lic_text = fp.read()

        with open(control_file, "r") as fp:
            control_text = fp.read()

        self.log.info("Updating license text in control file")
        new_control = re.sub(r"@LICENSE@", lic_text, control_text)
        with open(control_file, "w") as fp:
            fp.write(new_control)

    def _prepare(self, tarball=None, base=None):
        """Prepare Debian files."""
        # Rename tarball to conform Debian's Policy
        if tarball:
            self.orig_tarball = os.path.join(
                os.path.dirname(tarball),
                self._get_orig_name()) + ".tar.gz"
            move_file(tarball, self.orig_tarball)

            unarchive_targz(self.orig_tarball)
            self.debian_base = os.path.join(
                tarball.replace(".tar.gz", ""), "debian")
        elif base:
            self.debian_base = os.path.join(base, "debian")

        self.mkpath(self.debian_base)
        self.mkpath(os.path.join(self.debian_base, "source"))
        self._populate_debian()

    def _make_dpkg(self):
        """Create Debian package in the source distribution folder."""
        self.log.info("creating Debian package using '%s'", DPKG_MAKER)

        orig_pwd = os.getcwd()
        os.chdir(os.path.join(self.build_base,
                 self.distribution.get_fullname()))
        cmd = [DPKG_MAKER, "-uc"]

        if not self.sign:
            cmd.append("-us")

        success = True
        env = os.environ.copy()
        env["MYSQL_CAPI"] = self.with_mysql_capi or ""
        env["OPENSSL_INCLUDE_DIR"] = self.with_openssl_include_dir or ""
        env["OPENSSL_LIB_DIR"] = self.with_openssl_lib_dir or ""
        env["MYSQLXPB_PROTOBUF_INCLUDE_DIR"] = \
            self.with_protobuf_include_dir or ""
        env["MYSQLXPB_PROTOBUF_LIB_DIR"] = self.with_protobuf_lib_dir or ""
        env["MYSQLXPB_PROTOC"] = self.with_protoc or ""
        env["WITH_CEXT"] = "1" if self.with_cext else ""
        env["EXTRA_COMPILE_ARGS"] = self.extra_compile_args or ""
        env["EXTRA_LINK_ARGS"] = self.extra_link_args or ""
        env["LABEL"] = self.label if self.label else "0"
        env["BYTE_CODE_ONLY"] = "1" if self.byte_code_only else ""
        env["SKIP_VENDOR"] = "1" if self.skip_vendor else ""
        proc = subprocess.Popen(cmd,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                universal_newlines=True,
                                env=env)
        stdout, stderr = proc.communicate()
        for line in stdout.split("\n"):
            if self.debug:
                self.log.info(line)
            if "error:" in line or "E: " in line:
                if not self.debug:
                    self.log.info(line)
                success = False

        if stderr:
            for line in stderr.split("\n"):
                if self.debug:
                    self.log.info(line)
                if "error:" in line or "E: " in line:
                    if not self.debug:
                        self.log.info(line)
                    success = False

        os.chdir(orig_pwd)
        return success

    def _move_to_dist(self):
        """Move *.deb files to dist/ (dist_dir) folder."""
        for base, dirs, files in os.walk(self.build_base):
            for filename in files:
                if "-py3" in filename and not self._have_python3:
                    continue
                if not self.with_mysql_capi and "cext" in filename:
                    continue
                if filename.endswith(".deb"):
                    filepath = os.path.join(base, filename)
                    copy_file(filepath, self.dist_dir)

    def run(self):
        """Run the command."""
        self.mkpath(self.dist_dir)

        sdist = self.reinitialize_command("sdist")
        sdist.dist_dir = self.build_base
        sdist.formats = ["gztar"]
        sdist.label = self.label
        sdist.ensure_finalized()
        sdist.run()

        self._prepare(sdist.archive_files[0])
        success = self._make_dpkg()

        if not success:
            self.log.error("Building Debian package failed")
        else:
            self._move_to_dist()

        self.remove_temp()
