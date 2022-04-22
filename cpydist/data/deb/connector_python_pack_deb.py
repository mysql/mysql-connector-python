#!/usr/bin/env python3

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

"""Script to create MySQL Connector/Python Debian packages"""

# This is a slight rewrite of the Distutils script "cpydist/bdist_deb.py"
#
# To make pylint not complain about import, use something like
# SRCDIR=.....
# PYTHONPATH=$SRCDIR/lib/mysql/connector pylint connector_python_pack_deb.py
#
# pylint: disable=C0103,C0116,W0511,R0912,R0914,R0915

import os
import sys
import re
import argparse
from subprocess import Popen, PIPE, STDOUT
from datetime import datetime
from shutil import copytree, rmtree, copyfile


##############################################################################
#
#  Basic settings
#
##############################################################################

product_name = "mysql-connector-python"

debian_support_dir = "cpydist/data/deb"

no_debug_filter = r'^(byte-compiling|copying|creating /|dpkg-source: warning: ignoring deletion)'

script_name = os.path.basename(__file__).replace(".py", "")

##############################################################################
#
#  Command line handling
#
##############################################################################

# FIXME should lots of these, all, have required=True ?

parser = argparse.ArgumentParser(description=__doc__)

parser.add_argument("source_directory",
                    nargs=1,
                    help="Source directory")
parser.add_argument("--with-mysql-capi",
                    action="store",
                    default="",
                    help="")
parser.add_argument("--with-openssl-include-dir",
                    action="store",
                    default="",
                    help="")
parser.add_argument("--with-openssl-lib-dir",
                    action="store",
                    default="",
                    help="")
parser.add_argument("--with-protobuf-include-dir",
                    action="store",
                    default="",
                    help="")
parser.add_argument("--with-protobuf-lib-dir",
                    action="store",
                    default="",
                    help="")
parser.add_argument("--with-protoc",
                    action="store",
                    default="",
                    help="")
parser.add_argument("--extra-compile-args",
                    action="store",
                    default="",
                    help="")
parser.add_argument("--extra-link-args",
                    action="store",
                    default="",
                    help="")
parser.add_argument("--label",
                    action="store",
                    default="",
                    help="")
parser.add_argument("--byte-code-only",
                    action="store_const",
                    const="1",
                    default="",
                    help="")
parser.add_argument("--skip-vendor",
                    action="store_const",
                    const="1",
                    default="",
                    help="")
parser.add_argument("--dry-run",
                    action="store_true",
                    help="Run without modifying anything")
parser.add_argument("--debug", "-d",
                    action="count",
                    default=0,
                    dest="debug",
                    help="Enable debug trace output")

options = parser.parse_args()


##############################################################################
#
#  Misc help functions
#
##############################################################################

# ----------------------------------------------------------------------
# Info/error/warning/debug functions
# ----------------------------------------------------------------------

# A a convention, start debug messages in uppercase. Except debug
# messages, start in lowercase unless a symbol like "OSError"

def print_splash(msg):
    print()
    print("########")
    print("######## WARNING: %s" % (msg))
    print("########")

def print_info(msg):
    if options.debug:
        print("DEBUG[%s]: %s" % (script_name, msg))
    else:
        print("INFO[%s]: %s" % (script_name, msg))

def print_warning(msg):
    print("WARNING[%s]: %s" % (script_name, msg))

def print_error(msg):
    print("ERROR[%s]: %s" % (script_name, msg))
    sys.exit(1)

def print_debug(msg, level = 1):
    if options.debug and level <= options.debug:
        print("DEBUG[%s]: %s" % (script_name, msg))

# ----------------------------------------------------------------------
# Read the "CHANGES.txt" file for entries
# ----------------------------------------------------------------------

def get_changes():
    """Get changes from CHANGES.txt."""
    log_lines = []
    found_version = False
    found_items = False
    with open("CHANGES.txt", "r") as fp:
        for line in fp.readlines():
            line = line.rstrip()
            if line.endswith(version_text_short):
                found_version = True
            if not line.strip() and found_items:
                break
            if found_version and line.startswith("- "):
                log_lines.append(" " * 2 + "* " + line[2:])
                found_items = True

    return log_lines

##############################################################################
#
# Basic verification and adjusting current directory
#
##############################################################################

if not os.path.isdir(options.source_directory[0]):
    print_error("argument needs to point to an unpacked source dist")

os.chdir(options.source_directory[0])
cwd = os.getcwd()

print_info("working in '%s'" % (cwd))

if not os.path.isdir(debian_support_dir):
    print_error("argument needs to point to an unpacked source dist")

##############################################################################
#
# Import modules from the source distribution
#
##############################################################################

# NOTE there is a similar "utils.py" in "cpydist" and "lib/mysql/connector"

sys.path.insert(0, os.path.join(cwd, "lib"))

from mysql.connector.version import EDITION, VERSION, VERSION_EXTRA # pylint: disable=C0413
from mysql.connector.utils import linux_distribution                # pylint: disable=C0413

version_text_short = "{0}.{1}.{2}".format(*VERSION[0:3])

##############################################################################
#
#  Initialize more version variables and Linux dist variables
#
##############################################################################

linux_dist = linux_distribution()

platform = linux_dist[0].lower()
if "debian" in platform:
    # For Debian we only use the first part of the version, Ubuntu two
    platform_version = linux_dist[1].split(".", 2)[0]
else:
    platform_version = ".".join(linux_dist[1].split(".", 2)[0:2])
sign = False
edition = EDITION
codename = linux_dist[2].lower()
version_extra = "-{0}".format(VERSION_EXTRA) \
    if VERSION_EXTRA else ""

# Get if commercial from the directory name
is_commercial = bool("commercial" in os.path.basename(cwd))

# Create the name for tarball according to Debian's policies

print("NAME", product_name)
print("LABEL", "-%s" % options.label if options.label else "")
print("VERSION", version_text_short)
print("VERSION_EXTRA", version_extra)
basename_tar = "%(name)s%(label)s-%(version)s%(version_extra)s-src" % {
    "name": product_name,
    "label": "-%s" % options.label if options.label else "",
    "version": version_text_short,
    "version_extra": version_extra
}

basename_orig_tar = "%(name)s%(label)s_%(version)s%(version_extra)s.orig" % {
    "name": product_name,
    "label": "-%s" % options.label if options.label else "",
    "version": version_text_short,
    "version_extra": version_extra
}

print_info("basename_tar       : %s" % (basename_tar))
print_info("basename_orig_tar  : %s" % (basename_orig_tar))
print_info("version_text_short : %s" % (version_text_short))
print_info("version_extra      : %s" % (version_extra))
print_info("label              : %s" % (options.label))
print_info("is_commercial      : %s" % (is_commercial))
print_info("platform           : %s" % (platform))
print_info("platform_version   : %s" % (platform_version))
print_info("sign               : %s" % (sign))
print_info("codename           : %s" % (codename))
print_info("changes            : ")
for change_line in get_changes():
    print("  " + change_line)

##############################################################################
#
#  Rename the TAR to conform Debian's Policy
#
##############################################################################

def rename_tar():

    # Here "orig" is not "original", but the TAR naming the Deb build needs
    # The TAR is assumed to be one level up, i.e. in PB2WORKDIR
    tarball = os.path.join(os.path.dirname(cwd), basename_tar + ".tar.gz")
    orig_tarball = os.path.join(os.path.dirname(cwd), basename_orig_tar + ".tar.gz")

    copyfile(tarball, orig_tarball)

##############################################################################
#
#  Function to pupulate the "debian" directory
#
##############################################################################

def populate_debian():
    """Copy and make files ready in the debian/ folder."""

    if os.path.isdir("debian"):
        rmtree("debian")
    copytree(debian_support_dir, "debian")

    # Update the version and log in the Debian changelog
    changelog_file = os.path.join("debian", "changelog")
    with open(changelog_file, "r") as fp:
        changelog = fp.readlines()
    print_info("changing changelog '%s' version and log" % (changelog_file))

    log_lines = get_changes()
    if not log_lines:
        print_error("failed reading change history from CHANGES.txt")

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
            if options.label:
                line = line.replace(
                    "mysql-connector-python",
                    "mysql-connector-python-%s" % (options.label))
            line = line.replace("UNRELEASED", codename)
            line = line.replace("-1",
                                "{version_extra}-1{platform}{version}"
                                .format(platform=platform,
                                        version=platform_version,
                                        version_extra=version_extra))
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

    control_file = os.path.join("debian", "control")
    if options.label:
        # Update the Source, Package and Conflicts fields
        # in control file, if label is present
        with open(control_file, "r") as fp:
            control = fp.readlines()

        print_info("changing control '%s' Source, Package and Conflicts fields" % (control_file))

        new_control = []
        add_label_regex = re.compile(r"^((?:Source|Package): mysql-connector-python)")
        remove_label_regex = re.compile("^(Conflicts: .*?)-%s" % (options.label))
        for line in control:
            line = line.rstrip()

            match = add_label_regex.match(line)
            if match:
                line = add_label_regex.sub(r"\1-%s" % (options.label), line)

            match = remove_label_regex.match(line)
            if match:
                line = remove_label_regex.sub(r"\1", line)

            new_control.append(line)

        with open(control_file, "w") as fp:
            fp.write("\n".join(new_control))

    copyright_file = os.path.join("debian", "copyright")
    print_info("reading license text from copyright file '%s'" % (copyright_file))
    with open(copyright_file, "r") as fp:
        # Skip to line just before the text we want to copy
        while True:
            line = fp.readline()
            if not line:
                break
            if line.startswith("License:"):
                # Read the rest of the text
                lic_text = fp.read()

    with open(control_file, "r") as fp:
        control_text = fp.read()

    print_info("updating license text in control file")
    new_control = re.sub(r"@LICENSE@", lic_text, control_text)
    with open(control_file, "w") as fp:
        fp.write(new_control)

##############################################################################
#
#  Function to create the package
#
##############################################################################

def make_dpkg():
    """Create Debian package in the source distribution folder."""

    cmd = ["dpkg-buildpackage", "-uc"]

    if not sign:
        cmd.append("-us")

    print_info("creating Debian package using '%s'" % (cmd))

    env = os.environ.copy()
    env["MYSQL_CAPI"]                    = options.with_mysql_capi
    env["OPENSSL_INCLUDE_DIR"]           = options.with_openssl_include_dir
    env["OPENSSL_LIB_DIR"]               = options.with_openssl_lib_dir
    env["MYSQLXPB_PROTOBUF_INCLUDE_DIR"] = options.with_protobuf_include_dir
    env["MYSQLXPB_PROTOBUF_LIB_DIR"]     = options.with_protobuf_lib_dir
    env["MYSQLXPB_PROTOC"]               = options.with_protoc
    env["WITH_CEXT"]                     = "1"
    env["EXTRA_COMPILE_ARGS"]            = options.extra_compile_args
    env["EXTRA_LINK_ARGS"]               = options.extra_link_args
    env["SKIP_VENDOR"]                   = options.skip_vendor
    env["LABEL"]                         = options.label or "0"
    env["BYTE_CODE_ONLY"]                = options.byte_code_only
    env["DH_VERBOSE"]                    = "1"

    success = True
    with Popen(cmd,
               stdout=PIPE,
               stderr=STDOUT,
               universal_newlines=True,
               env=env) as proc:
        stdout, stderr = proc.communicate()
        for line in stdout.split("\n"):
            if "error:" in line or "E: " in line:
                print_info(line)
                success = False
            elif options.debug:
                print_info(line)
            elif not re.search(no_debug_filter, line):
                print_info(line)

        if stderr:
            for line in stderr.split("\n"):
                if options.debug:
                    print_info(line)
                if "error:" in line or "E: " in line:
                    if not options.debug:
                        print_info(line)
                    success = False

    return success

##############################################################################
#
#  Well, now call the functions
#
##############################################################################

if not options.dry_run:
    rename_tar()
    populate_debian()
    make_dpkg()
