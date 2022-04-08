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

"""Miscellaneous utility functions."""

import gzip
import logging
import os
import platform
import re
import shlex
import struct
import subprocess
import sys
import tarfile

from datetime import datetime
from distutils.dir_util import mkpath
from distutils.errors import DistutilsInternalError
from distutils.file_util import copy_file
from distutils.spawn import find_executable
from distutils.sysconfig import get_python_version
from distutils.version import LooseVersion
from subprocess import PIPE, Popen
from xml.dom.minidom import parse, parseString

try:
    from dateutil.tz import tzlocal

    NOW = datetime.now(tzlocal())
except ImportError:
    NOW = datetime.now()

try:
    from urllib.parse import parse_qsl
except ImportError:
    from urlparse import parse_qsl


ARCH = "64-bit" if sys.maxsize > 2**33 else "32-bit"
ARCH_64BIT = ARCH == "64-bit"
MYSQL_C_API_MIN_VERSION = (8, 0, 0)
LOGGER = logging.getLogger("cpydist")

# 64bit Conditional check, only includes VCPPREDIST2015 property
VC_RED_64 = (
    "<Product>"
    "<!-- Check Visual c++ Redistributable is Installed -->"
    '<Property Id="VS14REDIST">'
    '  <RegistrySearch Id="FindRedistVS14" Root="HKLM"'
    '   Key="SOFTWARE\\Microsoft\\DevDiv\\vc\\Servicing\\14.0\\RuntimeMinimum"'
    '   Name="Version" Type="raw" />'
    "</Property>"
    '<Condition Message="This application requires Visual Studio 2015'
    " Redistributable. Please install the Redistributable then run this"
    ' installer again.">'
    "  Installed OR VS14REDIST"
    "</Condition>"
    "</Product>"
)

# 64bit Conditional check, only install if OS is 64bit. Used in MSI-64
ONLY_64bit = (
    "<Product>"
    '<Condition Message="This version of the installer is only suitable to'
    ' run on 64 bit operating systems.">'
    "<![CDATA[Installed OR (VersionNT64 >=600)]]>"
    "</Condition>"
    "</Product>"
)


def _parse_mysql_info_line(line):
    """Parse a command line.

    This will never be perfect without special knowledge about all possible
    command lines "mysql_config" might output. But it should be close enough
    for our usage.
    """
    args = shlex.split(line)

    # Find out what kind of argument it is first,
    # if starts with "--", "-" or nothing
    pre_parsed_line = []
    for arg in args:
        re_obj = re.search(r"^(--|-|)(.*)", arg)
        pre_parsed_line.append(re_obj.group(1, 2))

    parsed_line = []

    while pre_parsed_line:
        (type1, opt1) = pre_parsed_line.pop(0)

        if "=" in opt1:
            # One of "--key=val", "-key=val" or "key=val"
            parsed_line.append(tuple(opt1.split("=", 1)))
        elif type1:
            # We have an option that might have a value
            # in the next element in the list
            if pre_parsed_line:
                (type2, opt2) = pre_parsed_line[0]
                if type2 == "" and "=" not in opt2:
                    # Value was in the next list element
                    parsed_line.append((opt1, opt2))
                    pre_parsed_line.pop(0)
                    continue
            if type1 == "--":
                # If "--" and no argument then it is an option like "--fast"
                parsed_line.append(opt1)
            else:
                # If "-" (and no "=" handled above) then it is a
                # traditional one character option name that might
                # have a value
                val = opt1[1:]
                if val:
                    parsed_line.append((opt1[:1], val))
                else:
                    parsed_line.append(opt1)
        else:
            LOGGER.warning("Could not handle '%s' in '%s'", opt1, line)

    return parsed_line


def _mysql_c_api_info_win(mysql_capi):
    """Get MySQL information without using mysql_config tool.

    Returns:
        dict: A dict containing the information about the last commit.
    """
    info = {}
    mysql_version_h = os.path.join(mysql_capi, "include", "mysql_version.h")

    if not os.path.exists(mysql_version_h):
        LOGGER.error("Invalid MySQL C API installation (mysql_version.h not found)")
        sys.exit(1)

    # Get MySQL version
    with open(mysql_version_h, "rb") as fp:
        for line in fp.readlines():
            if b"#define LIBMYSQL_VERSION" in line:
                version = LooseVersion(
                    line.split()[2].replace(b'"', b"").decode()
                ).version
                if tuple(version) < MYSQL_C_API_MIN_VERSION:
                    LOGGER.error(
                        "MySQL C API {} or later required"
                        "".format(MYSQL_C_API_MIN_VERSION)
                    )
                    sys.exit(1)
                break

    info["libraries"] = ["libmysql"]
    info["library_dirs"] = [os.path.join(mysql_capi, "lib")]
    info["include_dirs"] = [os.path.join(mysql_capi, "include")]

    # Get libmysql.dll arch
    connc_64bit = _win_dll_is64bit(os.path.join(mysql_capi, "lib", "libmysql.dll"))
    LOGGER.debug("connc_64bit: {0}".format(connc_64bit))
    info["arch"] = "x86_64" if connc_64bit else "i386"
    LOGGER.debug("# _mysql_c_api_info_win info: %s", info)

    return info


def mysql_c_api_info(mysql_config):
    """Get MySQL information using mysql_config tool.

    Returns:
        dict: Containing MySQL information about libraries.
    """
    if os.name == "nt":
        return _mysql_c_api_info_win(mysql_config)

    if os.path.isdir(mysql_config):
        mysql_config = os.path.join(mysql_config, "bin", "mysql_config")

    LOGGER.info("Getting MySQL information from %s", mysql_config)

    process = Popen([mysql_config], stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    if not stdout:
        raise ValueError(
            "Error executing command: {} ({})".format(mysql_config, stderr)
        )

    # Parse the output. Try to be future safe in case new options
    # are added. This might of course fail.
    info = {}

    for line in stdout.splitlines():
        re_obj = re.search(r"^\s+(?:--)?(\w+)\s+\[\s*(.*?)\s*\]", line.decode("utf-8"))
        if re_obj:
            mc_key = re_obj.group(1)
            mc_val = re_obj.group(2)

            # We always add the raw output from the different "mysql_config"
            # options. And in some cases, like "port", "socket", that is enough
            # for use from Python.
            info[mc_key] = mc_val
            LOGGER.debug("%s: %s", mc_key, mc_val)

            if not re.search(r"^-", mc_val) and "=" not in mc_val:
                # Not a Unix command line
                continue

            # In addition form useful information parsed from the
            # above command line
            parsed_line = _parse_mysql_info_line(mc_val)

            if mc_key == "include":
                # Lets assume all arguments are paths with "-I", "--include",..
                include_dirs = [val for _, val in parsed_line]
                info["include_dirs"] = include_dirs
                LOGGER.debug("include_dirs: %s", " ".join(include_dirs))
            elif mc_key == "libs_r":
                info["link_dirs"] = [
                    val
                    for key, val in parsed_line
                    if key
                    in (
                        "L",
                        "library-path",
                    )
                ]
                info["libraries"] = [
                    val
                    for key, val in parsed_line
                    if key
                    in (
                        "l",
                        "library",
                    )
                ]
                LOGGER.debug("link_dirs: %s", " ".join(info["link_dirs"]))
                LOGGER.debug("libraries: %s", " ".join(info["libraries"]))

    # Try to figure out the architecture
    info["arch"] = "x86_64" if sys.maxsize > 2**32 else "i386"
    # Return a tuple for version instead of a string
    info["version"] = tuple(
        [int(num) if num.isdigit() else num for num in info["version"].split(".")]
    )
    return info


def get_git_info():
    """Get Git information about the last commit.

    Returns:
        dict: A dict containing the information about the last commit.
    """
    is_git_repo = False
    if find_executable("git") is not None:
        # Check if it's a Git repository
        proc = Popen(["git", "--no-pager", "branch"], universal_newlines=True)
        proc.communicate()
        is_git_repo = proc.returncode == 0

    if is_git_repo:
        cmd = [
            "git",
            "log",
            "-n",
            "1",
            "--date=iso",
            "--pretty=format:'branch=%D&date=%ad&commit=%H&short=%h'",
        ]
        proc = Popen(cmd, stdout=PIPE, universal_newlines=True)
        stdout, _ = proc.communicate()
        git_info = dict(
            parse_qsl(
                stdout.replace("'", "").replace("+", "%2B").split(",")[-1:][0].strip()
            )
        )
        try:
            git_info["branch"] = stdout.split(",")[0].split("->")[1].strip()
        except IndexError:
            git_info["branch"] = stdout.split(",")[0].split("=")[1].strip()
        return git_info

    branch_src = os.getenv("BRANCH_SOURCE")
    push_rev = os.getenv("PUSH_REVISION")
    if branch_src and push_rev:
        git_info = {
            "branch": branch_src.split()[-1],
            "date": None,
            "commit": push_rev,
            "short": push_rev[:7],
        }
        return git_info
    return None


def write_info_src(version):
    """Generate docs/INFO_SRC.

    Returns:
        bool: ``True`` if `docs/INFO_SRC` was written successfully.
    """
    git_info = get_git_info()
    if git_info:
        with open(os.path.join("docs", "INFO_SRC"), "w") as info_src:
            info_src.write("version: {}\n".format(version))
            if git_info:
                info_src.write("branch: {}\n".format(git_info["branch"]))
                if git_info.get("date"):
                    info_src.write("date: {}\n".format(git_info["date"]))
                info_src.write("commit: {}\n".format(git_info["commit"]))
                info_src.write("short: {}\n".format(git_info["short"]))
        return True
    return False


def write_info_bin(mysql_version=None, compiler=None):
    """Generate docs/INFO_BIN.

    Args:
        mysql_version (Optional[str]): The MySQL version.

    Returns:
        bool: ``True`` if `docs/INFO_BIN` was written successfully.
    """
    now = NOW.strftime("%Y-%m-%d %H:%M:%S %z")
    with open(os.path.join("docs", "INFO_BIN"), "w") as info_bin:
        info_bin.write("build-date: {}\n".format(now))
        info_bin.write("os-info: {}\n".format(platform.platform()))
        if mysql_version:
            info_bin.write("mysql-version: {}\n".format(mysql_version))
        if compiler:
            info_bin.write("compiler: {}\n".format(compiler))


def _parse_release_file(release_file):
    """Parse the contents of /etc/lsb-release or /etc/os-release file.

    Returns:
        A dictionary containing release information.
    """
    distro = {}
    if os.path.exists(release_file):
        with open(release_file) as file_obj:
            for line in file_obj:
                key_value = line.split("=")
                if len(key_value) != 2:
                    continue
                key = key_value[0].lower()
                value = key_value[1].rstrip("\n").strip('"')
                distro[key] = value
    return distro


def _parse_lsb_release_command():
    """Parse the output of the lsb_release command.

    Returns:
        A dictionary containing release information.
    """
    distro = {}
    with open(os.devnull, "w") as devnull:
        try:
            stdout = subprocess.check_output(("lsb_release", "-a"), stderr=devnull)
        except OSError:
            return None
        lines = stdout.decode(sys.getfilesystemencoding()).splitlines()
        for line in lines:
            key_value = line.split(":")
            if len(key_value) != 2:
                continue
            key = key_value[0].replace(" ", "_").lower()
            value = key_value[1].strip("\t")
            distro[key] = value
    return distro


def linux_distribution():
    """Try to determine the name of the Linux OS distribution name.

    First try to get information from ``/etc/lsb-release`` file.
    If it fails, try to get the information of ``lsb-release`` command.
    And finally the information of ``/etc/os-release`` file.

    Returns:
        tuple: A tuple with (`name`, `version`, `codename`)
    """
    distro = _parse_release_file(os.path.join("/etc", "lsb-release"))
    if distro:
        return (
            distro.get("distrib_id", ""),
            distro.get("distrib_release", ""),
            distro.get("distrib_codename", ""),
        )

    distro = _parse_lsb_release_command()
    if distro:
        return (
            distro.get("distributor_id", ""),
            distro.get("release", ""),
            distro.get("codename", ""),
        )

    distro = _parse_release_file(os.path.join("/etc", "os-release"))
    if distro:
        return (
            distro.get("name", ""),
            distro.get("version_id", ""),
            distro.get("version_codename", ""),
        )

    return ("", "", "")


def get_dist_name(
    distribution,
    source_only_dist=False,
    platname=None,
    python_version=None,
    label="",
    edition="",
):
    """Get the distribution name.

    Get the distribution name usually used for creating the egg file. The
    Python version is excluded from the name when source_only_dist is True.
    The platname will be added when it is given at the end.

    Returns:
        str: The distribution name.
    """
    name = [distribution.metadata.name]
    if edition:
        name.append(edition)
    if label:
        name.append("-{}".format(label))
    name.append("-{}".format(distribution.metadata.version))
    if not source_only_dist or python_version:
        pyver = python_version or get_python_version()
        name.append("-py{}".format(pyver))
    if platname:
        name.append("-{}".format(platname))
    return "".join(name)


def get_magic_tag():
    """Return the magic tag for .pyc files."""
    return sys.implementation.cache_tag


def unarchive_targz(tarball):
    """Unarchive a tarball.

    Unarchives the given tarball. If the tarball has the extension
    '.gz', it will be first uncompressed.

    Returns the path to the folder of the first unarchived member.

    Returns str.
    """
    orig_wd = os.getcwd()

    (dstdir, tarball_name) = os.path.split(tarball)
    if dstdir:
        os.chdir(dstdir)

    if ".gz" in tarball_name:
        new_file = tarball_name.replace(".gz", "")
        gz = gzip.GzipFile(tarball_name)
        tar = open(new_file, "wb")
        tar.write(gz.read())
        tar.close()
        tarball_name = new_file

    tar = tarfile.TarFile(tarball_name)
    tar.extractall()

    os.unlink(tarball_name)
    os.chdir(orig_wd)

    return os.path.abspath(os.path.join(dstdir, tar.getmembers()[0].name))


def add_docs(doc_path, doc_files=None):
    """Prepare documentation files for Connector/Python."""
    mkpath(doc_path)

    if not doc_files:
        doc_files = [
            "mysql-connector-python.pdf",
            "mysql-connector-python.html",
            "mysql-html.css",
        ]
    for file_name in doc_files:
        # Check if we have file in docs/
        doc_file = os.path.join("docs", file_name)
        if not os.path.exists(doc_file):
            # it might be in build/
            doc_file = os.path.join("build", file_name)
            if not os.path.exists(doc_file):
                # we do not have it, create a fake one
                LOGGER.warning(
                    "documentation '%s' does not exist; creating empty",
                    doc_file,
                )
                open(doc_file, "w").close()

        if not os.path.exists(doc_file):
            # don't copy yourself
            copy_file(doc_file, doc_path)


# Windows MSI descriptor parser
# Customization utility functions for the C/py product msi descriptor


def _win_dll_is64bit(dll_file):
    """Check if a Windows DLL is 64 bit or not.

    Raises:
        ValueError: When magic of header is invalid.
        IOError: When file could not be read.
        OSError: when execute on none-Windows platform.

    Returns:
        bool: True if is a 64 bit library.
    """
    if os.name != "nt":
        raise OSError("win_ddl_is64bit only useful on Windows")

    with open(dll_file, "rb") as fp:
        # IMAGE_DOS_HEADER
        e_magic = fp.read(2)
        if e_magic != b"MZ":
            raise ValueError("Wrong magic in header")

        fp.seek(60)
        offset = struct.unpack("I", fp.read(4))[0]

        # IMAGE_FILE_HEADER
        fp.seek(offset)
        file_header = fp.read(6)
        (_, machine) = struct.unpack("<4sH", file_header)
        if machine == 0x014C:  # IMAGE_FILE_MACHINE_I386
            return False
        elif machine in (0x8664, 0x2000):  # IMAGE_FILE_MACHINE_I386/AMD64
            return True


def _append_child_from_unparsed_xml(father_node, unparsed_xml):
    """Append child xml nodes to a node."""
    dom_tree = parseString(unparsed_xml)
    if dom_tree.hasChildNodes():
        first_child = dom_tree.childNodes[0]
        if first_child.hasChildNodes():
            child_nodes = first_child.childNodes
            for _ in range(len(child_nodes)):
                childNode = child_nodes.item(0)
                father_node.appendChild(childNode)
            return

    raise DistutilsInternalError(
        "Could not Append append elements to the Windows msi descriptor."
    )


def _get_element(dom_msi, tag_name, name=None, id_=None):
    """Get a xml element defined on Product."""
    product = dom_msi.getElementsByTagName("Product")[0]
    elements = product.getElementsByTagName(tag_name)
    for element in elements:
        if name and id_:
            if (
                element.getAttribute("Name") == name
                and element.getAttribute("Id") == id_
            ):
                return element
        elif id_:
            if element.getAttribute("Id") == id_:
                return element


def _add_64bit_elements(dom_msi, log, add_vs_redist=True):
    """Add the properties and conditions elements to the xml msi descriptor."""
    # Get the Product xml element
    product = dom_msi.getElementsByTagName("Product")[0]
    # Append children
    if add_vs_redist:
        LOGGER.info("Adding vc_red_64 element")
        _append_child_from_unparsed_xml(product, VC_RED_64)
    LOGGER.info("Adding only_64bit element")
    _append_child_from_unparsed_xml(product, ONLY_64bit)


def add_arch_dep_elems(xml_path, result_path, for32=False, add_vs_redist=True):
    """Add the architecture dependent properties and conditions.

    Args:
        xml_path (str): The original xml msi descriptor path.
        result_path (str): Path to save the resulting xml.
        add_vs_redist (bool): Add the VS redistributable requirement.
    """
    LOGGER.info("Adding arch_dep_elems xml to:%s", xml_path)
    dom_msi = parse(xml_path)
    if for32:
        LOGGER.info("No elements to add for 32bit msi")
    else:
        LOGGER.info("Adding 64bit elements")
        _add_64bit_elements(dom_msi, add_vs_redist)

    LOGGER.info("Saving xml to:%s working directory:%s", result_path, os.getcwd())
    with open(result_path, "w+") as fp:
        fp.write(dom_msi.toprettyxml())
        fp.flush()
        fp.close()
