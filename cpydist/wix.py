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

import os
import re
import subprocess

from distutils import log
from distutils.errors import DistutilsError

WIX_INSTALL_PATH = r"C:\Program Files (x86)\Windows Installer XML v3.5"
WIX_REQUIRED_VERSION = "3.5"


def find_candle_bindir(wix_install_path):
    """Find candle.

    There are two different types of WiX installs with different
    directory layout (MSI or ZIP install), one "normal" with "bin"
    directory, one "flat" with executables directly in the install
    directory.
    """
    candle = os.path.join(wix_install_path, "bin/candle.exe")
    if os.path.isfile(candle) and os.access(candle, os.X_OK):
        return (candle, os.path.join(wix_install_path, "bin"))

    candle = os.path.join(wix_install_path, "candle.exe")
    if os.path.isfile(candle) and os.access(candle, os.X_OK):
        return (candle, wix_install_path)

    raise DistutilsError(
        "Could not find candle.exe under %s" % wix_install_path
    )


def check_wix_install(
    wix_install_path=WIX_INSTALL_PATH,
    wix_required_version=WIX_REQUIRED_VERSION,
    dry_run=0,
):
    """Check the WiX installation.

    Check whether the WiX tools are available in given wix_install_path
    and also check the wix_required_version.

    Raises DistutilsError when the tools are not available or
    when the version is not correct.
    """
    if dry_run:
        return  # FIXME why not check things if dry run?

    (candle, _bindir) = find_candle_bindir(wix_install_path)

    cmd = [candle, "-?"]

    proc = subprocess.Popen(
        " ".join(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    data = proc.communicate()[0]
    try:
        verline = data.split("\n")[0].strip()
    except TypeError:
        # Must be Python v3
        verline = data.decode("utf8").split("\n")[0].strip()

    # Lines like "Windows Installer XML Toolset Compiler version 3.10.1.2213"
    m = re.search(r"version\s+(?P<major>\d+)\.(?P<minor>\d+)", verline)
    if not m:
        raise DistutilsError(
            "Can't parse version output from candle.exe: {}" "".format(verline)
        )
    wix_version = [int(m.group("major")), int(m.group("minor"))]
    wix_min_version = [int(i) for i in wix_required_version.split(".")[:2]]
    if wix_version[0] < wix_min_version[0] or (
        wix_version[0] == wix_min_version[0]
        and wix_version[1] < wix_min_version[1]
    ):
        raise DistutilsError(
            "Minimal WiX v{}, we found v{}"
            "".format(".".join(wix_min_version), ".".join(wix_version))
        )


class WiX:
    """Class for creating a Windows Installer using WiX."""

    def __init__(
        self,
        wxs,
        out=None,
        msi_out=None,
        base_path=None,
        install=WIX_INSTALL_PATH,
    ):
        """Constructor.

        The Windows Installer will be created using the WiX document
        wxs. The msi_out argument can be used to set the name of the
        resulting Windows Installer (.msi file).
        The argument install can be used to point to the WiX installation.
        The default location is:
            '%s'
        Temporary and other files needed to create the Windows Installer
        will be by default created in the current working dir. You can
        change this using the base_path argument.
        """ % (
            WIX_INSTALL_PATH
        )
        if out:
            self.set_out(out)
        self._msi_out = msi_out
        self._wxs = wxs
        self._install = install
        self._base_path = base_path
        if self._install:
            (_candle, bindir) = find_candle_bindir(self._install)
            self._bin = bindir
        else:
            self._bin = None
        self._parameters = None

    def set_parameters(self, parameters):
        """Set parameters to use in the WXS document(s)."""
        self._parameters = parameters

    def set_out(self, out):
        """Set the name of the resulting Windows Installer."""
        self._out = out

    def _run_tool(self, cmdname, cmdargs):
        """Run a WiX tool.

        Run the given command with arguments.

        Raises DistutilsError on errors.
        """
        cmd = [os.path.join(self._bin, cmdname)]
        cmd += cmdargs

        log.info("Running: %s", " ".join(cmd))
        prc = subprocess.Popen(
            " ".join(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdoutdata = prc.communicate()[0]

        for line in stdoutdata.splitlines():
            try:
                if "warning" in line:
                    log.info(line)
                elif "error" in line:
                    raise DistutilsError("WiX Error: " + line)
            except TypeError:
                if b"warning" in line:
                    log.info(line)
                elif b"error" in line:
                    raise DistutilsError("WiX Error: " + line.decode("utf8"))
            except DistutilsError:
                raise

        if prc.returncode:
            raise DistutilsError(
                "%s exited with return code %d" % (cmdname, prc.returncode)
            )

    def compile(self, wxs=None, out=None, parameters=None):
        wxs = wxs or self._wxs
        log.info("WiX: Compiling %s" % wxs)
        out = out or self._out

        cmdargs = [
            r"-nologo",
            r"-v",
            r"-ext WixUIExtension",
            wxs,
            r"cpydist\data\msi\PY37.wxs",
            r"cpydist\data\msi\PY38.wxs",
            r"cpydist\data\msi\PY39.wxs",
            r"cpydist\data\msi\PY310.wxs",
            r"cpydist\data\msi\cpy_msi_gui.wxs",
        ]
        if parameters:
            params = dict(self._parameters.items() + parameters.items())
        else:
            params = self._parameters

        for parameter, value in params.items():
            cmdargs.append("-d%s=%s" % (parameter, value))

        self._run_tool("candle.exe", cmdargs)

    def link(self, wixobj=None, base_path=None, data_path=None):
        wixobj = wixobj or self._out
        base_path = base_path or self._base_path
        cwd = os.getcwd()
        if data_path is None:
            data_path = cwd
        msi_out = self._msi_out or wixobj.replace(".wixobj", ".msi")
        log.info("WiX: Linking %s" % wixobj)

        # light.exe -b option does not seem to work, we change to buld dir
        print("cwd: {}".format(cwd))
        os.chdir(base_path)
        print("base_path: {}".format(base_path))
        wxlfile = os.path.join(
            data_path, "cpydist\\data\\msi\\WixUI_en-us.wxl"
        )
        print("wxlfile loc file: {}".format(wxlfile))

        cmdargs = [
            r"-loc {0}".format(wxlfile),
            r"-ext WixUIExtension",
            r"-cultures:en-us",
            r"-nologo",
            r"-sw1076",
            r"-out %s" % msi_out,
            r"{}\cpy_product_desc.wixobj".format(data_path),
            r"{}\cpy_msi_gui.wixobj".format(data_path),
            r"{}\PY37.wixobj".format(data_path),
            r"{}\PY38.wixobj".format(data_path),
            r"{}\PY39.wixobj".format(data_path),
            r"{}\PY310.wixobj".format(data_path),
        ]

        self._run_tool("light.exe", cmdargs)
        os.chdir(cwd)
