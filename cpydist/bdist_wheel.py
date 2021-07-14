# Copyright (c) 2020, 2021, Oracle and/or its affiliates.
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

"""Implements the Distutils command for creating Wheel packages."""

from wheel.bdist_wheel import bdist_wheel

from . import BaseCommand


class DistWheel(bdist_wheel, BaseCommand):
    """Create a Wheel distribution."""

    user_options = bdist_wheel.user_options + BaseCommand.user_options + [
        ("metadata-license=", None, "metadata license text"),
    ]
    metadata_license = ""

    def initialize_options(self):
        """Initialize the options."""
        bdist_wheel.initialize_options(self)
        BaseCommand.initialize_options(self)

    def finalize_options(self):
        """Finalize the options."""
        bdist_wheel.finalize_options(self)
        BaseCommand.finalize_options(self)
        if self.universal:
            self.root_is_pure = True

    def run(self):
        """Run the command."""
        if self.metadata_license:
            self.distribution.metadata.license = self.metadata_license
        bdist_wheel.run(self)
        self.remove_temp()
