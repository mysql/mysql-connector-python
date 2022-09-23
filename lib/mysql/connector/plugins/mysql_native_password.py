# Copyright (c) 2022, Oracle and/or its affiliates.
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

"""Native Password Authentication Plugin."""

import logging
import struct

from hashlib import sha1

from .. import errors
from ..types import StrOrBytes
from . import BaseAuthPlugin

logging.getLogger(__name__).addHandler(logging.NullHandler())

_LOGGER = logging.getLogger(__name__)

AUTHENTICATION_PLUGIN_CLASS = "MySQLNativePasswordAuthPlugin"


class MySQLNativePasswordAuthPlugin(BaseAuthPlugin):
    """Class implementing the MySQL Native Password authentication plugin"""

    requires_ssl: bool = False
    plugin_name: str = "mysql_native_password"

    def prepare_password(self) -> bytes:
        """Prepares and returns password as native MySQL 4.1+ password"""
        if not self._auth_data:
            raise errors.InterfaceError("Missing authentication data (seed)")

        if not self._password:
            return b""
        password: StrOrBytes = self._password

        if isinstance(self._password, str):
            password = self._password.encode("utf-8")
        else:
            password = self._password

        auth_data = self._auth_data

        hash4 = None
        try:
            hash1 = sha1(password).digest()
            hash2 = sha1(hash1).digest()
            hash3 = sha1(auth_data + hash2).digest()
            xored = [h1 ^ h3 for (h1, h3) in zip(hash1, hash3)]
            hash4 = struct.pack("20B", *xored)
        except (struct.error, TypeError) as err:
            raise errors.InterfaceError(f"Failed scrambling password; {err}") from err

        return hash4
