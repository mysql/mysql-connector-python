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

"""Caching SHA2 Password Authentication Plugin."""

import struct

from hashlib import sha256

from .. import errors
from . import BaseAuthPlugin

AUTHENTICATION_PLUGIN_CLASS = "MySQLCachingSHA2PasswordAuthPlugin"


class MySQLCachingSHA2PasswordAuthPlugin(BaseAuthPlugin):
    """Class implementing the MySQL caching_sha2_password authentication plugin

    Note that encrypting using RSA is not supported since the Python
    Standard Library does not provide this OpenSSL functionality.
    """

    requires_ssl = False
    plugin_name = "caching_sha2_password"
    perform_full_authentication = 4
    fast_auth_success = 3

    def _scramble(self):
        """Return a scramble of the password using a Nonce sent by the
        server.

        The scramble is of the form:
        XOR(SHA2(password), SHA2(SHA2(SHA2(password)), Nonce))
        """
        if not self._auth_data:
            raise errors.InterfaceError("Missing authentication data (seed)")

        if not self._password:
            return b""

        password = (
            self._password.encode("utf-8")
            if isinstance(self._password, str)
            else self._password
        )
        auth_data = self._auth_data

        hash1 = sha256(password).digest()
        hash2 = sha256()
        hash2.update(sha256(hash1).digest())
        hash2.update(auth_data)
        hash2 = hash2.digest()
        xored = [h1 ^ h2 for (h1, h2) in zip(hash1, hash2)]
        hash3 = struct.pack("32B", *xored)
        return hash3

    def _full_authentication(self):
        """Returns password as as clear text"""
        if not self._ssl_enabled:
            raise errors.InterfaceError(f"{self.plugin_name} requires SSL")
        return super().prepare_password()

    def prepare_password(self):
        """Prepare and return password.

        Returns:
            bytes: Prepared password.
        """
        if len(self._auth_data) > 1:
            return self._scramble()
        if self._auth_data[0] == self.perform_full_authentication:
            return self._full_authentication()
        return None
