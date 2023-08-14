# Copyright (c) 2016, 2022, Oracle and/or its affiliates.
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

"""Implementation of MySQL Authentication Plugin."""

import hashlib
import struct

from typing import Optional

from .helpers import hexlify


def xor_string(hash1: bytes, hash2: bytes, hash_size: int) -> bytes:
    """Encrypt/Decrypt function used for password encryption in
    authentication, using a simple XOR.

    Args:
        hash1 (str): The first hash.
        hash2 (str): The second hash.

    Returns:
        str: A string with the xor applied.
    """
    xored = [h1 ^ h2 for (h1, h2) in zip(hash1, hash2)]
    return struct.pack(f"{hash_size}B", *xored)


class BaseAuthPlugin:
    """Base class for implementing the authentication plugins."""

    def __init__(self, username: Optional[str] = None, password: Optional[str] = None):
        self._username: Optional[str] = username
        self._password: Optional[str] = password

    def name(self) -> str:
        """Returns the plugin name.

        Returns:
            str: The plugin name.
        """
        raise NotImplementedError

    def auth_name(self) -> str:
        """Returns the authentication name.

        Returns:
            str: The authentication name.
        """
        raise NotImplementedError


class MySQL41AuthPlugin(BaseAuthPlugin):
    """Class implementing the MySQL Native Password authentication plugin."""

    def name(self) -> str:
        """Returns the plugin name.

        Returns:
            str: The plugin name.
        """
        return "MySQL 4.1 Authentication Plugin"

    def auth_name(self) -> str:
        """Returns the authentication name.

        Returns:
            str: The authentication name.
        """
        return "MYSQL41"

    def auth_data(self, data: bytes) -> str:
        """Hashing for MySQL 4.1 authentication.

        Args:
            data (bytes): The authentication data.

        Returns:
            str: The authentication response.
        """
        if self._password:
            password = (
                self._password.encode("utf-8")
                if isinstance(self._password, str)
                else self._password
            )
            hash1 = hashlib.sha1(password).digest()
            hash2 = hashlib.sha1(hash1).digest()
            xored = xor_string(hash1, hashlib.sha1(data + hash2).digest(), 20)
            return f"\0{self._username}\0*{hexlify(xored)}\0"
        return f"\0{self._username}\0"


class PlainAuthPlugin(BaseAuthPlugin):
    """Class implementing the MySQL Plain authentication plugin."""

    def name(self) -> str:
        """Returns the plugin name.

        Returns:
            str: The plugin name.
        """
        return "Plain Authentication Plugin"

    def auth_name(self) -> str:
        """Returns the authentication name.

        Returns:
            str: The authentication name.
        """
        return "PLAIN"

    def auth_data(self) -> str:
        """Returns the authentication data.

        Returns:
            str: The authentication data.
        """
        return f"\0{self._username}\0{self._password}"


class Sha256MemoryAuthPlugin(BaseAuthPlugin):
    """Class implementing the SHA256_MEMORY authentication plugin."""

    def name(self) -> str:
        """Returns the plugin name.

        Returns:
            str: The plugin name.
        """
        return "SHA256_MEMORY Authentication Plugin"

    def auth_name(self) -> str:
        """Returns the authentication name.

        Returns:
            str: The authentication name.
        """
        return "SHA256_MEMORY"

    def auth_data(self, data: bytes) -> str:
        """Hashing for SHA256_MEMORY authentication.

        The scramble is of the form:
            SHA256(SHA256(SHA256(PASSWORD)),NONCE) XOR SHA256(PASSWORD)

        Args:
            data (bytes): The authentication data.

        Returns:
            str: The authentication response.
        """
        password = (
            self._password.encode("utf-8")
            if isinstance(self._password, str)
            else self._password
        )
        hash1 = hashlib.sha256(password).digest()
        hash2 = hashlib.sha256(hashlib.sha256(hash1).digest() + data).digest()
        xored = xor_string(hash2, hash1, 32)
        return f"\0{self._username}\0{hexlify(xored)}"
