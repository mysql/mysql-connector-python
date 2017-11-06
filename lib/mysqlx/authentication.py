# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2016, 2017, Oracle and/or its affiliates. All rights reserved.

# MySQL Connector/Python is licensed under the terms of the GPLv2
# <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most
# MySQL Connectors. There are special exceptions to the terms and
# conditions of the GPLv2 as it is applied to this software, see the
# FOSS License Exception
# <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA

"""Implementation of MySQL Authentication Plugin."""

import hashlib
import struct

from .compat import PY3, UNICODE_TYPES, hexlify


class MySQL41AuthPlugin(object):
    """Class implementing the MySQL Native Password authentication plugin."""
    def __init__(self, username, password):
        self._username = username
        self._password = password.encode("utf-8") \
            if isinstance(password, UNICODE_TYPES) else password

    def name(self):
        """Returns the plugin name.

        Returns:
            str: The plugin name.
        """
        return "MySQL 4.1 Authentication Plugin"

    def auth_name(self):
        """Returns the authentication name.

        Returns:
            str: The authentication name.
        """
        return "MYSQL41"

    def xor_string(self, hash1, hash2):
        """Encrypt/Decrypt function used for password encryption in
        authentication, using a simple XOR.

        Args:
            hash1 (str): The first hash.
            hash2 (str): The second hash.

        Returns:
            str: A string with the xor applied.
        """
        if PY3:
            xored = [h1 ^ h2 for (h1, h2) in zip(hash1, hash2)]
        else:
            xored = [ord(h1) ^ ord(h2) for (h1, h2) in zip(hash1, hash2)]
        return struct.pack("20B", *xored)

    def build_authentication_response(self, data):
        """Hashing for MySQL 4.1 authentication.

        Args:
            data (str): The authentication data.

        Returns:
            str: The authentication response.
        """
        if self._password:
            hash1 = hashlib.sha1(self._password).digest()
            hash2 = hashlib.sha1(hash1).digest()
            auth_response = self.xor_string(
                hash1, hashlib.sha1(data + hash2).digest())
            return "{0}\0{1}\0*{2}\0".format("", self._username,
                                             hexlify(auth_response))
        return "{0}\0{1}\0".format("", self._username)


class PlainAuthPlugin(object):
    """Class implementing the MySQL Plain authentication plugin."""
    def __init__(self, username, password):
        self._username = username
        self._password = password.encode("utf-8") \
            if isinstance(password, UNICODE_TYPES) and not PY3 else password

    def name(self):
        """Returns the plugin name.

        Returns:
            str: The plugin name.
        """
        return "Plain Authentication Plugin"

    def auth_name(self):
        """Returns the authentication name.

        Returns:
            str: The authentication name.
        """
        return "PLAIN"

    def auth_data(self):
        """Returns the authentication data.

        Returns:
            str: The authentication data.
        """
        return "\0{0}\0{1}".format(self._username, self._password)


class ExternalAuthPlugin(object):
    """Class implementing the External authentication plugin."""
    def name(self):
        """Returns the plugin name.

        Returns:
            str: The plugin name.
        """
        return "External Authentication Plugin"

    def auth_name(self):
        """Returns the authentication name.

        Returns:
            str: The authentication name.
        """
        return "EXTERNAL"

    def initial_response(self):
        """Returns the initial response.

        Returns:
            str: The initial response.
        """
        return ""
