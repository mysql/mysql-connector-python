# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

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

import hashlib


class MySQL41AuthPlugin(object):
    def __init__(self, username, password):
        self._username = username
        self._password = password

    def name(self):
        return "MySQL 4.1 Authentication Plugin"

    def auth_name(self):
        return "MYSQL41"

    def xor_string(self, a, b):
        """Encrypt/Decrypt function used for password encryption in
        authentication, using a simple XOR.
        """
        return "".join([chr(ord(x) ^ ord(y)) for x, y in zip(a, b)])

    def build_authentication_response(self, data):
        """Hashing for MySQL 4.1 authentication
        """
        if self._password:
            h1 = hashlib.sha1(self._password).digest()
            h2 = hashlib.sha1(h1).digest()
            auth_response = self.xor_string(
                h1, hashlib.sha1(data + h2).digest()).encode("hex")
            return "{0}\0{1}\0*{2}\0".format("", self._username, auth_response)
        else:
            return "{0}\0{1}\0".format("", self._username)
