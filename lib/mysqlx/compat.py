# Copyright (c) 2016, 2017, Oracle and/or its affiliates. All rights reserved.
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

"""This module handles compatibility issues between Python 2 and Python 3."""

import sys
import decimal
import binascii


PY3 = sys.version_info[0] == 3


# pylint: disable=E0401,E0602,E0611,W0611,
if PY3:
    from urllib.parse import urlparse, unquote, parse_qsl

    def hexlify(data):
        """Return the hexadecimal representation of the binary data.

        Args:
            data (str): The binary data.

        Returns:
            bytes: The hexadecimal representation of data.
        """
        return binascii.hexlify(data).decode("utf-8")

    NUMERIC_TYPES = (int, float, decimal.Decimal,)
    INT_TYPES = (int,)
    UNICODE_TYPES = (str,)
    STRING_TYPES = (str,)
    BYTE_TYPES = (bytearray, bytes,)


else:
    from urlparse import urlparse, unquote, parse_qsl

    def hexlify(data):
        """Return the hexadecimal representation of the binary data.

        Args:
            data (str): The binary data.

        Returns:
            bytes: The hexadecimal representation of data.
        """
        return data.encode("hex")

    NUMERIC_TYPES = (int, float, decimal.Decimal, long,)
    INT_TYPES = (int, long,)
    UNICODE_TYPES = (unicode,)
    STRING_TYPES = (str, unicode,)
    BYTE_TYPES = (bytearray,)
