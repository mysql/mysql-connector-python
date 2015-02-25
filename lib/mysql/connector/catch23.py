# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2014, Oracle and/or its affiliates. All rights reserved.

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

"""Python v2 to v3 migration module"""

from decimal import Decimal
import struct
import sys

from .custom_types import HexLiteral

# pylint: disable=E0602,E1103

PY2 = sys.version_info[0] == 2

if PY2:
    NUMERIC_TYPES = (int, float, Decimal, HexLiteral, long)
    INT_TYPES = (int, long)
    UNICODE_TYPES = (unicode,)
    STRING_TYPES = (str, unicode)
    BYTE_TYPES = (bytearray,)
else:
    NUMERIC_TYPES = (int, float, Decimal, HexLiteral)
    INT_TYPES = (int,)
    UNICODE_TYPES = (str,)
    STRING_TYPES = (str,)
    BYTE_TYPES = (bytearray, bytes)


def init_bytearray(payload=b'', encoding='utf-8'):
    """Initializes a bytearray from the payload"""
    if isinstance(payload, bytearray):
        return payload

    if PY2:
        return bytearray(payload)

    if isinstance(payload, int):
        return bytearray(payload)
    elif not isinstance(payload, bytes):
        try:
            return bytearray(payload.encode(encoding=encoding))
        except AttributeError:
            raise ValueError("payload must be a str or bytes")


    return bytearray(payload)


def isstr(obj):
    """Returns whether a variable is a string"""
    if PY2:
        return isinstance(obj, basestring)
    else:
        return isinstance(obj, str)

def isunicode(obj):
    """Returns whether a variable is a of unicode type"""
    if PY2:
        return isinstance(obj, unicode)
    else:
        return isinstance(obj, str)


if PY2:
    def struct_unpack(fmt, buf):
        """Wrapper around struct.unpack handling buffer as bytes and strings"""
        if isinstance(buf, (bytearray, bytes)):
            return struct.unpack_from(fmt, buffer(buf))
        return struct.unpack_from(fmt, buf)
else:
    struct_unpack = struct.unpack  # pylint: disable=C0103


def make_abc(base_class):
    """Decorator used to create a abstract base class

    We use this decorator to create abstract base classes instead of
    using the abc-module. The decorator makes it possible to do the
    same in both Python v2 and v3 code.
    """
    def wrapper(class_):
        """Wrapper"""
        attrs = class_.__dict__.copy()
        for attr in '__dict__', '__weakref__':
            attrs.pop(attr, None)  # ignore missing attributes

        bases = class_.__bases__
        if PY2:
            attrs['__metaclass__'] = class_
        else:
            bases = (class_,) + bases
        return base_class(class_.__name__, bases, attrs)
    return wrapper
