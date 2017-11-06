# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.

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

"""This module contains helper functions."""

def encode_to_bytes(value, encoding="utf-8"):
    """Returns an encoded version of the string as a bytes object.

    Args:
        encoding (str): The encoding.

    Resturns:
        bytes: The encoded version of the string as a bytes object.
    """
    return value if isinstance(value, bytes) else value.encode(encoding)


def decode_from_bytes(value, encoding="utf-8"):
    """Returns a string decoded from the given bytes.

    Args:
        value (bytes): The value to be decoded.
        encoding (str): The encoding.

    Returns:
        str: The value decoded from bytes.
    """
    return value.decode(encoding) if isinstance(value, bytes) else value


def get_item_or_attr(obj, key):
    """Get item from dictionary or attribute from object.

    Args:
        obj (object): Dictionary or object.
        key (str): Key.

    Returns:
        object: The object for the provided key.
    """
    return obj[key] if isinstance(obj, dict) else getattr(obj, key)
