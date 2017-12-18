# Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.
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
