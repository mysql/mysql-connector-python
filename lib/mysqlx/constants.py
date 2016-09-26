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

"""Constants."""

from collections import namedtuple


def create_enum(name, fields, values=None):
    """Emulates an enum by creating a namedtuple.

    Args:
        name (string): The type name.
        fields (tuple): The fields names.
        values (tuple): The values of the fields.

    Returns:
        namedtuple: A namedtuple object.
    """
    Enum = namedtuple(name, fields, verbose=False)
    if values is None:
        return Enum(*fields)
    return Enum(*values)


Algorithms = create_enum("Algorithms", ("MERGE", "TMPTABLE", "UNDEFINED"))
Securities = create_enum("Securities", ("DEFINER", "INVOKER"))
CheckOptions = create_enum("CheckOptions", ("CASCADED", "LOCAL"))


__all__ = ["Algorithms", "Securities", "CheckOptions"]
