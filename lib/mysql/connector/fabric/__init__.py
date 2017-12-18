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

"""MySQL Fabric support"""


from collections import namedtuple

from .connection import (
    MODE_READONLY, MODE_READWRITE,
    STATUS_PRIMARY, STATUS_SECONDARY,
    SCOPE_GLOBAL, SCOPE_LOCAL,
    Fabric, FabricConnection,
    MySQLFabricConnection,
    FabricSet,
)

# Order of field_names must match how Fabric is returning the data
FabricMySQLServer = namedtuple(
    'FabricMySQLServer',
    ['uuid', 'group', 'host', 'port', 'mode', 'status', 'weight']
    )

# Order of field_names must match how Fabric is returning the data
FabricShard = namedtuple(
    'FabricShard',
    ['database', 'table', 'column', 'key',
     'shard', 'shard_type', 'group', 'global_group']
    )


def connect(**kwargs):
    """Create a MySQLFabricConnection object"""
    return MySQLFabricConnection(**kwargs)

__all__ = [
    'MODE_READWRITE',
    'MODE_READONLY',
    'STATUS_PRIMARY',
    'STATUS_SECONDARY',
    'SCOPE_GLOBAL',
    'SCOPE_LOCAL',
    'FabricMySQLServer',
    'FabricShard',
    'connect',
    'Fabric',
    'FabricConnection',
    'MySQLFabricConnection',
    'FabricSet',
]
