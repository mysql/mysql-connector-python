# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved.

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

"""MySQL Fabric support"""


from collections import namedtuple

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

CNX_FABRIC_ARGS = ['fabric_host', 'fabric_username', 'fabric_password',
                   'fabric_port', 'fabric_connect_attempts',
                   'fabric_connect_delay', 'fabric_report_errors',
                   'fabric_ssl_ca', 'fabric_ssl_key', 'fabric_ssl_cert',
                   'fabric_user']

from .connection import (
    MODE_READONLY, MODE_READWRITE,
    STATUS_PRIMARY, STATUS_SECONDARY,
    SCOPE_GLOBAL, SCOPE_LOCAL,
    Fabric, FabricConnection,
    MySQLFabricConnection,
    FabricSet,
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
