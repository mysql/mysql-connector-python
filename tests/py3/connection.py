# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2013, Oracle and/or its affiliates. All rights reserved.

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

"""Unit tests for mysql.connector.connection specific to Python v3
"""

OK_PACKET = b'\x07\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00'
OK_PACKET_RESULT = {
    'insert_id': 0,
    'affected_rows': 0,
    'field_count': 0,
    'warning_count': 0,
    'server_status': 0
}

ERR_PACKET = b'\x47\x00\x00\x02\xff\x15\x04\x23\x32\x38\x30\x30\x30'\
             b'\x41\x63\x63\x65\x73\x73\x20\x64\x65\x6e\x69\x65\x64'\
             b'\x20\x66\x6f\x72\x20\x75\x73\x65\x72\x20\x27\x68\x61'\
             b'\x6d\x27\x40\x27\x6c\x6f\x63\x61\x6c\x68\x6f\x73\x74'\
             b'\x27\x20\x28\x75\x73\x69\x6e\x67\x20\x70\x61\x73\x73'\
             b'\x77\x6f\x72\x64\x3a\x20\x59\x45\x53\x29'

EOF_PACKET = b'\x05\x00\x00\x00\xfe\x00\x00\x00\x00'
EOF_PACKET_RESULT = {'status_flag': 0, 'warning_count': 0}
