# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2012, 2017, Oracle and/or its affiliates. All rights reserved.

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
"""MySQL Connector/Python version information

The file version.py gets installed and is available after installation
as mysql.connector.version.
"""

VERSION = (2, 1, 6, '', 0)

if VERSION[3] and VERSION[4]:
    VERSION_TEXT = '{0}.{1}.{2}{3}{4}'.format(*VERSION)
else:
    VERSION_TEXT = '{0}.{1}.{2}'.format(*VERSION[0:3])

LICENSE = 'GPLv2 with FOSS License Exception'
EDITION = ''  # Added in package names, after the version
