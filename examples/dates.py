#!/usr/bin/env python
# -*- coding: utf-8 -*-

# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2009, 2014, Oracle and/or its affiliates. All rights reserved.

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

"""

Example using MySQL Connector/Python showing:
* How to get datetime, date and time types
* Shows also invalid dates returned and handled
* Force sql_mode to be not set for the active session

"""

from datetime import datetime

import mysql.connector


# Note that by default MySQL takes invalid timestamps. This is for
# backward compatibility. As of 5.0, use sql modes NO_ZERO_IN_DATE,NO_ZERO_DATE
# to prevent this.
_adate = datetime(1977, 6, 14, 21, 10, 00)
DATA = [
    (_adate.date(), _adate, _adate.time()),
    ('0000-00-00', '0000-00-00 00:00:00', '00:00:00'),
    ('1000-00-00', '9999-00-00 00:00:00', '00:00:00'),
]

def main(config):
    output = []
    db = mysql.connector.Connect(**config)
    cursor = db.cursor()

    tbl = 'myconnpy_dates'

    cursor.execute('SET sql_mode = ""')

    # Drop table if exists, and create it new
    stmt_drop = "DROP TABLE IF EXISTS {0}".format(tbl)
    cursor.execute(stmt_drop)

    stmt_create = (
        "CREATE TABLE {0} ( "
        "  `id` tinyint(4) NOT NULL AUTO_INCREMENT, "
        "  `c1` date DEFAULT NULL, "
        "  `c2` datetime NOT NULL, "
        "  `c3` time DEFAULT NULL, "
        "  `changed` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP "
        "    ON UPDATE CURRENT_TIMESTAMP, "
        "PRIMARY KEY (`id`))"
    ).format(tbl)
    cursor.execute(stmt_create)

    # not using executemany to handle errors better
    stmt_insert = ("INSERT INTO {0} (c1,c2,c3) VALUES "
                   "(%s,%s,%s)".format(tbl))
    for data in DATA:
        try:
            cursor.execute(stmt_insert, data)
        except (mysql.connector.errors.Error, TypeError) as exc:
            output.append("Failed inserting {0}\nError: {1}\n".format(
                data, exc))
            raise

    # Read the names again and print them
    stmt_select = "SELECT * FROM {0} ORDER BY id".format(tbl)
    cursor.execute(stmt_select)

    for row in cursor.fetchall():
        output.append("%3s | %10s | %19s | %8s |" % (
            row[0],
            row[1],
            row[2],
            row[3],
        ))

    # Cleaning up, dropping the table again
    cursor.execute(stmt_drop)

    cursor.close()
    db.close()
    return output


if __name__ == '__main__':
    #
    # Configure MySQL login and database to use in config.py
    #
    config = {
        'host': 'localhost',
        'port': 3306,
        'database': 'test',
        'user': 'root',
        'password': '',
        'charset': 'utf8',
        'use_unicode': True,
        'get_warnings': True,
    }

    out = main(config)
    print('\n'.join(out))
