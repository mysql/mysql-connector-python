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
* dropping and creating a table
* inserting 3 rows using executemany()
* selecting data and showing it

"""

import mysql.connector


def main(config):
    output = []
    cnx = mysql.connector.connect(**config)
    cur = cnx.cursor()

    # Drop table if exists, and create it new
    stmt_drop = "DROP TABLE IF EXISTS names"
    cur.execute(stmt_drop)

    stmt_create = (
        "CREATE TABLE names ("
        "    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT, "
        "    name VARCHAR(30) DEFAULT '' NOT NULL, "
        "    info TEXT DEFAULT '', "
        "    age TINYINT UNSIGNED DEFAULT '30', "
        "PRIMARY KEY (id))"
    )
    cur.execute(stmt_create)

    info = "abc" * 10000

    # Insert 3 records
    names = (('Geert', info, 30), ('Jan', info, 31), ('Michel', info, 32))
    stmt_insert = "INSERT INTO names (name, info, age) VALUES (%s, %s, %s)"
    cur.executemany(stmt_insert, names)
    cnx.commit()

    # Read the names again and print them
    stmt_select = "SELECT id, name, info, age FROM names ORDER BY id"
    cur.execute(stmt_select)

    for row in cur.fetchall():
        output.append("%d | %s | %d\nInfo: %s..\n" % (
            row[0], row[1], row[3], row[2][:20]))

    # Cleaning up, dropping the table again
    cur.execute(stmt_drop)

    cur.close()
    cnx.close()
    return output


if __name__ == '__main__':

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
