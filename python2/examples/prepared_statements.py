#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
"""Example using MySQL Prepared Statements

Example using MySQL Connector/Python showing:
* usage of Prepared Statements
"""
from __future__ import print_function

import mysql.connector
from mysql.connector.cursor import MySQLCursorPrepared


def main(config):
    output = []
    cnx = mysql.connector.Connect(**config)

    curprep = cnx.cursor(cursor_class=MySQLCursorPrepared)
    cur = cnx.cursor()
    
    # Drop table if exists, and create it new
    stmt_drop = "DROP TABLE IF EXISTS names"
    cur.execute(stmt_drop)
    
    stmt_create = (
        "CREATE TABLE names ("
        "id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT, "
        "name VARCHAR(30) DEFAULT '' NOT NULL, "
        "cnt TINYINT UNSIGNED DEFAULT 0, "
        "PRIMARY KEY (id))"
        )
    cur.execute(stmt_create)

    # Connector/Python also allows ? as placeholders for MySQL Prepared
    # statements.
    prepstmt = "INSERT INTO names (name) VALUES (%s)"

    # Preparing the statement is done only once. It can be done before
    # without data, or later with data.
    curprep.execute(prepstmt)

    # Insert 3 records
    names = ('Geert', 'Jan', 'Michel')
    for name in names:
        curprep.execute(prepstmt, (name,))
        cnx.commit()

    # We use a normal cursor issue a SELECT
    output.append("Inserted data")
    cur.execute("SELECT id, name FROM names")
    for row in cur:
        output.append("%d | %s" % (row[0], row[1]))

    # Cleaning up, dropping the table again
    cur.execute(stmt_drop)

    cnx.close()
    return output

if __name__ == '__main__':
    #
    # Configure MySQL login and database to use in config.py
    #
    from config import Config
    config = Config.dbinfo().copy()
    out = main(config)
    print('\n'.join(out))
