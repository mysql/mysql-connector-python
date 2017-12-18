#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2013, 2017, Oracle and/or its affiliates. All rights reserved.
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

"""Example using MySQL Prepared Statements

Example using MySQL Connector/Python showing:
* usage of Prepared Statements
"""

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
        output.append("{0} | {1}".format(*row))

    # Cleaning up, dropping the table again
    cur.execute(stmt_drop)

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
