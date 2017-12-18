#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2009, 2017, Oracle and/or its affiliates. All rights reserved.
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

"""

Example using MySQL Connector/Python showing:
* the usefulness of unicode, if it works correctly..
* dropping and creating a table
* inserting and selecting a row
"""

import mysql.connector

info = """
For this to work you need to make sure your terminal can output
unicode character correctly. Check if the encoding of your terminal
is set to UTF-8.
"""

def main(config):
    output = []
    db = mysql.connector.Connect(**config)
    cursor = db.cursor()
    
    # Show the unicode string we're going to use
    unistr = u"\u00bfHabla espa\u00f1ol?"
    output.append("Unicode string: %s" % unistr)
    
    # Drop table if exists, and create it new
    stmt_drop = "DROP TABLE IF EXISTS unicode"
    cursor.execute(stmt_drop)
    
    stmt_create = (
        "CREATE TABLE unicode ("
        "    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT, "
        "    str VARCHAR(50) DEFAULT '' NOT NULL, "
        "    PRIMARY KEY (id)"
        ") CHARACTER SET 'utf8'"
    )
    cursor.execute(stmt_create)
    
    # Insert a row
    stmt_insert = "INSERT INTO unicode (str) VALUES (%s)"
    cursor.execute(stmt_insert, (unistr,))
    
    # Select it again and show it
    stmt_select = "SELECT str FROM unicode WHERE id = %s"
    cursor.execute(stmt_select, (1,))
    row = cursor.fetchone()

    output.append("Unicode string coming from db: " + row[0])
    
    # Cleaning up, dropping the table again
    cursor.execute(stmt_drop)
    
    cursor.close()
    db.close()
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
