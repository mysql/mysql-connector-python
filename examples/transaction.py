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
* dropping and creating a table
* using warnings
* doing a transaction, rolling it back and committing one.

"""

import mysql.connector


def main(config):
    output = []
    db = mysql.connector.Connect(**config)
    cursor = db.cursor()
    
    # Drop table if exists, and create it new
    stmt_drop = "DROP TABLE IF EXISTS names"
    cursor.execute(stmt_drop)
    
    stmt_create = """
    CREATE TABLE names (
        id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
        name VARCHAR(30) DEFAULT '' NOT NULL,
        cnt TINYINT UNSIGNED DEFAULT 0,
        PRIMARY KEY (id)
    ) ENGINE=InnoDB"""
    cursor.execute(stmt_create)
    
    warnings = cursor.fetchwarnings()
    if warnings:
        ids = [ i for l,i,m in warnings]
        output.append("Oh oh.. we got warnings..")
        if 1266 in ids:
            output.append("""
            Table was created as MYISAM, no transaction support.
            
            Bailing out, no use to continue. Make sure InnoDB is available!
            """)
            db.close()
            return

    # Insert 3 records
    output.append("Inserting data")
    names = ( ('Geert',), ('Jan',), ('Michel',) )
    stmt_insert = "INSERT INTO names (name) VALUES (%s)"
    cursor.executemany(stmt_insert, names)
    
    # Roll back!!!!
    output.append("Rolling back transaction")
    db.rollback()

    # There should be no data!
    stmt_select = "SELECT id, name FROM names ORDER BY id"
    cursor.execute(stmt_select)
    rows = None
    try:
        rows = cursor.fetchall()
    except mysql.connector.InterfaceError as e:
        raise
        
    if rows == []:
        output.append("No data, all is fine.")
    else:
        output.append("Something is wrong, we have data although we rolled back!")
        output.append(rows)
        cursor.close()
        db.close()
        return output
        
    # Do the insert again.
    cursor.executemany(stmt_insert, names)

    # Data should be already there
    cursor.execute(stmt_select)
    output.append("Data before commit:")
    for row in cursor.fetchall():
        output.append("%d | %s" % (row[0], row[1]))
    
    # Do a commit
    db.commit()
    
    cursor.execute(stmt_select)
    output.append("Data after commit:")
    for row in cursor.fetchall():
        output.append("%d | %s" % (row[0], row[1]))
    	
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
