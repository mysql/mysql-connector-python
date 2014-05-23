#!/usr/bin/env python
# -*- coding: utf-8 -*-

# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2009, 2013, Oracle and/or its affiliates. All rights reserved.

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

from __future__ import print_function

import sys, os

import mysql.connector

"""

Example using MySQL Connector/Python showing:
* dropping and creating a table
* using warnings
* doing a transaction, rolling it back and committing one.

"""

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
        if 1266L in ids:
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
    except (mysql.connector.errors.InterfaceError) as e:
        raise
        
    if rows == []:
        output.append("No data, all is fine.")
    else:
        output.append("Something is wrong, we have data although we rolled back!")
        output.append([repr(r) for r in rows])
        raise
        
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

    db.close()
    return output

if __name__ == '__main__':
    #
    # Configure MySQL login and database to use in config.py
    #
    from config import Config
    config = Config.dbinfo().copy()
    out = main(config)
    print('\n'.join(out))
