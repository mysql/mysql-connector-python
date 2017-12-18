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
* sending multiple statements and iterating over the results

"""

import mysql.connector


def main(config):
    output = []
    db = mysql.connector.Connect(**config)
    cursor = db.cursor()

    # Drop table if exists, and create it new
    stmt_drop = "DROP TABLE IF EXISTS names"
    cursor.execute(stmt_drop)

    stmt_create = (
        "CREATE TABLE names ("
        "    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT, "
        "    name VARCHAR(30) DEFAULT '' NOT NULL, "
        "    info TEXT DEFAULT '', "
        "    age TINYINT UNSIGNED DEFAULT '30', "
        "    PRIMARY KEY (id))"
    )
    cursor.execute(stmt_create)

    info = "abc" * 10000

    stmts = [
        "INSERT INTO names (name) VALUES ('Geert')",
        "SELECT COUNT(*) AS cnt FROM names",
        "INSERT INTO names (name) VALUES ('Jan'),('Michel')",
        "SELECT name FROM names",
    ]

    # Note 'multi=True' when calling cursor.execute()
    for result in cursor.execute(' ; '.join(stmts), multi=True):
        if result.with_rows:
            if result.statement == stmts[3]:
                output.append("Names in table: " +
                              ' '.join([name[0] for name in result]))
            else:
                output.append(
                    "Number of rows: {0}".format(result.fetchone()[0]))
        else:
            output.append("Inserted {0} row{1}".format(
                result.rowcount,
                's' if result.rowcount > 1 else ''))

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
