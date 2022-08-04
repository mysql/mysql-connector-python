#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2009, 2022, Oracle and/or its affiliates. All rights reserved.
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
* How to save timestamps including microseconds
* Check the MySQL server version

NOTE: This only works with MySQL 5.6.4 or greater. This example will work
with earlier versions, but the microseconds information will be lost.

Story: We keep track of swimmers in a freestyle 4x 100m relay swimming event
with millisecond precision.
"""

from datetime import datetime, time

import mysql.connector

CREATE_TABLE = (
    "CREATE TABLE relay_laps ("
    "teamid TINYINT UNSIGNED NOT NULL, "
    "swimmer TINYINT UNSIGNED NOT NULL, "
    "lap TIME(3), "
    "start_shot DATETIME(6), "
    "PRIMARY KEY (teamid, swimmer)"
    ") ENGINE=InnoDB"
)


def main(config):
    output = []
    cnx = mysql.connector.Connect(**config)
    if cnx.get_server_version() < (5, 6, 4):
        output.append(
            "MySQL {0} does not support fractional precision"
            " for timestamps.".format(cnx.get_server_info())
        )
        return output
    cursor = cnx.cursor()

    try:
        cursor.execute("DROP TABLE IF EXISTS relay_laps")
    except:
        # Ignoring the fact that it was not there
        pass
    cursor.execute(CREATE_TABLE)

    teams = {}
    teams[1] = [
        (
            1,
            time(second=47, microsecond=510000),
            datetime(2009, 6, 7, 9, 15, 2, 234),
        ),
        (
            2,
            time(second=47, microsecond=20000),
            datetime(2009, 6, 7, 9, 30, 5, 102345),
        ),
        (
            3,
            time(second=47, microsecond=650000),
            datetime(2009, 6, 7, 9, 50, 23, 2300),
        ),
        (
            4,
            time(second=46, microsecond=60000),
            datetime(2009, 6, 7, 10, 30, 56, 1),
        ),
    ]

    insert = (
        "INSERT INTO relay_laps (teamid, swimmer, lap, start_shot) "
        "VALUES (%s, %s, %s, %s)"
    )
    for team, swimmers in teams.items():
        for swimmer in swimmers:
            cursor.execute(insert, (team, swimmer[0], swimmer[1], swimmer[2]))
    cnx.commit()

    cursor.execute("SELECT * FROM relay_laps")
    for row in cursor:
        output.append("{0: 2d} | {1: 2d} | {2} | {3}".format(*row))

    try:
        cursor.execute("DROP TABLE IF EXISTS relay_lapss")
    except:
        # Ignoring the fact that it was not there
        pass

    cursor.execute("DROP TABLE IF EXISTS relay_laps")
    cursor.close()
    cnx.close()

    return output


if __name__ == "__main__":

    config = {
        "host": "localhost",
        "port": 3306,
        "database": "test",
        "user": "root",
        "password": "",
        "charset": "utf8",
        "use_unicode": True,
        "get_warnings": True,
    }

    out = main(config)
    print("\n".join(out))
