#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016, 2017, Oracle and/or its affiliates. All rights reserved.
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
* using warnings

"""

import mysql.connector

STMT = "SELECT 'abc'+1"


def main(config):
    output = []
    config['get_warnings'] = True
    db = mysql.connector.Connect(**config)
    cursor = db.cursor()
    db.sql_mode = ''
    
    output.append("Executing '%s'" % STMT)
    cursor.execute(STMT)
    cursor.fetchall()
    
    warnings = cursor.fetchwarnings()
    if warnings:
        for w in warnings:
            output.append("%d: %s" % (w[1],w[2]))
    else:
        output.append("We should have got warnings.")
        raise Exception("Got no warnings")

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
