# Copyright (c) 2023, Oracle and/or its affiliates.
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

"""Tests for IPv6 support."""

import unittest

import mysqlx

import tests


@unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 25), "XPlugin not compatible")
@unittest.skipUnless(tests.IPV6_AVAILABLE, "IPv6 is not available")
class ConnIPv6Tests(tests.MySQLxTests):
    """Tests for IPv6 support."""

    @tests.foreach_session(host="127.0.0.1", port=33060)
    def test_wl10081_test1(self):
        schema = self.session.get_schema("myconnpy_ipv6_test")
        if schema.exists_in_database():
            self.session.drop_schema("myconnpy_ipv6_test")
        schema = self.session.get_schema("myconnpy_ipv6_test")
        self.assertFalse(schema.exists_in_database())
        schema = self.session.create_schema("myconnpy_ipv6_test")
        self.assertEqual(schema.get_name(), "myconnpy_ipv6_test")
        self.assertTrue(schema.exists_in_database())
        self.session.drop_schema("myconnpy_ipv6_test")

    @tests.foreach_session(host="[::]", port=33060)
    def test_wl10081_test2(self):
        schema = self.session.get_schema("myconnpy_ipv6_test")
        if schema.exists_in_database():
            self.session.drop_schema("myconnpy_ipv6_test")
        schema = self.session.get_schema("myconnpy_ipv6_test")
        self.assertFalse(schema.exists_in_database())
        schema = self.session.create_schema("myconnpy_ipv6_test")
        self.assertEqual(schema.get_name(), "myconnpy_ipv6_test")
        self.assertTrue(schema.exists_in_database())
        self.session.drop_schema("myconnpy_ipv6_test")

    @tests.foreach_session(host="localhost", port=33060)
    def test_wl10081_test3(self):
        schema = self.session.get_schema("myconnpy_ipv6_test")
        if schema.exists_in_database():
            self.session.drop_schema("myconnpy_ipv6_test")
        schema = self.session.get_schema("myconnpy_ipv6_test")
        self.assertFalse(schema.exists_in_database())
        schema = self.session.create_schema("myconnpy_ipv6_test")
        self.assertEqual(schema.get_name(), "myconnpy_ipv6_test")
        self.assertTrue(schema.exists_in_database())
        self.session.drop_schema("myconnpy_ipv6_test")

    @tests.foreach_session(host="localhost")
    def test_wl10081_test4(self):
        schema = self.session.get_schema("myconnpy_ipv6_test")
        if schema.exists_in_database():
            self.session.drop_schema("myconnpy_ipv6_test")
        schema = self.session.get_schema("myconnpy_ipv6_test")
        self.assertFalse(schema.exists_in_database())
        schema = self.session.create_schema("myconnpy_ipv6_test")
        self.assertEqual(schema.get_name(), "myconnpy_ipv6_test")
        self.assertTrue(schema.exists_in_database())
        self.session.drop_schema("myconnpy_ipv6_test")

    @tests.foreach_session(host="localhost", port=33060)
    def test_wl10081_test5(self):
        schema = self.session.get_schema("myconnpy_ipv6_test")
        if schema.exists_in_database():
            self.session.drop_schema("myconnpy_ipv6_test")
        schema = self.session.create_schema("myconnpy_ipv6_test")
        self.session.sql("USE myconnpy_ipv6_test").execute()
        self.session.sql(
            "drop user if exists node@'2606:b400:85c:1048:221:f6ff:febb:95';"
        ).execute()
        self.session.sql(
            "create user node@'2606:b400:85c:1048:221:f6ff:febb:95' identified by 'abc';"
        ).execute()
        self.session.sql(
            "Grant all on *.* to  node@'2606:b400:85c:1048:221:f6ff:febb:95';"
        ).execute()
        session2 = mysqlx.get_session(
            "node:abc@[2606:b400:85c:1048:221:f6ff:febb:95]:33060"
        )
        self.session.drop_schema("myconnpy_ipv6_test")
