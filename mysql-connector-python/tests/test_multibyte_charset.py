# Copyright (c) 2026, Oracle and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0, as
# published by the Free Software Foundation.
#
# This program is designed to work with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation. The authors of MySQL hereby grant you an
# additional permission to link the program and your derivative works
# with the separately licensed software that they have either included with
# the program or referenced in the documentation.
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

"""Unit and live-server tests for multibyte parameter conversion.

Unit tests check converter output without a server. Each live-server
connection path covers 72 combinations:
- Six connection charsets, including UTF8MB4 as a control.
- Two SQL modes: backslash escaping enabled and NO_BACKSLASH_ESCAPES.
- Three parameter types: str, bytes, and bytearray.
- Positional and named parameters.

Each combination checks comparisons and stored values using execute() and
executemany(). Subtest labels identify the combination being checked.
"""

import unittest

import tests

from mysql.connector import HAVE_CEXT, MySQLConnection
from mysql.connector.conversion import MySQLConverter

if HAVE_CEXT:
    from mysql.connector.connection_cext import CMySQLConnection
else:
    CMySQLConnection = None


CHARSETS = {
    "big5": "\u03b1",
    "sjis": "\u2015",
    "gbk": "\u2010",
    "cp932": "\u2015",
    "gb18030": "\u2010",
    "utf8mb4": "\u2015",  # Unaffected control.
}
SQL_MODES = ("", "NO_BACKSLASH_ESCAPES")
TABLE = "multibyte_parameter_conversion"
CREATE_TABLE = (
    f"CREATE TEMPORARY TABLE {TABLE} ("
    "id INT PRIMARY KEY, b VARBINARY(512), "
    "s VARCHAR(512) CHARACTER SET {charset})"
)


def scenarios(charset):
    """Share values and SQL between the sync and async regression tests."""
    codec = "utf8" if charset == "utf8mb4" else charset
    text_values = [
        "",
        "ordinary",
        "quote'",
        'double"',
        "slash\\",
        "nul\x00",
        "\n\r\x1a",
        CHARSETS[charset],
    ]
    sample_suffix = "' OR 1=1 -- "
    for value_type in (str, bytes, bytearray):
        if value_type is str:
            column = "s"
            values = text_values + [CHARSETS[charset] + sample_suffix]
            stored_hex = "HEX(s)"
            expected = [value.encode(codec) for value in values]
        else:
            column = "b"
            raw_values = [value.encode(codec) for value in text_values]
            raw_values += [bytes(range(256)), b"\x95" + sample_suffix.encode("ascii")]
            values = [value_type(value) for value in raw_values]
            stored_hex = "HEX(b)"
            expected = raw_values
        for named in (False, True):

            def parameters(index, value):
                return {"id": index, "value": value} if named else (index, value)

            placeholders = "%(id)s, %(value)s" if named else "%s, %s"
            placeholder = "%(value)s" if named else "%s"
            yield {
                "type": value_type.__name__,
                "named": named,
                "insert": f"INSERT INTO {TABLE} (id, {column}) VALUES ({placeholders})",
                "select": f"SELECT id FROM {TABLE} WHERE {column} = {placeholder}",
                "seed": [parameters(i, value) for i, value in enumerate(values[:-1])],
                "sample_insert": parameters(len(values) - 1, values[-1]),
                "lookups": [
                    {"value": value} if named else (value,) for value in values
                ],
                "hex_query": f"SELECT id, {stored_hex} FROM {TABLE} ORDER BY id",
                "expected": [
                    (i, value.hex().upper()) for i, value in enumerate(expected)
                ],
            }


class MultibyteCharsetUnitTests(tests.MySQLConnectorTests):
    """Check multibyte parameter conversion without a server connection."""

    def test_multibyte_charset_str_uses_hex_literal(self):
        """Strings using a 0x5c trail byte are sent as hex literals."""
        cases = (
            ("big5", "α"),
            ("sjis", "―"),
            ("gbk", "‐"),
            ("cp932", "―"),
            ("gb18030", "‐"),
        )
        suffix = "' OR 1=1 -- "

        for charset, value in cases:
            converter = MySQLConverter(charset=charset)
            parameter = value + suffix
            result = converter.quote(converter.escape(converter.to_mysql(parameter)))

            self.assertEqual(
                b"0x" + parameter.encode(charset).hex().encode(),
                result,
                charset,
            )

    def test_multibyte_charset_binary_uses_hex_literal(self):
        """Binary values use hex literals for selected multibyte charsets."""
        sample = bytes.fromhex("9527204f5220313d31202d2d20")
        expected = b"0x" + sample.hex().encode()

        for charset in ("big5", "sjis", "gbk", "cp932", "gb18030"):
            converter = MySQLConverter(charset=charset)
            for value in (sample, bytearray(sample)):
                result = converter.quote(converter.escape(converter.to_mysql(value)))

                self.assertEqual(expected, result, charset)


class MultibyteCharsetTests(tests.MySQLConnectorTests):
    """Run the 72-case matrix for pure Python and both C-extension paths."""

    @tests.foreach_cnx(MySQLConnection)
    def test_pure_python(self):
        self._check_matrix()

    @unittest.skipUnless(HAVE_CEXT, "C extension not available")
    @tests.foreach_cnx(CMySQLConnection, converter_class=MySQLConverter)
    def test_cext_custom_converter(self):
        self.assertIsInstance(self.cnx.converter, MySQLConverter)
        self._check_matrix()

    @unittest.skipUnless(HAVE_CEXT, "C extension not available")
    @tests.foreach_cnx(CMySQLConnection)
    def test_cext_native_control(self):
        self.assertIsNone(self.cnx.converter)
        self._check_matrix(native_conversion=True)

    def _check_matrix(self, native_conversion=False):
        with self.cnx.cursor() as cur:
            for charset in CHARSETS:
                self.cnx.set_charset_collation(charset)
                for sql_mode in SQL_MODES:
                    self.cnx.sql_mode = sql_mode
                    for case in scenarios(charset):
                        with self.subTest(
                            charset=charset,
                            sql_mode=sql_mode,
                            value_type=case["type"],
                            named=case["named"],
                        ):
                            # These native-conversion cases also fail before
                            # the Python converter changes; keep them separate.
                            if (
                                native_conversion
                                and charset == "gb18030"
                                and sql_mode == ""
                                and case["type"] in ("bytes", "bytearray")
                            ):
                                self.skipTest(
                                    "Existing native GB18030 binary conversion issue"
                                )
                            cur.execute(CREATE_TABLE.format(charset=charset))
                            try:
                                cur.executemany(case["insert"], case["seed"])
                                cur.execute(case["select"], case["lookups"][-1])
                                self.assertEqual(cur.fetchall(), [])
                                cur.execute(case["insert"], case["sample_insert"])
                                for index, params in enumerate(case["lookups"]):
                                    cur.execute(case["select"], params)
                                    self.assertEqual(cur.fetchall(), [(index,)])
                                cur.execute(case["hex_query"])
                                self.assertEqual(cur.fetchall(), case["expected"])
                                # Exercise the batched INSERT rewrite with
                                # the additional sample too.
                                cur.execute(f"DELETE FROM {TABLE}")
                                cur.executemany(
                                    case["insert"],
                                    case["seed"] + [case["sample_insert"]],
                                )
                                cur.execute(case["hex_query"])
                                self.assertEqual(cur.fetchall(), case["expected"])
                            finally:
                                cur.execute(f"DROP TEMPORARY TABLE {TABLE}")


class MultibyteCharsetAioTests(tests.MySQLConnectorAioTestCase):
    """Run the same 72-case matrix through asyncio's independent cursor code."""

    @tests.foreach_cnx_aio()
    async def test_asyncio(self):
        async with await self.cnx.cursor() as cur:
            for charset in CHARSETS:
                await self.cnx.set_charset_collation(charset)
                for sql_mode in SQL_MODES:
                    await self.cnx.set_sql_mode(sql_mode)
                    for case in scenarios(charset):
                        with self.subTest(
                            charset=charset,
                            sql_mode=sql_mode,
                            value_type=case["type"],
                            named=case["named"],
                        ):
                            await cur.execute(CREATE_TABLE.format(charset=charset))
                            try:
                                await cur.executemany(case["insert"], case["seed"])
                                await cur.execute(case["select"], case["lookups"][-1])
                                self.assertEqual(await cur.fetchall(), [])
                                await cur.execute(case["insert"], case["sample_insert"])
                                for index, params in enumerate(case["lookups"]):
                                    await cur.execute(case["select"], params)
                                    self.assertEqual(await cur.fetchall(), [(index,)])
                                await cur.execute(case["hex_query"])
                                self.assertEqual(await cur.fetchall(), case["expected"])
                                await cur.execute(f"DELETE FROM {TABLE}")
                                await cur.executemany(
                                    case["insert"],
                                    case["seed"] + [case["sample_insert"]],
                                )
                                await cur.execute(case["hex_query"])
                                self.assertEqual(await cur.fetchall(), case["expected"])
                            finally:
                                await cur.execute(f"DROP TEMPORARY TABLE {TABLE}")
