# Copyright (c) 2021, Oracle and/or its affiliates.
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

import os
import unittest

import mysqlx
import tests


@unittest.skipIf(tests.MYSQL_VERSION < (8, 0, 25), "XPlugin not compatible")
class CollectionIndexTests(tests.MySQLxTests):
    """Tests for collection index."""

    def _drop_collection_if_exists(self, name):
        collection = self.schema.get_collection(name)
        if collection.exists_in_database():
            self.schema.drop_collection(name)

    def _verify_index_creation(self, coll_name, index_name):
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, coll_name, index_name
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        return len(rows) == 1 and rows[0][2] == index_name

    def verify_multi_value_index_creation(
        self, coll_name, index_name, num_of_index_fields
    ):
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, coll_name, index_name
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        return len(rows) == num_of_index_fields and rows[0][2] == index_name

    @tests.foreach_session()
    def test_collection_index1(self):
        """Create a basic index."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self._drop_collection_if_exists("mycoll1")
        collection = self.schema.create_collection("mycoll1")
        collection.create_index(
            "myIndex",
            {
                "fields": [{"field": "$.intField", "type": "INT", "required": True}],
                "type": "INDEX",
            },
        ).execute()
        collection.add({"intField": 1}).execute()
        collection.add({"intField": "10"}).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll1", "myIndex"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][2], "myIndex")
        self.assertEqual(collection.count(), 2)
        collection.drop_index("myIndex")
        self.schema.drop_collection("mycoll1")

    @tests.foreach_session()
    def test_collection_index2(self):
        """Create index with "unique" option is True."""
        self._drop_collection_if_exists("mycoll2")
        collection = self.schema.create_collection("mycoll2")
        self.assertRaises(
            mysqlx.NotSupportedError,
            collection.create_index(
                "myIndex",
                {
                    "fields": [
                        {
                            "field": "$.intField",
                            "type": "INT",
                            "required": True,
                        }
                    ],
                    "unique": True,
                    "type": "INDEX",
                },
            ).execute,
        )
        self.schema.drop_collection("mycoll2")

    @tests.foreach_session()
    def test_collection_index3(self):
        """Create index on all int datatypes."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self._drop_collection_if_exists("mycoll3")
        collection = self.schema.create_collection("mycoll3")
        collection.create_index(
            "myIndexTinyINT",
            {
                "fields": [
                    {
                        "field": "$.intField",
                        "type": "TINYINT",
                        "required": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        collection.add({"intField": 1}).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll3", "myIndexTinyINT"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][2], "myIndexTinyINT")

        collection.create_index(
            "myIndexSmallINT",
            {
                "fields": [
                    {
                        "field": "$.intField",
                        "type": "SMALLINT",
                        "required": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        collection.add({"intField": 1}).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll3", "myIndexSmallINT"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][2], "myIndexSmallINT")

        collection.create_index(
            "myIndexMediumINT",
            {
                "fields": [
                    {
                        "field": "$.intField",
                        "type": "MEDIUMINT",
                        "required": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        collection.add({"intField": 1}).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll3", "myIndexMediumINT"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][2], "myIndexMediumINT")

        collection.create_index(
            "myIndexINT",
            {
                "fields": [
                    {
                        "field": "$.intField",
                        "type": "INT",
                        "required": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        collection.add({"intField": 1}).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll3", "myIndexINT"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][2], "myIndexINT")

        collection.create_index(
            "myIndexBigINT",
            {
                "fields": [
                    {
                        "field": "$.intField",
                        "type": "BIGINT",
                        "required": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        collection.add({"intField": 1}).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll3", "myIndexBigINT"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][2], "myIndexBigINT")

        collection.add({"intField": 1}).execute()
        collection.add({"intField": 10}).execute()
        self.assertEqual(collection.count(), 7)

        # Try to insert a different field - should fail as intField was
        # required to be provided ("required" option is True)
        self.assertRaises(
            mysqlx.OperationalError,
            collection.add({"myField": 1}).execute,
        )
        self.schema.drop_collection("mycoll3")

    @tests.foreach_session()
    def test_collection_index4(self):
        """Create basic index on all float types."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self._drop_collection_if_exists("mycoll4")
        collection = self.schema.create_collection("mycoll4")
        collection.create_index(
            "myIndexReal",
            {
                "fields": [{"field": "$.realField", "type": "REAL", "required": True}],
                "type": "INDEX",
            },
        ).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll4", "myIndexReal"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(rows[0][2], "myIndexReal")

        collection.create_index(
            "myIndexFloat",
            {
                "fields": [
                    {
                        "field": "$.floatField",
                        "type": "FLOAT",
                        "required": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll4", "myIndexFloat"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(rows[0][2], "myIndexFloat")

        collection.create_index(
            "myIndexDouble",
            {
                "fields": [
                    {
                        "field": "$.doubleField",
                        "type": "DOUBLE",
                        "required": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll4", "myIndexDouble"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(rows[0][2], "myIndexDouble")

        collection.create_index(
            "myIndexDecimal",
            {
                "fields": [
                    {
                        "field": "$.decimalField",
                        "type": "DECIMAL",
                        "required": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll4", "myIndexDecimal"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(rows[0][2], "myIndexDecimal")

        collection.create_index(
            "myIndexNumeric",
            {
                "fields": [
                    {
                        "field": "$.numericField",
                        "type": "NUMERIC",
                        "required": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll4", "myIndexNumeric"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(rows[0][2], "myIndexNumeric")

        collection.add(
            {
                "realField": 123.123,
                "floatField": 101.12,
                "doubleField": 13.145,
                "decimalField": 11.123,
                "numericField": 12.1,
            }
        ).execute()
        collection.add(
            {
                "realField": -101.1,
                "floatField": -111.11,
                "doubleField": -123.134,
                "decimalField": -11.13,
                "numericField": -13.12,
            }
        ).execute()
        collection.add(
            {
                "realField": "101.1",
                "floatField": "111.11",
                "doubleField": "123.134",
                "decimalField": "11.13",
                "numericField": "13.12",
            }
        ).execute()
        assert collection.count() == 3

        # Try to insert a different field - should fail as intField was
        # required to be provided ("required" option is True)
        self.assertRaises(
            mysqlx.OperationalError,
            collection.add({"myField": 1}).execute,
        )

        self.assertRaises(
            mysqlx.OperationalError,
            collection.add(
                {
                    "realField": 123.123,
                    "floatField": 101.12,
                    "doubleField": 13.145,
                    "decimalField": 11.123,
                }
            ).execute,
        )

        collection.drop_index("myIndexReal")
        collection.drop_index("myIndexFloat")
        collection.drop_index("myIndexDouble")
        collection.drop_index("myIndexDecimal")
        collection.drop_index("myIndexNumeric")
        self.schema.drop_collection("mycoll4")

    @tests.foreach_session()
    def test_collection_index5(self):
        """Create basic index on other datatypes.
        Server bug 27252354 has been opened. When we load the data and then
        try to create index gives error: Incorrect date value: '0000-00-00'
        for column <>"""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self._drop_collection_if_exists("mycoll5")
        collection = self.schema.create_collection("mycoll5")
        collection.create_index(
            "myIndexDate",
            {
                "fields": [{"field": "$.dateField", "type": "DATE", "required": True}],
                "type": "INDEX",
            },
        ).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll5", "myIndexDate"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(rows[0][2], "myIndexDate")

        collection.create_index(
            "myIndexTime",
            {
                "fields": [{"field": "$.timeField", "type": "TIME", "required": True}],
                "type": "INDEX",
            },
        ).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll5", "myIndexTime"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(rows[0][2], "myIndexTime")

        collection.create_index(
            "myIndexTimestamp",
            {
                "fields": [{"field": "$.timestampField", "type": "TIMESTAMP"}],
                "type": "INDEX",
            },
        ).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll5", "myIndexTimestamp"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(rows[0][2], "myIndexTimestamp")

        collection.create_index(
            "myIndexDatetime",
            {
                "fields": [
                    {
                        "field": "$.datetimeField",
                        "type": "DATETIME",
                        "required": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll5", "myIndexDatetime"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(rows[0][2], "myIndexDatetime")

        collection.add(
            {
                "dateField": "2017-12-12",
                "timeField": "12:20",
                "timestampField": "2017-12-12 20:12:07",
                "datetimeField": "2017-12-12 20:12:07",
            }
        ).execute()
        collection.add(
            {
                "dateField": "1000-01-01",
                "timeField": "12:20",
                "timestampField": "1970-01-01 01:01:01",
                "datetimeField": "1000-01-01 00:00:00",
            }
        ).execute()
        collection.add(
            {
                "dateField": "9999-12-31",
                "timeField": "12:20",
                "timestampField": "2038-01-19 03:14:07",
                "datetimeField": "9999-12-31 23:59:59",
            }
        ).execute()
        collection.add(
            {
                "dateField": "9999-12-31",
                "timeField": "12:20",
                "datetimeField": "9999-12-31 23:59:59",
            }
        ).execute()
        self.assertEqual(collection.count(), 4)

        # Try to omit a required field from adding into collection
        self.assertRaises(
            mysqlx.OperationalError,
            collection.add(
                {
                    "dateField": "9999-12-31",
                    "timestampField": "2038-01-19 03:14:07",
                    "datetimeField": "9999-12-31 23:59:59",
                }
            ).execute,
        )

        self.assertRaises(
            mysqlx.OperationalError,
            collection.add(
                {
                    "timestampField": "2038-01-19 03:14:07",
                    "datetimeField": "9999-12-31 23:59:59",
                }
            ).execute,
        )
        collection.drop_index("myIndexDate")
        collection.drop_index("myIndexTime")
        collection.drop_index("myIndexTimestamp")
        collection.drop_index("myIndexDatetime")
        self.schema.drop_collection("mycoll5")

    @tests.foreach_session()
    def test_collection_index6(self):
        """Test create index with TEXT type."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self._drop_collection_if_exists("mycoll6")
        collection = self.schema.create_collection("mycoll6")
        collection.create_index(
            "myIndexText",
            {
                "fields": [
                    {
                        "field": "$.textField",
                        "type": "TEXT(50)",
                        "required": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll6", "myIndexText"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(rows[0][2], "myIndexText")
        collection.add({"textField": "ijdir98jfrf98"}).execute()
        collection.add({"textField": "162376"}).execute()
        collection.add({"textField": "#&Y$(*#T$"}).execute()
        collection.add(
            {
                "textField": "#&Y$(*#T$jejioefko4fneifkfroilnrkfiufhiubfyuf3i8389u498343u8y4834u9348"
            }
        ).execute()
        self.assertEqual(collection.count(), 4)

        # Try to omit a required field from adding into collection
        self.assertRaises(
            mysqlx.OperationalError,
            collection.add({"dateField": "9999-12-31"}).execute,
        )
        collection.drop_index("myIndexTest")
        self.schema.drop_collection("mycoll6")

    @unittest.skip("TODO: Bug#27252609 Server bug")
    @tests.foreach_session()
    def test_collection_index7(self):
        """Test create index on GEOJSON type."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self._drop_collection_if_exists("mycoll7")
        collection = self.schema.create_collection("mycoll7")
        collection.add(
            {"geoField": {"type": "Point", "$.Coordinates": [1, 1]}}
        ).execute()
        collection.create_index(
            "myIndexGEOJSON",
            {
                "fields": [
                    {
                        "field": "$.geoField",
                        "type": "GEOJSON",
                        "required": True,
                        "options": 2,
                        "srid": 4326,
                    }
                ],
                "type": "SPATIAL",
            },
        ).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll7", "myIndexGEOJSON"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(rows[0][2], "myIndexGEOJSON")
        collection.add(
            {"geoField": {"type": "Point", "$.Coordinates": [1, 1]}}
        ).execute()
        self.assertEqual(collection.count(), 2)

        # Try to omit a required field from adding into collection
        self.assertRaises(
            mysqlx.OperationalError,
            collection.add({"dateField": "9999-12-31"}).execute,
        )
        collection.drop_index("myIndexGEOJSON")
        self.schema.drop_collection("mycoll7")

    @unittest.skip("TODO: BUG#27252609 Server bug")
    @tests.foreach_session()
    def test_collection_index8(self):
        """Test create_index on collection which already has some data."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self._drop_collection_if_exists("mycoll8")
        collection = self.schema.create_collection("mycoll8")
        collection.add({"intField": 10000}).execute()
        collection.add({"intField": "11"}).execute()
        collection.create_index(
            "myIndex",
            {
                "fields": [{"field": "$.intField", "type": "INT", "required": True}],
                "type": "INDEX",
            },
        ).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll8", "myIndex"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(rows[0][2], "myIndex")
        self.assertEqual(collection.count(), 2)
        collection.drop_index("myIndex")
        collection.add({"intField": [1, 1]}).execute()
        self.assertRaises(
            mysqlx.OperationalError,
            collection.create_index(
                "myIndex",
                {
                    "fields": [
                        {
                            "field": "$.intField",
                            "type": "INT",
                            "required": True,
                        }
                    ],
                    "type": "INDEX",
                },
            ).execute,
        )

        collection.remove("True").execute()
        collection.add({"intField": {"int1": 1, "int2": 2}}).execute()
        self.assertRaises(
            mysqlx.OperationalError,
            collection.create_index(
                "myIndex",
                {
                    "fields": [
                        {
                            "field": "$.intField",
                            "type": "INT",
                            "required": True,
                        }
                    ],
                    "type": "INDEX",
                },
            ).execute,
        )

        collection.remove("True").execute()
        collection.add({"dateField": "2017-10-10"}).execute()
        collection.create_index(
            "myIndex",
            {
                "fields": [{"field": "$.dateField", "type": "DATE"}],
                "type": "INDEX",
            },
        ).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll8", "myIndex"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(rows[0][2], "myIndex")
        self.schema.drop_collection("mycoll18")

    @unittest.skipIf(
        tests.MYSQL_EXTERNAL_SERVER,
        "Test not available for external MySQL servers",
    )
    @tests.foreach_session()
    def test_collection_index9(self):
        """Test create index with multiple fields."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self._drop_collection_if_exists("mycoll9")
        collection = self.schema.create_collection("mycoll9")
        collection.create_index(
            "myIndex",
            {
                "fields": [
                    {"field": "$.intField", "type": "INT", "required": True},
                    {
                        "field": "$.textField",
                        "type": "TEXT(30)",
                        "required": True,
                    },
                    {
                        "field": "$.floatField",
                        "type": "FLOAT",
                        "required": True,
                    },
                    {"field": "$.dateField", "type": "DATE", "required": True},
                ],
                "type": "INDEX",
            },
        ).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll9", "myIndex"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(rows[0][2], "myIndex")
        collection.add(
            {
                "intField": 1,
                "textField": "abcd",
                "floatField": 1234.2,
                "dateField": "2017-10-10",
            }
        ).execute()
        self.assertEqual(collection.count(), 1)
        self.assertRaises(
            mysqlx.OperationalError,
            collection.add({"intField": 1}).execute,
        )

        collection.drop_index("myIndex")

        collection.create_index(
            "myIndex",
            {
                "fields": [
                    {"field": "$.intField", "type": "INT", "required": True},
                    {
                        "field": "$.textField",
                        "type": "TEXT(30)",
                        "required": True,
                    },
                    {
                        "field": "$.floatField",
                        "type": "FLOAT",
                        "required": True,
                    },
                    {"field": "$.dateField", "type": "DATE"},
                ],
                "type": "INDEX",
            },
        ).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll9", "myIndex"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(rows[0][2], "myIndex")
        collection.drop_index("myIndex")

        collection.create_index(
            "myIndex",
            {
                "fields": [
                    {"field": "$.intField", "type": "INT", "required": True},
                    {
                        "field": "$.textField",
                        "type": "TEXT(30)",
                        "required": True,
                    },
                    {
                        "field": "$.floatField",
                        "type": "FLOAT",
                        "required": True,
                    },
                    {"field": "$.dateField", "type": "DATE"},
                    {
                        "field": "$.intField1",
                        "type": "INT UNSIGNED",
                        "required": True,
                    },
                ],
                "type": "INDEX",
            },
        ).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll9", "myIndex"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(rows[0][2], "myIndex")
        collection.drop_index("myIndex")

        self.assertRaises(
            mysqlx.ProgrammingError,
            collection.create_index(
                "myIndex1",
                {
                    "fields": [
                        {
                            "field": "$.intField",
                            "type": "INT",
                            "required": True,
                        },
                        {
                            "field": "$.floatField",
                            "type": "FLOAT",
                            "required": True,
                        },
                        {"field": "$.dateField", "type": "DATE"},
                        {
                            "field": "$.geoField",
                            "type": "GEOJSON",
                            "required": True,
                            "options": 2,
                            "srid": 4326,
                        },
                    ],
                    "type": "INDEX",
                },
            ).execute,
        )

        self.schema.drop_collection("mycoll9")

    @tests.foreach_session()
    def test_collection_index10(self):
        """Test create index with different index names."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self._drop_collection_if_exists("mycoll10")
        collection = self.schema.create_collection("mycoll10")
        self.assertRaises(
            mysqlx.ProgrammingError,
            collection.create_index(
                "123myIndex",
                {
                    "fields": [
                        {
                            "field": "$.intField",
                            "type": "INT",
                            "required": True,
                        }
                    ],
                    "type": "INDEX",
                },
            ).execute,
        )
        collection.create_index(
            "index12345678900123456789012",
            {
                "fields": [{"field": "$.intField", "type": "INT", "required": True}],
                "type": "INDEX",
            },
        ).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll10", "index12345678900123456789012"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(rows[0][2], "index12345678900123456789012")
        collection.drop_index("index12345678900123456789012")

        self.assertRaises(
            mysqlx.ProgrammingError,
            collection.create_index(
                "!myIndex",
                {
                    "fields": [
                        {
                            "field": "$.intField",
                            "type": "INT",
                            "required": True,
                        }
                    ],
                    "type": "INDEX",
                },
            ).execute,
        )

        self.assertRaises(
            mysqlx.ProgrammingError,
            collection.create_index(
                "@$^myIndex",
                {
                    "fields": [
                        {
                            "field": "$.intField",
                            "type": "INT",
                            "required": True,
                        }
                    ],
                    "type": "INDEX",
                },
            ).execute,
        )

        self.assertRaises(
            mysqlx.ProgrammingError,
            collection.create_index(
                "",
                {
                    "fields": [
                        {
                            "field": "$.intField",
                            "type": "INT",
                            "required": True,
                        }
                    ],
                    "type": "INDEX",
                },
            ).execute,
        )

        self.assertRaises(
            mysqlx.ProgrammingError,
            collection.create_index(
                "   ",
                {
                    "fields": [
                        {
                            "field": "$.intField",
                            "type": "INT",
                            "required": True,
                        }
                    ],
                    "type": "INDEX",
                },
            ).execute,
        )

        self.assertRaises(
            mysqlx.ProgrammingError,
            collection.create_index(
                None,
                {
                    "fields": [
                        {
                            "field": "$.intField",
                            "type": "INT",
                            "required": True,
                        }
                    ],
                    "type": "INDEX",
                },
            ).execute,
        )

        self.assertRaises(
            mysqlx.ProgrammingError,
            collection.create_index(
                "-myIndex",
                {
                    "fields": [
                        {
                            "field": "$.intField",
                            "type": "INT",
                            "required": True,
                        }
                    ],
                    "type": "INDEX",
                },
            ).execute,
        )

        self.schema.drop_collection("mycoll10")

    @tests.foreach_session()
    def test_collection_index11(self):
        """Test create invalid indexes."""
        self._drop_collection_if_exists("mycoll11")
        collection = self.schema.create_collection("mycoll11")
        collection.create_index(
            "myIndex",
            {
                "fields": [{"field": "$.intField", "type": "INT", "required": True}],
                "type": "INDEX",
            },
        ).execute()
        self.assertRaises(
            mysqlx.OperationalError,
            collection.create_index(
                "myIndex",
                {
                    "fields": [
                        {
                            "field": "$.intField",
                            "type": "INT",
                            "required": True,
                        }
                    ],
                    "type": "INDEX",
                },
            ).execute,
        )

        self.assertRaises(
            mysqlx.ProgrammingError,
            collection.create_index(
                "MyIndex",
                {
                    "fields": [
                        {
                            "field": "$.intField",
                            "mytype": "INT",
                            "required": True,
                        }
                    ]
                },
            ).execute,
        )

        self.assertRaises(
            mysqlx.OperationalError,
            collection.create_index(
                "myIndex",
                {
                    "fields": [
                        {
                            "field": "$.intField",
                            "type": "INT",
                            "required": True,
                        }
                    ],
                    "type": "WRONG",
                },
            ).execute,
        )

        self.assertRaises(
            mysqlx.OperationalError,
            collection.create_index(
                "myIndex",
                {
                    "fields": [
                        {
                            "field": "$.intField",
                            "type": "WRONG",
                            "required": True,
                        }
                    ],
                    "type": "INDEX",
                },
            ).execute,
        )

        self.assertRaises(
            mysqlx.OperationalError,
            collection.create_index(
                "myIndex",
                {
                    "fields": [
                        {
                            "field": "$.intField",
                            "type": "INT",
                            "required": True,
                        }
                    ],
                    "type": "SPATIAL",
                },
            ).execute,
        )

        self.assertRaises(
            AttributeError,
            collection.create_index,
            "myIndex",
            "",
        )

        collection.drop_index("nonExistant")

        self.assertRaises(
            mysqlx.ProgrammingError,
            collection.create_index(
                "myIndex",
                {
                    "fields": [
                        {
                            "field": "$.dateField",
                            "type": "DATETIME",
                            "collation": "utf8_general_ci",
                        }
                    ]
                },
            ).execute,
        )

        self.schema.drop_collection("mycoll11")

    @tests.foreach_session()
    def test_collection_index12(self):
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self._drop_collection_if_exists("mycoll12")
        collection = self.schema.create_collection("mycoll12")
        collection.add({"intField": {"subfield": 10}}).execute()

        collection.create_index(
            "myIndex",
            {
                "fields": [
                    {
                        "field": "$.intField.subfield",
                        "type": "INT",
                        "required": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll12", "myIndex"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(rows[0][2], "myIndex")

        collection.add({"intField": {"subfield": 1}}).execute()

        self.assertRaises(
            mysqlx.OperationalError,
            collection.add({"intField": "10"}).execute,
        )  # subfield is not provided, should throw exception

        collection.drop_index("myIndex")

        collection.remove("True").execute()

        collection.add({"intField": [10, 11, 12]}).execute()

        collection.create_index(
            "myIndex",
            {
                "fields": [{"field": "$.intField[1]", "type": "INT", "required": True}],
                "type": "INDEX",
            },
        ).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll12", "myIndex"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(rows[0][2], "myIndex")

        collection.add({"intField": [10, 11]}).execute()

        self.assertRaises(
            mysqlx.OperationalError,
            collection.add({"intField": [10]}).execute,
        )  # 0th value is provided but 1st is required

        collection.drop_index("myIndex")

        collection.remove("True").execute()

        collection.add(
            {
                "intField": [
                    {"subfield": 15},
                    {"subfield": 10},
                    {"subfield": 25},
                ]
            }
        ).execute()

        collection.create_index(
            "myIndex",
            {
                "fields": [
                    {
                        "field": "$.intField[1].subfield",
                        "type": "INT",
                        "required": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        show_indexes_sql = "SHOW INDEXES FROM `{0}`.`{1}` WHERE Key_name='{2}'".format(
            schema_name, "mycoll12", "myIndex"
        )
        result = self.session.sql(show_indexes_sql).execute()
        rows = result.fetch_all()
        self.assertEqual(rows[0][2], "myIndex")

        collection.add(
            {
                "intField": [
                    {"subfield": 35},
                    {"subfield": 30},
                    {"subfield": 45},
                ]
            }
        ).execute()

        self.assertRaises(
            mysqlx.OperationalError,
            collection.add({"intField": [{"subfield": 15}]}).execute,
        )

        collection.drop_index("myIndex")

        self.schema.drop_collection("mycoll12")

    @tests.foreach_session()
    def test_collection_index13(self):
        self._drop_collection_if_exists("mycoll13")
        collection = self.schema.create_collection("mycoll13")
        collection.create_index(
            "myIndex",
            {"fields": [{"field": "$._id", "type": "INT", "required": True}]},
        ).execute()
        collection.add({"_id": 1, "name": "a"}).execute()
        result = collection.find("$._id == 1").execute()
        self.assertEqual(collection.count(), 1)
        collection.drop_index("myIndex")
        self.schema.drop_collection("mycoll13")

    @tests.foreach_session()
    def test_collection_index14(self):
        self._drop_collection_if_exists("mycoll14")
        collection = self.schema.create_collection(
            "mycoll14",
        )
        collection.create_index(
            "index1",
            {"fields": [{"field": "$.myid", "type": "INT", "required": True}]},
        ).execute()
        collection.add({"myid": 1, "name": "a"}).execute()
        result = collection.find("$.myid == 1").execute()
        self.assertEqual(collection.count(), 1)
        collection.drop_index("index1")
        self.schema.drop_collection("mycoll14")

    @tests.foreach_session()
    def test_collection_index15(self):
        """Test dropping an invalid index."""
        self._drop_collection_if_exists("mycoll15")
        collection = self.schema.create_collection("mycoll15")
        collection.create_index(
            "index2",
            {"fields": [{"field": "$.myid", "type": "INT", "required": True}]},
        ).execute()
        collection.add({"myid": 1, "name": "a"}).execute()
        result = collection.find("$.myid == 1").execute()
        self.assertEqual(collection.count(), 1)
        collection.drop_index("index2")
        self.schema.drop_collection("mycoll15")

    @tests.foreach_session()
    def test_collection_index23(self):
        """
        Create index on different datatypes with field type as array.
        Create index on DECIMAL array field.
        """
        self._drop_collection_if_exists("mycoll23")
        collection = self.schema.create_collection("mycoll23")
        collection.add(
            {
                "_id": 1,
                "name": "decimalArray1",
                "decimalField": [
                    835975.76,
                    87349829932749.67,
                    89248481498149882498141.12,
                ],
                "dateField": ["2017-12-12", "2018-12-12", "2019-12-12"],
                "timeField": ["12:20", "11:20", "10:20"],
                "timestampField": [
                    "2017-12-12 20:12:07",
                    "2018-12-12 20:12:07",
                    "2019-12-12 20:12:07",
                ],
                "datetimeField": [
                    "2017-12-12 20:12:07",
                    "2018-12-12 20:12:07",
                    "2019-12-12 20:12:07",
                ],
                "binaryField": [0xE240, 0x0001E240],
            }
        ).execute()
        collection.add(
            {
                "_id": 2,
                "name": "name2",
                "dateField": ["2017-12-12", "2018-12-12", "2019-12-12"],
                "timeField": ["12:20", "11:20", "10:20"],
                "timestampField": [
                    "2017-12-12 20:12:07",
                    "2018-12-12 20:12:07",
                    "2019-12-12 20:12:07",
                ],
                "datetimeField": [
                    "2017-12-12 20:12:07",
                    "2018-12-12 20:12:07",
                    "2019-12-12 20:12:07",
                ],
                "binaryField": [0xE240, 0x0001E240],
                "decimalField": [835975.76, 87349829932.839],
            }
        ).execute()
        collection.create_index(
            "decimalIndex",
            {
                "fields": [
                    {
                        "field": "$.decimalField",
                        "type": "DECIMAL(65,2)",
                        "array": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        self.assertTrue(self._verify_index_creation("mycoll23", "decimalIndex"))
        # TODO: Enable the below line once the binding issue is resolved
        # result = collection.find(
        #     ":decimalField IN $.decimalField"
        # ).bind(
        #     {"decimalField": 89248481498149882498141.12}
        # ).execute()
        result = collection.find(
            "89248481498149882498141.12 IN $.decimalField"
        ).execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["name"], "decimalArray1")
        result = collection.find("87349829932749.67 OVERLAPS $.decimalField").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["name"], "decimalArray1")
        result = collection.find(
            "87349829932749.67 NOT OVERLAPS $.decimalField"
        ).execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["name"], "name2")
        collection.drop_index("decimalIndex")

        collection.create_index(
            "dateIndex",
            {
                "fields": [{"field": "$.dateField", "type": "DATE", "array": True}],
                "type": "INDEX",
            },
        ).execute()
        self.assertTrue(self._verify_index_creation("mycoll23", "dateIndex"))
        # TODO: Enable below line and disable explicit cast once the
        # Server bug 29752056 is fixed.
        # result = collection.find('"2019-12-12" IN $.dateField').execute()
        # result = collection.find('"1000-01-01" IN $.dateField').execute()
        result = collection.find("'2019-12-12' IN $.dateField").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 2)
        result = collection.find("'2017-12-12' NOT IN $.dateField").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 0)
        result = collection.find("'2018-12-12' OVERLAPS $.dateField").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 2)
        result = collection.find("'2017-12-12' NOT OVERLAPS $.dateField").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 0)
        collection.drop_index("dateIndex")

        collection.create_index(
            "timeIndex",
            {
                "fields": [{"field": "$.timeField", "type": "TIME", "array": True}],
                "type": "INDEX",
            },
        ).execute()
        self.assertTrue(self._verify_index_creation("mycoll23", "timeIndex"))
        result = collection.find('"12:20" IN $.timeField').execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 2)
        result = collection.find('"12:20" NOT IN $.timeField').execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 0)
        result = collection.find('"11:20" OVERLAPS $.timeField').execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 2)
        result = collection.find('"10:20" NOT OVERLAPS $.timeField').execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 0)
        collection.drop_index("timeIndex")

        collection.create_index(
            "datetimeIndex",
            {
                "fields": [
                    {
                        "field": "$.datetimeField",
                        "type": "DATETIME",
                        "array": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        self.assertTrue(self._verify_index_creation("mycoll23", "datetimeIndex"))
        result = collection.find('"2018-12-12 20:12:07" IN $.datetimeField').execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 2)
        result = collection.find(
            '"2018-12-12 20:12:07" NOT IN $.datetimeField'
        ).execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 0)
        result = collection.find(
            '"2019-12-12 20:12:07" OVERLAPS $.datetimeField'
        ).execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 2)
        result = collection.find(
            '"2017-12-12 20:12:07" NOT OVERLAPS $.datetimeField'
        ).execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 0)
        collection.drop_index("datetimeIndex")

        self.schema.drop_collection("mycoll23")

    @tests.foreach_session()
    def test_collection_index24(self):
        """Create array index with "unique" option is True.
        Should raise exception.
        """
        self._drop_collection_if_exists("mycoll24")
        collection = self.schema.create_collection("mycoll24")
        operation = collection.create_index(
            "uniqueIndex",
            {
                "fields": [
                    {
                        "field": "$.intField",
                        "type": "INT UNSIGNED",
                        "array": True,
                    }
                ],
                "unique": True,
                "type": "INDEX",
            },
        )
        self.assertRaises(mysqlx.NotSupportedError, operation.execute)
        self.schema.drop_collection("mycoll24")

    @tests.foreach_session()
    def test_collection_index25(self):
        """Test create index with different index names."""
        self._drop_collection_if_exists("mycoll25")
        collection = self.schema.create_collection("mycoll25")
        stmt = collection.create_index(
            "123myIndex",
            {
                "fields": [{"field": "$.intField", "type": "INT", "array": True}],
                "type": "INDEX",
            },
        )
        self.assertRaises(mysqlx.ProgrammingError, stmt.execute)

        self.schema.drop_collection("mycoll24")
        collection.create_index(
            "index12345678900123456789012",
            {
                "fields": [{"field": "$.intField", "type": "CHAR(100)", "array": True}],
                "type": "INDEX",
            },
        ).execute()
        self.assertTrue(
            self._verify_index_creation("mycoll25", "index12345678900123456789012")
        )

        stmt = collection.create_index(
            "!myIndex",
            {
                "fields": [
                    {
                        "field": "$.intField",
                        "type": "INT SIGNED",
                        "array": True,
                    }
                ],
                "type": "INDEX",
            },
        )
        self.assertRaises(mysqlx.ProgrammingError, stmt.execute)

        stmt = collection.create_index(
            "@$^myIndex",
            {
                "fields": [{"field": "$.intField", "type": "INT", "array": True}],
                "type": "INDEX",
            },
        )
        self.assertRaises(mysqlx.ProgrammingError, stmt.execute)

        stmt = collection.create_index(
            "",
            {
                "fields": [{"field": "$.intField", "type": "INT", "array": True}],
                "type": "INDEX",
            },
        )
        self.assertRaises(mysqlx.ProgrammingError, stmt.execute)

        collection.create_index(
            "   ",
            {
                "fields": [{"field": "$.intField", "type": "INT", "array": True}],
                "type": "INDEX",
            },
        )
        self.assertRaises(mysqlx.ProgrammingError, stmt.execute)

        collection.create_index(
            None,
            {
                "fields": [{"field": "$.intField", "type": "INT", "array": True}],
                "type": "INDEX",
            },
        )
        self.assertRaises(mysqlx.ProgrammingError, stmt.execute)

        collection.create_index(
            "-myIndex",
            {
                "fields": [{"field": "$.intField", "type": "INT", "array": True}],
                "type": "INDEX",
            },
        )
        self.assertRaises(mysqlx.ProgrammingError, stmt.execute)
        self.schema.drop_collection("mycoll25")

    @tests.foreach_session()
    def test_collection_index26(self):
        """Create array index on fields with mismatched datatypes."""
        # In below case,creating index on character field using SIGNED INTEGER
        # type of index.
        self._drop_collection_if_exists("mycoll26")
        collection = self.schema.create_collection(
            "mycoll26",
        )
        collection.add(
            {
                "_id": 1,
                "name": "charArray",
                "charField": ["char1", "char2", "char3"],
            }
        ).execute()
        stmt = collection.create_index(
            "mismatchIndex",
            {
                "fields": [{"field": "$.charField", "type": "SIGNED", "array": True}],
                "type": "INDEX",
            },
        )
        self.assertRaises(mysqlx.OperationalError, stmt.execute)
        self.schema.drop_collection("mycoll26")

    @tests.foreach_session()
    def test_collection_index_test27(self):
        """Create duplicate indexes."""
        self._drop_collection_if_exists("mycoll27")
        collection = self.schema.create_collection("mycoll27")
        collection.add(
            {
                "_id": 1,
                "name": "dateArray",
                "dateField": ["2017-12-12", "2018-12-12", "2018-12-12"],
            }
        ).execute()
        collection.create_index(
            "duplicateIndex",
            {
                "fields": [{"field": "$.dateField", "type": "DATE", "array": True}],
                "type": "INDEX",
            },
        ).execute()
        stmt = collection.create_index(
            "duplicateIndex",
            {
                "fields": [{"field": "$.dateField", "type": "DATE", "array": True}],
                "type": "INDEX",
            },
        )
        self.assertRaises(mysqlx.OperationalError, stmt.execute)
        self.schema.drop_collection("mycoll27")

    @unittest.skipIf(
        tests.MYSQL_EXTERNAL_SERVER,
        "Test not available for external MySQL servers",
    )
    @tests.foreach_session()
    def test_collection_index28(self):
        """Test creating an index on array field with "array" set to false."""
        self._drop_collection_if_exists("mycoll27")
        collection = self.schema.create_collection("mycoll28")
        collection.add(
            {
                "_id": 1,
                "name": "dateArray",
                "dateField": ["2017-12-12", "2018-12-12", "2018-12-12"],
            }
        ).execute()
        stmt = collection.create_index(
            "falseArrayIndex",
            {
                "fields": [{"field": "$.dateField", "type": "DATE", "array": False}],
                "type": "INDEX",
            },
        )
        stmt.execute()
        # self.assertRaises(mysqlx.OperationalError, stmt.execute)
        self.schema.drop_collection("mycoll28")

    @tests.foreach_session()
    def test_collection_index29(self):
        """Test creating an index on empty array field - should pass."""
        self._drop_collection_if_exists("mycoll29")
        collection = self.schema.create_collection("mycoll29")
        collection.add({"_id": 1, "name": "emptyArray", "emptyField": []}).execute()
        collection.create_index(
            "emptyArrayIndex",
            {
                "fields": [{"field": "$.emptyField", "type": "SIGNED", "array": True}],
                "type": "INDEX",
            },
        ).execute()
        collection.drop_index("emptyArrayIndex")
        self.schema.drop_collection("mycoll29")

    @unittest.skipIf(
        tests.MYSQL_EXTERNAL_SERVER,
        "Test not available for external MySQL servers",
    )
    @tests.foreach_session()
    def test_collection_index30(self):
        """Create a NOT NULL index on array field - should fail."""
        config = tests.get_mysqlx_config()
        schema_name = config["schema"]
        self._drop_collection_if_exists("mycoll30")
        collection = self.schema.create_collection("mycoll30")
        collection.add(
            {
                "_id": 1,
                "name": "dateArray",
                "dateField": ["2017-12-12", "2018-12-12", "2019-12-12"],
            }
        ).execute()
        self.session.sql(
            "ALTER TABLE `{}`.`mycoll30` ADD COLUMN `$dateField` DATE "
            "GENERATED ALWAYS AS (JSON_EXTRACT(doc, '$.dateField')) NOT NULL, "
            "ADD INDEX `notnullIndex`(`$dateField`)".format(schema_name)
        ).execute()
        self.assertTrue(self._verify_index_creation("mycoll30", "notnullIndex"))
        collection.drop_index("notnullIndex")

        self.assertRaises(
            mysqlx.OperationalError,
            collection.add(
                {"_id": 2, "name": "intArray", "intField": [10, 15, 20]}
            ).execute,
        )
        self.session.sql(
            "ALTER TABLE `{}`.`mycoll30` ADD COLUMN `$intField` "
            "INT UNSIGNED GENERATED ALWAYS AS "
            "(JSON_EXTRACT(doc, '$.intField')) NOT NULL, "
            "ADD INDEX `notnullIndex`(`$intField`)".format(schema_name)
        ).execute()
        self.assertTrue(self._verify_index_creation("mycoll30", "notnullIndex"))
        collection.drop_index("notnullIndex")

        self.assertRaises(
            mysqlx.OperationalError,
            collection.add(
                {
                    "_id": 3,
                    "name": "binaryArray",
                    "binaryField": [0xE240, 0x0001E240],
                }
            ).execute,
        )
        self.schema.drop_collection("mycoll30")

    @tests.foreach_session()
    def test_collection_index31(self):
        """Create index on nested array - should fail."""
        self._drop_collection_if_exists("mycoll31")
        collection = self.schema.create_collection("mycoll31")
        collection.add(
            {
                "_id": 1,
                "name": "nestedArrayIndex",
                "nestedArrayField": ["running", ["marathon", "walkathon"]],
            }
        ).execute()
        self.assertRaises(
            mysqlx.OperationalError,
            collection.create_index(
                "nestedArrayIndex",
                {
                    "fields": [
                        {
                            "field": "$.nestedArrayField",
                            "type": "CHAR(256)",
                            "array": True,
                        }
                    ],
                    "type": "INDEX",
                },
            ).execute,
        )
        self.schema.drop_collection("mycoll31")

    @unittest.skipUnless(tests.ARCH_64BIT, "Test available only for 64 bit platforms")
    @unittest.skipIf(os.name == "nt", "Test not available for Windows")
    @tests.foreach_session()
    def test_collection_index32(self):
        """Use overlaps/not-overlaps operators which intern uses the index.
        This is integration scenario: Index WL + overlaps WL
        """
        self._drop_collection_if_exists("mycoll32")
        collection = self.schema.create_collection("mycoll32")
        collection.add(
            {
                "_id": 1,
                "name": "dateArray1",
                "dateField": ["2017-12-12", "2018-12-12", "2019-12-12"],
            }
        ).add(
            {
                "_id": 2,
                "name": "dateArray2",
                "dateField": ["2017-10-10", "2018-10-10", "2019-10-10"],
            }
        ).execute()
        collection.add(
            {
                "_id": 3,
                "charField": "charField",
                "binaryField": 0xE240,
                "unsignedIntField": 4294967294,
                "dateField": "2017-12-12",
                "datetimeField": "2017-12-12 20:12:07",
                "decimalField": 9384.009939093,
                "timeField": "12:20",
            }
        ).execute()
        collection.add(
            {
                "_id": 4,
                "charFieldArr": ["charField1", "charField2" "charField3"],
                "binaryFieldArr": [0xE240, 0xE00240, 0xE460],
                "unsignedIntFieldArr": [4294967294, 87383278, 87263929],
                "dateFieldArr": ["2017-12-12", "2018-12-12", "2019-12-12"],
                "datetimeFieldArr": [
                    "2017-12-12 20:12:07",
                    "2018-12-12 18:12:07",
                    "2019-12-12 22:12:07",
                ],
                "decimalFieldArr": [
                    9384.009939093,
                    1384.009939093,
                    7884.009939093,
                ],
                "timeFieldArr": ["12:20, 11:11,06:45"],
            }
        ).execute()
        collection.create_index(
            "dateArrayIndex",
            {
                "fields": [{"field": "$.dateField", "type": "DATE", "array": True}],
                "type": "INDEX",
            },
        ).execute()
        self.assertTrue(self._verify_index_creation("mycoll32", "dateArrayIndex"))
        # Casting is required due to server Bug#29752056.
        # Once this is fixed it should work without casting
        result = collection.find("'2019-12-12' OVERLAPS $.dateField").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["name"], "dateArray1")
        result = collection.modify("true").set("name", "modifiedArray").execute()
        result = (
            collection.find(":dateField OVERLAPS $.dateField")
            .bind({"dateField": "2018-12-12"})
            .execute()
        )
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["name"], "modifiedArray")
        result = collection.remove("$.name OVERLAPS 'modifiedArray'").execute()
        self.assertEqual(result.get_affected_items_count(), 4)
        collection.drop_index("dateArrayIndex")
        self.schema.drop_collection("mycoll32")

    @tests.foreach_session()
    def test_collection_index33(self):
        """Create index on array fields with mixed datatype values."""
        self._drop_collection_if_exists("mycoll33")
        collection = self.schema.create_collection("mycoll33")
        collection.add(
            {
                "_id": 1,
                "name": "mixedDataIndex",
                "mixedDataField": ["running", 100, "2018-11-12", 1098309.8937],
            }
        ).execute()
        self.assertRaises(
            mysqlx.OperationalError,
            collection.create_index(
                "mixedDataIndex",
                {
                    "fields": [
                        {
                            "field": "$.mixedDataField",
                            "type": "SIGNED INTEGER",
                            "array": True,
                        }
                    ],
                    "type": "INDEX",
                },
            ).execute,
        )
        self.schema.drop_collection("mycoll33")

    @tests.foreach_session()
    def test_collection_index34(self):
        """Test creating an index on array field which contains None value."""
        self._drop_collection_if_exists("mycoll34")
        collection = self.schema.create_collection("mycoll34")
        collection.add({"_id": 1, "name": "nullArray", "nullField": [None]}).execute()
        self.assertRaises(
            mysqlx.OperationalError,
            collection.create_index(
                "nullArrayIndex",
                {
                    "fields": [
                        {
                            "field": "$.nullField",
                            "type": "SIGNED",
                            "array": True,
                        }
                    ],
                    "type": "INDEX",
                },
            ).execute,
        )
        self.schema.drop_collection("mycoll34")

    @tests.foreach_session()
    def test_collection_index35(self):
        """Test giving an invalid value to array property."""
        self._drop_collection_if_exists("mycoll35")
        collection = self.schema.create_collection("mycoll35")
        collection.add(
            {
                "_id": 1,
                "name": "dateArray",
                "dateField": ["2017-12-12", "2018-12-12", "2019-12-12"],
            }
        ).execute()
        self.assertRaises(
            TypeError,
            collection.create_index(
                "invalidArrayIndex",
                {
                    "fields": [
                        {
                            "field": "$.dateField",
                            "type": "DATE",
                            "array": "Invalid",
                        }
                    ],
                    "type": "INDEX",
                },
            ).execute,
        )
        # Invalid value to field type property
        collection.add(
            {"_id": 2, "name": "intArray", "intField": [10, 15, 20]}
        ).execute()
        self.assertRaises(
            mysqlx.OperationalError,
            collection.create_index(
                "invalidIndex",
                {
                    "fields": [{"field": "$.intField", "type": "FLOAT", "array": True}],
                    "type": "INDEX",
                },
            ).execute,
        )
        self.assertRaises(
            TypeError,
            collection.create_index(
                "nullArrayIndex",
                {
                    "fields": [
                        {
                            "field": "$.intField",
                            "type": "SIGNED",
                            "array": None,
                        }
                    ],
                    "type": "INDEX",
                },
            ).execute,
        )
        self.schema.drop_collection("mycoll35")

    @tests.foreach_session()
    def test_collection_index36(self):
        """Integration scenario between Index and overlaps."""
        # Test overlaps/not overlaps with two keys both having index
        self._drop_collection_if_exists("mycoll36")
        collection = self.schema.create_collection(
            "mycoll36",
        )
        collection.add(
            {
                "_id": 1,
                "name": "decimalArray1",
                "decimalField1": [
                    835975.76,
                    87349829932749.67,
                    89248481498149882498141.12,
                ],
                "decimalField2": [835975.76, 87349829932.839],
                "decimalField3": [835977.76, 87349829932.839],
            }
        ).execute()
        collection.add(
            {
                "_id": 2,
                "name": "dateArray1",
                "dateField1": ["2017-12-12", "2018-12-12", "2019-12-12"],
                "dateField2": ["2017-12-12", "2018-11-11", "2019-11-11"],
                "dateField3": ["2017-12-12", "2018-10-10", "2019-10-10"],
            }
        ).execute()
        collection.add(
            {
                "_id": 3,
                "name": "timeArray1",
                "timeField1": ["12:20", "11:20", "10:20"],
                "timeField2": ["12:00", "11:00", "10:20"],
                "timeField3": ["12:10", "11:10", "10:00"],
            }
        ).execute()
        collection.add(
            {
                "_id": 4,
                "name": "timestampArray1",
                "timestampField1": [
                    "2017-12-12 20:12:07",
                    "2018-12-12 20:12:07",
                    "2019-12-12 20:12:07",
                ],
                "timestampField2": [
                    "2017-12-12 20:12:07",
                    "2018-11-11 20:12:07",
                    "2019-11-11 20:12:07",
                ],
                "timestampField3": [
                    "2017-12-12 20:12:07",
                    "2018-10-11 20:12:07",
                    "2019-12-12 20:12:07",
                ],
            }
        ).execute()
        collection.add(
            {
                "_id": 5,
                "name": "datetimeArray1",
                "datetimeField1": [
                    "2017-12-12 20:12:07",
                    "2018-12-12 20:12:07",
                    "2019-12-12 20:12:07",
                ],
                "datetimeField2": [
                    "2017-12-12 20:12:07",
                    "2018-11-11 20:12:07",
                    "2019-11-11 20:12:07",
                ],
                "datetimeField3": [
                    "2017-10-10 20:12:07",
                    "2018-10-10 20:12:07",
                    "2019-10-10 20:12:07",
                ],
            }
        ).execute()
        collection.add(
            {
                "_id": 6,
                "name": "binaryArray1",
                "binaryField1": [0xE240, 0x0001E240],
                "binaryField2": [0xE240, 0x0001E240],
                "binaryField3": [0xE240, 0x0001E240],
            }
        ).execute()
        collection.add(
            {
                "_id": 7,
                "name": "dateArray2",
                "dateField1": ["2017-12-12", "2018-12-12", "2019-12-12"],
                "dateField2": ["2017-11-11", "2018-11-11", "2019-11-11"],
                "dateField3": ["2017-10-10", "2018-10-10", "2019-10-10"],
            }
        ).execute()
        collection.add(
            {
                "_id": 8,
                "name": "timeArray2",
                "timeField1": ["12:20", "11:20", "10:20"],
                "timeField2": ["12:00", "11:00", "10:00"],
                "timeField3": ["12:10", "11:10", "10:10"],
            }
        ).execute()
        collection.add(
            {
                "_id": 9,
                "name": "datetimeArray2",
                "datetimeField1": [
                    "2017-12-12 20:12:07",
                    "2018-12-12 20:12:07",
                    "2019-12-12 20:12:07",
                ],
                "datetimeField2": [
                    "2017-11-11 20:12:07",
                    "2018-11-11 20:12:07",
                    "2019-11-11 20:12:07",
                ],
                "datetimeField3": [
                    "2017-10-10 20:12:07",
                    "2018-10-10 20:12:07",
                    "2019-10-10 20:12:07",
                ],
            }
        ).execute()
        collection.add(
            {
                "_id": 10,
                "name": "binaryArray2",
                "binaryField1": [0xE240, 0x0001E240],
                "binaryField2": [0xE2040, 0x0001E2040],
                "binaryField3": [0xE02040, 0x0001E02040],
            }
        ).execute()
        collection.add(
            {
                "_id": 11,
                "name": "charArray1",
                "charField1": ["char1", "char2"],
                "charField2": ["char1", "char3"],
                "charField3": ["char1", "char2"],
            }
        ).execute()
        collection.add(
            {
                "_id": 12,
                "name": "charArray2",
                "charField1": ["char1", "char2"],
                "charField2": ["char3", "char4"],
                "charField3": ["char5", "char6"],
            }
        ).execute()
        collection.create_index(
            "decimalIndex1",
            {
                "fields": [
                    {
                        "field": "$.decimalField1",
                        "type": "DECIMAL(65,2)",
                        "array": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        collection.create_index(
            "decimalIndex2",
            {
                "fields": [
                    {
                        "field": "$.decimalField2",
                        "type": "DECIMAL(65,2)",
                        "array": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        self.assertTrue(self._verify_index_creation("mycoll36", "decimalIndex1"))
        self.assertTrue(self._verify_index_creation("mycoll36", "decimalIndex2"))
        # TODO: enable the below line once the binding issue is resolved
        result = collection.find("$.decimalField1 OVERLAPS $.decimalField2").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["name"], "decimalArray1")
        # Test with JSON array OVERLAPS key with index
        result = collection.find(
            "[835975.76,87349829932749.67,89248481498149882498141.12] OVERLAPS $.decimalField2"
        ).execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["name"], "decimalArray1")
        result = collection.find(
            "$.decimalField1 NOT OVERLAPS $.decimalField2"
        ).execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 0)
        collection.drop_index("decimalIndex1")
        collection.drop_index("decimalIndex2")

        collection.create_index(
            "dateIndex1",
            {
                "fields": [{"field": "$.dateField1", "type": "DATE", "array": True}],
                "type": "INDEX",
            },
        ).execute()
        collection.create_index(
            "dateIndex2",
            {
                "fields": [{"field": "$.dateField2", "type": "DATE", "array": True}],
                "type": "INDEX",
            },
        ).execute()
        self.assertTrue(self._verify_index_creation("mycoll36", "dateIndex1"))
        self.assertTrue(self._verify_index_creation("mycoll36", "dateIndex2"))
        result = collection.find("$.dateField1 IN $.dateField1").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 2)
        result = collection.find("$.dateField1 NOT IN $.dateField2").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 2)
        result = collection.find("$.dateField1 OVERLAPS $.dateField2").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["name"], "dateArray1")
        result = collection.find("$.dateField1 NOT OVERLAPS $.dateField2").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["name"], "dateArray2")
        collection.drop_index("dateIndex1")
        collection.drop_index("dateIndex2")

        collection.create_index(
            "timeIndex1",
            {
                "fields": [{"field": "$.timeField1", "type": "TIME", "array": True}],
                "type": "INDEX",
            },
        ).execute()
        collection.create_index(
            "timeIndex2",
            {
                "fields": [{"field": "$.timeField2", "type": "TIME", "array": True}],
                "type": "INDEX",
            },
        ).execute()
        self.assertTrue(self._verify_index_creation("mycoll36", "timeIndex1"))
        self.assertTrue(self._verify_index_creation("mycoll36", "timeIndex2"))

        result = collection.find("$.timeField1 IN $.timeField1").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 2)

        result = collection.find("$.timeField1 NOT IN $.timeField2").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 2)

        result = collection.find("$.timeField1 OVERLAPS $.timeField2").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["name"], "timeArray1")

        result = collection.find("$.timeField1 NOT OVERLAPS $.timeField2").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["name"], "timeArray2")

        collection.drop_index("timeIndex1")
        collection.drop_index("timeIndex2")

        collection.create_index(
            "datetimeIndex1",
            {
                "fields": [
                    {
                        "field": "$.datetimeField1",
                        "type": "DATETIME",
                        "array": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        collection.create_index(
            "datetimeIndex2",
            {
                "fields": [
                    {
                        "field": "$.datetimeField2",
                        "type": "DATETIME",
                        "array": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        self.assertTrue(self._verify_index_creation("mycoll36", "datetimeIndex1"))
        self.assertTrue(self._verify_index_creation("mycoll36", "datetimeIndex2"))
        result = collection.find("$.datetimeField1 IN $.datetimeField1").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 2)
        result = collection.find("$.datetimeField1 NOT IN $.datetimeField2").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 2)
        result = collection.find("$.datetimeField1 OVERLAPS $.datetimeField2").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["name"], "datetimeArray1")
        result = collection.find(
            "$.datetimeField1 NOT OVERLAPS $.datetimeField2"
        ).execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["name"], "datetimeArray2")
        collection.drop_index("datetimeIndex1")
        collection.drop_index("datetimeIndex2")

        collection.create_index(
            "binaryIndex1",
            {
                "fields": [
                    {
                        "field": "$.binaryField1",
                        "type": "BINARY(100)",
                        "array": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        collection.create_index(
            "binaryIndex2",
            {
                "fields": [
                    {
                        "field": "$.binaryField2",
                        "type": "BINARY(100)",
                        "array": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        self.assertTrue(self._verify_index_creation("mycoll36", "binaryIndex1"))
        self.assertTrue(self._verify_index_creation("mycoll36", "binaryIndex2"))
        result = collection.find("$.binaryField1 IN $.binaryField2").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["name"], "binaryArray1")
        result = collection.find("$.binaryField1 NOT IN $.binaryField2").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["name"], "binaryArray2")
        result = collection.find("$.binaryField1 OVERLAPS $.binaryField2").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["name"], "binaryArray1")
        result = collection.find("$.binaryField1 NOT OVERLAPS $.binaryField2").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["name"], "binaryArray2")
        collection.drop_index("binaryIndex")

        collection.create_index(
            "charIndex1",
            {
                "fields": [
                    {
                        "field": "$.charField1",
                        "type": "CHAR(512)",
                        "array": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        collection.create_index(
            "charIndex2",
            {
                "fields": [
                    {
                        "field": "$.charField2",
                        "type": "CHAR(512)",
                        "array": True,
                    }
                ],
                "type": "INDEX",
            },
        ).execute()
        self.assertTrue(self._verify_index_creation("mycoll36", "charIndex1"))
        self.assertTrue(self._verify_index_creation("mycoll36", "charIndex2"))
        result = collection.find("$.charField1 IN $.charField3").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["name"], "charArray1")
        result = collection.find("$.charField1 NOT IN $.charField2").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 2)
        result = collection.find("$.charField1 OVERLAPS $.charField2").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["name"], "charArray1")
        result = collection.find("$.charField1 NOT OVERLAPS $.charField2").execute()
        row = result.fetch_all()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["name"], "charArray2")
        collection.drop_index("binaryIndex")
        self.schema.drop_collection("mycoll36")
