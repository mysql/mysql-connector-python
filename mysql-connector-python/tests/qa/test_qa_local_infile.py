# Copyright (c) 2025, 2026, Oracle and/or its affiliates.
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

"""Local Infile Tests."""

import asyncio
import os
import unittest
import tests


try:
    from mysql.connector.connection_cext import CMySQLConnection
except ImportError:
    # Test without C Extension
    CMySQLConnection = None

from mysql.connector import (
    connection,
    constants,
    errors,
)
from mysql.connector.abstracts import DEFAULT_CONFIGURATION


class LocalInfileTests(tests.MySQLConnectorTests):
    def setUp(self):
        config = tests.get_mysql_config()
        if "unix_socket" in config:
            del config["unix_socket"]
        self.cnx = connection.MySQLConnection(**config)

    def tearDown(self):
        try:
            self.cnx.close()
        except:
            pass

    @unittest.skipIf(
        tests.MYSQL_VERSION < (8, 0, 0),
        "The local_infile option is disabled only in MySQL 8.0.",
    )
    @tests.foreach_cnx()
    def test_default_allow_local_infile(self):
        cur = self.cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS local_data")
        cur.execute("CREATE TABLE local_data (id int, c1 VARCHAR(6), c2 VARCHAR(6))")
        data_file = os.path.join("tests", "data", "local_data.csv")
        cur = self.cnx.cursor()
        sql = "LOAD DATA LOCAL INFILE %s INTO TABLE local_data"
        self.assertRaises(
            (errors.DatabaseError, errors.ProgrammingError),
            cur.execute,
            sql,
            (data_file,),
        )
        cur.execute("DROP TABLE IF EXISTS local_data")
        cur.close()

    @unittest.skipIf(
        tests.MYSQL_EXTERNAL_SERVER,
        "Test not available for external MySQL servers",
    )
    @tests.foreach_cnx(allow_local_infile=True)
    def test_allow_local_infile(self):
        cur = self.cnx.cursor()
        cur.execute("DROP TABLE IF EXISTS local_data")
        cur.execute("CREATE TABLE local_data (id int, c1 VARCHAR(6), c2 VARCHAR(6))")
        data_file = os.path.join("tests", "data", "local_data.csv")
        cur = self.cnx.cursor()
        sql = "LOAD DATA LOW_PRIORITY LOCAL INFILE %s INTO TABLE local_data"
        cur.execute(sql, (data_file,))
        cur.execute("SELECT * FROM local_data")

        exp = [
            (1, "c1_1", "c2_1"),
            (2, "c1_2", "c2_2"),
            (3, "c1_3", "c2_3"),
            (4, "c1_4", "c2_4"),
            (5, "c1_5", "c2_5"),
            (6, "c1_6", "c2_6"),
        ]
        self.assertEqual(exp, cur.fetchall())
        cur.execute("DROP TABLE IF EXISTS local_data")
        cur.close()

    @unittest.skipIf(
        tests.MYSQL_EXTERNAL_SERVER,
        "Test not available for external MySQL servers",
    )
    @tests.foreach_cnx()
    def test_allow_local_infile_in_path(self):
        if isinstance(self.cnx, connection.MySQLConnection):
            connector_class = connection.MySQLConnection
        else:
            connector_class = CMySQLConnection
        def_settings = tests.get_mysql_config()
        database = def_settings["database"]
        if "unix_socket" in def_settings:
            def_settings.pop("unix_socket")
        def_cur = self.cnx.cursor()

        def create_table():
            def_cur.execute(
                "DROP TABLE IF EXISTS {}.local_data_in_path".format(database)
            )
            def_cur.execute(
                "CREATE TABLE {}.local_data_in_path "
                "(id int, c1 VARCHAR(6), c2 VARCHAR(6))"
                "".format(database)
            )

        def verify_load_success(cur, data_file, exp):
            sql = (
                "LOAD DATA LOCAL INFILE %s INTO TABLE {}.local_data_in_path"
                "".format(database)
            )
            cur.execute(sql, (data_file,))
            cur.execute("SELECT * FROM {}.local_data_in_path".format(database))

            self.assertEqual(exp, cur.fetchall())
            cur.execute("TRUNCATE TABLE {}.local_data_in_path".format(database))
            cur.close()

        def verify_load_fails(cur, data_file, err_msgs, exception=errors.DatabaseError):
            sql = (
                "LOAD DATA LOCAL INFILE %s INTO TABLE {}.local_data_in_path"
                "".format(database)
            )
            with self.assertRaises(exception) as context:
                cur.execute(sql, (data_file,))

            exception_msg = str(context.exception)
            if isinstance(err_msgs, (list, tuple)):
                self.assertTrue(
                    [err for err in err_msgs if err in exception_msg],
                    "Unexpected exception message found: {}"
                    "".format(context.exception),
                )
            else:
                self.assertTrue(
                    (err_msgs in exception_msg),
                    "Unexpected exception "
                    "message found: {}".format(context.exception),
                )
            cur.close()

        exp1 = [
            (1, "c1_1", "c2_1"),
            (2, "c1_2", "c2_2"),
            (3, "c1_3", "c2_3"),
            (4, "c1_4", "c2_4"),
            (5, "c1_5", "c2_5"),
            (6, "c1_6", "c2_6"),
        ]

        exp2 = [
            (10, "c1_10", "c2_10"),
            (20, "c1_20", "c2_20"),
            (30, "c1_30", "c2_30"),
            (40, "c1_40", "c2_40"),
            (50, "c1_50", "c2_50"),
            (60, "c1_60", "c2_60"),
        ]

        create_table()
        # Verify defaults
        settings = def_settings.copy()
        self.assertEqual(constants.DEFAULT_CONFIGURATION["allow_local_infile"], False)
        self.assertEqual(
            constants.DEFAULT_CONFIGURATION["allow_local_infile_in_path"], None
        )

        # With allow_local_infile default value (False), upload must remain
        # disabled regardless of allow_local_infile_in_path value.
        cnx = connector_class(**settings)
        cur = cnx.cursor()
        data_file = os.path.join("tests", "data", "local_data.csv")
        verify_load_fails(
            cur,
            data_file,
            ["INFILE file request rejected", "command is not allowed"],
        )

        # With allow_local_infile set to  True without setting a value or
        # with None value or empty string for allow_local_infile_in_path user
        # must be able to upload files from any location.
        # allow_local_infile_in_path is None by default
        settings = def_settings.copy()
        settings["allow_local_infile"] = True
        cnx = connector_class(**settings)
        cur = cnx.cursor()
        data_file = os.path.join("tests", "data", "local_data.csv")
        verify_load_success(cur, data_file, exp1)

        # allow_local_infile_in_path as empty string
        settings["allow_local_infile"] = True
        settings["allow_local_infile_in_path"] = ""
        cnx = connector_class(**settings)
        cur = cnx.cursor()
        data_file = os.path.join(
            "tests", "data", "in_file_path", "local_data_in_path.csv"
        )
        verify_load_success(cur, data_file, exp2)

        # allow_local_infile_in_path as certain base_path but not used
        settings["allow_local_infile"] = True
        settings["allow_local_infile_in_path"] = os.path.join(
            "tests", "data", "in_file_path"
        )
        cnx = connector_class(**settings)
        cur = cnx.cursor()
        data_file = os.path.join("tests", "data", "local_data.csv")
        verify_load_success(cur, data_file, exp1)

        # allow_local_infile_in_path as certain base_path and using it
        settings["allow_local_infile"] = True
        settings["allow_local_infile_in_path"] = os.path.join(
            "tests", "data", "in_file_path"
        )
        cnx = connector_class(**settings)
        cur = cnx.cursor()
        data_file = os.path.join(
            "tests", "data", "in_file_path", "local_data_in_path.csv"
        )
        verify_load_success(cur, data_file, exp2)

        # With allow_local_infile set to False, upload must remain disabled
        # with default value of allow_local_infile_in_path or empty string.
        settings = def_settings.copy()
        settings["allow_local_infile"] = False
        cnx = connector_class(**settings)
        cur = cnx.cursor()
        data_file = os.path.join("tests", "data", "local_data.csv")
        verify_load_fails(
            cur,
            data_file,
            ["INFILE file request rejected", "command is not allowed"],
        )

        settings["allow_local_infile_in_path"] = None
        cnx = connector_class(**settings)
        cur = cnx.cursor()
        data_file = os.path.join("tests", "data", "local_data.csv")
        verify_load_fails(
            cur,
            data_file,
            ["INFILE file request rejected", "command is not allowed"],
        )

        settings["allow_local_infile_in_path"] = ""
        cnx = connector_class(**settings)
        cur = cnx.cursor()
        data_file = os.path.join("tests", "data", "local_data.csv")
        verify_load_fails(
            cur,
            data_file,
            ["INFILE file request rejected", "command is not allowed"],
        )

        # With allow_local_infile set to False and allow_local_infile_in_path
        # set to <base_path> user must be able to upload files from <base_path>
        # and any subfolder.
        settings = def_settings.copy()
        settings["allow_local_infile"] = False
        settings["allow_local_infile_in_path"] = os.path.join("tests", "data")
        cnx = connector_class(**settings)
        cur = cnx.cursor()
        data_file = os.path.join("tests", "data", "local_data.csv")
        verify_load_success(cur, data_file, exp1)

        settings["allow_local_infile_in_path"] = "tests"
        cnx = connector_class(**settings)
        cur = cnx.cursor()
        data_file = os.path.join("tests", "data", "local_data.csv")
        verify_load_success(cur, data_file, exp1)

        # Using subtree
        cnx = connector_class(**settings)
        cur = cnx.cursor()
        data_file = os.path.join(
            "tests", "data", "in_file_path", "local_data_in_path.csv"
        )
        verify_load_success(cur, data_file, exp2)

        cnx = connector_class(**settings)
        cur = cnx.cursor()
        data_file = os.path.join(
            "tests", "data", "in_file_path", "..", "local_data.csv"
        )
        verify_load_success(cur, data_file, exp1)

        # Upload from a file located outside allow_local_infile_in_path must
        # raise an error
        settings = def_settings.copy()
        settings["allow_local_infile"] = False
        settings["allow_local_infile_in_path"] = os.path.join(
            "tests", "data", "in_file_path"
        )
        cnx = connector_class(**settings)
        cur = cnx.cursor()
        data_file = os.path.join("tests", "data", "local_data.csv")
        verify_load_fails(cur, data_file, ("file request rejected", "not found in"))

        # Changing allow_local_infile_in_path
        cnx = connector_class(**settings)
        cur = cnx.cursor()
        cnx.set_allow_local_infile_in_path(os.path.join("tests", "data"))
        data_file = os.path.join("tests", "data", "local_data.csv")
        verify_load_success(cur, data_file, exp1)

        # Changing allow_local_infile_in_path to disallow upload
        cnx = connector_class(**settings)
        cur = cnx.cursor()
        cnx.set_allow_local_infile_in_path("")
        data_file = os.path.join("tests", "data", "local_data.csv")
        verify_load_fails(cur, data_file, "file request rejected")

        # Changing disabled allow_local_infile_in_path to allow upload
        settings["allow_local_infile_in_path"] = None
        cnx = connector_class(**settings)
        cur = cnx.cursor()
        cnx.set_allow_local_infile_in_path("")
        data_file = os.path.join("tests", "data", "local_data.csv")
        verify_load_fails(
            cur, data_file, ("file request rejected", "command is not allowed")
        )

        # relative path that results outside of infile_in_path
        settings["allow_local_infile_in_path"] = os.path.join(
            "tests", "data", "in_file_path"
        )
        cnx = connector_class(**settings)
        cur = cnx.cursor()
        data_file = os.path.join(
            "tests", "data", "in_file_path", "..", "local_data.csv"
        )
        verify_load_fails(
            cur,
            data_file,
            ("file request rejected", "not found", "command is not allowed"),
        )

        # Using a file instead of a directory
        settings = def_settings.copy()
        settings["allow_local_infile"] = False
        settings["allow_local_infile_in_path"] = os.path.join(
            "tests", "data", "local_data.csv"
        )
        with self.assertRaises(AttributeError) as _:
            cnx = connector_class(**settings)
        cnx.close()

        if os.name != "nt":
            # Using a Symlink in allow_local_infile_in_path is forbiden
            target = os.path.abspath(os.path.join("tests", "data"))
            link = os.path.join(os.path.abspath("tests"), "data_sl")
            os.symlink(target, link)
            settings = def_settings.copy()
            settings["allow_local_infile"] = False
            settings["allow_local_infile_in_path"] = link
            with self.assertRaises(AttributeError) as _:
                cnx = connector_class(**settings)
            cnx.close()
            try:
                os.remove(link)
            except FileNotFoundError:
                pass

        if os.name != "nt" and connector_class != CMySQLConnection:
            # Load from a Symlink is not allowed
            data_dir = os.path.abspath(os.path.join("tests", "data"))
            target = os.path.abspath(os.path.join(data_dir, "local_data.csv"))
            link = os.path.join(data_dir, "local_data_sl.csv")
            os.symlink(target, link)
            settings = def_settings.copy()
            settings["allow_local_infile"] = False
            settings["allow_local_infile_in_path"] = data_dir
            cnx = connector_class(**settings)
            cur = cnx.cursor()
            verify_load_fails(cur, link, "link is not allowed", errors.OperationalError)
            cnx.close()
            try:
                os.remove(link)
            except FileNotFoundError:
                pass

        # Clean up
        def_cur.execute("DROP TABLE IF EXISTS {}.local_data_in_path".format(database))
        def_cur.close()


class LocalInfileAioTests(tests.MySQLConnectorAioTestCase):

    @unittest.skipIf(
        tests.MYSQL_VERSION < (8, 0, 0),
        "The local_infile option is disabled only in MySQL 8.0.",
    )
    @tests.foreach_cnx_aio()
    async def test_default_allow_local_infile(self):
        async with await self.cnx.cursor() as cur:
            await cur.execute("DROP TABLE IF EXISTS local_data")
            await cur.execute(
                "CREATE TABLE local_data (id int, c1 VARCHAR(6), c2 VARCHAR(6))"
            )
            data_file = os.path.join("tests", "data", "local_data.csv")
            sql = "LOAD DATA LOCAL INFILE %s INTO TABLE local_data"
            with self.assertRaises((errors.DatabaseError, errors.ProgrammingError)):
                await cur.execute(sql, (data_file,))
            await cur.execute("DROP TABLE IF EXISTS local_data")

    @unittest.skipIf(
        tests.MYSQL_EXTERNAL_SERVER,
        "Test not available for external MySQL servers",
    )
    @tests.foreach_cnx_aio(allow_local_infile=True)
    async def test_allow_local_infile(self):
        async with await self.cnx.cursor() as cur:
            await cur.execute("DROP TABLE IF EXISTS local_data")
            await cur.execute(
                "CREATE TABLE local_data (id int, c1 VARCHAR(6), c2 VARCHAR(6))"
            )
            data_file = os.path.join("tests", "data", "local_data.csv")
            sql = "LOAD DATA LOW_PRIORITY LOCAL INFILE %s INTO TABLE local_data"
            await cur.execute(sql, (data_file,))
            await cur.execute("SELECT * FROM local_data")

            exp = [
                (1, "c1_1", "c2_1"),
                (2, "c1_2", "c2_2"),
                (3, "c1_3", "c2_3"),
                (4, "c1_4", "c2_4"),
                (5, "c1_5", "c2_5"),
                (6, "c1_6", "c2_6"),
            ]
            self.assertEqual(exp, await cur.fetchall())
            cur.execute("DROP TABLE IF EXISTS local_data")

    @unittest.skipIf(
        tests.MYSQL_EXTERNAL_SERVER,
        "Test not available for external MySQL servers",
    )
    @tests.foreach_cnx_aio(allow_local_infile=True, write_timeout=10)
    async def test_allow_local_infile_with_write_timeout(self):
        """LOAD DATA LOCAL INFILE must not hang when write_timeout is set."""
        async with await self.cnx.cursor() as cur:
            await cur.execute("DROP TABLE IF EXISTS local_data")
            await cur.execute(
                "CREATE TABLE local_data (id int, c1 VARCHAR(6), c2 VARCHAR(6))"
            )
            data_file = os.path.join("tests", "data", "local_data.csv")
            sql = "LOAD DATA LOCAL INFILE %s INTO TABLE local_data"
            await asyncio.wait_for(cur.execute(sql, (data_file,)), 30)
            await cur.execute("SELECT COUNT(*) FROM local_data")
            self.assertEqual((6,), await cur.fetchone())
            await cur.execute("DROP TABLE IF EXISTS local_data")

    @unittest.skipIf(
        tests.MYSQL_EXTERNAL_SERVER,
        "Test not available for external MySQL servers",
    )
    @tests.foreach_cnx_aio(allow_local_infile=True)
    async def test_allow_local_infile_in_path(self):
        connector_class = self.cnx.__class__
        def_settings = tests.get_mysql_config()
        database = def_settings["database"]
        if "unix_socket" in def_settings:
            def_settings.pop("unix_socket")
        def_cur = await self.cnx.cursor()

        async def create_table():
            await def_cur.execute(f"DROP TABLE IF EXISTS {database}.local_data_in_path")
            await def_cur.execute(
                f"CREATE TABLE {database}.local_data_in_path "
                "(id int, c1 VARCHAR(6), c2 VARCHAR(6))"
            )

        async def verify_load_success(cur, data_file, exp):
            sql = f"LOAD DATA LOCAL INFILE %s INTO TABLE {database}.local_data_in_path"
            await cur.execute(sql, (data_file,))
            await cur.execute(f"SELECT * FROM {database}.local_data_in_path")

            self.assertEqual(exp, await cur.fetchall())
            await cur.execute(f"TRUNCATE TABLE {database}.local_data_in_path")
            await cur.close()

        async def verify_load_fails(cur, data_file, err_msgs, exception=errors.DatabaseError):
            sql = (
                "LOAD DATA LOCAL INFILE %s INTO TABLE {}.local_data_in_path"
                "".format(database)
            )
            with self.assertRaises(exception) as context:
                await cur.execute(sql, (data_file,))

            exception_msg = str(context.exception)
            if isinstance(err_msgs, (list, tuple)):
                self.assertTrue(
                    [err for err in err_msgs if err in exception_msg],
                    f"Unexpected exception message found: {context.exception}",
                )
            else:
                self.assertTrue(
                    (err_msgs in exception_msg),
                    f"Unexpected exception message found: {context.exception}",
                )
            await cur.close()

        exp1 = [
            (1, "c1_1", "c2_1"),
            (2, "c1_2", "c2_2"),
            (3, "c1_3", "c2_3"),
            (4, "c1_4", "c2_4"),
            (5, "c1_5", "c2_5"),
            (6, "c1_6", "c2_6"),
        ]

        exp2 = [
            (10, "c1_10", "c2_10"),
            (20, "c1_20", "c2_20"),
            (30, "c1_30", "c2_30"),
            (40, "c1_40", "c2_40"),
            (50, "c1_50", "c2_50"),
            (60, "c1_60", "c2_60"),
        ]

        await create_table()
        # Verify defaults
        settings = def_settings.copy()
        self.assertEqual(DEFAULT_CONFIGURATION["allow_local_infile"], False)
        self.assertEqual(DEFAULT_CONFIGURATION["allow_local_infile_in_path"], None)

        # With allow_local_infile default value (False), upload must remain
        # disabled regardless of allow_local_infile_in_path value.
        cnx = connector_class(**settings)
        cur = cnx.cursor()
        data_file = os.path.join("tests", "data", "local_data.csv")
        verify_load_fails(
            cur,
            data_file,
            ["INFILE file request rejected", "command is not allowed"],
        )

        # With allow_local_infile set to  True without setting a value or
        # with None value or empty string for allow_local_infile_in_path user
        # must be able to upload files from any location.
        # allow_local_infile_in_path is None by default
        settings = def_settings.copy()
        settings["allow_local_infile"] = True
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_success(cur, data_file, exp1)

        # allow_local_infile_in_path as empty string
        settings["allow_local_infile"] = True
        settings["allow_local_infile_in_path"] = ""
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join(
                    "tests", "data", "in_file_path", "local_data_in_path.csv"
                )
                await verify_load_success(cur, data_file, exp2)

        # allow_local_infile_in_path as certain base_path but not used
        settings["allow_local_infile"] = True
        settings["allow_local_infile_in_path"] = os.path.join(
            "tests", "data", "in_file_path"
        )
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_success(cur, data_file, exp1)

        # allow_local_infile_in_path as certain base_path and using it
        settings["allow_local_infile"] = True
        settings["allow_local_infile_in_path"] = os.path.join(
            "tests", "data", "in_file_path"
        )
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join(
                    "tests", "data", "in_file_path", "local_data_in_path.csv"
                )
                await verify_load_success(cur, data_file, exp2)

        # With allow_local_infile set to False, upload must remain disabled
        # with default value of allow_local_infile_in_path or empty string.
        settings = def_settings.copy()
        settings["allow_local_infile"] = False
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_fails(
                    cur,
                    data_file,
                    ["INFILE file request rejected", "command is not allowed"],
                )

        settings["allow_local_infile_in_path"] = None
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_fails(
                    cur,
                    data_file,
                    ["INFILE file request rejected", "command is not allowed"],
                )

        settings["allow_local_infile_in_path"] = ""
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_fails(
                    cur,
                    data_file,
                    ["INFILE file request rejected", "command is not allowed"],
                )

        # With allow_local_infile set to False and allow_local_infile_in_path
        # set to <base_path> user must be able to upload files from <base_path>
        # and any subfolder.
        settings = def_settings.copy()
        settings["allow_local_infile"] = False
        settings["allow_local_infile_in_path"] = os.path.join("tests", "data")
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_success(cur, data_file, exp1)

        settings["allow_local_infile_in_path"] = "tests"
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_success(cur, data_file, exp1)

        # Using subtree
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join(
                    "tests", "data", "in_file_path", "local_data_in_path.csv"
                )
                await verify_load_success(cur, data_file, exp2)

        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join(
                    "tests", "data", "in_file_path", "..", "local_data.csv"
                )
                await verify_load_success(cur, data_file, exp1)

        # Upload from a file located outside allow_local_infile_in_path must
        # raise an error
        settings = def_settings.copy()
        settings["allow_local_infile"] = False
        settings["allow_local_infile_in_path"] = os.path.join(
            "tests", "data", "in_file_path"
        )
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_fails(
                    cur, data_file, ("file request rejected", "not found in")
                )

        # Changing allow_local_infile_in_path
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                cnx.set_allow_local_infile_in_path(os.path.join("tests", "data"))
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_success(cur, data_file, exp1)

        # Changing allow_local_infile_in_path to disallow upload
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                cnx.set_allow_local_infile_in_path("")
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_fails(cur, data_file, "file request rejected")

        # Changing disabled allow_local_infile_in_path to allow upload
        settings["allow_local_infile_in_path"] = None
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                cnx.set_allow_local_infile_in_path("")
                data_file = os.path.join("tests", "data", "local_data.csv")
                await verify_load_fails(
                    cur, data_file, ("file request rejected", "command is not allowed")
                )

        # relative path that results outside of infile_in_path
        settings["allow_local_infile_in_path"] = os.path.join(
            "tests", "data", "in_file_path"
        )
        async with connector_class(**settings) as cnx:
            async with await cnx.cursor() as cur:
                data_file = os.path.join(
                    "tests", "data", "in_file_path", "..", "local_data.csv"
                )
                await verify_load_fails(
                    cur,
                    data_file,
                    ("file request rejected", "not found", "command is not allowed"),
                )

        # Using a file instead of a directory
        settings = def_settings.copy()
        settings["allow_local_infile"] = False
        settings["allow_local_infile_in_path"] = os.path.join(
            "tests", "data", "local_data.csv"
        )
        with self.assertRaises(AttributeError):
            cnx = connector_class(**settings)
            await cnx.connect()

        if os.name != "nt":
            # Using a Symlink in allow_local_infile_in_path is forbiden
            target = os.path.abspath(os.path.join("tests", "data"))
            link = os.path.join(os.path.abspath("tests"), "data_sl")
            os.symlink(target, link)
            settings = def_settings.copy()
            settings["allow_local_infile"] = False
            settings["allow_local_infile_in_path"] = link
            with self.assertRaises(AttributeError):
                cnx = connector_class(**settings)
                await cnx.close()
            try:
                os.remove(link)
            except FileNotFoundError:
                pass

        if os.name != "nt" and connector_class != CMySQLConnection:
            # Load from a Symlink is not allowed
            data_dir = os.path.abspath(os.path.join("tests", "data"))
            target = os.path.abspath(os.path.join(data_dir, "local_data.csv"))
            link = os.path.join(data_dir, "local_data_sl.csv")
            os.symlink(target, link)
            settings = def_settings.copy()
            settings["allow_local_infile"] = False
            settings["allow_local_infile_in_path"] = data_dir
            async with connector_class(**settings) as cnx:
                async with await cnx.cursor() as cur:
                    await verify_load_fails(
                        cur, link, "link is not allowed", errors.OperationalError
                    )
            try:
                os.remove(link)
            except FileNotFoundError:
                pass

        # Clean up
        await def_cur.execute(f"DROP TABLE IF EXISTS {database}.local_data_in_path")
        await def_cur.close()
