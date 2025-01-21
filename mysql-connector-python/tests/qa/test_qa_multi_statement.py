# Copyright (c) 2024, Oracle and/or its affiliates.
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

"""Multi Statement Tests."""


import os

from typing import Any, TypedDict, Union

import tests

from mysql.connector._scripting import MySQLScriptSplitter, split_multi_statement
from mysql.connector.aio import connect as aio_connect
from mysql.connector.errors import InterfaceError, ProgrammingError, NotSupportedError


class TestCaseItem(TypedDict):
    num_mappable_stmts: int
    num_single_stmts: int
    mapping: list[tuple[str, list]]  # 2-tuple -> statement, result set


QA_FOLDER = os.path.join("tests", "data", "qa")
TEST_FOLDER = os.path.abspath(os.path.join(QA_FOLDER, "multi_statement"))
TEST_CASES = {
    "script1.sql": TestCaseItem(
        num_mappable_stmts=6,
        num_single_stmts=18,
        mapping=[
            ("DROP PROCEDURE IF EXISTS myconnpy.mirror_proc", []),
            ("DROP PROCEDURE IF EXISTS myconnpy.twice_proc", []),
            ("DROP PROCEDURE IF EXISTS myconnpy.sample_proc_untyped", []),
            ("DROP TABLE IF EXISTS `delimiter`", []),
            ("DROP FUNCTION IF EXISTS hello", []),
            ("CREATE TABLE `delimiter` (begin INT, end INT)", []),
            (
                r"""
CREATE PROCEDURE myconnpy.mirror_proc(INOUT channel CHAR(4), INOUT `delimiter` INT)
BEGIN
    SELECT REVERSE(channel) INTO channel;
    BEGIN
        SELECT 'hello' as col1, '"hello"' as col2, '""hello""' as col3, 'hel''lo' as col4, '\'hello' as col5;
        SELECT '"' as res1, '\'' as res2, "\"" as res3;
    END;
END""",
                [],
            ),
            (
                """
CREATE PROCEDURE myconnpy.twice_proc (IN number INT, OUT `DELIMITER` FLOAT, OUT number_twice INT)
BEGIN
    SELECT number*2 INTO number_twice;
    SELECT "DELIMITER ?" as myres1;
    SELECT '//' as myres2;
    SELECT "//" as myres3;
END""",
                [],
            ),
            (
                """
CREATE PROCEDURE myconnpy.sample_proc_untyped(
    IN arg1 CHAR(5), INOUT arg2 CHAR(5), OUT arg3 FLOAT
)
BEGIN
    SELECT "foo" as name, 42 as age;
    SELECT "bar''`" as something;
    CALL mirror_proc(arg2);
END""",
                [],
            ),
            ("SET @x = 0", []),
            ("SET @y = 0", []),
            ("CALL myconnpy.twice_proc(13, @x, @y)", [("DELIMITER ?",)]),
            ("CALL myconnpy.twice_proc(13, @x, @y)", [("//",)]),
            ("CALL myconnpy.twice_proc(13, @x, @y)", [("//",)]),
            ("CALL myconnpy.twice_proc(13, @x, @y)", []),
            ("SELECT /*+ BKA(t1) NO_BKA(t2) */ @y as select_y", [(26,)]),
            ("SET @x = 'roma'", []),
            (
                "CALL mirror_proc(@x, @y)",
                [("hello", '"hello"', '""hello""', "hel'lo", "'hello")],
            ),
            ("CALL mirror_proc(@x, @y)", [('"', "'", '"')]),
            ("CALL mirror_proc(@x, @y)", []),
            ("SELECT @x as select_x", [("amor",)]),
            (
                """CREATE FUNCTION hello (s CHAR(20))
RETURNS CHAR(50) DETERMINISTIC
RETURN CONCAT('Hello, ',s,'!')""",
                [],
            ),
            (
                "CALL mirror_proc(@x, @y)",
                [("hello", '"hello"', '""hello""', "hel'lo", "'hello")],
            ),
            ("CALL mirror_proc(@x, @y)", [('"', "'", '"')]),
            ("CALL mirror_proc(@x, @y)", []),
        ],
    ),
    "script2.sql": TestCaseItem(
        num_mappable_stmts=6,
        num_single_stmts=31,
        mapping=[
            ("DROP PROCEDURE IF EXISTS wl16285_multi_read", []),
            ("DROP PROCEDURE IF EXISTS wl16285_multi_insert", []),
            ("DROP PROCEDURE IF EXISTS wl16285_single_read", []),
            ("DROP PROCEDURE IF EXISTS wl16285_read_and_insert", []),
            ("DROP PROCEDURE IF EXISTS wl16285_callproc", []),
            ("DROP TABLE IF EXISTS wl16285", []),
            (
                """CREATE PROCEDURE wl16285_single_read(val integer)
BEGIN
    SELECT val;
END""",
                [],
            ),
            (
                """CREATE PROCEDURE wl16285_multi_read(val integer)
BEGIN
    SELECT val;
    SELECT val + 1 as val_plus_one;
    SELECT 'bar';
END""",
                [],
            ),
            (
                """CREATE PROCEDURE wl16285_multi_insert()
BEGIN
    INSERT INTO wl16285 (city, country_id) VALUES ('Chiapas', '33');
    INSERT INTO wl16285 (city, country_id) VALUES ('Yucatan', '28');
    INSERT INTO wl16285 (city, country_id) VALUES ('Oaxaca', '13');
END""",
                [],
            ),
            (
                """CREATE PROCEDURE wl16285_read_and_insert()
BEGIN
    INSERT INTO wl16285 (city, country_id) VALUES ('CCC', '33');
    SELECT 'Oracle /* F1 */';
    INSERT INTO wl16285 (city, country_id) VALUES ('AAA', '44');
    INSERT INTO wl16285 (city, country_id) VALUES ('BBB', '99');
    SELECT 'MySQL';
END""",
                [],
            ),
            (
                """CREATE PROCEDURE wl16285_callproc()
BEGIN
    CALL wl16285_multi_read(1);
    CALL wl16285_multi_insert();
END""",
                [],
            ),
            ('SELECT "hello -- " as hello', [("hello -- ",)]),
            ("SELECT '-- hello' as hey", [("-- hello",)]),
            (
                """CREATE TABLE wl16285 (
    id INT AUTO_INCREMENT PRIMARY KEY, city VARCHAR(20),country_id INT
)""",
                [],
            ),
            ("select 2 as a_select", [(2,)]),
            ("SET @x = 13", []),
            ("SELECT @x as select_x", [(13,)]),
            ("DROP PROCEDURE IF EXISTS mirror_proc", []),
            ("SELECT 76 as another_select", [(76,)]),
            (
                "INSERT INTO wl16285 (city, country_id) VALUES ('#Ciudad de Mexico', '38')",
                [],
            ),
            ("call wl16285_multi_read(2)", [(2,)]),
            ("call wl16285_multi_read(2)", [(3,)]),
            ("call wl16285_multi_read(2)", [("bar",)]),
            ("call wl16285_multi_read(2)", []),
            ("call wl16285_multi_insert()", []),
            ("SET @x = 'blue'", []),
            ("SELECT @x as selectx", [("blue",)]),
            ("CALL wl16285_callproc()", [(1,)]),
            ("CALL wl16285_callproc()", [(2,)]),
            ("CALL wl16285_callproc()", [("bar",)]),
            ("CALL wl16285_callproc()", []),
            ("DROP PROCEDURE IF EXISTS wl16285_multi_read", []),
            ("DROP PROCEDURE IF EXISTS wl16285_multi_insert", []),
            ("DROP PROCEDURE IF EXISTS wl16285_single_read", []),
            ("DROP PROCEDURE IF EXISTS wl16285_read_and_insert", []),
            ("DROP PROCEDURE IF EXISTS wl16285_callproc", []),
            ("DROP TABLE IF EXISTS wl16285", []),
        ],
    ),
    "script3.sql": TestCaseItem(
        num_mappable_stmts=4,
        num_single_stmts=5,
        mapping=[
            ("DROP PROCEDURE IF EXISTS myconnpy.test", []),
            (
                """CREATE PROCEDURE myconnpy.test (IN max_i INT)
BEGIN
declare i int default 0;
    while i < max_i do
    SELECT i+1000 as i_1000;
    SET i=i+1;
end while;
END""",
                [],
            ),
            ("CALL myconnpy.test(2)", [(1000,)]),
            ("CALL myconnpy.test(2)", [(1001,)]),
            ("CALL myconnpy.test(2)", []),
            ("select 1 as one", [(1,)]),
            ("CALL myconnpy.test(4)", [(1000,)]),
            ("CALL myconnpy.test(4)", [(1001,)]),
            ("CALL myconnpy.test(4)", [(1002,)]),
            ("CALL myconnpy.test(4)", [(1003,)]),
            ("CALL myconnpy.test(4)", []),
        ],
    ),
    "script4.sql": TestCaseItem(
        num_mappable_stmts=9,
        num_single_stmts=14,
        mapping=[
            ("DROP PROCEDURE IF EXISTS sample_proc", []),
            ("DROP PROCEDURE IF EXISTS dorepeat", []),
            (
                """CREATE PROCEDURE sample_proc()
BEGIN
    SELECT "history"; SELECT "of mankind" as col;
END""",
                [],
            ),
            ("DROP PROCEDURE IF EXISTS sample_proc_2", []),
            ("CALL sample_proc()", [("history",)]),
            ("CALL sample_proc()", [("of mankind",)]),
            ("CALL sample_proc()", []),
            (
                """CREATE PROCEDURE dorepeat(p1 INT)
BEGIN
    SET @x = 0;
    REPEAT SET @x = @x + 1; UNTIL @x > p1 END REPEAT;
END""",
                [],
            ),
            ("CALL dorepeat(1000)", []),
            ("SELECT @x as var", [(1001,)]),
            ("CALL sample_proc()", [("history",)]),
            ("CALL sample_proc()", [("of mankind",)]),
            ("CALL sample_proc()", []),
            (
                "CREATE  PROCEDURE  sample_proc_2(IN `DELIMITER`    INT) SELECT 10 + `DELIMITER` as res",
                [],
            ),
            ("call  sample_proc_2(10)", [(20,)]),
            ("call  sample_proc_2(10)", []),
            ("DROP PROCEDURE IF EXISTS sample_proc", []),
            ("DROP PROCEDURE IF EXISTS dorepeat", []),
            ("DROP PROCEDURE IF EXISTS sample_proc_2", []),
        ],
    ),
    "script5.sql": TestCaseItem(
        num_mappable_stmts=1,
        num_single_stmts=17,
        mapping=[
            ("DROP PROCEDURE IF EXISTS sp1", []),
            ("DROP PROCEDURE IF EXISTS sp2", []),
            ("DROP PROCEDURE IF EXISTS begin", []),
            ("DROP TABLE IF EXISTS `delimiter`", []),
            ("DROP EVENT IF EXISTS my_event", []),
            (
                """CREATE PROCEDURE sp1(INOUT channel CHAR(4), INOUT `delimiter` INT)
BEGIN
    SELECT REVERSE(channel) INTO channel;
    BEGIN
        SELECT 10;
        SELECT 30;
    END;
    SELECT 'hello' as col1, '"hello"' as col2, '""hello""' as col3, 'hel''lo' as col4;

    BEGIN
    END;

    SELECT '"' as res;
    SET `delimiter` = 10;
    SET @begin = "x";
    SET @end = "y";
END""",
                [],
            ),
            ("CREATE TABLE `delimiter` (begin INT, end INT)", []),
            (
                """INSERT INTO `delimiter` (begin, end)
VALUES (1, 10), (2, 20), (3, 30)""",
                [],
            ),
            ("SELECT begin, end FROM `delimiter`", [(1, 10), (2, 20), (3, 30)]),
            (
                """CREATE PROCEDURE sp2(IN begin INT, IN end INT, OUT rowcount INT)
BEGIN
    INSERT INTO `delimiter` (begin, end)
    VALUES (begin, end);
    SELECT COUNT(*) FROM `delimiter` INTO rowcount;
    SELECT begin, end FROM `delimiter`;
END""",
                [],
            ),
            (
                """CREATE PROCEDURE begin(IN end INT)
BEGIN
    DECLARE v INT DEFAULT 1;

    CASE end
        WHEN 1 THEN
        BEGIN
            SELECT end;
            SELECT 30;
        END;
        WHEN 2 THEN SELECT v; SELECT 10; SELECT 20;
        WHEN 3 THEN SELECT 0;
        ELSE
        BEGIN
        END;
    END CASE;
END""",
                [],
            ),
            (
                """CREATE DEFINER = root EVENT my_event
ON SCHEDULE EVERY 1 HOUR
DO
BEGIN
    BEGIN
        SELECT 100;
        INSERT INTO totals VALUES (NOW());
    END;
END""",
                [],
            ),
            ("DROP PROCEDURE IF EXISTS sp1", []),
            ("DROP PROCEDURE IF EXISTS sp2", []),
            ("DROP PROCEDURE IF EXISTS begin", []),
            ("DROP TABLE IF EXISTS `delimiter`", []),
            ("DROP EVENT IF EXISTS my_event", []),
        ],
    ),
    "sakila-schema.sql": TestCaseItem(
        num_mappable_stmts=1,
        num_single_stmts=46,
        mapping=[],
    ),
    "sakila-data.sql": TestCaseItem(
        num_mappable_stmts=1,
        num_single_stmts=62,
        mapping=[],
    ),
}


class MySQLScriptSplitterUnitTests(tests.MySQLConnectorTests):
    """Unit Tests."""

    def test_split_script_with_qa_scripts(self):
        """Verify the qa scripts can be split."""
        for script_name, script_info in TEST_CASES.items():
            with open(os.path.join(TEST_FOLDER, script_name), encoding="utf-8") as code:
                tok = MySQLScriptSplitter(sql_script=code.read().encode())
            stmts = tok.split_script()
            self.assertEqual(script_info["num_single_stmts"], len(stmts))

    def test_unsupported_delimiter(self):
        """The backslash is an invalid delimiter.

        See last 2 paragraphs of
        https://dev.mysql.com/doc/refman/8.4/en/stored-programs-defining.html.
        """
        scripts = [
            r"""
            SELECT 1;
            DELIMITER \
            SELECT 'invaliad'\
            Dlimiter ;
            """,
            r"""
            DELIMITER blue\red
            SELECT 'foo'blue\red
            """,
        ]
        for code in scripts:
            with self.assertRaises(InterfaceError):
                tok = MySQLScriptSplitter(sql_script=code.encode())
                stmts = tok.split_script()

    def test_split_script_with_weird_scripts(self):
        """Try some unorthodox sample scripts."""
        scripts = [
            (
                r"""
            SELECT 1;
            DELIMITER !!
            SELECT '"', '\'', "\"", '\'hello'!!
            DeLImITER end-of-line
            CREATE PROCEDURE myconnpy.mirror_proc(INOUT channel CHAR(4), INOUT `delimiter` INT)
            BEGIN
                SELECT REVERSE(channel) INTO channel;
            ENDend-of-line
            """,
                3,
            ),
            (
                r"""
            DELIMITER &*(
            CREATE PROCEDURE myconnpy.mirror_proc(INOUT channel CHAR(4), INOUT `delimiter` INT)
            BEGIN
                SELECT REVERSE(channel) INTO channel;
            END&*(delimiTer ; SELECT 'foo';
            """,
                2,
            ),
        ]
        for code, exp_num_single_stmts in scripts:
            tok = MySQLScriptSplitter(sql_script=code.encode())
            stmts = tok.split_script()
            self.assertEqual(exp_num_single_stmts, len(stmts))

    def test_split_script_with_fails_with_forbidden_delimiters(self):
        """This is a limitation of the current implementation."""
        # Unexpected behavior might happen if your script includes the following
        # symbols as delimiters `"`, `'`, `#`, , `--`, `/*` and `*/`. The use of these
        # should be avoided for now.
        scripts = [
            (
                r"""
            SELECT 1;
            DELIMITER --
            SELECT 'Heraldo'--
            DeLImITER end-of-line
            CREATE PROCEDURE myconnpy.mirror_proc(INOUT channel CHAR(4), INOUT `delimiter` INT)
            BEGIN
                SELECT REVERSE(channel) INTO channel;
            ENDend-of-line
            """,
                [
                    b"SELECT 1",
                    b"SELECT 'Heraldo'",
                    (
                        b"CREATE PROCEDURE myconnpy.mirror_proc(INOUT channel CHAR(4), "
                        b"INOUT `delimiter` INT)\n            BEGIN\n                "
                        b"SELECT REVERSE(channel) INTO channel;\n            END"
                    ),
                ],
            ),
            (
                r"""
            DELIMITER #
            CREATE PROCEDURE myconnpy.mirror_proc(INOUT channel CHAR(4), INOUT `delimiter` INT)
            BEGIN
                SELECT REVERSE(channel) INTO channel;
            END# delimiTer ; SELECT 'foo';
            """,
                [
                    (
                        b"CREATE PROCEDURE myconnpy.mirror_proc(INOUT channel CHAR(4), "
                        b"INOUT `delimiter` INT)\n            BEGIN\n                "
                        b"SELECT REVERSE(channel) INTO channel;\n            END"
                    ),
                    b"SELECT 'foo'",
                ],
            ),
        ]
        for code, exp_single_stmts in scripts:
            tok = MySQLScriptSplitter(sql_script=code.encode())
            stmts = tok.split_script()
            self.assertNotEqual(exp_single_stmts, stmts)


class SplitMultiStatementUnitTests(tests.MySQLConnectorTests):
    """Integration Tests."""

    def test_behaves_as_generator_mapping_enabled(self):
        """Verify behavior correctness of `cursor.split_multi_statement`."""
        for script_name, script_info in TEST_CASES.items():
            with open(os.path.join(TEST_FOLDER, script_name), encoding="utf-8") as code:
                partition = split_multi_statement(
                    sql_code=code.read().encode(), map_results=True
                )
            cnt_map = cnt_stmts = 0
            while True:
                try:
                    item = next(partition)
                    cnt_map += 1
                    cnt_stmts += len(item["single_stmts"])
                except StopIteration:
                    self.assertEqual(script_info["num_mappable_stmts"], cnt_map)
                    self.assertEqual(script_info["num_single_stmts"], cnt_stmts)
                    break

    def test_behaves_as_generator_mapping_disabled(self):
        """Verify behavior correctness of `cursor.split_multi_statement`."""
        for script_name, script_info in TEST_CASES.items():
            with open(os.path.join(TEST_FOLDER, script_name), encoding="utf-8") as code:
                partition = split_multi_statement(
                    sql_code=code.read().encode(), map_results=False
                )
            cnt_map = cnt_stmts = 0
            while True:
                try:
                    item = next(partition)
                    cnt_map += 1
                    cnt_stmts += len(item["single_stmts"])
                except StopIteration:
                    self.assertEqual(1, cnt_map)
                    if script_name == "sakila-data.sql":
                        # `sakila-data.sql` does not utilize delimiters, hence
                        # the "script split" processing is bypassed.
                        # This means that `cnt_stmts` is expected to be zero.
                        self.assertEqual(0, cnt_stmts)
                    else:
                        self.assertEqual(script_info["num_single_stmts"], cnt_stmts)
                    break


class MySQLCursorTests(tests.MySQLConnectorTests):
    """Integration tests."""

    @staticmethod
    def _format_based_on_cursor_flavor(
        flavor: str, result_set: list[tuple]
    ) -> list[tuple]:
        if "raw" in flavor:
            return [
                tuple([str(item).encode("utf-8") for item in tup]) for tup in result_set
            ]
        return result_set

    @staticmethod
    def get_processed_script(raw_script: str) -> str:
        """Get the processed script."""
        partition = split_multi_statement(sql_code=raw_script.encode("utf-8"))
        return next(partition)["mappable_stmt"].strip().decode("utf-8")

    @staticmethod
    def verify_result_sets(
        test_obj: Union[tests.MySQLConnectorTests, tests.MySQLConnectorAioTestCase],
        script: str,
        script_info: TestCaseItem,
        map_results: bool,
        cur_flavor: str,
        cur_fetchsets: list[tuple],
    ):
        if not script_info["mapping"]:
            return

        for i, (statement, result_set) in enumerate(cur_fetchsets):
            exp_statement, exp_result_set = script_info["mapping"][i]

            exp_result_set = MySQLCursorTests._format_based_on_cursor_flavor(
                flavor=cur_flavor, result_set=exp_result_set
            )

            test_obj.assertEqual(
                (
                    exp_statement.strip()
                    if map_results
                    else MySQLCursorTests.get_processed_script(script)
                ),
                statement,
            )
            if "dict" in cur_flavor:
                test_obj.assertEqual(
                    [sorted(tup) for tup in exp_result_set],
                    [sorted(dict_.values()) for dict_ in result_set],
                )
            else:
                test_obj.assertEqual(exp_result_set, result_set)

    def _test_execute(self, map_results: bool, **cursor_config):
        for script_name, script_info in TEST_CASES.items():
            if script_name in ("sakila-schema.sql", "sakila-data.sql"):
                continue

            with open(os.path.join(TEST_FOLDER, script_name), encoding="utf-8") as code:
                script = code.read()

            with self.cnx.cursor(**cursor_config) as cur:
                flavor = cur.__class__.__name__.lower()

                # Multi statement is not supported for prepared statements
                if "prepared" in flavor:
                    with self.assertRaises((ProgrammingError, InterfaceError)):
                        cur.execute(script, map_results=map_results)
                    continue

                cur.execute(script, map_results=map_results)
                cur_fetchsets = list(cur.fetchsets())

            MySQLCursorTests.verify_result_sets(
                test_obj=self,
                script=script,
                script_info=script_info,
                map_results=map_results,
                cur_flavor=flavor,
                cur_fetchsets=cur_fetchsets,
            )

    def _test_execute_mapping_disabled(self, **cursor_config):
        self._test_execute(map_results=False, **cursor_config)

    def _test_execute_mapping_enabled(self, **cursor_config):
        self._test_execute(map_results=True, **cursor_config)

    def _test_rowcount(self, **cursor_config):
        scripts = [
            (
                """
                DROP TABLE IF EXISTS names;
                CREATE TABLE names (
                    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    name VARCHAR(30) DEFAULT '' NOT NULL,
                    info TEXT,
                    age TINYINT UNSIGNED DEFAULT '30',
                PRIMARY KEY (id));
                INSERT INTO names (name) VALUES ('Geert');
                SELECT COUNT(*) AS cnt FROM names;
                INSERT INTO names (name) VALUES ('Jan'),('Michel');
                SELECT name FROM names;
            """,
                [(False, 0), (False, 0), (False, 1), (True, 1), (False, 2), (True, 3)],
            )
        ]
        for script, exp_seq in scripts:
            with self.cnx.cursor(**cursor_config) as cur:
                cur.execute(script.encode())
                for (exp_with_rows, exp_rowcount), (statement, result_set) in zip(
                    exp_seq, cur.fetchsets()
                ):
                    self.assertEqual(exp_with_rows, cur.with_rows)
                    self.assertEqual(exp_rowcount, cur.rowcount)

    @tests.foreach_cnx()
    def test_sakila_database(self):
        """Verify `BUG#35810050 Executing multiple statements fails when importing Sakila
        database` has been fixed.

        See https://dev.mysql.com/doc/sakila/en/sakila-installation.html.
        """
        for script_name in ("sakila-schema.sql", "sakila-data.sql"):
            with open(os.path.join(TEST_FOLDER, script_name), encoding="utf-8") as code:
                script = code.read()

            with self.cnx.cursor() as cur:
                cur.execute(script.encode())
                self.assertEqual(
                    len([rs for _, rs in cur.fetchsets()]),
                    TEST_CASES[script_name]["num_single_stmts"],
                )

        with self.cnx.cursor() as cur:
            exp_fetchsets = [
                ("SET @@default_storage_engine = 'MyISAM'", []),
                ("/*!50610 SET @@default_storage_engine = 'InnoDB'*/", []),
                ("SELECT /*+ SEMIJOIN(FIRSTMATCH, LOOSESCAN) */ 3", [(3,)]),
            ]
            cur.execute(
                "SET @@default_storage_engine = 'MyISAM';\n"
                "/*!50610 SET @@default_storage_engine = 'InnoDB'*/;\n"
                "SELECT /*+ SEMIJOIN(FIRSTMATCH, LOOSESCAN) */ 3;",
                map_results=True,
            )
            i = 0
            for statement, result in cur.fetchsets():
                self.assertTupleEqual(exp_fetchsets[i], (statement, result))
                i += 1

            exp_fetchsets = exp_fetchsets + [
                ("SET @aux = 'sakila-test'", []),
                ("SELECT @aux", [("sakila-test",)]),
            ]
            cur.execute(
                "SET @@default_storage_engine = 'MyISAM';\n"
                "/*!50610 SET @@default_storage_engine = 'InnoDB'*/;\n"
                "SELECT /*+ SEMIJOIN(FIRSTMATCH, LOOSESCAN) */ 3;\n"
                "SET @aux = 'sakila-test'; SELECT @aux;",
                map_results=True,
            )
            i = 0
            for statement, result in cur.fetchsets():
                self.assertTupleEqual(exp_fetchsets[i], (statement, result))
                i += 1

    @tests.foreach_cnx()
    def test_empty_query(self):
        """If mapping or delimiter parsing is enabled, comments are ignored,
        therefore a script only including comments will end up being an empty
        multi statement as comments aren't statements."""
        scripts = [
            "-- comment;#comment;  \n-- comment",
            ";",
        ]
        for code in scripts:
            with self.cnx.cursor() as cur:
                with self.assertRaises(ProgrammingError):
                    cur.execute(code, map_results=True)

    @tests.foreach_cnx()
    def test_cursor_plain(self):
        kwargs = {}
        self._test_execute_mapping_disabled(**kwargs)
        self._test_execute_mapping_enabled(**kwargs)
        self._test_rowcount(**kwargs)

    @tests.foreach_cnx()
    def test_cursor_raw(self):
        kwargs = {"raw": True}
        self._test_execute_mapping_disabled(**kwargs)
        self._test_execute_mapping_enabled(**kwargs)
        self._test_rowcount(**kwargs)

    @tests.foreach_cnx()
    def test_cursor_dict(self):
        kwargs = {"dictionary": True}
        self._test_execute_mapping_disabled(**kwargs)
        self._test_execute_mapping_enabled(**kwargs)
        self._test_rowcount(**kwargs)

    @tests.foreach_cnx()
    def test_cursor_buffered(self):
        kwargs = {"buffered": True}
        self._test_execute_mapping_disabled(**kwargs)
        self._test_execute_mapping_enabled(**kwargs)
        self._test_rowcount(**kwargs)

    @tests.foreach_cnx()
    def test_cursor_named_tuple(self):
        kwargs = {"named_tuple": True}
        self._test_execute_mapping_disabled(**kwargs)
        self._test_execute_mapping_enabled(**kwargs)
        self._test_rowcount(**kwargs)

    @tests.foreach_cnx()
    def test_cursor_buffered_raw(self):
        kwargs = {"buffered": True, "raw": True}
        self._test_execute_mapping_disabled(**kwargs)
        self._test_execute_mapping_enabled(**kwargs)
        self._test_rowcount(**kwargs)

    @tests.foreach_cnx()
    def test_cursor_buffered_dictionary(self):
        kwargs = {"buffered": True, "dictionary": True}
        self._test_execute_mapping_disabled(**kwargs)
        self._test_execute_mapping_enabled(**kwargs)
        self._test_rowcount(**kwargs)

    @tests.foreach_cnx()
    def test_cursor_buffered_named_tuple(self):
        kwargs = {"buffered": True, "named_tuple": True}
        self._test_execute_mapping_disabled(**kwargs)
        self._test_execute_mapping_enabled(**kwargs)
        self._test_rowcount(**kwargs)

    @tests.foreach_cnx()
    def test_cursor_prepared(self):
        self._test_execute_mapping_disabled(prepared=True)
        self._test_execute_mapping_enabled(prepared=True)


class MySQLAsyncCursorTests(tests.MySQLConnectorAioTestCase):
    """Integration tests."""

    async def _test_execute(
        self, map_results: bool, use_executemulti: bool = False, **cursor_config: Any
    ) -> None:
        for script_name, script_info in TEST_CASES.items():
            if script_name in ("sakila-schema.sql", "sakila-data.sql"):
                continue

            with open(os.path.join(TEST_FOLDER, script_name), encoding="utf-8") as code:
                script = code.read()

            async with await self.cnx.cursor(**cursor_config) as cur:
                flavor = cur.__class__.__name__.lower()

                # Multi statement is not supported for prepared statements
                if "prepared" in flavor:
                    exp_err = (
                        (NotSupportedError,)
                        if use_executemulti
                        else (ProgrammingError, InterfaceError)
                    )
                    with self.assertRaises(exp_err):
                        if not use_executemulti:
                            await cur.execute(script, map_results=map_results)
                        else:
                            await cur.executemulti(script, map_results=map_results)
                    continue

                if not use_executemulti:
                    await cur.execute(script, map_results=map_results)
                else:
                    with self.assertWarns(DeprecationWarning):
                        await cur.executemulti(script, map_results=map_results)

                cur_fetchsets = [x async for x in cur.fetchsets()]

            MySQLCursorTests.verify_result_sets(
                test_obj=self,
                script=script,
                script_info=script_info,
                map_results=map_results,
                cur_flavor=flavor,
                cur_fetchsets=cur_fetchsets,
            )

    async def _test_execute_mapping_disabled(self, **cursor_config):
        await self._test_execute(map_results=False, **cursor_config)
        await self._test_execute(
            map_results=False, use_executemulti=True, **cursor_config
        )

    async def _test_execute_mapping_enabled(self, **cursor_config):
        await self._test_execute(map_results=True, **cursor_config)
        await self._test_execute(
            map_results=True, use_executemulti=True, **cursor_config
        )

    # TODO: row count not working correctly - it's a bug and it's unrelated to WL#16285.
    # async def _test_rowcount(self, **cursor_config):
    #     ...

    @tests.foreach_cnx()
    async def test_sakila_database(self):
        """Verify `BUG#35810050 Executing multiple statements fails when importing Sakila
        database` has been fixed.

        See https://dev.mysql.com/doc/sakila/en/sakila-installation.html.
        """
        config = {**tests.get_mysql_config(), **{"use_pure": True}}
        for script_name in ("sakila-schema.sql", "sakila-data.sql"):
            with open(os.path.join(TEST_FOLDER, script_name), encoding="utf-8") as code:
                script = code.read()

            async with await aio_connect(**config) as cnx:
                async with await cnx.cursor() as cur:
                    await cur.execute(script.encode())
                    result_sets = [rs async for _, rs in cur.fetchsets()]

            self.assertEqual(
                len(result_sets),
                TEST_CASES[script_name]["num_single_stmts"],
            )

        async with await aio_connect(**config) as cnx:
            async with await cnx.cursor() as cur:
                exp_fetchsets = [
                    ("SET @@default_storage_engine = 'MyISAM'", []),
                    ("/*!50610 SET @@default_storage_engine = 'InnoDB'*/", []),
                    ("SELECT /*+ SEMIJOIN(FIRSTMATCH, LOOSESCAN) */ 3", [(3,)]),
                ]
                await cur.execute(
                    "SET @@default_storage_engine = 'MyISAM';\n"
                    "/*!50610 SET @@default_storage_engine = 'InnoDB'*/;\n"
                    "SELECT /*+ SEMIJOIN(FIRSTMATCH, LOOSESCAN) */ 3;",
                    map_results=True,
                )
                i = 0
                async for statement, result in cur.fetchsets():
                    self.assertTupleEqual(exp_fetchsets[i], (statement, result))
                    i += 1

                exp_fetchsets = exp_fetchsets + [
                    ("SET @aux = 'sakila-test'", []),
                    ("SELECT @aux", [("sakila-test",)]),
                ]
                await cur.execute(
                    "SET @@default_storage_engine = 'MyISAM';\n"
                    "/*!50610 SET @@default_storage_engine = 'InnoDB'*/;\n"
                    "SELECT /*+ SEMIJOIN(FIRSTMATCH, LOOSESCAN) */ 3;\n"
                    "SET @aux = 'sakila-test'; SELECT @aux;",
                    map_results=True,
                )
                i = 0
                async for statement, result in cur.fetchsets():
                    self.assertTupleEqual(exp_fetchsets[i], (statement, result))
                    i += 1

    @tests.foreach_cnx_aio()
    async def test_empty_query(self):
        scripts = [
            "-- comment;#comment;  \n-- comment",
            ";",
        ]
        for code in scripts:
            async with await self.cnx.cursor() as cur:
                with self.assertRaises(ProgrammingError):
                    await cur.execute(code, map_results=True)

    @tests.foreach_cnx_aio()
    async def test_cursor_plain(self):
        kwargs = {}
        await self._test_execute_mapping_disabled(**kwargs)
        await self._test_execute_mapping_enabled(**kwargs)

    @tests.foreach_cnx_aio()
    async def test_cursor_raw(self):
        kwargs = {"raw": True}
        await self._test_execute_mapping_disabled(**kwargs)
        await self._test_execute_mapping_enabled(**kwargs)

    @tests.foreach_cnx_aio()
    async def test_cursor_dict(self):
        kwargs = {"dictionary": True}
        await self._test_execute_mapping_disabled(**kwargs)
        await self._test_execute_mapping_enabled(**kwargs)

    @tests.foreach_cnx_aio()
    async def test_cursor_buffered(self):
        kwargs = {"buffered": True}
        await self._test_execute_mapping_disabled(**kwargs)
        await self._test_execute_mapping_enabled(**kwargs)

    @tests.foreach_cnx_aio()
    async def test_cursor_named_tuple(self):
        kwargs = {"named_tuple": True}
        await self._test_execute_mapping_disabled(**kwargs)
        await self._test_execute_mapping_enabled(**kwargs)

    @tests.foreach_cnx_aio()
    async def test_cursor_buffered_raw(self):
        kwargs = {"buffered": True, "raw": True}
        await self._test_execute_mapping_disabled(**kwargs)
        await self._test_execute_mapping_enabled(**kwargs)

    @tests.foreach_cnx_aio()
    async def test_cursor_buffered_dictionary(self):
        kwargs = {"buffered": True, "dictionary": True}
        await self._test_execute_mapping_disabled(**kwargs)
        await self._test_execute_mapping_enabled(**kwargs)

    @tests.foreach_cnx_aio()
    async def test_cursor_buffered_named_tuple(self):
        kwargs = {"buffered": True, "named_tuple": True}
        await self._test_execute_mapping_disabled(**kwargs)
        await self._test_execute_mapping_enabled(**kwargs)

    @tests.foreach_cnx_aio()
    async def test_cursor_prepared(self):
        kwargs = {"prepared": True}
        await self._test_execute_mapping_disabled(**kwargs)
        await self._test_execute_mapping_enabled(**kwargs)
