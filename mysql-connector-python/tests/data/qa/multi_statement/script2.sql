/*
Copyright (c) 2024, Oracle and/or its affiliates.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License, version 2.0, as
published by the Free Software Foundation.

This program is designed to work with certain software (including
but not limited to OpenSSL) that is licensed under separate terms,
as designated in a particular file or component or in included license
documentation. The authors of MySQL hereby grant you an
additional permission to link the program and your derivative works
with the separately licensed software that they have either included with
the program or referenced in the documentation.

Without limiting anything contained in the foregoing, this file,
which is part of MySQL Connector/Python, is also subject to the
Universal FOSS Exception, version 1.0, a copy of which can be found at
http://oss.oracle.com/licenses/universal-foss-exception.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU General Public License, version 2.0, for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/
-- Nested comments are not supported, and are deprecated;
-- expect them to be removed in a future MySQL release.
-- (Under some conditions, nested comments might be permitted,
-- but usually are not, and users should avoid them.)
--
-- "MySQL Server supports certain "`'variants of C-style comments.
-- These enable -- you to write code that includes MySQL extensions,
-- but is still portable, by using comments of the following form:

DROP PROCEDURE IF EXISTS wl16285_multi_read;
DROP PROCEDURE IF EXISTS wl16285_multi_insert;
DROP PROCEDURE IF EXISTS wl16285_single_read;
DROP PROCEDURE IF EXISTS wl16285_read_and_insert;
DROP PROCEDURE IF EXISTS wl16285_callproc;
DROP TABLE IF EXISTS wl16285;

DELIMITER **

--
-- Procedure structure for dummy `tests`
--
CREATE PROCEDURE wl16285_single_read(val integer)
BEGIN
    SELECT val;
END**

#Adding a comment
#
CREATE PROCEDURE wl16285_multi_read(val integer)
BEGIN
    SELECT val;
    SELECT val + 1 as val_plus_one;
    SELECT 'bar';
END**

CREATE PROCEDURE wl16285_multi_insert()
BEGIN
    INSERT INTO wl16285 (city, country_id) VALUES ('Chiapas', '33');
    INSERT INTO wl16285 (city, country_id) VALUES ('Yucatan', '28');
    INSERT INTO wl16285 (city, country_id) VALUES ('Oaxaca', '13');
END**

CREATE PROCEDURE wl16285_read_and_insert()
BEGIN
    INSERT INTO wl16285 (city, country_id) VALUES ('CCC', '33');
    SELECT 'Oracle /* F1 */';
    -- Add GEOMETRY column for MySQL 5.7.5 and higher
    -- Also include SRID attribute for MySQL 8.0.3 and higher
    INSERT INTO wl16285 (city, country_id) VALUES ('AAA', '44');
    INSERT INTO wl16285 (city, country_id) VALUES ('BBB', '99');
    SELECT 'MySQL';
END**

CREATE PROCEDURE wl16285_callproc()
BEGIN
    CALL wl16285_multi_read(1);
    CALL wl16285_multi_insert();
END**

DELIMITER eol

SELECT "hello -- " as helloeol
SELECT '-- hello' as heyeol

CREATE TABLE wl16285 (
    id INT AUTO_INCREMENT PRIMARY KEY, city VARCHAR(20),country_id INT
)eol

delimiter $$
select 2 as a_select$$
delimiter *_*

SET @x = 13*_* SELECT @x as select_x*_*

DROP PROCEDURE IF EXISTS mirror_proc*_*

SELECT 76 as another_select*_*
INSERT INTO wl16285 (city, country_id) VALUES ('#Ciudad de Mexico', '38')*_*
call wl16285_multi_read(2)*_*
call wl16285_multi_insert()*_*

SET @x = 'blue'*_*SELECT @x as selectx*_*

CALL wl16285_callproc()*_*

DELIMITER ;

/*
Let's do some cleaning
*/
DROP PROCEDURE IF EXISTS wl16285_multi_read;
DROP PROCEDURE IF EXISTS wl16285_multi_insert;
DROP PROCEDURE IF EXISTS wl16285_single_read;
DROP PROCEDURE IF EXISTS wl16285_read_and_insert;
DROP PROCEDURE IF EXISTS wl16285_callproc;
DROP TABLE IF EXISTS wl16285;
