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

DROP PROCEDURE IF EXISTS myconnpy.mirror_proc;
DROP PROCEDURE IF EXISTS myconnpy.twice_proc;
DROP PROCEDURE IF EXISTS myconnpy.sample_proc_untyped;

DELIMITER @!?
DROP TABLE IF EXISTS `delimiter`@!?DROP FUNCTION IF EXISTS hello@!?

CREATE TABLE `delimiter` (begin INT, end INT)@!? # end of line comment

DELIMITER //

CREATE PROCEDURE myconnpy.mirror_proc(INOUT channel CHAR(4), INOUT `delimiter` INT)
BEGIN
    SELECT REVERSE(channel) INTO channel;
# Add GEOMETRY column for MySQL 5.7.5 and higher
#Also include SRID attribute for MySQL 8.0.3 and higher
/*hey location GEOMETRY */ /*:D SRID 0 */ /*this is a #comment --,*/
    BEGIN
        SELECT 'hello' as col1, '"hello"' as col2, '""hello""' as col3, 'hel''lo' as col4, '\'hello' as col5;
        SELECT '"' as res1, '\'' as res2, "\"" as res3;
    END;
    -- another comment
END//-- comments everywhere


CREATE PROCEDURE myconnpy.twice_proc (IN number INT, OUT `DELIMITER` FLOAT, OUT number_twice INT)
BEGIN
    SELECT number*2 INTO number_twice;
    SELECT "DELIMITER ?" as myres1;
    SELECT '//' as myres2;
    SELECT "//" as myres3;
END//


CREATE PROCEDURE myconnpy.sample_proc_untyped(
    IN arg1 CHAR(5), INOUT arg2 CHAR(5), OUT arg3 FLOAT
)
BEGIN
    SELECT "foo" as name, 42 as age;
    SELECT "bar''`" as something;
    CALL mirror_proc(arg2);
END//DELIMITER ;

SET @x = 0; SET @y = 0;
CALL myconnpy.twice_proc(13, @x, @y);
SELECT /*+ BKA(t1) NO_BKA(t2) */ @y as select_y;

SET @x = 'roma';
CALL mirror_proc(@x, @y);

--               
SELECT @x as select_x;
DELIMITER $$
CREATE FUNCTION hello (s CHAR(20))
RETURNS CHAR(50) DETERMINISTIC
RETURN CONCAT('Hello, ',s,'!')$$

DELIMITER ^^ CALL mirror_proc(@x, @y) ^^

DELIMITER ;
DELIMITER ^
DELIMITER //