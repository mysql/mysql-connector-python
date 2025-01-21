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

DROP PROCEDURE IF EXISTS sp1;
DROP PROCEDURE IF EXISTS sp2;
DROP PROCEDURE IF EXISTS begin;
DROP TABLE IF EXISTS `delimiter`;
DROP EVENT IF EXISTS my_event;

DELIMITER ^_^

CREATE PROCEDURE sp1(INOUT channel CHAR(4), INOUT `delimiter` INT)
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
END ^_^

DELIMITER ;

CREATE TABLE `delimiter` (begin INT, end INT);
INSERT INTO `delimiter` (begin, end)
VALUES (1, 10), (2, 20), (3, 30);

SELECT begin, end FROM `delimiter`;

DELIMITER =+-

CREATE PROCEDURE sp2(IN begin INT, IN end INT, OUT rowcount INT)
BEGIN
    INSERT INTO `delimiter` (begin, end)
    VALUES (begin, end);
    SELECT COUNT(*) FROM `delimiter` INTO rowcount;
    SELECT begin, end FROM `delimiter`;
END =+-

CREATE PROCEDURE begin(IN end INT)
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
END=+-

CREATE DEFINER = root EVENT my_event
ON SCHEDULE EVERY 1 HOUR
DO
BEGIN
    BEGIN
        SELECT 100;
        INSERT INTO totals VALUES (NOW());
    END;
END=+-

-- clean
delimiter ; DROP PROCEDURE IF EXISTS sp1; DROP PROCEDURE IF EXISTS sp2;
DROP PROCEDURE IF EXISTS begin; DROP TABLE IF EXISTS `delimiter`;
    DROP EVENT IF EXISTS my_event;