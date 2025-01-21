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

DELIMITER %%
DROP PROCEDURE IF EXISTS sample_proc%%
DROP PROCEDURE IF EXISTS dorepeat%%

CREATE PROCEDURE sample_proc()
BEGIN
    SELECT "history"; SELECT "of mankind" as col;
END%%

DROP PROCEDURE IF EXISTS sample_proc_2%% CALL sample_proc()%% delimiter @@

CREATE PROCEDURE dorepeat(p1 INT)
BEGIN
    SET @x = 0;
    REPEAT SET @x = @x + 1; UNTIL @x > p1 END REPEAT;
END@@

delimiter         _end

#
CALL dorepeat(1000)_end SELECT @x as var_end CALL sample_proc()_end

-- When declaring procedures with no begin-end block, there's actually
-- no need to use a custom delimiter.
--
Delimiter   $$
CREATE  PROCEDURE  sample_proc_2(IN `DELIMITER`    INT) SELECT 10 + `DELIMITER` as res $$

DelimiTer  ; call  sample_proc_2(10) /* ///* */;

DROP PROCEDURE IF EXISTS sample_proc;
DROP PROCEDURE IF EXISTS dorepeat;
DROP PROCEDURE IF EXISTS sample_proc_2;