/*
 * Copyright (c) 2014, 2017, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0, as
 * published by the Free Software Foundation.
 *
 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an
 * additional permission to link the program and your derivative works
 * with the separately licensed software that they have included with
 * MySQL.
 *
 * Without limiting anything contained in the foregoing, this file,
 * which is part of MySQL Connector/Python, is also subject to the
 * Universal FOSS Exception, version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

#include <Python.h>

#ifdef MS_WINDOWS
#include <windows.h>
#endif
#include <mysql.h>

#include "catch23.h"
#include "exceptions.h"
#include "mysql_connector.h"

extern PyObject *MySQLError;
extern PyObject *MySQLInterfaceError;

/**
  Set the error indicator using a MySQL session.

  Set the error indicator using the MySQL error returned
  by the MySQL server.

  When exc_type is NULL, MySQLInterfaceError will be
  used.

  @param    conn        MYSQL connection handle
  @param    exc_type    Python exception to raise
*/
void
raise_with_session(MYSQL *conn, PyObject *exc_type)
{
	PyObject *err_object= NULL;
	PyObject *error_msg, *error_no, *sqlstate;
	int err= 0;

    // default exception
	if (!exc_type)
	{
	    exc_type= MySQLInterfaceError;
	}

    Py_BEGIN_ALLOW_THREADS
	err= mysql_errno(conn);
    Py_END_ALLOW_THREADS

	if (!err)
	{
		error_msg= PyString_FromString("MySQL server has gone away");
        error_no= PyInt_FromLong(2006);
        sqlstate= PyString_FromString("HY000");
	}
	else
	{
		error_msg= PyString_FromString(mysql_error(conn));
        error_no= PyInt_FromLong(err);
        sqlstate= PyString_FromString(mysql_sqlstate(conn));
    }

	err_object= PyObject_CallFunctionObjArgs(exc_type, error_msg, NULL);
	if (!err_object)
	{
        goto ERR;
    }

    PyObject_SetAttrString(err_object, "sqlstate", sqlstate);
    PyObject_SetAttrString(err_object, "errno", error_no);
    PyObject_SetAttrString(err_object, "msg", error_msg);

    PyErr_SetObject(exc_type, err_object);
    goto CLEANUP;

    ERR:
        PyErr_SetObject(PyExc_RuntimeError,
            PyString_FromString("Failed raising error."));
        goto CLEANUP;
    CLEANUP:
        Py_XDECREF(err_object);
        Py_XDECREF(error_msg);
        Py_XDECREF(error_no);
        Py_XDECREF(sqlstate);
}

/**
  Set the error indicator using a prepare statement.

  Set the error indicator using the MySQL error returned
  when using prepared statements.

  When exc_type is NULL, MySQLInterfaceError will be
  used.

  @param    stmt        pointer to MYSQL_STMT
  @param    exc_type    Python exception to raise
*/
void
raise_with_stmt(MYSQL_STMT *stmt, PyObject *exc_type)
{
	PyObject *err_object= NULL;
	PyObject *error_msg, *error_no, *sqlstate;
	int err= 0;

    // default exception
	if (!exc_type)
	{
	    exc_type = MySQLInterfaceError;
	}

    Py_BEGIN_ALLOW_THREADS
	err= mysql_stmt_errno(stmt);
    Py_END_ALLOW_THREADS

	if (!err)
	{
		error_msg= PyString_FromString("MySQL server has gone away");
        error_no= PyInt_FromLong(2006);
        sqlstate= PyString_FromString("HY000");
	}
	else
	{
		error_msg= PyString_FromString(mysql_stmt_error(stmt));
        error_no= PyInt_FromLong(err);
        sqlstate= PyString_FromString(mysql_stmt_sqlstate(stmt));
    }

	err_object= PyObject_CallFunctionObjArgs(exc_type, error_msg, NULL);
	if (!err_object)
	{
        goto ERR;
    }

    PyObject_SetAttrString(err_object, "sqlstate", sqlstate);
    PyObject_SetAttrString(err_object, "errno", error_no);
    PyObject_SetAttrString(err_object, "msg", error_msg);

    PyErr_SetObject(exc_type, err_object);
    goto CLEANUP;

    ERR:
        PyErr_SetObject(PyExc_RuntimeError,
            PyString_FromString("Failed raising error."));
        goto CLEANUP;
    CLEANUP:
        Py_XDECREF(err_object);
        Py_XDECREF(error_msg);
        Py_XDECREF(error_no);
        Py_XDECREF(sqlstate);
}

/**
  Set the error indicator using a string.

  Set the error indicator using the given string.

  When exc_type is NULL, MySQLInterfaceError will be
  used.

  @param    error_msg   message for the exception
  @param    exc_type    Python exception to raise
*/
void
raise_with_string(PyObject *error_msg, PyObject *exc_type)
{
   	PyObject *err_object= NULL;
	PyObject *error_no= PyInt_FromLong(-1);

    // default exception
	if (!exc_type)
	{
	    exc_type= MySQLInterfaceError;
	}

	err_object= PyObject_CallFunctionObjArgs(exc_type, error_msg, NULL);
	if (!err_object)
	{
        goto ERR;
    }
    PyObject_SetAttrString(err_object, "sqlstate", Py_None);
    PyObject_SetAttrString(err_object, "errno", error_no);
    PyObject_SetAttrString(err_object, "msg", error_msg);

    PyErr_SetObject(exc_type, err_object);
    goto CLEANUP;

    ERR:
        PyErr_SetObject(PyExc_RuntimeError,
            PyString_FromString("Failed raising error."));
        goto CLEANUP;
    CLEANUP:
        Py_XDECREF(err_object);
        Py_XDECREF(error_no);
}
