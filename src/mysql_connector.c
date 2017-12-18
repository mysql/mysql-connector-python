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

#include <stdio.h>
#include <stdlib.h>

#include <Python.h>
#include "datetime.h"
#include "structmember.h"

#ifdef MS_WINDOWS
#include <windows.h>
#endif
#include <mysql.h>

#include "catch23.h"
#include "exceptions.h"
#include "mysql_connector.h"
#include "mysql_capi.h"
#include "mysql_capi_conversion.h"

#ifdef PY3
    #define MODULE_SUCCESS_VALUE(val) val
    #define MODULE_ERROR_VALUE NULL
    #define MODULE_DEF(ob, name, methods, doc) \
        {static struct PyModuleDef moduledef = { \
            PyModuleDef_HEAD_INIT, \
            name, \
            doc, \
            -1, \
            methods, \
            NULL, NULL, NULL, NULL, \
        }; \
        ob = PyModule_Create(&moduledef); }
    #define MODULE_INIT PyMODINIT_FUNC PyInit__mysql_connector(void)
#else
    #define MODULE_SUCCESS_VALUE(val)
    #define MODULE_ERROR_VALUE
    #define MODULE_DEF(ob, name, methods, doc) \
        ob = Py_InitModule3(name, methods, doc);
    #define MODULE_INIT PyMODINIT_FUNC init_mysql_connector(void)
#endif


PyObject *MySQLError;
PyObject *MySQLInterfaceError;

/*
 * class _mysql_connector.MySQL
 */

static PyMemberDef MySQL_members[]=
{
    {"have_result_set", T_OBJECT, offsetof(MySQL, have_result_set), 0,
     "True if current session has result set"},
    {NULL}  /* Sentinel */
};

static PyMethodDef MySQL_methods[]=
{
    {"affected_rows", (PyCFunction)MySQL_affected_rows,
     METH_NOARGS,
	 "Returns num of rows changed by the last statement"},
    {"autocommit", (PyCFunction)MySQL_autocommit,
     METH_O,
     "Set autocommit mode"},

    {"buffered", (PyCFunction)MySQL_buffered,
     METH_VARARGS,
     "Set and get current setting of buffered"},

    {"change_user", (PyCFunction)MySQL_change_user,
     METH_VARARGS | METH_KEYWORDS,
     "Changes the user and causes db to become the default"},
    {"connect", (PyCFunction)MySQL_connect,
     METH_VARARGS | METH_KEYWORDS,
     "Connect with a MySQL server"},
    {"consume_result", (PyCFunction)MySQL_consume_result,
     METH_NOARGS,
	 "Consumes the result by reading all rows"},
    {"convert_to_mysql", (PyCFunction)MySQL_convert_to_mysql,
     METH_VARARGS,
	 "Convert Python objects to MySQL values"},
    {"close", (PyCFunction)MySQL_close,
     METH_NOARGS,
     "Closes an open connection."},
    {"character_set_name", (PyCFunction)MySQL_character_set_name,
     METH_NOARGS,
     "Returns the default character set name for the current connection"},
    {"commit", (PyCFunction)MySQL_commit,
     METH_NOARGS,
	 "Commits the current transaction"},
    {"connected", (PyCFunction)MySQL_connected,
     METH_NOARGS,
	 "Returns True when connected; False otherwise"},

    {"escape_string", (PyCFunction)MySQL_escape_string,
     METH_O,
	 "Create a legal SQL string that you can use in an SQL statement"},

    {"fetch_fields", (PyCFunction)MySQL_fetch_fields,
     METH_VARARGS | METH_KEYWORDS,
	 "Fetch information about fields in result set"},
    {"fetch_row", (PyCFunction)MySQL_fetch_row,
     METH_VARARGS | METH_KEYWORDS,
	 "Fetch a row"},

    {"field_count", (PyCFunction)MySQL_field_count,
     METH_NOARGS,
	 "Returns number of columns for the most recent query"},
    {"free_result", (PyCFunction)MySQL_free_result,
     METH_NOARGS,
	 "Returns number of columns for the most recent query"},


    {"get_character_set_info", (PyCFunction)MySQL_get_character_set_info,
     METH_NOARGS,
     "Provides information about the default client character set"},
    {"get_client_info", (PyCFunction)MySQL_get_client_info,
     METH_NOARGS,
     "Returns a string that represents the client library version"},
    {"get_client_version", (PyCFunction)MySQL_get_client_version,
     METH_NOARGS,
     "Returns a tuple that represents the client library version"},
    {"get_host_info", (PyCFunction)MySQL_get_host_info,
     METH_NOARGS,
     "Returns a string describing the type of connection in use"},
    {"get_proto_info", (PyCFunction)MySQL_get_proto_info,
     METH_NOARGS,
     "Returns the protocol version used by current connection"},
    {"get_server_info", (PyCFunction)MySQL_get_server_info,
     METH_NOARGS,
     "Returns a string that represents the server version number"},
    {"get_server_version", (PyCFunction)MySQL_get_server_version,
     METH_NOARGS,
     "Returns the version number of the server as a tuple"},
    {"get_ssl_cipher", (PyCFunction)MySQL_get_ssl_cipher,
     METH_NOARGS,
     "Returns the SSL cipher used for the given connection"},

    {"hex_string", (PyCFunction)MySQL_hex_string,
     METH_O,
     "Encode string in hexadecimal format"},

	{"insert_id", (PyCFunction)MySQL_insert_id,
	 METH_VARARGS | METH_KEYWORDS,
	 "Returns the value generated for an AUTO_INCREMENT column"},

    {"more_results", (PyCFunction)MySQL_more_results,
     METH_NOARGS,
	 "Returns True if more results exists"},

    {"next_result", (PyCFunction)MySQL_next_result,
     METH_NOARGS,
	 "Reads next statement result and returns if more results are available"},
    {"num_fields", (PyCFunction)MySQL_num_fields,
     METH_NOARGS,
	 "Returns number of fields in result set"},
    {"num_rows", (PyCFunction)MySQL_num_rows,
     METH_NOARGS,
	 "Returns number of rows in result set"},

    {"ping", (PyCFunction)MySQL_ping,
     METH_NOARGS,
	 "Checks whether the connection to the server is working"},
    {"query", (PyCFunction)MySQL_query,
     METH_VARARGS | METH_KEYWORDS,
	 "Execute the SQL statement"},

    {"raw", (PyCFunction)MySQL_raw,
     METH_VARARGS,
     "Set and get current raw setting"},
    {"refresh", (PyCFunction)MySQL_refresh,
     METH_VARARGS,
     "Flush tables, caches or reset replication server info"},
    {"rollback", (PyCFunction)MySQL_rollback,
     METH_NOARGS,
	 "Rolls back the current transaction"},

    {"select_db", (PyCFunction)MySQL_select_db,
     METH_O,
     "Causes the database specified by db to become the default database"},
    {"set_character_set", (PyCFunction)MySQL_set_character_set,
     METH_VARARGS,
     "Set the default character set for the current connection"},
    {"shutdown", (PyCFunction)MySQL_shutdown,
     METH_VARARGS,
     "Ask MySQL server to shut down"},
    {"stat", (PyCFunction)MySQL_stat,
     METH_NOARGS,
     "Returns server information like uptime, running threads, .."},
    {"st_affected_rows", (PyCFunction)MySQL_st_affected_rows,
     METH_NOARGS,
	 "Returns affected rows"},
    {"st_client_flag", (PyCFunction)MySQL_st_client_flag,
     METH_NOARGS,
	 "Returns client flags for current session"},
    {"st_insert_id", (PyCFunction)MySQL_st_insert_id,
     METH_NOARGS,
	 "Returns insert ID"},
    {"st_field_count", (PyCFunction)MySQL_st_field_count,
     METH_NOARGS,
	 "Returns field count"},
    {"st_server_capabilities", (PyCFunction)MySQL_st_server_capabilities,
     METH_NOARGS,
	 "Returns server capabilities"},
    {"st_server_status", (PyCFunction)MySQL_st_server_status,
     METH_NOARGS,
	 "Returns server status flag"},
    {"st_warning_count", (PyCFunction)MySQL_st_warning_count,
     METH_NOARGS,
	 "Returns warning count"},

    {"thread_id", (PyCFunction)MySQL_thread_id,
     METH_NOARGS,
	 "Returns the thread ID of the current connection"},

    {"use_unicode", (PyCFunction)MySQL_use_unicode,
     METH_VARARGS,
     "Set and get current use_unicode setting"},

    {"warning_count", (PyCFunction)MySQL_warning_count,
     METH_NOARGS,
	 "Returns the number of errors, warnings, and notes"},

    {NULL}  /* Sentinel */
};

PyTypeObject MySQLType=
{
    PyVarObject_HEAD_INIT(NULL, 0)
    "_mysql_connector.MySQL",  /*tp_name*/
    sizeof(MySQL),             /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)MySQL_dealloc, /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,        /*tp_flags*/
    "MySQL objects",           /* tp_doc */
    0,		               	   /* tp_traverse */
    0,		               	   /* tp_clear */
    0,		                   /* tp_richcompare */
    0,		                   /* tp_weaklistoffset */
    0,		                   /* tp_iter */
    0,		                   /* tp_iternext */
    MySQL_methods,             /* tp_methods */
    MySQL_members,             /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)MySQL_init,      /* tp_init */
    0,                         /* tp_alloc */
    MySQL_new,                 /* tp_new */
};

static PyMethodDef module_methods[]=
{
    {"datetime_to_mysql", (PyCFunction)datetime_to_mysql,
     METH_O,
     "Convert a Python datetime.datetime to MySQL DATETIME"},
    {"time_to_mysql", (PyCFunction)time_to_mysql,
     METH_O,
     "Convert a Python datetime.time to MySQL TIME"},
    {"date_to_mysql", (PyCFunction)date_to_mysql,
     METH_O,
     "Convert a Python datetime.date to MySQL DATE"},
    {NULL}  /* Sentinel */
};


#ifndef PyMODINIT_FUNC	/* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif
MODULE_INIT
{
    PyObject* mod;

    if (PyType_Ready(&MySQLType) < 0)
    {
        return MODULE_ERROR_VALUE;
    }

    MODULE_DEF(mod, "_mysql_connector", module_methods,
               "Python C Extension using MySQL Connector/C");

    if (mod == NULL)
    {
        return MODULE_ERROR_VALUE;
    }

    MySQLError = PyErr_NewException("_mysql_connector.MySQLError",
                                    PyExc_Exception, NULL);
    Py_INCREF(MySQLError);
    PyModule_AddObject(mod, "MySQLError", MySQLError);

    MySQLInterfaceError = PyErr_NewException(
        "_mysql_connector.MySQLInterfaceError", MySQLError, NULL);
    Py_INCREF(MySQLInterfaceError);
    PyModule_AddObject(mod, "MySQLInterfaceError", MySQLInterfaceError);

    Py_INCREF(&MySQLType);
    PyModule_AddObject(mod, "MySQL",
                       (PyObject *)&MySQLType);

    return MODULE_SUCCESS_VALUE(mod);
}
