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
#include <time.h>

#include <Python.h>
#include <datetime.h>

#ifdef MS_WINDOWS
#include <windows.h>
#endif
#include <mysql.h>

#include "catch23.h"
#include "mysql_connector.h"
#include "exceptions.h"
#include "mysql_capi.h"
#include "mysql_capi_conversion.h"

extern PyObject *MySQLError;
extern PyObject *MySQLInterfaceError;

// Forward declarations
PyObject* MySQL_connected(MySQL *self);

// Handy Macros
#define CHECK_SESSION(session) if (session == NULL) {\
    raise_with_string(PyStringFromString("MySQL session not available."),\
        NULL); return 0; }

#define IS_CONNECTED(cnx) \
    if ((PyObject*)MySQL_connected(cnx) == Py_False) {\
    raise_with_session(&cnx->session, MySQLInterfaceError); return 0; }

// Constants and defaults
#define CONNECTION_TIMEOUT 13
#define VERSION_OFFSET_MAJOR 10000
#define VERSION_OFFSET_MINOR 100

/**
  Helper function printing a string as hexadecimal.
*/
void
print_buffer(const char *buffer, unsigned long size)
{
    unsigned int i;
    for (i=0; i < size; i++)
    {
        printf("%02x ", (unsigned char)buffer[i]);
    }
    printf("\n");
}

/**
  Convert an Python object to a bytes type.

  Convert the given PyObject using the given character set
  to either a PyString_Type or PyBytes_Type for Python v3.

  The PyObject value must be either a PyUnicode_Type or
  PyBytes_Type, or PyString_Type for Python v2.

  @param    charset     name of the character set to use
  @param    value       PyObject to convert

  @return   PyObject
    @retval PyObject    OK
    @retval NULL        Exception
*/
PyObject*
str_to_bytes(const char* charset, PyObject *value)
{
    PyObject *bytes;

    // Unicode strings for Python v2 and v3
    if (PyUnicode_Check(value))
    {
        bytes = PyUnicode_AsEncodedString(value, charset, NULL);
        if (!bytes)
        {
            return NULL;
        }
        return bytes;
    }
    else
#ifndef PY3
    // Python v2 strings, they are expected to be encoded if they were
    // unicode
    if (PyString_Check(value))
    {
        return value;
    }
    else
#else
    // Python v3 bytes
    if (PyBytes_Check(value))
    {
        return value;
    }
    else
#endif
    {
        PyErr_SetString(PyExc_TypeError, "Argument must be str or bytes");
        return NULL;
    }

    return NULL;
}

/**
  Get Python character name based on MySQL character name
 */
static char*
python_characterset_name(const char* mysql_name)
{
    if (!mysql_name) {
        return "latin1"; // MySQL default
    }

    if (strcmp(mysql_name, "utf8mb4") == 0) {
        return "utf8";
    }

    return (char*)mysql_name;
}

/**
  Get the character set name from the current MySQL session.

  Get the character set name from the current MySQL session.
  Some MySQL character sets have no equivalent names in
  Python. When this is the case, a name for usable by Python
  will be returned.
  For example, 'utf8mb4' MySQL character set name will be
  returned as 'utf8'.

  @param    session     MySQL database connection

  @return   Character set name
    @retval const char* OK
    @retval NULL        Exception
*/
static const char*
my2py_charset_name(MYSQL *session)
{
    const char *name;

    if (!session)
    {
        return NULL;
    }

    name= mysql_character_set_name(session);
    return python_characterset_name(name);
}

/**
  Fetch column information using the MySQL result.

  Fetch the column information using the given MySQL result
  and the number of fields.
  The returned PyObject is a PyList which consists of
  PyTuple objects.

  @param    result      a MySQL result
  @param    num_fields  number of fields/columns

  @return   PyList of PyTuple objects
    @retval PyList  OK
    @retval NULL    Exception
*/
static PyObject*
fetch_fields(MYSQL_RES *result, unsigned int num_fields, MY_CHARSET_INFO *cs,
             unsigned int use_unicode)
{
    PyObject *fields= NULL;
    PyObject *field= NULL;
    PyObject *decoded= NULL;
    MYSQL_FIELD *myfs;
    unsigned int i;
    char *charset= python_characterset_name(cs->csname);

    fields = PyList_New(0);

    if (!result)
    {
        Py_RETURN_NONE;
    }

    Py_BEGIN_ALLOW_THREADS
    myfs = mysql_fetch_fields(result);
    Py_END_ALLOW_THREADS

    for (i = 0; i < num_fields; i++)
    {
        field = PyTuple_New(11);

        decoded= mytopy_string(myfs[i].catalog, myfs[i].catalog_length,
                               myfs[i].flags, charset, use_unicode);
        if (NULL == decoded) return NULL; // decode error
        PyTuple_SET_ITEM(field, 0, decoded);

        decoded= mytopy_string(myfs[i].db, myfs[i].db_length,
                               myfs[i].flags, charset, use_unicode);
        if (NULL == decoded) return NULL; // decode error
        PyTuple_SET_ITEM(field, 1, decoded);

        decoded= mytopy_string(myfs[i].table, myfs[i].table_length,
                               myfs[i].flags, charset, use_unicode);
        if (NULL == decoded) return NULL; // decode error
        PyTuple_SET_ITEM(field, 2, decoded);

        decoded= mytopy_string(myfs[i].org_table, myfs[i].org_table_length,
                               myfs[i].flags, charset, use_unicode);
        if (NULL == decoded) return NULL; // decode error
        PyTuple_SET_ITEM(field, 3, decoded);

        decoded= mytopy_string(myfs[i].name, myfs[i].name_length,
                               myfs[i].flags, charset, use_unicode);
        if (NULL == decoded) return NULL; // decode error
        PyTuple_SET_ITEM(field, 4, decoded);

        decoded= mytopy_string(myfs[i].org_name, myfs[i].org_name_length,
                               myfs[i].flags, charset, use_unicode);
        if (NULL == decoded) return NULL; // decode error
        PyTuple_SET_ITEM(field, 5, decoded);

    	PyTuple_SET_ITEM(field, 6, PyInt_FromLong(myfs[i].charsetnr));
    	PyTuple_SET_ITEM(field, 7, PyInt_FromLong(myfs[i].max_length));
    	PyTuple_SET_ITEM(field, 8, PyInt_FromLong(myfs[i].type));
    	PyTuple_SET_ITEM(field, 9, PyInt_FromLong(myfs[i].flags));
    	PyTuple_SET_ITEM(field, 10, PyInt_FromLong(myfs[i].decimals));
        PyList_Append(fields, field);
		Py_DECREF(field);
    }

    return fields;
}

/**
  MySQL instance destructor function.

  MySQL instance destructor freeing result (if any) and
  closing the connection.

  @param    self      MySQL instance
*/
void
MySQL_dealloc(MySQL *self)
{
    if (self) {
        MySQL_free_result(self);
        if (&self->session)
        {
            mysql_close(&self->session);
        }
        Py_TYPE(self)->tp_free((PyObject*)self);
	}
}

/**
  MySQL instance creation function.

  MySQL instance creation function. It allocates the new
  MySQL instance and sets default values private members.

  @param    type    type of object being created
  @param    args    positional arguments
  @param    kwargs  keyword arguments

  @return   Instance of MySQL
    @retval PyObject    OK
    @retval NULL        Exception
*/
PyObject *
MySQL_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
	MySQL *self;

	self = (MySQL *)type->tp_alloc(type, 0);

	if (self == NULL)
	{
		return NULL;
	}
	self->result_num_fields=    0;
	self->buffered=             Py_False;
	self->raw=                  Py_False;
	self->raw_as_string=        Py_False;
	self->buffered_at_connect=  Py_False;
	self->raw_at_connect=       Py_False;
	self->charset_name=         PyStringFromString("latin1");
	self->connected=            0;
	self->have_result_set=      Py_False;
	self->connection_timeout=   CONNECTION_TIMEOUT;
	self->result=               NULL;
	self->fields=               NULL;
	self->use_unicode=          1;
	self->auth_plugin=          PyStringFromString("mysql_native_password");

	return (PyObject *)self;
}

/**
  MySQL instance initialization function.

  MySQL instance initialization function. It handles the
  connection arguments passed as positional or keyword
  arguments.
  
  Not all connection arguments are used with the initialization
  function. List of arguments which can be used:
    buffered, raw, charset_name,
	connection_timeout, use_unicode,
	auth_plugin

  Other connection argument are used when actually connecting
  with the MySQL server using the MySQL_connect() function.

  @param    self    MySQL instance
  @param    args    positional arguments
  @param    kwargs  keyword arguments

  @return   Instance of MySQL
    @retval PyObject    OK
    @retval NULL        Exception
*/
int
MySQL_init(MySQL *self, PyObject *args, PyObject *kwds)
{
    PyObject *use_unicode= NULL, *auth_plugin= NULL, *tmp, *con_timeout= NULL;

	static char *kwlist[]=
	{
	    "buffered", "raw", "charset_name",
	    "connection_timeout", "use_unicode",
	    "auth_plugin",
	    NULL
	};

    PyDateTime_IMPORT;

    // Initialization expect -1 when parsing arguments failed
    if (!PyArg_ParseTupleAndKeywords(args, kwds,
                                     "|O!O!O!O!O!O!", kwlist,
                                     &PyBool_Type, &self->buffered_at_connect,
                                     &PyBool_Type, &self->raw_at_connect,
                                     &PyStringType, &self->charset_name,
                                     &PyIntType, &con_timeout,
                                     &PyBool_Type, &use_unicode,
                                     &PyStringType, &auth_plugin))
        return -1;

    if (self->buffered_at_connect)
    {
        self->buffered= self->buffered_at_connect;
    }

    if (self->raw_at_connect)
    {
        self->raw= self->raw_at_connect;
    }

    self->use_unicode= 0;
    if (use_unicode)
    {
        if (use_unicode == Py_True)
        {
            self->use_unicode= 1;
        }
    }

    if (auth_plugin)
    {
        if (strcmp(PyStringAsString(auth_plugin), "") == 0)
        {
            auth_plugin= Py_None;
        }
        if (auth_plugin != Py_None)
        {
            tmp= self->auth_plugin;
            Py_INCREF(auth_plugin);
            self->auth_plugin= auth_plugin;
            Py_XDECREF(tmp);
        }
    }

    if (con_timeout)
    {
        self->connection_timeout= (unsigned int)PyIntAsULong(con_timeout);
    }

    return 0;
}

/**
  Reset stored result.

  Reset the stored result for this MySQL instance.

  @param    self    MySQL instance

  @return   None
    @retval Py_None OK
*/
PyObject *
MySQL_reset_result(MySQL *self)
{
    self->result= NULL;

    Py_XDECREF(self->fields);

    self->fields=           NULL;
    self->have_result_set=  Py_False;

    Py_RETURN_NONE;
}

/**
  Free stored result.

  Free the stored result for this MySQL instance.
  MySQL_reset_result() is called to reset the result in the
  MySQL instance.

  Note that if there is a result it is being consumed if
  needed. If the statement that was executed returned
  multiple results, it will loop over all sets and consume.

  @param    self    MySQL instance

  @return   None
    @retval Py_None OK
*/
PyObject *
MySQL_free_result(MySQL *self)
{
    if (self->result)
    {
        Py_BEGIN_ALLOW_THREADS
        mysql_free_result(self->result);
        Py_END_ALLOW_THREADS
    }

    MySQL_reset_result(self);

    Py_RETURN_NONE;
}

/**
  Consume the result.

  Consume the stored result for this MySQL instance by
  fetching all rows. MySQL_free_result() is called to reset
  the result in the MySQL instance.

  Note that if there is a result it is being consumed if
  needed. If the statement that was executed returned
  multiple results, it will loop over all sets and consume.

  @param    self    MySQL instance

  @return   None
    @retval Py_None OK
*/
PyObject *
MySQL_consume_result(MySQL *self)
{
    int res = 0;
    if (self->result)
    {
        Py_BEGIN_ALLOW_THREADS
        while (mysql_fetch_row(self->result))
        {
            res++;
        }
        Py_END_ALLOW_THREADS
    }

    MySQL_free_result(self);

    Py_RETURN_NONE;
}

/**
  Toggle and return whether results are buffered (stored) or not.

  Return whether the MySQL instance is buffering or storing
  the results. If a boolean value is specified in the arguments
  then the instance will be reconfigured.

  Raises TypeError when the value is not PyBool_type.

  @param    self    MySQL instance
  @param    args    optional boolean type to toggle buffering

  @return   Boolean Object Py_True or Py_False
    @retval PyBool_type OK
    @retval NULL        Exception
*/
PyObject *
MySQL_buffered(MySQL *self, PyObject *args)
{
    PyObject *value = NULL;

    if (!PyArg_ParseTuple(args, "|O!", &PyBool_Type, &value))
    {
        return NULL;
    }

    if (value)
    {
        if (value == Py_True)
        {
            self->buffered= Py_True;
        }
        else
        {
            self->buffered= Py_False;
        }
    }

    if (self->buffered == Py_True)
    {
        Py_RETURN_TRUE;
    }
    else
    {
        Py_RETURN_FALSE;
    }
}

/**
  Toggle and return whether results are converted to Python types or not.

  Return whether the MySQL instance will return the rows
  as-is, meaning not converted to Python types. If a boolean value
  is specified in the arguments then the instance will be
  reconfigured.

  @param    self    MySQL instance
  @param    args    optional boolean type to toggle raw

  @return   Boolean Object Py_True or Py_False
    @retval PyBool_type OK
    @retval NULL        Exception
*/
PyObject*
MySQL_raw(MySQL *self, PyObject *args)
{
    PyObject *value = NULL;

    if (!PyArg_ParseTuple(args, "|O!", &PyBool_Type, &value)) {
        return NULL;
    }

    if (value)
    {
        if (value == Py_True)
        {
            self->raw= Py_True;
        } else {
            self->raw= Py_False;
        }
    }

    if (self->raw == Py_True)
    {
        Py_RETURN_TRUE;
    }
    else
    {
        Py_RETURN_FALSE;
    }
}

/**
  Toggle and return whether Unicode strings will be returned.

  Return whether the MySQL instance will return non-binary
  strings as Unicode. If a boolean value is specified in the
  arguments then the instance will be

  @param    self    MySQL instance
  @param    args    optional boolean type to toggle unicode

  @return   Boolean Object Py_True or Py_False
    @retval PyBool_type OK
    @retval NULL        Exception
*/
PyObject*
MySQL_use_unicode(MySQL *self, PyObject *args)
{
    PyObject *value = NULL;

    if (!PyArg_ParseTuple(args, "|O!", &PyBool_Type, &value))
    {
        return NULL;
    }

    if (value)
    {
        if (value == Py_True)
        {
            self->use_unicode= 1;
        }
        else
        {
            self->use_unicode= 0;
        }
    }

    if (self->use_unicode)
    {
        Py_RETURN_TRUE;
    }
    else
    {
        Py_RETURN_FALSE;
    }
}

/**
  Return number of rows changed by the last statement.

  Return number of rows changed by the last statement.

  @param    self    MySQL instance

  @return   PyInt_Type or PyNone
    @retval PyInt_Type  OK
    @retval PyNone      no active session
*/
PyObject*
MySQL_st_affected_rows(MySQL *self)
{
    if (&self->session)
    {
        return PyIntFromULongLong((&self->session)->affected_rows);
    }

    Py_RETURN_NONE;
}

/**
  Return client flags of the current session.

  Return client flags of the current session.

  @param    self    MySQL instance

  @return   PyInt_Type or PyNone
    @retval PyInt_Type  OK
    @retval PyNone      no active session
*/
PyObject*
MySQL_st_client_flag(MySQL *self)
{
    if (&self->session)
    {
        return PyInt_FromLong((&self->session)->client_flag);
    }

    Py_RETURN_NONE;
}

/**
  Return field count of the current session.

  Return field count of the current session.

  @param    self    MySQL instance

  @return   PyInt_Type or PyNone
    @retval PyInt_Type  OK
    @retval PyNone      no active session
*/
PyObject*
MySQL_st_field_count(MySQL *self)
{
    if (&self->session)
    {
        return PyInt_FromLong((&self->session)->field_count);
    }

    Py_RETURN_NONE;
}

/**
  Return insert ID.

  Return insert ID.

  @param    self    MySQL instance

  @return   PyInt_Type or PyNone
    @retval PyInt_Type  OK
    @retval PyNone      no active session
*/
PyObject*
MySQL_st_insert_id(MySQL *self)
{
    if (&self->session)
    {
        return PyIntFromULongLong((&self->session)->insert_id);
    }

    Py_RETURN_NONE;
}

/**
  Return server capabilities.

  Return server capabilities.

  @param    self    MySQL instance

  @return   PyInt_Type or PyNone
    @retval PyInt_Type  OK
    @retval PyNone      no active session
*/
PyObject*
MySQL_st_server_capabilities(MySQL *self)
{
    if (&self->session)
    {
        return PyInt_FromLong((&self->session)->server_capabilities);
    }

    Py_RETURN_NONE;
}

/**
  Return server status flag.

  Return server status flag.

  @param    self    MySQL instance

  @return   PyInt_Type or PyNone
    @retval PyInt_Type  OK
    @retval PyNone      no active session
*/
PyObject*
MySQL_st_server_status(MySQL *self)
{
    if (&self->session)
    {
        return PyInt_FromLong((&self->session)->server_status);
    }

    Py_RETURN_NONE;
}

/**
  Return warning count.

  Return warning count.

  @param    self    MySQL instance

  @return   PyInt_Type or PyNone
    @retval PyInt_Type  OK
    @retval PyNone      no active session
*/
PyObject*
MySQL_st_warning_count(MySQL *self)
{
    if (&self->session)
    {
        return PyInt_FromLong((&self->session)->warning_count);
    }

    Py_RETURN_NONE;
}

/**
  Return whether MySQL instance is connected or not.

  Return whether the MySQL instance is connected or not.

  This function uses the mysql_ping() C API function.

  @param    self    MySQL instance

  @return   Boolean Object Py_True or Py_False
    @retval PyBool_type OK
*/
PyObject*
MySQL_connected(MySQL *self)
{
    if (!self->connected)
    {
        Py_RETURN_FALSE;
    }

    self->connected= 1;
    Py_RETURN_TRUE;
}

/**
  Toggle autocommit.

  Toggle autocommit to be on or off.

  Raises ValueError when mode is not a PyBool_type (Py_True or
  Py_False).

  @param    self    MySQL instance

  @return   PyNone or NULL
    @retval PyNone  OK
    @retval NULL    Exception
*/
PyObject*
MySQL_autocommit(MySQL *self, PyObject *mode)
{
	int res= 0;
	int new_mode= 0;

	if (Py_TYPE(mode) == &PyBool_Type)
	{
        new_mode= (mode == Py_True) ? 1 : 0;

        res= (int)mysql_autocommit(&self->session, new_mode);
        if (res == -1 && mysql_errno(&self->session))
        {
            raise_with_session(&self->session, NULL);
            return NULL;
        }
        Py_RETURN_NONE;
    }

    PyErr_SetString(PyExc_ValueError, "mode must be boolean");
    return NULL;
}

/**
  Change user and set new default database.

  Change user and set new default database using the positional
  and keyword arguments.
  Arguments can be user, password or database.

  @param    self    MySQL instance
  @param    args    positional arguments
  @param    kwargs  keyword arguments

  @return   Boolean Object Py_True or Py_False
    @retval PyNone  OK
    @retval NULL    Exception
*/
PyObject*
MySQL_change_user(MySQL *self, PyObject *args, PyObject *kwds)
{
	char *user= NULL, *password= NULL, *database= NULL;
	int res;
	static char *kwlist[]= {"user", "password", "database", NULL};

    IS_CONNECTED(self);

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|sss", kwlist,
                                     &user, &password, &database))
    {
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS
    res= mysql_change_user(&self->session, user, password, database);
    Py_END_ALLOW_THREADS

    if (res)
    {
    	raise_with_session(&self->session, NULL);
    	return NULL;
    }

    Py_RETURN_NONE;
}

/**
  Return the default character set name for the current session.

  Return the default character set name for the current session.

  Raises ValueError when mode is not a PyBool_type (Py_True or
  Py_False).

  @param    self    MySQL instance

  @return   MySQL character set name.
    @retval PyUnicode   Python v3
    @retval PyString    Python v2
*/
PyObject*
MySQL_character_set_name(MySQL *self)
{
    const char *name;

    IS_CONNECTED(self);

    Py_BEGIN_ALLOW_THREADS
    name= mysql_character_set_name(&self->session);
    Py_END_ALLOW_THREADS

    return PyStringFromString(name);
}

/**
  Set the default character set for the current session.

  Set the default character set for the current session. The
  only arg allowed is a PyString_Type which has contains the
  character set name.

  Raises TypeError when the argument is not a PyString_type.

  @param    self    MySQL instance

  @return   MySQL character set name.
    @retval None    OK
    @retval NULL    Exception.
*/
PyObject*
MySQL_set_character_set(MySQL *self, PyObject *args)
{
    PyObject* value;
    int res;

    if (!PyArg_ParseTuple(args, "O!", &PyStringType, &value))
    {
        return NULL;
    }

    IS_CONNECTED(self);

    Py_BEGIN_ALLOW_THREADS
    res= mysql_set_character_set(&self->session, PyStringAsString(value));
    Py_END_ALLOW_THREADS

    if (res)
    {
    	raise_with_session(&self->session, NULL);
    	return NULL;
    }

    self->charset_name= value;

    Py_RETURN_NONE;
}

/**
  Commit the current transaction.

  Commit the current transaction.

  Raises MySQLInterfaceError on errors.

  @param    self    MySQL instance

  @return   PyNone or NULL.
    @retval PyNone  OK
    @retval NULL    Exception.
*/
PyObject*
MySQL_commit(MySQL *self)
{
    int res = 0;

    IS_CONNECTED(self);

    res= mysql_commit(&self->session);
    if (res)
    {
    	raise_with_session(&self->session, NULL);
	    return NULL;
    }

    Py_RETURN_NONE;
}

/**
  Connect with a MySQL server.

  Connect with a MySQL server using the positional and keyword
  arguments.
  Note that some connection argument are first passed to the
  MySQL instance using the MySQL_init() function. The other
  connection argument can be passed to MySQL_connect().

  MySQL_connect() will first try to reset results and close
  the open connection. It will then use mysql_init() C API
  function to allocate the MYSQL structure. mysql_options()
  is then used to configure the connection. Finally,
  mysql_real_connect() is called.

  Raises TypeError when one of the arguements is of an invalid
  type.

  @param    self    MySQL instance
  @param    args    positional arguments
  @param    kwargs  keyword arguments

  @return   Boolean Object Py_True or Py_False
    @retval PyNone  OK
    @retval NULL    Exception
*/
PyObject*
MySQL_connect(MySQL *self, PyObject *args, PyObject *kwds)
{
	char *host= NULL, *user= NULL, *database= NULL, *unix_socket= NULL;
	char *ssl_ca= NULL, *ssl_cert= NULL, *ssl_key= NULL;
	PyObject *charset_name, *compress, *ssl_verify_cert, *password, *ssl_disabled;
	const char* auth_plugin;
	unsigned long client_flags= 0;
	unsigned int port= 3306, tmp_uint;
	unsigned int protocol= 0;
#if MYSQL_VERSION_ID >= 50711
	unsigned int ssl_mode;
#endif
#if MYSQL_VERSION_ID >= 80001
	bool abool;
	bool ssl_enabled= 0;
#else
  my_bool abool;
  my_bool ssl_enabled= 0;
#endif
	MYSQL *res;

	static char *kwlist[]=
	{
	    "host", "user", "password", "database",
		"port", "unix_socket", "client_flags",
		"ssl_ca", "ssl_cert", "ssl_key", "ssl_verify_cert", "ssl_disabled",
		"compress",
		NULL
    };

#ifdef PY3
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|zzzzkzkzzzO!O!O!", kwlist,
#else
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|zzOzkzkzzzO!O!O!", kwlist,
#endif
                                     &host, &user, &password, &database,
                                     &port, &unix_socket,
                                     &client_flags,
                                     &ssl_ca, &ssl_cert, &ssl_key,
                                     &PyBool_Type, &ssl_verify_cert,
                                     &PyBool_Type, &ssl_disabled,
                                     &PyBool_Type, &compress))
    {
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS
    if (self->connected)
    {
        self->connected= 0;
        mysql_close(&self->session);
    }

    mysql_init(&self->session);

#ifdef MS_WINDOWS
    if (NULL == host)
    {
        // if host is NULL, we try with named pipe
        mysql_options(&self->session, MYSQL_OPT_NAMED_PIPE, 0);
        protocol= MYSQL_PROTOCOL_PIPE;
    }
#else
    if (unix_socket)
    {
        // Unix Sockets are only used when unix_socket is given.
        // 'localhost' is not a special case, and it use TCP protocol
        protocol= MYSQL_PROTOCOL_SOCKET;
        host= NULL;
    }
#endif
    else
    {
        protocol= MYSQL_PROTOCOL_TCP;
    }

#ifdef PY3
    charset_name= PyUnicode_AsASCIIString(self->charset_name);
    if (NULL == charset_name)
    {
        return NULL;
    }
#else
    charset_name= self->charset_name;
#endif

    mysql_options(&self->session, MYSQL_OPT_PROTOCOL, (char*)&protocol);
    mysql_options(&self->session, MYSQL_SET_CHARSET_NAME,
                  PyBytesAsString(charset_name));

    tmp_uint= self->connection_timeout;
    mysql_options(&self->session, MYSQL_OPT_CONNECT_TIMEOUT, (char*)&tmp_uint);
    mysql_options(&self->session, MYSQL_OPT_READ_TIMEOUT, (char*)&tmp_uint);
    mysql_options(&self->session, MYSQL_OPT_WRITE_TIMEOUT, (char*)&tmp_uint);

    if (ssl_disabled && ssl_disabled == Py_False) {
        ssl_enabled= 1;
        client_flags |= CLIENT_SSL;
        if (ssl_verify_cert && ssl_verify_cert == Py_True)
        {
#if MYSQL_VERSION_ID >= 50711
            ssl_mode= SSL_MODE_VERIFY_IDENTITY;
            mysql_options(&self->session, MYSQL_OPT_SSL_MODE, &ssl_mode);
#else
            abool= 1;
#if MYSQL_VERSION_ID > 50703
            mysql_options(&self->session, MYSQL_OPT_SSL_ENFORCE, (char*)&abool);
#endif
            mysql_options(&self->session,
                          MYSQL_OPT_SSL_VERIFY_SERVER_CERT, (char*)&abool);
#endif
        } else {
          ssl_ca= NULL;
        }
        mysql_ssl_set(&self->session, ssl_key, ssl_cert, ssl_ca, NULL, NULL);
    } else {
        // Make sure to not enforce SSL
#if MYSQL_VERSION_ID > 50703 && MYSQL_VERSION_ID < 50711
        {
            abool= 0;
            mysql_options(&self->session, MYSQL_OPT_SSL_ENFORCE, (char*)&abool);
        }
#endif
#if MYSQL_VERSION_ID >= 50711
        {
            ssl_mode= SSL_MODE_DISABLED;
            mysql_options(&self->session, MYSQL_OPT_SSL_MODE, &ssl_mode);
        }
#endif
    }

    Py_END_ALLOW_THREADS
    if (PyString_Check(self->auth_plugin)) {
        auth_plugin= PyStringAsString(self->auth_plugin);
        mysql_options(&self->session, MYSQL_DEFAULT_AUTH, auth_plugin);
        if (strcmp(auth_plugin, "sha256_password") == 0 && !ssl_enabled)
        {
            PyObject *exc_type= MySQLInterfaceError;
            PyObject *err_no= PyInt_FromLong(2002);
            PyObject *err_msg= PyStringFromString("sha256_password requires SSL");
            PyObject *err_obj= NULL;
            err_obj= PyObject_CallFunctionObjArgs(exc_type, err_msg, NULL);
            PyObject_SetAttr(err_obj, PyStringFromString("sqlstate"), Py_None);
            PyObject_SetAttr(err_obj, PyStringFromString("errno"), err_no);
            PyObject_SetAttr(err_obj, PyStringFromString("msg"), err_msg);
            PyErr_SetObject(exc_type, err_obj);
            Py_XDECREF(exc_type);
            Py_XDECREF(err_no);
            Py_XDECREF(err_msg);
            return NULL;
        }

        if (strcmp(auth_plugin, "mysql_clear_password") == 0)
        {
            abool= 1;
            mysql_options(&self->session, MYSQL_ENABLE_CLEARTEXT_PLUGIN,
                          (char*)&abool);
        }
    }
    Py_BEGIN_ALLOW_THREADS

    if (database && strlen(database) == 0)
    {
        database= NULL;
    }

    if (!database)
    {
        client_flags= client_flags & ~CLIENT_CONNECT_WITH_DB;
    }

    if (client_flags & CLIENT_LOCAL_FILES) {
        abool= 1;
        mysql_options(&self->session, MYSQL_OPT_LOCAL_INFILE, (unsigned int*)&abool);
    }

#ifdef PY3
    res= mysql_real_connect(&self->session, host, user, password, database,
                            port, unix_socket, client_flags);
#else
{
    char* c_password;
    if (PyUnicode_Check(password))
    {
        PyObject* u_password= PyUnicode_AsUTF8String(password);
        c_password= PyString_AsString(u_password);
        Py_DECREF(u_password);
    }
    else
    {
        c_password= PyString_AsString(password);
    }
    res= mysql_real_connect(&self->session, host, user, c_password, database,
                            port, unix_socket, client_flags);
}
#endif

    Py_END_ALLOW_THREADS

    if (!res)
    {
    	raise_with_session(&self->session, NULL);
    	return NULL;
    }

    self->connected= 1;

    Py_RETURN_NONE;
}

/**
  Close the MySQL connection.

  Close the MySQL connection.

  @param    self    MySQL instance

  @return   PyNone.
    @retval PyNone  OK
*/
PyObject*
MySQL_close(MySQL *self)
{
    if (self->connected)
    {
        self->connected= 0;
        Py_BEGIN_ALLOW_THREADS
        mysql_close(&self->session);
        Py_END_ALLOW_THREADS
    }

    Py_RETURN_NONE;
}

/**
  Create a legal SQL string that you can use in an SQL statement.

  Create a legal SQL string that you can use in an SQL statement
  using the mysql_escape_string() C API function.

  Raises TypeError if value is not a PyUnicode_Type,
  PyBytes_Type or, for Python v2, PyString_Type.
  Raises MySQLError when the string could not be escaped.

  @param    self    MySQL instance
  @param    value   the string to escape

  @return   PyObject with the escaped string.
    @retval PyBytes     Python v3
    @retval PyString    Python v2
    @retval NULL        Exception.
*/
PyObject*
MySQL_escape_string(MySQL *self, PyObject *value)
{
    PyObject *to= NULL, *from= NULL;
    char *from_str, *to_str;
    Py_ssize_t from_size= 0;
    Py_ssize_t escaped_size= 0;
    const char *charset;

    IS_CONNECTED(self);

    charset= my2py_charset_name(&self->session);

    if (PyUnicode_Check(value))
    {
        if (strcmp(charset, "binary") == 0)
        {
            charset = "utf8";
        }
        from= PyUnicode_AsEncodedString(value, charset, NULL);
        if (!from)
        {
            return NULL;
        }
        from_size= BytesSize(from);
        from_str= PyBytesAsString(from);
    }
#ifndef PY3
    // Python v2 str
    else if (PyString_Check(value))
#else
    // Python v3 bytes
    else if (PyBytes_Check(value))
#endif
    {
        from_size= BytesSize(value);
        from_str= PyBytesAsString(value);
    }
    else
    {
#ifdef PY3
        PyErr_SetString(PyExc_TypeError, "Argument must be str or bytes");
#else
        PyErr_SetString(PyExc_TypeError, "Argument must be unicode or str");
#endif
        return NULL;
    }

    to= BytesFromStringAndSize(NULL, from_size * 2 + 1);
    to_str= PyBytesAsString(to);

    Py_BEGIN_ALLOW_THREADS
    escaped_size= (Py_ssize_t)mysql_real_escape_string(&self->session, to_str,
                                                       from_str,
                                                       (unsigned long)from_size);
    Py_END_ALLOW_THREADS

    BytesResize(&to, escaped_size);
    Py_XDECREF(from);

    if (!to)
    {
        PyErr_SetString(MySQLError, "Failed escaping string.");
        return NULL;
    }

    return to;
}

/**
  Get information about the default character set.

  Get information about the default character set for
  the current MySQL session. The returned dictionary
  has the keys number, name, csname, comment, dir,
  mbminlen and mbmaxlen.

  Raises TypeError if value is not a PyUnicode_Type,
  PyBytes_Type or, for Python v2, PyString_Type.
  Raises MySQLError when the string could not be escaped.

  @param    self    MySQL instance
  @param    value   the string to escape

  @return   A mapping with information as key/value pairs.
    @retval PyDict  Python v3
    @retval NULL    Exception.
*/
PyObject*
MySQL_get_character_set_info(MySQL *self)
{
    MY_CHARSET_INFO cs;
    PyObject *cs_info;

    IS_CONNECTED(self);

    Py_BEGIN_ALLOW_THREADS
    mysql_get_character_set_info(&self->session, &cs);
    Py_END_ALLOW_THREADS

	cs_info= PyDict_New();
	PyDict_SetItemString(cs_info, "number", PyInt_FromLong(cs.number));
	PyDict_SetItemString(cs_info, "name",
	                     PyString_FromStringAndSize(cs.name, strlen(cs.name)));
	PyDict_SetItemString(cs_info, "csname",
                         PyString_FromStringAndSize(cs.csname,
                                                    strlen(cs.csname)));

	PyDict_SetItemString(cs_info, "comment",
                         PyString_FromStringAndSize(cs.comment,
                                                    strlen(cs.comment)));

    if (cs.dir)
    {
	    PyDict_SetItemString(cs_info, "dir",
                             PyString_FromStringAndSize(cs.dir,
                                                        strlen(cs.dir)));
    }

	PyDict_SetItemString(cs_info, "mbminlen", PyInt_FromLong(cs.mbminlen));
	PyDict_SetItemString(cs_info, "mbmaxlen", PyInt_FromLong(cs.mbmaxlen));

	return cs_info;
}

/**
  Get MySQL client library version as string.

  @param    self    MySQL instance

  @return   MySQL client version as string.
    @retval PyUnicode   Python v3
    @retval PyString    Python v2
*/
PyObject*
MySQL_get_client_info(MySQL *self)
{
    const char *name;

    Py_BEGIN_ALLOW_THREADS
    name= mysql_get_client_info();
    Py_END_ALLOW_THREADS

    return PyStringFromString(name);
}

/**
  Get MySQL client library version as tuple.

  @param    self    MySQL instance

  @return   MySQL version as sequence of integers.
    @retval PyTuple     OK
*/
PyObject*
MySQL_get_client_version(MySQL *self)
{
    unsigned long ver;
    PyObject *version;

    Py_BEGIN_ALLOW_THREADS
    ver= mysql_get_client_version();
    Py_END_ALLOW_THREADS

    version= PyTuple_New(3);
    // ver has format XYYZZ: X=major, YY=minor, ZZ=sub-version
    PyTuple_SET_ITEM(version, 0, PyInt_FromLong(ver / VERSION_OFFSET_MAJOR));
    PyTuple_SET_ITEM(version, 1,
                     PyInt_FromLong((ver / VERSION_OFFSET_MINOR) \
                                    % VERSION_OFFSET_MINOR));
    PyTuple_SET_ITEM(version, 2, PyInt_FromLong(ver % VERSION_OFFSET_MINOR));
    return version;
}

/**
  Get description of the type of connection in use.

  @param    self    MySQL instance

  @return   Connection description as string.
    @retval PyUnicode   Python v3
    @retval PyString    Python v2
*/
PyObject*
MySQL_get_host_info(MySQL *self)
{
    const char *host;

    IS_CONNECTED(self);

    Py_BEGIN_ALLOW_THREADS
    host= mysql_get_host_info(&self->session);
    Py_END_ALLOW_THREADS

    return PyStringFromString(host);
}

/**
  Get protocol version used by current connection.

  @param    self    MySQL instance

  @return   MySQL server version as string.
    @retval PyInt   OK
    @retval NULL    Exception
*/
PyObject*
MySQL_get_proto_info(MySQL *self)
{
    unsigned int proto;

    IS_CONNECTED(self);

    Py_BEGIN_ALLOW_THREADS
    proto= mysql_get_proto_info(&self->session);
    Py_END_ALLOW_THREADS

    return PyInt_FromLong(proto);
}

/**
  Get MySQL server version as string.

  @param    self    MySQL instance

  @return   MySQL server version as string.
    @retval PyUnicode   Python v3
    @retval PyString    Python v2
*/
PyObject*
MySQL_get_server_info(MySQL *self)
{
    const char *name;

    IS_CONNECTED(self);

    Py_BEGIN_ALLOW_THREADS
    name= mysql_get_server_info(&self->session);
    Py_END_ALLOW_THREADS

    return PyStringFromString(name);
}

/**
  Get MySQL server version as tuple.

  @param    self    MySQL instance

  @return   MySQL version as sequence of integers.
    @retval PyTuple     OK
*/
PyObject*
MySQL_get_server_version(MySQL *self)
{
    unsigned long ver;
    PyObject *version;

    IS_CONNECTED(self);

    Py_BEGIN_ALLOW_THREADS
    ver= mysql_get_server_version(&self->session);
    Py_END_ALLOW_THREADS

    version= PyTuple_New(3);
    PyTuple_SET_ITEM(version, 0, PyInt_FromLong(ver / VERSION_OFFSET_MAJOR));
    PyTuple_SET_ITEM(version, 1,
                     PyInt_FromLong((ver / VERSION_OFFSET_MINOR) \
                                    % VERSION_OFFSET_MINOR));
    PyTuple_SET_ITEM(version, 2, PyInt_FromLong(ver % VERSION_OFFSET_MINOR));
    return version;
}

/**
  Get SSL cipher used for the current connection.

  @param    self    MySQL instance

  @return   SSL cipher as string.
    @retval PyUnicode   Python v3
    @retval PyString    Python v2
*/
PyObject*
MySQL_get_ssl_cipher(MySQL *self)
{
    const char *name;

    IS_CONNECTED(self);

    name= mysql_get_ssl_cipher(&self->session);
    if (name == NULL)
    {
        Py_RETURN_NONE;
    }
    return PyStringFromString(name);
}

/**
  Encode string in hexadecimal format.

  Encode value in hexadecimal format and wrap it inside
  X''. For example, "spam" becomes X'68616d'.

  @param    self    MySQL instance
  @param    value   string to encode

  @return   Encoded string prefixed with X and quoted.
    @retval PyBytes     Python v3
    @retval PyString    Python v2
*/
PyObject*
MySQL_hex_string(MySQL *self, PyObject *value)
{
    PyObject *to, *from, *result= NULL;
    char *from_str, *to_str;
    Py_ssize_t from_size= 0;
    Py_ssize_t hexed_size= 0;
    const char *charset;

    charset= my2py_charset_name(&self->session);
    from= str_to_bytes(charset, value);
    if (!from)
    {
        return NULL;
    }

    from_size= BytesSize(from);
    to= BytesFromStringAndSize(NULL, from_size * 2 + 1);
    if (!to)
    {
        return NULL;
    }
    to_str= PyBytesAsString(to);
    from_str= PyBytesAsString(from);

    Py_BEGIN_ALLOW_THREADS
    hexed_size= (Py_ssize_t)mysql_hex_string(to_str, from_str,
                                             (unsigned long)from_size);
    Py_END_ALLOW_THREADS

    BytesResize(&to, hexed_size);

    result= PyBytes_FromString("X'");
    PyBytes_Concat(&result, to);
    PyBytes_Concat(&result, PyBytes_FromString("'"));

    return result;
}

/**
  Get the ID generated by AUTO_INCREMENT column.

  Get the ID generated by the AUTO_INCREMENT column by the
  previous executed query.

  @param    self    MySQL instance

  @return   ID generated by AUTO_INCREMENT.
    @retval PyInt   OK
    @retval NULL    Exception
*/
PyObject*
MySQL_insert_id(MySQL *self)
{
    my_ulonglong id;

    CHECK_SESSION(self);

    // if there was an error, result is undefined, better check:
    if (mysql_errno(&self->session))
    {
        raise_with_session(&self->session, NULL);
	    return NULL;
    }

    Py_BEGIN_ALLOW_THREADS
    id= mysql_insert_id(&self->session);
    Py_END_ALLOW_THREADS

    return PyIntFromULongLong(id);
}

/**
  Check whether connection is working.

  Check whether connection to the MySQL is working.

  @param    self    MySQL instance

  @return   Boolean Object Py_True or Py_False
    @retval Py_True connection available
    @retval Py_False connection not available
*/
PyObject*
MySQL_ping(MySQL *self)
{
    int res;

    if (!self->connected)
    {
        Py_RETURN_FALSE;
    }

    res= mysql_ping(&self->session);

    if (!res)
    {
        Py_RETURN_TRUE;
    }

    Py_RETURN_FALSE;
}

/**
  Convert Python values to MySQL values.

  Convert Python values to MySQL values based on the Python
  type of each value to convert. The converted values are
  escaped and quoted.

  Raises MySQLInterfaceError when a Python value can not
  be converted.

  @param    self    MySQL instance
  @param    args    Python values to be converted

  @return   PyTuple which contains all converted values.
    @retval PyTuple OK
    @retval NULL    Exception
*/
PyObject*
MySQL_convert_to_mysql(MySQL *self, PyObject *args)
{
    PyObject *value, *new_value;
    PyObject *prepared, *quoted;
    int i;
    Py_ssize_t size;
    char error[100];
#ifndef PY3
    int tmp_size;
    PyObject *numeric, *new_num;
    char *tmp;
#endif

    size = PyTuple_Size(args);
    prepared = PyTuple_New(size);

    for (i= 0; i < size; i++) {
        value= PyTuple_GetItem(args, i);

        if (value == NULL)
        {
            goto error;
        }

        // None is SQL's NULL
        if (value == Py_None)
        {
            PyTuple_SET_ITEM(prepared, i, PyBytesFromString("NULL"));
            continue;
        }

        if (PyIntLong_Check(value) || PyFloat_Check(value))
        {
#ifdef PY3
            PyTuple_SET_ITEM(prepared, i,
                             PyBytes_FromString(
                                (const char *)PyUnicode_1BYTE_DATA(
                                PyObject_Str(value))));
#else
            numeric= PyObject_Repr(value);
            tmp= PyString_AsString(numeric);
            tmp_size= (int)PyString_Size(numeric);
            if (tmp[tmp_size - 1] == 'L')
            {
                new_num= PyString_FromStringAndSize(tmp, tmp_size);
                _PyString_Resize(&new_num, tmp_size - 1);
                PyTuple_SET_ITEM(prepared, i, new_num);
            }
            else
            {
                PyTuple_SET_ITEM(prepared, i, numeric);
            }
#endif
            continue;
        }

        // All values that need to be quoted
        if (PyString_Check(value)
            || PyUnicode_Check(value)
            || PyBytes_Check(value))
        {
            new_value= MySQL_escape_string(self, value);
        }
        else if (PyDateTime_Check(value))
        {
            // datetime is handled first
            new_value= pytomy_datetime(value);
        } else if (PyDate_CheckExact(value))
        {
            new_value= pytomy_date(value);
        }
        else if (PyTime_Check(value))
        {
            new_value= pytomy_time(value);
        }
        else if (PyDelta_CheckExact(value))
        {
            new_value= pytomy_timedelta(value);
#ifndef PY3
        }
        else if (strcmp((value)->ob_type->tp_name, "Decimal") == 0)
        {
#else
        }
        else if (strcmp((value)->ob_type->tp_name, "decimal.Decimal") == 0)
        {
#endif
            new_value= pytomy_decimal(value);
        }
        else
        {
            PyOS_snprintf(error, 100,
                          "Python type %s cannot be converted",
                          (value)->ob_type->tp_name);
            PyErr_SetString(MySQLInterfaceError, (const char*)error);
            goto error;
        }

        if (!new_value)
        {
            PyOS_snprintf(error, 100,
                          "Failed converting Python '%s'",
                          (value)->ob_type->tp_name);
            PyErr_SetString(MySQLInterfaceError, error);
            goto error;
        }

        // Some conversions could return None instead of raising errors
        if (new_value == Py_None)
        {
            PyTuple_SET_ITEM(prepared, i, PyBytesFromString("NULL"));

#ifdef PY3
        }
        else if (PyBytes_Check(new_value))
        {
            quoted= PyBytes_FromFormat("'%s'", PyBytes_AsString(new_value));
            PyTuple_SET_ITEM(prepared, i, quoted);
#endif
        }
        else if (PyString_Check(new_value))
        {
#ifdef PY3
            quoted= PyBytes_FromFormat("'%s'", PyUnicode_AS_DATA(new_value));
#else
            quoted= PyString_FromFormat("'%s'", PyStringAsString(new_value));
#endif
            PyTuple_SET_ITEM(prepared, i, quoted);
        }
        else
        {
            PyErr_SetString(PyExc_ValueError, (const char*)"Fail!");
            goto error;
        }
        Py_DECREF(new_value);
    }
    return prepared;

error:
    Py_XDECREF(prepared);
    return NULL;
}

/**
  Execute an SQL query.

  Execute an SQL query using the current connection. The
  arguments allowed are statement, buffered, raw and
  raw_as_string.

  buffered and raw, if not provided, will have the default
  set from the MySQL instance. raw_as_string is a special
  argument for Python v2 and will return PyString instead
  of PyByteArray (compatible with Connector/Python v1.x).

  Raises TypeError when one of the arguments has an invalid
  type.
  Raises MySQLInterfaceError for any MySQL error returned
  by the MySQL server.

  @param    self    MySQL instance
  @param    args    Python values to be converted

  @return   PyTuple which contains all converted values.
    @retval PyBool_type OK
*/
PyObject*
MySQL_query(MySQL *self, PyObject *args, PyObject *kwds)
{
	PyObject *buffered= NULL, *raw= NULL, *raw_as_string= NULL;
	int res= 0, stmt_length;
	char *stmt= NULL;
	static char *kwlist[]=
	{
	    "statement", "buffered", "raw",
	    "raw_as_string", NULL
	};

    IS_CONNECTED(self);

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "s#|O!O!O!", kwlist,
                                     &stmt, &stmt_length,
                                     &PyBool_Type, &buffered,
                                     &PyBool_Type, &raw,
                                     &PyBool_Type, &raw_as_string))
    {
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS
    res= mysql_real_query(&self->session, stmt, stmt_length);
    Py_END_ALLOW_THREADS

    if (res != 0)
    {
    	raise_with_session(&self->session, NULL);
    	return NULL;
    }

    if ((&self->session)->field_count == 0)
    {
        MySQL_reset_result(self);
        self->have_result_set= Py_False;
        Py_RETURN_TRUE;
    }

    if (raw_as_string)
    {
        self->raw_as_string= raw_as_string;
    }

	if (buffered)
	{
		self->buffered= buffered;
	}
	else
	{
	    self->buffered= self->buffered_at_connect;
	}

	if (raw)
	{
	    self->raw= raw;
	}
	else
	{
		self->raw= self->raw_at_connect;
	}

    mysql_get_character_set_info(&self->session, &self->cs);
    return MySQL_handle_result(self);
}

/**
  Get the current thread or connection ID.

  @param    self    MySQL instance

  @return   MySQL connection thread ID
    @retval PyInt   Python v3
    @retval PyLong  Python v2
*/
PyObject*
MySQL_thread_id(MySQL *self)
{
    unsigned long thread_id;

    IS_CONNECTED(self);

    Py_BEGIN_ALLOW_THREADS
    thread_id= (unsigned long)mysql_thread_id(&self->session);
    Py_END_ALLOW_THREADS

    return PyIntFromULong(thread_id);
}

/**
  Select and set the default (current) database.

  Select and set the default or current database for the
  current connection.

  Raises MySQLInterfaceError for any MySQL error returned
  by the MySQL server.

  @param    self    MySQL instance
  @param    value   name of the database

  @return   MySQL character set name.
    @retval None    OK
    @retval NULL    Exception.
*/
PyObject*
MySQL_select_db(MySQL *self, PyObject* value)
{
    int res;
    PyObject *db;
    const char *charset;

    charset= my2py_charset_name(&self->session);
    db= str_to_bytes(charset, value);

	if (db)
	{
        Py_BEGIN_ALLOW_THREADS
        res= (int)mysql_select_db(&self->session, PyBytesAsString(db));
        Py_END_ALLOW_THREADS

        if (res != 0)
        {
            raise_with_session(&self->session, NULL);
            return NULL;
        }
        Py_RETURN_NONE;
    }

    Py_XDECREF(db);

    PyErr_SetString(PyExc_ValueError, "db must be a string");
    return NULL;
}

/**
  Return the warning count of previous SQL statement.

  @param    self    MySQL instance

  @return   MySQL connection thread ID
    @retval PyInt   Python v3
    @retval PyLong  Python v2
*/
PyObject*
MySQL_warning_count(MySQL *self)
{
    unsigned int count;

    CHECK_SESSION(self);

    Py_BEGIN_ALLOW_THREADS
    count= mysql_warning_count(&self->session);
    Py_END_ALLOW_THREADS

    return PyIntFromULong(count);
}

/**
  Return number of rows updated by the last statement.

  Return the number of rows changed, inserted or deleted
  by the last UPDATE, INSERT or DELETE queries.

  @param    self    MySQL instance

  @return   MySQL connection thread ID
    @retval PyInt   Python v3
    @retval PyLong  Python v2
*/
PyObject*
MySQL_affected_rows(MySQL *self)
{
    unsigned PY_LONG_LONG affected= 0;

    CHECK_SESSION(&self->session);

    Py_BEGIN_ALLOW_THREADS
    affected= mysql_affected_rows(&self->session);
    Py_END_ALLOW_THREADS

    if ((my_ulonglong) - 1 == affected)
    {
        affected= 0;
    }

    return PyIntFromULongLong(affected);
}

/**
  Return number of columns in last result.

  @param    self    MySQL instance

  @return   MySQL connection thread ID
    @retval PyInt   Python v3
    @retval PyLong  Python v2
*/
PyObject*
MySQL_field_count(MySQL *self)
{
    unsigned int count= 0;

    CHECK_SESSION(&self->session);

    Py_BEGIN_ALLOW_THREADS
    count= mysql_field_count(&self->session);
    Py_END_ALLOW_THREADS

    return PyIntFromULong(count);
}

/**
  Roll back the current transaction.

  Raises MySQLInterfaceError on errors.

  @param    self    MySQL instance

  @return   PyNone or NULL.
    @retval PyNone  OK
    @retval NULL    Exception.
*/
PyObject*
MySQL_rollback(MySQL *self)
{
    int res= 0;

    IS_CONNECTED(self);

    Py_BEGIN_ALLOW_THREADS
    res= mysql_rollback(&self->session);
    Py_END_ALLOW_THREADS

    if (res)
    {
    	raise_with_session(&self->session, NULL);
	    return NULL;
    }

    Py_RETURN_NONE;
}

/**
  Check whether any more results exists.

  @param    self    MySQL instance
  @param    args    Python values to be converted

  @return   Boolean Object Py_True or Py_False
    @retval Py_True     More results available
    @retval Py_False    No more results available
*/
PyObject*
MySQL_more_results(MySQL *self)
{
    int res= 0;

    CHECK_SESSION(self);

    Py_BEGIN_ALLOW_THREADS
    res= mysql_more_results(&self->session);
    Py_END_ALLOW_THREADS

    if (res == 1)
    {
        Py_RETURN_TRUE;
    }

    Py_RETURN_FALSE;
}

/**
  Handle a result.

  Handle a result after executing a statement. It will
  either store or use the result.

  Raises MySQLInterfaceError for any MySQL error returned
  by the MySQL server.

  @param    self    MySQL instance

  @return   Boolean Object Py_True or Py_False
    @retval Py_True OK
    @retval NULL    Exception
*/
PyObject*
MySQL_handle_result(MySQL *self)
{
    Py_BEGIN_ALLOW_THREADS
    if (self->buffered == Py_True)
    {
        self->result= mysql_store_result(&self->session);
    }
    else
    {
        self->result= mysql_use_result(&self->session);
    }
    Py_END_ALLOW_THREADS

    if (self->result == NULL && mysql_errno(&self->session))
    {
        // Must be an error
        raise_with_session(&self->session, NULL);
        return NULL;
    }

    if (self->result && (&self->session)->field_count)
    {
        self->have_result_set= Py_True;
    }
    else
    {
        self->have_result_set= Py_False;
    }

    Py_RETURN_TRUE;
}

/**
  Initiates the next result with multiple-results.

  Raises MySQLInterfaceError for any MySQL error returned
  by the MySQL server.

  @param    self    MySQL instance

  @return   Boolean Object Py_True or Py_False
    @retval Py_True OK
    @retval NULL    Exception
*/
PyObject*
MySQL_next_result(MySQL *self)
{
    int have_more= 0;

    if (mysql_more_results(&self->session) == 0)
    {
        // No more results
        Py_RETURN_FALSE;
    }

    MySQL_free_result(self);
    // We had a result before, we check if we can get next one
    Py_BEGIN_ALLOW_THREADS
    have_more= mysql_next_result(&self->session);
    Py_END_ALLOW_THREADS
    if (have_more > 0)
    {
        raise_with_session(&self->session, NULL);
        return NULL;
    }

    MySQL_free_result(self);
    return MySQL_handle_result(self);
}

/**
  Fetch column information for active MySQL result.

  Fetch column information for active MySQL result.

  The returned PyObject is a PyList which consists of
  PyTuple objects.

  Raises MySQLInterfaceError for any MySQL error returned
  by the MySQL server.

  @param    result      a MySQL result

  @return   PyList of PyTuple objects
    @retval PyList  OK
    @retval NULL    Exception
*/
PyObject*
MySQL_fetch_fields(MySQL *self)
{
    unsigned int count;

    CHECK_SESSION(self);

	if (!self->result)
	{
        raise_with_string(PyString_FromString("No result"), NULL);
        return NULL;
	}

	if (self->fields)
	{
	    return self->fields;
	}

    Py_BEGIN_ALLOW_THREADS
    count= mysql_num_fields(self->result);
    Py_END_ALLOW_THREADS

    return fetch_fields(self->result, count, &self->cs, self->use_unicode);
}

/**
  Fetch the next row from the active result.

  Fetch the next row from the active result. The row is returned
  as a tuple which contains the values converted to Python types,
  unless raw was set.

  The returned PyObject is a PyList which consists of
  PyTuple objects.

  Raises MySQLInterfaceError for any MySQL error returned
  by the MySQL server.

  @param    result      a MySQL result

  @return   PyTuple with row values.
    @retval PyTuple OK
    @retval PyNone  No row available
    @retval NULL    Exception
*/
PyObject*
MySQL_fetch_row(MySQL *self)
{
    MYSQL *session;
	MYSQL_ROW row;
	PyObject *result_row;
	PyObject *field_info;
	PyObject *value;
    PyObject *mod_decimal, *decimal, *dec_args;
	unsigned long *field_lengths;
	unsigned int num_fields;
	unsigned int i;
	unsigned long field_type, field_flags;
	const char *charset= NULL;

    CHECK_SESSION(self);

	if (!self->result)
	{
	    Py_RETURN_NONE;
	}

    session= &self->session;
    charset= my2py_charset_name(session);

    Py_BEGIN_ALLOW_THREADS
    row= mysql_fetch_row(self->result);
    Py_END_ALLOW_THREADS


    if (row == NULL)
    {
        if (mysql_errno(session))
        {
    	    raise_with_session(session, NULL);
	        return NULL;
	    }
	    Py_RETURN_NONE;
    }

    Py_BEGIN_ALLOW_THREADS
    num_fields= mysql_num_fields(self->result);
    field_lengths= mysql_fetch_lengths(self->result);
    Py_END_ALLOW_THREADS
    if (field_lengths == NULL)
    {
	    Py_RETURN_NONE;
    }

    if (self->fields == NULL) {
        self->fields= fetch_fields(self->result, num_fields, &self->cs,
                                   self->use_unicode);
    }

    result_row = PyTuple_New(num_fields);
    for (i= 0; i < num_fields; i++) {
    	if (row[i] == NULL)
    	{
            Py_INCREF(Py_None);
    		PyTuple_SET_ITEM(result_row, i, Py_None);
    		continue;
        }
        // Raw result
        if (self->raw == Py_True)
        {
            if (self->raw_as_string && self->raw_as_string == Py_True)
            {
                PyTuple_SET_ITEM(result_row, i,
    			                 PyStringFromStringAndSize(row[i],
    			                                           field_lengths[i]));
            }
            else
            {
    		    PyTuple_SET_ITEM(result_row, i,
    			                 PyByteArray_FromStringAndSize(row[i],
    			                                               field_lengths[i]));
    		}
    		continue;
        }

        field_info= PyList_GetItem(self->fields, i);
        if (!field_info)
        {
            Py_XDECREF(result_row);
            Py_RETURN_NONE;
        }

        field_type= PyLong_AsUnsignedLong(PyTuple_GetItem(field_info, 8));
        field_flags= PyLong_AsUnsignedLong(PyTuple_GetItem(field_info, 9));

        // Convert MySQL values to Python objects
        if (field_type == MYSQL_TYPE_TINY ||
            field_type == MYSQL_TYPE_SHORT ||
            field_type == MYSQL_TYPE_LONG ||
            field_type == MYSQL_TYPE_LONGLONG ||
            field_type == MYSQL_TYPE_INT24 ||
            field_type == MYSQL_TYPE_YEAR) {
    		PyTuple_SET_ITEM(result_row, i, PyInt_FromString(row[i], NULL, 0));
        }
        else if (field_type == MYSQL_TYPE_DATETIME ||
                 field_type == MYSQL_TYPE_TIMESTAMP)
        {
            PyTuple_SET_ITEM(result_row, i,
                mytopy_datetime(row[i], field_lengths[i]));
        }
        else if (field_type == MYSQL_TYPE_DATE)
        {
            PyTuple_SET_ITEM(result_row, i, mytopy_date(row[i]));
        }
        else if (field_type == MYSQL_TYPE_TIME)
        {
            // The correct conversion is to a timedelta
            PyTuple_SET_ITEM(result_row, i, mytopy_time(row[i],
                             field_lengths[i]));
        }
        else if (field_type == MYSQL_TYPE_VARCHAR ||
                 field_type == MYSQL_TYPE_STRING ||
                 field_type == MYSQL_TYPE_ENUM ||
                 field_type == MYSQL_TYPE_VAR_STRING)
        {
            value= mytopy_string(row[i], field_lengths[i], field_flags,
                                  charset, self->use_unicode);
            if (!value)
            {
                goto error;
            }
            else
            {
                if (field_flags & SET_FLAG)
                {
                    if (!strlen(row[i]))
                    {
                        value= PySet_New(NULL);
                    }
                    else
                    {
                        value= PySet_New(PyUnicode_Split(
                                         value, PyStringFromString(","), -1));
                    }
                    if (!value)
                    {
                        goto error;
                    }
                }
                PyTuple_SET_ITEM(result_row, i, value);
            }
        }
        else if (field_type == MYSQL_TYPE_NEWDECIMAL ||
                 field_type == MYSQL_TYPE_DECIMAL)
        {
            mod_decimal= PyImport_ImportModule("decimal");
            if (mod_decimal) {
                dec_args= PyTuple_New(1);
                PyTuple_SET_ITEM(dec_args, 0, PyStringFromString(row[i]));
                decimal= PyObject_GetAttrString(mod_decimal, "Decimal");
                PyTuple_SET_ITEM(result_row, i,
                                 PyObject_Call(decimal, dec_args, NULL));
                Py_DECREF(dec_args);
                Py_DECREF(decimal);
            }
            Py_XDECREF(mod_decimal);
        }
        else if (field_type == MYSQL_TYPE_FLOAT ||
                 field_type == MYSQL_TYPE_DOUBLE)
        {
#ifdef PY3
            value= PyFloat_FromString(PyStringFromString(row[i]));
#else
            value= PyFloat_FromString(PyStringFromString(row[i]), NULL);
#endif
            if (!value)
            {
                value= Py_None;
            }
            PyTuple_SET_ITEM(result_row, i, value);
        }
        else if (field_type == MYSQL_TYPE_BIT)
        {
            PyTuple_SET_ITEM(result_row, i,
                             mytopy_bit(row[i], field_lengths[i]));
        }
        else if (field_type == MYSQL_TYPE_BLOB)
        {
            value= mytopy_string(row[i], field_lengths[i], field_flags,
                                  charset, self->use_unicode);
            PyTuple_SET_ITEM(result_row, i, value);
        }
        else if (field_type == MYSQL_TYPE_GEOMETRY)
        {
            PyTuple_SET_ITEM(result_row, i,
                             PyByteArray_FromStringAndSize(row[i],
                                                           field_lengths[i]));
    	}
    	else
    	{
    	    // Do our best to convert whatever we got from MySQL to a str/bytes
            value = mytopy_string(row[i], field_lengths[i], field_flags,
                                  charset, self->use_unicode);
    		PyTuple_SET_ITEM(result_row, i, value);
    	}
    }

    return result_row;
error:
    Py_DECREF(result_row);
    return NULL;
}

/**
  Get number of rows in active result.

  @param    self    MySQL instance

  Raises MySQLError when there is no result.

  @return   Number of rows as PyInt
    @retval PyInt   Python v3
    @retval PyLong  Python v2
    @retval NULL    Exception
*/
PyObject*
MySQL_num_rows(MySQL *self)
{
    my_ulonglong count;

    if (!self->result)
    {
        raise_with_string(PyString_FromString(
                          "Statement did not return result set"), NULL);
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS
    count= mysql_num_rows(self->result);
    Py_END_ALLOW_THREADS

    return PyIntFromULongLong(count);
}

/**
  Get number of fields in active result.

  @param    self    MySQL instance

  @return   Number of fields as PyInt
    @retval PyInt   Python v3
    @retval PyLong  Python v2
    @retval None    No result available
*/
PyObject*
MySQL_num_fields(MySQL *self)
{
    unsigned int count;

    if (!self->result)
    {
        Py_RETURN_NONE;
    }

    Py_BEGIN_ALLOW_THREADS
    count= mysql_num_fields(self->result);
    Py_END_ALLOW_THREADS

    return PyIntFromULong(count);
}

/**
  Flush or reset tables and caches.

  Flush or reset tables and caches. The only argument currently
  allowed is an integer.

  Raises TypeError when first argument is not an integer.

  @param    self    MySQL instance
  @param    args    positional arguments

  @return   Number of fields as PyInt
    @retval PyNone  OK
    @retval NULL    Exception
*/
PyObject*
MySQL_refresh(MySQL *self, PyObject *args)
{
	unsigned int options;
	int res;

    IS_CONNECTED(self);

    if (!PyArg_ParseTuple(args, "I", &options))
    {
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS
    res= mysql_refresh(&self->session, options);
    Py_END_ALLOW_THREADS

    if (res)
    {
    	raise_with_session(&self->session, NULL);
    	return NULL;
    }

    Py_RETURN_NONE;
}

/**
  Shut down the MySQL server.

  Shut down the MySQL server. The only argument currently allowed
  is an integer which describes the shutdown type.

  Raises TypeError when first argument is not an integer.
  Raises MySQLErrorInterface when an error is retured by
  the MySQL server.

  @param    self    MySQL instance
  @param    args    positional arguments

  @return   Number of fields as PyInt
    @retval PyNone  OK
    @retval NULL    Exception
*/
PyObject*
MySQL_shutdown(MySQL *self, PyObject *args)
{
    unsigned int level= 0;
    int res;

    CHECK_SESSION(self);

    if (!PyArg_ParseTuple(args, "I", &level))
    {
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS
    res= mysql_shutdown(&self->session, level);
    Py_END_ALLOW_THREADS

    if (res)
    {
    	raise_with_session(&self->session, NULL);
    	return NULL;
    }

    Py_RETURN_NONE;
}

/**
  Get the server status as string.

  Get the server status as string.

  Raises MySQLErrorInterface when an error is retured by
  the MySQL server.

  @param    self    MySQL instance

  @return   Number of fields as PyInt
    @retval PyBytes     Python v3
    @retval PyByteArray Python v2
    @retval NULL        Exception
*/
PyObject*
MySQL_stat(MySQL *self)
{
    const char *stat;

    CHECK_SESSION(self);

    Py_BEGIN_ALLOW_THREADS
    stat= mysql_stat(&self->session);
    Py_END_ALLOW_THREADS

    if (!stat)
    {
    	raise_with_session(&self->session, NULL);
    	return NULL;
    }

#ifndef PY3
    return PyByteArray_FromStringAndSize(stat, strlen(stat));
#else
    return PyBytesFromString(stat);
#endif
}
