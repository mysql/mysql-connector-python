/*
# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2014, Oracle and/or its affiliates. All rights reserved.

# MySQL Connector/Python is licensed under the terms of the GPLv2
# <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most
# MySQL Connectors. There are special exceptions to the terms and
# conditions of the GPLv2 as it is applied to this software, see the
# FOSS License Exception
# <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
*/

#ifndef MYCONNPY_MYSQL_CAPI_H
#define MYCONNPY_MYSQL_CAPI_H

#include <Python.h>
#include "structmember.h"

typedef struct {
    PyObject_HEAD
    // private
    MYSQL session;
    MYSQL_RES *result;
    unsigned char connected;
    int result_num_fields;
    unsigned int use_unicode;
    PyObject *buffered;
    PyObject *raw;
    PyObject *raw_as_string;
    PyObject *buffered_at_connect;
    PyObject *raw_at_connect;
    PyObject *charset_name;
    PyObject *have_result_set;
    PyObject *fields;
    PyObject *auth_plugin;
    MY_CHARSET_INFO cs;
    unsigned int connection_timeout;
    // class members

} MySQL;

void
MySQL_dealloc(MySQL *self);

PyObject *
MySQL_new(PyTypeObject *type, PyObject *args, PyObject *kwds);

int
MySQL_init(MySQL *self, PyObject *args, PyObject *kwds);

PyObject *
MySQL_buffered(MySQL *self, PyObject *args);

PyObject *
MySQL_raw(MySQL *self, PyObject *args);

PyObject *
MySQL_connected(MySQL *self);

PyObject*
MySQL_st_affected_rows(MySQL *self);

PyObject*
MySQL_st_client_flag(MySQL *self);

PyObject*
MySQL_st_field_count(MySQL *self);

PyObject*
MySQL_st_insert_id(MySQL *self);

PyObject*
MySQL_st_server_capabilities(MySQL *self);

PyObject*
MySQL_st_server_status(MySQL *self);

PyObject*
MySQL_st_warning_count(MySQL *self);

PyObject*
MySQL_convert_to_mysql(MySQL *self, PyObject *args);

PyObject*
MySQL_handle_result(MySQL *self);

PyObject*
MySQL_consume_result(MySQL *self);

PyObject*
MySQL_reset_result(MySQL *self);

/*
 * MySQL C API functions mapping
 */

PyObject*
MySQL_autocommit(MySQL *self, PyObject *mode);

PyObject*
MySQL_affected_rows(MySQL *self);

PyObject*
MySQL_change_user(MySQL *self, PyObject *args, PyObject *kwds);

PyObject*
MySQL_character_set_name(MySQL *self);

PyObject*
MySQL_commit(MySQL *self);

PyObject*
MySQL_connect(MySQL *self, PyObject *args, PyObject *kwds);

PyObject*
MySQL_close(MySQL *self);

PyObject*
MySQL_escape_string(MySQL *self, PyObject *value);

PyObject*
MySQL_fetch_fields(MySQL *self);

PyObject*
MySQL_fetch_row(MySQL *self);

PyObject*
MySQL_field_count(MySQL *self);

PyObject*
MySQL_free_result(MySQL *self);

PyObject*
MySQL_get_character_set_info(MySQL *self);

PyObject*
MySQL_get_client_info(MySQL *self);

PyObject*
MySQL_get_client_version(MySQL *self);

PyObject*
MySQL_get_host_info(MySQL *self);

PyObject*
MySQL_get_proto_info(MySQL *self);

PyObject*
MySQL_get_server_info(MySQL *self);

PyObject*
MySQL_get_server_version(MySQL *self);

PyObject*
MySQL_get_ssl_cipher(MySQL *self);

PyObject*
MySQL_hex_string(MySQL *self, PyObject *value);

PyObject*
MySQL_insert_id(MySQL *self);

PyObject*
MySQL_next_result(MySQL *self);

PyObject*
MySQL_num_fields(MySQL *self);

PyObject*
MySQL_num_rows(MySQL *self);

PyObject*
MySQL_more_results(MySQL *self);

PyObject*
MySQL_ping(MySQL *self);

PyObject*
MySQL_query(MySQL *self, PyObject *args, PyObject *kwds);

PyObject*
MySQL_refresh(MySQL *self, PyObject *args);

PyObject*
MySQL_rollback(MySQL *self);

PyObject*
MySQL_select_db(MySQL *self, PyObject *db);

PyObject*
MySQL_set_character_set(MySQL *self, PyObject *args);

PyObject*
MySQL_shutdown(MySQL *self, PyObject *args);

PyObject*
MySQL_stat(MySQL *self);

PyObject*
MySQL_thread_id(MySQL *self);

PyObject*
MySQL_use_unicode(MySQL *self, PyObject *args);

PyObject*
MySQL_warning_count(MySQL *self);


#endif /* MYCONNPY_MYSQL_CAPI_H */
