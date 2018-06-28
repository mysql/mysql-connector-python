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

#ifndef MYCONNPY_MYSQL_CAPI_CONVERSION_H
#define MYCONNPY_MYSQL_CAPI_CONVERSION_H

#include <Python.h>

PyObject*
pytomy_date(PyObject *obj);

PyObject*
pytomy_datetime(PyObject *obj);

PyObject*
pytomy_timedelta(PyObject *obj);

PyObject*
pytomy_decimal(PyObject *obj);

PyObject*
pytomy_time(PyObject *obj);

PyObject*
mytopy_date(const char *data);

PyObject*
mytopy_datetime(const char *data, const unsigned long length);

PyObject*
mytopy_time(const char *data, const unsigned long length);

PyObject*
datetime_to_mysql(PyObject *self, PyObject *datetime);

PyObject*
time_to_mysql(PyObject *self, PyObject *time);

PyObject*
date_to_mysql(PyObject *self, PyObject *date);

PyObject*
mytopy_bit(const char *data, const unsigned long length);

PyObject*
mytopy_string(const char *data, const unsigned long length,
              const unsigned long flags, const char *charset,
              unsigned int use_unicode);

#endif /* MYCONNPY_MYSQL_CAPI_CONVERSION_H */