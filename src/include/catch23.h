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

#ifndef MYCONNPY_CATCH23_H
#define MYCONNPY_CATCH23_H

#include <Python.h>

#if PY_MAJOR_VERSION >= 3
    #define PY3
    #define PyString PyUnicode
    #define PyString_FromString PyUnicode_FromString
    #define PyString_FromStringAndSize PyUnicode_FromStringAndSize
    #define PyString_AS_STRING PyUnicode_AS_UNICODE
    #define PyString_Concat PyUnicode_Concat
    #define PyInt_FromString PyLong_FromString
    #define PyInt_FromLong PyLong_FromLong
    #define PyInt_FromUnsignedLong PyLong_FromUnsignedLong
#endif

#if PY_MAJOR_VERSION >= 3
    #define PyIntType PyLong_Type
    #define PyIntCheck PyLong_Check
    #define PyStringType PyUnicode_Type
    #define PyIntLong_Check(o) PyLong_Check(o)

    #define PyIntAsLong PyLong_AsLong
    #define PyIntAsULong PyLong_AsUnsignedLong

    #define BytesResize _PyBytes_Resize
    #define BytesSize PyBytes_Size
    #define PyBytesAsString PyBytes_AsString
    #define BytesFromStringAndSize PyBytes_FromStringAndSize
    #define PyStringAsString PyUnicode_AsUTF8

    #define PyStringFromString PyUnicode_FromString
    #define PyStringFromStringAndSize PyUnicode_FromStringAndSize
    #define UnicodeFromStringAndSize PyUnicode_FromStringAndSize
    #define PyIntFromULongLong PyLong_FromUnsignedLongLong
    #define PyIntFromULong PyLong_FromUnsignedLong
    #define PyBytesFromString PyBytes_FromString
    #define PyBytesFromFormat PyBytes_FromFormat

    #define PyString_Check PyUnicode_Check
    #define PyString_FromFormat PyUnicode_FromString
#else
    #define PyIntType PyInt_Type
    #define PyIntCheck PyInt_Check
    #define PyStringType PyString_Type
    #define PyIntLong_Check(o) PyInt_Check(o) || PyLong_Check(o)

    #define PyIntAsLong PyInt_AsLong
    #define PyIntAsULong PyLong_AsUnsignedLong

    #define BytesResize _PyString_Resize
    #define BytesSize PyString_Size
    #define PyBytesAsString PyString_AsString
    #define BytesFromStringAndSize PyString_FromStringAndSize
    #define PyStringAsString PyString_AsString
    #define PyStringFromString PyString_FromString
    #define PyStringFromStringAndSize PyString_FromStringAndSize
    #define UnicodeFromStringAndSize PyUnicode_FromStringAndSize
    #define PyIntFromULongLong PyLong_FromUnsignedLongLong
    #define PyIntFromULong PyLong_FromUnsignedLong
    #define PyBytesFromString PyString_FromString
    #define PyBytesFromFormat PyString_FromFormat
#endif

#endif