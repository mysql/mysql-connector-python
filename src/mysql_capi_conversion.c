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

#include <ctype.h>
#include <stdio.h>

#include <Python.h>
#include <datetime.h>

#ifdef MS_WINDOWS
#include <windows.h>
#endif
#include <mysql.h>

#include "catch23.h"
#include "exceptions.h"

#define MINYEAR 1
#define MAXYEAR 9999

/**
  Check whether a year is a leap year.

  Check whether a year is a leap year or not.

  Year is not checked. This helper function is used by other
  functions which validate temporal values.

  @param    year    year

  @return   1 if year is leap year, 0 otherwise.
    @retval 1   Leap year
    @retval 0   Not a leap year
*/
static int
leap_year(int year)
{
    if ((year % 4 == 0) && (year % 100 != 0 || year % 400 == 0))
    {
        return 1;
    }

    return 0;
}

/**
  Return number of days in month.

  Return the number of days in a month considering leap years.

  Year and month are not checked. This helper function is used
  by other functions which validate temporal values.

  @param    year    year
  @param    month   month

  @return   Number of days in month
    @retval int number of days
*/
static int
nr_days_month(int year, int month)
{
    int days[]= {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (month == 2 && leap_year(year))
    {
        return 29;
    }
    return days[month];
}

/**
  Check whether a date is valid or not.

  Check whether the date defined by the arguments year,
  month, and day, is valid or not.

  @param    day     day
  @param    month   month
  @param    day     day

  @return   1 if date is valid, 0 otherwise.
    @retval 1   Valid
    @retval 0   Invalid
*/
static int
is_valid_date(int year, int month, int day)
{
    if ((year < MINYEAR || year > MAXYEAR)
        || (month < 1 || month > 12)
        || (day < 1 || day > nr_days_month(year, month)))
    {
        return 0;
    }

    return 1;
}

/**
  Check whether a time is valid or not.

  Check whether the time defined by the arguments hours,
  mins, secs and usecs, is valid or not.

  @param    hours   hours
  @param    mins    minutes
  @param    secs    secs
  @param    usecs   microsecons

  @return   1 if time is valid, 0 otherwise.
    @retval 1   Valid
    @retval 0   Invalid
*/
static int
is_valid_time(int hours, int mins, int secs, int usecs)
{
    if ((hours < 0 || hours > 23)
        || (mins < 0 || mins > 59)
        || (secs < 0 || secs > 59)
        || (usecs < 0 || usecs > 999999))
    {
        return 0;
    }

    return 1;
}

/**
  Convert a Python datetime.timedelta to MySQL TIME.

  Convert the PyObject obj, which must be a datetime.timedelta,
  to MySQL TIME value.

  Raises TypeError when obj is not a PyDelta_Type.

  @param    obj     the PyObject to be converted

  @return   Converted timedelta object.
    @retval PyBytes     Python v3
    @retval PyString    Python v2
    @retval NULL        Exception
*/
PyObject*
pytomy_timedelta(PyObject *obj)
{
    int days= 0, secs= 0 , micro_secs= 0, total_secs= 0;
    int hours= 0, mins= 0, remainder= 0;
    char fmt[32]= "";
    char result[17]= "";
    char minus[1]= "";

    PyDateTime_IMPORT;

    if (!obj || !PyDelta_Check(obj))
    {
        PyErr_SetString(PyExc_ValueError,
                        "Object must be a datetime.timedelta");
        return NULL;
    }

    // Cannot use PyDateTime_DELTA_* (new in Python v3.3)
    days= ((PyDateTime_Delta*)obj)->days;
    secs= ((PyDateTime_Delta*)obj)->seconds;
    micro_secs= ((PyDateTime_Delta*)obj)->microseconds;

    total_secs= abs(days * 86400 + secs);

#pragma warning(push)
// result of strncpy does not accept direct user input
#pragma warning(disable: 4996)
    if (micro_secs)
    {
        strncpy(fmt, "%s%02d:%02d:%02d.%06d", 21);
        if (days < 0)
        {
            micro_secs= 1000000 - micro_secs;
            total_secs-= 1;
        }
    }
    else
    {
        strncpy(fmt, "%s%02d:%02d:%02d", 16);
    }
#pragma warning(pop)

    if (days < 0)
    {
        minus[0]= '-';
    }

    hours= total_secs / 3600;
    remainder= total_secs % 3600;
    mins= remainder / 60;
    secs= remainder % 60;

    if (micro_secs)
    {
        PyOS_snprintf(result, 17, fmt, minus, hours, mins, secs, micro_secs);
    }
    else
    {
        PyOS_snprintf(result, 17, fmt, minus, hours, mins, secs);
    }

    return PyBytesFromString(result);
}

/**
  Convert a Python datetime.time to MySQL TIME.

  Convert the PyObject obj, which must be a datetime.time,
  to MySQL TIME value.

  Raises TypeError when obj is not a PyTime_Type.

  @param    obj     the PyObject to be converted

  @return   Converted time object.
    @retval PyBytes     Python v3
    @retval PyString    Python v2
    @retval NULL        Exception
*/
PyObject*
pytomy_time(PyObject *obj)
{
    char result[17]= "";

    PyDateTime_IMPORT;

    if (!obj || !PyTime_Check(obj))
    {
        PyErr_SetString(PyExc_ValueError,
                        "Object must be a datetime.time");
        return NULL;
    }

    if (PyDateTime_TIME_GET_MICROSECOND(obj))
    {
        PyOS_snprintf(result, 17, "%02d:%02d:%02d.%06d",
                 PyDateTime_TIME_GET_HOUR(obj),
                 PyDateTime_TIME_GET_MINUTE(obj),
                 PyDateTime_TIME_GET_SECOND(obj),
                 PyDateTime_TIME_GET_MICROSECOND(obj));
    }
    else
    {
        PyOS_snprintf(result, 17, "%02d:%02d:%02d",
                 PyDateTime_TIME_GET_HOUR(obj),
                 PyDateTime_TIME_GET_MINUTE(obj),
                 PyDateTime_TIME_GET_SECOND(obj));
    }

    return PyBytesFromString(result);
}

/**
  Convert a Python datetime.datetime to MySQL DATETIME.

  Convert the PyObject obj, which must be a datetime.datetime,
  to MySQL DATETIME value.

  Raises TypeError when obj is not a PyDateTime_Type.

  @param    obj     the PyObject to be converted

  @return   Converted datetime object.
    @retval PyBytes     Python v3
    @retval PyString    Python v2
    @retval NULL        Exception
*/
PyObject*
pytomy_datetime(PyObject *obj)
{
    char result[27]= "";
    PyDateTime_IMPORT;

    if (!obj || !PyDateTime_Check(obj))
    {
        PyErr_SetString(PyExc_ValueError,
                        "Object must be a datetime.datetime");
        return NULL;
    }

    if (PyDateTime_DATE_GET_MICROSECOND(obj))
    {
        PyOS_snprintf(result, 27, "%04d-%02d-%02d %02d:%02d:%02d.%06d",
                 PyDateTime_GET_YEAR(obj),
                 PyDateTime_GET_MONTH(obj),
                 PyDateTime_GET_DAY(obj),
                 PyDateTime_DATE_GET_HOUR(obj),
                 PyDateTime_DATE_GET_MINUTE(obj),
                 PyDateTime_DATE_GET_SECOND(obj),
                 PyDateTime_DATE_GET_MICROSECOND(obj));
    }
    else
    {
        PyOS_snprintf(result, 27, "%04d-%02d-%02d %02d:%02d:%02d",
                 PyDateTime_GET_YEAR(obj),
                 PyDateTime_GET_MONTH(obj),
                 PyDateTime_GET_DAY(obj),
                 PyDateTime_DATE_GET_HOUR(obj),
                 PyDateTime_DATE_GET_MINUTE(obj),
                 PyDateTime_DATE_GET_SECOND(obj));
    }

    return PyBytesFromString(result);
}

/**
  Convert a Python datetime.date to MySQL DATE.

  Convert the PyObject obj, which must be a datetime.date,
  to MySQL DATE value.

  Raises TypeError when obj is not a PyDate_Type.

  @param    date    the PyObject to be converted

  @return   Converted date object.
    @retval PyBytes     Python v3
    @retval PyString    Python v2
    @retval NULL        Exception
*/
PyObject*
pytomy_date(PyObject *obj)
{
    PyDateTime_IMPORT;

    if (!obj || !PyDate_Check(obj))
    {
        PyErr_SetString(PyExc_TypeError, "Object must be a datetime.date");
        return NULL;
    }

    return PyBytesFromFormat("%04d-%02d-%02d",
                             PyDateTime_GET_YEAR(obj),
                             PyDateTime_GET_MONTH(obj),
                             PyDateTime_GET_DAY(obj));
}

/**
  Convert a DATE MySQL value to Python datetime.date.

  Convert a DATETIME MySQL value to Python datetime.date. When a date
  can be parsed, but it is invalid, None is returned.

  Raises ValueError when the date is not for format %d-%d-%d.

  @param    data        string to be converted

  @return   datetime.date object.
    @retval PyDate  OK
    @retval None    Invalid date
    @retval NULL    Exception
*/
PyObject*
mytopy_date(const char *data)
{
    int year= 0, month= 0, day= 0;

    PyDateTime_IMPORT;

#pragma warning(push)
// sscanf data comes from MySQL and is fixed
#pragma warning(disable: 4996)
    if (3 == sscanf(data, "%d-%d-%d", &year, &month, &day))
#pragma warning(pop)
    {
        // Invalid dates are returned as None instead of raising ValueError
        if (!is_valid_date(year, month, day))
        {
            Py_RETURN_NONE;
        }
        return PyDate_FromDate(year, month, day);
    }

    PyErr_SetString(PyExc_ValueError,
                    "Received incorrect DATE value from MySQL server");
    return NULL;
}

/**
  Convert a DATETIME MySQL value to Python datetime.datetime.

  Convert a DATETIME MySQL value to Python datetime.datetime. The
  fractional part is supported.

  @param    data        string to be converted
  @param    length      length of data

  @return   datetime.datetime object.
    @retval PyDateTime OK
*/
PyObject*
mytopy_datetime(const char *data, const unsigned long length)
{
	int year= 0, month= 0, day= 0;
	int hours= 0, mins= 0, secs= 0, usecs= 0;
    int value= 0;
    int parts[7]= {0};
    int part= 0;
    const char *end= data + length;

    PyDateTime_IMPORT;

    /* Parse year, month, days, hours, minutes and seconds */
    for (;;)
    {
        for (value= 0; data != end && isdigit(*data) ; data++)
        {
            value= (value * 10) + (unsigned int)(*data - '0');
        }
        parts[part++]= (unsigned int)value;
        if (part == 8 || (end-data) < 2
            || (*data != '-' && *data != ':' && *data != ' ')
            || !isdigit(data[1]))
        {
            break;
        }
        data++;  // skip separators '-' and ':'
    }

    if (data != end && end - data >= 2 && *data == '.')
    {
        // Fractional part
        int field_length= 6;   // max fractional - 1
        data++;
        value= (unsigned int)(*data - '0');
        while (data++ != end && isdigit(*data))
        {
            if (field_length-- > 0)
            {
                value= (value * 10) + (unsigned int)(*data - '0');
            }
        }
        parts[6]= value;
    }

    year= parts[0];
    month= parts[1];
    day= parts[2];
    hours= parts[3];
    mins= parts[4];
    secs= parts[5];
    usecs= parts[6];

    if (!is_valid_date(year, month, day))
    {
        Py_RETURN_NONE;
    }

    if (!is_valid_time(hours, mins, secs, usecs))
    {
        Py_RETURN_NONE;
    }

    return PyDateTime_FromDateAndTime(year, month, day,
                                      hours, mins, secs, usecs);
}

/**
  Convert a TIME MySQL value to Python datetime.timedelta.

  Convert a TIME MySQL value to a Python datetime.timedelta returned
  as PyDelta_FromDSU object.

  @param    data        string to be converted
  @param    length      length of data

  @return   datetime.timedelta object.
    @retval PyDelta_FromDSU OK
*/
PyObject*
mytopy_time(const char *data, const unsigned long length)
{
    int hr= 0, min= 0, sec= 0, usec= 0;
    int days= 0, hours= 0, seconds= 0;
    int negative= 0;
    int value= 0;
    int parts[4]= {0};
    int part= 0;
    const char *end= data + length;

    PyDateTime_IMPORT;

    // Negative times
    if (*data == '-')
    {
        negative= 1;
        data++;
    }

    /* Parse hours, minutes and seconds */
    for (;;)
    {
        for (value= 0; data != end && isdigit(*data) ; data++)
        {
            value= (value * 10) + (unsigned int)(*data - '0');
        }
        parts[part++]= (unsigned int)value;
        if (part == 4 || (end-data) < 2 || *data != ':' || !isdigit(data[1]))
        {
            break;
        }
        data++;  // skip time separator ':'
    }

    if (data != end && end - data >= 2 && *data == '.')
    {
        // Fractional part
        int field_length= 5;
        data++;
        value= (unsigned int)(*data - '0');
        while (data++ != end && isdigit(*data))
        {
            if (field_length-- > 0)
            {
                value= (value * 10) + (unsigned int)(*data - '0');
            }
        }
        if (field_length >= 0)
        {
            while (field_length-- > 0)
            {
                value*= 10;
            }
        }
        parts[3]= value;
    }

    hr= parts[0];
    min= parts[1];
    sec= parts[2];
    usec= parts[3];

    // negative time
    if (negative) {
        hr= hr * -1;
        min= min * -1;
        sec= sec * -1;
        usec= usec * -1;
    }

    days= hr / 24;
    hours= hr % 24;

    seconds= (hours * 3600) + (min * 60) + sec;

    return PyDelta_FromDSU(days, seconds, usec);
}

/**
  Convert a Python datetime.datetime to MySQL DATETIME.

  Convert a Python datetime.datetime to MySQL DATETIME using the
  pytomy_date()function.

  datetime_to_mysql() is a module function and can be used as
  _mysql_connector.datetime_to_mysql.

  Raises TypeError when obj is not a PyDateTime_Type.

  @param    self        module instance
  @param    datetime    the PyObject to be converted

  @return   Converted datetime object.
    @retval PyBytes     Python v3
    @retval PyString    Python v2
    @retval NULL        Exception
*/
PyObject*
datetime_to_mysql(PyObject *self, PyObject *datetime)
{
    return pytomy_datetime(datetime);
}

/**
  Convert a Python datetime.time to MySQL TIME.

  Convert a Python datetime.time to MySQL TIME using the
  pytomy_time()function.

  time_to_mysql() is a module function and can be used as
  _mysql_connector.time_to_mysql.

  Raises TypeError when obj is not a PyTime_Type.

  @param    self    module instance
  @param    time    the PyObject to be converted

  @return   Converted time object.
    @retval PyBytes     Python v3
    @retval PyString    Python v2
    @retval NULL        Exception
*/
PyObject*
time_to_mysql(PyObject *self, PyObject *time)
{
    return pytomy_time(time);
}

/**
  Convert a Python datetime.date to MySQL DATE.

  Convert a Python datetime.date to MySQL DATE using the
  pytomy_date()function.

  date_to_mysql() is a module function and can be used as
  _mysql_connector.date_to_mysql.

  Raises TypeError when obj is not a PyDate_Type.

  @param    self    module instance
  @param    date    the PyObject to be converted

  @return   Converted date object.
    @retval PyBytes     Python v3
    @retval PyString    Python v2
    @retval NULL        Exception
*/
PyObject*
date_to_mysql(PyObject *self, PyObject *date)
{
    return pytomy_date(date);
}

/**
  Convert a MySQL BIT to Python int/long.

  @param    obj         PyObject to be converted

  @return   Converted decimal as string
    @retval PyInt   Python v3
    @retval PyLong  Python v2
*/
PyObject*
mytopy_bit(const char *data, const unsigned long length)
{
#ifdef HAVE_LONG_LONG
    unsigned PY_LONG_LONG value= 0;
#else
    unsigned PY_LONG value= 0;
#endif
    const unsigned char *d= (const unsigned char*)data;
    unsigned long size= length;
    while (size > 0)
    {
        value= (value << 8) | *d++;
        size--;
    }
#ifdef HAVE_LONG_LONG
    return PyIntFromULongLong(value);
#else
    return PyIntFromULong(value);
#endif
}

/**
  Convert a Python decimal.Decimal to MySQL DECIMAL.

  Convert a Python decimal.Decimal to MySQL DECIMAL. This function also
  removes the 'L' suffix from the resulting string when using Python v2.

  @param    obj         PyObject to be converted

  @return   Converted decimal as string
    @retval PyBytes     Python v3
    @retval PyString    Python v2
*/
PyObject*
pytomy_decimal(PyObject *obj)
{
#ifdef PY3
    return PyBytes_FromString((const char *)PyUnicode_1BYTE_DATA(
                              PyObject_Str(obj)));
#else
    PyObject *numeric, *new_num;
    int tmp_size;
    char *tmp;

    numeric= PyObject_Str(obj);
    tmp= PyString_AsString(numeric);
    tmp_size= (int)PyString_Size(numeric);
    if (tmp[tmp_size - 1] == 'L')
    {
        new_num= PyString_FromStringAndSize(tmp, tmp_size);
        _PyString_Resize(&new_num, tmp_size - 1);
        return new_num;
    }
    else
    {
        return numeric;
    }

#endif
}

/**
  Convert a string MySQL value to Python str or bytes.

  Convert, and decode if needed, a string MySQL value to
  Python str or bytes.

  @param    data        string to be converted
  @param    length      length of data
  @param    flags       field flags
  @param    charset     character used for decoding
  @param    use_unicode return Unicode

  @return   Converted string
    @retval PyUnicode   if not BINARY_FLAG
    @retval PyBytes     Python v3 if not use_unicode
    @retval PyString    Python v2 if not use_unicode
    @retval NULL    Exception
 */
PyObject*
mytopy_string(const char *data, const unsigned long length,
              const unsigned long flags, const char *charset,
              unsigned int use_unicode)
{
    if (!charset || !data) {
        printf("\n==> here ");
        if (charset) {
            printf(" charset:%s", charset);
        }
        if (data) {
            printf(" data:'%s'", data);
        }
        printf("\n");
        return NULL;
    }

    if (!(flags & BINARY_FLAG) && use_unicode && strcmp(charset, "binary") != 0)
    {
        return PyUnicode_Decode(data, length, charset, NULL);
    }
    else
    {
#ifndef PY3
        return PyStringFromStringAndSize(data, length);
#else
        return PyBytes_FromStringAndSize(data, length);
#endif
    }
}
