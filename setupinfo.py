# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2009, 2017, Oracle and/or its affiliates. All rights reserved.

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

from setuptools import Extension
import os
import sys

from lib.cpy_distutils import (
    Install, InstallLib, BuildExtDynamic, BuildExtStatic,
    BDistWheel,
)

# Development Status Trove Classifiers significant for Connector/Python
DEVELOPMENT_STATUSES = {
    'a': '3 - Alpha',
    'b': '4 - Beta',
    'rc': '4 - Beta',  # There is no Classifier for Release Candidates
    '': '5 - Production/Stable'
}

if not (((2, 6) <= sys.version_info < (3, 0)) or sys.version_info >= (3, 3)):
    raise RuntimeError("Python v{major}.{minor} is not supported".format(
        major=sys.version_info[0], minor=sys.version_info[1]
    ))

# Load version information
VERSION = [999, 0, 0, 'a', 0]  # Set correct after version.py is loaded
version_py = os.path.join('lib', 'mysql', 'connector', 'version.py')
with open(version_py, 'rb') as fp:
    exec(compile(fp.read(), version_py, 'exec'))

BuildExtDynamic.min_connector_c_version = (5, 5, 8)
command_classes = {
    'build_ext': BuildExtDynamic,
    'build_ext_static': BuildExtStatic,
    'install_lib': InstallLib,
    'install': Install,
    'bdist_wheel': BDistWheel,
}

package_dir = {'': 'lib'}
name = 'mysql-connector-python'
version = '{0}.{1}.{2}'.format(*VERSION[0:3])
mysqlxpb_macros = [("PY3", 1,)] if sys.version_info[0] == 3 else []
extensions = [
    Extension("_mysql_connector",
              sources=[
                  "src/exceptions.c",
                  "src/mysql_capi.c",
                  "src/mysql_capi_conversion.c",
                  "src/mysql_connector.c",
                  "src/force_cpp_linkage.cc",
              ],
              include_dirs=['src/include']),
    Extension(name="_mysqlxpb",
              define_macros=mysqlxpb_macros,
              sources=[
                  "src/mysqlxpb/mysqlx/mysqlx.pb.cc",
                  "src/mysqlxpb/mysqlx/mysqlx_connection.pb.cc",
                  "src/mysqlxpb/mysqlx/mysqlx_crud.pb.cc",
                  "src/mysqlxpb/mysqlx/mysqlx_datatypes.pb.cc",
                  "src/mysqlxpb/mysqlx/mysqlx_expect.pb.cc",
                  "src/mysqlxpb/mysqlx/mysqlx_expr.pb.cc",
                  "src/mysqlxpb/mysqlx/mysqlx_notice.pb.cc",
                  "src/mysqlxpb/mysqlx/mysqlx_resultset.pb.cc",
                  "src/mysqlxpb/mysqlx/mysqlx_session.pb.cc",
                  "src/mysqlxpb/mysqlx/mysqlx_sql.pb.cc",
                  "src/mysqlxpb/mysqlxpb.cc"
              ])
]

packages = [
    'mysql',
    'mysql.connector',
    'mysql.connector.locales',
    'mysql.connector.locales.eng',
    'mysql.connector.django',
    'mysql.connector.fabric',
    'mysqlx',
    'mysqlx.locales',
    'mysqlx.locales.eng',
]
description = "MySQL driver written in Python"
long_description = """
MySQL driver written in Python which does not depend on MySQL C client
libraries and implements the DB API v2.0 specification (PEP-249).
"""
author = 'Oracle and/or its affiliates'
author_email = ''
maintainer = 'Nuno Mariz'
maintainer_email = 'nuno.mariz@oracle.com'
cpy_gpl_license = "GNU GPLv2 (with FOSS License Exception)"
keywords = "mysql db",
url = 'http://dev.mysql.com/doc/connector-python/en/index.html'
download_url = 'http://dev.mysql.com/downloads/connector/python/'
classifiers = [
    'Development Status :: %s' % (DEVELOPMENT_STATUSES[VERSION[3]]),
    'Environment :: Other Environment',
    'Intended Audience :: Developers',
    'Intended Audience :: Education',
    'Intended Audience :: Information Technology',
    'Intended Audience :: System Administrators',
    'License :: OSI Approved :: GNU General Public License (GPL)',
    'Operating System :: OS Independent',
    'Programming Language :: Python :: 2.6',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.1',
    'Programming Language :: Python :: 3.2',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Topic :: Database',
    'Topic :: Software Development',
    'Topic :: Software Development :: Libraries :: Application Frameworks',
    'Topic :: Software Development :: Libraries :: Python Modules'
]
