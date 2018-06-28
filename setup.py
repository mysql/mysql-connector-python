#!/usr/bin/env python
# -*- coding: utf-8 -*-
# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2009, 2014, Oracle and/or its affiliates. All rights reserved.

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

"""

To install MySQL Connector/Python:

    shell> python ./setup.py install

"""

from distutils.core import setup
from distutils.command.install import INSTALL_SCHEMES

# Make sure that data files are actually installed in the package directory
for install_scheme in INSTALL_SCHEMES.values():
    install_scheme['data'] = install_scheme['purelib']

import setupinfo
try:
    from cpyint import metasetupinfo
    setupinfo.command_classes.update(metasetupinfo.command_classes)
except (ImportError, AttributeError):
    # python-internal not available
    pass

setup(
    name=setupinfo.name,
    version=setupinfo.version,
    description=setupinfo.description,
    long_description=setupinfo.long_description,
    author=setupinfo.author,
    author_email=setupinfo.author_email,
    license=setupinfo.cpy_gpl_license,
    keywords=setupinfo.keywords,
    url=setupinfo.url,
    download_url=setupinfo.download_url,
    package_dir=setupinfo.package_dir,
    packages=setupinfo.packages,
    classifiers=setupinfo.classifiers,
    cmdclass=setupinfo.command_classes,
    ext_modules=setupinfo.extensions,
)

