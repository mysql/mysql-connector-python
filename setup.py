#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2009, 2017, Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0, as
# published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an
# additional permission to link the program and your derivative works
# with the separately licensed software that they have included with
# MySQL.
#
# Without limiting anything contained in the foregoing, this file,
# which is part of MySQL Connector/Python, is also subject to the
# Universal FOSS Exception, version 1.0, a copy of which can be found at
# http://oss.oracle.com/licenses/universal-foss-exception.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

"""

To install MySQL Connector/Python:

    shell> python ./setup.py install

"""

from setuptools import setup
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
    install_requires=setupinfo.install_requires,
)

