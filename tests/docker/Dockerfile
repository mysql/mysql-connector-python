# Copyright (c) 2009, 2023, Oracle and/or its affiliates.
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

# By default the container will use oracle linux 9 from OCR.
# The BASE_IMAGE environment variable can be used to pull
# any other Oracle Linux image from any other Docker registry.
ARG BASE_IMAGE=container-registry.oracle.com/os/oraclelinux:9-slim


FROM ${BASE_IMAGE} as pure_python
# In order to be able to run the tests within a private network,
# optional proxy configurations can be used.
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY
ENV HTTP_PROXY=${HTTP_PROXY} HTTPS_PROXY=${HTTPS_PROXY} NO_PROXY=${NO_PROXY}
WORKDIR /mysql-connector-python
RUN microdnf install -y dnf \
    && dnf install -y gcc-c++ krb5-devel
# Install Python (including dev files), and create a virtual environment
RUN dnf -y install python3-pip python3-devel python3-setuptools
RUN python3 -m venv /pyenvs/venv
ENV PATH="/pyenvs/venv/bin:$PATH"
RUN pip install --upgrade setuptools
# This volume can be used to share, among other things,
# the Unix socket file with the container.
VOLUME [ "/shared" ]
# Providing a private PyPI repository is allowed
# [PIP_INDEX_URL]: https://wiki.onap.org/display/DW/Configure+and+customize+pip
ARG PYPI_REPOSITORY
ENV PIP_INDEX_URL=${PYPI_REPOSITORY:-https://pypi.org/simple}
# Install MySQL Connector/Python (python only)
COPY . .
RUN pip install -r tests/requirements.txt \
    && pip install .


FROM pure_python AS c_extension
RUN dnf install -y yum wget
# Install MySQL including dev files to compile the C Extension
ENV MYSQL_ENDPOINT="mysql80-community-release-el9-1.noarch.rpm"
RUN wget https://dev.mysql.com/get/${MYSQL_ENDPOINT} \
    && yum install -y ${MYSQL_ENDPOINT} \
    && dnf install -y mysql-community-server mysql-community-devel
# Build mysql cext
RUN python setup.py install --with-mysql-capi=/usr/bin/mysql_config
