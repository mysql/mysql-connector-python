#!/bin/bash
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

function exists_in_list() {
    LIST=$1
    DELIMITER=$2
    VALUE=$3
    LIST_WHITESPACES=`echo $LIST | tr "$DELIMITER" " "`
    for x in $LIST_WHITESPACES; do
        if [ "$x" = "$VALUE" ]; then
            return 0
        fi
    done
    return 1
}

# localvar=${ENVAR:-default} means `localvar` will store
# a value of whatever `ENVAR` is, if it exists. If such
# an environment variable is unset, then `default` is used
# instead.
version_name=${MYSQL_VERSION:-latest}
image_name=mysql-connector-python
basedir=$(dirname $0)
list_of_true_values="true True yes Yes"

if exists_in_list "$list_of_true_values" " " "$MYSQL_CEXT" \
    && exists_in_list "$list_of_true_values" " " "$MYSQLX_CEXT";
then
    version_name=${version_name}-mysql-mysqlx-cext
    MYSQL_BUILD_CEXT="mysql_mysqlx_cext"
    echo "C-EXT for classic and XdevAPI protocols enabled!"
elif exists_in_list "$list_of_true_values" " " "$MYSQL_CEXT" \
    && ! exists_in_list "$list_of_true_values" " " "$MYSQLX_CEXT";
then
    version_name=${version_name}-mysql-cext
    MYSQL_BUILD_CEXT="mysql_cext"
    echo "C-EXT for classic protocol enabled!"
elif ! exists_in_list "$list_of_true_values" " " "$MYSQL_CEXT" \
    && exists_in_list "$list_of_true_values" " " "$MYSQLX_CEXT";
then
    version_name=${version_name}-mysqlx-cext
    MYSQL_BUILD_CEXT="mysqlx_cext"
    echo "C-EXT for XdevAPI protocol enabled!"
else
    echo "C-EXT disabled!"
fi

# The BASE_IMAGE, HTTPS_PROXY, HTTP_PROXY, NO_PROXY and PYPI_REPOSITORY
# environment variables are used as build arguments. Unless they are
# explicitly specified, the script will use their system-wide values.
# It should be possible to run script from anywhere in the file system. So,
# the absolute Dockerfile and context paths should be specified.
docker build \
    --build-arg BASE_IMAGE \
    --build-arg HTTP_PROXY \
    --build-arg HTTPS_PROXY \
    --build-arg NO_PROXY \
    --build-arg PYPI_REPOSITORY \
    --file $basedir/Dockerfile \
    --tag $image_name:$version_name \
    --target ${MYSQL_BUILD_CEXT:-pure_python} \
    $basedir/../../

# If MYSQL_HOST is empty, "localhost" should be used by default.
# The variable needs to be re-assigned in order to determine if it contains a
# loopback address (implicitly or explicitly).
if [ -z "$MYSQL_HOST" ]
then
    MYSQL_HOST="localhost"
fi

# If MYSQL_HOST is a loopback address, a new flag is created. This flag
# allows to determine the network mode in which the Docker container will
# run. The container should run in "host" mode if MYSQL_HOST is a loopback
# address and should run in "bridge" mode (default) if MYSQL_HOST is
# a different host name or IP address.
if [ "$MYSQL_HOST" = "localhost" ] || [ "$MYSQL_HOST" = "127.0.0.1" ]
then
    MYSQL_LOCALHOST=$MYSQL_HOST
fi

# localvar=${ENVAR:+action} means `localvar` will trigger `action`
# and store the value produced (if any) if `ENVAR` is not empty/null,
# else `localvar` will be empty.

# If MYSQL_SOCKET is not empty, the corresponding path to the Unix socket
# file should be shared with the container using a Docker volume.
# Additionally, the variable should be assigned the appropriate absolute
# file path from the container standpoint.
# If MYSQL_LOCALHOST is not empty, the container should run using the "host"
# network mode. If it is empty, the container should run using the "bridge"
# network mode, which is what happens by default.
docker run \
    --rm \
    --interactive \
    --tty \
    ${MYSQL_LOCALHOST:+ --network host} \
    ${MYSQL_SOCKET:+ --volume $MYSQL_SOCKET:/shared/mysqlx.sock} \
    ${MYSQL_SOCKET:+ --env MYSQL_SOCKET=/shared/mysqlx.sock} \
    --env MYSQL_USER \
    --env MYSQL_PASSWORD \
    --env MYSQL_HOST \
    --env MYSQL_PORT \
    --env MYSQLX_PORT \
    $image_name:$version_name \
    python unittests.py \
        --use-external-server \
        --verbosity 2 \
        ${TEST_PATTERN:+ -r $TEST_PATTERN}
