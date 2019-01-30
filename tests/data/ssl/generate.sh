#!/bin/sh
# Copyright (c) 2012, 2018, Oracle and/or its affiliates. All rights reserved.
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

# This shell script generates keys and certificates for testing the SSL
# capabilities of MySQL Connector/Python.
#
# Usage:
#  shell> sh generate.sh [destination_folder]
#
DAYS=3306
OU="MySQLConnectorPython"
DESTDIR="."

OPENSSL=`which openssl`
if [ $? -ne 0 ]; then
    echo "openssl not found. Please make sure openssl is in your PATH."
    exit 1
fi

# Destination directory for generate files
if [ "$1" != "" ]; then
    DESTDIR=$1
fi
if [ ! -d $DESTDIR ]; then
    echo "Need a valid destination directory for generated files."
    exit 2
fi

mkdir -p $DESTDIR/ca.db.certs   # Signed certificates storage
touch $DESTDIR/ca.db.index      # Index of signed certificates
echo 01 > $DESTDIR/ca.db.serial # Next (sequential) serial number

# Configuration
cat>$DESTDIR/ca.conf<<'EOF'
[ ca ]
default_ca = ca_default

[ ca_default ]
dir = REPLACE_LATER
certs = $dir
new_certs_dir = $dir/ca.db.certs
database = $dir/ca.db.index
serial = $dir/ca.db.serial
RANDFILE = $dir/ca.db.rand
certificate = $dir/ca.crt
private_key = $dir/ca.key
default_days = 365
default_crl_days = 30
default_md = sha256
preserve = no
policy = generic_policy
[ generic_policy ]
countryName = optional
stateOrProvinceName = optional
localityName = optional
organizationName = optional
organizationalUnitName = optional
commonName = supplied
emailAddress = optional
EOF

sed -i "s|REPLACE_LATER|$DESTDIR|" $DESTDIR/ca.conf

echo
echo "Generating Root Certificate"
echo
$OPENSSL genrsa -out $DESTDIR/tests_CA_key.pem 2048
if [ $? -ne 0 ]; then
    exit 3
fi
SUBJ="/OU=$OU Root CA/CN=MyConnPy Root CA"
$OPENSSL req -new -key $DESTDIR/tests_CA_key.pem \
    -out $DESTDIR/tests_CA_req.csr -subj "$SUBJ"
if [ $? -ne 0 ]; then
    exit 3
fi
$OPENSSL x509 -req -days $DAYS \
    -in $DESTDIR/tests_CA_req.csr \
    -out $DESTDIR/tests_CA_cert.pem \
    -signkey $DESTDIR/tests_CA_key.pem
if [ $? -ne 0 ]; then
    exit 3
fi

# MySQL Server Certificate: generate, remove passphrase, sign
echo
echo "Generating Server Certificate"
echo
SUBJ="/OU=$OU Server Cert/CN=localhost"
$OPENSSL genrsa -out $DESTDIR/tests_server_key.pem 2048
if [ $? -ne 0 ]; then
    exit 3
fi
$OPENSSL req -new -key $DESTDIR/tests_server_key.pem \
    -out $DESTDIR/tests_server_req.csr -subj "$SUBJ"
if [ $? -ne 0 ]; then
    exit 3
fi
$OPENSSL ca -config $DESTDIR/ca.conf -in $DESTDIR/tests_server_req.csr \
    -cert $DESTDIR/tests_CA_cert.pem \
    -keyfile $DESTDIR/tests_CA_key.pem \
    -out $DESTDIR/tests_server_cert.pem -batch
if [ $? -ne 0 ]; then
    exit 3
fi

# MySQL Root Certificate: generate, remove passphrase, sign
echo
echo "Generating Another Root Certificate"
echo
$OPENSSL genrsa -out $DESTDIR/tests_CA_key_1.pem 2048
if [ $? -ne 0 ]; then
    exit 3
fi
SUBJ="/OU=$OU Root CA/CN=MyConnPy Root CA"
$OPENSSL req -new -key $DESTDIR/tests_CA_key_1.pem \
    -out $DESTDIR/tests_CA_req_1.csr -subj "$SUBJ"
if [ $? -ne 0 ]; then
    exit 3
fi
$OPENSSL x509 -req -days $DAYS \
    -in $DESTDIR/tests_CA_req_1.csr \
    -out $DESTDIR/tests_CA_cert_1.pem \
    -signkey $DESTDIR/tests_CA_key_1.pem
if [ $? -ne 0 ]; then
    exit 3
fi

# MySQL Client Certificate: generate, remove passphrase, sign
echo
echo "Generating Client Certificate"
echo
SUBJ="/OU=$OU Client Cert/CN=localhost"
$OPENSSL genrsa -out $DESTDIR/tests_client_key.pem 2048
if [ $? -ne 0 ]; then
    exit 3
fi
$OPENSSL req -new -key $DESTDIR/tests_client_key.pem \
    -out $DESTDIR/tests_client_req.csr -subj "$SUBJ"
if [ $? -ne 0 ]; then
    exit 3
fi
$OPENSSL ca -config $DESTDIR/ca.conf -in $DESTDIR/tests_client_req.csr \
    -cert $DESTDIR/tests_CA_cert.pem \
    -keyfile $DESTDIR/tests_CA_key.pem \
    -out $DESTDIR/tests_client_cert.pem -batch
if [ $? -ne 0 ]; then
    exit 3
fi

# Clean up
echo
echo "Cleaning up"
echo
(cd $DESTDIR; rm -rf tests_server_req.pem tests_client_req.pem \
    ca.db.certs ca.db.index* ca.db.serial* ca.conf tests_CA_req.csr \
    tests_server_req.csr tests_CA_req_1.csr tests_client_req.csr)

