# Copyright (c) 2024, Oracle and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0, as
# published by the Free Software Foundation.
#
# This program is designed to work with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation. The authors of MySQL hereby grant you an
# additional permission to link the program and your derivative works
# with the separately licensed software that they have either included with
# the program or referenced in the documentation.
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

"""Cipher and cipher-suite Tests."""

import ast
import platform
import unittest

from contextlib import nullcontext

import tests

import mysqlx

from mysqlx.errors import InterfaceError, NotSupportedError
from mysqlx.tls_ciphers import (
    APPROVED_TLS_CIPHERSUITES,
    DEPRECATED_TLS_CIPHERSUITES,
    MANDATORY_TLS_CIPHERSUITES,
)

LOCAL_PLATFORM = platform.platform().lower() if hasattr(platform, "platform") else ""
PLATFORM_IS_SOLARIS = "sunos-" in LOCAL_PLATFORM
PLATFORM_IS_EL7 = "el7uek" in LOCAL_PLATFORM or "oracle-7" in LOCAL_PLATFORM


# Reference: https://dev.mysql.com/doc/refman/8.3/en/encrypted-connection-protocols-ciphers.html
# Server 5.7 and older are no longer supported
@unittest.skipIf(
    PLATFORM_IS_EL7,
    "Tests deactivated for Oracle Linux 7",
)
@unittest.skipIf(
    PLATFORM_IS_SOLARIS,
    "Tests deactivated for Solaris.",
)
@unittest.skipIf(
    tests.MYSQL_VERSION < (8, 0, 0),
    "MySQL Server should be 8.0 or newer",
)
def setUpModule() -> None:
    pass


@unittest.skipIf(
    not tests.SSL_AVAILABLE,
    "No SSL support.",
)
class CipherTests(tests.MySQLxTests):
    """Testing cipher and cipher-suite lists."""

    # when more than one approved TLS version is defined,
    # the latest available version is enforced.
    test_case_values = {
        "tls_versions": {
            "1": (NotSupportedError, ["TLSv1"]),
            "1.1": (NotSupportedError, ["TLSv1.0"]),
            "2": (NotSupportedError, ["TLSv1.1"]),
            "3": (NotSupportedError, ["TLSv1", "TLSv1.1"]),
            "3.1": (NotSupportedError, ["TLSv1", "TLSv1.0"]),
            "4": ("TLSv1.2", ["TLSv1.2", "TLSv1.1"]),
            "5": ("TLSv1.2", ["foo", "TLSv1.2"]),
            "6": ("TLSv1.3", ["TLSv1.3", "TLSv1.2"]),
            "7": (InterfaceError, ["foo", "bar"]),
            "8": ("TLSv1.3", ["TLSv1.2", "TLSv1.3"]),
            "9": (InterfaceError, []),
            "10": ("TLSv1.2", ["TLSv1.0", "TLSv1.2", "TLSv1.4", "TLSv1", "TLSv1.1"]),
        },
        "tls_ciphersuites": {
            "1": (
                NotSupportedError,  # expected error/warning if any
                None,  # expected cipher OpenSSL name
                ["TLSv1.2"],  # tls version to be used
                ["TLS_DH_DSS_WITH_AES_256_GCM_SHA384"],  # unacceptable
            ),
            "2": (
                DeprecationWarning,
                [
                    DEPRECATED_TLS_CIPHERSUITES["TLSv1.2"][
                        "TLS_RSA_WITH_AES_128_GCM_SHA256"
                    ]
                ],
                ["TLSv1.2"],
                ["TLS_RSA_WITH_AES_128_GCM_SHA256"],  # deprecated
            ),
            "3": (
                None,
                [
                    APPROVED_TLS_CIPHERSUITES["TLSv1.2"][
                        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
                    ]
                ],
                ["TLSv1.2"],
                ["ECDHE-RSA-AES256-GCM-SHA384"],  # approved
            ),
            "4": (
                None,
                [
                    MANDATORY_TLS_CIPHERSUITES["TLSv1.2"][
                        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
                    ]
                ],
                ["TLSv1.2"],
                ["TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"],  # mandatory
            ),
            "5": (
                None,
                [
                    MANDATORY_TLS_CIPHERSUITES["TLSv1.2"][
                        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
                    ]
                ],
                ["TLSv1.2"],
                [
                    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",  # mandatory
                    "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",  # approved
                ],
            ),
            "6": (
                InterfaceError,  # expected error/warning if any
                None,  # expected cipher OpenSSL name
                ["TLSv1.3"],  # tls version to be used
                [
                    "TLS_RSA_WITH_AES_128_GCM_SHA256",  # deprecated
                    "TLS-DH_DSS?WITH_AES_256_POLY_SHA384",  # invalid
                ],
            ),
            "7": (
                None,
                # the pure-python implementation does not support cipher selection for
                # TLSv1.3. The ultimate cipher to be used will be determined by the
                # MySQL Server during TLS negotiation. As of MySQL Server 9.2.0,
                # `TLS_AES_128_GCM_SHA256` is used over `TLS_AES_256_GCM_SHA384` as
                # default since it is more efficient.
                # While AES-256 offers a higher theoretical security level due to its
                # larger key size, for most practical applications, AES-128 is
                # considered sufficiently secure and provides a good balance between
                # security and performance. Hence, both are acceptable expected cipher
                # OpenSSL name values.
                [
                    "TLS_AES_128_GCM_SHA256",
                    "TLS_AES_256_GCM_SHA384",
                ],  # expected cipher OpenSSL name
                ["TLSv1.3"],
                ["TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256"],  # acceptable
            ),
            "8": (
                DeprecationWarning,
                [
                    DEPRECATED_TLS_CIPHERSUITES["TLSv1.2"][
                        "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256"
                    ]
                ],
                ["TLSv1.2"],
                ["TLS_DHE_RSA_WITH_AES_128_GCM_SHA256"],  # deprecated
            ),
        },
    }

    def _test_tls_versions(self, test_case_id: str):
        expected_res, tls_versions = self.test_case_values["tls_versions"][test_case_id]

        conf = tests.get_mysqlx_config()
        conf["tls-versions"] = tls_versions
        conf["use_pure"] = self.session.use_pure
        conf["socket"] = None

        with (
            self.assertRaises(expected_res)
            if not isinstance(expected_res, str)
            else nullcontext()
        ):
            with mysqlx.get_session(**conf) as session:
                res = (
                    session.sql("SHOW STATUS LIKE 'Mysqlx_ssl_version'")
                    .execute()
                    .fetch_all()[0]
                )
                res = ast.literal_eval(repr(res))  # res[0] is mysqlx.result.Row
                self.assertEqual(res[-1], expected_res)

    def _test_tls_ciphersuites(self, test_case_id: str):
        exp_event, exp_ciphers, tls_versions, tls_ciphersuites = self.test_case_values[
            "tls_ciphersuites"
        ][test_case_id]

        conf = tests.get_mysqlx_config()
        conf["use_pure"] = self.session.use_pure
        conf["socket"] = None
        conf["tls-ciphersuites"] = tls_ciphersuites

        if tls_versions:
            conf["tls-versions"] = tls_versions

        event_catcher = (
            self.assertWarns if exp_event == DeprecationWarning else self.assertRaises
        )

        with nullcontext() if exp_event is None else event_catcher(exp_event):
            with mysqlx.get_session(**conf) as session:
                if tls_versions:
                    res = (
                        session.sql("SHOW STATUS LIKE 'Mysqlx_ssl_version'")
                        .execute()
                        .fetch_all()[0]
                    )
                    res = ast.literal_eval(repr(res))  # res[0] is mysqlx.result.Row
                    self.assertEqual(res[-1], tls_versions[0])

                res = (
                    session.sql("SHOW STATUS LIKE 'Mysqlx_ssl_cipher'")
                    .execute()
                    .fetch_all()[0]
                )
                res = ast.literal_eval(repr(res))  # res[0] is mysqlx.result.Row
                self.assertIn(res[-1], exp_ciphers)

    @tests.foreach_session()
    def test_tls_versions_1(self):
        self._test_tls_versions(test_case_id="1")

    @tests.foreach_session()
    def test_tls_versions_2(self):
        self._test_tls_versions(test_case_id="2")

    @tests.foreach_session()
    def test_tls_versions_3(self):
        self._test_tls_versions(test_case_id="3")

    @tests.foreach_session()
    def test_tls_versions_4(self):
        self._test_tls_versions(test_case_id="4")

    @tests.foreach_session()
    def test_tls_versions_5(self):
        self._test_tls_versions(test_case_id="5")

    @tests.foreach_session()
    def test_tls_versions_6(self):
        self._test_tls_versions(test_case_id="6")

    @tests.foreach_session()
    def test_tls_versions_7(self):
        self._test_tls_versions(test_case_id="7")

    @tests.foreach_session()
    def test_tls_versions_8(self):
        self._test_tls_versions(test_case_id="8")

    @tests.foreach_session()
    def test_tls_versions_9(self):
        self._test_tls_versions(test_case_id="9")

    @tests.foreach_session()
    def test_tls_versions_10(self):
        self._test_tls_versions(test_case_id="10")

    @tests.foreach_session()
    def test_tls_ciphersuites_1(self):
        self._test_tls_ciphersuites(test_case_id="1")

    @unittest.skipIf(
        tests.MYSQL_VERSION >= (8, 1, 0),
        "MySQL Server should be 8.0 or older.",
    )
    @tests.foreach_session()
    def test_tls_ciphersuites_2(self):
        self._test_tls_ciphersuites(test_case_id="2")

    @tests.foreach_session()
    def test_tls_ciphersuites_3(self):
        self._test_tls_ciphersuites(test_case_id="3")

    @tests.foreach_session()
    def test_tls_ciphersuites_4(self):
        self._test_tls_ciphersuites(test_case_id="4")

    @tests.foreach_session()
    def test_tls_ciphersuites_5(self):
        self._test_tls_ciphersuites(test_case_id="5")

    @tests.foreach_session()
    def test_tls_ciphersuites_6(self):
        self._test_tls_ciphersuites(test_case_id="6")

    @tests.foreach_session()
    def test_tls_ciphersuites_7(self):
        self._test_tls_ciphersuites(test_case_id="7")

    @tests.foreach_session()
    def test_tls_ciphersuites_8(self):
        self._test_tls_ciphersuites(test_case_id="8")
