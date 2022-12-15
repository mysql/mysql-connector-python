# Copyright (c) 2022, Oracle and/or its affiliates.
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

"""OCI Authentication Plugin."""

import logging
import os

from base64 import b64encode

from .. import errors

try:
    from cryptography.exceptions import UnsupportedAlgorithm
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
except ImportError:
    raise errors.ProgrammingError("Package 'cryptography' is not installed") from None

try:
    from oci import config, exceptions
except ImportError:
    raise errors.ProgrammingError(
        "Package 'oci' (Oracle Cloud Infrastructure Python SDK) is not installed"
    ) from None

from . import BaseAuthPlugin

logging.getLogger(__name__).addHandler(logging.NullHandler())

_LOGGER = logging.getLogger(__name__)

AUTHENTICATION_PLUGIN_CLASS = "MySQLOCIAuthPlugin"


class MySQLOCIAuthPlugin(BaseAuthPlugin):
    """Implement the MySQL OCI IAM authentication plugin."""

    plugin_name = "authentication_oci_client"
    requires_ssl = False
    context = None

    @staticmethod
    def _prepare_auth_response(signature, oci_config):
        """Prepare client's authentication response

        Prepares client's authentication response in JSON format
        Args:
            signature:  server's nonce to be signed by client.
            oci_config: OCI configuration object.

        Returns:
            JSON_STRING {"fingerprint": string, "signature": string}
        """
        signature_64 = b64encode(signature)
        auth_response = {
            "fingerprint": oci_config["fingerprint"],
            "signature": signature_64.decode(),
        }
        return repr(auth_response).replace(" ", "").replace("'", '"')

    @staticmethod
    def _get_private_key(key_path):
        """Get the private_key form the given location"""
        try:
            with open(os.path.expanduser(key_path), "rb") as key_file:
                private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=None,
                )
        except (TypeError, OSError, ValueError, UnsupportedAlgorithm) as err:
            raise errors.ProgrammingError(
                "An error occurred while reading the API_KEY from "
                f'"{key_path}": {err}'
            )

        return private_key

    @staticmethod
    def _get_valid_oci_config(oci_path=None, profile_name="DEFAULT"):
        """Get a valid OCI config from the given configuration file path"""
        if not oci_path:
            oci_path = config.DEFAULT_LOCATION

        error_list = []
        req_keys = {
            "fingerprint": (lambda x: len(x) > 32),
            "key_file": (lambda x: os.path.exists(os.path.expanduser(x))),
        }

        try:
            # key_file is validated by oci.config if present
            oci_config = config.from_file(oci_path, profile_name)
            for req_key, req_value in req_keys.items():
                try:
                    # Verify parameter in req_key is present and valid
                    if oci_config[req_key] and not req_value(oci_config[req_key]):
                        error_list.append(f'Parameter "{req_key}" is invalid')
                except KeyError:
                    error_list.append(f"Does not contain parameter {req_key}")
        except (
            exceptions.ConfigFileNotFound,
            exceptions.InvalidConfig,
            exceptions.InvalidKeyFilePath,
            exceptions.InvalidPrivateKey,
            exceptions.ProfileNotFound,
        ) as err:
            error_list.append(str(err))

        # Raise errors if any
        if error_list:
            raise errors.ProgrammingError(
                f'Invalid profile {profile_name} in: "{oci_path}". '
                f" Errors found: {error_list}"
            )

        return oci_config

    def auth_response(self, auth_data=None):
        """Prepare authentication string for the server."""
        oci_path = auth_data
        _LOGGER.debug("server nonce: %s, len %d", self._auth_data, len(self._auth_data))
        _LOGGER.debug("OCI configuration file location: %s", oci_path)

        oci_config = self._get_valid_oci_config(oci_path)

        private_key = self._get_private_key(oci_config["key_file"])
        signature = private_key.sign(
            self._auth_data, padding.PKCS1v15(), hashes.SHA256()
        )

        auth_response = self._prepare_auth_response(signature, oci_config)
        _LOGGER.debug("authentication response: %s", auth_response)
        return auth_response.encode()
