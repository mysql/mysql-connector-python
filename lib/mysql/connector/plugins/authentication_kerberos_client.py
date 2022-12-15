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

"""Kerberos Authentication Plugin."""

import getpass
import logging
import os
import struct

from .. import errors

try:
    import gssapi
except ImportError:
    gssapi = None
    if os.name != "nt":
        raise errors.ProgrammingError(
            "Module gssapi is required for GSSAPI authentication "
            "mechanism but was not found. Unable to authenticate "
            "with the server"
        ) from None

try:
    import sspi
    import sspicon
except ImportError:
    sspi = None
    sspicon = None

from . import BaseAuthPlugin

logging.getLogger(__name__).addHandler(logging.NullHandler())

_LOGGER = logging.getLogger(__name__)

AUTHENTICATION_PLUGIN_CLASS = "MySQLKerberosAuthPlugin"


# pylint: disable=c-extension-no-member,no-member
class MySQLKerberosAuthPlugin(BaseAuthPlugin):
    """Implement the MySQL Kerberos authentication plugin."""

    plugin_name = "authentication_kerberos_client"
    requires_ssl = False
    context = None

    @staticmethod
    def get_user_from_credentials():
        """Get user from credentials without realm."""
        try:
            creds = gssapi.Credentials(usage="initiate")
            user = str(creds.name)
            if user.find("@") != -1:
                user, _ = user.split("@", 1)
            return user
        except gssapi.raw.misc.GSSError:
            return getpass.getuser()

    def _acquire_cred_with_password(self, upn):
        """Acquire credentials through provided password."""
        _LOGGER.debug("Attempt to acquire credentials through provided password")

        username = gssapi.raw.names.import_name(
            upn.encode("utf-8"), name_type=gssapi.NameType.user
        )

        try:
            acquire_cred_result = gssapi.raw.acquire_cred_with_password(
                username, self._password.encode("utf-8"), usage="initiate"
            )
        except gssapi.raw.misc.GSSError as err:
            raise errors.ProgrammingError(
                f"Unable to acquire credentials with the given password: {err}"
            )
        creds = acquire_cred_result[0]
        return creds

    @staticmethod
    def _parse_auth_data(packet):
        """Parse authentication data.

        Get the SPN and REALM from the authentication data packet.

        Format:
            SPN string length two bytes <B1> <B2> +
            SPN string +
            UPN realm string length two bytes <B1> <B2> +
            UPN realm string

        Returns:
            tuple: With 'spn' and 'realm'.
        """
        spn_len = struct.unpack("<H", packet[:2])[0]
        packet = packet[2:]

        spn = struct.unpack(f"<{spn_len}s", packet[:spn_len])[0]
        packet = packet[spn_len:]

        realm_len = struct.unpack("<H", packet[:2])[0]
        realm = struct.unpack(f"<{realm_len}s", packet[2:])[0]

        return spn.decode(), realm.decode()

    def auth_response(self, auth_data=None):
        """Prepare the fist message to the server."""
        spn = None
        realm = None

        if auth_data:
            try:
                spn, realm = self._parse_auth_data(auth_data)
            except struct.error as err:
                raise InterruptedError(f"Invalid authentication data: {err}") from err

        if spn is None:
            return self.prepare_password()

        upn = f"{self._username}@{realm}" if self._username else None

        _LOGGER.debug("Service Principal: %s", spn)
        _LOGGER.debug("Realm: %s", realm)
        _LOGGER.debug("Username: %s", self._username)

        try:
            # Attempt to retrieve credentials from default cache file
            creds = gssapi.Credentials(usage="initiate")
            creds_upn = str(creds.name)

            _LOGGER.debug("Cached credentials found")
            _LOGGER.debug("Cached credentials UPN: %s", creds_upn)

            # Remove the realm from user
            if creds_upn.find("@") != -1:
                creds_user, creds_realm = creds_upn.split("@", 1)
            else:
                creds_user = creds_upn
                creds_realm = None

            upn = f"{self._username}@{realm}" if self._username else creds_upn

            # The user from cached credentials matches with the given user?
            if self._username and self._username != creds_user:
                _LOGGER.debug(
                    "The user from cached credentials doesn't match with the "
                    "given user"
                )
                if self._password is not None:
                    creds = self._acquire_cred_with_password(upn)
            if creds_realm and creds_realm != realm and self._password is not None:
                creds = self._acquire_cred_with_password(upn)
        except gssapi.raw.exceptions.ExpiredCredentialsError as err:
            if upn and self._password is not None:
                creds = self._acquire_cred_with_password(upn)
            else:
                raise errors.InterfaceError(f"Credentials has expired: {err}")
        except gssapi.raw.misc.GSSError as err:
            if upn and self._password is not None:
                creds = self._acquire_cred_with_password(upn)
            else:
                raise errors.InterfaceError(
                    f"Unable to retrieve cached credentials error: {err}"
                )

        flags = (
            gssapi.RequirementFlag.mutual_authentication,
            gssapi.RequirementFlag.extended_error,
            gssapi.RequirementFlag.delegate_to_peer,
        )
        name = gssapi.Name(spn, name_type=gssapi.NameType.kerberos_principal)
        cname = name.canonicalize(gssapi.MechType.kerberos)
        self.context = gssapi.SecurityContext(
            name=cname, creds=creds, flags=sum(flags), usage="initiate"
        )

        try:
            initial_client_token = self.context.step()
        except gssapi.raw.misc.GSSError as err:
            raise errors.InterfaceError(f"Unable to initiate security context: {err}")

        _LOGGER.debug("Initial client token: %s", initial_client_token)
        return initial_client_token

    def auth_continue(self, tgt_auth_challenge):
        """Continue with the Kerberos TGT service request.

        With the TGT authentication service given response generate a TGT
        service request. This method must be invoked sequentially (in a loop)
        until the security context is completed and an empty response needs to
        be send to acknowledge the server.

        Args:
            tgt_auth_challenge: the challenge for the negotiation.

        Returns:
            tuple (bytearray TGS service request,
            bool True if context is completed otherwise False).
        """
        _LOGGER.debug("tgt_auth challenge: %s", tgt_auth_challenge)

        resp = self.context.step(tgt_auth_challenge)

        _LOGGER.debug("Context step response: %s", resp)
        _LOGGER.debug("Context completed?: %s", self.context.complete)

        return resp, self.context.complete

    def auth_accept_close_handshake(self, message):
        """Accept handshake and generate closing handshake message for server.

        This method verifies the server authenticity from the given message
        and included signature and generates the closing handshake for the
        server.

        When this method is invoked the security context is already established
        and the client and server can send GSSAPI formated secure messages.

        To finish the authentication handshake the server sends a message
        with the security layer availability and the maximum buffer size.

        Since the connector only uses the GSSAPI authentication mechanism to
        authenticate the user with the server, the server will verify clients
        message signature and terminate the GSSAPI authentication and send two
        messages; an authentication acceptance b'\x01\x00\x00\x08\x01' and a
        OK packet (that must be received after sent the returned message from
        this method).

        Args:
            message: a wrapped gssapi message from the server.

        Returns:
            bytearray (closing handshake message to be send to the server).
        """
        if not self.context.complete:
            raise errors.ProgrammingError("Security context is not completed")
        _LOGGER.debug("Server message: %s", message)
        _LOGGER.debug("GSSAPI flags in use: %s", self.context.actual_flags)
        try:
            unwraped = self.context.unwrap(message)
            _LOGGER.debug("Unwraped: %s", unwraped)
        except gssapi.raw.exceptions.BadMICError as err:
            _LOGGER.debug("Unable to unwrap server message: %s", err)
            raise errors.InterfaceError(f"Unable to unwrap server message: {err}")

        _LOGGER.debug("Unwrapped server message: %s", unwraped)
        # The message contents for the clients closing message:
        #   - security level 1 byte, must be always 1.
        #   - conciliated buffer size 3 bytes, without importance as no
        #     further GSSAPI messages will be sends.
        response = bytearray(b"\x01\x00\x00\00")
        # Closing handshake must not be encrypted.
        _LOGGER.debug("Message response: %s", response)
        wraped = self.context.wrap(response, encrypt=False)
        _LOGGER.debug(
            "Wrapped message response: %s, length: %d",
            wraped[0],
            len(wraped[0]),
        )

        return wraped.message


class MySQLSSPIKerberosAuthPlugin(BaseAuthPlugin):
    """Implement the MySQL Kerberos authentication plugin with Windows SSPI"""

    plugin_name = "authentication_kerberos_client"
    requires_ssl = False
    context = None
    clientauth = None

    @staticmethod
    def _parse_auth_data(packet):
        """Parse authentication data.

        Get the SPN and REALM from the authentication data packet.

        Format:
            SPN string length two bytes <B1> <B2> +
            SPN string +
            UPN realm string length two bytes <B1> <B2> +
            UPN realm string

        Returns:
            tuple: With 'spn' and 'realm'.
        """
        spn_len = struct.unpack("<H", packet[:2])[0]
        packet = packet[2:]

        spn = struct.unpack(f"<{spn_len}s", packet[:spn_len])[0]
        packet = packet[spn_len:]

        realm_len = struct.unpack("<H", packet[:2])[0]
        realm = struct.unpack(f"<{realm_len}s", packet[2:])[0]

        return spn.decode(), realm.decode()

    def auth_response(self, auth_data=None):
        """Prepare the first message to the server."""
        _LOGGER.debug("auth_response for sspi")
        spn = None
        realm = None

        if auth_data:
            try:
                spn, realm = self._parse_auth_data(auth_data)
            except struct.error as err:
                raise InterruptedError(f"Invalid authentication data: {err}") from err

        _LOGGER.debug("Service Principal: %s", spn)
        _LOGGER.debug("Realm: %s", realm)
        _LOGGER.debug("Username: %s", self._username)

        if sspicon is None or sspi is None:
            raise errors.ProgrammingError(
                'Package "pywin32" (Python for Win32 (pywin32) extensions)'
                " is not installed."
            )

        flags = (sspicon.ISC_REQ_MUTUAL_AUTH, sspicon.ISC_REQ_DELEGATE)

        if self._username and self._password:
            _auth_info = (self._username, realm, self._password)
        else:
            _auth_info = None

        targetspn = spn
        _LOGGER.debug("targetspn: %s", targetspn)
        _LOGGER.debug("_auth_info is None: %s", _auth_info is None)

        self.clientauth = sspi.ClientAuth(
            "Kerberos",
            targetspn=targetspn,
            auth_info=_auth_info,
            scflags=sum(flags),
            datarep=sspicon.SECURITY_NETWORK_DREP,
        )

        try:
            data = None
            err, out_buf = self.clientauth.authorize(data)
            _LOGGER.debug("Context step err: %s", err)
            _LOGGER.debug("Context step out_buf: %s", out_buf)
            _LOGGER.debug("Context completed?: %s", self.clientauth.authenticated)
            initial_client_token = out_buf[0].Buffer
            _LOGGER.debug("pkg_info: %s", self.clientauth.pkg_info)
        except Exception as err:
            raise errors.InterfaceError(
                f"Unable to initiate security context: {err}"
            ) from err

        _LOGGER.debug("Initial client token: %s", initial_client_token)
        return initial_client_token

    def auth_continue(self, tgt_auth_challenge):
        """Continue with the Kerberos TGT service request.

        With the TGT authentication service given response generate a TGT
        service request. This method must be invoked sequentially (in a loop)
        until the security context is completed and an empty response needs to
        be send to acknowledge the server.

        Args:
            tgt_auth_challenge: the challenge for the negotiation.

        Returns:
            tuple (bytearray TGS service request,
            bool True if context is completed otherwise False).
        """
        _LOGGER.debug("tgt_auth challenge: %s", tgt_auth_challenge)

        err, out_buf = self.clientauth.authorize(tgt_auth_challenge)

        _LOGGER.debug("Context step err: %s", err)
        _LOGGER.debug("Context step out_buf: %s", out_buf)
        resp = out_buf[0].Buffer
        _LOGGER.debug("Context step resp: %s", resp)
        _LOGGER.debug("Context completed?: %s", self.clientauth.authenticated)

        return resp, self.clientauth.authenticated


# pylint: enable=c-extension-no-member,no-member


if os.name == "nt":
    MySQLKerberosAuthPlugin = MySQLSSPIKerberosAuthPlugin
