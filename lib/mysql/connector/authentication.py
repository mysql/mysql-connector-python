# Copyright (c) 2014, 2020, Oracle and/or its affiliates.
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

"""Implementing support for MySQL Authentication Plugins"""

from base64 import b64encode, b64decode
from hashlib import sha1, sha256
import hmac
import logging
import struct
from uuid import uuid4

from . import errors
from .catch23 import PY2, isstr, UNICODE_TYPES, STRING_TYPES
from .utils import (normalize_unicode_string as norm_ustr,
                    validate_normalized_unicode_string as valid_norm)

_LOGGER = logging.getLogger(__name__)


class BaseAuthPlugin(object):
    """Base class for authentication plugins


    Classes inheriting from BaseAuthPlugin should implement the method
    prepare_password(). When instantiating, auth_data argument is
    required. The username, password and database are optional. The
    ssl_enabled argument can be used to tell the plugin whether SSL is
    active or not.

    The method auth_response() method is used to retrieve the password
    which was prepared by prepare_password().
    """

    requires_ssl = False
    plugin_name = ''

    def __init__(self, auth_data, username=None, password=None, database=None,
                 ssl_enabled=False):
        """Initialization"""
        self._auth_data = auth_data
        self._username = username
        self._password = password
        self._database = database
        self._ssl_enabled = ssl_enabled

    def prepare_password(self):
        """Prepares and returns password to be send to MySQL

        This method needs to be implemented by classes inheriting from
        this class. It is used by the auth_response() method.

        Raises NotImplementedError.
        """
        raise NotImplementedError

    def auth_response(self):
        """Returns the prepared password to send to MySQL

        Raises InterfaceError on errors. For example, when SSL is required
        by not enabled.

        Returns str
        """
        if self.requires_ssl and not self._ssl_enabled:
            raise errors.InterfaceError("{name} requires SSL".format(
                name=self.plugin_name))
        return self.prepare_password()


class MySQLNativePasswordAuthPlugin(BaseAuthPlugin):
    """Class implementing the MySQL Native Password authentication plugin"""

    requires_ssl = False
    plugin_name = 'mysql_native_password'

    def prepare_password(self):
        """Prepares and returns password as native MySQL 4.1+ password"""
        if not self._auth_data:
            raise errors.InterfaceError("Missing authentication data (seed)")

        if not self._password:
            return b''
        password = self._password

        if isstr(self._password):
            password = self._password.encode('utf-8')
        else:
            password = self._password

        if PY2:
            password = buffer(password)  # pylint: disable=E0602
            try:
                auth_data = buffer(self._auth_data)  # pylint: disable=E0602
            except TypeError:
                raise errors.InterfaceError("Authentication data incorrect")
        else:
            password = password
            auth_data = self._auth_data

        hash4 = None
        try:
            hash1 = sha1(password).digest()
            hash2 = sha1(hash1).digest()
            hash3 = sha1(auth_data + hash2).digest()
            if PY2:
                xored = [ord(h1) ^ ord(h3) for (h1, h3) in zip(hash1, hash3)]
            else:
                xored = [h1 ^ h3 for (h1, h3) in zip(hash1, hash3)]
            hash4 = struct.pack('20B', *xored)
        except Exception as exc:
            raise errors.InterfaceError(
                "Failed scrambling password; {0}".format(exc))

        return hash4


class MySQLClearPasswordAuthPlugin(BaseAuthPlugin):
    """Class implementing the MySQL Clear Password authentication plugin"""

    requires_ssl = True
    plugin_name = 'mysql_clear_password'

    def prepare_password(self):
        """Returns password as as clear text"""
        if not self._password:
            return b'\x00'
        password = self._password

        if PY2:
            if isinstance(password, unicode):  # pylint: disable=E0602
                password = password.encode('utf8')
        elif isinstance(password, str):
            password = password.encode('utf8')

        return password + b'\x00'


class MySQLSHA256PasswordAuthPlugin(BaseAuthPlugin):
    """Class implementing the MySQL SHA256 authentication plugin

    Note that encrypting using RSA is not supported since the Python
    Standard Library does not provide this OpenSSL functionality.
    """

    requires_ssl = True
    plugin_name = 'sha256_password'

    def prepare_password(self):
        """Returns password as as clear text"""
        if not self._password:
            return b'\x00'
        password = self._password

        if PY2:
            if isinstance(password, unicode):  # pylint: disable=E0602
                password = password.encode('utf8')
        elif isinstance(password, str):
            password = password.encode('utf8')

        return password + b'\x00'


class MySQLCachingSHA2PasswordAuthPlugin(BaseAuthPlugin):
    """Class implementing the MySQL caching_sha2_password authentication plugin

    Note that encrypting using RSA is not supported since the Python
    Standard Library does not provide this OpenSSL functionality.
    """
    requires_ssl = False
    plugin_name = 'caching_sha2_password'
    perform_full_authentication = 4
    fast_auth_success = 3

    def _scramble(self):
        """ Returns a scramble of the password using a Nonce sent by the
        server.

        The scramble is of the form:
        XOR(SHA2(password), SHA2(SHA2(SHA2(password)), Nonce))
        """
        if not self._auth_data:
            raise errors.InterfaceError("Missing authentication data (seed)")

        if not self._password:
            return b''

        password = self._password.encode('utf-8') \
            if isinstance(self._password, UNICODE_TYPES) else self._password

        if PY2:
            password = buffer(password)  # pylint: disable=E0602
            try:
                auth_data = buffer(self._auth_data)  # pylint: disable=E0602
            except TypeError:
                raise errors.InterfaceError("Authentication data incorrect")
        else:
            password = password
            auth_data = self._auth_data

        hash1 = sha256(password).digest()
        hash2 = sha256()
        hash2.update(sha256(hash1).digest())
        hash2.update(auth_data)
        hash2 = hash2.digest()
        if PY2:
            xored = [ord(h1) ^ ord(h2) for (h1, h2) in zip(hash1, hash2)]
        else:
            xored = [h1 ^ h2 for (h1, h2) in zip(hash1, hash2)]
        hash3 = struct.pack('32B', *xored)

        return hash3

    def prepare_password(self):
        if len(self._auth_data) > 1:
            return self._scramble()
        elif self._auth_data[0] == self.perform_full_authentication:
            return self._full_authentication()
        return None

    def _full_authentication(self):
        """Returns password as as clear text"""
        if not self._ssl_enabled:
            raise errors.InterfaceError("{name} requires SSL".format(
                name=self.plugin_name))

        if not self._password:
            return b'\x00'
        password = self._password

        if PY2:
            if isinstance(password, UNICODE_TYPES):  # pylint: disable=E0602
                password = password.encode('utf8')
        elif isinstance(password, str):
            password = password.encode('utf8')

        return password + b'\x00'


class MySQLLdapSaslPasswordAuthPlugin(BaseAuthPlugin):
    """Class implementing the MySQL ldap sasl authentication plugin.

    The MySQL's ldap sasl authentication plugin support two authentication
    methods SCRAM-SHA-1 and GSSAPI (using Kerberos). This implementation only
    support SCRAM-SHA-1.

    SCRAM-SHA-1
        This method requires 2 messages from client and 2 responses from
        server.

        The first message from client will be generated by prepare_password(),
        after receive the response from the server, it is required that this
        response is passed back to auth_continue() which will return the
        second message from the client. After send this second message to the
        server, the second server respond needs to be passed to auth_finalize()
        to finish the authentication process.
    """
    requires_ssl = False
    plugin_name = 'authentication_ldap_sasl_client'
    def_digest_mode = sha1
    client_nonce = None
    client_salt = None
    server_salt = None
    iterations = 0
    server_auth_var = None

    def _xor(self, bytes1, bytes2):
        if PY2:
            xor = [ord(b1) ^ ord(b2) for b1, b2 in zip(bytes1, bytes2)]
            xor = b"".join([chr(i) for i in xor])
            return xor
        else:
            return bytes([b1 ^ b2 for b1, b2 in zip(bytes1, bytes2)])

    def _hmac(self, password, salt):
        digest_maker = hmac.new(password, salt, self.def_digest_mode)
        return digest_maker.digest()

    def _hi(self, password, salt, count):
        """Prepares Hi
        Hi(password, salt, iterations) where Hi(p,s,i) is defined as
        PBKDF2 (HMAC, p, s, i, output length of H).

        """
        pw = password.encode()
        hi = self._hmac(pw, salt + b'\x00\x00\x00\x01')
        aux = hi
        for _ in range(count - 1):
            aux = self._hmac(pw, aux)
            hi = self._xor(hi, aux)
        return hi

    def _normalize(self, string):
        norm_str = norm_ustr(string)
        broken_rule = valid_norm(norm_str)
        if broken_rule is not None:
            raise errors.InterfaceError("broken_rule: {}".format(broken_rule))
            char, rule = broken_rule
            raise errors.InterfaceError(
                "Unable to normalice character: `{}` in `{}` due to {}"
                "".format(char, string, rule))
        return norm_str

    def _first_message(self):
        """This method generates the first message to the server to start the

        The client-first message consists of a gs2-header,
        the desired username, and a randomly generated client nonce cnonce.

        The first message from the server has the form:
            b'n,a=<user_name>,n=<user_name>,r=<client_nonce>

        Returns client's first message
        """
        cfm_fprnat = "n,a={user_name},n={user_name},r={client_nonce}"
        self.client_nonce = str(uuid4()).replace("-", "")
        cfm = cfm_fprnat.format(user_name=self._normalize(self._username),
                                client_nonce=self.client_nonce)

        if PY2:
            if isinstance(cfm, UNICODE_TYPES):  # pylint: disable=E0602
                cfm = cfm.encode('utf8')
        elif isinstance(cfm, str):
            cfm = cfm.encode('utf8')
        return cfm

    def auth_response(self):
        """This method will prepare the fist message to the server.

        Returns bytes to send to the server as the first message.
        """
        # We only support SCRAM-SHA-1 authentication method.
        _LOGGER.debug("read_method_name_from_server: %s",
                      self._auth_data.decode())
        if self._auth_data != b'SCRAM-SHA-1':
            raise errors.InterfaceError(
                'The sasl authentication method "{}" requested from the server '
                'is not supported. Only "{}" is supported'.format(
                    self._auth_data, "SCRAM-SHA-1"))

        return self._first_message()

    def _second_message(self):
        """This method generates the second message to the server

        Second message consist on the concatenation of the client and the
        server nonce, and cproof.

        c=<n,a=<user_name>>,r=<server_nonce>,p=<client_proof>
        where:
            <client_proof>: xor(<client_key>, <client_signature>)

            <client_key>: hmac(salted_password, b"Client Key")
            <client_signature>: hmac(<stored_key>, <auth_msg>)
            <stored_key>: h(<client_key>)
            <auth_msg>: <client_first_no_header>,<servers_first>,
                        c=<client_header>,r=<server_nonce>
            <client_first_no_header>: n=<username>r=<client_nonce>
        """
        if not self._auth_data:
            raise errors.InterfaceError("Missing authentication data (seed)")

        passw = self._normalize(self._password)
        salted_password = self._hi(passw,
                                   b64decode(self.server_salt),
                                   self.iterations)

        _LOGGER.debug("salted_password: %s",
                      b64encode(salted_password).decode())

        client_key = self._hmac(salted_password, b"Client Key")
        _LOGGER.debug("client_key: %s", b64encode(client_key).decode())

        stored_key = self.def_digest_mode(client_key).digest()
        _LOGGER.debug("stored_key: %s", b64encode(stored_key).decode())

        server_key = self._hmac(salted_password, b"Server Key")
        _LOGGER.debug("server_key: %s", b64encode(server_key).decode())

        client_first_no_header = ",".join([
            "n={}".format(self._normalize(self._username)),
            "r={}".format(self.client_nonce)])
        _LOGGER.debug("client_first_no_header: %s", client_first_no_header)
        auth_msg = ','.join([
            client_first_no_header,
            self.servers_first,
            "c={}".format(b64encode("n,a={},".format(
                self._normalize(self._username)).encode()).decode()),
            "r={}".format(self.server_nonce)])
        _LOGGER.debug("auth_msg: %s", auth_msg)

        client_signature = self._hmac(stored_key, auth_msg.encode())
        _LOGGER.debug("client_signature: %s",
                      b64encode(client_signature).decode())

        client_proof = self._xor(client_key, client_signature)
        _LOGGER.debug("client_proof: %s", b64encode(client_proof).decode())

        self.server_auth_var = b64encode(
            self._hmac(server_key, auth_msg.encode())).decode()
        _LOGGER.debug("server_auth_var: %s", self.server_auth_var)

        client_header = b64encode(
            "n,a={},".format(self._normalize(self._username)).encode()).decode()
        msg = ",".join(["c={}".format(client_header),
                        "r={}".format(self.server_nonce),
                        "p={}".format(b64encode(client_proof).decode())])
        _LOGGER.debug("second_message: %s", msg)
        return msg.encode()

    def _validate_first_reponse(self, servers_first):
        """Validates first message from the server.

        Extracts the server's salt and iterations from the servers 1st response.
        First message from the server is in the form:
            <server_salt>,i=<iterations>
        """
        self.servers_first = servers_first
        if not servers_first or not isinstance(servers_first, STRING_TYPES):
            raise errors.InterfaceError("Unexpected server message: {}"
                                        "".format(servers_first))
        try:
            r_server_nonce, s_salt, i_counter = servers_first.split(",")
        except ValueError:
            raise errors.InterfaceError("Unexpected server message: {}"
                                        "".format(servers_first))
        if not r_server_nonce.startswith("r=") or \
           not s_salt.startswith("s=") or \
           not i_counter.startswith("i="):
            raise errors.InterfaceError("Incomplete reponse from the server: {}"
                                        "".format(servers_first))
        if self.client_nonce in r_server_nonce:
            self.server_nonce = r_server_nonce[2:]
            _LOGGER.debug("server_nonce: %s", self.server_nonce)
        else:
            raise errors.InterfaceError("Unable to authenticate response: "
                                        "response not well formed {}"
                                        "".format(servers_first))
        self.server_salt = s_salt[2:]
        _LOGGER.debug("server_salt: %s length: %s", self.server_salt,
                       len(self.server_salt))
        try:
            i_counter = i_counter[2:]
            _LOGGER.debug("iterations: {}".format(i_counter))
            self.iterations = int(i_counter)
        except:
            raise errors.InterfaceError("Unable to authenticate: iterations "
                                        "not found {}".format(servers_first))

    def auth_continue(self, servers_first_response):
        """return the second message from the client.

        Returns bytes to send to the server as the second message.
        """
        self._validate_first_reponse(servers_first_response)
        return self._second_message()

    def _validate_second_reponse(self, servers_second):
        """Validates second message from the server.

        The client and the server prove to each other they have the same Auth
        variable.

        The second message from the server consist of the server's proof:
            server_proof = HMAC(<server_key>, <auth_msg>)
            where:
                <server_key>: hmac(<salted_password>, b"Server Key")
                <auth_msg>: <client_first_no_header>,<servers_first>,
                            c=<client_header>,r=<server_nonce>

        Our server_proof must be equal to the Auth variable send on this second
        response.
        """
        if not servers_second or not isinstance(servers_second, bytearray) or \
           len(servers_second)<=2 or not servers_second.startswith(b"v="):
            raise errors.InterfaceError("The server's proof is not well formated.")
        server_var = servers_second[2:].decode()
        _LOGGER.debug("server auth variable: %s", server_var)
        return self.server_auth_var == server_var

    def auth_finalize(self, servers_second_response):
        """finalize the authentication process.

        Raises errors.InterfaceError if the ervers_second_response is invalid.

        Returns True in succesfull authentication False otherwise.
        """
        if not self._validate_second_reponse(servers_second_response):
            raise errors.InterfaceError("Authentication failed: Unable to "
                                        "proof server identity.")
        return True


def get_auth_plugin(plugin_name):
    """Return authentication class based on plugin name

    This function returns the class for the authentication plugin plugin_name.
    The returned class is a subclass of BaseAuthPlugin.

    Raises errors.NotSupportedError when plugin_name is not supported.

    Returns subclass of BaseAuthPlugin.
    """
    for authclass in BaseAuthPlugin.__subclasses__():  # pylint: disable=E1101
        if authclass.plugin_name == plugin_name:
            return authclass

    raise errors.NotSupportedError(
        "Authentication plugin '{0}' is not supported".format(plugin_name))
