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

"""Base Authentication Plugin class."""

from abc import ABC
from typing import Optional, Union

from .. import errors
from ..types import StrOrBytes


class BaseAuthPlugin(ABC):
    """Base class for authentication pluginsF


    Classes inheriting from BaseAuthPlugin should implement the method
    prepare_password(). When instantiating, auth_data argument is
    required. The username, password and database are optional. The
    ssl_enabled argument can be used to tell the plugin whether SSL is
    active or not.

    The method auth_response() method is used to retrieve the password
    which was prepared by prepare_password().
    """

    requires_ssl: int = False
    plugin_name: str = ""

    def __init__(
        self,
        auth_data: Optional[bytes],
        username: Optional[StrOrBytes] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        ssl_enabled: bool = False,
    ) -> None:
        """Initialization"""
        self._auth_data: bytes = auth_data
        self._username: Optional[str] = (
            username.decode("utf8")
            if isinstance(username, (bytes, bytearray))
            else username
        )
        self._password: Optional[str] = password
        self._database: Optional[str] = database
        self._ssl_enabled: bool = ssl_enabled

    def prepare_password(self) -> bytes:
        """Prepare and return password as as clear text.

        Returns:
            bytes: Prepared password.
        """
        if not self._password:
            return b"\x00"
        password: StrOrBytes = self._password

        if isinstance(password, str):
            password = password.encode("utf8")

        return password + b"\x00"

    def auth_response(
        self, auth_data: Optional[bytes] = None  # pylint: disable=unused-argument
    ) -> bytes:
        """Return the prepared password to send to MySQL.

        Raises:
            InterfaceError: When SSL is required by not enabled.

        Returns:
            str: The prepared password.
        """
        if self.requires_ssl and not self._ssl_enabled:
            raise errors.InterfaceError(f"{self.plugin_name} requires SSL")
        return self.prepare_password()
