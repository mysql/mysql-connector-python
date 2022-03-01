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

import logging

from .. import errors

logging.getLogger(__name__).addHandler(logging.NullHandler())

_LOGGER = logging.getLogger(__name__)


class BaseAuthPlugin:
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
    plugin_name = ""

    def __init__(
        self,
        auth_data,
        username=None,
        password=None,
        database=None,
        ssl_enabled=False,
        instance=None,
    ):
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
            raise errors.InterfaceError(
                "{name} requires SSL".format(name=self.plugin_name)
            )
        return self.prepare_password()
