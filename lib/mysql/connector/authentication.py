# Copyright (c) 2014, 2022, Oracle and/or its affiliates.
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

import importlib
import logging

from functools import lru_cache

from .errors import NotSupportedError, ProgrammingError

logging.getLogger(__name__).addHandler(logging.NullHandler())

_LOGGER = logging.getLogger(__name__)

DEFAULT_PLUGINS_PKG = "mysql.connector.plugins"


@lru_cache(maxsize=10, typed=False)
def get_auth_plugin(plugin_name):
    """Return authentication class based on plugin name

    This function returns the class for the authentication plugin plugin_name.
    The returned class is a subclass of BaseAuthPlugin.

    Raises NotSupportedError when plugin_name is not supported.

    Returns subclass of BaseAuthPlugin.
    """
    package = DEFAULT_PLUGINS_PKG
    if plugin_name:
        try:
            _LOGGER.info("package: %s", package)
            _LOGGER.info("plugin_name: %s", plugin_name)
            plugin_module = importlib.import_module(f".{plugin_name}", package)
            _LOGGER.info(
                "AUTHENTICATION_PLUGIN_CLASS: %s",
                plugin_module.AUTHENTICATION_PLUGIN_CLASS,
            )
            return getattr(plugin_module, plugin_module.AUTHENTICATION_PLUGIN_CLASS)
        except ModuleNotFoundError as err:
            _LOGGER.warning("Requested Module was not found: %s", err)
        except ValueError as err:
            raise ProgrammingError(f"Invalid module name: {err}") from err
    raise NotSupportedError(f"Authentication plugin '{plugin_name}' is not supported")
