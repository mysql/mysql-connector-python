# Copyright (c) 2009, 2024, Oracle and/or its affiliates.
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

"""Decorators Hub."""

import functools
import warnings

from typing import TYPE_CHECKING, Any, Callable

from .constants import RefreshOption

if TYPE_CHECKING:
    from .abstracts import MySQLConnectionAbstract


def cmd_refresh_verify_options() -> Callable:
    """Decorator verifying which options are relevant and which aren't based on
    the server version the client is connecting to."""

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(
            cnx: "MySQLConnectionAbstract", *args: Any, **kwargs: Any
        ) -> Callable:
            options: int = args[0]
            if (options & RefreshOption.GRANT) and cnx.get_server_version() >= (
                9,
                2,
                0,
            ):
                warnings.warn(
                    "As of MySQL Server 9.2.0, refreshing grant tables is not needed "
                    "if you use statements GRANT, REVOKE, CREATE, DROP, or ALTER. "
                    "You should expect this option to be unsupported in a future "
                    "version of MySQL Connector/Python when MySQL Server removes it.",
                    category=DeprecationWarning,
                    stacklevel=1,
                )

            return func(cnx, options, **kwargs)

        return wrapper

    return decorator
