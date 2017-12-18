# Copyright (c) 2016, 2017, Oracle and/or its affiliates. All rights reserved.
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

"""Implementation of the DbDoc."""

import json
import uuid

from .compat import STRING_TYPES
from .errors import ProgrammingError


class DbDoc(object):
    """Represents a generic document in JSON format.

    Args:
        value (object): The value can be a JSON string or a dict.

    Raises:
        ValueError: If ``value`` type is not a basestring or dict.
    """
    def __init__(self, value):
        if isinstance(value, dict):
            self.__dict__ = value
        elif isinstance(value, STRING_TYPES):
            self.__dict__ = json.loads(value)
        else:
            raise ValueError("Unable to handle type: {0}".format(type(value)))

    def __str__(self):
        return json.dumps(self.__dict__)

    def __setitem__(self, index, value):
        if index == "_id":
            raise ProgrammingError("Cannot modify _id")
        self.__dict__[index] = value

    def __getitem__(self, index):
        return self.__dict__[index]

    def keys(self):
        """Returns the keys.

        Returns:
            `list`: The keys.
        """
        return self.__dict__.keys()

    def ensure_id(self, doc_id=None):
        """Ensure ID.

        Args:
            doc_id (str): Document ID.
        """
        if doc_id:
            self.__dict__["_id"] = doc_id
        elif "_id" not in self.__dict__:
            uuid1 = str(uuid.uuid1()).upper().split("-")
            uuid1.reverse()
            self.__dict__["_id"] = "".join(uuid1)
        return self.__dict__["_id"]
