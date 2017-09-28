# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.

# MySQL Connector/Python is licensed under the terms of the GPLv2
# <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most
# MySQL Connectors. There are special exceptions to the terms and
# conditions of the GPLv2 as it is applied to this software, see the
# FOSS License Exception
# <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA

"""Implementation of Session Configuration"""

import os
import sys
import json

from .compat import STRING_TYPES
from .errors import OperationalError, InterfaceError

class SessionConfig(object):
    """A SessionConfig object represents all the information associated to a
    session including name, connection data and custom application data.
    """
    def __init__(self, manager, name, data=None):
        if not (name.isalnum() and 0 < len(name) < 31):
            raise InterfaceError("Invalid session name '{0}'".format(name))

        self._name = name
        self._manager = manager
        self._uri = None
        self._connection_data = {}
        self._appdata = {}
        self.set_connection_data(data)

    @property
    def name(self):
        """string: Name of the session.
        """
        return self._name

    def set_connection_data(self, data):
        """Sets the data required to connect to a MySQLx enabled server.
        If the argument provided is a dict, the `password` field is
        automatically removed.

        Args:
            data (object): Can be a string or a dict containing the information
            to connect and application specific data which is optional.
        """
        if isinstance(data, STRING_TYPES):
            self.set_uri(data)
        elif isinstance(data, dict):
            self._connection_data = data.copy()
            self._appdata = self._connection_data.pop("appdata", {})

            if "uri" in self._connection_data:
                self.set_uri(self._connection_data.pop("uri", None))
            else:
                self.user = self._connection_data.get("user")
                self.password = self._connection_data.pop("password", None)
                if "host" in self._connection_data:
                    self.host = self._connection_data.get("host")
                elif "routers" in self._connection_data:
                    hosts = []
                    for router in self._connection_data["routers"]:
                        address = "{0}:{1}".format(router["host"],
                                                   router["port"])
                        if "priority" in router:
                            address = "(address={0}, priority={1})" \
                                      "".format(address, router["priority"])
                        hosts.append(address)
                    self.host = ",".join(hosts)

    def save(self):
        """Persists the session information on the disk.
        """
        self._manager.save(self)

    def get_name(self):
        """Returns the name of the session provided by the user.

        Returns:
            string: The name of the session.
        """
        return self._name

    def set_name(self, name):
        """Sets the name for the session.

        Args:
            name (string): Name of the session.
        """
        self._name = name

    def set_uri(self, uri):
        """Sets the URI for the session. Automatically drops the password from
        the URI string.

        Args:
            uri (string): A URI.
        """
        if uri is None:
            return

        start = uri.find("://")
        start = 0 if start is -1 else start + 3
        end = uri.find("@")
        self.user, self.password = uri[start:end].split(":", 1)
        uri = "".join([uri[:start + uri[start:].find(":") + 1], uri[end:]])
        start = uri.find("@") + 1
        end = uri.rfind("/")
        if uri[end:].find(")") is -1 and end > 0:
            self.host = uri[start:end]
        else:
            end = uri.find("?")
            self.host = uri[start:] if end is -1 else uri[start:end]
        self._uri = uri

    def get_uri(self):
        """Returns the URI this session connects to.

        Returns:
            string: The URI this session connects to.
        """
        return self._uri

    def set_appdata(self, *args, **kwargs):
        """Set application specific data for the session.

        Args:
            *args: A key and a value.
            **kwargs: Set multiple application data values at once.
        """
        if args and len(args) is 2:
            self._appdata[args[0]] = args[1]
        elif kwargs:
            self._appdata.update(kwargs)

    def get_appdata(self, key):
        """Returns application data associated with a specific key.

        Args:
            key (string): Key associated with application data.
        """
        return self._appdata[key]

    def delete_appdata(self, key):
        """Deletes application data associated with a specific key.

        Args:
            key (string): Key associated with application data.
        """
        del self._appdata[key]

    def to_dict(self):
        """Converts the session parameters into a dictionary.

        Returns:
            dict: Dictionary containing the session data.
        """
        settings = {}
        if self._uri:
            settings["uri"] = self._uri
        if self._appdata:
            settings["appdata"] = self._appdata.copy()
        if self._connection_data:
            settings.update(self._connection_data)
        return settings


class SessionConfigManager(object):
    """A SessionConfigManager class is the user interface to store/retrieve
    Session Configuration data, this class is responsible for:

    1. Support for Persistent Interfaces
    2. Expose Storing/Loading Operations
    """
    def __init__(self):
        self.persistence_handler = None
        self.password_handler = None

    def _save_config(self, config, json=None):
        if json:
            config = SessionConfig(self, config, json)
        self.persistence_handler.save(config.name, config.to_dict())
        if self.password_handler:
            self.password_handler.save(config.user, config.host,
                                       config.password)
        return config

    def _load_config(self, name, config=None):
        if config:
            config = SessionConfig(self, name, config)
        else:
            config = SessionConfig(self, name,
                                   self.persistence_handler.load(name))
        if self.password_handler:
            config.password = self.password_handler.load(config.user,
                                                         config.host)
        return config

    def save(self, config, settings=None, appdata=None):
        """Persists the provided SessionConfig object onto the disk.

        Args:
            config (object): Can be a :class`mysqlx.SessionConfig` or
            a string (name of the session).
            settings (object): Can be a string (URI) or dict (connection
            settings).

        Returns:
            mysqlx.SessionConfig: SessionConfig object.
        """
        if not self.persistence_handler:
            raise OperationalError("Persistence Handler not defined.")

        if isinstance(config, STRING_TYPES):
            config = SessionConfig(self, config)

        if settings:
            config.set_connection_data(settings)
        if appdata:
            config.set_appdata(**appdata)

        return self._save_config(config)

    def get(self, session_name):
        """Retrieves a SessionConfig by name.

        Args:
            session_name (string): Name of the session.

        Returns:
            mysqlx.SessionConfig: SessionConfig object.
        """
        if not self.persistence_handler:
            raise OperationalError("Persistence Handler not defined.")

        return self._load_config(session_name)

    def delete(self, session_name):
        """Deletes a SessionConfig by name.

        Args:
            session_name (string): Name of the session.

        Returns:
            mysqlx.SessionConfig: SessionConfig object.
        """
        if not self.persistence_handler:
            raise OperationalError("Persistence Handler not defined.")

        self.persistence_handler.delete(session_name)
        return True

    def list(self):
        """A list of all Session configuration stored on the system.

        Returns:
            `list`: List of :class`mysqlx.SessionConfig` objects.
        """
        if not self.persistence_handler:
            raise OperationalError("Persistence Handler not defined.")

        return [self._load_config(name, config) for name, config in
                self.persistence_handler.items()]

    def set_persistence_handler(self, handler):
        """Sets the Persistence Handler to persist the session data.

        Args:
            handler (mysqlx.PersistenceHandler): Required to store
            and load configuration details from the disk in JSON format.
        """
        self.persistence_handler = handler

    def set_password_handler(self, handler):
        """Sets the Password Handler to store passwords for the session data.

        Args:
            handler (mysqlx.PasswordHandler): Required to securely store
            and load the password for a session.
        """
        self.password_handler = handler


class PersistenceHandler(object):
    """Stores and loads session configuration data from a persistent storage.

    By default, it looks in:
        1. User configuration data
           ``%APPDATA%/MySQLsessions.json`` or ``$HOME/.mysql/sessions.json``
        2. System configuration data
           ``%PROGRAMDATA%/MySQLsessions.json`` or ``/etc/mysql/sessions.json``

    System configuration data is defined by the user (by hand) honouring the
    established format with no support for appdata and is available in read
    only mode.

    User configuration data can be defined by the user and the Connector. It
    is available in read/write mode.

    When loading the configuration data, it will merge the configuration data
    from both locations. First the System configuration and then the User
    configuration.

    Args:
        sys_file (Optional[str]): System configuration file path.
        usr_file (Optional[str]): User configuration file path.
    """
    def __init__(self, sys_file=None, usr_file=None):
        self._configs = {}
        if not (usr_file and sys_file):
            self._load_default_paths()
        else:
            self._sys_file = sys_file
            self._usr_file = usr_file

        self._read_config(self._sys_file)
        self._read_config(self._usr_file)

    def items(self):
        """Returns the pairs of session names and it's session configuration
        data.

        Returns:
            `list`: List of tuples of session name and session
            configuration data.
        """
        return self._configs.items()

    def _load_default_paths(self):
        if sys.platform.startswith("win"):
            self._usr_file = os.path.join(
                os.getenv("APPDATA"), "MySQLsessions.json")
            self._sys_file = os.path.join(
                os.getenv("PROGRAMDATA"), "MySQLsessions.json")
        elif sys.platform.startswith("linux") or \
             sys.platform.startswith("darwin") or \
             sys.platform.startswith("sunos"):
            self._usr_file = os.path.join(
                os.getenv("HOME"), ".mysql", "sessions.json")
            self._sys_file = os.path.join(
                os.sep, "etc", "mysql", "sessions.json")
        else:
            raise OperationalError("Unable to load configuration.")

    def save(self, name, config):
        """Persists the session configuration data onto the disk.

        Args:
            name (string): Session name.
            config (dict): Session data.
        """
        self._configs[name] = config
        self._write_config(self._usr_file)

    def load(self, name):
        """Loads session data given a name.

        Args:
            name (string): Session name.
        """
        return self._configs[name]

    def delete(self, name):
        """Deletes session data given a name.

        Args:
            name (string): Session name.
        """
        try:
            del self._configs[name]
            self._write_config(self._usr_file)
        except (IOError, KeyError):
            pass

    def list(self):
        """Returns a list of all existing session configuration data.

        Returns:
            `list`: List of dicts containing all session configuration data.
        """
        return self._configs.values()

    def exists(self, name):
        """Checks for the existence of a session.

        Args:
            name (string): Session name.

        Returns:
            boolean: If the Session exists or not.
        """
        return name in self._configs

    def _read_config(self, file_path):
        if not os.path.exists(file_path) or os.path.getsize(file_path) is 0:
            return
        with open(file_path, "r") as config_file:
            self._configs.update(json.load(config_file))

    def _write_config(self, file_path):
        path = os.path.split(file_path)[0]
        if not os.path.exists(path):
            os.makedirs(path)
        with open(file_path, "w") as config_file:
            json.dump(self._configs, config_file)


class PasswordHandler(object):
    def save(self, key, service, password):
        """Securely stores the password for a session.

        Args:
            key (string): Username.
            service (string): Hostname.
            password (string): Password.
        """
        raise NotImplementedError

    def load(self, key, service):
        """Loads the password for a session.

        Args:
            key (string): Username.
            service (string): Hostname.
        """
        raise NotImplementedError
