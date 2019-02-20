Installation
------------

Packages are available at the `Connector/Python download site <http://dev.mysql.com/downloads/connector/python/>`_. For some packaging formats, there are different packages for different versions of Python; choose the one appropriate for the version of Python installed on your system.

Installing Connector/Python with pip
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is the recommended way to install Connector/Python.

Make sure you have a recent `pip <https://pip.pypa.io/>`_ version installed on your system. If your system already has ``pip`` installed, you might need to update it. Or you can use the `standalone pip installer <https://pip.pypa.io/en/latest/installing/#installing-with-get-pip-py>`_.

.. code-block:: bash

    shell> pip install mysql-connector-python

Installing Connector/Python on Microsoft Windows Using an MSI Package
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To use the MSI Installer, launch it and follow the prompts in the screens it presents to install Connector/Python in the location of your choosing.

Installing Connector/Python on Linux Using the MySQL Yum Repository
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You must have the MySQL Yum repository on your system's repository list. To make sure that your Yum repository is up-to-date, use this command:

.. code-block:: bash

    shell> sudo yum update mysql-community-release

Then install Connector/Python as follows:

.. code-block:: bash

    shell> sudo yum install mysql-connector-python

Installing Connector/Python on Linux Using an RPM Package
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To install a Connector/Python RPM package (denoted here as PACKAGE.rpm), use this command:

.. code-block:: bash

    shell> rpm -i PACKAGE.rpm

Installing Connector/Python on Linux Using a Debian Package
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To install a Connector/Python Debian package (denoted here as PACKAGE.deb), use this command:

.. code-block:: bash

    shell> dpkg -i PACKAGE.deb

Installing Connector/Python on OS X Using a Disk Image
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Download the .dmg file and install Connector/Python by opening it and double clicking the resulting .pkg file.

Installing Connector/Python from source
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Prerequisites
~~~~~~~~~~~~~

As of Connector/Python 2.2.3, source distributions include a C++ Extension, that interfaces with a MySQL server with the X Plugin enabled using Protobuf as data interchange format.

To build Connector/Python C++ Extension for Protobuf, you must satisfy the following prerequisites:

* A C/C++ compiler, such as ``gcc``
* Protobuf C++ (version >= 3.6.1)
* Python development files
* MySQL Connector/C or MySQL Server installed, including development files to compile the optional C Extension that interfaces with the MySQL C client library

Installing Connector/Python from source on Unix and Unix-Like Systems
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To install Connector/Python from a tar archive, download the latest version (denoted here as <version>), and execute these commands:

.. code-block:: bash

   shell> tar xzf mysql-connector-python-<version>.tar.gz
   shell> cd mysql-connector-python-<version>.tar.gz
   shell> python setup.py install --with-protobuf-include-dir=<protobuf-include-dir> --with-protobuf-lib-dir=<protobuf-lib-dir> --with-protoc=<protoc-binary>

To include the C Extension that interfaces with the MySQL C client library, add the ``--with-mysql-capi`` option:

.. code-block:: bash

   shell> python setup.py install --with-protobuf-include-dir=<protobuf-include-dir> --with-protobuf-lib-dir=<protobuf-lib-dir> --with-protoc=<protoc-binary> --with-mysql-capi=<mysql-capi>

The argument to --with-mysql-capi is the path to the installation directory of either MySQL Connector/C or MySQL Server, or the path to the mysql_config command.

To see all options and commands supported by setup.py, use this command:

.. code-block:: bash

   shell> python setup.py --help
