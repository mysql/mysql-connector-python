Tutorial
========

Requirements
------------

* MySQL 5.7.12 or higher, with the X Plugin enabled
* Python 2.7 or >= 3.4

Installation
------------

Packages are available at the `Connector/Python download site <http://dev.mysql.com/downloads/connector/python/>`_. For some packaging formats, there are different packages for different versions of Python; choose the one appropriate for the version of Python installed on your system.

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
* Protobuf C++ (version >= 2.6.0)
* Python development files
* MySQL Connector/C or MySQL Server installed, including development files to compile the optional C Extension that interfaces with the MySQL C client library

Installing Connector/Python from source on Unix and Unix-Like Systems
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To install Connector/Python from a tar archive, download the latest version (denoted here as <version>), and execute these commands:

.. code-block:: bash

   shell> tar xzf mysql-connector-python-<version>.tar.gz
   shell> cd mysql-connector-python-<version>.tar.gz
   shell> python setup.py --with-protobuf-include-dir=<protobuf-include-dir> --with-protobuf-lib-dir=<protobuf-lib-dir> --with-protoc=<protoc-binary>

To include the C Extension that interfaces with the MySQL C client library, add the ``--with-mysql-capi`` option:

.. code-block:: bash

   shell> python setup.py --with-protobuf-include-dir=<protobuf-include-dir> --with-protobuf-lib-dir=<protobuf-lib-dir> --with-protoc=<protoc-binary> --with-mysql-capi=<mysql-capi>

The argument to --with-mysql-capi is the path to the installation directory of either MySQL Connector/C or MySQL Server, or the path to the mysql_config command.

To see all options and commands supported by setup.py, use this command:

.. code-block:: bash

   shell> python setup.py --help

Getting started
---------------

A simple python script using this library follows:

.. code-block:: python

   import mysqlx

   # Connect to server on localhost
   session = mysqlx.get_session({
       'host': 'localhost',
       'port': 33060,
       'user': 'mike',
       'password': 's3cr3t!'
   })

   schema = session.get_schema('test')

   # Use the collection 'my_collection'
   collection = schema.get_collection('my_collection')

   # Specify which document to find with Collection.find()
   result = collection.find('name like :param').bind('param', 'S%').limit(1).execute()

   # Print document
   docs = result.fetch_all()
   print('Name: {0}'.format(docs[0]['name']))

   session.close()

After importing the ``mysqlx`` module, we have access to the :func:`mysqlx.get_session()` function which takes a dictionary object or a connection string with the connection settings. 33060 is the port which the X DevAPI Protocol uses by default. This function returns a :class:`mysqlx.Session` object on successful connection to a MySQL server, which enables schema management operations, as well as access to the full SQL language if needed.

.. code-block:: python

   session = mysqlx.get_session({
       'host': 'localhost',
       'port': 33060,
       'user': 'mike',
       'password': 's3cr3t!'
   })

SSL is activated by default. The :func:`mysqlx.get_session()` will throw an error if the server doesn't support SSL. To disable SSL, ``ssl-mode`` must be manually set to disabled. The :class:`mysqlx.SSLMode` contains the following SSL Modes: :data:`REQUIRED`, :data:`DISABLED`, :data:`VERIFY_CA`, :data:`VERIFY_IDENTITY`. Strings ('required', 'disabled', 'verify_ca', 'verify_identity') can also be used to specify the ``ssl-mode`` option. It is case-insensitive.

SSL is not used if the mode of connection is a Unix Socket since it is already considered secure.

If ``ssl-ca`` option is not set, the following SSL Modes are allowed:

- :data:`REQUIRED` is set by default.
- :data:`DISABLED` connects to the MySQL Server without SSL.

If ``ssl-ca`` option is set, only the following SSL Modes are allowed:

- :data:`VERIFY_CA` validates the server Certificate with the CA Certificate.
- :data:`VERIFY_IDENTITY` verifies the common name on the server Certificate and the hostname.

.. code-block:: python

   session = mysqlx.get_session('mysqlx://root:@localhost:33060?ssl-mode=verify_ca&ssl-ca=(/path/to/ca.cert)')
   session = mysqlx.get_session({
       'host': 'localhost',
       'port': 33060,
       'user': 'root',
       'password': '',
       'ssl-mode': mysqlx.SSLMode.VERIFY_CA,
       'ssl-ca': '/path/to/ca.cert'
   })

The :func:`mysqlx.Schema.get_schema()` method returns a :class:`mysqlx.Schema` object. We can use this :class:`mysqlx.Schema` object to access collections and tables. X DevAPI's ability to chain all object constructions, enables you to get to the schema object in one line. For example:

.. code-block:: python

   schema = mysqlx.get_session().get_schema('test')

This object chain is equivalent to the following, with the difference that the intermediate step is omitted:

.. code-block:: python

   session = mysqlx.get_session()
   schema = session.get_schema('test')

In the following example, the :func:`mysqlx.get_session()` function is used to open a session. We then get the reference to ``test`` schema and create a collection using the :func:`mysqlx.Schema.create_collection()` method of the :class:`mysqlx.Schema` object.

.. code-block:: python

   # Connecting to MySQL and working with a Session
   import mysqlx

   # Connect to a dedicated MySQL server
   session = mysqlx.get_session({
       'host': 'localhost',
       'port': 33060,
       'user': 'mike',
       'password': 's3cr3t!'
   })

   schema = session.get_schema('test')

   # Create 'my_collection' in schema
   schema.create_collection('my_collection')

   # Get 'my_collection' from schema
   collection = schema.get_collection('my_collection')

The next step would be to run CRUD operations on a collection which belongs to a particular schema. Once we have the :class:`mysqlx.Schema` object, we can use :func:`mysqlx.Schema.get_collection()` to obtain a reference to the collection on which we can perform operations like :func:`add()` or :func:`remove()`.

.. code-block:: python

   my_coll = db.get_collection('my_collection')

   # Add a document to 'my_collection'
   my_coll.add({'_id': '2', 'name': 'Sakila', 'age': 15}).execute()

   # You can also add multiple documents at once
   my_coll.add({'_id': '2', 'name': 'Sakila', 'age': 15},
               {'_id': '3', 'name': 'Jack', 'age': 15},
               {'_id': '4', 'name': 'Clare', 'age': 37}).execute()

   # Remove the document with '_id' = '1'
   my_coll.remove('_id = 1').execute()

   assert(3 == my_coll.count())


Parameter binding is also available as a chained method to each of the CRUD operations. This can be accomplished by using a placeholder string with a ``:`` as a prefix and binding it to the placeholder using the :func:`bind()` method.

.. code-block:: python

   my_coll = db.get_collection('my_collection')
   my_coll.remove('name = :data').bind('data', 'Sakila').execute()
