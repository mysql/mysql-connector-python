Getting started
===============

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

The connection settings accepts a connect timeout option ``connect-timeout``, which should be a non-negative integer that defines a time frame in milliseconds. The timeout will assume a default value of 10000 ms (10s) if a value is not provided. And can be disabled if it's value is set to 0, and in that case, the client will wait until the underlying socket (platform-dependent) times-out.

.. code-block:: python

   session = mysqlx.get_session('mysqlx://root:@localhost:33060?connect-timeout=5000')

Connector/Python has a C extension for `Protobuf <https://developers.google.com/protocol-buffers/>`_ message serialization, this C extension is enabled by default if available. It can be disabled by setting the ``use-pure`` option to :data:`True`.

.. code-block:: python

   session = mysqlx.get_session('mysqlx://root:@localhost:33060?use-pure=true')
   session = mysqlx.get_session(host='localhost', port=33060, user='root', password='', use_pure=True)
   session = mysqlx.get_session({
       'host': 'localhost',
       'port': 33060,
       'user': 'root',
       'password': '',
       'use-pure': True
   })

.. note:: The `urllib.parse.quote <https://docs.python.org/3/library/urllib.parse.html#urllib.parse.quote>`_ function should be used to quote special characters for user and password when using a connection string in the :func:`mysqlx.get_session()` function.

.. code-block:: python

   from urllib.parse import quote
   session = mysqlx.get_session('mysqlx://root:{0}@localhost:33060?use-pure=true'
                                ''.format(quote('pass?!#%@/')))

The :func:`mysqlx.Session.get_schema()` method returns a :class:`mysqlx.Schema` object. We can use this :class:`mysqlx.Schema` object to access collections and tables. X DevAPI's ability to chain all object constructions, enables you to get to the schema object in one line. For example:

.. code-block:: python

   schema = mysqlx.get_session().get_schema('test')

This object chain is equivalent to the following, with the difference that the intermediate step is omitted:

.. code-block:: python

   session = mysqlx.get_session()
   schema = session.get_schema('test')

The connection settings accepts a default schema option ``schema``, which should be a valid name for a preexisting schema in the server.

.. code-block:: python

   session = mysqlx.get_session('mysqlx://root:@localhost:33060/my_schema')
   # or
   session = mysqlx.get_session({
       'host': 'localhost',
       'port': 33060,
       'user': 'root',
       'password': '',
       'schema': 'my_schema'
   })

.. Note:: The default schema provided must exists in the server otherwise it will raise an error at connection time.

This way the session will use the given schema as the default schema, which can be retrieved by :func:`mysqlx.Session.get_default_schema()` and also allows to run SQL statements without specifying the schema name:

.. code-block:: python

   session = mysqlx.get_session('mysqlx://root:@localhost:33060/my_schema')
   my_schema = session.get_default_schema()
   assert my_test_schema.get_name() == 'my_schema'
   session.sql('CREATE TABLE Pets(name VARCHAR(20))').execute()
   # instead of 'CREATE TABLE my_schema.Pets(name VARCHAR(20))'
   res = session.sql('SELECT * FROM Pets').execute().fetch_all()
   # instead of 'SELECT * FROM my_schema.Pets'

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
