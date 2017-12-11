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


Using Collection patch (:func:`mysqlx.ModifyStatement.patch()`)
---------------------------------------------------------------

First we need to get a session and a schema.

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

Next step is create a sample collection and add some sample data.

.. code-block:: python

    # Create 'collection_GOT' in schema
    schema.create_collection('collection_GOT')

    # Get 'collection_GOT' from schema
    collection = schema.get_collection('collection_GOT')

    collection.add(
        {"name": "Bran", "family_name": "Stark", "age": 18,
         "parents": ["Eddard Stark", "Catelyn Stark"]},
        {"name": "Sansa", "family_name": "Stark", "age": 21,
         "parents": ["Eddard Stark", "Catelyn Stark"]},
        {"name": "Arya", "family_name": "Stark", "age": 20,
         "parents": ["Eddard Stark", "Catelyn Stark"]},
        {"name": "Jon", "family_name": "Snow", "age": 30},
        {"name": "Daenerys", "family_name": "Targaryen", "age": 30},
        {"name": "Margaery", "family_name": "Tyrell", "age": 35},
        {"name": "Cersei", "family_name": "Lannister", "age": 44,
         "parents": ["Tywin Lannister, Joanna Lannister"]},
        {"name": "Tyrion", "family_name": "Lannister", "age": 48,
         "parents": ["Tywin Lannister, Joanna Lannister"]},
    ).execute()

This example shows how to add a new field to a matching  documents in a
collection, in this case the new field name will be ``_is`` with the value
of ``young`` for those documents with ``age`` field equal or smaller than 21 and
the value ``old`` for documents with ``age`` field value greater than 21.

.. code-block:: python

    collection.modify("age <= 21").patch(
        '{"_is": "young"}').execute()
    collection.modify("age > 21").patch(
        '{"_is": "old"}').execute()

    for doc in mys.collection.find().execute().fetch_all():
        if doc.age <= 21:
            assert(doc._is == "young")
        else:
            assert(doc._is == "old")

This example shows how to add a new field with an array value.
The code will add the field "parents" with the value of
``["Mace Tyrell", "Alerie Tyrell"]``
to documents whose ``family_name`` field has value ``Tyrell``.

.. code-block:: python

    collection.modify('family_name == "Tyrell"').patch(
        {"parents": ["Mace Tyrell", "Alerie Tyrell"]}).execute()
    doc = collection.find("name = 'Margaery'").execute().fetch_all()[0]

    assert(doc.parents == ["Mace Tyrell", "Alerie Tyrell"])


This example shows how to add a new field ``dragons`` with a JSON document as
value.

.. code-block:: python

    collection.modify('name == "Daenerys"').patch('''
    {"dragons":{"drogon": "black with red markings",
                "Rhaegal": "green with bronze markings",
                "Viserion": "creamy white, with gold markings",
                "count": 3}}
                ''').execute()
    doc = collection.find("name = 'Daenerys'").execute().fetch_all()[0]
    assert(doc.dragons == {"count": 3,
                           "drogon": "black with red markings",
                           "Rhaegal": "green with bronze markings",
                           "Viserion": "creamy white, with gold markings"})


This example uses the previews one to show how to remove of the nested field
``Viserion`` on ``dragons`` field and at the same time how to update the value of
the ``count`` field with a new value based in the current one.

.. code-block:: python

    collection.modify('name == "Daenerys"').patch('''
        JSON_OBJECT("dragons", JSON_OBJECT("count", $.dragons.count -1,
                                           "Viserion", Null))
        ''').execute()
    doc = mys.collection.find("name = 'Daenerys'").execute().fetch_all()[0]
    assert(doc.dragons == {'count': 2,
                           'Rhaegal': 'green with bronze markings',
                           'drogon': 'black with red markings'})
