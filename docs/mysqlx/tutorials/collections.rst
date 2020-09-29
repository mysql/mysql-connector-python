Collections
===========

Documents of the same type are grouped together and stored in the database as collections. The X DevAPI uses Collection objects to store and retrieve documents.

Creating collections
--------------------

In order to create a new collection call the :func:`mysqlx.Schema.create_collection()` function from a :class:`mysqlx.Schema` object. It returns a Collection object that can be used right away, for example to insert documents into the collection.

Optionally, the argument ``reuse_existing`` can be set to ``True`` to prevent an error being generated if a collection with the same name already exists.

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

   # Create 'my_collection' in schema
   schema.create_collection('my_collection', reuse_existing=True)

Schema validation
~~~~~~~~~~~~~~~~~

Optionally, the argument ``validation`` can be set to create a server-side document validation schema. This argument should be a :class:`dict`, which includes a ``schema`` key matching a valid `JSON schema <https://json-schema.org/>`_ definition. You should also include the ``level`` key to effectively enable (`STRICT`) or disable (`OFF`) it.

.. code-block:: python

   validation = {
       "level": "STRICT",
       "schema": {
           "id": "http://json-schema.org/geo",
           "$schema": "http://json-schema.org/draft-07/schema#",
           "title": "Longitude and Latitude Values",
           "description": "A geographical coordinate",
           "required": ["latitude", "longitude"],
           "type": "object",
           "properties": {
               "latitude": {
                   "type": "number",
                    "minimum": -90,
                   "maximum": 90
               },
               "longitude": {
                   "type": "number",
                   "minimum": -180,
                   "maximum": 180
               }
           },
       }
   }

   # Create 'my_collection' in schema with a schema validation
   schema.create_collection('my_collection', validation=validation)

When trying to insert a document that violates the schema definition for the collection, an error is thrown.

Modifying collections
---------------------

To enable a JSON schema validation on an existing collection (or to update it if already exists), you can use :func:`mysqlx.Schema.modify_collection()` function.

.. code-block:: python

   # Using the same 'validation' dictionary used above, we can
   # modify 'my_collection' to include a schema validation
   schema.modify_collection('my_collection', validation=validation)

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
   {"dragons":{"drogon": "dark grayish with red markings",
               "Rhaegal": "green with bronze markings",
               "Viserion": "creamy white, with gold markings",
               "count": 3}}
               ''').execute()
   doc = collection.find("name = 'Daenerys'").execute().fetch_all()[0]
   assert(doc.dragons == {"count": 3,
                          "drogon": "dark grayish with red markings",
                          "Rhaegal": "green with bronze markings",
                          "Viserion": "creamy white, with gold markings"})


This example uses the previews one to show how to remove of the nested field
``Viserion`` on ``dragons`` field and at the same time how to update the value of
the ``count`` field with a new value based in the current one.

.. note:: In the :func:`mysqlx.ModifyStatement.patch()` all strings are considered literals,
          for expressions the usage of the :func:`mysqlx.expr()` is required.

.. code-block:: python

   collection.modify('name == "Daenerys"').patch(mysqlx.expr('''
       JSON_OBJECT("dragons", JSON_OBJECT("count", $.dragons.count -1,
                                           "Viserion", Null))
       ''')).execute()
   doc = mys.collection.find("name = 'Daenerys'").execute().fetch_all()[0]
   assert(doc.dragons == {'count': 2,
                          'Rhaegal': 'green with bronze markings',
