Using Collection patch (:func:`mysqlx.ModifyStatement.patch()`)
===============================================================

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
                           'drogon': 'black with red markings'})
