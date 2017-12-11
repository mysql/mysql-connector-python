Creating Indexes
================

Collection indexes can be created using one or more fields. The method used to
create these indexes is :func:`mysqlx.Collection.create_index()` and the
following sections describes the required arguments for the function and some
examples of use.

Arguments for :func:`mysqlx.Collection.create_index()`
------------------------------------------------------

To use the :func:`mysqlx.Collection.create_index()` we need to specify the name
of the index to be created and the members to be part of the index, in addition
for each member we need to specify the type of data that holds the field in the
document and if it is required or not. Fields marked as required must appear on
each document in the collection.

.. code-block:: python

    {"fields": [{"field": member_path, # required str
                 "type": member_type, # required str, must be a valid type
                 "required": member_required, # optional, True or (default) False
                 "collation": collation, # optional str only for TEXT field type
                 "options": options, # optional (int) only for GEOJSON field type
                 "srid": srid}, # optional (int) only for GEOJSON field type
                 # {... more members,
                 #      repeated as many times
                 #      as needed}
                 ],
     "type": type} # optional, SPATIAL or (default) INDEX

The valid types for the ``type`` field are:

* INT [UNSIGNED]
* TINYINT [UNSIGNED]
* SMALLINT [UNSIGNED]
* MEDIUMINT [UNSIGNED]
* INTEGER [UNSIGNED]
* BIGINT [UNSIGNED]
* REAL [UNSIGNED]
* FLOAT [UNSIGNED]
* DOUBLE [UNSIGNED]
* DECIMAL [UNSIGNED]
* NUMERIC [UNSIGNED]
* DATE
* TIME
* TIMESTAMP
* DATETIME
* TEXT[(length)]
* GEOJSON (extra options: options, srid)

Note: The use of ``type`` GEOJSON, requires the index ``type`` to be set to
``SPATIAL``.

Using :func:`mysqlx.Collection.create_index()`
----------------------------------------------

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

Next step is create a sample collection.

.. code-block:: python

    # Create 'collection_GOT' in schema
    schema.create_collection('collection_GOT')

    # Get 'collection_GOT' from schema
    collection = schema.get_collection('collection_GOT')

The following example shows how to create a simple index with name
``index_age`` that will use a field ``age`` from the document which will
hold integer values.

.. code-block:: python

    collection.create_index("index_age", {"fields": [{"field": "age",
                                                      "type": "INT"}],
                                          "type":"INDEX"})

The following example shows how to create a multi field index with name
``index_name`` that will use the fields ``family_name`` and ``name``
from the document that will hold small texts. This time the ``required``
member has been set to ``True``, which means these fields are required for all
the documents in this collection.

.. code-block:: python

    collection.create_index("index_name", {"fields": [{"field": "family_name",
                                                       "type": "TEXT(12)",
                                                       "required": True}],
                                           "fields": [{"field": "name",
                                                       "type": "TEXT(12)",
                                                       "required": True}],
                                           "type":"INDEX"})


The following example shows how to create a multi field index with name
``geojson_name``, which will use fields with GEOJSON data, so for this will
require the index ``type`` to be set to ``SPATIAL``, that will use the fields
``$.geoField``, ``$.intField``, ``$.floatField`` and ``$.dateField``.
Each field hold the data that compounds the name of the file. Note that by
setting ``SPATIAL`` to the index ``type`` we will require to set for each of
these members ``required`` to ``True``, which means these fields are required
for all the documents in this collection.

.. code-block:: python

    collection.create_index("index_age",
                            {"fields": [{"field": "$.geoField",
                                         "type": "GEOJSON",
                                         "required": False, "options": 2,
                                         "srid": 4326},
                                        {"field": "$.intField", "type": "INT",
                                         "required": True},
                                        {"field": "$.floatField",
                                         "type": "FLOAT",
                                         "required": True},
                                        {"field": "$.dateField",
                                         "type": "DATE", "required": True}],
                             "type" : "SPATIAL"})
