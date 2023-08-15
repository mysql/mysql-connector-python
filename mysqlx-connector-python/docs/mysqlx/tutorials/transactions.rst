Transactions
============

Savepoints
----------

The ``SAVEPOINT`` statement sets a named transaction allowing parts of a transaction to be rolled back before ``COMMIT``.

Get the collection object
^^^^^^^^^^^^^^^^^^^^^^^^^

Assuming the existence of ``test_schema.test_collection`` collection.

.. code-block:: python

   [{
       "_id": 1,
       "name": "Fred",
       "age": 21
   }]


Get the collection object.

.. code-block:: python

   session = mysqlx.get_session("root:@localhost:33060")
   schema = session.get_schema("test_schema")
   collection = schema.get_collection("test_collection")

Set and rollback to a named transaction savepoint
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A savepoint name can be provided to create a transaction savepoint, which can later be used to rollback.

A :class:`mysqlx.OperationalError` will be raised if the savepoint name is an invalid string or if a nonexistent savepoint is being used in :func:`mysqlx.Session.rollback_to`.

.. code-block:: python

   # Start transaction
   session.start_transaction()

   collection.add({"name": "Wilma", "age": 33}).execute()
   assert(2 == collection.count())

   # Create a savepoint
   session.set_savepoint("sp")

   collection.add({"name": "Barney", "age": 42}).execute()
   assert(3 == collection.count())

   # Rollback to a savepoint
   session.rollback_to("sp")

   assert(2 == collection.count())

   # Commit all operations
   session.commit()

Set and rollback to an unnamed transaction savepoint
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If a savepoint name is not provided, :func:`mysqlx.Session.release_savepoint` will return a generated savepoint name.

.. code-block:: python

   # Start transaction
   session.start_transaction()

   collection.add({"name": "Wilma", "age": 33}).execute()
   assert(2 == collection.count())

   # Create a savepoint
   savepoint = session.set_savepoint()

   collection.add({"name": "Barney", "age": 42}).execute()
   assert(3 == collection.count())

   # Rollback to a savepoint
   session.rollback_to(savepoint)

   assert(2 == collection.count())

   # Commit all operations
   session.commit()

Releasing a transaction savepoint
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A :class:`mysqlx.OperationalError` will be raised if a nonexistent savepoint is being used in :func:`mysqlx.Session.release_savepoint`.

.. code-block:: python

   # Start transaction
   session.start_transaction()

   collection.add({"name": "Wilma", "age": 33}).execute()
   assert(2 == collection.count())

   # Create a savepoint
   session.set_savepoint("sp")

   collection.add({"name": "Barney", "age": 42}).execute()
   assert(3 == collection.count())

   # Release a savepoint
   session.release_savepoint("sp")

   assert(3 == collection.count())

   # Commit all operations
   session.commit()
