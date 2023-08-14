Locking
=======

Shared and Exclusive Locks
--------------------------

The X DevAPI supports locking matching rows, for the :func:`mysqlx.Collection.find()` and :func:`mysqlx.Table.select()` methods, which allows safe and transactional document/row updates on collections or tables.

There are two types of locks:

- :func:`mysqlx.ReadStatement.lock_shared()` permits the transaction that holds the lock to read a row.

- :func:`mysqlx.ReadStatement.lock_exclusive()` permits the transaction that holds the lock to update or delete a row.

Examples
^^^^^^^^

**Setup**

Assuming the existence of ``test_schema.test_collection`` collection.

.. code-block:: python

   [{
       "_id": "1",
       "name": "Fred",
       "age": 21
    },{
       "_id": "2",
       "name": "Sakila",
       "age": 23
    },{
       "_id": "3",
       "name": "Mike",
       "age": 42
   }]

Get the session and collection objects.

.. code-block:: python

   # client 1
   session_1 = mysqlx.get_session("root:@localhost:33060")
   schema_1 = session_1.get_schema("test_schema")
   collection_1 = schema_1.get_collection("test_collection")

   # client 2
   session_2 = mysqlx.get_session("root:@localhost:33060")
   schema_2 = session_2.get_schema("test_schema")
   collection_2 = schema_2.get_collection("test_collection")

**Shared lock**

.. code-block:: python


    # client 1
    session_1.start_transaction()
    collection_1.find("_id = '1'").lock_shared().execute()

    # client 2
    session_2.start_transaction()
    collection_2.find("_id = '2'").lock_shared().execute()  # should return immediately
    collection_2.find("_id = '1'").lock_shared().execute()  # should return immediately

    # client 1
    session_1.rollback()

    # client 2
    session_2.rollback()

**Exclusive Lock**

.. code-block:: python

    # client 1
    session_1.start_transaction()
    collection_1.find("_id = '1'").lock_exclusive().execute()

    # client 2
    session_2.start_transaction()
    collection_2.find("_id = '2'").lock_exclusive().execute()  # should return immediately
    collection_2.find("_id = '1'").lock_exclusive().execute()  # session_2 should block

    # client 1
    session_1.rollback()  # session_2 should unblock now

    # client 2
    session_2.rollback()

**Shared Lock after Exclusive**

.. code-block:: python

    # client 1
    session_1.start_transaction()
    collection_1.find("_id = '1'").lock_exclusive().execute()

    # client 2
    session_2.start_transaction()
    collection_2.find("_id = '2'").lock_shared().execute()  # should return immediately
    collection_2.find("_id = '1'").lock_shared().execute()  # session_2 blocks

    # client 1
    session_1.rollback()  # session_2 should unblock now

    # client 2
    session_2.rollback()

**Exclusive Lock after Shared**

.. code-block:: python

    # client 1
    session_1.start_transaction()
    collection_1.find("_id in ('1', '3')").lock_shared().execute()

    # client 2
    session_2.start_transaction()
    collection_2.find("_id = '2'").lock_exclusive().execute()  # should return immediately
    collection_2.find("_id = '3'").lock_shared().execute()     # should return immediately
    collection_2.find("_id = '1'").lock_exclusive().execute()  # session_2 should block

    # client 1
    session_1.rollback()  # session_2 should unblock now

    # client 2
    session_2.rollback()

Locking with NOWAIT and SKIP_LOCKED
-----------------------------------

If a row is locked by a transaction, a transaction that requests the same locked row must wait until the blocking transaction releases the row lock. However, waiting for a row lock to be released is not necessary if you want the query to return immediately when a requested row is locked, or if excluding locked rows from the result set is acceptable.

To avoid waiting for other transactions to release row locks, ``mysqlx.LockContention.NOWAIT`` and ``mysqlx.LockContention.SKIP_LOCKED`` lock contentions options may be used.

**NOWAIT**

A locking read that uses ``mysqlx.LockContention.NOWAIT`` never waits to acquire a row lock. The query executes immediately, failing with an error if a requested row is locked.

Example of reading a share locked document using :func:`mysqlx.ReadStatement.lock_shared()`:

.. code-block:: python

    # client 1
    session_1.start_transaction()
    collection_1.find("_id = :id").lock_shared().bind("id", "1").execute()

    # client 2
    session_2.start_transaction()
    collection_2.find("_id = :id").lock_shared(mysqlx.LockContention.NOWAIT) \
                .bind("id", "1").execute()
    # The execution should return immediately, no block and no error is thrown

    collection_2.modify("_id = '1'").set("age", 43).execute()
    # The transaction should be blocked

    # client 1
    session_1.commit()
    # session_2 should unblock now

    # client 2
    session_2.rollback()

**SKIP_LOCKED**

A locking read that uses ``mysqlx.LockContention.SKIP_LOCKED`` never waits to acquire a row lock. The query executes immediately, removing locked rows from the result set.

Example of reading a share locked document using :func:`mysqlx.ReadStatement.lock_exclusive()`:

.. code-block:: python

    # client 1
    session_1.start_transaction()
    collection_1.find("_id = :id").lock_shared().bind("id", "1").execute()

    # client 2
    session_2.start_transaction()
    collection_2.find("_id = :id").lock_exclusive(mysqlx.LockContention.SKIP_LOCKED) \
                .bind("id", "1").execute()
    # The execution should return immediately, no error is thrown

    # client 1
    session_1.commit()

    # client 2
    collection_2.find("_id = :id").lock_exclusive(mysqlx.LockContention.SKIP_LOCKED) \
                .bind("id", 1).execute()
    # Since commit is done in 'client 1' then the read must be possible now and
    # no error is thrown
    session_2.rollback()

