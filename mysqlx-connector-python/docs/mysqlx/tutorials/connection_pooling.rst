Connection Pooling
==================

Connection pooling is a technique of creating and managing a pool of connections
that are ready for use, which greatly increase the performance of your
applications by reducing the connection creation time.

The way of using connection pooling in Connector/Python with the X Protocol, is
by calling the :func:`mysqlx.get_client()` function as follows:

.. code-block:: python

   import mysqlx

   connection_str = 'mysqlx://mike:s3cr3t!@localhost:33060'
   options_string = '{}'  # An empty document

   client = mysqlx.get_client(connection_str, options_string)
   session = client.get_session()

   # (...)

   session.close()
   client.close()

The connection settings and options can also be a dict:

.. code-block:: python

   import mysqlx

   connection_dict = {
       'host': 'localhost',
       'port': 33060,
       'user': 'mike',
       'password': 's3cr3t!'
   }
   options_dict = {}

   client = mysqlx.get_client(connection_dict, options_dict)
   session = client.get_session()

   # (...)

   session.close()
   client.close()


All sessions created by :func:`mysqlx.Client.get_session()` have a pooled connection,
which after being closed by :func:`mysqlx.Session.close()` returns to the pool of
connections, so it can be reused.

Until now we didn't supply any configuration for :class:`mysqlx.Client`. We can
set the pooling options by passing a dict or a JSON document string in the
second parameter.

The available options for the :class:`mysqlx.Client` are:

.. code-block:: python

   options = {
       'pooling': {
           'enabled': (bool), # [True | False], True by default
           'max_size': (int), # Maximum connections per pool
           "max_idle_time": (int), # milliseconds that a connection will remain active
                                   # while not in use. By default 0, means infinite.
           "queue_timeout": (int), # milliseconds a request will wait for a connection
                                   # to become available. By default 0, means infinite.
       }
   }

To disable pooling in the client we can set the ``enabled`` option to ``False``:

.. code-block:: python

   client = mysqlx.get_client(connection_str, {'pooling':{'enabled': False}})

To define the pool maximum size we can set the ``max_size`` in the ``pooling``
options. In the following example ``'max_size': 5`` sets 5 as the maximum number
of connections allowed in the pool.

.. code-block:: python

   connection_dict = {
       'host': 'localhost',
       'port': 33060,
       'user': 'mike',
       'password': 's3cr3t!'
   }
   options_dict = {'pooling':{'max_size': 5, 'queue_timeout': 1000}}

   client = mysqlx.get_client(connection_dict, options_dict)

   for _ in range(5):
       client.get_session()

   # This will raise a pool error:
   # mysqlx.errors.PoolError: pool max size has been reached
   client.get_session()

The ``queue_timeout`` sets the maximum number of milliseconds a request will
wait for a connection to become available. The default value is ``0`` (zero)
and means infinite.

The following example shows the usage of threads that will have to wait for a
session to become available:

.. code-block:: python

   import mysqlx
   import time
   import random

   from threading import Thread

   connection_dict = {
       'host': 'localhost',
       'port': 33060,
       'user': 'mike',
       'password': 's3cr3t!'
   }

   options_dict = {'pooling':{'max_size': 6, 'queue_timeout':5000}}

   schema_name = 'test'
   collection_name = 'collection_test04'

   def job(client, worker_number):
       """This method keeps the tasks for a thread.

          Args:
              client (Client): to get the sessions to interact with the server.
              worker_number (int): the id number for the worker.
       """
       rand = random.Random()
       worker_name = "Worker_{}".format(worker_number)
       print("starting Worker: {} \n".format(worker_name))

       # Take a nap before do the job, (gets a chance to other thread to start)
       time.sleep(rand.randint(0,9)/10)

       # Get a session from client
       session1 = client.get_session()

       # Get a schema to work on
       schema = session1.get_schema(schema_name)

       # Get the collection to put some documents in
       collection = schema.get_collection(collection_name)

       # Add 10 documents to the collection
       for _ in range(10):
           collection.add({'name': worker_name}).execute()

       # close session
       session1.close()
       print("Worker: {} finish\n".format(worker_name))

   def call_workers(client, job_thread, workers):
       """Create threads and start them.

          Args:
              client (Client): to get the sessions.
              job_thread (method): the method to run by each thread.
              workers (int): the number of threads to create.
       """
       workers_list = []
       for n in range(workers):
           workers_list.append(Thread(target=job_thread, args=[client, n]))
       for worker in workers_list:
           worker.start()

   # Get a client to manage the sessions
   client = mysqlx.get_client(connection_dict, options_dict)

   # Get a session to create an schema and a collection
   session1 = client.get_session()

   schema = session1.create_schema(schema_name)
   collection = schema.create_collection(collection_name)

   # Close the session to have another free connection
   session1.close()

   # Invoke call_workers with the client, the method to run by the thread and
   # the number of threads, on this example 18 workers
   call_workers(client, job, 18)

   # Give some time for the workers to do the job
   time.sleep(10)

   session1 = client.get_session()
   schema = session1.get_schema(schema_name)

   collection = schema.get_collection(collection_name)

   print(collection.find().execute().fetch_all())

The output of the last print will look like the following:

.. code-block:: python

     [{'_id': '00005b770c7f0000000000000389', 'name': 'Worker_2'}, \
      {'_id': '00005b770c7f000000000000038a', 'name': 'Worker_2'}, \
      {'_id': '00005b770c7f000000000000038b', 'name': 'Worker_2'}, \
      {'_id': '00005b770c7f000000000000038c', 'name': 'Worker_2'}, \
      {'_id': '00005b770c7f000000000000038d', 'name': 'Worker_2'}, \
      {'_id': '00005b770c7f000000000000038e', 'name': 'Worker_2'}, \
      {'_id': '00005b770c7f000000000000038f', 'name': 'Worker_2'}, \
      {'_id': '00005b770c7f0000000000000390', 'name': 'Worker_2'}, \
      {'_id': '00005b770c7f0000000000000391', 'name': 'Worker_2'}, \
      {'_id': '00005b770c7f0000000000000392', 'name': 'Worker_2'}, \
      {'_id': '00005b770c7f0000000000000393', 'name': 'Worker_1'}, \
      {'_id': '00005b770c7f0000000000000394', 'name': 'Worker_4'}, \
      {'_id': '00005b770c7f0000000000000395', 'name': 'Worker_1'}, \
      {'_id': '00005b770c7f0000000000000396', 'name': 'Worker_4'}, \
      {'_id': '00005b770c7f0000000000000397', 'name': 'Worker_7'}, \
      {'_id': '00005b770c7f0000000000000398', 'name': 'Worker_1'}, \
      {'_id': '00005b770c7f0000000000000399', 'name': 'Worker_4'}, \
      {'_id': '00005b770c7f000000000000039a', 'name': 'Worker_7'}, \
      {'_id': '00005b770c7f000000000000039b', 'name': 'Worker_1'}, \
      {'_id': '00005b770c7f000000000000039c', 'name': 'Worker_4'}, \
      {'_id': '00005b770c7f000000000000039d', 'name': 'Worker_7'}, \
      {'_id': '00005b770c7f000000000000039e', 'name': 'Worker_1'}, \
      {'_id': '00005b770c7f000000000000039f', 'name': 'Worker_8'}, \
      {'_id': '00005b770c7f00000000000003a0', 'name': 'Worker_4'}, \
      {'_id': '00005b770c7f00000000000003a1', 'name': 'Worker_7'}, \
      ... \
      {'_id': '00005b770c7f000000000000043c', 'name': 'Worker_9'}]

The **18** workers took random turns to add their documents to the collection,
sharing only **6** active connections given by ``'max_size': 6`` in the
``options_dict`` given to the client instance at the time was created on
:func:`mysqlx.get_client(connection_dict, options_dict)`.
