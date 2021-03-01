Connection Routers
==================

*Connection Routers* is a technique used for connecting to one of multiple hosts using connection failover, which attempts to connect to the next endpoint if the current endpoint is not available before raising an error. For this technique, define multiple hosts by specifying a URI-like string containing multiple hosts, ports, and an optional priority or using the ``routers`` option when invoking :func:`mysqlx.get_client()`.

This technique enables the connector to perform automatic connection failover selection when an endpoint are not available. When multiple endpoints are available, the chosen server used for the session depends on the ``priority`` option. If a priority is set, it must be defined for each endpoint. The available endpoint with the highest :data:`priority` is prioritized first. If not specified, a randomly available endpoint is used.

Here's an example of how to specify multiple hosts `URI-like` :data:`string` and without priority when calling the :func:`mysqlx.get_client()`. The URI-like connection string is formatted as:

.. code-block:: python

   import mysqlx

   connection_str = 'mysqlx://root:@[(address=unreachable_host),(address=127.0.0.1:33060)]?connect-timeout=2000'
   options_string = '{}'  # An empty document

   client = mysqlx.get_client(connection_str, options_string)
   session = client.get_session()

   # (...)

   session.close()
   client.close()


The multiple hosts can also be given as a :data:`list` with the ``routers`` in the connection settings:

.. code-block:: python

   import mysqlx

   routers = [
       {"host": "unreachable_host"},  # default port is 33060
       {"host": "127.0.0.1", "port": 33060},
   ]

   connection_dict = {
       'routers': routers,
       'port': 33060,
       'user': 'mike',
       'password': 's3cr3t!'
       'connect_timeout': 2000,
   }
   options_dict = {}  # empty dict object

   client = mysqlx.get_client(connection_dict, options_dict)
   session = client.get_session()

   # (...)

   session.close()
   client.close()


The above examples have two hosts but many more hosts and ports can be defined, and it's important to understand that the supplied MySQL user and password supplied (in either the URI-like string or in the user and password options) applies to all of the possible endpoints. Therefore the same MySQL account must exist on each of the endpoints.

.. note:: Because of the failover, a connection attempt for establishing a connection on all the given hosts will occur before an error is raised, so using the ``connect_timeout`` option is recommended when a large number of hosts could be down. The order for the connection attempts occur randomly unless the ``priority`` option is defined.

.. note:: The ``connect_timeout`` option's value must be a positive integer.

Specifying multiple hosts with a priority in the `URI-like` :data:`string` is formatted as such:

.. code-block:: python

   import mysqlx

   connection_str = 'mysqlx://root:@[(address=unreachable_host, priority=100),(address=127.0.0.1:33060, priority=90)]?connect-timeout=2000'
   options_string = '{}'  # An empty dictionary object

   client = mysqlx.get_client(connection_str, options_string)
   session = client.get_session()

   # (...)

   session.close()
   client.close()


Specifying multiple hosts with a priority in the connection settings is formatted as such:

.. code-block:: python

   import mysqlx

   routers = [{"host": "unreachable_host", "priority": 100},  # default port is 33060
              {"host": "127.0.0.1", "port": 33060, "priority": 90}
   ]

   connection_dict = {
       'routers': routers,
       'port': 33060,
       'user': 'mike',
       'password': 's3cr3t!',
       'connect_timeout': 2000
   }
   options_dict = {}

   client = mysqlx.get_client(connection_dict, options_dict)
   session = client.get_session()

   # (...)

   session.close()
   client.close()

.. note:: Valid values for the ``priority`` option are values :data:`1` to :data:`100``, where 100 is the highest priority value. Priority determines the connection order with highest priority value being first. If priority is given for one host, then a priority value must be given for all the hosts.

The Routers technique can be combined with the pooling technique by passing a pooling configuration for :class:`mysqlx.Client`. Set the pooling options by passing a :data:`dict` or a JSON document string in the second parameter.

The following example uses the same MySQL as in previous examples, but with different hostnames to emulate two other servers, and the ``options_dict`` is a dictionary with the settings for each pool. Notice that with ``max_size`` option set to 5, we can get up to 10 sessions because a connection pool is created for each server with 5 connections.

.. code-block:: python

    import mysqlx

    routers = [{"host": "localhost", "priority": 100},  # default port is 33060
               {"host": "127.0.0.1", "port": 33060, "priority": 90}
    ]

    connection_dict = {
        'routers': routers,
        'port': 33060,
        'user': 'root',
        'password': '',
        'connect_timeout':2000
    }

    options_dict = {'pooling':{'max_size': 5, 'queue_timeout': 1000}}

    client = mysqlx.get_client(connection_dict, options_dict)

    # We can get 5 sessions from each pool.

    for n in range(5):
        print(f"session: {n}")
        session = client.get_session()
        res = session.sql("select connection_id()").execute().fetch_all()
        for row in res:
            print(f"connection id: {row[0]}")

    for n in range(5):
        print(f"session: {n}")
        session = client.get_session()
        res = session.sql("select connection_id()").execute().fetch_all()
        for row in res:
            print(f"connection id: {row[0]}")


The output:

.. code-block:: python

    session: 0
    connection id: 603
    session: 1
    connection id: 604
    session: 2
    connection id: 605
    session: 3
    connection id: 606
    session: 4
    connection id: 607
    session: 0
    connection id: 608
    session: 1
    connection id: 609
    session: 2
    connection id: 610
    session: 3
    connection id: 611
    session: 4
    connection id: 612


The following example shows using Multi-host and failover while using a pool. In this example, the “unreachable_host” has higher priority than the second host :data:`"127.0.0.1"`, so the connection is attempted to :data:`"unreachable_host"` first but it will fail. However, this does not raise an error and the connection attempt to the host :data:`"127.0.0.1"` that's available will succeed. However, an error is raised when the pool is maxed out.

.. code-block:: python

   import mysqlx

   routers = [{"host": "unreachable_host", "priority": 100},
              {"host": "127.0.0.1", "port": 33060, "priority": 90}
   ]

   connection_dict = {
       'routers': routers,
       'port': 33060,
       'user': 'mike',
       'password': 's3cr3t!',
       'connect_timeout': 2000
   }

   options_dict = {'pooling':{'max_size': 5, 'queue_timeout': 1000}}

   client = mysqlx.get_client(connection_dict, options_dict)

   for n in range(5):
        print(f"session: {n}")
        session = client.get_session()
        res = session.sql("select connection_id()").execute().fetch_all()
        for row in res:
            print(f"connection id: {row[0]}")

   # Since the "unreachable_host" is unavailable and the max_size option for
   # the pools is set to 5, we can only get 5 sessions prior to get an error.
   # By requiring another session a mysqlx.errors.PoolError error is raised.
   client.get_session()  # This line raises an PoolError


The code above will give an output similar to the following:

.. code-block:: python

    session: 0
    connection id: 577
    session: 1
    connection id: 578
    session: 2
    connection id: 579
    session: 3
    connection id: 580
    session: 4
    connection id: 581

    mysqlx.errors.PoolError: Unable to connect to any of the target hosts: [
        pool: 127.0.0.1_33060_... error: pool max size has been reached
        pool: unreachable_host_33060_... error: Cannot connect to host: [Errno 11001] getaddrinfo failed
    ]