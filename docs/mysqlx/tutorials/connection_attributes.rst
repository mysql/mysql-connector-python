Handling Connection Attributes (:func:`mysqlx.get_session()`)
=============================================================

The MySQL server stores operational details for all and each client that is connected, such attributes are called connection attributes. Some connection attributes are defined by X DevAPI itself, these can be observed in the following example.

.. code-block:: python

   import mysqlx

   # Connect to server on localhost
   session = mysqlx.get_session({
       'host': 'localhost',
       'port': 33060,
       'user': 'mike',
       'password': 's3cr3t!'
   })

On the server the connection attributes may look like the following:

.. code-block:: python

   mysql> SELECT ATTR_NAME, ATTR_VALUE FROM performance_schema.session_account_connect_attrs;

   +-----------------+------------------------+
   | ATTR_NAME       | ATTR_VALUE             |
   +-----------------+------------------------+
   | _pid            | 16988                  |
   | program_name    | mysql                  |
   | _client_name    | mysql-connector-python |
   | _thread         | 17588                  |
   | _client_version | 8.0.15                 |
   | _client_license | GPL-2.0                |
   | _os             | Win64                  |
   | _platform       | x86_64                 |
   +-----------------+------------------------+

The other kind of connection attributes are user specified, these can be specified by the key ``connection-attributes`` or in the form of URL attribute while getting the connection for example:

.. code-block:: python

   import mysqlx

   mysqlx.getSession('mysqlx://mike@localhost:33060/schema?connection-attributes=[my_attribute=some_value,foo=bar]')

On the server the connection attributes may look like the following:

.. code-block:: python

   mysql> SELECT ATTR_NAME, ATTR_VALUE FROM performance_schema.session_account_connect_attrs;

   +-----------------+------------------------+
   | ATTR_NAME       | ATTR_VALUE             |
   +-----------------+------------------------+
   | _pid            | 16988                  |
   | program_name    | mysql                  |
   | _client_name    | mysql-connector-python |
   | _thread         | 17588                  |
   | _client_version | 8.0.15                 |
   | _client_license | GPL-2.0                |
   | _os             | Win64                  |
   | _platform       | x86_64                 |
   | foo             | bar                    |
   | my_attribute    | some_value             |
   +-----------------+------------------------+

.. note:: connection attributes defined by the user can not start with the underscore ( _ ) character.
