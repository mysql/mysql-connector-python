MySQL Connector/Python
======================

.. === <both> [repl-mysqlx("mysql-connector-python", "mysqlx-connector-python")] ===

.. image::
    https://img.shields.io/pypi/v/mysql-connector-python.svg
    :target: https://pypi.org/project/mysql-connector-python/

.. image::
    https://img.shields.io/pypi/pyversions/mysql-connector-python.svg
    :target: https://pypi.org/project/mysql-connector-python/

.. image::
    https://img.shields.io/pypi/l/mysql-connector-python.svg
    :target: https://pypi.org/project/mysql-connector-python/

.. === </both> ===

.. === <mysql> [repl(" - We refer to it as the", "."), repl("`Classic API <https://dev.mysql.com/doc/connector-python/en/connector-python-reference.html>`__.", "")] ====

MySQL Connector/Python enables Python programs to access MySQL databases, using
an API that is compliant with the `Python Database API Specification v2.0
(PEP 249) <https://www.python.org/dev/peps/pep-0249/>`__ - We refer to it as the
`Classic API <https://dev.mysql.com/doc/connector-python/en/connector-python-reference.html>`__.

.. === </mysql> ====

.. === <mysqlx> [repl("It also", "MySQL Connector/Python")] ===

It also contains an implementation of the `X DevAPI <https://dev.mysql.com/doc/x-devapi-userguide/en>`__
- An Application Programming Interface for working with the `MySQL Document Store
<https://dev.mysql.com/doc/refman/en/document-store.html>`__.

.. === </mysqlx> ===

.. === <mysql> [repl("* `X DevAPI <https://dev.mysql.com/doc/x-devapi-userguide/en>`__", "")] ====

Features
--------

* `Asynchronous Connectivity <https://dev.mysql.com/doc/connector-python/en/connector-python-asyncio.html>`__
* `C-extension <https://dev.mysql.com/doc/connector-python/en/connector-python-cext.html>`__
* `Telemetry <https://dev.mysql.com/doc/connector-python/en/connector-python-opentelemetry.html>`__
* `X DevAPI <https://dev.mysql.com/doc/x-devapi-userguide/en>`__

.. === </mysql> ====

Licensing
---------

Please refer to the `README.txt <README.txt>`__ and `LICENSE.txt <LICENSE.txt>`__
files, available in this repository, for further details.

Contributing
------------

We greatly appreciate feedback from our users, including bug reports and code contributions. Your input helps us improve, and we thank you for any issues you report or code you contribute. Please refer to the `contributing <CONTRIBUTING.md>`__ document for additional information.

Installation
------------

Connector/Python contains the Classic and X DevAPI connector APIs, which are
installed separately. Any of these can be installed from a binary
or source distribution.

Binaries are distributed in the following package formats:

* `RPM <https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/8/html/packaging_and_distributing_software/introduction-to-rpm_packaging-and-distributing-software>`__
* `WHEEL <https://packaging.python.org/en/latest/discussions/package-formats/#what-is-a-wheel>`__

On the other hand, the source code is distributed as a compressed file
from which a wheel package can be built.

The recommended way to install Connector/Python is via `pip <https://pip.pypa.io/>`__,
which relies on WHEEL packages. For such a reason, it is the installation procedure
that is going to be described moving forward.

Please, refer to the official MySQL documentation `Connector/Python Installation
<https://dev.mysql.com/doc/connector-python/en/connector-python-installation.html>`__ to
know more about installing from an RPM, or building and installing a WHEEL package from
a source distribution.

Before installing a package with `pip <https://pip.pypa.io/>`__, it is strongly suggested
to have the most recent ``pip`` version installed on your system.
If your system already has ``pip`` installed, you might need to update it. Or you can use
the `standalone pip installer <https://pip.pypa.io/en/latest/installation/>`__.

.. === <mysql> [repl("The *Classic API* can be installed via pip as follows:", "")] ===

The *Classic API* can be installed via pip as follows:

.. code-block:: bash

    $ pip install mysql-connector-python

.. === </mysql> ====

.. === <mysqlx> [repl("similarly, the *X DevAPI* can be installed with:", "")] ===

similarly, the *X DevAPI* can be installed with:

.. code-block:: bash

    $ pip install mysqlx-connector-python

Please refer to the `installation tutorial <https://dev.mysql.com/doc/dev/connector-python/installation.html>`__
for installation alternatives of the X DevAPI.

.. === </mysqlx> ===

Installation Options
++++++++++++++++++++

Connector packages included in MySQL Connector/Python allow you to install
optional dependencies to unleash certain functionalities.

.. === <mysql> ===
.. code-block:: bash

    # 3rd party packages to unleash the telemetry functionality are installed
    $ pip install mysql-connector-python[telemetry]

.. === </mysql> ===

.. === <mysqlx> [repl("similarly, for the X DevAPI:", "")] ===

similarly, for the X DevAPI:

.. code-block:: bash

    # 3rd party packages to unleash the compression functionality are installed
    $ pip install mysqlx-connector-python[compression]

.. === </mysqlx> ===

This installation option can be seen as a shortcut to install all the
dependencies needed by a particular feature. Mind that this is optional
and you are free to install the required dependencies by yourself.

.. === <mysql> [repl("Options for the Classic API connector:", "Available options:")] ===

Options for the Classic API connector:

* dns-srv
* gssapi
* webauthn
* telemetry

.. === </mysql> ===

.. === <mysqlx> [repl("Options for the X DevAPI connector:", "Available options:")] ===

Options for the X DevAPI connector:

* dns-srv
* compression

.. === </mysqlx> ===

.. === <mysql> [repl("Classic API ", ""), repl("-------", "-----------")] ===

Classic API Sample Code
-----------------------

.. code:: python

    import mysql.connector

    # Connect to server
    cnx = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="mike",
        password="s3cre3t!")

    # Get a cursor
    cur = cnx.cursor()

    # Execute a query
    cur.execute("SELECT CURDATE()")

    # Fetch one result
    row = cur.fetchone()
    print("Current date is: {0}".format(row[0]))

    # Close connection
    cnx.close()

.. === </mysql> ===

.. === <mysqlx> [repl("X DevAPI ", ""), repl("-------", "-----------")] ===

X DevAPI Sample Code
--------------------

.. code:: python

    import mysqlx

    # Connect to server
    session = mysqlx.get_session(
       host="127.0.0.1",
       port=33060,
       user="mike",
       password="s3cr3t!")
    schema = session.get_schema("test")

    # Use the collection "my_collection"
    collection = schema.get_collection("my_collection")

    # Specify which document to find with Collection.find()
    result = collection.find("name like :param") \
                       .bind("param", "S%") \
                       .limit(1) \
                       .execute()

    # Print document
    docs = result.fetch_all()
    print(r"Name: {0}".format(docs[0]["name"]))

    # Close session
    session.close()

.. === </mysqlx> ===

.. === <mysql> ===

HeatWave GenAI and Machine Learning Support
-------------------------------------------

MySQL Connector/Python now includes an optional API for integrating directly with MySQL HeatWave's AI and Machine Learning capabilities. This new SDK is designed to reduce the time required to generate proofs-of-concept (POCs) by providing an intuitive Pythonic interface that automates the management of SQL tables and procedures.

The new ``mysql.ai`` module offers two primary components:

* **GenAI:** Provides implementations of LangChain's abstract ``LLM``, ``VectorStore``, and ``Embeddings`` classes (``MyLLM``, ``MyVectorStore``, ``MyEmbeddings``). This ensures full interoperability with existing LangChain pipelines, allowing developers to easily substitute existing components with HeatWave-backed versions.
* **AutoML:** Provides Scikit-Learn compatible estimators (``MyClassifier``, ``MyRegressor``, ``MyAnomalyDetector``, ``MyGenericTransformer``) that inherit from standard Scikit-Learn mixins. These components accept Pandas DataFrames and can be dropped directly into existing Scikit-Learn pipelines and grid searches.

**Note on Dependencies:** These features introduce dependencies on ``langchain``, ``pandas``, and ``scikit-learn``. To keep existing installations unchanged and the base connector lightweight, these dependencies are **not installed by default**. You must install them separately to use the ``mysql.ai`` features.

**Example: GenAI Chatbot with Memory**

This example demonstrates how to use ``MyLLM`` within a loop to create a simple chatbot that maintains conversation history.

.. code:: python

    from collections import deque
    from mysql import connector
    from mysql.ai.genai import MyLLM

    def run_chatbot(db_connection, chat_history_size=5):
        # Initialize MyLLM with the database connection
        my_llm = MyLLM(db_connection)

        # Maintain a limited history for context
        chat_history = deque(maxlen=chat_history_size)
        system_msg = "System: You are a helpful AI assistant."

        while True:
            user_input = input("\nUser: ")
            if user_input.lower() in ["exit", "quit"]:
                break

            # Format history and invoke the LLM
            history = [system_msg] + list(chat_history) + [f"User: {user_input}"]
            prompt = "\n".join(history)

            # Invoke HeatWave GenAI
            response = my_llm.invoke(prompt)
            print(f"Bot: {response}")

            # Update history
            chat_history.append(f"User: {user_input}")
            chat_history.append(f"Bot: {response}")

    # Usage
    with connector.connect(user='root', database='mlcorpus') as db_connection:
        run_chatbot(db_connection)

**Example: HeatWave AutoML in a Scikit-Learn Pipeline**

This example shows how to use ``MyClassifier`` as a drop-in replacement within a standard Scikit-Learn pipeline.

.. code:: python

    import pandas as pd
    from mysql import connector
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    from mysql.ai.ml import MyClassifier

    # 1. Setup Data (Pandas DataFrame)
    X = pd.DataFrame([[0.5, 0.1], [1.0, 0.8], [0.1, 0.2]], columns=["feat1", "feat2"])
    y = pd.Series([0, 1, 0], name="target")

    # 2. Connect and Train
    with connector.connect(user='root', database='mlcorpus') as db_connection:
        # Initialize the HeatWave classifier
        clf = MyClassifier(db_connection)

        # Create a standard Scikit-Learn pipeline
        pipe = Pipeline([
            ("scaler", StandardScaler()),
            ("mysql_clf", clf)
        ])

        # Fit the model (automates upload and training on HeatWave)
        pipe.fit(X, y)

        # Predict
        preds = pipe.predict(X)
        print(f"Predictions: {preds}")

        # Score
        score = pipe.score(X, y)
        print(f"Accuracy: {score}")

.. === </mysql> ===

.. === <both> [repl-mysql("- `MySQL Connector/Python X DevAPI Reference <https://dev.mysql.com/doc/dev/connector-python/>`__", ""), repl-mysqlx("- `MySQL Connector/Python Developer Guide <https://dev.mysql.com/doc/connector-python/en/>`__", "")] ===

Additional Resources
--------------------

- `MySQL Connector/Python Developer Guide <https://dev.mysql.com/doc/connector-python/en/>`__
- `MySQL Connector/Python X DevAPI Reference <https://dev.mysql.com/doc/dev/connector-python/>`__
- `MySQL Connector/Python Forum <http://forums.mysql.com/list.php?50>`__
- `MySQL Public Bug Tracker <https://bugs.mysql.com>`__
- `Slack <https://mysqlcommunity.slack.com>`__ (`Sign-up <https://lefred.be/mysql-community-on-slack/>`__ required if you do not have an Oracle account)
- `Stack Overflow <https://stackoverflow.com/questions/tagged/mysql-connector-python>`__
- `Oracle Blogs <https://blogs.oracle.com/search.html?q=connector-python>`__

.. === </both> ===
