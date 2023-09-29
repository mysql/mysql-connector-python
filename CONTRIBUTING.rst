Contributing Guidelines
=======================

We love getting feedback from our users. Bugs and code contributions are great forms of feedback and we thank you for any bugs you report or code you contribute.

Reporting Issues
----------------

Before reporting a new bug, please `check first <https://bugs.mysql.com/search.php>`_ to see if a similar bug already exists.

Bug reports should be as complete as possible. Please try and include the following:

- Complete steps to reproduce the issue.
- Any information about platform and environment that could be specific to the bug.
- Specific version of the product you are using.
- Specific version of the server being used.
- Sample code to help reproduce the issue if possible.

Contributing Code
-----------------

Contributing to this project is easy. You just need to follow these steps.

- Make sure you have a user account at `bugs.mysql.com <https://bugs.mysql.com>`_. You will need to reference this user account when you submit your Oracle Contributor Agreement (a.k.a. OCA).
- Sign the Oracle Contributor Agreement. You can find instructions for doing that at the `OCA Page <https://oca.opensource.oracle.com/>`_.
- Develop your pull request. Make sure you are aware of the `requirements <https://dev.mysql.com/doc/dev/connector-python/8.0/requirements.html>`_ for the project.
- Validate your pull request by including tests that sufficiently cover the functionality you are adding.
- Verify that the entire test suite passes with your code applied.
- Submit your pull request. While you can submit the pull request via `GitHub <https://github.com/mysql/mysql-connector-python/pulls>`_, you can also submit it directly via `bugs.mysql.com <https://bugs.mysql.com>`_.

Thanks again for your wish to contribute to MySQL. We truly believe in the principles of open source development and appreciate any contributions to our projects.

Setting Up a Development Environment
------------------------------------

The following tips provide all the technical directions you should follow when writing code and before actually submitting your contribution.

1) Make sure you have the necessary `prerequisites <https://dev.mysql.com/doc/dev/connector-python/8.0/installation.html#prerequisites>`_ for building the project and `Pylint <https://www.pylint.org/>`_ for static code analysis

2) Clone MySQL Connector/Python

   .. code-block:: bash

       shell> git clone https://github.com/mysql/mysql-connector-python.git

Coding Style
~~~~~~~~~~~~

Please follow the MySQL Connector/Python coding standards when contributing with code.

All files should be formatted using the `black <https://github.com/psf/black>`_ auto-formatter and `isort <https://pycqa.github.io/isort/>`_. This will be run by `pre-commit <https://pre-commit.com>`_ if it's configured.

For C files, the `PEP 7 <https://peps.python.org/pep-0007/>`_ should be followed. A ``.clang-format`` configuration is included in the source, so you can manually format the code using the `clang-format <https://clang.llvm.org/docs/ClangFormat.html>`_ tool.

Pre-commit Checks
~~~~~~~~~~~~~~~~~

MySQL Connector/Python comes with a pre-commit config file, which manages Git pre-commit hooks. These hooks are useful for identifing issues before committing code.

To use the pre-commit hooks, you first need to install the `pre-commit <https://pre-commit.com>`_ package and then the git hooks:

   .. code-block:: bash

       shell> python -m pip install pre-commit
       shell> pre-commit install

The first time pre-commit runs, it will automatically download, install, and run the hooks. Running the hooks for the first time may be slow, but subsequent checks will be significantly faster.

Now, pre-commit will run on every commit.

Running `mysql-connector-python` Tests
--------------------------------------

Any code you contribute needs to pass our test suite. Please follow these steps to run our tests and validate your contributed code.

Run the entire test suite:

   .. code-block:: bash

       shell> python unittests.py --with-mysql=<mysql-dir> --with-mysql-capi=<mysql-capi-dir>

Example:

   .. code-block:: sh

       shell> python unittests.py --with-mysql=/usr/local/mysql --with-mysql-capi=/usr/local/mysql

The tests can be configured to be launched using an external server or bootstrapping it. The former is preferred (we'll assume so moving forward).

As you can see, there are several parameters that can be injected into the ``unittests`` module. The parameters shown above are optional, or a must if you want to run the tests with the *C extension* enabled for the ``mysql.connector`` module.

The ``with-mysql-capi`` flag is needed to build the `C extension` of ``mysql.connector``.

Additionally, there are parameters or flags that can be provided to set values to be used when connecting to the server:

* **user**: the value stored by the environment variable ``MYSQL_USER`` is used (if set), otherwise, ``root`` is used by default.
* **password**: the value of ``MYSQL_PASSWORD`` is used (if set), otherwise, ``empty_string`` is used by default.
* **port**: the value of ``MYSQL_PORT`` is used (if set), otherwise, ``3306`` is used by default.
* **host**: the value of ``MYSQL_HOST`` is used (if set), otherwise, ``127.0.0.1`` (localhost) is used by default.

The previous defaults conform to the standard or default configuration implemented by the MySQL server. Actually, there are many more flags available, you can explore them via ``python unittests.py --help``.

There are two core flags you can use to control the unit tests selection:

1. **-t** which is a shortcut for **--test**. This command executes one test module provided the module name::

    $ python unittests.py --use-external-server --verbosity 2 --password=$MYPASS -t cext_cursor

2. **-T** which is a shortcut for **--one-test**. This command executes a particular test following a finer-grained syntax such as ``<module>[.<class>[.<method>]]``::

    $ python unittests.py --use-external-server --verbosity 2 --password=$MYPASS -T tests.test_bugs.BugOra16660356
    $ python unittests.py --use-external-server --verbosity 2 --password=$MYPASS -T tests.test_bugs.BugOra17041240.test_cursor_new

If you do not provide any flag regarding *control of the unit tests selection*, all available test modules will be run. Some of the available test modules are:

- abstracts
- authentication
- bugs
- cext_api
- cext_cursor
- connection
- constants
- conversion
- cursor
- errors
- mysql_datatypes
- network
- optionfiles
- pooling
- protocol
- qa_bug16217743
- qa_caching_sha2_password
- utils

The list is not complete, but you can deduce and find more module names by inspecting the **tests** folder and its subfolders.


Running `mysqlx-connector-python` Tests
---------------------------------------

Any code you contribute needs to pass our test suite. Please follow these steps to run our tests and validate your contributed code.

Run the entire test suite:

   .. code-block:: bash

       shell> python unittests.py --with-mysql=<mysql-dir> --with-protobuf-include-dir=<protobuf-include-dir> --with-protobuf-lib-dir=<protobuf-lib-dir> --with-protoc=<protoc-binary>

Example:

   .. code-block:: sh

       shell> python unittests.py --with-mysql=/usr/local/mysql --with-protobuf-include-dir=/usr/local/protobuf/include --with-protobuf-lib-dir=/usr/local/protobuf/lib --with-protoc=/usr/local/protobuf/bin/protoc

The tests can be configured to be launched using an external server or bootstrapping it. The former is preferred (we'll assume so moving forward).

As you can see, there are several parameters that can be injected into the ``unittests`` module. The parameters shown above are optional, or a must if you want to run the tests with the *C extension* enabled for the ``mysqlx`` module.

The ``protobuf`` flags are needed to build the `C extension` of  ``mysqlx``.

Additionally, there are parameters or flags that can be provided to set values to be used when connecting to the server:

* **user**: the value stored by the environment variable ``MYSQL_USER`` is used (if set), otherwise, ``root`` is used by default.
* **password**: the value of ``MYSQL_PASSWORD`` is used (if set), otherwise, ``empty_string`` is used by default.
* **mysqlx-port**: the value of ``MYSQLX_PORT`` is used (if set), otherwise, ``33060`` is used by default.
* **host**: the value of ``MYSQL_HOST`` is used (if set), otherwise, ``127.0.0.1`` (localhost) is used by default.

The previous defaults conform to the standard or default configuration implemented by the MySQL server. Actually, there are many more flags available, you can explore them via ``python unittests.py --help``.

There are two core flags you can use to control the unit tests selection:

1. **-t** which is a shortcut for **--test**. This command executes one test module provided the module name::

    $ python unittests.py --use-external-server --verbosity 2 --password=$MYPASS -t mysqlx_connection

2. **-T** which is a shortcut for **--one-test**. This command executes a particular test following a finer-grained syntax such as ``<module>[.<class>[.<method>]]``::

    $ python unittests.py --use-external-server --verbosity 2 --password=$MYPASS -T tests.test_mysqlx_crud.MySQLxDbDocTests
    $ python unittests.py --use-external-server --verbosity 2 --password=$MYPASS -T tests.test_mysqlx_crud.MySQLxDbDocTests.test_dbdoc_creation

If you do not provide any flag regarding *control of the unit tests selection*, all available test modules will be run. Some of the available test modules are:

- mysql_datatypes
- mysqlx_connection
- mysqlx_crud
- mysqlx_errorcode
- mysqlx_pooling

The list is not complete, but you can deduce and find more module names by inspecting the **tests** folder and its subfolders.


Running `mysql-connector-python` Tests using a Docker Container
---------------------------------------------------------------

For **Linux** and **macOS** users, there is a script that builds and runs a Docker container which then executes the test suite (*the C extension is built and enabled only if explicitly instructed*). This means no external dependency, apart from a running MySQL server, is needed.

The script uses the environment variables described previously and introduces a few new ones. These are mostly meant to be used for configuring the Docker container itself. They allow to specify the path to a *Oracle Linux* engine image, the network proxy setup, the URL of the PyPI repository to use and whether you want the **C-EXT** enabled or not.

* ``BASE_IMAGE`` (**container-registry.oracle.com/os/oraclelinux:9-slim** by default)
* ``HTTP_PROXY`` (value of the environment variable in the host by default)
* ``HTTPS_PROXY`` (value of the environment variable in the host by default)
* ``NO_PROXY`` (value of the environment variable in the host by default)
* ``PYPI_REPOSITORY`` (https://pypi.org/pypi by default)
* ``MYSQL_CEXT`` (used to control the building of the **connector.mysql** C-EXT. If set to ``true`` or ``yes``, the extension is built, otherwise it is not)
* ``MYSQL_SOCKET`` (described below)

There is one additional environment variable called ``TEST_PATTERN`` which can be used to provide a string or a regular expression that is applied for filtering one or more matching unit tests to execute.

For instance, if you want to run the test module named *cursor* you'd be using::

    $ TEST_PATTERN='cursor' MYSQL_PASSWORD='s3cr3t' ./tests/docker/runner.sh

Similarly, if you want to run all tests including the pattern *con* you'd be issuing::

    $ TEST_PATTERN='.*con.*' MYSQL_PASSWORD='s3cr3t' ./tests/docker/runner.sh

If you want to run **connector.mysql** tests related to the C-EXT functionality you could use::

    $ MYSQL_CEXT='true' TEST_PATTERN='cext.*' MYSQL_PASSWORD='s3cr3t' ./tests/docker/runner.sh

In the examples above, a standard MySQL server configuration is assumed, that's the reason the values for ``MYSQL_HOST``, ``MYSQL_USER`` or ``MYSQL_PORT``  weren't specified.

For **Windows** users, you can set up a suitable environment to run bash scripts by installing `Git Bash <https://git-scm.com/>`_, and using the console it provides instead of the natives *PowerShell* or *CMD*.

Similar to when the tests run on a local environment, the ``MYSQL_HOST`` variable is only relevant for the functional tests.

On **Linux**, the variable is optional and the Docker container will run using the "host" network mode whilst tests assume the MySQL server is listening on ``localhost``.

On **macOS** and **Windows**, since containers run on a virtual machine, host loopback addresses are not reachable. In that case, the ``MYSQL_HOST`` variable is required and should specify the hostname or IP address of the MySQL server. Optionally, you can use ``host.docker.internal`` as ``MYSQL_HOST`` if you want to connect to a server hosted locally `[reference] <https://stackoverflow.com/questions/24319662/from-inside-of-a-docker-container-how-do-i-connect-to-the-localhost-of-the-mach>`_.

Due to some `know limitations <https://github.com/docker/for-mac/issues/483>`_ on the macOS Docker architecture, Unix socket tests can only run on Linux. In that case, if the ``MYSQL_SOCKET`` variable is explicitly specified, a shared volume between the host and the container will be created as a mount point from the socket file path in the host and an internal container directory specified as a volume, where the socket file path becomes available.

That being said, the following are some examples of possible use cases:

* Running the test modules whose name follows the pattern ``c.*`` from a mac whose IP is ``232.188.98.520``, and the password for ``root`` is ``s3cr3t``. Classic protocol is listening on port ``3306``::

    $ TEST_PATTERN='c.*' MYSQL_HOST='192.168.68.111' MYSQL_PASSWORD='s3cr3t' ./tests/docker/runner.sh

* Running the whole test suite from Linux with MySQL user account ``docker``, and password ``s3cr3t``. Classic protocol is listening on port ``3308``::

    $ MYSQL_PORT='3308' MYSQL_USER='docker' MYSQL_PASSWORD='s3cr3t' ./tests/docker/runner.sh

* Same setup as before but with the **connector.mysql** C-EXT enabled::

    $ MYSQL_CEXT='true' MYSQL_PORT='3308' MYSQL_USER='docker' MYSQL_PASSWORD='s3cr3t' ./tests/docker/runner.sh


Running `mysqlx-connector-python` Tests using a Docker Container
----------------------------------------------------------------

For **Linux** and **macOS** users, there is a script that builds and runs a Docker container which then executes the test suite (*the C extension is built and enabled only if explicitly instructed*). This means no external dependency, apart from a running MySQL server, is needed.

The script uses the environment variables described previously and introduces a few new ones. These are mostly meant to be used for configuring the Docker container itself. They allow to specify the path to a *Oracle Linux* engine image, the network proxy setup, the URL of the PyPI repository to use and whether you want the **C-EXT** enabled or not.

* ``BASE_IMAGE`` (**container-registry.oracle.com/os/oraclelinux:9-slim** by default)
* ``HTTP_PROXY`` (value of the environment variable in the host by default)
* ``HTTPS_PROXY`` (value of the environment variable in the host by default)
* ``NO_PROXY`` (value of the environment variable in the host by default)
* ``PYPI_REPOSITORY`` (https://pypi.org/pypi by default)
* ``MYSQLX_CEXT`` (used to control the building of the **mysqlx** C-EXT. If set to ``true`` or ``yes``, the extension is built, otherwise it is not)
* ``MYSQL_SOCKET`` (described below)

There is one additional environment variable called ``TEST_PATTERN`` which can be used to provide a string or a regular expression that is applied for filtering one or more matching unit tests to execute.

For instance, if you want to run the test module named *cursor* you'd be using::

    $ TEST_PATTERN='mysqlx_connection' MYSQL_PASSWORD='s3cr3t' ./tests/docker/runner.sh

Similarly, if you want to run all tests including the pattern *con* you'd be issuing::

    $ TEST_PATTERN='.*con.*' MYSQL_PASSWORD='s3cr3t' ./tests/docker/runner.sh

If you want to run **mysqlx** tests with the C-EXT enabled::

    $ MYSQLX_CEXT='true' TEST_PATTERN='mysqlx_crud' MYSQL_PASSWORD='s3cr3t' ./tests/docker/runner.sh


In the examples above, a standard MySQL server configuration is assumed, that's the reason the values for ``MYSQL_HOST``, ``MYSQL_USER`` or ``MYSQLX_PORT`` weren't specified.

For **Windows** users, you can set up a suitable environment to run bash scripts by installing `Git Bash <https://git-scm.com/>`_, and using the console it provides instead of the natives *PowerShell* or *CMD*.

Similar to when the tests run on a local environment, the ``MYSQL_HOST`` variable is only relevant for the functional tests.

On **Linux**, the variable is optional and the Docker container will run using the "host" network mode whilst tests assume the MySQL server is listening on ``localhost``.

On **macOS** and **Windows**, since containers run on a virtual machine, host loopback addresses are not reachable. In that case, the ``MYSQL_HOST`` variable is required and should specify the hostname or IP address of the MySQL server. Optionally, you can use ``host.docker.internal`` as ``MYSQL_HOST`` if you want to connect to a server hosted locally `[reference] <https://stackoverflow.com/questions/24319662/from-inside-of-a-docker-container-how-do-i-connect-to-the-localhost-of-the-mach>`_.

Due to some `know limitations <https://github.com/docker/for-mac/issues/483>`_ on the macOS Docker architecture, Unix socket tests can only run on Linux. In that case, if the ``MYSQL_SOCKET`` variable is explicitly specified, a shared volume between the host and the container will be created as a mount point from the socket file path in the host and an internal container directory specified as a volume, where the socket file path becomes available.

That being said, the following there are some examples of possible use cases:

* Running the test modules whose name follows the pattern ``c.*`` from a mac whose IP is ``232.188.98.520``, and the password for ``root`` is ``s3cr3t``. XDevAPI protocol listening on port ``33060``::

    $ TEST_PATTERN='c.*' MYSQL_HOST='192.168.68.111' MYSQL_PASSWORD='s3cr3t' ./tests/docker/runner.sh

* Running the *mysqlx_crud* test module from Linux with MySQL user account ``root``, and password ``empty_string``. XDevAPI protocol listening on port ``33070``::

    $ MYSQLX_PORT='33070' TEST_PATTERN='mysqlx_crud' ./tests/docker/runner.sh


Test Coverage
-------------

When submitting a patch that introduces changes to the source code, you need to make sure that those changes are be accompanied by a proper set of tests that cover 100% of the affected code paths. This is easily auditable by generating proper test coverage HTML and stdout reports using the following commands:

1) Install the `coverage.py <https://github.com/nedbat/coveragepy>`_ package

   .. code-block:: bash

       shell> python -m pip install coverage

2) Use coverage run to run your test suite (assuming `mysql-connector-python`) and gather data

   .. code-block:: bash

       shell> coverage run unittests.py --with-mysql=<mysql-dir> --with-mysql-capi=<mysql-capi-dir>

3) Use ``coverage report`` to report on the results

   .. code-block:: bash

       shell> coverage report -m

4) For a nicer presentation, use ``coverage html`` to get annotated HTML listings

   .. code-block:: bash

       shell> coverage html

   The HTML will be generated in ``build/coverage_html``.

Getting Help
------------

If you need help or just want to get in touch with us, please use the following resources:

- `MySQL Connector/Python Developer Guide <https://dev.mysql.com/doc/connector-python/en/>`_
- `MySQL Connector/Python X DevAPI Reference <https://dev.mysql.com/doc/dev/connector-python/>`_
- `MySQL Connector/Python Forum <http://forums.mysql.com/list.php?50>`_
- `MySQL Public Bug Tracker <https://bugs.mysql.com>`_
- `Slack <https://mysqlcommunity.slack.com>`_ (`Sign-up <https://lefred.be/mysql-community-on-slack/>`_ required if you do not have an Oracle account)
- `Stack Overflow <https://stackoverflow.com/questions/tagged/mysql-connector-python>`_
- `InsideMySQL.com Connectors Blog <https://insidemysql.com/category/mysql-development/connectors/>`_
