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

- Make sure you have a user account at `bugs.mysql.com <https://bugs.mysql.com>`_. You will need to reference this user account when you submit your Oracle Contributor Agreement (OCA).
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

Running Tests
-------------

Any code you contribute needs to pass our test suite. Please follow these steps to run our tests and validate your contributed code.

Run the entire test suite:

   .. code-block:: bash

       shell> python unittests.py --with-mysql=<mysql-dir> --with-mysql-capi=<mysql-capi-dir> --with-protobuf-include-dir=<protobuf-include-dir> --with-protobuf-lib-dir=<protobuf-lib-dir> --with-protoc=<protoc-binary>

Example:

   .. code-block:: sh

       shell> python unittests.py --with-mysql=/usr/local/mysql --with-mysql-capi=/usr/local/mysql --with-protobuf-include-dir=/usr/local/protobuf/include --with-protobuf-lib-dir=/usr/local/protobuf/lib --with-protoc=/usr/local/protobuf/bin/protoc

Test Coverage
-------------

When submitting a patch that introduces changes to the source code, you need to make sure that those changes are be accompanied by a proper set of tests that cover 100% of the affected code paths. This is easily auditable by generating proper test coverage HTML and stdout reports using the following commands:

1) Install the `coverage.py <https://github.com/nedbat/coveragepy>`_ package

   .. code-block:: bash

       shell> python -m pip install coverage

2) Use coverage run to run your test suite and gather data

   .. code-block:: bash

       shell> coverage run unittests.py --with-mysql=<mysql-dir> --with-mysql-capi=<mysql-capi-dir> --with-protobuf-include-dir=<protobuf-include-dir> --with-protobuf-lib-dir=<protobuf-lib-dir> --with-protoc=<protoc-binary>

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

