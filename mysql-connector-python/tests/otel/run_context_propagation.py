"""Context propagation script.

Trace context propagation is carried on via query attributes.
MySQL client/server protocol commands supporting query attributes
are `cmd_query` and `cmd_stmt_execute`.

In order to execute this script you need:

1. An OTEL Collector listening to port `COLLECTOR_PORT` at `COLLECTOR_URL`.
2. A MySQL Server with OTEL configured and enabled.
3. mysql-connector-python >= 8.1.0
4. Python packages opentelemetry-api, opentelemetry-sdk and \
    opentelemetry-exporter-otlp-proto-http

This isn't an automated test, it is meant to be executed manually. To confirm
success you must verify trace continuity (linkage between connector and server
spans) by looking at the Collector's UI.

Default configuration is assumed for MySQL Server and OTEL Collector.

```
MySQL Server
------------
port: 3306
host: localhost
user: root
password: ""  # empty string
use_pure: True

OTEL Collector
--------------
port: 4318
url (host): localhost

Context Propagation
-------------------
test_cmd_query: True
test_cmd_stmt_execute: True
```

However you can override any of these defaults according to the following flags:
--mysql-port: int
--mysql-host: str
--mysql-user: str
--mysql-password: str
--cpy-use-pure: bool
--collector-port: int
--collector-host: str
--test-cmd_query: bool
--test-cmd-stmt-execute: bool
--debug: bool

Example: default config for mysql server and collector, using the c-ext
implementation of connector-python and just running context propagation
for prepared statements:

`$ python run_context_propagation --cpy-use-pure=False --test-cmd-query=False`
"""

import datetime
import io

from argparse import ArgumentParser, Namespace
from contextlib import nullcontext

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

import mysql.connector

from mysql.connector.opentelemetry.instrumentation import MySQLInstrumentor
from mysql.connector.version import VERSION_TEXT

instrumentor = MySQLInstrumentor()
BOOL_ARGS = {"cpy_use_pure", "test_cmd_query", "test_cmd_stmt_execute"}


# Helper to setup command-line argument parser needed to run the tests
def setup_cmd_parser() -> Namespace:
    parser = ArgumentParser(
        description="Script to manually test OTEL context propagation for connector-python."
    )
    parser.add_argument(
        "--mysql-user",
        nargs="?",
        default="root",
        help="client username needed for server connection",
    )
    parser.add_argument(
        "--mysql-password",
        nargs="?",
        default="",
        help="client password needed for server connection",
    )
    parser.add_argument(
        "--mysql-port",
        nargs="?",
        default=3306,
        help="port the server is listening to",
        type=int,
    )
    parser.add_argument(
        "--mysql-host",
        nargs="?",
        default="127.0.0.1",
        help="where the server is hosted at",
    )
    parser.add_argument(
        "--cpy-use-pure",
        nargs="?",
        default=True,
        help="use pure python or c-ext implementation",
    )
    parser.add_argument(
        "--collector-port",
        nargs="?",
        default=4318,
        help="port the collector is listening to",
        type=int,
    )
    parser.add_argument(
        "--collector-host",
        nargs="?",
        default="127.0.0.1",
        help="where the collector is hosted at",
    )
    parser.add_argument(
        "--test-cmd-query",
        nargs="?",
        default=True,
        help="run context propagation when working with simple statements",
    )
    parser.add_argument(
        "--test-cmd-stmt-execute",
        nargs="?",
        default=True,
        help="run context propagation when working with prepared statements",
    )

    return parser.parse_args()


class TestContextPropagation:
    def __init__(self, **config) -> None:
        self._config = config.copy()
        self._tracer = None
        self._mysql_config = {
            "username": config["mysql_user"],
            "password": config["mysql_password"],
            "port": config["mysql_port"],
            "use_pure": config["cpy_use_pure"],
        }

        self._init_otel()

    def _init_otel(self):
        collector_url = (
            f"http://{self._config['collector_host']}:{self._config['collector_port']}"
        )

        resource = Resource(attributes={SERVICE_NAME: "connector-python"})
        provider = TracerProvider(resource=resource)
        processor = BatchSpanProcessor(
            OTLPSpanExporter(endpoint=collector_url + "/v1/traces")
        )
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)
        self._tracer = trace.get_tracer(__name__)

    def test_simple_stmts(
        self,
        app_span_name="cmd_query",
        with_app_span=True,
        with_otel_context_propagation=True,
    ):
        """Test context propagation via query attributes for `cmd_query`."""
        app_span_name = (
            ("python" if self._config["cpy_use_pure"] else "cext") + "_" + app_span_name
        )

        new_user_name = "ramon"
        new_user_password = "s3cr3t"
        new_database = "colors"
        new_user_stmt = (
            f"CREATE USER '{new_user_name}'@'%' IDENTIFIED BY '{new_user_password}'"
        )
        grant_stmt = (
            f"GRANT ALL PRIVILEGES ON *.* TO '{new_user_name}'@'%' WITH GRANT OPTION"
        )
        table_name = "employees"
        create_stmt = f"""CREATE TABLE {table_name} (
            emp_no int,
            first_name varchar(255),
            last_name varchar(255),
            hire_date DATETIME
        )
        """
        insert_stmt = (
            f"INSERT INTO {table_name} (emp_no, first_name, last_name, hire_date)"
            "VALUES (%s, %s, %s, %s)"
        )
        data = [
            (2, "Jane", "Doe", datetime.datetime(2012, 3, 23)),
            (7, "John", "Williams", datetime.datetime(2006, 5, 11)),
            (11, "Joe", "Lopez", datetime.datetime(2014, 10, 4)),
        ]
        inti_database_name = "test"

        instrumentor.instrument()
        with self._tracer.start_as_current_span(
            app_span_name
        ) if with_app_span else nullcontext():
            with mysql.connector.connect(**self._mysql_config) as cnx:
                cnx.otel_context_propagation = with_otel_context_propagation

                with cnx.cursor() as cur:
                    cur.execute(f"CREATE DATABASE IF NOT EXISTS {inti_database_name}")
                    cur.execute(f"USE {inti_database_name}")

                    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                    cur.execute(create_stmt)
                    cur.executemany(insert_stmt, data)
                    cur.execute(f"SELECT * from {table_name}")
                    res = cur.fetchall()
                    cur.execute(f"DROP TABLE IF EXISTS {table_name}")

                    # create a new user
                    cur.execute(f"DROP USER IF EXISTS '{new_user_name}'")
                    cur.execute(new_user_stmt)
                    cur.execute(grant_stmt)
                    cur.execute("FLUSH PRIVILEGES")
                    cur.execute(f"CREATE DATABASE IF NOT EXISTS {new_database}")

                cnx.cmd_change_user(
                    username=new_user_name,
                    password=new_user_password,
                    database=new_database,
                )
                with cnx.cursor() as cur:
                    cur.execute(f"DROP DATABASE IF EXISTS {new_database}")
                    cur.execute(f"DROP DATABASE IF EXISTS {inti_database_name}")

                with cnx.cursor(prepared=False) as cur:
                    cur.execute("SELECT @@version")
                    _ = cur.fetchall()

                with cnx.cursor(dictionary=True) as cur:
                    cur.execute("SELECT 1")
                    _ = cur.fetchall()

                with cnx.cursor(raw=True) as cur:
                    cur.execute("SELECT 2")
                    _ = cur.fetchall()

        instrumentor.uninstrument()

    def test_prepared_stmt(
        self,
        app_span_name="cmd_stmt_execute",
        with_app_span=True,
        with_otel_context_propagation=True,
    ):
        app_span_name = (
            ("python" if self._config["cpy_use_pure"] else "cext") + "_" + app_span_name
        )

        instrumentor.instrument()
        stmt = "SELECT  %s, %s, %s"
        with self._tracer.start_as_current_span(
            app_span_name
        ) if with_app_span else nullcontext:
            with mysql.connector.connect(**self._mysql_config) as cnx:
                cnx.otel_context_propagation = with_otel_context_propagation

                with cnx.cursor(prepared=True) as cur:
                    cur.execute(stmt, (5, "a", 8))
                    res = cur.fetchall()

                    cur.execute(stmt, (10, 0, 1))
                    res = cur.fetchall()

                    cur.execute("SELECT %s", ("Hello QA",))
                    res = cur.fetchall()

        instrumentor.uninstrument()


if __name__ == "__main__":
    config = vars(setup_cmd_parser())

    print("Script configuration")
    print("--------------------")
    for key in config.keys():
        if key in BOOL_ARGS and isinstance(config[key], str):
            if config[key].lower() == "false":
                config[key] = False
            elif config[key].lower() == "true":
                config[key] = True
            else:
                raise ValueError(f"{key} must be a bool")
        print(f"\t{key}: {config[key]}")
    print()

    runner = TestContextPropagation(**config)

    print("Context propagation tests")
    print("-------------------------")

    if config["test_cmd_query"]:
        print("\tRunning for simple statements...")
        runner.test_simple_stmts()

    if config["test_cmd_stmt_execute"]:
        print("\tRunning for prepared statements...")
        if tuple([int(x) for x in VERSION_TEXT.split(".")]) < (8, 3, 0):
            print(
                f"\tWARNING: you're using version CPY {VERSION_TEXT}. "
                "Context propagation for prepared statements "
                "supported as per CPY >= 8.3.0."
            )
        runner.test_prepared_stmt()
