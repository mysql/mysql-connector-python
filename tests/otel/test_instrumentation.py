import datetime
import json
import threading
import time
import unittest

from contextlib import nullcontext
from typing import Any, Dict, List, Tuple

import mysql.connector
import mysqlx
import tests

from mysql.connector import HAVE_CEXT, errors
from mysql.connector.opentelemetry.constants import (
    CONNECTION_SPAN_NAME,
    DB_SYSTEM,
    DEFAULT_THREAD_ID,
    DEFAULT_THREAD_NAME,
    NET_SOCK_FAMILY,
    NET_SOCK_HOST_ADDR,
    NET_SOCK_HOST_PORT,
    NET_SOCK_PEER_ADDR,
    NET_SOCK_PEER_PORT,
    OTEL_ENABLED,
)

REQPKGS = [
    "opentelemetry-api",
    "opentelemetry-sdk",
    "opentelemetry-exporter-otlp-proto-http",
]
APP_SPAN_NAME = "app"
ERROR_CODE = 2
CLIENT_KIND = 3
COLLECTOR_PORT = 4318
COLLECTOR_URL = f"http://localhost:{COLLECTOR_PORT}"
NO_DEPENDENCIES_ERR = f"Following packages are required {REQPKGS}"
NO_CEXT_ERR = "Cext is required"

try:
    import requests

    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
    from opentelemetry.semconv.trace import SpanAttributes

    from .collector import CollectorHTTPRequestHandler, OtelCollector

    DEPENDENCIES = True
except ImportError:
    DEPENDENCIES = False


if OTEL_ENABLED and DEPENDENCIES:
    from mysql.connector.opentelemetry.instrumentation import (
        MySQLInstrumentor as OracleMySQLInstrumentor,
        TracedMySQLConnection,
        TracedMySQLCursor,
    )


try:
    from opentelemetry.instrumentation.mysql import MySQLInstrumentor

    OTEL_INSTRUMENTOR = True
except ImportError:
    OTEL_INSTRUMENTOR = False


@unittest.skipIf(not DEPENDENCIES, NO_DEPENDENCIES_ERR)
def setUpModule() -> None:
    global collector
    collector = OtelCollector(
        ("", COLLECTOR_PORT),
        CollectorHTTPRequestHandler,
        mysqlx_config=tests.get_mysqlx_config(),
        log_silent=True,
    )
    collector_thread = threading.Thread(target=collector.serve_forever)
    collector_thread.daemon = True
    collector_thread.start()
    time.sleep(2)


@unittest.skipIf(not DEPENDENCIES, NO_DEPENDENCIES_ERR)
def tearDownModule() -> None:
    pass


def start_trace_tracking():
    # tell collector to start trace tracking
    return requests.post(
        COLLECTOR_URL + "/record",
        json={"session_name": "otel_test"},
        headers={"Content-Type": "application/json"},
    )


def end_trace_tracking():
    return requests.get(COLLECTOR_URL + "/record")


def dump_spans(spans: List[mysqlx.DbDoc], indent: int = 4) -> None:
    for span in spans:
        print(json.dumps(span.__dict__, indent=indent))


def parse_end_of_session_payload(
    r,
) -> Tuple[List[mysqlx.DbDoc], mysqlx.DbDoc, mysqlx.DbDoc]:
    """Returns query spans, connection span and app span."""
    spans, cnx_span, app_span = [], None, None

    query_spans_filter = "trace_id like :param1 AND name != :param2 AND name != :param3"
    cnx_or_app_span_filter = "trace_id like :param1 AND name like :param2"

    try:
        trace_id = json.loads(r.text)["traces_id"].pop()
    except IndexError as err:
        raise IndexError("no traces available in the collector") from err

    docs = (
        collector.spans_collection.find(query_spans_filter)
        .bind("param1", f"{trace_id}")
        .bind("param2", CONNECTION_SPAN_NAME)
        .bind("param3", APP_SPAN_NAME)
        .execute()
    )
    spans.extend(docs.fetch_all())

    try:
        docs = (
            collector.spans_collection.find(cnx_or_app_span_filter)
            .bind("param1", f"{trace_id}")
            .bind("param2", CONNECTION_SPAN_NAME)
            .execute()
        )
        cnx_span = docs.fetch_all().pop()
    except IndexError as err:
        raise IndexError("no connection span available") from err

    try:
        docs = (
            collector.spans_collection.find(cnx_or_app_span_filter)
            .bind("param1", f"{trace_id}")
            .bind("param2", APP_SPAN_NAME)
            .execute()
        )
        app_span = docs.fetch_all().pop()
    except IndexError as err:
        raise IndexError("no app span available") from err

    return spans, cnx_span, app_span


class CollectorTests(tests.MySQLConnectorTests):
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
    data = (2, "Jane", "Doe", datetime.date(2012, 3, 23))

    def setUp(self) -> None:
        super().setUp()
        # Service name is required for most backends
        resource = Resource(attributes={SERVICE_NAME: "collector.testing"})

        # Exporter
        self.provider = TracerProvider(resource=resource)
        processor = SimpleSpanProcessor(
            OTLPSpanExporter(endpoint=COLLECTOR_URL + "/v1/traces")
        )
        self.provider.add_span_processor(processor)

        # Instrumentor
        self.instrumentor = OracleMySQLInstrumentor()
        self.instrumentor.instrument(tracer_provider=self.provider)

    def tearDown(self) -> None:
        super().tearDown()
        self.instrumentor.uninstrument()

    def _run_client_app1(self, with_client_span: bool = True) -> None:
        tracer = trace.get_tracer(__name__, tracer_provider=self.provider)
        config = self.get_clean_mysql_config()

        with tracer.start_as_current_span("app") if with_client_span else nullcontext():
            with mysql.connector.connect(**config) as cnx:
                with cnx.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {self.table_name}")
                    cur.execute(self.create_stmt)
                    cur.execute(self.insert_stmt, self.data)
                    cur.execute(f"DROP TABLE IF EXISTS {self.table_name}")

    def _run_client_app2(self) -> None:
        tracer = trace.get_tracer(__name__, tracer_provider=self.provider)
        config = self.get_clean_mysql_config()

        with tracer.start_as_current_span("app"):
            with mysql.connector.connect(**config) as cnx:
                with cnx.cursor() as cur:
                    cur.execute(f"SELECT @@version")
                    _ = cur.fetchall()

    def test_get_status(self) -> None:
        num_its = 3
        session_name = "test_case_1"

        for _ in range(num_its):
            r = requests.get(COLLECTOR_URL + "/status")
            self.assertEqual(r.text, json.dumps({"status": "no_recording"}))

        r = requests.post(
            COLLECTOR_URL + "/record",
            json={"session_name": session_name},
            headers={"Content-Type": "application/json"},
        )

        for _ in range(num_its):
            r = requests.get(COLLECTOR_URL + "/status")
            self.assertEqual(r.text, json.dumps({"status": "recording"}))

        r = requests.get(COLLECTOR_URL + "/record")
        self.assertEqual(
            r.text, json.dumps({"session_name": session_name, "traces_id": []})
        )

        r = requests.get(COLLECTOR_URL + "/status")
        self.assertEqual(r.text, json.dumps({"status": "no_recording"}))

    def test_get_record(self) -> None:
        session_name = "test_case_2"

        r = requests.get(COLLECTOR_URL + "/status")
        self.assertEqual(r.text, json.dumps({"status": "no_recording"}))

        r = requests.post(
            COLLECTOR_URL + "/record",
            json={"session_name": session_name},
            headers={"Content-Type": "application/json"},
        )

        self._run_client_app2()

        r = requests.get(COLLECTOR_URL + "/record")
        self.assertNotEqual(
            r.text, json.dumps({"session_name": session_name, "traces_id": []})
        )
        spans, traces_ids = [], []
        for trace_id in json.loads(r.text)["traces_id"]:
            docs = (
                collector.spans_collection.find("trace_id like :param")
                .bind("param", f"{trace_id}")
                .execute()
            )
            traces_ids.append(trace_id)
            spans.extend(docs.fetch_all())

        self.assertEqual(len(spans), 3)
        self.assertEqual(
            r.text, json.dumps({"session_name": session_name, "traces_id": traces_ids})
        )

        for _ in range(3):
            r = requests.get(COLLECTOR_URL + "/record")
            self.assertEqual(r.text, json.dumps({"session_name": "", "traces_id": []}))

    def test_post_record(self) -> None:
        session_name = "test_case_3"

        for _ in range(3):
            self._run_client_app1()

        r = requests.get(COLLECTOR_URL + "/record")
        self.assertEqual(r.text, json.dumps({"session_name": "", "traces_id": []}))

        r = requests.post(
            COLLECTOR_URL + "/record",
            json={"session_name": session_name},
            headers={"Content-Type": "application/json"},
        )

        self._run_client_app1()

        r = requests.get(COLLECTOR_URL + "/record")
        self.assertNotEqual(
            r.text, json.dumps({"session_name": session_name, "traces_id": []})
        )

        spans, traces_ids = [], []
        for trace_id in json.loads(r.text)["traces_id"]:
            docs = (
                collector.spans_collection.find("trace_id like :param")
                .bind("param", f"{trace_id}")
                .execute()
            )
            traces_ids.append(trace_id)
            spans.extend(docs.fetch_all())

        self.assertEqual(len(spans), 6)
        self.assertEqual(
            r.text, json.dumps({"session_name": session_name, "traces_id": traces_ids})
        )

    def test_post_unsupported_media(self) -> None:
        session_name = "test_case_6"
        post_unsupported_media = "text/plain"

        r = requests.post(
            COLLECTOR_URL + "/record",
            json={"session_name": session_name},
            headers={"Content-Type": post_unsupported_media},
        )
        self.assertEqual(415, r.status_code)


class PythonWithGlobalInstSpanTests(tests.MySQLConnectorTests):
    """Span tests with global instrumentation and pure python connections."""

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
    local_instrumentation = False
    pure_python = True
    with_query_span_response_event = False
    with_query_span_database_attribute = False

    def setUp(self) -> None:
        super().setUp()

        # Service name is required for most backends
        resource = Resource(attributes={SERVICE_NAME: "query.span.testing"})

        # Exporter
        self.provider = TracerProvider(resource=resource)
        processor = SimpleSpanProcessor(
            OTLPSpanExporter(endpoint=COLLECTOR_URL + "/v1/traces")
        )
        self.provider.add_span_processor(processor)
        self.tracer = trace.get_tracer(__name__, tracer_provider=self.provider)
        self.cnx_config = self.get_clean_mysql_config()
        self.cnx_config["use_pure"] = self.pure_python

        # Instrumentor
        self.instrumentor = OracleMySQLInstrumentor()
        if not self.local_instrumentation:
            self.instrumentor.instrument(tracer_provider=self.provider)

    def tearDown(self) -> None:
        super().tearDown()
        if not self.local_instrumentation:
            self.instrumentor.uninstrument()

    def _otel_connect(self, **kwargs) -> Any:
        cnx = (
            self.instrumentor.instrument_connection(
                mysql.connector.connect(**kwargs),
                tracer_provider=self.provider,
            )
            if self.local_instrumentation
            else mysql.connector.connect(**kwargs)
        )
        return cnx

    def _check_query_span_attrs(self, span: mysqlx.DbDoc) -> None:
        attrs = {a["key"]: a["value"] for a in span["attributes"]}
        self.assertIn(SpanAttributes.DB_USER, attrs)
        self.assertIn(SpanAttributes.DB_SYSTEM, attrs)
        if self.with_query_span_database_attribute:
            self.assertIn(SpanAttributes.DB_NAME, attrs)
        self.assertIn(SpanAttributes.THREAD_ID, attrs)
        self.assertIn(SpanAttributes.THREAD_NAME, attrs)

        self.assertEqual(attrs[SpanAttributes.DB_SYSTEM]["string_value"], DB_SYSTEM)
        self.assertEqual(
            attrs[SpanAttributes.THREAD_NAME]["string_value"], DEFAULT_THREAD_NAME
        )
        self.assertEqual(
            attrs[SpanAttributes.THREAD_ID]["int_value"], str(DEFAULT_THREAD_ID)
        )

    def _check_query_span_events(self, span: mysqlx.DbDoc) -> None:
        event_names = [e["name"] for e in span["events"]]
        if self.with_query_span_response_event:
            self.assertEqual(
                len([e_name for e_name in event_names if e_name == "server_response"]),
                1,
            )

    def _check_query_span_links(
        self, span: mysqlx.DbDoc, connection_span_id: str
    ) -> None:
        self.assertEqual(len(span["links"]), 1)
        self.assertEqual(span["links"][0]["span_id"], connection_span_id)

    def _check_query_spans(
        self,
        query_spans: List[mysqlx.DbDoc],
        cnx_span: mysqlx.DbDoc,
        app_span: mysqlx.DbDoc,
        expected_num_query_spans: int,
    ) -> None:
        for query_span in query_spans:
            self._check_query_span_attrs(query_span)
            self._check_query_span_links(query_span, cnx_span["span_id"])
            self.assertEqual(query_span["kind"], CLIENT_KIND)
            self.assertEqual(query_span["parent_span_id"], app_span["span_id"])

        self.assertEqual(expected_num_query_spans, len(query_spans))

    def _check_connection_span_attrs(
        self, span: mysqlx.DbDoc, config: Dict[str, Any]
    ) -> None:
        is_tcp = "unix_socket" not in config
        attrs = {a["key"]: a["value"] for a in span["attributes"]}

        self.assertIn(SpanAttributes.DB_SYSTEM, attrs)
        self.assertIn(SpanAttributes.NET_TRANSPORT, attrs)
        self.assertIn(NET_SOCK_FAMILY, attrs)

        self.assertEqual(attrs[SpanAttributes.DB_SYSTEM]["string_value"], DB_SYSTEM)
        self.assertEqual(
            attrs[SpanAttributes.NET_TRANSPORT]["string_value"],
            "ip_tcp" if is_tcp else "inproc",
        )
        self.assertEqual(
            attrs[NET_SOCK_FAMILY]["string_value"], "inet" if is_tcp else "unix"
        )

        if is_tcp:
            self.assertIn(SpanAttributes.NET_PEER_NAME, attrs)
            self.assertIn(SpanAttributes.NET_PEER_PORT, attrs)
            if config.get("use_pure"):
                self.assertIn(NET_SOCK_PEER_ADDR, attrs)
                self.assertIn(NET_SOCK_HOST_ADDR, attrs)
                self.assertIn(NET_SOCK_HOST_PORT, attrs)

                if attrs.get("NET_SOCK_PEER_PORT"):
                    self.assertNotEqual(
                        attrs[NET_SOCK_PEER_PORT]["string_value"],
                        attrs[SpanAttributes.NET_PEER_PORT]["string_value"],
                    )
        else:
            self.assertNotIn(SpanAttributes.NET_PEER_PORT, attrs)
            self.assertNotIn(NET_SOCK_HOST_PORT, attrs)

            self.assertIn(NET_SOCK_PEER_ADDR, attrs)
            if config.get("use_pure"):
                self.assertIn(NET_SOCK_HOST_ADDR, attrs)

    def _check_connection_span(
        self, cnx_span: mysqlx.DbDoc, app_span: mysqlx.DbDoc
    ) -> None:
        self.assertEqual(cnx_span["kind"], CLIENT_KIND)
        self.assertEqual(cnx_span["parent_span_id"], app_span["span_id"])

        self._check_connection_span_attrs(cnx_span, self.get_clean_mysql_config())

    def _run_client_app1(
        self,
        use_pure: bool = True,
        with_client_span: bool = True,
        **cur_kwargs: Any,
    ) -> Tuple[List, int]:
        """Dummy client app.

        Workflow is as follows:
        -> Creates a new table
        -> Inserts data into it
        -> Selects its content
        -> Drops the table
        -> Creates new user
        -> Creates new database
        -> Does a `cmd change user` operation
        -> Selects server version
        -> Drops the database created

        With this test we can:
        - check if the user attribute changes accordingly
        - check if the database attribute changes accordingly
        - check query span morphology
        - check query span attributes
        - check query span events
        - check that the CHILD/ROOT span particulars are met based if there is a client
        app span or not.
        """
        num_query_spans_client_app = 12
        with self.tracer.start_as_current_span(
            "app"
        ) if with_client_span else nullcontext():
            cnx = self._otel_connect(**self.cnx_config)

            with cnx.cursor(**cur_kwargs) as cur:
                # do some dummy ops
                cur.execute(f"DROP TABLE IF EXISTS {self.table_name}")
                cur.execute(self.create_stmt)
                cur.executemany(self.insert_stmt, self.data)
                cur.execute(f"SELECT * from {self.table_name}")
                res = cur.fetchall()
                cur.execute(f"DROP TABLE IF EXISTS {self.table_name}")

                # create a new user
                cur.execute(f"DROP USER IF EXISTS '{self.new_user_name}'")
                cur.execute(self.new_user_stmt)
                cur.execute(self.grant_stmt)
                cur.execute("FLUSH PRIVILEGES")
                cur.execute(f"CREATE DATABASE IF NOT EXISTS {self.new_database}")

            cnx.cmd_change_user(
                username=self.new_user_name,
                password=self.new_user_password,
                database=self.new_database,
            )
            with cnx.cursor() as cur:
                cur.execute("SELECT @@version")
                _ = cur.fetchall()
                cur.execute(f"DROP DATABASE IF EXISTS {self.new_database}")

            cnx.close()

        return res, num_query_spans_client_app

    def _run_client_app2(
        self,
        use_pure: bool = True,
        with_client_span: bool = True,
        **cur_kwargs: Any,
    ) -> Tuple[List, int]:
        """Dummy client app."""
        num_query_spans_client_app = 8
        table_name = "my_table"
        proc_name = "my_procedure"

        with self.tracer.start_as_current_span(
            "app"
        ) if with_client_span else nullcontext():
            cnx = self._otel_connect(**self.cnx_config)

            with cnx.cursor(**cur_kwargs) as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                cur.execute(f"DROP PROCEDURE IF EXISTS {proc_name}")
                cur.execute(
                    f"CREATE TABLE {table_name} (c1 VARCHAR(20), c2 INT PRIMARY KEY)"
                )
                cur.execute(
                    f"CREATE PROCEDURE {proc_name} (p1 VARCHAR(20), p2 INT) "
                    f"BEGIN INSERT INTO {table_name} (c1, c2) "
                    "VALUES (p1, p2); END;"
                )
                cur.callproc(f"{proc_name}", ("ham", 42))
                cur.execute(f"SELECT c1, c2 FROM {table_name}")
                res = cur.fetchall()
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                cur.execute(f"DROP PROCEDURE IF EXISTS {proc_name}")

            cnx.close()
        return res, num_query_spans_client_app

    def test_plain_cursor(
        self,
        expected: bool = None,
        buffered: bool = False,
        raw: bool = False,
        dictionary: bool = False,
        named_tuple: bool = False,
        prepared: bool = False,
    ) -> None:
        exps = expected or [self.data, [("ham", 42)]]
        apps = [self._run_client_app1, self._run_client_app2]
        if prepared:
            # callproc not supported in the prepared statement protocol yet
            apps.pop()
        for app, exp in zip(apps, exps):
            start_trace_tracking()

            # run client application
            res, expected_num_query_spans = app(
                use_pure=self.pure_python,
                with_client_span=True,
                buffered=buffered,
                raw=raw,
                dictionary=dictionary,
                named_tuple=named_tuple,
                prepared=prepared,
            )
            self.assertListEqual(res, exp)

            # tell collector to end trace tracking, and parse reply
            query_spans, cnx_span, app_span = parse_end_of_session_payload(
                r=end_trace_tracking()
            )

            self.assertNotEqual(cnx_span, None)
            self.assertNotEqual(app_span, None)

            self._check_query_spans(
                query_spans, cnx_span, app_span, expected_num_query_spans
            )
            self._check_connection_span(cnx_span, app_span)

    def test_buffered_cursor(self) -> None:
        self.test_plain_cursor(buffered=True)

    def test_raw_cursor(self) -> None:
        exp = [
            [
                (
                    bytearray(b"2"),
                    bytearray(b"Jane"),
                    bytearray(b"Doe"),
                    bytearray(b"2012-03-23 00:00:00"),
                ),
                (
                    bytearray(b"7"),
                    bytearray(b"John"),
                    bytearray(b"Williams"),
                    bytearray(b"2006-05-11 00:00:00"),
                ),
                (
                    bytearray(b"11"),
                    bytearray(b"Joe"),
                    bytearray(b"Lopez"),
                    bytearray(b"2014-10-04 00:00:00"),
                ),
            ],
            [(bytearray(b"ham"), b"42")],
        ]
        self.test_plain_cursor(expected=exp, raw=True)

    def test_dictionary_cursor(self) -> None:
        exp = [
            [
                dict(zip(["emp_no", "first_name", "last_name", "hire_date"], row))
                for row in self.data
            ],
            [dict(zip(["c1", "c2"], ("ham", 42)))],
        ]
        self.test_plain_cursor(expected=exp, dictionary=True)

    def test_named_tuple_cursor(self) -> None:
        self.test_plain_cursor(named_tuple=True)

    def test_prepared_cursor(self) -> None:
        self.test_plain_cursor(prepared=True)

    def _check_error_event(self, span: mysqlx.DbDoc, ex: Exception) -> None:
        """Check an error event."""
        events = {
            e["name"]: {a["key"]: a["value"] for a in e["attributes"]}
            for e in span["events"]
        }
        self.assertIn("exception", events)
        self.assertEqual(span["status"]["code"], ERROR_CODE)
        self.assertEqual(
            events["exception"]["exception.message"]["string_value"], str(ex)
        )
        self.assertEqual(
            events["exception"]["exception.type"]["string_value"], ex.__class__.__name__
        )

    def test_connection_error(self) -> None:
        """Check connection span has `status=ERROR` when
        something goes wrong at connection time."""
        if self.local_instrumentation:
            # do no apply for local instrumentation
            return

        cnx_config = self.cnx_config.copy()
        cnx_config["host"] = "bad.host"

        def case1():
            """Expecting an InterfaceError."""
            with self.tracer.start_as_current_span("app"):
                with self._otel_connect(**cnx_config) as cnx:
                    pass

        def case2():
            """Expecting an InterfaceError."""
            with self.tracer.start_as_current_span("app"):
                cnx = self._otel_connect(**cnx_config)
                cnx.close()

        for case in [case1, case2]:
            start_trace_tracking()
            self.assertRaises(
                errors.InterfaceError if self.pure_python else errors.DatabaseError,
                case,
            )
            query_spans, cnx_span, app_span = parse_end_of_session_payload(
                r=end_trace_tracking()
            )
            try:
                case()
            except Exception as err:
                self._check_error_event(cnx_span, err)

    def test_query_error(self):
        """Check query span has `status=ERROR` when
        something goes wrong at query time."""

        def case1():
            """Expecting a ProgrammingError."""
            with self.tracer.start_as_current_span("app"):
                with self._otel_connect(**self.cnx_config) as cnx:
                    with cnx.cursor() as cur:
                        cur.execute("BAD_CMD? @@version")
                        _ = cur.fetchall()

        def case2():
            """Expecting a ProgrammingError."""
            with self.tracer.start_as_current_span("app"):
                cnx = self._otel_connect(**self.cnx_config)
                try:
                    with cnx.cursor() as cur:
                        cur.execute("BAD_CMD? @@version")
                        _ = cur.fetchall()
                except Exception:
                    cnx.close()
                    raise

        for case in [case1, case2]:
            start_trace_tracking()
            self.assertRaises(errors.ProgrammingError, case)
            query_spans, cnx_span, app_span = parse_end_of_session_payload(
                r=end_trace_tracking()
            )
            try:
                case()
            except Exception as err:
                for query_span in query_spans:
                    self._check_error_event(query_span, err)

    def test_cursor_error(self) -> None:
        """Check connection span has `status=ERROR` when
        something goes wrong at cursor creation time."""

        def case1():
            """Expecting a ProgrammingError."""
            with self.tracer.start_as_current_span("app"):
                with self._otel_connect(**self.cnx_config) as cnx:
                    with cnx.cursor(cursor_class=int) as cur:
                        pass

        def case2():
            """Expecting a ProgrammingError."""
            with self.tracer.start_as_current_span("app"):
                cnx = self._otel_connect(**self.cnx_config)
                try:
                    with cnx.cursor(cursor_class=int) as cur:
                        pass
                except Exception:
                    cnx.close()
                    raise

        for case in [case1, case2]:
            start_trace_tracking()
            self.assertRaises(errors.ProgrammingError, case)
            query_spans, cnx_span, app_span = parse_end_of_session_payload(
                r=end_trace_tracking()
            )
            try:
                case()
            except Exception as err:
                self._check_error_event(cnx_span, err)

    def test_reconnect(self) -> None:
        """Check a connection is still instrumented
        after closing it and reconnecting."""
        num_query_spans_client_app = 1
        with self.tracer.start_as_current_span("app"):
            cnx = self._otel_connect(**self.cnx_config)
            with cnx.cursor() as cur:
                cur.execute("SELECT @@version")
                _ = cur.fetchall()
            cnx.close()

        start_trace_tracking()

        with self.tracer.start_as_current_span("app"):
            cnx.reconnect()
            with cnx.cursor() as cur:
                cur.execute("SELECT @@version")
                _ = cur.fetchall()
            cnx.close()

        query_spans, cnx_span, app_span = parse_end_of_session_payload(
            r=end_trace_tracking()
        )

        self.assertNotEqual(cnx_span, None)
        self.assertNotEqual(app_span, None)
        self.assertGreater(len(query_spans), 0)
        self._check_query_spans(
            query_spans, cnx_span, app_span, num_query_spans_client_app
        )
        self._check_connection_span(cnx_span, app_span)

    def test_reconnect_error(self) -> None:
        """Check a connection records an event error when `reconnect()` fails."""

        def do_warmup():
            with self.tracer.start_as_current_span("app"):
                cnx = self._otel_connect(**self.cnx_config)
                with cnx.cursor() as cur:
                    cur.execute("SELECT @@version")
                    _ = cur.fetchall()
                cnx.close()
            return cnx

        def do_reconnect(cnx):
            """Expecting an InterfaceError."""
            # intentionally set a bad user
            self.assertEqual(cnx._user, cnx._wrapped._user)
            cnx._user = "baduser"
            self.assertEqual(cnx._user, cnx._wrapped._user)
            with self.tracer.start_as_current_span("app"):
                try:
                    cnx.reconnect()
                except:
                    cnx.close()
                    raise

        cnx = do_warmup()
        start_trace_tracking()
        self.assertRaises(errors.InterfaceError, do_reconnect, cnx)
        query_spans, cnx_span, app_span = parse_end_of_session_payload(
            r=end_trace_tracking()
        )
        try:
            do_reconnect(do_warmup())
        except Exception as err:
            self._check_error_event(cnx_span, err)


class PythonWithLocalInstSpanTests(PythonWithGlobalInstSpanTests):
    """Span tests with local instrumentation and pure python connections."""

    local_instrumentation = True

    def setUp(self) -> None:
        super().setUp()

        # turn off global instrumentation
        self.instrumentor.uninstrument()

    def tearDown(self) -> None:
        # skip turning off global instrumentation,
        # it has already been deactivated.
        pass


@unittest.skipIf(HAVE_CEXT == False, reason=NO_CEXT_ERR)
class CextWithGlobalInstSpanTests(PythonWithGlobalInstSpanTests):
    """Span tests with global instrumentation and cext connections."""

    pure_python = False


@unittest.skipIf(HAVE_CEXT == False, reason=NO_CEXT_ERR)
class CextWithLocalInstSpanTests(PythonWithLocalInstSpanTests):
    """Span tests with local instrumentation and cext connections."""

    pure_python = False


class MySQLInstrumentorTests(tests.MySQLConnectorTests):
    """Test instrumentation and uninstrumentation."""

    pure_python = True

    def setUp(self) -> None:
        super().setUp()

        # Service name is required for most backends
        resource = Resource(attributes={SERVICE_NAME: "query.span.testing"})

        # Exporter
        self.provider = TracerProvider(resource=resource)
        processor = SimpleSpanProcessor(
            OTLPSpanExporter(endpoint=COLLECTOR_URL + "/v1/traces")
        )
        self.provider.add_span_processor(processor)

    def tearDown(self) -> None:
        super().tearDown()

    def test_global_uninstrumentation(self) -> None:
        instrumentor = OracleMySQLInstrumentor()
        instrumentor.instrument(tracer_provider=self.provider)

        config = self.get_clean_mysql_config()
        config["use_pure"] = self.pure_python
        with mysql.connector.connect(**config) as cnx:
            self.assertIsInstance(cnx, TracedMySQLConnection)
            with cnx.cursor() as cur:
                self.assertIsInstance(cur, TracedMySQLCursor)

        instrumentor.uninstrument()

        config["use_pure"] = self.pure_python
        with mysql.connector.connect(**config) as cnx:
            self.assertNotIsInstance(cnx, TracedMySQLConnection)
            with cnx.cursor() as cur:
                self.assertNotIsInstance(cur, TracedMySQLCursor)

    def test_local_uninstrumentation(self) -> None:
        instrumentor = OracleMySQLInstrumentor()
        config = self.get_clean_mysql_config()

        config["use_pure"] = self.pure_python

        cnx = instrumentor.instrument_connection(
            mysql.connector.connect(**config), tracer_provider=self.provider
        )
        self.assertIsInstance(cnx, TracedMySQLConnection)

        with cnx.cursor() as cur:
            self.assertIsInstance(cur, TracedMySQLCursor)

        cnx = instrumentor.uninstrument_connection(cnx)
        self.assertNotIsInstance(cnx, TracedMySQLConnection)

        with cnx.cursor() as cur:
            self.assertNotIsInstance(cur, TracedMySQLCursor)

        cnx.close()


class PythonPerformanceTests(tests.MySQLConnectorTests):
    """Compare the connector's performance when otel tracing is on/off."""

    table_name = "leaderboard"

    create_table_stmt = f"""CREATE TABLE {table_name} (
        competitor_id int,
        first_name varchar(255),
        last_name varchar(255),
        event_1 int,
        event_2 int,
        event_3 int
    )
    """

    insert_stmt = (
        f"INSERT INTO {table_name} (competitor_id, first_name, last_name, event_1, event_2, event_3)"
        "VALUES (%s, %s, %s, %s, %s, %s)"
    )
    rows = [
        (32, "Philip", "Doe", 1, 2, 1),
        (20, "Charly", "Godwell", 2, 3, 3),
        (37, "Federick", "McConell", 3, 1, 4),
        (58, "Jhon", "Conor", 4, 4, 2),
    ]
    pure_python = True

    def setUp(self) -> None:
        # Service name is required for most backends
        resource = Resource(attributes={SERVICE_NAME: "query.span.testing"})

        # Exporter
        self.provider = TracerProvider(resource=resource)
        processor = SimpleSpanProcessor(
            OTLPSpanExporter(endpoint=COLLECTOR_URL + "/v1/traces")
        )
        self.provider.add_span_processor(processor)
        self.tracer = trace.get_tracer(__name__, tracer_provider=self.provider)
        self.cnx_config = self.get_clean_mysql_config()
        self.cnx_config["use_pure"] = self.pure_python

    def tearDown(self) -> None:
        super().tearDown()

    def run_bench(self, check_instrumented: bool = False, iters: int = 100) -> float:
        runn_time = time.perf_counter()  # secs
        with self.tracer.start_as_current_span("app"):
            with mysql.connector.connect(**self.cnx_config) as cnx:
                with cnx.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {self.table_name}")
                    cur.execute(self.create_table_stmt)
                    for _ in range(iters):
                        for i in range(len(self.rows)):
                            cur.execute(self.insert_stmt, self.rows[i])
                        cur.execute("SELECT @@version")
                        _ = cur.fetchall()
                    cur.execute(f"DROP TABLE IF EXISTS {self.table_name}")
                if check_instrumented:
                    self.assertTrue(not cnx._span is None)

        return time.perf_counter() - runn_time

    def test_benchmark1(self):
        """Compare performance: plain vs instrumented connection.

        Run about iters*5 execute() ops.
        """
        tolerance = 2  # secs
        instrumentor = OracleMySQLInstrumentor()

        instrumentor.instrument(tracer_provider=None)
        time_instrumented = self.run_bench(check_instrumented=True, iters=5000)
        instrumentor.uninstrument()

        time_plain = self.run_bench(iters=5000)

        diff = abs(time_plain - time_instrumented)
        self.assertTrue(
            diff <= tolerance,
            msg=f"Performance test failed! Got a diff={diff}",
        )

    def test_benchmark2(self):
        """Compare performance: plain vs instrumented connection.

        Run about iters*5 connect() ops.
        """

        def run(check_instrumented: bool = False, iters: int = 100) -> float:
            runn_time = time.perf_counter()  # secs
            for _ in range(iters):
                with self.tracer.start_as_current_span("app"):
                    with mysql.connector.connect(**self.cnx_config) as cnx:
                        if check_instrumented:
                            self.assertTrue(not cnx._span is None)

            return time.perf_counter() - runn_time

        tolerance = 2  # secs
        instrumentor = OracleMySQLInstrumentor()

        instrumentor.instrument(tracer_provider=None)
        time_instrumented = run(check_instrumented=True, iters=100)
        instrumentor.uninstrument()

        time_plain = run(iters=100)

        diff = abs(time_plain - time_instrumented)
        self.assertTrue(
            diff <= tolerance,
            msg=f"Performance test failed! Got a diff={diff}",
        )

    @unittest.skipIf(
        not OTEL_INSTRUMENTOR,
        "Opentelemetry MySQLInstrumentor required (opentelemetry-instrumentation-mysql)",
    )
    def test_benchmark3(self):
        """Compare performance: otel instrumented vs oracle instrumented connection.

        Run about iters*5 execute() ops.
        """
        tolerance = 2  # secs
        times = []
        instrumentors = [OracleMySQLInstrumentor(), MySQLInstrumentor()]

        for check_instrumented, instrumentor in zip([True, False], instrumentors):
            if check_instrumented:
                instrumentor.instrument(tracer_provider=self.provider)
            else:
                instrumentor.instrument(tracer_provider=self.provider)
            t = self.run_bench(check_instrumented=check_instrumented, iters=100)
            instrumentor.uninstrument()
            times.append(t)

        time_oracle_inst, time_otel_inst = times[0], times[1]

        diff = abs(time_otel_inst - time_oracle_inst)
        self.assertTrue(
            diff <= tolerance,
            msg=f"Performance test failed! Got a diff={diff}",
        )


@unittest.skipIf(HAVE_CEXT == False, reason=NO_CEXT_ERR)
class CextPerformanceTests(PythonPerformanceTests):
    """Compare the connector's performance when otel tracing is on/off."""

    pure_python = False
