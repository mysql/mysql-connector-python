"""This collector agent is exclusive for testing purposes."""

import base64
import json
import threading

from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict

# requirements: opentelemetry-proto and mysql-connector-python
from google.protobuf.json_format import MessageToDict

try:
    # try to load otel from the system
    from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
        ExportTraceServiceRequest,
    )
except ImportError:
    # falling back to the bundled installation
    from mysql.opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
        ExportTraceServiceRequest,
    )

import mysqlx


def parse_trace_id(obj: Dict) -> None:
    attrs = ["trace_id", "span_id", "parent_span_id"]
    for attr in attrs:
        if attr in obj:
            b64_decoded_value = base64.b64decode(obj[attr].encode("utf-8"))
            obj[attr] = hex(int.from_bytes(b64_decoded_value, byteorder="big"))[2:]
    return


class CollectorHTTPRequestHandler(BaseHTTPRequestHandler):
    """Custom HTTPRequestHandler class to accommodate collector's needs."""

    def _insert_spans_into_mysql(
        self, msg: Dict[str, Any], need_id_parsing: bool
    ) -> None:
        for resource_span in msg["resource_spans"]:
            for scope_span in resource_span["scope_spans"]:
                for span in scope_span["spans"]:
                    if need_id_parsing:
                        # Parse trace_id, span_id and parent_span_id
                        parse_trace_id(span)
                        for link in span.get("links", []):
                            parse_trace_id(link)
                    self.server.refresh_collections(span)

    def do_POST(self) -> None:
        # Get the content length
        content_length = int(self.headers["Content-Length"])
        if not self.server.log_silent:
            print(f"got request with {content_length} bytes")
            print(f"type: {self.headers['Content-Type']}")

        # Read the request body
        request_body = self.rfile.read(content_length)

        # Define incoming message based on "Content-Type"
        msg: Dict = {}
        if self.headers["Content-Type"] == "application/json":
            msg = json.loads(request_body.decode("utf-8"))
        elif self.headers["Content-Type"] == "application/x-protobuf":
            msg_proto = ExportTraceServiceRequest()
            msg_proto.ParseFromString(request_body)
            msg = MessageToDict(
                msg_proto,
                preserving_proto_field_name=True,
                use_integers_for_enums=True,
            )
        else:
            # reply with bad response
            self.send_response(415)  # client error
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Unsupported Media Type")

        # Define endpoint workflow
        if self.path == "/v1/traces" and self.server.recording:
            self._insert_spans_into_mysql(
                msg=msg,
                need_id_parsing=self.headers["Content-Type"] != "application/json",
            )
        elif self.path == "/record" and not self.server.recording:
            self.server.new_tracker(new_session_name=msg.get("session_name", "unknown"))
            self.server.recording = True

        # Send the response
        self.send_response(200)  # success response
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

    def do_GET(self) -> None:
        if self.path == "/record":
            payload = self.server.tracker.copy()
            if self.server.recording:
                self.server.recording = False
                self.server.new_tracker()
        elif self.path == "/status":
            payload = {
                "status": "no_recording" if not self.server.recording else "recording"
            }
        elif self.path == "/stop":
            # Note: from https://stackoverflow.com/questions/38196446/
            # how-to-stop-a-simplehttpserver-in-python-from-httprequest-handler
            payload = self.server.tracker.copy()
            if not self.server.log_silent:
                print("Close")
            self.close_connection = True
            threading.Thread(target=self.server.shutdown, daemon=True).start()

        # Send the response
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode())

    def log_message(self, format: str, *args: Any) -> None:
        if not self.server.log_silent:
            return super().log_message(format, *args)
        return


class OtelCollector(HTTPServer):
    """The collector is implemented as an HTTP-based server."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.log_silent: bool = kwargs.pop("log_silent", True)
        self.recording: bool = False
        self.my_session: mysqlx.Session = mysqlx.get_session(
            **kwargs.pop("mysqlx_config")
        )
        self.my_db: mysqlx.Schema = self.get_schema(schema_name="otel")
        self.spans_collection: mysqlx.Collection = self.get_collection(
            collection_name="spans"
        )
        self.tracker: Dict[str, Any] = {"session_name": "", "traces_id": []}
        super().__init__(*args, **kwargs)

    def get_collection(self, collection_name: str) -> mysqlx.Collection:
        try:
            my_coll = self.my_db.create_collection(collection_name)
        except:
            self.my_db.drop_collection(collection_name)
            my_coll = self.my_db.create_collection(collection_name)
        return my_coll

    def get_schema(self, schema_name: str = "otel") -> mysqlx.Schema:
        my_db = self.my_session.get_schema(schema_name)
        if not my_db.exists_in_database():
            my_db = self.my_session.create_schema(schema_name)
        return my_db

    def refresh_collections(self, span: Dict[str, Any]) -> None:
        trace_id = span.get("trace_id", "0" * 32)
        if trace_id not in self.tracker["traces_id"]:
            self.tracker["traces_id"].append(trace_id)
        self.spans_collection.add(span).execute()

    def new_tracker(self, new_session_name: str = "") -> None:
        self.tracker = {
            "session_name": new_session_name,
            "traces_id": list(),
        }

    def serve_forever(self, *args: Any, **kwargs: Any) -> None:
        try:
            super().serve_forever(*args, **kwargs)
        except:
            self.shutdown()
        return

    def shutdown(self) -> None:
        self.my_db.drop_collection("traces")
        self.my_db.drop_collection("spans")
        self.my_session.close()
        return super().shutdown()
