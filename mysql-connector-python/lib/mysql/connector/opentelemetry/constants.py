"""Constants used by the opentelemetry instrumentation implementation."""
# mypy: disable-error-code="no-redef,assignment"

# pylint: disable=unused-import
OTEL_ENABLED = True
try:
    # try to load otel from the system
    from opentelemetry import trace  # check api
    from opentelemetry.sdk.trace import TracerProvider  # check sdk
    from opentelemetry.semconv.trace import SpanAttributes  # check semconv
except ImportError:
    # falling back to the bundled installation
    try:
        from mysql.opentelemetry import trace
        from mysql.opentelemetry.sdk.trace import TracerProvider
        from mysql.opentelemetry.semconv.trace import SpanAttributes
    except ImportError:
        # bundled installation has missing dependencies
        OTEL_ENABLED = False


OPTION_CNX_SPAN = "_span"
"""
Connection option name used to inject the connection span.
This connection option name must not be used, is reserved.
"""

OPTION_CNX_TRACER = "_tracer"
"""
Connection option name used to inject the opentelemetry tracer.
This connection option name must not be used, is reserved.
"""

CONNECTION_SPAN_NAME = "connection"
"""
Connection span name to be used by the instrumentor.
"""

FIRST_SUPPORTED_VERSION = "8.1.0"
"""
First mysql-connector-python version to support opentelemetry instrumentation.
"""

TRACEPARENT_HEADER_NAME = "traceparent"

DB_SYSTEM = "mysql"
DEFAULT_THREAD_NAME = "main"
DEFAULT_THREAD_ID = 0

# Reference: https://github.com/open-telemetry/opentelemetry-specification/blob/main/
# specification/trace/semantic_conventions/span-general.md
NET_SOCK_FAMILY = "net.sock.family"
NET_SOCK_PEER_ADDR = "net.sock.peer.addr"
NET_SOCK_PEER_PORT = "net.sock.peer.port"
NET_SOCK_HOST_ADDR = "net.sock.host.addr"
NET_SOCK_HOST_PORT = "net.sock.host.port"
