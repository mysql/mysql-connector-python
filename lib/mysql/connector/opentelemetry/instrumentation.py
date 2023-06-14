"""MySQL instrumentation supporting mysql-connector."""
# mypy: disable-error-code="no-redef"
# pylint: disable=protected-access,global-statement,invalid-name

from __future__ import annotations

import functools
import re

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, Collection, Dict, Optional, Union

# pylint: disable=cyclic-import
if TYPE_CHECKING:
    # `TYPE_CHECKING` is always False at run time, hence circular import
    # will not happen at run time (no error happens whatsoever).
    # Since pylint is a static checker it happens that `TYPE_CHECKING`
    # is True when analyzing the code which makes pylint believe there
    # is a circular import issue when there isn't.

    from ..abstracts import MySQLConnectionAbstract
    from ..connection import MySQLConnection
    from ..cursor import MySQLCursor
    from ..pooling import PooledMySQLConnection

    try:
        from ..connection_cext import CMySQLConnection
        from ..cursor_cext import CMySQLCursor
    except ImportError:
        # The cext is not available.
        pass

from ... import connector
from ..constants import CNX_POOL_ARGS, DEFAULT_CONFIGURATION
from ..logger import logger
from ..version import VERSION_TEXT

try:
    # pylint: disable=unused-import
    # try to load otel from the system
    from opentelemetry import trace  # check api
    from opentelemetry.sdk.trace import TracerProvider  # check sdk
    from opentelemetry.semconv.trace import SpanAttributes  # check semconv

    OTEL_SYSTEM_AVAILABLE = True
except ImportError:
    try:
        # falling back to the bundled installation
        from mysql.opentelemetry import trace
        from mysql.opentelemetry.semconv.trace import SpanAttributes

        OTEL_SYSTEM_AVAILABLE = False
    except ImportError as missing_dependencies_err:
        raise connector.errors.ProgrammingError(
            "Bundled installation has missing dependencies. "
            "Please use `pip install mysql-connector-python[opentelemetry]`, "
            "or for an editable install use `pip install -e '.[opentelemetry]'`, "
            "to install the dependencies required by the bundled opentelemetry package."
        ) from missing_dependencies_err


from .constants import (
    CONNECTION_SPAN_NAME,
    DB_SYSTEM,
    DEFAULT_THREAD_ID,
    DEFAULT_THREAD_NAME,
    FIRST_SUPPORTED_VERSION,
    NET_SOCK_FAMILY,
    NET_SOCK_HOST_ADDR,
    NET_SOCK_HOST_PORT,
    NET_SOCK_PEER_ADDR,
    NET_SOCK_PEER_PORT,
    OPTION_CNX_SPAN,
    OPTION_CNX_TRACER,
)

leading_comment_remover: re.Pattern = re.compile(r"^/\*.*?\*/")


def record_exception_event(span: trace.Span, exc: Optional[Exception]) -> None:
    """Records an exeception event."""
    if not span or not span.is_recording() or not exc:
        return

    span.set_status(trace.Status(trace.StatusCode.ERROR))
    span.record_exception(exc)


def end_span(span: trace.Span) -> None:
    """Ends span."""
    if not span or not span.is_recording():
        return

    span.end()


def get_operation_name(operation: str) -> str:
    """Parse query to extract operation name."""
    if operation and isinstance(operation, str):
        # Strip leading comments so we get the operation name.
        return leading_comment_remover.sub("", operation).split()[0]
    return ""


def set_connection_span_attrs(
    cnx: Optional["MySQLConnectionAbstract"],
    cnx_span: trace.Span,
    cnx_kwargs: Optional[Dict[str, Any]] = None,
) -> None:
    """Defines connection span attributes. If `cnx` is None then we use `cnx_kwargs`
    to get basic net information. Basic net attributes are defined such as:

    * DB_SYSTEM
    * NET_TRANSPORT
    * NET_SOCK_FAMILY

    Socket-level attributes [*] are also defined [**].

    [*]: Socket-level attributes identify peer and host that are directly connected to
    each other. Since instrumentations may have limited knowledge on network
    information, instrumentations SHOULD populate such attributes to the best of
    their knowledge when populate them at all.

    [**]: `CMySQLConnection` connections have no access to socket-level
    details so socket-level attributes aren't included. `MySQLConnection`
    connections, on the other hand, do include socket-level attributes.

    References:
    [1]: https://github.com/open-telemetry/opentelemetry-specification/blob/main/
    specification/trace/semantic_conventions/span-general.md
    """
    # pylint: disable=broad-exception-caught
    if not cnx_span or not cnx_span.is_recording():
        return

    if cnx_kwargs is None:
        cnx_kwargs = {}

    is_tcp = not cnx._unix_socket if cnx else "unix_socket" not in cnx_kwargs

    attrs: Dict[str, Any] = {
        SpanAttributes.DB_SYSTEM: DB_SYSTEM,
        SpanAttributes.NET_TRANSPORT: "ip_tcp" if is_tcp else "inproc",
        NET_SOCK_FAMILY: "inet" if is_tcp else "unix",
    }

    # Only socket and tcp connections are supported.
    if is_tcp:
        attrs[SpanAttributes.NET_PEER_NAME] = (
            cnx._host if cnx else cnx_kwargs.get("host", DEFAULT_CONFIGURATION["host"])
        )
        attrs[SpanAttributes.NET_PEER_PORT] = (
            cnx._port if cnx else cnx_kwargs.get("port", DEFAULT_CONFIGURATION["port"])
        )

        if hasattr(cnx, "_socket") and cnx._socket:
            try:
                (
                    attrs[NET_SOCK_PEER_ADDR],
                    sock_peer_port,
                ) = cnx._socket.sock.getpeername()

                (
                    attrs[NET_SOCK_HOST_ADDR],
                    attrs[NET_SOCK_HOST_PORT],
                ) = cnx._socket.sock.getsockname()
            except Exception as sock_err:
                logger.warning("Connection socket is down %s", sock_err)
            else:
                if attrs[SpanAttributes.NET_PEER_PORT] != sock_peer_port:
                    # NET_SOCK_PEER_PORT is recommended if different than net.peer.port
                    # and if net.sock.peer.addr is set.
                    attrs[NET_SOCK_PEER_PORT] = sock_peer_port
    else:
        # For Unix domain socket, net.sock.peer.addr attribute represents
        # destination name and net.peer.name SHOULD NOT be set.
        attrs[NET_SOCK_PEER_ADDR] = (
            cnx._unix_socket if cnx else cnx_kwargs.get("unix_socket")
        )

        if hasattr(cnx, "_socket") and cnx._socket:
            try:
                attrs[NET_SOCK_HOST_ADDR] = cnx._socket.sock.getsockname()
            except Exception as sock_err:
                logger.warning("Connection socket is down %s", sock_err)

    cnx_span.set_attributes(attrs)


def instrument_execution(
    query_method: Callable,
    tracer: trace.Tracer,
    connection_span_link: trace.Link,
    wrapped: Union["MySQLCursor", "CMySQLCursor"],
    *args: Any,
    **kwargs: Any,
) -> Callable:
    """Instruments the execution of `query_method`.

    A query span with a link to the corresponding connection span is generated.
    """
    connection: Union["MySQLConnection", "CMySQLConnection"] = (
        getattr(wrapped, "_connection")
        if hasattr(wrapped, "_connection")
        else getattr(wrapped, "_cnx")
    )

    # SpanAttributes.DB_NAME: connection.database or ""; introduces performance
    # degradation, at this time the database attribute is something nice to have but
    # not a requirement.
    query_span_attributes: Dict = {
        SpanAttributes.DB_SYSTEM: DB_SYSTEM,
        SpanAttributes.DB_USER: connection._user,
        SpanAttributes.THREAD_ID: DEFAULT_THREAD_ID,
        SpanAttributes.THREAD_NAME: DEFAULT_THREAD_NAME,
        "cursor_type": wrapped.__class__.__name__,
    }
    with tracer.start_as_current_span(
        name=get_operation_name(args[0]) or "SQL statement",
        kind=trace.SpanKind.CLIENT,
        links=[connection_span_link],
        attributes=query_span_attributes,
    ):
        return query_method(*args, **kwargs)


class BaseMySQLTracer(ABC):
    """Base class that provides basic object wrapper functionality."""

    @abstractmethod
    def __init__(self) -> None:
        """Must be implemented by subclasses."""

    def __getattr__(self, attr: str) -> Any:
        """Gets an attribute.

        Attributes defined in the wrapper object have higher precedence
        than those wrapped object equivalent. Attributes not found in
        the wrapper are then searched in the wrapped object.
        """
        if attr in self.__dict__:
            # this object has it
            return getattr(self, attr)
        # proxy to the wrapped object
        return getattr(self._wrapped, attr)

    def __setattr__(self, name: str, value: Any) -> None:
        if "_wrapped" not in self.__dict__:
            self.__dict__["_wrapped"] = value
            return

        if name in self.__dict__:
            # this object has it
            super().__setattr__(name, value)
            return
        # proxy to the wrapped object
        self._wrapped.__setattr__(name, value)

    def __enter__(self) -> Any:
        """Magic method."""
        self._wrapped.__enter__()
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        """Magic method."""
        self._wrapped.__exit__(*args, **kwargs)

    def get_wrapped_class(self) -> str:
        """Gets the wrapped class name."""
        return self._wrapped.__class__.__name__


class TracedMySQLCursor(BaseMySQLTracer):
    """Wrapper class for a `MySQLCursor` or `CMySQLCursor` object."""

    def __init__(
        self,
        wrapped: Union["MySQLCursor", "CMySQLCursor"],
        tracer: trace.Tracer,
        connection_span: trace.Span,
    ):
        """Constructor."""
        self._wrapped: Union["MySQLCursor", "CMySQLCursor"] = wrapped
        self._tracer: trace.Tracer = tracer
        self._connection_span_link: trace.Link = trace.Link(
            connection_span.get_span_context()
        )

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """Instruments execute method."""
        return instrument_execution(
            self._wrapped.execute,
            self._tracer,
            self._connection_span_link,
            self._wrapped,
            *args,
            **kwargs,
        )

    def executemany(self, *args: Any, **kwargs: Any) -> Any:
        """Instruments executemany method."""
        return instrument_execution(
            self._wrapped.executemany,
            self._tracer,
            self._connection_span_link,
            self._wrapped,
            *args,
            **kwargs,
        )

    def callproc(self, *args: Any, **kwargs: Any) -> Any:
        """Instruments callproc method."""
        return instrument_execution(
            self._wrapped.callproc,
            self._tracer,
            self._connection_span_link,
            self._wrapped,
            *args,
            **kwargs,
        )


class TracedMySQLConnection(BaseMySQLTracer):
    """Wrapper class for a `MySQLConnection` or `CMySQLConnection` object."""

    def __init__(self, wrapped: Union["MySQLConnection", "CMySQLConnection"]) -> None:
        """Constructor."""
        self._wrapped: Union["MySQLConnection", "CMySQLConnection"] = wrapped

        # call `sql_mode` so its value is cached internally and querying it does not
        # interfere when recording query span events later.
        _ = self._wrapped.sql_mode

    def cursor(self, *args: Any, **kwargs: Any) -> TracedMySQLCursor:
        """Wraps the cursor object."""
        return TracedMySQLCursor(
            wrapped=self._wrapped.cursor(*args, **kwargs),
            tracer=self._tracer,
            connection_span=self._span,
        )


def instrument_connect(
    connect: Callable[
        ..., Union["MySQLConnection", "CMySQLConnection", "PooledMySQLConnection"]
    ],
    tracer_provider: Optional[trace.TracerProvider] = None,
) -> Callable[
    ..., Union["MySQLConnection", "CMySQLConnection", "PooledMySQLConnection"]
]:
    """Retrurn the instrumented version of `connect`."""

    # let's preserve `connect` identity.
    @functools.wraps(connect)
    def wrapper(
        *args: Any, **kwargs: Any
    ) -> Union["MySQLConnection", "CMySQLConnection", "PooledMySQLConnection"]:
        """Wraps the connection object returned by the method `connect`.

        Instrumentation for PooledConnections is not supported.
        """
        if any(key in kwargs for key in CNX_POOL_ARGS):
            logger.warning("Instrumentation for pooled connections not supported")
            return connect(*args, **kwargs)

        tracer = trace.get_tracer(
            instrumenting_module_name="MySQL Connector/Python",
            instrumenting_library_version=VERSION_TEXT,
            tracer_provider=tracer_provider,
        )

        # The connection span is passed in as an argument so the connection object can
        # keep a pointer to it.
        kwargs[OPTION_CNX_SPAN] = tracer.start_span(
            name=CONNECTION_SPAN_NAME, kind=trace.SpanKind.CLIENT
        )
        kwargs[OPTION_CNX_TRACER] = tracer

        # Add basic net information.
        set_connection_span_attrs(None, kwargs[OPTION_CNX_SPAN], kwargs)

        # Connection may fail at this point, in case it does, basic net info is already
        # included so the user can check the net configuration she/he provided.
        cnx = connect(*args, **kwargs)

        # connection went ok, let's refine the net information.
        set_connection_span_attrs(cnx, cnx._span, kwargs)  # type: ignore[arg-type]

        return TracedMySQLConnection(
            wrapped=cnx,  # type: ignore[return-value, arg-type]
        )

    return wrapper


class MySQLInstrumentor:
    """MySQL instrumentation supporting mysql-connector-python."""

    _instance: Optional[MySQLInstrumentor] = None

    def __new__(cls, *args: Any, **kwargs: Any) -> MySQLInstrumentor:
        """Singlenton.

        Restricts the instantiation to a singular instance.
        """
        if cls._instance is None:
            # create instance
            cls._instance = object.__new__(cls, *args, **kwargs)
            # keep a pointer to the uninstrumented connect method
            setattr(cls._instance, "_original_connect", connector.connect)
        return cls._instance

    def instrumentation_dependencies(self) -> Collection[str]:
        """Return a list of python packages with versions
        that the will be instrumented (e.g., versions >= 8.1.0)."""
        return [f"mysql-connector-python >= {FIRST_SUPPORTED_VERSION}"]

    def instrument(self, **kwargs: Any) -> None:
        """Instrument the library.

        Args:
            trace_module: reference to the 'trace' module from opentelemetry.
            tracer_provider (optional): TracerProvider instance.

        NOTE: Instrumentation for pooled connections not supported.
        """
        if connector.connect != getattr(self, "_original_connect"):
            logger.warning("MySQL Connector/Python module already instrumented.")
            return
        connector.connect = instrument_connect(
            connect=getattr(self, "_original_connect"),
            tracer_provider=kwargs.get("tracer_provider"),
        )

    def instrument_connection(
        self,
        connection: Union["MySQLConnection", "CMySQLConnection"],
        tracer_provider: Optional[trace.TracerProvider] = None,
    ) -> Union["MySQLConnection", "CMySQLConnection"]:
        """Enable instrumentation in a MySQL connection.

        Args:
            connection: uninstrumented connection instance.
            trace_module: reference to the 'trace' module from opentelemetry.
            tracer_provider (optional): TracerProvider instance.

        Returns:
            connection: instrumented connection instace.

        NOTE: Instrumentation for pooled connections not supported.
        """
        if isinstance(connection, TracedMySQLConnection):
            logger.warning("Connection already instrumented.")
            return connection

        if not hasattr(connection, "_span") or not hasattr(connection, "_tracer"):
            logger.warning(
                "Instrumentation for class %s not supported.",
                connection.__class__.__name__,
            )
            return connection

        tracer = trace.get_tracer(
            instrumenting_module_name="MySQL Connector/Python",
            instrumenting_library_version=VERSION_TEXT,
            tracer_provider=tracer_provider,
        )
        connection._span = tracer.start_span(
            name=CONNECTION_SPAN_NAME, kind=trace.SpanKind.CLIENT
        )
        connection._tracer = tracer

        set_connection_span_attrs(connection, connection._span)

        return TracedMySQLConnection(wrapped=connection)  # type: ignore[return-value]

    def uninstrument(self, **kwargs: Any) -> None:
        """Uninstrument the library."""
        # pylint: disable=unused-argument
        if connector.connect == getattr(self, "_original_connect"):
            logger.warning("MySQL Connector/Python module already uninstrumented.")
            return
        connector.connect = getattr(self, "_original_connect")

    def uninstrument_connection(
        self, connection: Union["MySQLConnection", "CMySQLConnection"]
    ) -> Union["MySQLConnection", "CMySQLConnection"]:
        """Disable instrumentation in a MySQL connection.

        Args:
            connection: instrumented connection instance.

        Returns:
            connection: uninstrumented connection instace.

        NOTE: Instrumentation for pooled connections not supported.
        """
        if not hasattr(connection, "_span"):
            logger.warning(
                "Uninstrumentation for class %s not supported.",
                connection.__class__.__name__,
            )
            return connection

        if not isinstance(connection, TracedMySQLConnection):
            logger.warning("Connection already uninstrumented.")
            return connection

        # stop connection span recording
        if connection._span and connection._span.is_recording():
            connection._span.end()
            connection._span = None

        return connection._wrapped
