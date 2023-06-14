"""Trace context propagation utilities."""
# mypy: disable-error-code="no-redef"
# pylint: disable=invalid-name

from typing import TYPE_CHECKING, Any, Callable, Union

from .constants import OTEL_ENABLED, TRACEPARENT_HEADER_NAME

if OTEL_ENABLED:
    from .instrumentation import OTEL_SYSTEM_AVAILABLE

    if OTEL_SYSTEM_AVAILABLE:
        # pylint: disable=import-error
        # load otel from the system
        from opentelemetry import trace
        from opentelemetry.trace.span import format_span_id, format_trace_id
    else:
        # load otel from the bundled installation
        from mysql.opentelemetry import trace
        from mysql.opentelemetry.trace.span import format_span_id, format_trace_id


if TYPE_CHECKING:
    from ..connection import MySQLConnection
    from ..connection_cext import CMySQLConnection


def build_traceparent_header(span: Any) -> str:
    """Build a traceparent header according to the provided span.

    The context information from the provided span is used to build the traceparent
    header that will be propagated to the MySQL server. For particulars regarding
    the header creation, refer to [1].

    This method assumes version 0 of the W3C specification.

    Args:
        span (opentelemetry.trace.span.Span): current span in trace.

    Returns:
        traceparent_header (str): HTTP header field that identifies requests in a
        tracing system.

    References:
        [1]: https://www.w3.org/TR/trace-context/#traceparent-header
    """
    ctx = span.get_span_context()

    version = "00"  # version 0 of the W3C specification
    trace_id = format_trace_id(ctx.trace_id)
    span_id = format_span_id(ctx.span_id)
    trace_flags = "00"  # sampled flag is off

    return "-".join([version, trace_id, span_id, trace_flags])


def with_context_propagation(method: Callable) -> Callable:
    """Perform trace context propagation.

    The trace context is propagated via query attributes. The `traceparent` header
    from W3C specification [1] is used, in this sense, the attribute name is
    `traceparent` (is RESERVED, avoid using it), and its value is built as per
    instructed in [1].

    If opentelemetry API/SDK is unavailable or there is no recording span,
    trace context propagation is skipped.

    References:
        [1]: https://www.w3.org/TR/trace-context/#traceparent-header
    """

    def wrapper(
        cnx: Union["MySQLConnection", "CMySQLConnection"], *args: Any, **kwargs: Any
    ) -> Any:
        """Context propagation decorator."""
        if not OTEL_ENABLED or not cnx.otel_context_propagation:
            return method(cnx, *args, **kwargs)

        current_span = trace.get_current_span()
        tp_header = None
        if current_span.is_recording():
            tp_header = build_traceparent_header(current_span)
            cnx.query_attrs_append(value=(TRACEPARENT_HEADER_NAME, tp_header))

        try:
            result = method(cnx, *args, **kwargs)
        finally:
            if tp_header is not None:
                cnx.query_attrs_remove(name=TRACEPARENT_HEADER_NAME)
        return result

    return wrapper
