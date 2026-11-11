from __future__ import annotations

import logging
from contextvars import ContextVar
from functools import wraps
from typing import Optional

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace.span import Span
from opentelemetry.trace.status import Status, StatusCode
from soda_core.cli.exit_codes import OK_CODES, ExitCode
from soda_core.common.env_config_helper import EnvConfigHelper

tracer = trace.get_tracer_provider().get_tracer(__name__)
_in_soda_span: ContextVar[bool] = ContextVar("in_soda_span", default=False)
# Read once at import, as SodaTelemetry does.
_telemetry_enabled: bool = EnvConfigHelper().soda_core_telemetry_enabled


from soda_core.common.logging_constants import soda_logger

logger: logging.Logger = soda_logger


def soda_trace(fn: callable):
    def _after_exec(span: Span, error: Optional[BaseException] = None):
        if error:
            code = getattr(error, "code", None)
            try:
                if ExitCode(code) in OK_CODES:
                    span.set_status(Status(StatusCode.OK))
                else:
                    span.set_status(Status(StatusCode.ERROR, str(code)))
            except (ValueError, TypeError):
                # The type only: exception messages name files, datasets and hosts.
                span.set_status(Status(StatusCode.ERROR, type(error).__name__))
        else:
            span.set_status(Status(StatusCode.OK))

    @wraps(fn)
    def wrapper(*original_args, **original_kwargs):
        # Opted out: no span at all, or it would reach a tracer provider the caller set up.
        # One span per command: an API that the CLI or another API calls adds to the open span.
        if not _telemetry_enabled or _in_soda_span.get():
            return fn(*original_args, **original_kwargs)
        token = _in_soda_span.set(True)
        try:
            return _traced(*original_args, **original_kwargs)
        finally:
            _in_soda_span.reset(token)

    def _traced(*original_args, **original_kwargs):
        with tracer.start_as_current_span(
            f"{fn.__module__}.{fn.__name__}",
            # A fresh root, so a caller's own span never becomes the parent of a span sent to Soda.
            context=Context(),
            # OTel would otherwise send the exception message and stack trace.
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            result = None
            try:
                result = fn(*original_args, **original_kwargs)
                _after_exec(span)
            except BaseException as e:
                # Catch base exception to deal with system exit codes as well.
                _after_exec(span, e)
                raise e
        return result

    return wrapper
