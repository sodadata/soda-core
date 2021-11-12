from functools import wraps
from os import name
import textwrap
from typing import Any, Dict, List, Optional

from opentelemetry import trace
from opentelemetry.trace.span import Span

import ast
import inspect

from opentelemetry.trace.status import Status, StatusCode
from opentelemetry.trace.propagation.tracecontext import \
    TraceContextTextMapPropagator

from sodasql.telemetry.soda_telemetry import soda_telemetry
trace_context_propagator = TraceContextTextMapPropagator()
trace_context_carrier = {}

tracer = trace.get_tracer_provider().get_tracer(__name__)


def get_decorators(function):
    """
    Fancy introspection - very WIP
    """
    decorators = {}

    def visit_FunctionDef(node):
        decorators[node.name] = {}
        for n in node.decorator_list:
            print(ast.dump(n))
            name = ''
            if isinstance(n, ast.Call):
                group = n.func.value.id
                name = n.func.attr if isinstance(n.func, ast.Attribute) else n.func.id

                for a in n.args:
                    print(ast.dump(a))
            else:
                group = n.attr if isinstance(n, ast.Attribute) else n.id
                name = None

            if group not in decorators[node.name]:
                decorators[node.name][group] = []

            if name:
                decorators[node.name][group].append({name})

    node_iter = ast.NodeVisitor()
    node_iter.visit_FunctionDef = visit_FunctionDef
    node_iter.visit(ast.parse(textwrap.dedent(inspect.getsource(function))))
    return decorators



def soda_trace(fn: callable):
    def _before_exec(span: Span, fn: callable):
        span.set_attribute("user_cookie_id", soda_telemetry.user_cookie_id)

    def _after_exec(span: Span, error: Optional[Exception] = None):
        pass


    @wraps(fn)
    def wrapper(*original_args, **original_kwargs):
        current_span = trace.get_current_span()
        if hasattr(current_span, 'context'):
            print(current_span.context.span_id)
        ctx = trace_context_propagator.extract(carrier=trace_context_carrier)
        with tracer.start_as_current_span(f"{fn.__module__}.{fn.__name__}", context=ctx) as span:
            trace_context_propagator.inject(carrier=trace_context_carrier)
            try:
                # print(get_decorators(fn))
                _before_exec(span, fn)
                result = fn(*original_args, **original_kwargs)
                span.set_status(Status(StatusCode.OK))
                _after_exec(span)
            except Exception as e:
                _after_exec(span, error=e)
                raise
        return result
    return wrapper

def extract_args(arguments: List, values: Dict, type: str, aliases: Optional[Dict] = {})  -> Dict:
    result = {}
    for key, value in values.items():
        if key in arguments:
            name = key
            if key in aliases:
                name = aliases[key]
            result[f'{type}_{name}'] = value or ""

    return result

def span_setup_function_args(args: Dict, config: Dict, aliases: Optional[Dict] = {}):
    for arg_type, args_to_extract in config.items():
        for span_arg_key, span_arg_value in extract_args(args_to_extract, args, arg_type, aliases).items():
            soda_telemetry.set_attribute(span_arg_key, span_arg_value)
