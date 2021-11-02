# from opentelemetry.trace import Tracer
#
#
# class SodaTracer(Tracer):
#     def __init__(self, tracer=None):
#         if not callable(tracer):
#             self.__tracer = tracer
#             self.__tracer_getter = None
#         else:
#             self.__tracer = None
#             self.__tracer_getter = tracer
#
#     @property
#     def tracer(self):
#         if not self.__tracer:
#             if self.__tracer_getter is None:
#                 return opentracing.tracer
#
#             self.__tracer = self.__tracer_getter()
#
#         return self.__tracer
#
#     def trace(self, *attributes):
#         """
#         Function decorator that traces functions
#         NOTE: Must be placed after the @app.route decorator
#         @param attributes any number of flask.Request attributes
#         (strings) to be set as tags on the created span
#         """
#
#         def decorator(f):
#             def wrapper(*args, **kwargs):
#
#                 self._before_request_fn(list(attributes))
#                 try:
#                     r = f(*args, **kwargs)
#                     self._after_request_fn()
#                 except Exception as e:
#                     self._after_request_fn(error=e)
#                     raise
#
#                 self._after_request_fn()
#                 return r
#
#             wrapper.__name__ = f.__name__
#             return wrapper
#
#         return decorator
#
#     def _before_request_fn(self, attributes):
#         request = stack.top.request
#         operation_name = request.endpoint
#         headers = {}
#         for k, v in request.headers:
#             headers[k.lower()] = v
#
#         try:
#             span_ctx = self.tracer.extract(opentracing.Format.HTTP_HEADERS,
#                                            headers)
#             scope = self.tracer.start_active_span(operation_name,
#                                                   child_of=span_ctx)
#         except (opentracing.InvalidCarrierException,
#                 opentracing.SpanContextCorruptedException):
#             scope = self.tracer.start_active_span(operation_name)
#
#         self._current_scopes[request] = scope
#
#         span = scope.span
#         span.set_tag(tags.COMPONENT, 'Flask')
#         span.set_tag(tags.HTTP_METHOD, request.method)
#         span.set_tag(tags.HTTP_URL, request.base_url)
#         span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_SERVER)
#
#         for attr in attributes:
#             if hasattr(request, attr):
#                 payload = getattr(request, attr)
#                 if payload not in ('', b''):  # python3
#                     span.set_tag(attr, str(payload))
#
#         self._call_start_span_cb(span, request)
#
#     def _after_request_fn(self, response=None, error=None):
#         request = stack.top.request
#
#         # the pop call can fail if the request is interrupted by a
#         # `before_request` method so we need a default
#         scope = self._current_scopes.pop(request, None)
#         if scope is None:
#             return
#
#         if response is not None:
#             scope.span.set_tag(tags.HTTP_STATUS_CODE, response.status_code)
#         if error is not None:
#             scope.span.set_tag(tags.ERROR, True)
#             scope.span.log_kv({
#                 'event': tags.ERROR,
#                 'error.object': error,
#             })
#
#         scope.close()
