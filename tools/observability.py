from opentelemetry import trace
import logging
from opentelemetry.trace import Status, StatusCode
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter


# ------------------------------
# OpenTelemetry Setup
# ------------------------------
class Observe:
    def __init__(self):
        # Initialize tracing
        trace_provider = TracerProvider()
        trace_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        trace.set_tracer_provider(trace_provider)
        self.tracer = trace.get_tracer(__name__)

        # Initialize logging
        logger_provider = LoggerProvider()
        logger_provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))
        set_logger_provider(logger_provider)
        otel_log_handler = LoggingHandler(logger_provider=logger_provider)

        # Attach OTel handler to Python's root logger
        logging.basicConfig(level=logging.INFO, handlers=[otel_log_handler])
        self.logger = logging.getLogger(__name__)

    def get_tracer(self):
        return self.tracer

    def get_logger(self):
        return self.logger

# Usage example:
# otel_logger = Logger()
# logger = otel_logger.get_logger()
# tracer = otel_logger.get_tracer()
#
# logger.info("This is a test log message")
#
# with tracer.start_as_current_span("test_span") as span:
#     logger.info("Log message within a span")
#     span.set_status(Status(StatusCode.OK))