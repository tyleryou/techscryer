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
from opentelemetry.sdk.resources import Resource
from dotenv import load_dotenv
import os


class Observe:
    def __init__(self, service_name):
        load_dotenv()
        # Create resource with service name
        self.resource = Resource.create({
            "service.name": service_name,
        })

        # Initialize tracing
        self._setup_tracing()

        # Initialize logging
        self._setup_logging()

    def _setup_tracing(self):
        """Configure OpenTelemetry tracing"""
        trace_provider = TracerProvider(resource=self.resource)
        trace_provider.add_span_processor(
            BatchSpanProcessor(
                OTLPSpanExporter(
                    endpoint=f"{os.getenv('OTEL_HOST')}:{os.getenv('OTEL_PORT')}",  # Explicit endpoint
                    insecure=True  # Only for development/testing
                )
            )
        )
        trace.set_tracer_provider(trace_provider)
        self.tracer = trace.get_tracer(__name__)

    def _setup_logging(self):
        """Configure OpenTelemetry logging"""
        logger_provider = LoggerProvider(resource=self.resource)
        logger_provider.add_log_record_processor(
            BatchLogRecordProcessor(
                OTLPLogExporter(
                    endpoint=f"{os.getenv('OTEL_HOST')}:{os.getenv('OTEL_PORT')}",  # Explicit endpoint
                    insecure=True  # Only for development/testing
                )
            )
        )
        set_logger_provider(logger_provider)

        # Configure Python logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger()

        # Add OpenTelemetry handler
        otel_log_handler = LoggingHandler(
            logger_provider=logger_provider,
            level=logging.INFO
        )
        logger.addHandler(otel_log_handler)

        # Store logger instance
        self.logger = logging.getLogger(__name__)

    def get_tracer(self):
        return self.tracer

    def get_logger(self):
        return self.logger

    def log_exception(self, message, exception):
        """Helper method to log exceptions with trace context"""
        self.logger.error(
            f"{message}: {str(exception)}",
            exc_info=exception,
            extra={"custom_attributes": {"error_type": type(exception).__name__}}
        )