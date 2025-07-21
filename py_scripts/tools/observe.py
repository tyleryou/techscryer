from opentelemetry import trace, metrics
from tools.encrypt import Encrypt
from tools import env_loader as env
import logging
from opentelemetry.trace import StatusCode
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

class Observe:
    def __init__(self, service_name):
        self.resource = Resource.create({"service.name": service_name})
        self.headers = {'authorization': f'Bearer {env.get_env("OTEL_BEARER_TOKEN")}'}
        self.encryptor = Encrypt(tls_cert_path=env.get_env('TLS_COLLECTOR_CERT_PATH'))
        self.service_name = service_name

class Tracer(Observe):
    def __init__(self, service_name):
        super().__init__(service_name)
        self._setup_tracing()

    def _setup_tracing(self):
        credentials = self.encryptor.get_tls_credentials()
        trace_provider = TracerProvider(resource=self.resource)
        trace_provider.add_span_processor(
            BatchSpanProcessor(
                OTLPSpanExporter(
                    endpoint=f"{env.get_env('OTEL_COLLECTOR_HOST')}:{env.get_env('OTEL_COLLECTOR_PORT')}",
                    headers=self.headers,
                    credentials=credentials
                )
            )
        )
        trace.set_tracer_provider(trace_provider)
        self.tracer = trace.get_tracer(self.service_name)
        
    def get_tracer(self):
        return self.tracer

class Logger(Observe):
    def __init__(self, service_name):
        super().__init__(service_name)
        self._setup_logging()

    def _setup_logging(self):
        credentials = self.encryptor.get_tls_credentials()
        logger_provider = LoggerProvider(resource=self.resource)
        logger_provider.add_log_record_processor(
            BatchLogRecordProcessor(
                OTLPLogExporter(
                    endpoint=f"{env.get_env('OTEL_COLLECTOR_HOST')}:{env.get_env('OTEL_COLLECTOR_PORT')}",
                    headers=self.headers,
                    credentials=credentials
                )
            )
        )
        set_logger_provider(logger_provider)
        logging.basicConfig(level=logging.INFO)
        handler = LoggingHandler(logger_provider=logger_provider)
        self.logger = logging.getLogger(self.service_name)
        self.logger.addHandler(handler)
        
    def get_logger(self):
        return self.logger

class Meter(Observe):
    def __init__(self, service_name):
        super().__init__(service_name)
        self._setup_metrics()
        
    def _setup_metrics(self):
        credentials = self.encryptor.get_tls_credentials()
        metric_exporter = OTLPMetricExporter(
            endpoint=f"{env.get_env('OTEL_COLLECTOR_HOST')}:{env.get_env('OTEL_COLLECTOR_PORT')}",
            headers=self.headers,
            credentials=credentials
        )
        metric_reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=5000)
        meter_provider = MeterProvider(resource=self.resource, metric_readers=[metric_reader])
        metrics.set_meter_provider(meter_provider)
        
        self.meter = metrics.get_meter(self.service_name)
        self._metrics = {
            'requests_total': self.meter.create_counter(
                f"{self.service_name}_requests_total",
                description="Total API requests"
            ),
            'request_duration': self.meter.create_histogram(
                f"{self.service_name}_request_duration_seconds",
                description="Request latency",
                unit="s"
            ),
            'extract_errors': self.meter.create_counter(
                f"{self.service_name}_extract_errors_total",
                description="Extraction failures"
            ),
            'extract_duration': self.meter.create_histogram(
                f"{self.service_name}_extract_duration_seconds",
                description="Extract latency",
                unit="s"
            ),
            'load_errors': self.meter.create_counter(
                f"{self.service_name}_load_errors_total",
                description="Loading failures"
            ),
            'load_duration': self.meter.create_histogram(
                f"{self.service_name}_load_duration_seconds",
                description="Load latency",
                unit="s"
            ),
            'documents_processed': self.meter.create_counter(
                f"{self.service_name}_documents_processed_total",
                description="Documents processed"
            ),
            'active_connections': self.meter.create_up_down_counter(
                f"{self.service_name}_active_connections",
                description="Active DB connections"
            )
        }
        
        # Create direct access to metrics
        self.requests_total = self._metrics['requests_total']
        self.request_duration = self._metrics['request_duration']
        self.extract_errors = self._metrics['extract_errors']
        self.extract_duration = self._metrics['extract_duration']
        self.load_errors = self._metrics['load_errors']
        self.load_duration = self._metrics['load_duration']
        self.documents_processed = self._metrics['documents_processed']
        self.active_connections = self._metrics['active_connections']
        
    def get_metric(self, name: str):
        """Get a pre-configured metric by name"""
        if name not in self._metrics:
            raise ValueError(f"Unknown metric: {name}. Available: {list(self._metrics.keys())}")
        return self._metrics[name]
    
    def get_request_counter(self):
        """Get the request counter metric"""
        return self.requests_total
    
    def get_request_duration(self):
        """Get the request duration metric"""
        return self.request_duration
    
    def increment_connections(self):
        """Increment active connections"""
        self.active_connections.add(1)
    
    def decrement_connections(self):
        """Decrement active connections"""
        self.active_connections.add(-1)