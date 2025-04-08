"""Unified metrics collection for Stream application"""
import time
import threading
import os
import logging
import json
from typing import Dict, Any, List, Optional
from datetime import datetime

import redis
from prometheus_client import start_http_server, Counter, Gauge, Summary
from confluent_kafka.admin import AdminClient
from confluent_kafka import KafkaException

from src.config import KAFKA_BROKER, REDIS_CONFIG, MONITORING_CONFIG

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create Prometheus metrics
MESSAGES_TOTAL = Counter(
    'stream_messages_total', 
    'Total number of messages processed',
    ['topic', 'source_id', 'status']
)

SENSOR_VALUE = Gauge(
    'stream_sensor_value',
    'Sensor reading value',
    ['sensor_id', 'sensor_type', 'location']
)

ANOMALY_COUNT = Counter(
    'stream_anomalies_total',
    'Total number of anomalies detected',
    ['sensor_id', 'severity']
)

PROCESSING_TIME = Summary(
    'stream_processing_time_seconds',
    'Time taken to process messages',
    ['topic', 'operation']
)

KAFKA_UP = Gauge(
    'kafka_up',
    'Whether the Kafka broker is available',
    ['broker']
)

REDIS_UP = Gauge(
    'redis_up',
    'Whether Redis is available',
    ['host', 'port']
)

class MetricsCollector:
    """Unified metrics collection for Stream application"""
    
    def __init__(self, bootstrap_servers: str, redis_config: Dict[str, Any]):
        self.bootstrap_servers = bootstrap_servers
        self.redis_config = redis_config
        self.running = False
        
        # Initialize Kafka client
        self.admin_client = None
        self.connect_to_kafka()
        
        # Initialize Redis client
        try:
            self.redis_client = redis.Redis(
                host=redis_config['host'],
                port=redis_config['port'],
                db=redis_config['db'],
                decode_responses=True
            )
            ping = self.redis_client.ping()
            if ping:
                REDIS_UP.labels(
                    host=redis_config['host'], 
                    port=redis_config['port']
                ).set(1)
                logger.info(f"Connected to Redis at {redis_config['host']}:{redis_config['port']}")
            else:
                REDIS_UP.labels(
                    host=redis_config['host'], 
                    port=redis_config['port']
                ).set(0)
                logger.warning(f"Redis ping failed at {redis_config['host']}:{redis_config['port']}")
        except redis.RedisError as e:
            REDIS_UP.labels(
                host=redis_config['host'], 
                port=redis_config['port']
            ).set(0)
            logger.error(f"Failed to connect to Redis: {e}")
            self.redis_client = None
    
    def connect_to_kafka(self) -> bool:
        """Attempt to connect to Kafka"""
        try:
            self.admin_client = AdminClient({
                'bootstrap.servers': self.bootstrap_servers,
                'security.protocol': 'PLAINTEXT',
                'socket.timeout.ms': 10000,
                'request.timeout.ms': 20000,
                'retry.backoff.ms': 500
            })
            # Test connection by fetching cluster metadata
            metadata = self.admin_client.list_topics(timeout=10)
            topics = metadata.topics
            logger.info(f"Connected to Kafka. Available topics: {', '.join(topics.keys())}")
            KAFKA_UP.labels(broker=self.bootstrap_servers).set(1)
            return True
        except KafkaException as e:
            logger.error(f"Failed to connect to Kafka at {self.bootstrap_servers}: {e}")
            KAFKA_UP.labels(broker=self.bootstrap_servers).set(0)
            return False
    
    def store_metric(self, metric_name: str, value: float, tags: Dict[str, str] = None) -> bool:
        """Store a metric in Redis"""
        if not self.redis_client:
            return False
        
        try:
            timestamp = datetime.now().isoformat()
            metric_data = {
                'timestamp': timestamp,
                'value': value,
                'tags': tags or {}
            }
            
            # Store in time series
            self.redis_client.lpush(
                f"metrics:{metric_name}", 
                json.dumps(metric_data)
            )
            self.redis_client.ltrim(
                f"metrics:{metric_name}", 
                0, 999
            )  # Keep last 1000 readings
            
            # Set current value
            self.redis_client.hset(
                "metrics:current", 
                metric_name, 
                value
            )
            
            return True
        except redis.RedisError as e:
            logger.error(f"Failed to store metric in Redis: {e}")
            return False
    
    def track_message(self, topic: str, source_id: str, status: str = "success") -> None:
        """Track message processing"""
        MESSAGES_TOTAL.labels(topic=topic, source_id=source_id, status=status).inc()
        
        if self.redis_client:
            # Update message count in Redis
            try:
                self.redis_client.incr(f"stream:counts:{topic}")
                
                # Also store by source_id
                if source_id:
                    self.redis_client.incr(f"stream:counts:{topic}:{source_id}")
            except redis.RedisError as e:
                logger.error(f"Failed to update message count in Redis: {e}")
    
    def track_sensor_reading(self, sensor_id: str, sensor_type: str, 
                           location: str, value: float) -> None:
        """Track sensor reading"""
        SENSOR_VALUE.labels(
            sensor_id=sensor_id,
            sensor_type=sensor_type,
            location=location
        ).set(value)
        
        # Store in Redis
        self.store_metric(
            f"sensor:{sensor_id}", 
            value, 
            {'sensor_type': sensor_type, 'location': location}
        )
        
        # Also add to sensor set
        if self.redis_client:
            try:
                # Store sensor ID in set
                self.redis_client.sadd("stream:sensors", sensor_id)
                
                # Store reading in time-ordered list
                reading = {
                    'sensor_id': sensor_id, 
                    'sensor_type': sensor_type,
                    'location': location, 
                    'value': value,
                    'timestamp': datetime.now().isoformat()
                }
                self.redis_client.lpush(
                    f"stream:sensor:{sensor_id}:readings", 
                    json.dumps(reading)
                )
                self.redis_client.ltrim(
                    f"stream:sensor:{sensor_id}:readings", 
                    0, 999
                )
            except redis.RedisError as e:
                logger.error(f"Failed to store sensor reading in Redis: {e}")
    
    def track_anomaly(self, sensor_id: str, severity: str, 
                    anomaly_data: Dict[str, Any]) -> None:
        """Track detected anomaly"""
        ANOMALY_COUNT.labels(sensor_id=sensor_id, severity=severity).inc()
        
        # Store in Redis
        if self.redis_client:
            try:
                # Store anomaly in time-ordered list
                self.redis_client.lpush(
                    "stream:anomalies:recent", 
                    json.dumps(anomaly_data)
                )
                self.redis_client.ltrim(
                    "stream:anomalies:recent", 
                    0, 199
                )  # Keep last 200 anomalies
                
                # Update count
                self.redis_client.incr("stream:counts:anomalies")
            except redis.RedisError as e:
                logger.error(f"Failed to store anomaly in Redis: {e}")
    
    def track_processing_time(self, topic: str, operation: str, duration: float) -> None:
        """Track processing time"""
        PROCESSING_TIME.labels(topic=topic, operation=operation).observe(duration)
        
        # Store in Redis
        self.store_metric(
            f"processing_time:{topic}:{operation}", 
            duration
        )
    
    def update_system_metrics(self) -> None:
        """Update system metrics"""
        if not self.redis_client:
            return
            
        try:
            # Example system metrics - in a real app these would come from actual system
            metrics = {
                "cpu_usage": round(30 + 20 * abs((time.time() % 60) / 60 - 0.5), 1),
                "memory_usage": round(40 + 10 * abs((time.time() % 120) / 120 - 0.5), 1),
                "messages_per_sec": round(10 + 5 * (time.time() % 10), 1)
            }
            
            # Store in Redis
            for key, value in metrics.items():
                self.store_metric(f"system:{key}", value)
                
            # Also store in hash for quick access
            self.redis_client.hset("stream:metrics:system", mapping=metrics)
        except redis.RedisError as e:
            logger.error(f"Failed to update system metrics in Redis: {e}")
    
    def start_collecting(self) -> None:
        """Start metrics collection in background thread"""
        if self.running:
            logger.warning("Metrics collector is already running")
            return
            
        self.running = True
        collector_thread = threading.Thread(target=self._collection_loop)
        collector_thread.daemon = True
        collector_thread.start()
        logger.info("Started metrics collection")
    
    def _collection_loop(self) -> None:
        """Background loop for metrics collection"""
        reconnect_delay = 5  # Initial reconnect delay
        max_reconnect_delay = 60  # Maximum delay
        
        while self.running:
            try:
                # Check Kafka connection
                if not self.admin_client:
                    if self.connect_to_kafka():
                        reconnect_delay = 5  # Reset delay on success
                    else:
                        # Use exponential backoff
                        time.sleep(reconnect_delay)
                        reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                        continue
                
                # Update system metrics
                self.update_system_metrics()
                
                # Sleep before next update
                time.sleep(15)
            except Exception as e:
                logger.error(f"Error in metrics collection: {e}", exc_info=True)
                self.admin_client = None  # Reset on error
                KAFKA_UP.labels(broker=self.bootstrap_servers).set(0)
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
    
    def stop(self) -> None:
        """Stop metrics collector"""
        logger.info("Stopping metrics collector")
        self.running = False

def start_metrics_server(port: int = 8000) -> MetricsCollector:
    """Start Prometheus metrics server and collector"""
    try:
        # Start Prometheus server
        start_http_server(port)
        logger.info(f"Prometheus metrics server started on port {port}")
        
        # Create and start metrics collector
        collector = MetricsCollector(
            bootstrap_servers=KAFKA_BROKER,
            redis_config=REDIS_CONFIG
        )
        collector.start_collecting()
        
        return collector
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}", exc_info=True)
        raise

# Singleton instance
_collector_instance = None

def get_collector() -> MetricsCollector:
    """Get or create the metrics collector instance"""
    global _collector_instance
    if _collector_instance is None:
        port = MONITORING_CONFIG['prometheus_port']
        _collector_instance = start_metrics_server(port=port)
    return _collector_instance

if __name__ == "__main__":
    # Start metrics server when run directly
    collector = start_metrics_server()
    
    try:
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        collector.stop()
