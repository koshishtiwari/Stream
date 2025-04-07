"""Prometheus metrics exporter for Kafka streams"""
import time
import threading
import os
import logging
from typing import Dict, Any
from prometheus_client import start_http_server, Counter, Gauge
from confluent_kafka.admin import AdminClient
from confluent_kafka import KafkaException

from src.config import KAFKA_BROKER

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create Prometheus metrics
MESSAGES_TOTAL = Counter(
    'stream_messages_total', 
    'Total number of messages processed',
    ['topic', 'sensor_id', 'status']
)

TEMPERATURE_VALUE = Gauge(
    'stream_temperature_celsius',
    'Temperature sensor reading in Celsius',
    ['sensor_id', 'location']
)

ANOMALY_COUNT = Counter(
    'stream_anomalies_total',
    'Total number of anomalies detected',
    ['sensor_id', 'severity']
)

PROCESSING_TIME = Gauge(
    'stream_processing_time_seconds',
    'Time taken to process messages',
    ['topic']
)

# Add new metrics for Kafka availability
KAFKA_UP = Gauge(
    'kafka_up',
    'Whether the Kafka broker is available',
    ['broker']
)

class KafkaMetricsCollector:
    """Collects metrics from Kafka topics and exports them to Prometheus"""
    
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        logger.info(f"Connecting to Kafka at: {bootstrap_servers}")
        self.admin_client = None
        self.running = False
        self.connect_to_kafka()
    
    def connect_to_kafka(self) -> bool:
        """Attempt to connect to Kafka"""
        try:
            self.admin_client = AdminClient({
                'bootstrap.servers': self.bootstrap_servers,
                'security.protocol': 'PLAINTEXT',  # Explicitly set security protocol
                'socket.timeout.ms': 10000,
                'request.timeout.ms': 20000,
                'retry.backoff.ms': 500
            })
            # Test connection by fetching cluster metadata
            metadata = self.admin_client.list_topics(timeout=10)
            topics = metadata.topics
            logger.info(f"Successfully connected to Kafka. Available topics: {', '.join(topics.keys())}")
            KAFKA_UP.labels(broker=self.bootstrap_servers).set(1)
            return True
        except KafkaException as e:
            logger.error(f"Failed to connect to Kafka at {self.bootstrap_servers}: {e}")
            KAFKA_UP.labels(broker=self.bootstrap_servers).set(0)
            return False
    
    def get_topic_offsets(self, topic: str) -> Dict[int, int]:
        """Get the latest offsets for all partitions in a topic"""
        if not self.admin_client:
            return {}  # Return empty if not connected
            
        try:
            # Implementation using AdminClient
            # This is simplified - in production you'd use admin_client.list_topics()
            # to get actual partition data
            return {0: 0, 1: 0, 2: 0, 3: 0}  # Mock data for now
        except Exception as e:
            logger.error(f"Error getting offsets for topic {topic}: {e}")
            return {}
    
    def update_metrics(self):
        """Update Prometheus metrics with Kafka data"""
        reconnect_delay = 5  # Start with 5 second delay
        max_reconnect_delay = 60  # Maximum delay between reconnection attempts
        
        while self.running:
            try:
                # Check if we need to connect/reconnect to Kafka
                if not self.admin_client:
                    if self.connect_to_kafka():
                        reconnect_delay = 5  # Reset delay on successful connection
                    else:
                        # Use exponential backoff for reconnection attempts
                        time.sleep(reconnect_delay)
                        reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                        continue  # Skip this iteration if we can't connect
                
                # Update processing time (mock data)
                PROCESSING_TIME.labels(topic='iot-sensors').set(0.05)
                PROCESSING_TIME.labels(topic='iot-anomalies').set(0.02)
                
                # Add some example temperature data if we're connected
                TEMPERATURE_VALUE.labels(sensor_id='temp-001', location='datacenter-a').set(22.5)
                TEMPERATURE_VALUE.labels(sensor_id='temp-002', location='datacenter-b').set(24.8)
                TEMPERATURE_VALUE.labels(sensor_id='temp-003', location='datacenter-c').set(30.2)
                
                # Add sample message counts
                MESSAGES_TOTAL.labels(topic='iot-sensors', sensor_id='temp-001', status='success').inc()
                MESSAGES_TOTAL.labels(topic='iot-sensors', sensor_id='temp-002', status='success').inc()
                MESSAGES_TOTAL.labels(topic='iot-sensors', sensor_id='temp-003', status='success').inc()
                
                # Sleep before next update
                time.sleep(15)
            except Exception as e:
                logger.error(f"Error updating metrics: {e}", exc_info=True)
                self.admin_client = None  # Reset client on error
                KAFKA_UP.labels(broker=self.bootstrap_servers).set(0)
                time.sleep(reconnect_delay)  # Wait before retry
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
    
    def start(self, port: int = 9090):
        """Start the Prometheus metrics server and collection thread"""
        try:
            # Start Prometheus HTTP server
            start_http_server(port)
            logger.info(f"Prometheus metrics server started on port {port}")
            
            # Start metrics collection thread
            self.running = True
            collector_thread = threading.Thread(target=self.update_metrics)
            collector_thread.daemon = True
            collector_thread.start()
            return True
        except Exception as e:
            logger.error(f"Failed to start metrics server: {e}", exc_info=True)
            return False
      # Changed default port from 9090 to 8000
    def stop(self):
        """Stop the metrics collection"""
        logger.info("Stopping metrics collector")
        self.running = False

def start_metrics_exporter(port: int = 9090):
    """Start the metrics exporter"""
    collector = KafkaMetricsCollector(bootstrap_servers=KAFKA_BROKER)
    collector.start(port=port)
    return collector

if __name__ == "__main__":
    # Start metrics exporter
    collector = start_metrics_exporter()
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping metrics exporter...")
        collector.stop()
