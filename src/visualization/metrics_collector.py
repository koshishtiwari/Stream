"""Redis metrics collector for real-time dashboard data"""
import os
import json
import time
import threading
import logging
from typing import Dict, Any, Optional
from datetime import datetime

import redis
from confluent_kafka import Consumer, KafkaError, KafkaException
from src.config import KAFKA_BROKER

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Redis
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB = int(os.environ.get('REDIS_DB', 0))
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

class RedisMetricsCollector:
    """Collects metrics from Kafka topics and stores them in Redis for real-time dashboards"""
    
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.consumers = {}
        self.running = True
        logger.info(f"Initializing Redis metrics collector, connecting to Kafka at {bootstrap_servers}")
    
    def start_consumer(self, topic: str, group_id: str):
        """Start a Kafka consumer for a specific topic"""
        consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': f"{group_id}-redis-metrics",
            'auto.offset.reset': 'latest',  # Only process new messages
            'session.timeout.ms': 30000,
            'security.protocol': 'PLAINTEXT',
        }
        
        consumer = Consumer(consumer_config)
        consumer.subscribe([topic])
        
        self.consumers[topic] = consumer
        logger.info(f"Started consumer for topic: {topic}")
        
        return consumer
    
    def process_iot_sensor_data(self, message):
        """Process IoT sensor data and store in Redis"""
        try:
            data = json.loads(message.value().decode('utf-8'))
            sensor_id = data.get('sensor_id')
            
            if not sensor_id:
                return
            
            # Store sensor ID in set of known sensors
            redis_client.sadd("stream:sensors", sensor_id)
            
            # Store reading in time-ordered list (most recent first)
            redis_client.lpush(f"stream:sensor:{sensor_id}:readings", json.dumps(data))
            redis_client.ltrim(f"stream:sensor:{sensor_id}:readings", 0, 999)  # Keep last 1000 readings
            
            # Update count
            redis_client.incr("stream:counts:iot-sensors")
            
        except Exception as e:
            logger.error(f"Error processing IoT sensor data: {e}")
    
    def process_stock_data(self, message):
        """Process stock data and store in Redis"""
        try:
            data = json.loads(message.value().decode('utf-8'))
            payload = data.get('payload', {})
            symbol = payload.get('symbol')
            
            if not symbol:
                return
            
            # Store symbol in set of known stocks
            redis_client.sadd("stream:stocks", symbol)
            
            # Store price data in time-ordered list (most recent first)
            redis_client.lpush(f"stream:stock:{symbol}:prices", json.dumps(payload))
            redis_client.ltrim(f"stream:stock:{symbol}:prices", 0, 999)  # Keep last 1000 prices
            
            # Update count
            redis_client.incr("stream:counts:stock-data")
            
        except Exception as e:
            logger.error(f"Error processing stock data: {e}")
    
    def process_news_data(self, message):
        """Process news data and store in Redis"""
        try:
            data = json.loads(message.value().decode('utf-8'))
            payload = data.get('payload', {})
            
            # Store news item in time-ordered list (most recent first)
            redis_client.lpush("stream:news:recent", json.dumps(payload))
            redis_client.ltrim("stream:news:recent", 0, 99)  # Keep last 100 news items
            
            # Update count
            redis_client.incr("stream:counts:news-data")
            
        except Exception as e:
            logger.error(f"Error processing news data: {e}")
    
    def process_anomaly_data(self, message):
        """Process anomaly data and store in Redis"""
        try:
            data = json.loads(message.value().decode('utf-8'))
            
            # Store anomaly in time-ordered list (most recent first)
            redis_client.lpush("stream:anomalies:recent", json.dumps(data))
            redis_client.ltrim("stream:anomalies:recent", 0, 199)  # Keep last 200 anomalies
            
            # Update count
            redis_client.incr("stream:counts:iot-anomalies")
            
        except Exception as e:
            logger.error(f"Error processing anomaly data: {e}")
    
    def update_processing_metrics(self):
        """Update processing metrics in Redis"""
        while self.running:
            try:
                # Example metrics - in a real system, these would come from your processing components
                metrics = {
                    "processing_time": round(time.time() % 0.5, 3),  # Mock processing time
                    "messages_per_sec": round(10 + 5 * (time.time() % 10), 1),  # Mock throughput
                    "cpu_usage": round(30 + 20 * abs((time.time() % 60) / 60 - 0.5), 1),  # Mock CPU usage
                    "memory_usage": round(40 + 10 * abs((time.time() % 120) / 120 - 0.5), 1)  # Mock memory usage
                }
                
                # Store metrics in Redis hash - use hset instead of deprecated hmset
                for key, value in metrics.items():
                    redis_client.hset("stream:metrics:processing", key, value)
                
            except Exception as e:
                logger.error(f"Error updating processing metrics: {e}")
                
            time.sleep(1)  # Update every second
    
    def consume_topics(self):
        """Consume messages from all configured topics"""
        try:
            # Start consumer for IoT sensor data
            iot_consumer = self.start_consumer('iot-sensors', 'metrics-collector')
            
            # Start consumer for stock data
            stock_consumer = self.start_consumer('stock-data', 'metrics-collector')
            
            # Start consumer for news data
            news_consumer = self.start_consumer('news-data', 'metrics-collector')
            
            # Start consumer for anomaly data
            anomaly_consumer = self.start_consumer('iot-anomalies', 'metrics-collector')
            
            # Process messages in a loop
            max_retries = 10  # Maximum number of consecutive errors before giving up
            retry_delay = 5  # Initial retry delay in seconds
            error_count = 0
            
            while self.running:
                try:
                    # Poll all consumers
                    for topic, consumer in self.consumers.items():
                        msg = consumer.poll(0.1)  # Poll with small timeout
                        
                        if msg is None:
                            continue
                        
                        if msg.error():
                            if msg.error().code() == KafkaError._PARTITION_EOF:
                                # End of partition
                                continue
                            elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                                # Topic doesn't exist yet - log once per topic but continue
                                logger.warning(f"Topic {topic} not available yet. Will retry automatically.")
                                continue
                            else:
                                logger.error(f"Consumer error for topic {topic}: {msg.error()}")
                                error_count += 1
                                if error_count > max_retries:
                                    logger.critical(f"Too many consecutive errors ({error_count}). Backing off for {retry_delay} seconds")
                                    time.sleep(retry_delay)
                                    retry_delay = min(retry_delay * 2, 60)  # Exponential backoff, max 60 seconds
                                continue
                        
                        # On successful message, reset error counters
                        error_count = 0
                        retry_delay = 5
                        
                        # Process message based on topic
                        if topic == 'iot-sensors':
                            self.process_iot_sensor_data(msg)
                        elif topic == 'stock-data':
                            self.process_stock_data(msg)
                        elif topic == 'news-data':
                            self.process_news_data(msg)
                        elif topic == 'iot-anomalies':
                            self.process_anomaly_data(msg)
                    
                    time.sleep(0.01)  # Small sleep to prevent tight loop
                    
                except Exception as e:
                    logger.error(f"Error polling topics: {e}")
                    error_count += 1
                    if error_count > max_retries:
                        logger.critical(f"Too many consecutive errors ({error_count}). Backing off for {retry_delay} seconds")
                        time.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, 60)  # Exponential backoff, max 60 seconds
                
        finally:
            # Close all consumers
            for consumer in self.consumers.values():
                consumer.close()
    
    def start(self):
        """Start the metrics collector"""
        # Start consumer thread
        consumer_thread = threading.Thread(target=self.consume_topics)
        consumer_thread.daemon = True
        consumer_thread.start()
        
        # Start metrics update thread
        metrics_thread = threading.Thread(target=self.update_processing_metrics)
        metrics_thread.daemon = True
        metrics_thread.start()
        
        logger.info("Redis metrics collector started")
        
        return consumer_thread, metrics_thread
    
    def stop(self):
        """Stop the metrics collector"""
        logger.info("Stopping Redis metrics collector")
        self.running = False

def start_metrics_collector():
    """Start the Redis metrics collector as a service"""
    collector = RedisMetricsCollector(bootstrap_servers=KAFKA_BROKER)
    threads = collector.start()
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping metrics collector...")
        collector.stop()

if __name__ == "__main__":
    start_metrics_collector()
