"""Kafka Consumers for processing real-time data streams"""
import json
import time
import threading
import socket
import logging
from typing import Dict, Any, Callable
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
from src.config import KAFKA_BROKER

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define thresholds for anomaly detection
TEMPERATURE_THRESHOLDS = {
    'datacenter-a': {'min': 18.0, 'max': 27.0},
    'datacenter-b': {'min': 15.0, 'max': 25.0},
    'datacenter-c': {'min': 20.0, 'max': 35.0},
    # Default threshold if location not specified
    'default': {'min': 10.0, 'max': 35.0}
}

# In-memory state storage (could be replaced with Redis or another store)
sensor_states = {}

class IoTDataProcessor:
    """Process IoT sensor data streams"""
    
    def __init__(self, bootstrap_servers: str, input_topic: str, output_topic: str, group_id: str):
        self.input_topic = input_topic
        self.output_topic = output_topic
        
        # Configure consumer with better timeouts
        self.consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'socket.timeout.ms': 10000,  # Increase timeouts for Codespaces
            'session.timeout.ms': 30000,
            'client.id': f"{socket.gethostname()}-consumer",
            'security.protocol': 'PLAINTEXT',  # Explicitly set security protocol
            'request.timeout.ms': 60000,  # Increase timeout
            'max.poll.interval.ms': 300000  # Increase poll interval
        }
        logger.info(f"Consumer connecting to Kafka at: {bootstrap_servers}")
        self.consumer = Consumer(self.consumer_config)
        
        # Configure producer with better timeouts
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'socket.timeout.ms': 10000,  # Increase timeouts for Codespaces
            'message.timeout.ms': 10000,
            'client.id': f"{socket.gethostname()}-producer",
            'security.protocol': 'PLAINTEXT',  # Explicitly set security protocol
            'retry.backoff.ms': 500,  # Add retry backoff
            'request.timeout.ms': 20000  # Increase request timeout
        }
        logger.info(f"Producer connecting to Kafka at: {bootstrap_servers}")
        self.producer = Producer(self.producer_config)
        
    def delivery_report(self, err, msg):
        """Callback invoked on message delivery success or failure"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
    
    def process_temperature_data(self, event: Dict[str, Any]) -> None:
        """Process temperature sensor data and detect anomalies"""
        # Extract event data
        sensor_id = event.get('sensor_id')
        location = event.get('location')
        value = event.get('value')
        
        # Get thresholds for this location
        thresholds = TEMPERATURE_THRESHOLDS.get(location, TEMPERATURE_THRESHOLDS['default'])
        
        # Check for anomalies
        is_anomaly = value < thresholds['min'] or value > thresholds['max']
        
        # Update sensor state
        sensor_states[sensor_id] = {
            'last_value': value,
            'last_timestamp': event.get('timestamp'),
            'location': location,
            'type': event.get('sensor_type')
        }
        
        # If anomaly detected, send to anomaly topic
        if is_anomaly:
            anomaly_event = {
                **event,
                'anomaly': True,
                'threshold_min': thresholds['min'],
                'threshold_max': thresholds['max'],
                'severity': 'high' if abs(value - (thresholds['min'] + thresholds['max'])/2) > 
                           (thresholds['max'] - thresholds['min'])/2 else 'medium'
            }
            
            # Send to anomaly topic
            self.producer.produce(
                topic=self.output_topic,
                value=json.dumps(anomaly_event).encode('utf-8'),
                key=sensor_id.encode('utf-8'),
                callback=self.delivery_report
            )
            logger.warning(f"ANOMALY DETECTED: {anomaly_event}")
        else:
            logger.debug(f"Processed normal reading: {sensor_id}, value: {value}")
    
    def process_message(self, message):
        """Process a single message from Kafka"""
        try:
            # Decode JSON message
            event = json.loads(message.value().decode('utf-8'))
            
            # Extract sensor type and process accordingly
            sensor_type = event.get('sensor_type')
            
            if sensor_type == 'temperature':
                self.process_temperature_data(event)
            # Add other sensor types here as needed
            
            # Make sure to trigger any producer callbacks
            self.producer.poll(0)
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
    
    def start_processing(self):
        """Start consuming and processing messages"""
        try:
            # Subscribe to topic
            self.consumer.subscribe([self.input_topic])
            logger.info(f"Subscribed to topic: {self.input_topic}")
            logger.info("Waiting for messages... (Press Ctrl+C to exit)")
            
            # Process messages
            topic_error_count = 0
            max_topic_errors = 10  # Allow some errors before giving up

            while True:
                try:
                    # Use shorter timeout for more responsive exits
                    msg = self.consumer.poll(timeout=1.0)
                    
                    if msg is None:
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            # End of partition event - not an error
                            continue
                        elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                            # Topic doesn't exist yet, retry with backoff
                            topic_error_count += 1
                            if topic_error_count <= max_topic_errors:
                                logger.warning(f"Topic {self.input_topic} not available yet. Retrying... ({topic_error_count}/{max_topic_errors})")
                                time.sleep(2)  # Wait before retry
                                continue
                            else:
                                logger.error(f"Topic {self.input_topic} still not available after {max_topic_errors} retries.")
                                break
                        else:
                            logger.error(f"Consumer error: {msg.error()}")
                            break
                    
                    # Reset error count on successful message
                    topic_error_count = 0
                    
                    logger.debug("Received message, processing...")
                    # Process the message
                    self.process_message(msg)
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    time.sleep(1)  # Brief pause before continuing
                    
        except KeyboardInterrupt:
            logger.info("Consumer interrupted")
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
        finally:
            # Close consumer and producer
            logger.info("Closing consumer...")
            self.consumer.close()
            self.producer.flush()

def start_processor_thread():
    """Start processor in a separate thread"""
    processor = IoTDataProcessor(
        bootstrap_servers=KAFKA_BROKER,
        input_topic='iot-sensors',
        output_topic='iot-anomalies',
        group_id='stream-processor'
    )
    
    processor_thread = threading.Thread(
        target=processor.start_processing
    )
    processor_thread.daemon = True
    processor_thread.start()
    logger.info("Processor thread started")
    
    return processor_thread

if __name__ == "__main__":
    processor_thread = start_processor_thread()
    
    try:
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Program interrupted")
