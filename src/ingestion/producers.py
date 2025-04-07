"""Kafka producers for streaming data"""
import json
import time
import threading
import socket
import logging
from typing import Dict, Any

from confluent_kafka import Producer
from src.data_sources.iot import IoTSensor, TemperatureSensor, IoTDataSource
from src.data_sources.alpaca import AlpacaDataSource  # Import new Alpaca data source
from src.data_sources.Search import GeminiSearchDataSource  # Import updated Search data source
from src.config import KAFKA_BROKER

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IoTProducer:
    """Producer for IoT sensor data"""
    def __init__(self, bootstrap_servers: str, topic: str, data_source: IoTDataSource):
        self.topic = topic
        self.data_source = data_source
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': socket.gethostname(),
            'socket.timeout.ms': 10000,  # Increase timeouts for Codespaces
            'message.timeout.ms': 10000,
            'security.protocol': 'PLAINTEXT',  # Explicitly set security protocol
            'retry.backoff.ms': 500,  # Add retry backoff time
            'request.timeout.ms': 20000  # Increase request timeout
        }
        logger.info(f"Connecting to Kafka broker at: {bootstrap_servers}")
        self.producer = Producer(self.producer_config)
        
    def delivery_report(self, err, msg):
        """Callback invoked on message delivery success or failure"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
        
    def start_streaming(self, interval: float = 1.0, run_forever: bool = True):
        """
        Start streaming data to Kafka
        
        Args:
            interval: Time between readings in seconds
            run_forever: Whether to run in an infinite loop
        """
        try:
            count = 0
            for reading in self.data_source.stream(interval=interval):
                # Convert reading to JSON
                message_value = json.dumps(reading).encode('utf-8')
                
                # Send the reading to Kafka
                self.producer.produce(
                    topic=self.topic,
                    value=message_value,
                    key=reading.get('sensor_id', '').encode('utf-8'),
                    callback=self.delivery_report
                )
                
                # For debugging
                logger.debug(f"Sent: {reading}")
                
                # Trigger any available delivery callbacks
                self.producer.poll(0)
                
                count += 1
                if not run_forever and count >= 10:  # For testing
                    break
                
        except KeyboardInterrupt:
            logger.info("Producer interrupted")
        except Exception as e:
            logger.error(f"Error in producer: {e}", exc_info=True)
        finally:
            # Wait for any outstanding messages to be delivered
            logger.info("Flushing producer...")
            self.producer.flush()

# New producer for stock data
class StockDataProducer:
    """Producer for stock market data"""
    def __init__(self, bootstrap_servers: str, topic: str, data_source: AlpacaDataSource):
        self.topic = topic
        self.data_source = data_source
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': f"{socket.gethostname()}-stock-producer",
            'socket.timeout.ms': 10000,
            'message.timeout.ms': 10000,
            'security.protocol': 'PLAINTEXT',
            'retry.backoff.ms': 500,
            'request.timeout.ms': 20000
        }
        logger.info(f"Connecting to Kafka broker at: {bootstrap_servers}")
        self.producer = Producer(self.producer_config)
        
    def delivery_report(self, err, msg):
        """Callback invoked on message delivery success or failure"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
    
    def stream_real_time_data(self, symbols: list, run_forever: bool = True):
        """Stream real-time stock data to Kafka"""
        logger.info(f"Starting real-time stock data stream for: {symbols}")
        
        try:
            count = 0
            for bar in self.data_source.stream_real_time_data(symbols):
                # Format the stock data for Kafka
                message_value = json.dumps(self.data_source.format_for_kafka(bar)).encode('utf-8')
                
                # Send to Kafka
                self.producer.produce(
                    topic=self.topic,
                    value=message_value,
                    key=bar.get('symbol', '').encode('utf-8'),
                    callback=self.delivery_report
                )
                
                # For debugging
                logger.debug(f"Sent stock data: {bar.get('symbol')}")
                
                # Trigger any available delivery callbacks
                self.producer.poll(0)
                
                count += 1
                if not run_forever and count >= 10:  # For testing
                    break
                    
        except KeyboardInterrupt:
            logger.info("Stock data producer interrupted")
        except Exception as e:
            logger.error(f"Error in stock producer: {e}", exc_info=True)
        finally:
            # Wait for any outstanding messages to be delivered
            logger.info("Flushing producer...")
            self.producer.flush()

# New producer for news data
class NewsProducer:
    """Producer for news data from online searches"""
    def __init__(self, bootstrap_servers: str, topic: str, data_source: GeminiSearchDataSource):
        self.topic = topic
        self.data_source = data_source
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': f"{socket.gethostname()}-news-producer",
            'socket.timeout.ms': 10000,
            'message.timeout.ms': 10000,
            'security.protocol': 'PLAINTEXT',
            'retry.backoff.ms': 500,
            'request.timeout.ms': 20000
        }
        logger.info(f"Connecting to Kafka broker at: {bootstrap_servers}")
        self.producer = Producer(self.producer_config)
        
    def delivery_report(self, err, msg):
        """Callback invoked on message delivery success or failure"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
    
    def monitor_topics(self, topics: list, interval: int = 3600, run_forever: bool = True):
        """Monitor news topics and send updates to Kafka"""
        logger.info(f"Starting news monitoring for topics: {topics} at {interval}s interval")
        
        try:
            count = 0
            while True:
                for topic in topics:
                    news_items = self.data_source.search_news(topic)
                    
                    for item in news_items:
                        # Format the news data for Kafka
                        message_value = json.dumps(self.data_source.format_for_kafka(item)).encode('utf-8')
                        
                        # Send to Kafka
                        self.producer.produce(
                            topic=self.topic,
                            value=message_value,
                            key=topic.encode('utf-8'),
                            callback=self.delivery_report
                        )
                        
                    logger.info(f"Sent {len(news_items)} news items for topic '{topic}'")
                    
                    # Trigger any available delivery callbacks
                    self.producer.poll(0)
                
                count += 1
                if not run_forever and count >= 1:  # For testing
                    break
                    
                time.sleep(interval)
                    
        except KeyboardInterrupt:
            logger.info("News producer interrupted")
        except Exception as e:
            logger.error(f"Error in news producer: {e}", exc_info=True)
        finally:
            # Wait for any outstanding messages to be delivered
            logger.info("Flushing producer...")
            self.producer.flush()
            
def start_producer_thread():
    """Start producer in a separate thread"""
    # Create some sample sensors
    sensors = [
        TemperatureSensor("temp-001", "datacenter-a"),
        TemperatureSensor("temp-002", "datacenter-b"),
        TemperatureSensor("temp-003", "datacenter-c", min_temp=20.0, max_temp=40.0),
    ]
    
    # Create data source with these sensors
    data_source = IoTDataSource(sensors)
    
    # Create and start the producer using the config from src/config.py
    producer = IoTProducer(
        bootstrap_servers=KAFKA_BROKER,
        topic='iot-sensors',
        data_source=data_source
    )
    
    # Start in a thread
    producer_thread = threading.Thread(
        target=producer.start_streaming, 
        kwargs={'interval': 2.0, 'run_forever': True}
    )
    producer_thread.daemon = True
    producer_thread.start()
    logger.info("IoT producer thread started")
    
    # Start stock data producer if API keys are available
    try:
        alpaca_source = AlpacaDataSource()
        stock_producer = StockDataProducer(
            bootstrap_servers=KAFKA_BROKER,
            topic='stock-data',
            data_source=alpaca_source
        )
        
        # Start in a thread with popular tech stocks
        stock_thread = threading.Thread(
            target=stock_producer.stream_real_time_data,
            kwargs={'symbols': ["AAPL", "MSFT", "GOOGL", "AMZN", "META"], 'run_forever': True}
        )
        stock_thread.daemon = True
        stock_thread.start()
        logger.info("Stock data producer thread started")
    except Exception as e:
        logger.warning(f"Could not start stock data producer: {e}")
        
    # Start news producer if API key is available
    try:
        news_source = GeminiSearchDataSource()
        news_producer = NewsProducer(
            bootstrap_servers=KAFKA_BROKER,
            topic='news-data',
            data_source=news_source
        )
        
        # Start in a thread with interesting topics
        news_thread = threading.Thread(
            target=news_producer.monitor_topics,
            kwargs={'topics': ["AI technology", "climate change", "financial markets"], 'interval': 3600, 'run_forever': True}
        )
        news_thread.daemon = True
        news_thread.start()
        logger.info("News producer thread started")
    except Exception as e:
        logger.warning(f"Could not start news producer: {e}")
    
    return producer_thread

if __name__ == "__main__":
    producer_thread = start_producer_thread()
    
    try:
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Program interrupted")
