"""Shared configuration for Stream application"""
import os
import socket
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Try to determine if we're running in Codespaces or similar container environment
def is_codespaces():
    """Check if we're running in GitHub Codespaces"""
    return os.environ.get('CODESPACES') == 'true'

# Kafka configuration with fallbacks for various environments
def get_kafka_broker():
    """Get the appropriate Kafka broker address based on environment"""
    # First check environment variable
    if os.environ.get('KAFKA_BROKER'):
        return os.environ.get('KAFKA_BROKER')
    
    # For Codespaces, try to use localhost
    if is_codespaces():
        return 'localhost:9092'
        
    # Default for Docker networking
    return 'broker:29092'

# Prometheus URL with fallbacks
def get_prometheus_url():
    """Get the appropriate Prometheus URL based on environment"""
    if os.environ.get('PROMETHEUS_URL'):
        return os.environ.get('PROMETHEUS_URL')
    
    if is_codespaces():
        return 'http://localhost:9090'
    
    return 'http://prometheus:9090'

# Grafana URL with fallbacks
def get_grafana_url():
    """Get the appropriate Grafana URL based on environment"""
    if os.environ.get('GRAFANA_URL'):
        return os.environ.get('GRAFANA_URL')
    
    if is_codespaces():
        return 'http://localhost:3000'
    
    return 'http://grafana:3000'

# Export constants
KAFKA_BROKER = get_kafka_broker()
PROMETHEUS_URL = get_prometheus_url()
GRAFANA_URL = get_grafana_url()

# Grafana credentials
GRAFANA_USER = os.environ.get('GRAFANA_USER', 'admin')
GRAFANA_PASSWORD = os.environ.get('GRAFANA_PASSWORD', 'admin')
GRAFANA_API_KEY = os.environ.get('GRAFANA_API_KEY', '')

logger.info(f"Configured Kafka broker: {KAFKA_BROKER}")
logger.info(f"Configured Prometheus URL: {PROMETHEUS_URL}")
logger.info(f"Configured Grafana URL: {GRAFANA_URL}")
