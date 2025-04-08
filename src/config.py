"""Centralized configuration for Stream application"""
import os
import logging
from typing import Dict, Any

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment detection
def is_codespaces() -> bool:
    """Check if we're running in GitHub Codespaces"""
    return os.environ.get('CODESPACES') == 'true'

def is_docker() -> bool:
    """Check if we're running in Docker"""
    return os.path.exists('/.dockerenv')

# Kafka configuration
def get_kafka_broker() -> str:
    """Get the appropriate Kafka broker address based on environment"""
    if os.environ.get('KAFKA_BROKER'):
        return os.environ.get('KAFKA_BROKER')
    
    if is_codespaces():
        return 'localhost:9092'
        
    # Default for Docker networking
    return 'broker:29092'

# Redis configuration
def get_redis_config() -> Dict[str, Any]:
    """Get Redis configuration based on environment"""
    return {
        'host': os.environ.get('REDIS_HOST', 'localhost'),
        'port': int(os.environ.get('REDIS_PORT', 6379)),
        'db': int(os.environ.get('REDIS_DB', 0))
    }

# API configuration
def get_api_config() -> Dict[str, Any]:
    """Get API service configuration"""
    return {
        'host': os.environ.get('API_HOST', '0.0.0.0'),
        'port': int(os.environ.get('API_PORT', 8000))
    }

# UI configuration
def get_ui_config() -> Dict[str, Any]:
    """Get UI configuration"""
    return {
        'host': os.environ.get('DASHBOARD_HOST', 'localhost'),
        'port': int(os.environ.get('DASHBOARD_PORT', 8501))
    }

# Monitoring configuration
def get_monitoring_config() -> Dict[str, Any]:
    """Get monitoring configuration"""
    return {
        'prometheus_port': int(os.environ.get('PROMETHEUS_PORT', 9090)),  # Changed from 8000 to 9090
        'grafana_url': os.environ.get('GRAFANA_URL', 'http://localhost:3000'),
        'grafana_user': os.environ.get('GRAFANA_USER', 'admin'),
        'grafana_password': os.environ.get('GRAFANA_PASSWORD', 'admin')
    }

# Export constants
KAFKA_BROKER = get_kafka_broker()
REDIS_CONFIG = get_redis_config()
API_CONFIG = get_api_config()
UI_CONFIG = get_ui_config()
MONITORING_CONFIG = get_monitoring_config()

# Data source settings
ALPACA_API_KEY = os.environ.get('ALPACA_API_KEY', '')
ALPACA_API_SECRET = os.environ.get('ALPACA_API_SECRET', '')
GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY', '')

# Log configuration details
logger.info(f"Kafka broker: {KAFKA_BROKER}")
logger.info(f"Redis: {REDIS_CONFIG['host']}:{REDIS_CONFIG['port']}")
logger.info(f"API server: {API_CONFIG['host']}:{API_CONFIG['port']}")
logger.info(f"Dashboard: {UI_CONFIG['host']}:{UI_CONFIG['port']}")
