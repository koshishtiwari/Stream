"""Grafana setup and configuration for monitoring Kafka streams"""
import os
import time
import json
import logging
import requests
from typing import Dict, Any, List
from src.config import GRAFANA_URL, PROMETHEUS_URL, GRAFANA_USER, GRAFANA_PASSWORD, GRAFANA_API_KEY

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GrafanaSetup:
    """Setup and configure Grafana dashboards for Stream monitoring"""
    
    def __init__(self):
        self.headers = {
            "Content-Type": "application/json",
        }
        
        # Use API key if provided, otherwise use basic auth
        if GRAFANA_API_KEY:
            self.headers["Authorization"] = f"Bearer {GRAFANA_API_KEY}"
            self.auth = None
            logger.info("Using Grafana API key for authentication")
        else:
            self.auth = (GRAFANA_USER, GRAFANA_PASSWORD)
            logger.info(f"Using Grafana basic auth with user: {GRAFANA_USER}")
    
    def wait_for_grafana(self, max_retries=30, delay=2):
        """Wait for Grafana to be available"""
        logger.info(f"Waiting for Grafana to be available at {GRAFANA_URL}...")
        
        for i in range(max_retries):
            try:
                response = requests.get(f"{GRAFANA_URL}/api/health", timeout=5)
                if response.status_code == 200:
                    logger.info("Grafana is available!")
                    return True
            except requests.RequestException as e:
                logger.debug(f"Request failed: {e}")
            
            logger.info(f"Waiting for Grafana... ({i+1}/{max_retries})")
            time.sleep(delay)
        
        logger.error("Grafana is not available. Setup failed.")
        return False
    
    def create_data_source(self):
        """Create Prometheus data source in Grafana"""
        logger.info(f"Creating Prometheus data source with URL: {PROMETHEUS_URL}")
        
        payload = {
            "name": "Prometheus",
            "type": "prometheus",
            "url": PROMETHEUS_URL,
            "access": "proxy",
            "isDefault": True
        }
        
        try:
            response = requests.post(
                f"{GRAFANA_URL}/api/datasources",
                headers=self.headers,
                auth=self.auth,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info("Data source created successfully!")
                return True
            else:
                # Check if it failed because it already exists
                if "data source with the same name already exists" in response.text:
                    logger.info("Data source already exists.")
                    return True
                else:
                    logger.error(f"Failed to create data source: {response.text}")
                    return False
        except requests.RequestException as e:
            logger.error(f"Request exception while creating data source: {e}")
            return False
    
    def create_iot_dashboard(self):
        """Create IoT monitoring dashboard"""
        logger.info("Creating IoT monitoring dashboard...")
        
        # Dashboard JSON model
        dashboard = {
            "dashboard": {
                "id": None,
                "title": "IoT Stream Monitoring",
                "tags": ["kafka", "iot", "stream"],
                "timezone": "browser",
                "schemaVersion": 16,
                "version": 0,
                "refresh": "10s",
                "panels": [
                    # Temperature panel
                    {
                        "id": 1,
                        "title": "Temperature Readings",
                        "type": "graph",
                        "datasource": "Prometheus",
                        "targets": [
                            {
                                "expr": "stream_temperature_celsius",
                                "legendFormat": "{{sensor_id}} - {{location}}"
                            }
                        ],
                        "gridPos": {
                            "h": 8,
                            "w": 12,
                            "x": 0,
                            "y": 0
                        }
                    },
                    # Message count panel
                    {
                        "id": 2,
                        "title": "Message Count",
                        "type": "graph",
                        "datasource": "Prometheus",
                        "targets": [
                            {
                                "expr": "stream_messages_total",
                                "legendFormat": "{{topic}} - {{sensor_id}}"
                            }
                        ],
                        "gridPos": {
                            "h": 8,
                            "w": 12,
                            "x": 12,
                            "y": 0
                        }
                    },
                    # Anomaly panel
                    {
                        "id": 3,
                        "title": "Anomalies Detected",
                        "type": "graph",
                        "datasource": "Prometheus",
                        "targets": [
                            {
                                "expr": "stream_anomalies_total",
                                "legendFormat": "{{sensor_id}} - {{severity}}"
                            }
                        ],
                        "gridPos": {
                            "h": 8,
                            "w": 24,
                            "x": 0,
                            "y": 8
                        }
                    }
                ]
            },
            "overwrite": True
        }
        
        try:
            response = requests.post(
                f"{GRAFANA_URL}/api/dashboards/db",
                headers=self.headers,
                auth=self.auth,
                json=dashboard,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info("Dashboard created successfully!")
                return True
            else:
                logger.error(f"Failed to create dashboard: {response.text}")
                return False
        except requests.RequestException as e:
            logger.error(f"Request exception while creating dashboard: {e}")
            return False

def setup_grafana():
    """Main function to setup Grafana"""
    grafana = GrafanaSetup()
    
    if not grafana.wait_for_grafana():
        return False
    
    if not grafana.create_data_source():
        return False
    
    if not grafana.create_iot_dashboard():
        return False
    
    logger.info("Grafana setup completed successfully!")
    logger.info(f"Access Grafana at {GRAFANA_URL} (user: {GRAFANA_USER}, password: {GRAFANA_PASSWORD})")
    return True

if __name__ == "__main__":
    setup_grafana()
