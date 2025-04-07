#!/usr/bin/env python3
"""Script to fix Kafka JMX metrics for Prometheus in Docker"""
import os
import subprocess
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_kafka_jmx_metrics():
    """
    Configure Kafka to expose JMX metrics on port 9101 for Prometheus to scrape
    """
    logger.info("Setting up Kafka JMX metrics for Prometheus...")
    
    try:
        # First, check if broker is running
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=broker", "--format", "{{.Names}}"],
            check=True, capture_output=True, text=True
        )
        
        if "broker" not in result.stdout:
            logger.error("Kafka broker container not found. Is Docker running?")
            return False
        
        # Set the JMX_PORT environment variable in the container
        logger.info("Setting JMX_PORT environment variable in the broker container...")
        subprocess.run(
            ["docker", "exec", "broker", "bash", "-c", "export JMX_PORT=9101"],
            check=True
        )
        
        # Modify the JVM options for Kafka to expose JMX
        logger.info("Setting up JMX options for Kafka broker...")
        jmx_opts = (
            "-Dcom.sun.management.jmxremote "
            "-Dcom.sun.management.jmxremote.authenticate=false "
            "-Dcom.sun.management.jmxremote.ssl=false "
            "-Dcom.sun.management.jmxremote.port=9101 "
            "-Dcom.sun.management.jmxremote.rmi.port=9101 "
            "-Djava.rmi.server.hostname=localhost"
        )
        
        subprocess.run(
            ["docker", "exec", "broker", "bash", "-c", f"export KAFKA_JMX_OPTS='{jmx_opts}'"],
            check=True
        )
        
        # Restart the Kafka broker to apply changes
        logger.info("Restarting Kafka broker container...")
        subprocess.run(["docker", "restart", "broker"], check=True)
        
        # Wait for the broker to restart
        logger.info("Waiting for Kafka broker to restart...")
        time.sleep(10)
        
        logger.info("Kafka JMX metrics setup complete. Prometheus should now be able to scrape Kafka metrics.")
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Error setting up Kafka JMX metrics: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False

if __name__ == "__main__":
    setup_kafka_jmx_metrics()
    logger.info("Run 'docker-compose restart prometheus' to ensure Prometheus picks up the changes.")
