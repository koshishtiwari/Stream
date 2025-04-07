#!/usr/bin/env python3
"""Script to run all components of the Stream application"""
import time
import os
import argparse
import threading
import subprocess
import sys
import logging
from src.config import KAFKA_BROKER

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_docker_services():
    """Check if the required Docker services are running"""
    logger.info("Checking if Docker services are running...")
    
    try:
        # Check if docker-compose is available
        result = subprocess.run(
            ["docker", "ps"],
            check=True,
            capture_output=True,
            text=True
        )
        
        if "broker" not in result.stdout:
            logger.warning("Kafka broker is not running! Starting Docker services...")
            try:
                subprocess.run(
                    ["docker-compose", "up", "-d"],
                    check=True,
                )
                logger.info("Docker services started. Waiting for Kafka to be ready...")
                time.sleep(15)  # Wait for services to initialize
            except subprocess.CalledProcessError:
                logger.error("Failed to start Docker services. Please run 'docker-compose up -d' manually.")
                return False
            except FileNotFoundError:
                logger.error("docker-compose command not found. Please install Docker Compose or start services manually.")
                return False
        else:
            logger.info("Docker services are already running")
        
        return True
    except subprocess.CalledProcessError:
        logger.error("Failed to check Docker services. Make sure Docker is running.")
        return False
    except FileNotFoundError:
        logger.error("Docker command not found. Please install Docker or start services manually.")
        return False

def start_producer_thread():
    """Import and start producer in a separate thread"""
    try:
        # Dynamic import to avoid circular imports
        from src.ingestion.producers import start_producer_thread as _start_producer
        return _start_producer()
    except ImportError as e:
        logger.error(f"Failed to import producer: {e}")
        return None

def start_processor_thread():
    """Import and start processor in a separate thread"""
    try:
        # Dynamic import to avoid circular imports
        from src.processing.agents import start_processor_thread as _start_processor
        return _start_processor()
    except ImportError as e:
        logger.error(f"Failed to import processor: {e}")
        return None

def start_metrics_exporter():
    """Import and start metrics exporter"""
    try:
        # Dynamic import to avoid circular imports
        from src.visualization.prometheus_exporter import start_metrics_exporter as _start_metrics
        return _start_metrics(port=8000)  # Use port 8000 for metrics
    except ImportError as e:
        logger.error(f"Failed to import metrics exporter: {e}")
        return None

def setup_grafana():
    """Import and setup Grafana"""
    try:
        # Dynamic import to avoid circular imports
        from src.visualization.grafana_setup import setup_grafana as _setup_grafana
        return _setup_grafana()
    except ImportError as e:
        logger.error(f"Failed to import Grafana setup: {e}")
        return None

def main():
    """Main entry point for running the Stream application"""
    parser = argparse.ArgumentParser(description='Run Stream application components')
    parser.add_argument('--producer', action='store_true', help='Run the IoT data producer')
    parser.add_argument('--processor', action='store_true', help='Run the data processor')
    parser.add_argument('--metrics', action='store_true', help='Run the Prometheus metrics exporter')
    parser.add_argument('--grafana', action='store_true', help='Setup Grafana dashboards')
    parser.add_argument('--all', action='store_true', help='Run all components')
    parser.add_argument('--no-docker-check', action='store_true', help='Skip Docker services check')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO',
                       help='Set the logging level')
    
    args = parser.parse_args()
    
    # Configure logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Default to running everything if no args specified
    if not (args.producer or args.processor or args.metrics or args.grafana or args.all):
        args.all = True
    
    # Check if Docker services are running (unless skipped)
    if not args.no_docker_check:
        if not check_docker_services():
            logger.warning("WARNING: Docker services might not be running correctly.")
            answer = input("Continue anyway? (y/n): ").strip().lower()
            if answer != 'y':
                logger.info("Exiting...")
                sys.exit(1)
    
    # Print Kafka broker information
    logger.info(f"Using Kafka broker: {KAFKA_BROKER}")
    
    threads = []
    
    # Start the producer
    if args.producer or args.all:
        logger.info("Starting IoT data producer...")
        producer_thread = start_producer_thread()
        if producer_thread:
            threads.append(producer_thread)
    
    # Start the processor
    if args.processor or args.all:
        logger.info("Starting data processor...")
        processor_thread = start_processor_thread()
        if processor_thread:
            threads.append(processor_thread)
    
    # Start the metrics exporter
    if args.metrics or args.all:
        logger.info("Starting Prometheus metrics exporter...")
        start_metrics_exporter()
    
    # Setup Grafana
    if args.grafana or args.all:
        logger.info("Setting up Grafana dashboards...")
        setup_grafana()
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")

if __name__ == "__main__":
    main()
