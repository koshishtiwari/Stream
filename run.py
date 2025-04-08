#!/usr/bin/env python3
"""Main entry point for the Stream application"""
import time
import os
import argparse
import subprocess
import sys
import logging
import threading
from typing import List, Optional

from src.config import KAFKA_BROKER, MONITORING_CONFIG

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_docker_services() -> bool:
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

def start_component(module: str, function: str, args: List[str] = None) -> Optional[threading.Thread]:
    """Import and start a component in a separate thread"""
    try:
        # Dynamically import the component
        module_path = f"src.{module}"
        module_obj = __import__(module_path, fromlist=[function])
        func = getattr(module_obj, function)
        
        # Start the component in a thread
        thread = threading.Thread(target=func, args=args or [])
        thread.daemon = True
        thread.start()
        
        logger.info(f"Started {module}.{function}")
        return thread
    except ImportError as e:
        logger.error(f"Failed to import {module}.{function}: {e}")
        return None
    except AttributeError as e:
        logger.error(f"Failed to find function {function} in {module}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error starting {module}.{function}: {e}")
        return None

def main():
    """Main entry point for running the Stream application"""
    parser = argparse.ArgumentParser(description='Run Stream application components')
    parser.add_argument('--producer', action='store_true', help='Run the data producers')
    parser.add_argument('--processor', action='store_true', help='Run the data processors')
    parser.add_argument('--api', action='store_true', help='Run the API service')
    parser.add_argument('--dashboard', action='store_true', help='Run the dashboard')
    parser.add_argument('--monitoring', action='store_true', help='Run the monitoring service')
    parser.add_argument('--all', action='store_true', help='Run all components')
    parser.add_argument('--no-docker-check', action='store_true', help='Skip Docker services check')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO',
                       help='Set the logging level')
    
    args = parser.parse_args()
    
    # Configure logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Default to running everything if no args specified
    if not (args.producer or args.processor or args.api or args.dashboard or args.monitoring or args.all):
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
    
    # Start the monitoring service
    if args.monitoring or args.all:
        logger.info("Starting monitoring service...")
        from src.monitoring.metrics import get_collector
        collector = get_collector()
        threads.append(threading.Thread(target=lambda: time.sleep(0.1)))  # Dummy thread as collector runs its own thread
    
    # Start the producer
    if args.producer or args.all:
        logger.info("Starting data producers...")
        thread = start_component("ingestion.producers", "start_producer_thread")
        if thread:
            threads.append(thread)
    
    # Start the processor
    if args.processor or args.all:
        logger.info("Starting data processors...")
        thread = start_component("processing.agents", "start_processor_thread")
        if thread:
            threads.append(thread)
    
    # Start the API service
    if args.api or args.all:
        logger.info("Starting API service...")
        thread = start_component("visualization.api", "start_api_server")
        if thread:
            threads.append(thread)
    
    # Start the dashboard
    if args.dashboard or args.all:
        logger.info("Starting dashboard...")
        thread = start_component("visualization.dashboard", "main")
        if thread:
            threads.append(thread)
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
            # Check if any thread has exited
            alive_threads = [t for t in threads if t.is_alive()]
            if len(alive_threads) < len(threads):
                dead_count = len(threads) - len(alive_threads)
                logger.warning(f"{dead_count} component(s) have exited unexpectedly")
                threads = alive_threads
    except KeyboardInterrupt:
        logger.info("Shutting down...")

if __name__ == "__main__":
    main()
