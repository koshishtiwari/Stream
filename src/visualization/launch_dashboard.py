#!/usr/bin/env python3
"""Script to launch the dashboard components"""
import os
import time
import argparse
import subprocess
import logging
import threading

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def start_api_server():
    """Start the FastAPI server"""
    logger.info("Starting API server...")
    process = subprocess.Popen([
        "python", "-m", "uvicorn", 
        "src.visualization.api_service:app", 
        "--host", "0.0.0.0", 
        "--port", "8000",
        "--reload"
    ])
    
    # Give the API server time to start
    time.sleep(3)
    logger.info("API server running at http://localhost:8000")
    return process

def start_streamlit_dashboard():
    """Start the Streamlit dashboard"""
    logger.info("Starting Streamlit dashboard...")
    
    # Try ports in sequence until we find one that works
    ports = [8501, 8502, 8503, 8504, 8505]
    
    for port in ports:
        try:
            process = subprocess.Popen([
                "streamlit", "run", 
                "src/visualization/dashboard.py",
                "--server.port", str(port), 
                "--browser.serverAddress", "localhost"
            ])
            
            # Wait a bit to see if process exits due to port conflict
            time.sleep(2)
            if process.poll() is None:  # None means process is still running
                logger.info(f"Streamlit dashboard running at http://localhost:{port}")
                return process
            else:
                logger.warning(f"Port {port} is not available, trying next port...")
        except Exception as e:
            logger.error(f"Error starting Streamlit on port {port}: {e}")
    
    logger.error(f"Could not start Streamlit dashboard: all ports {ports} are in use")
    return None

def start_metrics_collector():
    """Start the Redis metrics collector"""
    logger.info("Starting Redis metrics collector...")
    process = subprocess.Popen([
        "python", "-m", "src.visualization.metrics_collector"
    ])
    logger.info("Redis metrics collector started")
    return process

def check_redis():
    """Check if Redis is running"""
    import redis
    
    try:
        redis_host = os.environ.get('REDIS_HOST', 'localhost')
        redis_port = int(os.environ.get('REDIS_PORT', 6379))
        
        # Try to connect to Redis
        redis_client = redis.Redis(host=redis_host, port=redis_port)
        redis_client.ping()
        logger.info("Redis is running")
        return True
    except redis.ConnectionError:
        logger.error("Cannot connect to Redis. Is it running?")
        return False
    except Exception as e:
        logger.error(f"Error checking Redis: {e}")
        return False

def main():
    """Main function to launch the dashboard"""
    parser = argparse.ArgumentParser(description="Launch the Stream data dashboard")
    parser.add_argument('--no-redis-check', action='store_true', help='Skip Redis connection check')
    args = parser.parse_args()
    
    # Check if Redis is running (unless skipped)
    if not args.no_redis_check:
        if not check_redis():
            logger.info("Starting Redis container...")
            try:
                subprocess.run(["docker", "start", "redis"], check=True)
                logger.info("Redis container started")
                time.sleep(2)  # Give Redis time to start
            except subprocess.CalledProcessError:
                logger.warning("Could not start Redis container. Make sure Docker is running.")
                logger.info("Trying to launch Redis container...")
                try:
                    subprocess.run(["docker-compose", "up", "-d", "redis"], check=True)
                    logger.info("Redis container launched")
                    time.sleep(2)  # Give Redis time to start
                except subprocess.CalledProcessError:
                    logger.error("Failed to start Redis. Please start it manually.")
                    return
                except FileNotFoundError:
                    logger.error("docker-compose not found. Please install Docker Compose or start Redis manually.")
                    return
    
    # Store process handles so they can be properly managed
    processes = []
    
    # Start components in order
    metrics_process = start_metrics_collector()
    api_process = start_api_server()
    streamlit_process = start_streamlit_dashboard()
    
    processes = [p for p in [metrics_process, api_process, streamlit_process] if p is not None]
    
    # Keep the main process alive
    try:
        while all(p.poll() is None for p in processes if p is not None):
            time.sleep(1)
        logger.warning("One of the dashboard components has stopped unexpectedly")
    except KeyboardInterrupt:
        logger.info("Stopping dashboard components...")
        for process in processes:
            if process is not None:
                process.terminate()
                process.wait()
        logger.info("All components stopped")

if __name__ == "__main__":
    main()
