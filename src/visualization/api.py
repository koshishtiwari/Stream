"""FastAPI service for real-time dashboard data"""
import os
import json
import time
import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

import redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from src.config import REDIS_CONFIG, API_CONFIG
from src.monitoring.metrics import get_collector

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Redis
redis_client = redis.Redis(
    host=REDIS_CONFIG['host'], 
    port=REDIS_CONFIG['port'], 
    db=REDIS_CONFIG['db'],
    decode_responses=True
)

# Initialize FastAPI
app = FastAPI(
    title="Stream Data API", 
    description="Real-time streaming data API",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Models
class DataPoint(BaseModel):
    timestamp: str
    value: float
    source: str
    
class SensorData(BaseModel):
    sensor_id: str
    sensor_type: str
    location: str
    value: float
    timestamp: str
    
class AnomalyData(BaseModel):
    timestamp: str
    sensor_id: str
    value: float
    threshold_min: float
    threshold_max: float
    severity: str

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        
    async def broadcast(self, data: Dict[str, Any]):
        for connection in self.active_connections:
            await connection.send_json(data)

manager = ConnectionManager()

# Background task to publish updates
async def publish_updates():
    """Background task to periodically publish data updates to WebSocket clients"""
    while True:
        try:
            # Collect the latest data
            data_update = {}
            
            # Get IoT sensor data
            sensors = redis_client.smembers("stream:sensors")
            iot_data = {}
            for sensor_id in sensors:
                # Get the latest 100 readings
                readings = redis_client.lrange(f"stream:sensor:{sensor_id}:readings", 0, 99)
                if readings:
                    iot_data[sensor_id] = [json.loads(r) for r in readings]
            
            data_update["iot_sensors"] = iot_data
            
            # Get stock data
            stocks = redis_client.smembers("stream:stocks") if redis_client.exists("stream:stocks") else set()
            stock_data = {}
            for symbol in stocks:
                # Get the latest 100 stock prices
                prices = redis_client.lrange(f"stream:stock:{symbol}:prices", 0, 99)
                if prices:
                    stock_data[symbol] = [json.loads(p) for p in prices]
            
            data_update["stocks"] = stock_data
            
            # Get news data
            news_data = []
            if redis_client.exists("stream:news:recent"):
                news_items = redis_client.lrange("stream:news:recent", 0, 9)  # Latest 10 news items
                if news_items:
                    news_data = [json.loads(item) for item in news_items]
            
            data_update["news"] = news_data
            
            # Get anomaly data
            anomaly_data = []
            if redis_client.exists("stream:anomalies:recent"):
                anomalies = redis_client.lrange("stream:anomalies:recent", 0, 19)  # Latest 20 anomalies
                if anomalies:
                    anomaly_data = [json.loads(a) for a in anomalies]
                    
            data_update["anomalies"] = anomaly_data
            
            # Get system metrics
            system_metrics = redis_client.hgetall("stream:metrics:system") if redis_client.exists("stream:metrics:system") else {}
            # Convert string values to float
            system_metrics = {k: float(v) for k, v in system_metrics.items()}
            data_update["metrics"] = system_metrics
            
            # Get message counts
            message_counts = {}
            count_keys = redis_client.keys("stream:counts:*")
            for key in count_keys:
                topic = key.split(":")[-1]
                count = redis_client.get(key)
                message_counts[topic] = int(count) if count else 0
                
            data_update["message_counts"] = message_counts
            
            # Broadcast to all connected clients
            if manager.active_connections:
                await manager.broadcast(data_update)
            
        except redis.RedisError as e:
            logger.error(f"Redis error in publish_updates: {e}")
        except Exception as e:
            logger.error(f"Error in publish_updates: {e}")
        
        # Wait before next update
        await asyncio.sleep(1)  # Update every second

# Endpoints
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Stream Data API",
        "docs": "/docs",
        "status": "running"
    }

@app.get("/health")
async def health():
    """Health check endpoint"""
    try:
        # Check Redis
        redis_ok = redis_client.ping()
        
        # Check Kafka via metrics collector
        collector = get_collector()
        kafka_ok = collector is not None and hasattr(collector, 'admin_client') and collector.admin_client is not None
        
        return {
            "status": "healthy" if redis_ok and kafka_ok else "degraded",
            "redis": "up" if redis_ok else "down",
            "kafka": "up" if kafka_ok else "down",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/sensors")
async def get_sensors() -> List[str]:
    """Get list of active sensors"""
    try:
        return list(redis_client.smembers("stream:sensors"))
    except redis.RedisError as e:
        logger.error(f"Redis error: {e}")
        raise HTTPException(status_code=500, detail="Error accessing Redis")

@app.get("/sensor/{sensor_id}/data")
async def get_sensor_data(sensor_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    """Get data for a specific sensor"""
    try:
        readings = redis_client.lrange(f"stream:sensor:{sensor_id}:readings", 0, limit - 1)
        return [json.loads(r) for r in readings]
    except redis.RedisError as e:
        logger.error(f"Redis error: {e}")
        raise HTTPException(status_code=500, detail="Error accessing Redis")
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Error decoding sensor data")

@app.get("/stocks")
async def get_stocks() -> List[str]:
    """Get list of active stocks"""
    try:
        if redis_client.exists("stream:stocks"):
            return list(redis_client.smembers("stream:stocks"))
        return []
    except redis.RedisError as e:
        logger.error(f"Redis error: {e}")
        raise HTTPException(status_code=500, detail="Error accessing Redis")

@app.get("/stock/{symbol}/data")
async def get_stock_data(symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
    """Get data for a specific stock"""
    try:
        if not redis_client.exists(f"stream:stock:{symbol}:prices"):
            return []
        prices = redis_client.lrange(f"stream:stock:{symbol}:prices", 0, limit - 1)
        return [json.loads(p) for p in prices]
    except redis.RedisError as e:
        logger.error(f"Redis error: {e}")
        raise HTTPException(status_code=500, detail="Error accessing Redis")
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Error decoding stock data")

@app.get("/news")
async def get_news(limit: int = 10) -> List[Dict[str, Any]]:
    """Get recent news items"""
    try:
        if not redis_client.exists("stream:news:recent"):
            return []
        news_items = redis_client.lrange("stream:news:recent", 0, limit - 1)
        return [json.loads(item) for item in news_items]
    except redis.RedisError as e:
        logger.error(f"Redis error: {e}")
        raise HTTPException(status_code=500, detail="Error accessing Redis")
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Error decoding news data")

@app.get("/anomalies")
async def get_anomalies(limit: int = 20) -> List[Dict[str, Any]]:
    """Get recent anomalies"""
    try:
        if not redis_client.exists("stream:anomalies:recent"):
            return []
        anomalies = redis_client.lrange("stream:anomalies:recent", 0, limit - 1)
        return [json.loads(a) for a in anomalies]
    except redis.RedisError as e:
        logger.error(f"Redis error: {e}")
        raise HTTPException(status_code=500, detail="Error accessing Redis")
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Error decoding anomaly data")

@app.get("/metrics")
async def get_metrics() -> Dict[str, float]:
    """Get system metrics"""
    try:
        metrics = redis_client.hgetall("stream:metrics:system")
        return {k: float(v) for k, v in metrics.items()}
    except redis.RedisError as e:
        logger.error(f"Redis error: {e}")
        raise HTTPException(status_code=500, detail="Error accessing Redis")

@app.get("/message-counts")
async def get_message_counts() -> Dict[str, int]:
    """Get message counts by topic"""
    try:
        count_keys = redis_client.keys("stream:counts:*")
        counts = {}
        for key in count_keys:
            topic = key.split(":")[-1]
            count = redis_client.get(key)
            counts[topic] = int(count) if count else 0
        return counts
    except redis.RedisError as e:
        logger.error(f"Redis error: {e}")
        raise HTTPException(status_code=500, detail="Error accessing Redis")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates"""
    await manager.connect(websocket)
    try:
        while True:
            # This keeps the connection alive and handles client messages if any
            data = await websocket.receive_text()
            # We don't process any incoming data for now
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.on_event("startup")
async def startup_event():
    """Start background task on startup"""
    asyncio.create_task(publish_updates())
    # Connect to metrics collector
    get_collector()

def start_api_server():
    """Start the API server using uvicorn"""
    import uvicorn
    host = API_CONFIG['host']
    port = API_CONFIG['port']
    uvicorn.run(
        "src.visualization.api:app", 
        host=host, 
        port=port, 
        reload=True
    )

if __name__ == "__main__":
    start_api_server()
