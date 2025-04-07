"""FastAPI service for real-time dashboard data"""
import os
import json
import time
import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

import redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Redis
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB = int(os.environ.get('REDIS_DB', 0))
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

# Initialize FastAPI
app = FastAPI(title="Stream Data API", description="Real-time streaming data API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)

# Models
class DataPoint(BaseModel):
    timestamp: str
    value: float
    source: str
    
class TimeSeriesData(BaseModel):
    key: str
    data: List[DataPoint]
    
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
            stocks = redis_client.smembers("stream:stocks")
            stock_data = {}
            for symbol in stocks:
                # Get the latest 100 stock prices
                prices = redis_client.lrange(f"stream:stock:{symbol}:prices", 0, 99)
                if prices:
                    stock_data[symbol] = [json.loads(p) for p in prices]
            
            data_update["stocks"] = stock_data
            
            # Get news data
            news_data = []
            news_items = redis_client.lrange("stream:news:recent", 0, 9)  # Latest 10 news items
            if news_items:
                news_data = [json.loads(item) for item in news_items]
            
            data_update["news"] = news_data
            
            # Get anomaly data
            anomaly_data = []
            anomalies = redis_client.lrange("stream:anomalies:recent", 0, 19)  # Latest 20 anomalies
            if anomalies:
                anomaly_data = [json.loads(a) for a in anomalies]
                
            data_update["anomalies"] = anomaly_data
            
            # Get processing metrics
            processing_metrics = {}
            metrics = redis_client.hgetall("stream:metrics:processing")
            for key, value in metrics.items():
                processing_metrics[key] = float(value)
                
            data_update["metrics"] = processing_metrics
            
            # Get message counts
            message_counts = {}
            for topic in ["iot-sensors", "stock-data", "news-data", "iot-anomalies"]:
                count = redis_client.get(f"stream:counts:{topic}")
                message_counts[topic] = int(count) if count else 0
                
            data_update["message_counts"] = message_counts
            
            # Broadcast to all connected clients
            if manager.active_connections:
                await manager.broadcast(data_update)
            
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

@app.get("/sensors")
async def get_sensors() -> List[str]:
    """Get list of active sensors"""
    return list(redis_client.smembers("stream:sensors"))

@app.get("/sensor/{sensor_id}/data")
async def get_sensor_data(sensor_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    """Get data for a specific sensor"""
    readings = redis_client.lrange(f"stream:sensor:{sensor_id}:readings", 0, limit - 1)
    return [json.loads(r) for r in readings]

@app.get("/stocks")
async def get_stocks() -> List[str]:
    """Get list of active stocks"""
    return list(redis_client.smembers("stream:stocks"))

@app.get("/stock/{symbol}/data")
async def get_stock_data(symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
    """Get data for a specific stock"""
    prices = redis_client.lrange(f"stream:stock:{symbol}:prices", 0, limit - 1)
    return [json.loads(p) for p in prices]

@app.get("/news")
async def get_news(limit: int = 10) -> List[Dict[str, Any]]:
    """Get recent news items"""
    news_items = redis_client.lrange("stream:news:recent", 0, limit - 1)
    return [json.loads(item) for item in news_items]

@app.get("/anomalies")
async def get_anomalies(limit: int = 20) -> List[Dict[str, Any]]:
    """Get recent anomalies"""
    anomalies = redis_client.lrange("stream:anomalies:recent", 0, limit - 1)
    return [json.loads(a) for a in anomalies]

@app.get("/metrics")
async def get_metrics() -> Dict[str, float]:
    """Get processing metrics"""
    metrics = redis_client.hgetall("stream:metrics:processing")
    return {k: float(v) for k, v in metrics.items()}

@app.get("/message-counts")
async def get_message_counts() -> Dict[str, int]:
    """Get message counts by topic"""
    topics = ["iot-sensors", "stock-data", "news-data", "iot-anomalies"]
    counts = {}
    for topic in topics:
        count = redis_client.get(f"stream:counts:{topic}")
        counts[topic] = int(count) if count else 0
    return counts

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

def start_api_server():
    """Start the API server using uvicorn"""
    import uvicorn
    uvicorn.run("src.visualization.api_service:app", host="0.0.0.0", port=8000, reload=True)

if __name__ == "__main__":
    start_api_server()
