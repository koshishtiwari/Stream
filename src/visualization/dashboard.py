"""Streamlit dashboard for real-time visualization of streaming data"""
import os
import json
import time
import logging
import datetime
import threading
from typing import Dict, List, Any, Optional

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
from websocket import WebSocketApp  # Using websocket-client package

from src.config import API_CONFIG, UI_CONFIG

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# API service connection
API_BASE_URL = f"http://{API_CONFIG['host']}:{API_CONFIG['port']}"
WS_BASE_URL = f"ws://{API_CONFIG['host']}:{API_CONFIG['port']}/ws"

# Global state to store latest data
state = {
    "iot_sensors": {},
    "stocks": {},
    "news": [],
    "anomalies": [],
    "metrics": {},
    "message_counts": {},
    "last_updated": None
}

def init_websocket():
    """Initialize WebSocket connection for real-time updates"""
    ws = WebSocketApp(
        WS_BASE_URL,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )
    return ws

def on_message(ws, message):
    """Handle incoming WebSocket messages"""
    global state
    try:
        data = json.loads(message)
        # Update state with new data
        for key in data:
            if key in state:
                state[key] = data[key]
        
        # Update last updated timestamp
        state["last_updated"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.error(f"Error processing WebSocket message: {e}")

def on_error(ws, error):
    """Handle WebSocket errors"""
    logger.error(f"WebSocket error: {error}")

def on_close(ws, close_status_code=None, close_msg=None):
    """Handle WebSocket connection close"""
    logger.warning(f"WebSocket connection closed: {close_msg} (code: {close_status_code})")
    # Don't reconnect immediately to avoid hammering the server if it's down
    time.sleep(5)
    
    # Use threading.Timer to avoid blocking the main thread
    reconnect_timer = threading.Timer(1.0, run_websocket)
    reconnect_timer.daemon = True
    reconnect_timer.start()

def on_open(ws):
    """Handle WebSocket connection open"""
    logger.info("WebSocket connection established")

def run_websocket():
    """Run WebSocket client in a separate thread"""
    ws = init_websocket()
    ws.run_forever()

def fetch_initial_data():
    """Fetch initial data from REST API"""
    global state
    
    try:
        # Get sensors data
        response = requests.get(f"{API_BASE_URL}/sensors")
        sensors = response.json()
        
        iot_data = {}
        for sensor in sensors:
            sensor_data = requests.get(f"{API_BASE_URL}/sensor/{sensor}/data")
            iot_data[sensor] = sensor_data.json()
        
        state["iot_sensors"] = iot_data
        
        # Get stocks data
        response = requests.get(f"{API_BASE_URL}/stocks")
        stocks = response.json()
        
        stock_data = {}
        for symbol in stocks:
            stock_data_response = requests.get(f"{API_BASE_URL}/stock/{symbol}/data")
            stock_data[symbol] = stock_data_response.json()
            
        state["stocks"] = stock_data
        
        # Get news data
        news_response = requests.get(f"{API_BASE_URL}/news")
        state["news"] = news_response.json()
        
        # Get anomalies
        anomalies_response = requests.get(f"{API_BASE_URL}/anomalies")
        state["anomalies"] = anomalies_response.json()
        
        # Get metrics
        metrics_response = requests.get(f"{API_BASE_URL}/metrics")
        state["metrics"] = metrics_response.json()
        
        # Get message counts
        counts_response = requests.get(f"{API_BASE_URL}/message-counts")
        state["message_counts"] = counts_response.json()
        
        # Update last updated timestamp
        state["last_updated"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
    except Exception as e:
        logger.error(f"Error fetching initial data: {e}")

def create_iot_visualization():
    """Create visualization for IoT sensor data"""
    st.header("IoT Sensors")
    
    if not state["iot_sensors"]:
        st.info("No IoT sensor data available yet.")
        return
    
    # Create tabs for each sensor
    sensor_ids = list(state["iot_sensors"].keys())
    tabs = st.tabs([f"Sensor: {sensor_id}" for sensor_id in sensor_ids])
    
    for i, sensor_id in enumerate(sensor_ids):
        with tabs[i]:
            sensor_data = state["iot_sensors"][sensor_id]
            if not sensor_data:
                st.info(f"No data for sensor {sensor_id} yet.")
                continue
                
            # Convert to DataFrame
            df = pd.DataFrame(sensor_data)
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                
            # Plot sensor values over time
            if "value" in df.columns and "timestamp" in df.columns:
                fig = px.line(
                    df, 
                    x="timestamp", 
                    y="value", 
                    title=f"Temperature readings - {sensor_id}",
                    labels={"value": "Temperature (°C)", "timestamp": "Time"}
                )
                st.plotly_chart(fig, use_container_width=True)
                
            # Show metadata
            col1, col2 = st.columns(2)
            if "location" in df.columns and not df["location"].empty:
                with col1:
                    st.metric("Location", df["location"].iloc[0])
            
            if "sensor_type" in df.columns and not df["sensor_type"].empty:
                with col2:
                    st.metric("Sensor Type", df["sensor_type"].iloc[0])

def create_metrics_visualization():
    """Create visualization for system metrics"""
    st.header("System Metrics")
    
    # Message counts visualization
    if state["message_counts"]:
        st.subheader("Message Counts by Topic")
        counts_df = pd.DataFrame([
            {"topic": topic, "count": count} 
            for topic, count in state["message_counts"].items()
        ])
        
        if not counts_df.empty:
            fig = px.bar(
                counts_df,
                x="topic",
                y="count",
                color="topic",
                title="Message Counts by Topic"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # System metrics visualization
    if state["metrics"]:
        st.subheader("Processing Metrics")
        cols = st.columns(len(state["metrics"]))
        
        for i, (key, value) in enumerate(state["metrics"].items()):
            with cols[i]:
                formatted_name = key.replace("_", " ").title()
                st.metric(formatted_name, f"{value:.2f}")

def main():
    """Main function to run the Streamlit dashboard"""
    st.set_page_config(
        page_title="Stream Data Dashboard",
        page_icon="🌊",
        layout="wide"
    )
    
    # Header section
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title("Stream Data Platform")
    with col2:
        st.text(f"Last updated: {state['last_updated'] or 'Never'}")
        
    # Fetch initial data if needed
    if not state["last_updated"]:
        with st.spinner("Fetching initial data..."):
            fetch_initial_data()
    
    # Main content
    tabs = st.tabs(["IoT Sensors", "System Metrics"])
    
    with tabs[0]:
        create_iot_visualization()
        
    with tabs[1]:
        create_metrics_visualization()
        
    # Start WebSocket connection in background thread
    if "websocket_started" not in st.session_state:
        thread = threading.Thread(target=run_websocket)
        thread.daemon = True
        thread.start()
        st.session_state.websocket_started = True
    
    # Auto-refresh
    time.sleep(1)
    st.experimental_rerun()

if __name__ == "__main__":
    main()
