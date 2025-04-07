"""Streamlit dashboard for real-time visualization of streaming data"""
import os
import json
import time
import logging
import datetime
import asyncio
import threading
from typing import Dict, List, Any, Optional

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import websocket

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# API service connection
API_BASE_URL = os.environ.get('API_BASE_URL', 'http://localhost:8000')
WS_BASE_URL = os.environ.get('WS_BASE_URL', 'ws://localhost:8000/ws')

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
    ws = websocket.WebSocketApp(
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

def on_close(ws, close_status_code, close_msg):
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
                
                # Add thresholds if available in anomalies
                anomalies = [a for a in state["anomalies"] if a.get("sensor_id") == sensor_id]
                if anomalies:
                    latest = anomalies[0]
                    min_threshold = latest.get("threshold_min")
                    max_threshold = latest.get("threshold_max")
                    
                    if min_threshold is not None:
                        fig.add_hline(
                            y=min_threshold,
                            line_dash="dash",
                            line_color="blue",
                            annotation_text="Min threshold"
                        )
                        
                    if max_threshold is not None:
                        fig.add_hline(
                            y=max_threshold,
                            line_dash="dash", 
                            line_color="red",
                            annotation_text="Max threshold"
                        )
                
                st.plotly_chart(fig, use_container_width=True)
                
            # Show metadata
            col1, col2 = st.columns(2)
            if "location" in df.columns:
                with col1:
                    st.metric("Location", df["location"].iloc[0] if not df["location"].empty else "Unknown")
            
            if "sensor_type" in df.columns:
                with col2:
                    st.metric("Sensor Type", df["sensor_type"].iloc[0] if not df["sensor_type"].empty else "Unknown")
            
            # Show raw data in expandable section
            with st.expander("Raw Data"):
                st.dataframe(df.head(10))

def create_stock_visualization():
    """Create visualization for stock data"""
    st.header("Stock Market Data")
    
    if not state["stocks"]:
        st.info("No stock data available yet.")
        return
    
    # Create tabs for each stock
    symbols = list(state["stocks"].keys())
    tabs = st.tabs([f"Stock: {symbol}" for symbol in symbols])
    
    for i, symbol in enumerate(symbols):
        with tabs[i]:
            stock_data = state["stocks"][symbol]
            if not stock_data:
                st.info(f"No data for {symbol} yet.")
                continue
                
            # Convert to DataFrame
            df = pd.DataFrame(stock_data)
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                
            # Plot price over time
            if all(col in df.columns for col in ["timestamp", "close"]):
                fig = go.Figure()
                
                # Add price line
                fig.add_trace(
                    go.Scatter(
                        x=df["timestamp"],
                        y=df["close"],
                        mode="lines",
                        name="Price"
                    )
                )
                
                # Add volume as bar chart on secondary y-axis if available
                if "volume" in df.columns:
                    fig.add_trace(
                        go.Bar(
                            x=df["timestamp"],
                            y=df["volume"],
                            name="Volume",
                            opacity=0.3,
                            yaxis="y2"
                        )
                    )
                    
                    # Update layout for dual y-axis
                    fig.update_layout(
                        title=f"{symbol} Price and Volume",
                        yaxis=dict(title="Price ($)"),
                        yaxis2=dict(
                            title="Volume",
                            titlefont=dict(color="#d62728"),
                            tickfont=dict(color="#d62728"),
                            anchor="x",
                            overlaying="y",
                            side="right"
                        )
                    )
                else:
                    fig.update_layout(
                        title=f"{symbol} Price",
                        yaxis=dict(title="Price ($)")
                    )
                
                st.plotly_chart(fig, use_container_width=True)
                
            # Show stats
            if all(col in df.columns for col in ["open", "close", "high", "low"]):
                cols = st.columns(4)
                with cols[0]:
                    last_price = df["close"].iloc[0] if not df["close"].empty else None
                    prev_price = df["close"].iloc[1] if len(df["close"]) > 1 else None
                    
                    if last_price is not None and prev_price is not None:
                        delta = last_price - prev_price
                        st.metric("Last Price", f"${last_price:.2f}", f"{delta:.2f}")
                    else:
                        st.metric("Last Price", f"${last_price:.2f}" if last_price else "N/A")
                        
                with cols[1]:
                    st.metric("Open", f"${df['open'].iloc[0]:.2f}" if not df["open"].empty else "N/A")
                    
                with cols[2]:
                    st.metric("High", f"${df['high'].iloc[0]:.2f}" if not df["high"].empty else "N/A")
                    
                with cols[3]:
                    st.metric("Low", f"${df['low'].iloc[0]:.2f}" if not df["low"].empty else "N/A")
                    
            # Show raw data in expandable section
            with st.expander("Raw Data"):
                st.dataframe(df.head(10))

def create_news_visualization():
    """Create visualization for news data"""
    st.header("Latest News")
    
    if not state["news"]:
        st.info("No news data available yet.")
        return
    
    # Display news items
    for item in state["news"]:
        with st.container():
            st.subheader(item.get("title", "No title"))
            if "snippet" in item:
                st.write(item["snippet"])
            if "url" in item:
                st.write(f"[Read more]({item['url']})")
            st.caption(f"Source: {item.get('source', 'Unknown')} | {item.get('timestamp', '')}")
            st.divider()

def create_anomaly_visualization():
    """Create visualization for anomaly data"""
    st.header("Detected Anomalies")
    
    if not state["anomalies"]:
        st.info("No anomalies detected yet.")
        return
    
    # Create DataFrame for anomalies
    df = pd.DataFrame(state["anomalies"])
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    # Plot anomalies by severity
    if "severity" in df.columns:
        fig = px.bar(
            df.groupby("severity").size().reset_index(name="count"),
            x="severity",
            y="count",
            color="severity",
            title="Anomalies by Severity",
            labels={"count": "Count", "severity": "Severity"},
            color_discrete_map={"high": "red", "medium": "orange", "low": "yellow"}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Table of recent anomalies
    st.subheader("Recent Anomalies")
    
    # Calculate how far off from threshold
    if all(col in df.columns for col in ["value", "threshold_min", "threshold_max"]):
        df["deviation"] = df.apply(
            lambda row: max(row["threshold_min"] - row["value"], 0) + max(row["value"] - row["threshold_max"], 0),
            axis=1
        )
    
    # Display anomalies 
    display_cols = ["timestamp", "sensor_id", "value", "threshold_min", "threshold_max", "severity"]
    if "deviation" in df.columns:
        display_cols.append("deviation")
    
    st.dataframe(df[display_cols], use_container_width=True)

def create_metrics_visualization():
    """Create visualization for processing metrics"""
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
                title="Message Counts by Topic",
                labels={"count": "Count", "topic": "Topic"}
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Processing metrics visualization
    if state["metrics"]:
        st.subheader("Processing Metrics")
        cols = st.columns(len(state["metrics"]))
        
        for i, (key, value) in enumerate(state["metrics"].items()):
            with cols[i]:
                # Format the metric name for display
                formatted_name = key.replace("_", " ").title()
                st.metric(formatted_name, f"{value:.3f}")

def main():
    """Main function to run the Streamlit dashboard"""
    st.set_page_config(
        page_title="Stream Data Dashboard",
        page_icon="🌊",
        layout="wide"
    )
    
    # Set theme and styling
    st.markdown("""
    <style>
    .stApp {
        background-color: #0e1117;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Header section
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title("Stream Data Platform Dashboard")
    with col2:
        st.text(f"Last updated: {state['last_updated'] or 'Never'}")
        
    # Fetch initial data if needed
    if not state["last_updated"]:
        with st.spinner("Fetching initial data..."):
            fetch_initial_data()
    
    # Main content
    tabs = st.tabs(["IoT Sensors", "Stock Data", "News Feed", "Anomalies", "System Metrics"])
    
    with tabs[0]:
        create_iot_visualization()
    
    with tabs[1]:
        create_stock_visualization()
        
    with tabs[2]:
        create_news_visualization()
        
    with tabs[3]:
        create_anomaly_visualization()
        
    with tabs[4]:
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
