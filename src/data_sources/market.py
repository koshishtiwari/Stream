"""Market Data Source using Alpaca API"""
import os
import json
import time
import logging
from typing import Dict, Generator, List, Any, Optional
from datetime import datetime, timedelta

import pandas as pd
from alpaca.data import StockHistoricalDataClient, StockBarsRequest
from alpaca.data.live import StockDataStream
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass

from src.config import ALPACA_API_KEY, ALPACA_API_SECRET

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MarketDataSource:
    """Data source for market data using Alpaca API"""
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        """
        Initialize Alpaca data source
        
        Args:
            api_key: Alpaca API key (will use env var ALPACA_API_KEY if not provided)
            api_secret: Alpaca API secret (will use env var ALPACA_API_SECRET if not provided)
        """
        self.api_key = api_key or ALPACA_API_KEY
        self.api_secret = api_secret or ALPACA_API_SECRET
        
        if not self.api_key or not self.api_secret:
            raise ValueError("Alpaca API credentials not found. Set ALPACA_API_KEY and ALPACA_API_SECRET environment variables or provide them as parameters.")
        
        # Initialize clients
        self.historical_client = StockHistoricalDataClient(self.api_key, self.api_secret)
        self.trading_client = TradingClient(self.api_key, self.api_secret)
        self.stream_client = StockDataStream(self.api_key, self.api_secret)
        
        logger.info("Market data source initialized")
    
    def get_available_stocks(self, status: str = "active", asset_class: str = "us_equity") -> List[Dict[str, Any]]:
        """
        Get available stocks from Alpaca
        
        Args:
            status: Asset status filter ('active' or 'inactive')
            asset_class: Asset class filter ('us_equity', 'crypto', etc.)
            
        Returns:
            List of stock symbols with metadata
        """
        request = GetAssetsRequest(status=status, asset_class=AssetClass(asset_class))
        assets = self.trading_client.get_all_assets(request)
        
        stock_list = []
        for asset in assets:
            stock_list.append({
                "symbol": asset.symbol,
                "name": asset.name,
                "exchange": asset.exchange,
                "tradable": asset.tradable
            })
        
        logger.info(f"Retrieved {len(stock_list)} {status} {asset_class} assets")
        return stock_list
    
    def get_historical_data(self, symbols: List[str], start_date: datetime, 
                          end_date: Optional[datetime] = None, 
                          timeframe: str = "1Day") -> Dict[str, pd.DataFrame]:
        """
        Get historical price data for symbols
        
        Args:
            symbols: List of stock symbols
            start_date: Start date for historical data
            end_date: End date for historical data (defaults to current date)
            timeframe: Bar timeframe ('1Min', '5Min', '15Min', '1Hour', '1Day', '1Week', '1Month')
            
        Returns:
            Dictionary of DataFrames with historical data, keyed by symbol
        """
        # Set default end_date to today if not provided
        if not end_date:
            end_date = datetime.now()
        
        # Map string timeframe to TimeFrame enum
        timeframe_map = {
            '1Min': TimeFrame.Minute,
            '5Min': TimeFrame.Minute(5),
            '15Min': TimeFrame.Minute(15),
            '1Hour': TimeFrame.Hour,
            '1Day': TimeFrame.Day,
            '1Week': TimeFrame.Week,
            '1Month': TimeFrame.Month
        }
        
        try:
            tf = timeframe_map.get(timeframe, TimeFrame.Day)
            
            request = StockBarsRequest(
                symbol_or_symbols=symbols,
                timeframe=tf,
                start=start_date,
                end=end_date
            )
            
            bars = self.historical_client.get_stock_bars(request)
            
            # Convert to dictionary of DataFrames
            result = {}
            for symbol in symbols:
                if symbol in bars:
                    result[symbol] = bars[symbol].df
            
            logger.info(f"Retrieved historical data for {len(result)} symbols from {start_date} to {end_date}")
            return result
            
        except Exception as e:
            logger.error(f"Error getting historical data: {e}")
            return {}
    
    def stream_real_time_data(self, symbols: List[str]) -> Generator[Dict[str, Any], None, None]:
        """
        Stream real-time market data
        
        Args:
            symbols: List of stock symbols to stream
            
        Yields:
            Real-time bar data as dictionary
        """
        async def _process_bar(bar):
            # Convert bar to dictionary format
            bar_dict = {
                "symbol": bar.symbol,
                "timestamp": bar.timestamp,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "trade_count": bar.trade_count,
                "vwap": bar.vwap
            }
            return bar_dict
        
        # Set up stream connection and subscribe to bar events
        self.stream_client.subscribe_bars(_process_bar, symbols)
        self.stream_client.run()
        
        try:
            while True:
                # This is a synchronous wrapper for the async stream
                # In actual code, you'll want to handle this differently
                time.sleep(0.1)
                # The actual async callback function handles the event processing
                
        except KeyboardInterrupt:
            logger.info("Stopping real-time data stream")
            self.stream_client.stop()
            return
    
    def format_for_kafka(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format stock data for Kafka ingestion
        
        Args:
            data: Raw stock data
            
        Returns:
            Formatted data for Kafka
        """
        return {
            "source": "alpaca",
            "data_type": "stock",
            "timestamp": datetime.now().isoformat(),
            "payload": data
        }
