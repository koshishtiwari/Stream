# Stream - Real-Time Data Processing Platform

A clean, modular platform for ingesting, processing, and visualizing streaming data from multiple sources including IoT sensors, financial markets, and news feeds.

## Overview

Stream is built with these key components:

1. **Data Sources Layer**: Connects to IoT sensors, financial markets (via Alpaca), and news sources
2. **Data Ingestion Layer**: Uses Kafka to ingest real-time data streams
3. **Processing Layer**: Analyzes incoming data and detects anomalies
4. **Monitoring Layer**: Tracks system health and data flow metrics
5. **Visualization Layer**: Provides real-time dashboards and API access to data

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.9+
- For financial data: Alpaca API keys
- For news data: Google API key with Gemini access

### Installation

1. Clone the repository
```bash
git clone https://github.com/yourusername/Stream.git
cd Stream
```

2. Set up environment variables (optional)
```bash
# Create .env file
cp .env.example .env
# Edit with your API keys
nano .env
```

3. Install required Python packages
```bash
pip install -r requirements.txt
```

4. Start the infrastructure services
```bash
docker-compose up -d
```

### Running the Application

Use the centralized run.py script to start all components:

```bash
# Run everything
python run.py

# Or run specific components
python run.py --producer --processor  # Just producers and processors
python run.py --dashboard  # Just the dashboard
python run.py --monitoring  # Just the monitoring service
```

Access the components:
- Dashboard: http://localhost:8501
- API: http://localhost:8000
- API Documentation: http://localhost:8000/docs
- Prometheus Metrics: http://localhost:8000/metrics
- Grafana: http://localhost:3000 (admin/admin)

## Project Structure

```
Stream/
├── src/                   # Source code
│   ├── config.py          # Centralized configuration
│   ├── data_sources/      # Data source connectors
│   │   ├── iot.py         # IoT sensor data source
│   │   ├── market.py      # Financial market data
│   │   └── search.py      # News data using Google Gemini
│   ├── ingestion/         # Data ingestion
│   │   └── producers.py   # Kafka producers
│   ├── processing/        # Real-time processing
│   │   └── agents.py      # Kafka consumer agents
│   ├── monitoring/        # Monitoring
│   │   └── metrics.py     # Metrics collection
│   └── visualization/     # Visualization
│       ├── api.py         # FastAPI service
│       └── dashboard.py   # Streamlit dashboard
├── docker-compose.yml     # Docker services config
└── run.py                 # Centralized run script
```

## Development

To add a new data source:

1. Create a new file in `src/data_sources/`
2. Implement a class with the same interface as existing sources
3. Add the source to `src/ingestion/producers.py`

## License

MIT