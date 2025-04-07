# Project Structure

```
/workspaces/Stream/
├── README.md              # Project overview
├── SYSTEM.md              # System design documentation
├── requirements.txt       # Dependencies
├── docker-compose.yml     # Infrastructure setup (Kafka, Neo4j, etc.)
├── src/                   # Source code directory
│   ├── data_sources/      # Data source connectors
│   │   ├── iot.py         # IoT sensor data source
│   │   ├── Search.py      # Online Search using Gemini LLM
│   │   ├── alpaca.py      # Stock market data using Alpaca
|   |   |── Reddit.py      # Reddit scraping  
│   │   └── securities.py  # Securities data sources
│   ├── ingestion/         # Data ingestion layer
│   │   └── producers.py   # Kafka producers
│   ├── processing/        # Real-time processing
│   │   ├── agents.py      # Kafka consumer agents
│   │   └── ai_agents.py   # LangChain AI agents
│   ├── drift_detection/   # Drift detection module
│   │   └── metrics.py     # Drift metrics and computation
│   ├── multimodal/        # Multimodal AI processing
│   │   └── preprocessors.py # Multimodal preprocessors
│   ├── storage/           # Data storage layer
│   │   └── knowledge_graph.py # Neo4j interface
│   └── visualization/     # Visualization layer
│       ├── dashboards.py  # Grafana dashboard config
│       ├── api_service.py # FastAPI service for dashboard data
│       ├── dashboard.py   # Streamlit dashboard
│       ├── metrics_collector.py # Redis metrics collector
│       └── launch_dashboard.py  # Dashboard launcher script
└── config/                # Configuration files
    ├── kafka.yml          # Kafka configuration
    └── ml_models.yml      # ML model configurations
```
