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
│   │   ├── social_media.py # Social media APIs
│   │   └── securities.py   # Securities data sources
│   ├── ingestion/         # Data ingestion layer
│   │   └── producers.py   # Faust producers
│   ├── processing/        # Real-time processing
│   │   ├── agents.py      # Faust agents
│   │   └── ai_agents.py   # LangChain AI agents
│   ├── drift_detection/   # Drift detection module
│   │   └── metrics.py     # Drift metrics and computation
│   ├── multimodal/        # Multimodal AI processing
│   │   └── preprocessors.py # Multimodal preprocessors
│   ├── storage/           # Data storage layer
│   │   └── knowledge_graph.py # Neo4j interface
│   └── visualization/     # Visualization layer
│       └── dashboards.py  # Grafana dashboard config
└── config/                # Configuration files
    ├── kafka.yml          # Kafka configuration
    └── ml_models.yml      # ML model configurations
```
