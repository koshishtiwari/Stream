# Stream


## System Overview

The system is designed with multiple layers:
- Data Sources Layer
- Data Ingestion Layer (using confluent-kafka)
- Real-Time Processing Layer
- Drift Detection Module
- Multimodal AI Module
- Data Storage Layer
- Visualization and Monitoring Layer

For detailed system design, see [SYSTEM.md](./SYSTEM.md).

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.9+

### Installation

1. Clone the repository
```bash
git clone https://github.com/yourusername/Stream.git
cd Stream
```

2. Install required Python packages
```bash
pip install -r requirements.txt
```

3. Start the infrastructure services
```bash
docker-compose up -d
```

### Running the Application

1. Start the IoT data producer:
```bash
python -m src.ingestion.producers
```

2. Start the data processor:
```bash
python -m src.processing.agents

```

3. Start the Dashboard:
```bash
python -m src.visualization.launch_dashboard
```

## Project Structure

See [project_structure.md](./project_structure.md) for details about the code organization.

## License

MIT