# Smart City Real-Time Data Pipeline

End-to-end real-time data engineering pipeline for Smart City IoT systems with streaming analytics and cloud integration.

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Kafka-7.4.0-red.svg)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Spark-3.5.0-orange.svg)](https://spark.apache.org/)
[![AWS](https://img.shields.io/badge/AWS-Cloud-yellow.svg)](https://aws.amazon.com/)

## Overview

Production-grade data pipeline simulating Smart City vehicle telemetry from Ho Chi Minh City to Vung Tau. Processes real-time IoT data streams through Kafka, transforms with Spark, stores in AWS S3, and enables analytics via Glue, Athena, and Redshift.

**Key Features:**
- Real-time data ingestion from 5 IoT sources
- Event-driven architecture with Apache Kafka
- Distributed processing with Apache Spark
- Cloud-native storage and analytics on AWS
- PowerBI visualization

## Architecture

```
IoT Devices → Kafka → Spark Streaming → S3 → Glue Catalog → Athena/Redshift → PowerBI
```

**Components:**
- **Ingestion:** Kafka + Zookeeper (Docker Compose)
- **Processing:** Spark (1 master + 2 workers)
- **Storage:** AWS S3 (data lake)
- **Catalog:** AWS Glue
- **Analytics:** AWS Athena, Redshift
- **Visualization:** PowerBI

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.8+ |
| Message Broker | Apache Kafka 7.4.0 |
| Stream Processing | Apache Spark 3.5.0 |
| Containerization | Docker Compose |
| Cloud Platform | AWS (S3, Glue, Athena, Redshift, IAM) |
| Visualization | PowerBI |

## Quick Start

### Prerequisites
- Docker Desktop
- Python 3.8+
- AWS Account (for cloud components)

### Setup

```bash
# Clone repository
git clone https://github.com/minnobug/smartcity-data-pipeline.git
cd smartcity-data-pipeline

# Install dependencies
pip install -r requirements.txt

# Start infrastructure
docker-compose up -d

# Create Kafka topics
docker exec -it broker bash
kafka-topics --create --topic vehicle_data --bootstrap-server broker:29092 --partitions 1 --replication-factor 1
kafka-topics --create --topic gps_data --bootstrap-server broker:29092 --partitions 1 --replication-factor 1
kafka-topics --create --topic traffic_data --bootstrap-server broker:29092 --partitions 1 --replication-factor 1
kafka-topics --create --topic weather_data --bootstrap-server broker:29092 --partitions 1 --replication-factor 1
kafka-topics --create --topic emergency_data --bootstrap-server broker:29092 --partitions 1 --replication-factor 1
exit

# Run IoT simulator
python jobs/iot_simulator.py
```

### Monitor Data

```bash
# Console consumer
docker exec -it broker kafka-console-consumer --bootstrap-server broker:29092 --topic vehicle_data --from-beginning

# Spark UI
http://localhost:9090

# Submit Spark job
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/spark/jobs/spark-city.py
```

## Data Schema

### Vehicle Data
```json
{
  "id": "uuid",
  "deviceId": "Vehicle-XXX-123",
  "timestamp": "2026-01-11T...",
  "location": {"latitude": 10.77, "longitude": 106.70},
  "speed": 65.3,
  "fuelLevel": 75.2,
  "engineTemperature": 95.4
}
```

Additional schemas: GPS, Traffic Camera, Weather, Emergency Incident — see `/docs/schemas.md`

## Project Structure

```
├── docker-compose.yml      # Infrastructure configuration
├── requirements.txt        # Python dependencies
├── jobs/
│   ├── iot_simulator.py   # IoT data producer
│   └── spark-city.py      # Spark streaming job
├── config/                # Configuration files
├── data/                  # Local data storage
└── docs/                  # Documentation
```

## Configuration

### Environment Variables

```bash
KAFKA_BOOTSTRAP_SERVERS=broker:29092
VEHICLE_TOPIC=vehicle_data
GPS_TOPIC=gps_data
TRAFFIC_TOPIC=traffic_data
WEATHER_TOPIC=weather_data
EMERGENCY_TOPIC=emergency_data
```

### AWS Configuration

Set up AWS credentials in `~/.aws/credentials` or use IAM roles for production.

## Development

```bash
# Linting
pylint jobs/

# Formatting
black jobs/

# Tests
pytest tests/
```

## Deployment

### Local Development
```bash
docker-compose up -d
python jobs/iot_simulator.py
```

### Production (AWS)
1. Deploy Kafka cluster on AWS MSK
2. Run Spark on EMR
3. Configure S3 buckets and IAM roles
4. Set up Glue crawlers
5. Create Redshift cluster
6. Connect PowerBI to Redshift

## Monitoring

| Service | Access |
|---------|--------|
| Spark UI | http://localhost:9090 |
| Kafka logs | `docker logs broker -f` |
| Spark logs | `docker logs spark-master -f` |
| Zookeeper | Port 2181 |
| Kafka Broker | Port 29092 (internal) / 9092 (external) |

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Kafka connection refused | `docker ps` — ensure all containers are running |
| Topics not found | Run topic creation commands in Quick Start |
| Spark job fails | `docker logs spark-master -f` |
| Ivy cache error | `rm -rf /home/spark/.ivy2/cache`, re-submit job |
| `python-dotenv` missing | `docker exec -u root spark-master pip install python-dotenv` |

## Performance

- **Throughput:** ~100 messages/second
- **Latency:** <50ms average
- **Streams:** 5 concurrent IoT topics
- **Scalability:** Horizontal via Kafka partitions + Spark workers

## Roadmap

- [x] IoT data simulation
- [x] Kafka streaming
- [x] Docker containerization
- [x] Spark structured streaming
- [x] AWS S3 integration
- [ ] Glue catalog setup
- [ ] Redshift data warehouse
- [ ] PowerBI dashboards

## Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/your-feature`
3. Commit changes: `git commit -m 'feat: add your feature'`
4. Push to branch: `git push origin feature/your-feature`
5. Open a Pull Request

## License

MIT License — see [LICENSE](LICENSE)

## Contact

**Project Maintainer:** [Minnobug](https://github.com/minnobug)


[![GitHub](https://img.shields.io/badge/GitHub-minnobug-181717?style=for-the-badge&logo=github)](https://github.com/minnobug)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5?style=for-the-badge&logo=linkedin)](https://www.linkedin.com/in/le-van-minh-2129s)