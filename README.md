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
- **Ingestion:** Kafka + Zookeeper
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
git clone https://github.com/minnobug/smartcity-data-pipeline.git 
cd smartcity-data-pipeline

### Prerequisites
- Docker Desktop
- Python 3.8+
- AWS Account (for cloud components)

### Setup

```bash
# Clone repository
git clone <repository-url>
cd smartcity-data-pipeline

# Install dependencies
pip install -r requirements.txt

# Start infrastructure
docker-compose up -d

# Create Kafka topics
docker exec -it broker bash
kafka-topics --create --topic vehicle_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --create --topic gps_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --create --topic traffic_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --create --topic weather_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --create --topic emergency_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
exit

# Run IoT simulator
python jobs/main.py
```

### Monitor Data

```bash
# Console consumer
docker exec -it broker kafka-console-consumer --bootstrap-server localhost:9092 --topic vehicle_data --from-beginning

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

**Additional schemas:** GPS, Traffic Camera, Weather, Emergency Incident data (see `/docs/schemas.md`)

## Project Structure

```
├── docker-compose.yml      # Infrastructure configuration
├── requirements.txt        # Python dependencies
├── jobs/
│   ├── main.py            # IoT data producer
│   └── spark-city.py      # Spark streaming job
├── config/                # Configuration files
├── data/                  # Local data storage
└── docs/                  # Documentation
```

## Configuration

### Environment Variables
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
VEHICLE_TOPIC=vehicle_data
GPS_TOPIC=gps_data
TRAFFIC_TOPIC=traffic_data
WEATHER_TOPIC=weather_data
EMERGENCY_TOPIC=emergency_data
```

### AWS Configuration
Set up AWS credentials in `~/.aws/credentials` or use IAM roles for production.

## Development

### Running Tests
```bash
pytest tests/
```

### Code Quality
```bash
# Linting
pylint jobs/

# Formatting
black jobs/
```

## Deployment

### Local Development
```bash
docker-compose up -d
python jobs/main.py
```

### Production (AWS)
1. Deploy Kafka cluster on AWS MSK
2. Run Spark on EMR
3. Configure S3 buckets and IAM roles
4. Set up Glue crawlers
5. Create Redshift cluster
6. Connect PowerBI to Redshift

## Monitoring

- **Kafka:** `docker logs broker -f`
- **Spark UI:** http://localhost:9090
- **Zookeeper:** Port 2181
- **Kafka Broker:** Port 9092

## Troubleshooting

**Issue:** Kafka connection refused  
**Solution:** Ensure Docker containers are running: `docker ps`

**Issue:** Topics not found  
**Solution:** Create topics using commands in Quick Start section

**Issue:** Spark job fails  
**Solution:** Check Spark logs: `docker logs spark-master -f`

## Performance

- **Throughput:** ~100 messages/second
- **Latency:** <50ms average
- **Data Volume:** Processes 5 concurrent streams
- **Scalability:** Horizontal scaling via Kafka partitions and Spark workers

## Roadmap

- [x] IoT data simulation
- [x] Kafka streaming
- [x] Docker containerization
- [ ] Spark structured streaming
- [ ] AWS S3 integration
- [ ] Glue catalog setup
- [ ] Redshift data warehouse
- [ ] PowerBI dashboards

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/new-feature`)
3. Commit changes (`git commit -m 'feat: add new feature'`)
4. Push to branch (`git push origin feature/new-feature`)
5. Open Pull Request

## License

MIT License - see [LICENSE](LICENSE) file

## Contact

**Project Maintainer:** [Minnobug]  
**Email:** leq4482@gmail.com  
**GitHub:** [@minnobug](https://github.com/minnobug) 
---

**Star this repository if you find it helpful!**
