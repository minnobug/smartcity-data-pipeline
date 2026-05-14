import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('/opt/spark/jobs/.env')

# AWS Configuration
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "vehicle_data")
GPS_TOPIC = os.getenv("GPS_TOPIC", "gps_data")
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", "traffic_data")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather_data")
EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC", "emergency_data")

configuration = {
    'AWS_ACCESS_KEY': AWS_ACCESS_KEY,
    'AWS_SECRET_KEY': AWS_SECRET_KEY,
    'AWS_REGION': AWS_REGION,
    'AWS_BUCKET_NAME': AWS_BUCKET_NAME,
    'KAFKA_BOOTSTRAP_SERVERS': KAFKA_BOOTSTRAP_SERVERS,
    'VEHICLE_TOPIC': VEHICLE_TOPIC,
    'GPS_TOPIC': GPS_TOPIC,
    'TRAFFIC_TOPIC': TRAFFIC_TOPIC,
    'WEATHER_TOPIC': WEATHER_TOPIC,
    'EMERGENCY_TOPIC': EMERGENCY_TOPIC
}

# Validation
if not all([AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_BUCKET_NAME]):
    raise ValueError("Missing required AWS credentials in .env file")