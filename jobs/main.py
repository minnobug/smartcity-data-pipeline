import os
import datetime
from confluent_kafka import SerializingProducer
import simplejson as json

HO_CHI_MINH_COORDINATES = {"latitude": 10.776889, "longitude": 106.700806}
VUNG_TAU_COORDINATES = {"latitude": 10.341660, "longitude": 107.084343}

# Calculate movement increments
LATITUDE_INCREMENT = (VUNG_TAU_COORDINATES['latitude'] - HO_CHI_MINH_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (VUNG_TAU_COORDINATES['longitude'] - HO_CHI_MINH_COORDINATES['longitude']) / 100

# Test
# print(" All imports successful!")
# print(f"Latitude increment: {LATITUDE_INCREMENT}")
# print(f"Longitude increment: {LONGITUDE_INCREMENT}")

# Environment Variables for configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

start_time = datetime.datetime.now()
start_location = HO_CHI_MINH_COORDINATES.copy()
