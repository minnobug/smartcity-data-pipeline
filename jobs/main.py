import os
import datetime
import random
import uuid

from confluent_kafka import SerializingProducer
import simplejson as json

HO_CHI_MINH_COORDINATES = {"latitude": 10.776889, "longitude": 106.700806}
VUNG_TAU_COORDINATES = {"latitude": 10.341660, "longitude": 107.084343}

# Calculate movement increments
LATITUDE_INCREMENT = (VUNG_TAU_COORDINATES['latitude'] - HO_CHI_MINH_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (VUNG_TAU_COORDINATES['longitude'] - HO_CHI_MINH_COORDINATES['longitude']) / 100

# Environment Variables for configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

start_time = datetime.datetime.now()
start_location = HO_CHI_MINH_COORDINATES.copy()


def get_next_time():
    global start_time
    start_time += datetime.timedelta(seconds=random.randint(30, 60)) # update frequency
    return start_time

def simulate_vehicle_movement():
	global start_location
	#move towards Vung_Tau
	start_location['latitude'] += LATITUDE_INCREMENT
	start_location['longitude'] += LONGITUDE_INCREMENT

	#add some randomness to simulate actual road travel
	start_location['latitude' ] += random. uniform(-0.0005,0.0005)
	start_location['longitude' ] += random. uniform(-0.0005, 0.0005)

	return start_location

def generate_vehicle_data(device_id):
	location = simulate_vehicle_movement()
        return {
            'id': uuid.uuid4(),
            'deviceId': device_id,
            'timestamp': get_next_time().isoformat(),
            'Location': (location['Latitude' ], location['Longitude' ]),
            'speed': random. uniform(10,40),
            'direction': 'North-East',
            'make': 'BMW',
            'model': 'C500',
            'year': 2024,
            'fuelType': 'Hybrid'
        }

def simulate_journey(producer, device_id):
	while True:
	    vehicle_data = generate_vehicle_data(device_id)


if __name__ == "__main__":

	producer_config = {
		'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
		'error_cb': lambda err: print(f'Kafka error: {err}')
	}
	producer = SerializingProducer(producer_config)

	try:
	    simulate_journey(producer, 'Vehicle-Minnobug-123')

	except KeyboardInterrupt:
		print('Simulation ended by the user')
	except Exception as e:
