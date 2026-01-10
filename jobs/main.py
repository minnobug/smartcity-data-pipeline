import os
import datetime
import random
import uuid
import time

from confluent_kafka import SerializingProducer
import simplejson as json

HO_CHI_MINH_COORDINATES = {"latitude": 10.776889, "longitude": 106.700806}
VUNG_TAU_COORDINATES = {"latitude": 10.341660, "longitude": 107.084343}

LATITUDE_INCREMENT = (VUNG_TAU_COORDINATES['latitude'] - HO_CHI_MINH_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (VUNG_TAU_COORDINATES['longitude'] - HO_CHI_MINH_COORDINATES['longitude']) / 100

# Environment Variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

random.seed(42)
start_time = datetime.datetime.now()
start_location = HO_CHI_MINH_COORDINATES.copy()

update_counter = 0


def get_next_time():
    global start_time
    start_time += datetime.timedelta(seconds=random.randint(30, 60))
    return start_time


def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def delivery_report(err, msg):
    # call back to report the message sending the results.
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def generate_gps_data(device_id, timestamp, location, vehicle_type='private'):
    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'timestamp': timestamp,
        'latitude': location['latitude'],
        'longitude': location['longitude'],
        'speed': random.uniform(20, 80),
        'direction': 'South-East',
        'vehicleType': vehicle_type,
        'accuracy': random.uniform(5, 15)  # GPS accuracy in meters
    }


def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'cameraId': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString',
        'vehicleCount': random.randint(10, 100),
        'averageSpeed': random.uniform(40, 80)
    }


def generate_weather_data(device_id, timestamp, location):
    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform(25, 35),  # °C
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rainy', 'Partly Cloudy']),
        'precipitation': random.uniform(0, 15),  # mm
        'windSpeed': random.uniform(5, 30),  # km/h
        'humidity': random.uniform(60, 95),  # %
        'airQualityIndex': random.uniform(50, 150)  # AQI
    }


def generate_emergency_incident_data(device_id, timestamp, location):
    incident_type = random.choices(
        ['Accident', 'Fire', 'Medical', 'Police', 'None'],
        weights=[5, 2, 3, 5, 85]  # 85% none
    )[0]

    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'incidentId': str(uuid.uuid4()),
        'timestamp': timestamp,
        'location': location,
        'type': incident_type,
        'status': random.choice(['Active', 'Resolved']) if incident_type != 'None' else 'None',
        'description': f'Incident at location {location}' if incident_type != 'None' else 'No incident'
    }


def simulate_vehicle_movement():
    global start_location

    # moving towards Vung Tau
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    # simulate a non-straight path
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location.copy()


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': location,
        'speed': random.uniform(40, 90),  # km/h
        'direction': 'South-East',
        'make': 'BMW',
        'model': 'C500',
        'year': 2024,
        'fuelType': 'Hybrid',
        'fuelLevel': random.uniform(30, 100),  # %
        'engineTemperature': random.uniform(80, 105)  # °C
    }


def has_reached_destination(current_location):
    # check if you have arrived in Vung Tau within a 5 km radius.
    lat_diff = abs(current_location['latitude'] - VUNG_TAU_COORDINATES['latitude'])
    lon_diff = abs(current_location['longitude'] - VUNG_TAU_COORDINATES['longitude'])
    return lat_diff < 0.05 and lon_diff < 0.05


def produce_to_kafka(producer, topic, data):
    # send data to Kafka topic
    try:
        producer.produce(
            topic=topic,
            value=json.dumps(data, default=json_serializer),
            callback=delivery_report
        )
        producer.poll(0)  # Trigger delivery callbacks
    except Exception as e:
        print(f'Error producing to {topic}: {e}')


def simulate_journey(producer, device_id):
    global update_counter

    print(f"Starting journey from Ho Chi Minh City to Vung Tau...")
    print(f"Start: {HO_CHI_MINH_COORDINATES}")
    print(f"Destination: {VUNG_TAU_COORDINATES}\n")

    while True:
        update_counter += 1

        # generate and send vehicle data (every 30-60 seconds)
        vehicle_data = generate_vehicle_data(device_id)
        produce_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)

        # GPS data (each update)
        gps_data = generate_gps_data(
            device_id,
            vehicle_data['timestamp'],
            vehicle_data['location']
        )
        produce_to_kafka(producer, GPS_TOPIC, gps_data)

        # weather data (updates every 5 minutes = approximately 5-6 times)
        if update_counter % 6 == 0:
            weather_data = generate_weather_data(
                device_id,
                vehicle_data['timestamp'],
                vehicle_data['location']
            )
            produce_to_kafka(producer, WEATHER_TOPIC, weather_data)

        # traffic camera (updates approximately 3-4 times every 3 minutes)
        if update_counter % 4 == 0:
            traffic_camera_data = generate_traffic_camera_data(
                device_id,
                vehicle_data['timestamp'],
                vehicle_data['location'],
                f'CAM-{random.randint(1, 20):03d}'
            )
            produce_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)

        # emergency incidents (random checks)
        if random.random() < 0.1:  # 10% chance mỗi lần
            emergency_data = generate_emergency_incident_data(
                device_id,
                vehicle_data['timestamp'],
                vehicle_data['location']
            )
            if emergency_data['type'] != 'None':
                produce_to_kafka(producer, EMERGENCY_TOPIC, emergency_data)

        # log progress
        print(
            f"Update #{update_counter} | Location: ({vehicle_data['location']['latitude']:.6f}, {vehicle_data['location']['longitude']:.6f})")

        # check if you have arrived at your destination.
        if has_reached_destination(vehicle_data['location']):
            print(f"\nReached Vung Tau! Journey completed after {update_counter} updates.")
            break

        # Đợi 1-2 giây trước khi gửi batch tiếp theo (để demo, thực tế không cần)
        time.sleep(1)

    # Flush remaining messages
    producer.flush()


if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-Minnobug-123')
    except KeyboardInterrupt:
        print('\nSimulation ended by user')
    except Exception as e:
        print(f'Unexpected error occurred: {e}')
    finally:
        producer.flush()
        print("Producer closed")