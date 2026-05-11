import json
import os
import random
import uuid
import time
from datetime import datetime, timedelta
from confluent_kafka import SerializingProducer

# ── Toạ độ HCM → Vũng Tàu ─────────────────────────────────────────────────
HCM_COORDINATES      = {"latitude": 10.7769, "longitude": 106.7009}
VUNG_TAU_COORDINATES = {"latitude": 10.4113, "longitude": 107.1362}

LATITUDE_INCREMENT  = (VUNG_TAU_COORDINATES['latitude']  - HCM_COORDINATES['latitude'])  / 100
LONGITUDE_INCREMENT = (VUNG_TAU_COORDINATES['longitude'] - HCM_COORDINATES['longitude']) / 100

# ── Kafka config từ .env ───────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC   = os.getenv('VEHICLE_TOPIC',   'vehicle_data')
GPS_TOPIC       = os.getenv('GPS_TOPIC',        'gps_data')
TRAFFIC_TOPIC   = os.getenv('TRAFFIC_TOPIC',    'traffic_data')
WEATHER_TOPIC   = os.getenv('WEATHER_TOPIC',    'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC',  'emergency_data')

# ── State ──────────────────────────────────────────────────────────────────
start_time     = datetime.now()
start_location = HCM_COORDINATES.copy()


# ── Time helper ────────────────────────────────────────────────────────────
def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time


# ── Movement ───────────────────────────────────────────────────────────────
def simulate_vehicle_movement():
    global start_location
    start_location['latitude']  += LATITUDE_INCREMENT  + random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += LONGITUDE_INCREMENT + random.uniform(-0.0005, 0.0005)
    return start_location.copy()


# ── Data generators ────────────────────────────────────────────────────────
def generate_vehicle_data(vehicle_id):
    location = simulate_vehicle_movement()
    return {
        'id':        str(uuid.uuid4()),
        'vehicle_id': vehicle_id,
        'timestamp': get_next_time().isoformat(),
        'location':  (location['latitude'], location['longitude']),
        'speed':     round(random.uniform(40, 120), 2),   # km/h cao tốc
        'direction': 'South-East',                         # HCM → Vũng Tàu
        'make':      random.choice(['Toyota', 'Kia', 'Hyundai', 'VinFast', 'Ford']),
        'model':     random.choice(['Vios', 'Morning', 'Accent', 'VF8', 'Ranger']),
        'year':      random.randint(2018, 2024),
        'fuelType':  random.choice(['Gasoline', 'Electric', 'Hybrid']),
    }


def generate_gps_data(vehicle_id, timestamp, vehicle_type='private'):
    return {
        'id':          str(uuid.uuid4()),
        'vehicle_id':  vehicle_id,
        'timestamp':   timestamp,
        'speed':       round(random.uniform(40, 120), 2),
        'direction':   'South-East',
        'vehicleType': vehicle_type,
    }


def generate_traffic_camera_data(vehicle_id, timestamp, location, camera_id):
    return {
        'id':         str(uuid.uuid4()),
        'vehicle_id': vehicle_id,
        'camera_id':  camera_id,
        'location':   location,
        'timestamp':  timestamp,
        'snapshot':   'Base64EncodedString',
    }


def generate_weather_data(vehicle_id, timestamp, location):
    return {
        'id':               str(uuid.uuid4()),
        'vehicle_id':       vehicle_id,
        'location':         location,
        'timestamp':        timestamp,
        'temperature':      round(random.uniform(25, 38), 1),   # nhiệt đới
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rain', 'Stormy']),
        'precipitation':    round(random.uniform(0, 50), 1),
        'windSpeed':        round(random.uniform(0, 60), 1),
        'humidity':         random.randint(60, 95),              # độ ẩm cao
        'airQualityIndex':  round(random.uniform(0, 200), 1),
    }


def generate_emergency_incident_data(vehicle_id, timestamp, location):
    return {
        'id':          str(uuid.uuid4()),
        'vehicle_id':  vehicle_id,
        'incidentId':  str(uuid.uuid4()),
        'type':        random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'timestamp':   timestamp,
        'location':    location,
        'status':      random.choice(['Active', 'Resolved']),
        'description': 'Incident on HCM - Vung Tau Expressway',
    }


# ── Kafka helpers ──────────────────────────────────────────────────────────
def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')


def delivery_report(err, msg):
    if err is not None:
        print(f'  [FAIL] {msg.topic()} | {err}')
    else:
        print(f'  [OK]   {msg.topic():<20} partition={msg.partition()}  offset={msg.offset()}')


def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report,
    )
    producer.flush()


# ── Main simulation ────────────────────────────────────────────────────────
def simulate_journey(producer, vehicle_id):
    print(f"\n{'='*55}")
    print(f"  Smart City Pipeline — HCM → Vũng Tàu")
    print(f"  Vehicle : {vehicle_id}")
    print(f"  Broker  : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"{'='*55}\n")

    step = 0
    while True:
        step += 1
        vehicle_data = generate_vehicle_data(vehicle_id)
        timestamp    = vehicle_data['timestamp']
        location     = vehicle_data['location']

        gps_data       = generate_gps_data(vehicle_id, timestamp)
        traffic_data   = generate_traffic_camera_data(vehicle_id, timestamp, location, 'Camera-HighwayAI-01')
        weather_data   = generate_weather_data(vehicle_id, timestamp, location)
        emergency_data = generate_emergency_incident_data(vehicle_id, timestamp, location)

        # Điều kiện kết thúc: đã tới Vũng Tàu
        if (vehicle_data['location'][0] <= VUNG_TAU_COORDINATES['latitude']
                and vehicle_data['location'][1] >= VUNG_TAU_COORDINATES['longitude']):
            print('\n[DONE] Vehicle has reached Vung Tau. Simulation ending...')
            break

        print(f"\n[Step {step:03d}] lat={location[0]:.5f}  lng={location[1]:.5f}  speed={vehicle_data['speed']} km/h")
        produce_data_to_kafka(producer, VEHICLE_TOPIC,   vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC,       gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC,   traffic_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC,   weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_data)

        time.sleep(5)


if __name__ == '__main__':
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka Error: {err}'),
    }

    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-HCM-VT-001')
    except KeyboardInterrupt:
        print('\nSimulation ended by the user.')
    except Exception as e:
        print(f'Unexpected error: {e}')