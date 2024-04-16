import os
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
from random import uniform, randint, choice, seed
from uuid import uuid4, UUID
from time import sleep

LONDON_COORDINATES = {
    'latitude': 51.5074, 'longitude': -0.1278
}
BIRMINGHAM_COORDINATES = {
    'latitude': 52.4862, 'longitude': -1.8904
}

# Corrected the calculation for movement increment
LATITUDE_INCREAMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
LONGITUDE_INCREAMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude']) / 100

# environment variables for configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

seed(42)

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()


def get_next_time():
    global start_time
    start_time += timedelta(seconds=randint(30, 60))  # update frequency
    return start_time  # Added return statement


def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id': uuid4(),
        'device_id': device_id,
        'timestamp': timestamp,
        'speed': uniform(0, 40),
        'direction': 'North-East',
        'vehicle_type': vehicle_type
    }


def generate_traffic_camera_data(device_id, timestamp, camera_id):
    return {
        'id': uuid4(),
        'device_id': device_id,
        'camera_id': camera_id,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedStr'
    }


def generate_weather_data(device_id, timestamp, location):
    return {
        'id': uuid4(),
        'device_id': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': uniform(-5, 26),
        'weather_condition': choice(['Sunny', 'Rainy', 'cloudy', 'snowy']),
        'precipitation': uniform(0, 26),
        'wind_speed': uniform(0, 100),
        'humidity': uniform(0, 100),
        'AQI': uniform(0, 500)
    }


def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': uuid4(),
        'incident_id': uuid4(),
        'type': choice(['accident', 'fire', 'medical', 'police', 'None']),
        'timestamp': timestamp,
        'location': location,
        'status': choice(['active', 'resolved']),
        'description': 'Description of the incident'
    }


def simulate_vehicle_movement():
    global start_location

    # move towards birmingham
    start_location['latitude'] += LATITUDE_INCREAMENT
    start_location['longitude'] += LONGITUDE_INCREAMENT

    # add some randomness to simulate actual road travel
    start_location['latitude'] += uniform(-0.0005, 0.0005)
    start_location['longitude'] += uniform(-0.0005, 0.0005)

    return start_location


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid4(),
        'device_id': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),  # Added missing comma
        'speed': uniform(10, 40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'C500',
        'year': 2024,
        'fuel_type': 'hybrid'
    }


def json_serializer(obj):
    if isinstance(obj, UUID):
        return str(obj)
    raise TypeError(f'Object of Type {obj.__class__.__name__} is not JSON serializable')


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )
    producer.flush()


def simulate_journey(producer, device_id):  # Added missing colon
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], camera_id='nikon-123')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'],
                                                                   vehicle_data['location'])

        if (vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude'] and vehicle_data['location'][1] <=
                BIRMINGHAM_COORDINATES['longitude']):
            print('Vehicle has reached Birmingham. Simulation ending....')
            break

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        sleep(5)


if __name__ == '__main__':
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka Error: {err}')
    }

    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-CodeWithYu-123')
    except KeyboardInterrupt:
        print('Simulation ended by the user')
    except Exception as e:
        print(f'Unexpected Error occurred: {e}')
