# connect to your Vertica database
import random
import time
import requests
import vertica_python
from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro import CachedSchemaRegistryClient
from confluent_kafka import avro

conn_info = {
    'host': '44.205.97.150',
    'port': 5433,
    'user': 'dbadmin',
    'password': 'Pass$123',
    'database': 'verticadb'
}

connection = vertica_python.connect(**conn_info)
cur = connection.cursor()

# Boundaries of Massachusetts
massachusetts_bounds = {
    'min_lat': 41.187053,
    'max_lat': 42.886765,
    'min_long': -73.508142,
    'max_long': -69.858862
}

# List of commercial carriers
carriers = [
    'AT&T', 'Verizon', 'T-Mobile', 'Vodafone', 'China Mobile', 'Airtel',
    'Telefónica', 'Orange', 'Nippon Telegraph and Telephone', 'China Telecom'
]

def get_random_location_in_massachusetts(tower_id):
    cur.execute("SELECT Latitude, Longitude, Location FROM telco.tower_locations WHERE TowerID = %s", [tower_id])
    result = cur.fetchone()

    if result is not None:
        return result
    else:
        while True:
            lat = random.uniform(massachusetts_bounds['min_lat'], massachusetts_bounds['max_lat'])
            long = random.uniform(massachusetts_bounds['min_long'], massachusetts_bounds['max_long'])

            url = f'https://api.mapbox.com/geocoding/v5/mapbox.places/{long},{lat}.json'
            params = {
                'access_token': 'pk.eyJ1IjoidGltdGFkZW8iLCJhIjoiY2xpeXJ0NTNuMDBvaTNsbmVibmg4MG1veiJ9.Lp2g4WG4JC3egegBXMo9ow'
            }

            response = requests.get(url, params=params)

            if response.status_code == 200:
                data = response.json()
                try:
                    place_name = data['features'][0]['place_name']
                    cur.execute("INSERT INTO telco.tower_locations (TowerID, Latitude, Longitude, Location) VALUES (%s, %s, %s, %s)", [tower_id, lat, long, place_name])
                    connection.commit()
                    return lat, long, place_name
                except IndexError:
                    continue

def generate_tower_data(tower_ids):
    value_schema_str = """
    {
        "namespace": "TELCO",
        "name": "CellTowerDataV3",
        "type": "record",
        "fields": [
            {"name": "TowerID", "type": "int"},
            {"name": "Latitude", "type": "double"},
            {"name": "Longitude", "type": "double"},
            {"name": "Location", "type": "string"},
            {"name": "Status", "type": "string"},
            {"name": "Uptime", "type": "double"},
            {"name": "SignalStrength", "type": "int"},
            {"name": "NetworkType", "type": "string"},
            {"name": "Traffic", "type": "int"},
            {"name": "Carrier", "type": "string"},
            {"name": "Timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}}
        ]
    }
    """
    value_schema = avro.loads(value_schema_str)

    schema_registry_client = CachedSchemaRegistryClient('http://52.203.71.36:8081')
    avro_producer = AvroProducer({
        'bootstrap.servers': '52.203.71.36:9092',
        'schema.registry.url': 'http://52.203.71.36:8081'
    }, default_value_schema=value_schema)

    statuses = ['Offline', 'Maintenance', 'Online']
    status_weights = [0.04, 0.06, 0.9]
    spinners = ['|', '/', '-', '\\']
    produced_records = 0


    while True:
        for tower_id in tower_ids:
            lat, long, place_name = get_random_location_in_massachusetts(tower_id)
            
            # Select random values
            status = random.choices(statuses, status_weights)[0]
            signal_strength = random.randint(-120, -50)
            network_type = random.choice(['4G', '5G'])
            traffic = random.randint(0, 1000)
            carrier = random.choice(carriers)

            # Condition for specific tower, carrier, and network type to set uptime
            if tower_id in [5, 27, 81] and carrier == 'China Telecom' and network_type == '4G':
                uptime = 60.0
            else:
                uptime = round(random.uniform(90.0, 100.0), 2)

            tower_data = {
                'TowerID': tower_id,
                'Latitude': lat,
                'Longitude': long,
                'Location': place_name,
                'Status': status,
                'Uptime': uptime,
                'SignalStrength': signal_strength,
                'NetworkType': network_type,
                'Traffic': traffic,
                'Carrier': carrier,
                'Timestamp': int(time.time()*1000)
            }

            avro_producer.produce(topic='cell_tower_dataV3', value=tower_data)
            avro_producer.flush()

            produced_records += 1
            print(f"\r{spinners[produced_records % 4]} Producing record {produced_records}", end='', flush=True)

            if produced_records % 1000 == 0:
                print(f"\n{produced_records} records have been produced!")

        time.sleep(5)


if __name__ == '__main__':
    tower_ids = list(range(1, 51))  # Generate data for towers 1 to 50
    generate_tower_data(tower_ids)
