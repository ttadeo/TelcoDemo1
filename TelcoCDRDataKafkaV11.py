import time
import random
from faker import Faker
import json
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient
import vertica_python
from datetime import datetime, timezone, timedelta

# Create faker object
fake = Faker()

# Constants
STATUSES = ['Answered', 'Missed', 'Dropped']
TYPES = ['Voice', 'SMS', 'Data']
CARRIERS = [
    'AT&T', 'Verizon', 'T-Mobile', 'Vodafone', 'China Mobile', 'Airtel',
    'Telefónica', 'Orange', 'Nippon Telegraph and Telephone', 'China Telecom'
]
KAFKA_CONFIG = {
    'bootstrap.servers': '52.203.71.36:9092',
    'schema.registry.url': 'http://52.203.71.36:8081'
}
VERTICA_CONN_INFO = {
    'host': '44.205.97.150',
    'port': 5433,
    'user': 'dbadmin',
    'password': 'Pass$123',
    'database': 'verticadb'
}

SCHEMA_DEFINITION = {
    "type": "record",
    "name": "value",
    "namespace": "Telecom.CDR",
    "fields": [
        {"name": "CallID", "type": "string"},
        {"name": "StartTime", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "EndTime", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "Duration", "type": "int"},
        {"name": "CallingParty", "type": "string"},
        {"name": "CalledParty", "type": "string"},
        {"name": "TowerID", "type": "int"},
        {"name": "Status", "type": {"type": "enum", "name": "Status", "symbols": ["Answered", "Missed", "Dropped"]}},
        {"name": "Type", "type": {"type": "enum", "name": "Type", "symbols": ["Voice", "SMS", "Data"]}},
        {"name": "Charge", "type": "double"},
        {"name": "Latitude", "type": "double"},
        {"name": "Longitude", "type": "double"},
        {"name": "Carrier", "type": "string"},
        {"name": "NetworkType", "type": "string"},
        {"name": "Usage", "type": "int"}
    ]
}

SPECIAL_NUMBERS = ['(800) 332-2733', '(800) 392-6089', '(888) 830-6277']

def est_time_from_timestamp(timestamp):
    utc_time = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
    return utc_time.astimezone(timezone(timedelta(hours=-5))) 

def is_time_within_range(dt_obj, start_hr, end_hr):
    return start_hr <= dt_obj.hour < end_hr

def current_time_millis():
    return int(time.time() * 1000)

def generate_massachusetts_phone_number():
    area_codes = ['339', '351', '413', '508', '617', '774', '781', '857', '978']
    return f"{random.choice(area_codes)}-{random.randint(100,999)}-{random.randint(1000,9999)}"

def get_tower_locations():
    with vertica_python.connect(**VERTICA_CONN_INFO) as connection:
        cur = connection.cursor()
        cur.execute("SELECT TowerID FROM TELCO.tower_locations")
        return [row[0] for row in cur.fetchall()]

def get_tower_location(tower_id):
    with vertica_python.connect(**VERTICA_CONN_INFO) as connection:
        cur = connection.cursor()
        cur.execute(f"SELECT Latitude, Longitude FROM TELCO.tower_locations WHERE TowerID = {tower_id}")
        return cur.fetchone()

def get_random_timestamp_for_tower(tower_id):
    with vertica_python.connect(**VERTICA_CONN_INFO) as connection:
        cur = connection.cursor()
        cur.execute(f"SELECT \"Timestamp\" FROM TELCO.CellTowerDataV3 WHERE TowerID = {tower_id} ORDER BY RANDOM() LIMIT 1")
        result = cur.fetchone()
        if result:
            return int(result[0].timestamp() * 1000)
        return current_time_millis()

def calculate_charge(call_type, duration):
    rates = {'Voice': 0.05, 'SMS': 0.01, 'Data': 0.10}
    return round(duration/60 * rates.get(call_type, 0), 2)

def calculate_data_usage(call_type):
    if call_type == "Data":
        return random.randint(10, 20)
    elif call_type == "SMS":
        return random.randint(0, 1)
    else:
        return random.randint(0, 5)

def generate_cdr_data(num_records):
    value_schema = avro.loads(json.dumps(SCHEMA_DEFINITION))
    schema_registry_client = CachedSchemaRegistryClient('http://52.203.71.36:8081')
    avro_producer = AvroProducer(KAFKA_CONFIG, default_value_schema=value_schema)
    
    tower_ids = get_tower_locations()
    
    spinners = ['-', '\\', '|', '/']
    
    i = 0
    batch = []
    while i < num_records:
        call_type = random.choice(TYPES)
        
        start_timestamp = get_random_timestamp_for_tower(random.choice(tower_ids))
        duration = random.randint(1, 1200) * 1000
        end_timestamp = start_timestamp + duration

        charge = calculate_charge(call_type, duration)
        
        cdr_data = {
            'CallID': str(i),
            'StartTime': start_timestamp,
            'EndTime': end_timestamp,
            'Duration': int(duration/1000),
            'CallingParty': random.choice(SPECIAL_NUMBERS) if random.random() < 0.1 else generate_massachusetts_phone_number(),
            'CalledParty': generate_massachusetts_phone_number(),
            'TowerID': random.choice(tower_ids),
            'Status': random.choice(STATUSES),
            'Type': call_type,
            'Charge': charge,
            'Latitude': get_tower_location(random.choice(tower_ids))[0],
            'Longitude': get_tower_location(random.choice(tower_ids))[1],
            'Carrier': random.choice(CARRIERS),
            'NetworkType': random.choice(['4G', '5G']),
            'Usage': calculate_data_usage(call_type)
        }

        batch.append(cdr_data)

        i += 1
        print(f"\r{spinners[i % 4]} Producing record {i}", end='', flush=True)

        if i % 1000 == 0:
            print(f"\n{i} records have been produced!")
            for record in batch:  # Sending batch of 1000 records to Kafka
                avro_producer.produce(topic='Call_Detail_Record', value=record)
            avro_producer.flush()
            batch = []  # Clear the batch

    # Send the remaining records in the batch to Kafka (if any)
    for record in batch:
        avro_producer.produce(topic='Call_Detail_Record', value=record)
    avro_producer.flush()
    
    print(f"\nAll {i} records have been produced and flushed to Kafka.")

if __name__ == '__main__':
    generate_cdr_data(2000000)
