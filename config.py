import os

BROKER_URL = os.environ.get('BROKER_URL', 'localhost:9092')
NUM_PARTITIONS = 2
NUM_REPLICAS = 2

CRIME_DATA_TOPIC_NAME = 'sfpd.entity.crime_data'
