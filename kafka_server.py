from pathlib import Path

import producer_server
from config import CRIME_DATA_TOPIC_NAME


def run_kafka_server():
    input_file = f'{Path(__file__).parents[0]}/police-department-calls-for-service.json'

    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic=CRIME_DATA_TOPIC_NAME,
    )
    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
