import json
import time

from producer import KafkaProducer


class ProducerServer(object):

    def __init__(self, input_file, topic, **kwargs):
        self.input_file = input_file
        self.producer = KafkaProducer(topic)

    def generate_data(self):
        with open(self.input_file) as data_file:
            print(f"Opened input file {self.input_file}")
            sfpd_crime_data_json_list = json.loads(data_file.read())
            for crime_data in sfpd_crime_data_json_list:
                print(f'publish crime data ID: {crime_data["crime_id"]}')
                self.producer.produce(json.dumps(crime_data).encode('utf-8'))
                time.sleep(1)
