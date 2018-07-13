import time

import requests
from google.protobuf.json_format import MessageToJson
from confluent_kafka import Producer

import gtfs_realtime_pb2


class MTARealTime(object):

    def __init__(self):
        with open('.mta_api_key', 'r') as key_in:
            self.api_key = key_in.read().strip()

        self.mta_api_url = 'http://datamine.mta.info/mta_esi.php?key={}&feed_id=1'.format(
            self.api_key)
        self.kafka_topic = 'test'
        self.kafka_producer = Producer({'bootstrap.servers': 'localhost:9092'})

    def produce_trip_updates(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        response = requests.get(self.mta_api_url)
        feed.ParseFromString(response.content)

        for entity in feed.entity:
            if entity.HasField('trip_update'):
                update_json = MessageToJson(entity.trip_update)
                self.kafka_producer.produce(
                    self.kafka_topic, update_json.encode('utf-8'))

        self.kafka_producer.flush()

    def run(self):
        while True:
            self.produce_trip_updates()
            time.sleep(30)


if __name__ == '__main__':
    MTARealTime().run()

