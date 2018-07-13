import csv
from collections import defaultdict
import json

import arrow
from confluent_kafka import Consumer, KafkaError


class MTATrainTracker(object):

    def __init__(self):
        self.kafka_consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test_consumer_group',
            'default.topic.config': {
                'auto.offset.reset': 'smallest'
            }
        })
        self.kafka_topic = 'test'

        # subway line number -> (stop_id, direction) -> next arrival time
        self.arrival_times = defaultdict(lambda: defaultdict(lambda: -1))

        self.stations = {}
        with open('static/mta_stations.csv') as csvf:
            reader = csv.DictReader(csvf)
            for row in reader:
                self.stations[row['GTFS Stop ID']] = row['Stop Name']

    def process_message(self, message):
        trip_update = json.loads(message)

        trip_header = trip_update.get('trip')
        if not trip_header:
            return

        route_id = trip_header['routeId']
        stop_time_updates = trip_update.get('stopTimeUpdate')
        if not stop_time_updates:
            return

        for update in stop_time_updates:
            if 'arrival' not in update or 'stopId' not in update:
                continue

            stop_id, direction = update['stopId'][0:3], update['stopId'][3:]
            new_arrival_ts = int(update['arrival']['time'])

            next_arrival_ts = self.arrival_times[route_id][(stop_id, direction)]
            now = arrow.now(tz='US/Eastern')

            if new_arrival_ts >= now.timestamp and \
                    (next_arrival_ts == -1 or new_arrival_ts < next_arrival_ts):
                self.arrival_times[route_id][(stop_id, direction)] = new_arrival_ts

                # convert time delta to minutes
                time_delta = arrow.get(new_arrival_ts) - now
                minutes = divmod(divmod(time_delta.seconds, 3600)[1], 60)[0]
                print('Next {} bound {} train will arrive at station {} in {} minutes'.format(
                    direction, route_id, self.stations[stop_id], minutes))

    def run(self):
        self.kafka_consumer.subscribe([self.kafka_topic])

        while True:
            msg = self.kafka_consumer.poll(1.0)

            if msg is None or not msg.value():
                continue
            if msg.error() and msg.error().code() != KafkaError._PARTITION_EOF:
                raise ValueError('Kafka consumer exception: {}'.format(msg.error()))

            msg = msg.value()
            self.process_message(msg.decode('utf-8'))


if __name__ == '__main__':
    MTATrainTracker().run()
