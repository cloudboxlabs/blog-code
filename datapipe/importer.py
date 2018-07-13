import json
import logging
from datetime import datetime

import boto3
import requests


EXTERNAL_API_URL = 'https://jsonplaceholder.typicode.com/posts'
BUCKET_NAME = 'cloudboxlabs'
QUEUE_NAME = 'cloudboxlabs_datapipe_tutorial'


class Importer(object):
    def __init__(self):
        self.s3 = boto3.resource('s3')
        sqs = boto3.resource('sqs')
        self.queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)

    def run(self):
        response = requests.get(EXTERNAL_API_URL)

        file_name = 'posts_{}.json'.format(datetime.strftime(datetime.now(), '%Y%m%d%H%M%S'))
        with open(file_name, 'w') as file_obj:
            file_obj.write(json.dumps(response.json()))

        logging.info('Received API response')

        with open(file_name, 'r') as file_obj:
            self.s3.Bucket(BUCKET_NAME).put_object(Key=file_name, Body=file_obj)

        logging.info('Put json into S3')

        self.queue.send_message(MessageBody='post', MessageAttributes={
            's3_path': {
                'StringValue': 's3://{}/{}'.format(BUCKET_NAME, file_name),
                'DataType': 'String'
            }
        })


if __name__ == '__main__':
    Importer().run()

