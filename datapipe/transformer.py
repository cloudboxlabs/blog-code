import json

import boto3
import psycopg2

BUCKET_NAME = 'cloudboxlabs'
QUEUE_NAME = 'cloudboxlabs_datapipe_tutorial'


class Transformer(object):
    def __init__(self):
        self.s3 = boto3.client('s3')
        sqs = boto3.resource('sqs')
        self.queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)

        self.conn = psycopg2.connect('connection string for postgres RDS')
        self.cursor = self.conn.cursor()

    def run(self):
        for message in self.queue.receive_messages(MessageAttributeNames=['s3_path']):
            if message.message_attributes:
                s3_path = message.message_attributes.get('s3_path').get('StringValue')
                response = self.s3.get_object(Bucket=BUCKET_NAME,
                                              Key=s3_path.split('/')[-1])

                data = json.loads(response['Body'].read())

                for post in data:
                    self.cursor.execute(
                        'insert into posts (id, userId, title, body) '
                        'values (%s, %s, %s, %s)',
                        (post['id'], post['userId'], post['title'], post['body']))

            message.delete()
        self.conn.commit()


if __name__ == '__main__':
    Transformer().run()
