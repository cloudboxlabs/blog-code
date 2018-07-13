import unittest

import redis
import requests

from app import db
from app.models import Post


class TestRoutes(unittest.TestCase):

    def setUp(self):
        self.redis = redis.StrictRedis(host='redis', port=6379, db=0)

    def test_save_post_draft(self):
        response = requests.post('http://web_app:5000/save/', data={
            'title': 'Test integration draft title',
            'body': 'Test integration draft body',
            'date': '2018-07-01',
            'author': 'test_user',
            'action': 'draft'
        })

        self.assertEqual(response.status_code, 200)
        redis_value = self.redis.get('Test integration draft title')
        self.assertEqual(redis_value, 'Test integration draft body')

    def test_save_post(self):
        response = requests.post('http://web_app:5000/save/', data={
            'title': 'Test integration final title',
            'body': 'Test integration final body',
            'date': '2018-07-01',
            'author': 'test_user',
            'action': 'save'
        })

        self.assertEqual(response.status_code, 200)

        posts = db.session.query(Post).all()
        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0].title, 'Test integration final title')
        self.assertEqual(posts[0].body, 'Test integration final body')
