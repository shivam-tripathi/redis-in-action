from src.redis_conn import Redis
from src.ch11.create_status.status import Status
from unittest import TestCase

class TestCreateStatus(TestCase):
	def setUp(self):
		self.conn = Redis().conn
		self.status = Status(self.conn)
		self.status.login_user('shivam')
		self.del_key = []

	def test_create_status_lua(self):
		user_key, status_key, status_counter = self.status.lua_create('shivam', 'new status', posted_on='diwali')
		pipeline = self.conn.pipeline(True)
		pipeline.hgetall(user_key)
		pipeline.hgetall(status_key)
		pipeline.get(status_counter)
		user, status, counter = pipeline.execute()
		self.assertEqual(status[b'id'], counter)
		self.assertEqual(status[b'user_id'].decode('utf-8'), 'shivam')
		self.assertEqual(user[b'login'].decode('utf-8'), '1')
		assert True is True

	def tearDown(self):
		self.conn.delete('status:id', 'status:1', 'user:shivam')