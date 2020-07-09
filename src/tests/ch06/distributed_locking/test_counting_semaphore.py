from unittest import TestCase
from src.ch06.distributed_locking.counting_semaphore import SimpleSemaphore
from src.redis_conn import Redis

class TestSimpleSemaphore(TestCase):
	def setUp(self):
		self.conn = Redis().conn
		self.semaphore_name = 'sem'
		self.semaphore = SimpleSemaphore(semaphore_name=self.semaphore_name, limit=3, conn=self.conn)

	def test_simple_semaphore(self):
		limit = 3
		sem1 = self.semaphore.acquire_semaphore()
		sem2 = self.semaphore.acquire_semaphore()
		sem3 = self.semaphore.acquire_semaphore()
		sem4 = self.semaphore.acquire_semaphore()

		assert sem1 is not False
		assert sem2 is not False
		assert sem3 is not False
		assert sem4 is None

		assert self.semaphore.release_semaphore(sem1) is 1
		assert self.semaphore.release_semaphore(sem2) is 1
		assert self.semaphore.release_semaphore(sem3) is 1

	def tearDown(self):
		self.conn.delete(self.semaphore_name)
