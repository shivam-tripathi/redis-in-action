import redis
import uuid
import time
from src.ch06.distributed_locking.simple_lock import SimpleLock
from src.redis_conn import Redis

'''
- We can use basic semaphore if it's ok to
	- use system clocks
	- not refresh semaphore
	- going over the limit for processes acquiring semaphore once in a while
- We can use fair semaphore if
	- system clock are close to accurate in range of 1 seconds
	- going over the limit for processes acquiring semaphore once in a while
- We have to use fair semaphore with lock if we want it to be correct every time
'''

class SimpleSemaphore:
	def __init__(self, semaphore_name, limit, conn = Redis().conn):
		self.conn = conn
		self.semaphore_name = semaphore_name
		self.limit = limit

	def acquire_semaphore(self, timeout=10):
		identifier = str(uuid.uuid4())
		now = time.time()
		pipeline = self.conn.pipeline(True)
		pipeline.zremrangebyscore(self.semaphore_name, '-inf', now - timeout) # Handle timeouts
		pipeline.zadd(self.semaphore_name, { identifier: now })
		pipeline.zrank(self.semaphore_name, identifier)
		if pipeline.execute()[-1] < self.limit:
			return identifier

		self.conn.zrem(self.semaphore_name, identifier)
		return None

	def release_semaphore(self, identifier):
		return self.conn.zrem(self.semaphore_name, identifier)

	def acquire_fair_semaphore(self, timeout=10):
		identifier = str(uuid.uuid4())
		semaphore_zset = f'{self.semaphore_name}:owner'
		semaphore_counter = f'{self.semaphore_name}:counter'

		now = time.time()
		pipeline = self.conn.pipeline(True)

		# Remove timed out entries
		pipeline.zremrangebyscore(self.semaphore_name, '-inf', now - timeout)
		pipeline.zinterstore(semaphore_zset, { semaphore_zset: 1, self.semaphore_name: 0 })
		# Get new counter
		pipeline.incr(semaphore_counter)
		counter = pipeline.execute()[-1]

		# Add and check if the new identifier falls into limit
		pipeline.zadd(self.semaphore_name, identifier, now)
		pipeline.zadd(self.semaphore_zset, identifier, counter)
		pipeline.zrank(self.semaphore_zset, identifier)

		if pipeline.execute()[-1] < self.limit:
			return identifier

		# If out of limit, remove the resource
		pipeline.zrem(self.semaphore_name, identifier)
		pipeline.zrem(semaphore_zset, identifier)
		pipeline.execute()
		return None

	def release_fair_semaphore(self, identifier):
		semaphore_zset = f'{self.semaphore_name}:owner'
		pipeline = self.conn.pipeline(True)
		pipeline.zrem(self.semaphore_name, identifier)
		pipeline.zrem(semaphore_zset, identifier) # Not deleting it might cause race condition
		return pipeline.execute()[0]

	def refresh_fair_semaphore(self, identifier):
		if self.conn.zadd(self.semaphore_name, identifier, time.time()): # returns 0 if key exists
			self.release_fair_semaphore(self.semaphore_name, identifier) # lost the semaphore, remove
			return False
		return True # still have the semaphore with updated timestamp

	def acquire_fair_semaphore_with_lock(self, timeout):
		simple_lock = SimpleLock(conn = self.conn)
		identifier = simple_lock.acquire_lock(lockname=self.semaphore_name, acquire_timeout=0.01)
		if identifier:
			try:
				return acquire_fair_semaphore(identifier)
			finally:
				release_fair_semaphore(identifier)
