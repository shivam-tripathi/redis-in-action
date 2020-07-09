import redis
import uuid
import time
from src.redis_conn import Redis

class TimeoutLock:
	def __init__(self, conn = Redis().conn):
		self.conn = redis.Redis()

	@staticmethod
	def get_lock_key(lockname):
		return f'lock:{lockname}'

	def acquire_lock_with_timeout(self, lockname, acquire_timeout, lock_timeout):
		identifier = uuid.uuid4()
		lockkey = get_lock_key(lockname)
		lock_timeout_int = int(math.ceil(lock_timeout))
		acquire_timeout_timestamp = time.time() + acquire_timeout

		while time.time() < acquire_timeout_timestamp:
			if conn.setnx(lockkey, identifier):
				conn.expire(lockkey, lock_timeout_int)
				return identifier
			elif not conn.ttl(lockkey):
				conn.expire(lockkey, lock_timeout_int)

			sleep(0.001)

		return False
