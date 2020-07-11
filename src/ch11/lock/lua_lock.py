from src.redis_conn import Redis
from src.ch11.script_loader import ScriptLoader
import uuid
import time
import math

class LuaLock:
	def __init__(self, conn = Redis().conn):
		self.conn = conn

	def acquire_lock_with_timeout(self, lock_name, acquire_timeout = 10, lock_timeout = 10):
		identifier = str(uuid.uuid4())
		lock_key = f'lock:{lock_name}'
		lock_timeout = int(math.ceil(lock_timeout))
		abort_timestamp = time.time() + acquire_timeout

		while time.time() < abort_timestamp:
			if self.conn.setnx(lock_key, identifier):
				self.conn.expire(lock_key, lock_timeout)
				return identifier
			elif self.conn.ttl(lock_key):
				self.conn.expire(lock_key, lock_timeout)
			time.sleep(0.01)

		return None

	def acquire_lua_lock_with_timeout(self, lock_name, acquire_timeout = 10, lock_timeout = 10):
		identifier = str(uuid.uuid4())
		lock_cmd = ScriptLoader(open('./src/ch11/lock/lock.lua').read())
		lock_key = f'lock:{lock_name}'
		abort_timestamp = time.time() + acquire_timeout
		lock_timeout = int(math.ceil(lock_timeout))

		keys = [lock_key]
		args = [lock_timeout, identifier]
		print('\n\n\n', keys, args)
		while time.time() < abort_timestamp:
			ret = lock_cmd.run(keys = keys, args = args)
			print(ret)
			locked = ret == b'OK'
			if locked:
				return identifier
			time.sleep(0.01 * (not locked))
		return None

