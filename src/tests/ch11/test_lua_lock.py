from src.redis_conn import Redis
from src.ch11.lock.lua_lock import LuaLock
from unittest import TestCase

class TestLuaLock(TestCase):
	def setUp(self):
		self.conn = Redis().conn
		self.lua_lock = LuaLock(self.conn)
		self.id = None

	def test_lua_locking(self):
		id = self.lua_lock.acquire_lua_lock_with_timeout('lua')
		self.assertEqual(self.conn.get(f'lock:lua').decode('utf-8'), id)
		assert self.lua_lock.release_lua_lock('lua', id) is 1

