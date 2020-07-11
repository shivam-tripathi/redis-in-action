from src.redis_conn import Redis
from src.ch11.script_loader import ScriptLoader
import time

class Status:
	def __init__(self, conn = Redis().conn):
		self.conn = conn
		self.status_counter = 'status:id'

	def create(self, user_id, message, **data):
		user_key = f'user:{user_id}'
		login = self.conn.hget(user_key, 'login')
		if not login:
			return None
		id = self.conn.incr(self.status_counter)
		status_key = f'status:{id}'

		data.update({
			'message': message,
			'posted': time.time(),
			'id': id,
			'user_id': user_id,
			'login': login
		})

		pipeline = self.conn.pipeline(True)
		pipeline.hmset(status_key, data)
		pipeline.hincrby(user_key, 'posts', 1)
		pipeline.execute()
		return id

	def login_user(self, user_id):
		self.conn.hset(f'user:{user_id}', 'login', 1)

	def lua_create(self, user_id, message, **data):
		args = [
			'message', message,
			'posted', time.time(),
			'user_id', user_id,
		]

		for key, value in data.items():
			args.append(key)
			args.append(value)

		user_key = f'user:{user_id}'
		script_file = open('./src/ch11/create_status/create_status.lua')
		lua_script = script_file.read()
		script_file.close()
		create_status_cmd = ScriptLoader.load(lua_script)
		keys=[user_key, self.status_counter]
		status_key = create_status_cmd.run(keys = keys, args = args)
		return user_key, status_key, self.status_counter

