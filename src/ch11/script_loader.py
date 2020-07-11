from src.redis_conn import Redis
from redis.exceptions import ResponseError as RedisResponseError
import json

class ScriptLoader:
	def __init__(self, script, conn = Redis().conn):
		self.script = script
		self.conn = conn
		self.sha = self.conn.execute_command('SCRIPT', 'LOAD', self.script, parse='LOAD')

	@staticmethod
	def load(script, conn = Redis().conn):
		return ScriptLoader(script, conn)

	# keys will be checked if they are on the same server, will return error if not
	def run(self, keys = [], args = [], force_eval = False):
		print({ 'keys': keys, 'args': args })
		if not force_eval:
			try:
				if not self.sha:
					self.sha = self.conn.execute_command('SCRIPT', 'LOAD', self.script, parse='LOAD').decode('utf-8')
				return self.conn.execute_command('EVALSHA', self.sha, len(keys), *(keys + args))
			# If `no script` error, evaluate directly. This can happen if:
			# 	- redis server was restarted
			# 	- command `script flush` was executed
			# 	- different `conn` were given in args
			except RedisResponseError as msg:
				if not msg.args[0].startswith('NOSCRIPT'):
					raise

		return self.conn.execute_command('EVAL', self.script, len(keys), *(keys + args))


'''
Allowed return types: bytes, string, int or float

Lua value                         | What happens during conversion to Python (or Node)
----------------------------------------------------------------------------------------------------
true                              | Turns into 1
# false                           | (Doesn't work with latest redis wrapper) Node -> Turns into None
# nil                             | (Doesn't work with latest redis wrapper)
                                  | Node -> *stops remaining values in a table from being returned*
1.5 (or any other float)          | Fractional part is discarded, turning it into an integer
1e30 (or any other large float)	  | Is turned into the minimum integer for your version of Python
"strings"	                      | Unchanged
1 (or any other integer +/-2^53-1 |	Integer is returned unchanged

'''

'''
SCRIPT KILL (read only, exceeded lua-time-limit)
SHUTDOWN NOSAVE (write leading to inconsistent state - revert to last snapshot)

Do not use keys in lua script which is not being sent in execute_command, else it will be
incompatible with sharded cluster mode
'''
