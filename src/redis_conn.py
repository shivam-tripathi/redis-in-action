import redis

# monostate
class Redis:
	__state = { "conn": redis.Redis() }
	def __init__(self):
		self.__dict__ = self.__state
