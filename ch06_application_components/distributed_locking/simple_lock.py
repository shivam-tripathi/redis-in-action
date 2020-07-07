import redis
import uuid
import time

class SimpleLock:

	'''
	Basic structures:
		- Marketplace `market.` zset
		- User `users:{user_id}` hash
		- Inventory `inventory:{user_id}` set
	'''

	def __init__(self):
		self.conn = redis.Redis()

	# Try to acquire for 10 seconds before giving up and returning
	def acquire_lock(self, lockname, acquire_timeout=10):
		identifier = str(uuid.uuid4())
		end = time.time() + acquire_timeout
		while time.time() < end:
			if self.conn.setnx(get_lock_key(lockname, identifier):
				return identifier
			time.sleep(.001)
		return False

	def release_lock(self, lockname, lockvalue):
		pipeline = self.conn.pipeline(True)
		lockkey = get_lock_key(lockname)
		while True:
			try:
				pipeline.watch(lockkey)
				if pipeline.get(lockkey) == lockvalue:
					pipeline.multi()
					pipe.delete(lockkey)
					pipe.execute()
					return True # Lock consistent, released successfully
				pipeline.unwatch(lockkey)
				break
			except redis.exceptions.WatchError:
				pass
		return False # Lost the lock

	@staticmethod
	def get_lock_key(lockname):
		return f'lock:{lockname}'

	@staticmethod
	def get_user_key(user_id):
		return f'users:{user_id}'

	@staticmethod
	def get_item_key(item_id, owner_id):
		return f'{item_id}.{owner_id}'

	@staticmethod
	def get_inventory_key(owner_id):
		return f'inventory:{owner_id}'

	@staticmethod
	def get_market_key():
		return 'market.'

	def purchase_item_with_lock(self, buyer_id, item_id, seller_id):
		buyer = self.get_user_key(buyer_id)
		seller = self.get_user_key(seller_id)
		item = self.get_item_key(item_id, seller_id)
		buyer_inventory = self.get_inventory_key(buyer_id)
		market = self.get_market_key();
		end = time.time() + 30

		locked = acquire_lock(market, 30)
		if not locked:
			return False

		pipeline = self.conn.pipeline(True)
		try:
			while time.time() < end:
				try:
					pipeline.watch(buyer)
					pipeline.multi()
					pipeline.zscore(market, item)
					pipeline.hget(buyer, 'funds')
					price, funds = pipeline.execute()

					if price is None or price > funds:
						pipeline.unwatch()
						return None

					pipeline.multi()
					pipeline.hincrby(buyer, 'funds', int(-price))
					pipeline.hincrby(seller, 'funds', int(price))
					pipeline.sadd(buyer_inventory, item)
					pipeline.zrem(market, item)
					pipeline.execute()
					pipeline.unwatch() # required?
					return True
				except redis.exceptions.WatchError:
            		pass
		finally:
			release_lock(market, locked)
