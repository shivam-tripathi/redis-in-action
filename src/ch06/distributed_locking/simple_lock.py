import redis
import uuid
import time
from src.redis_conn import Redis

class SimpleLock:

	'''
	Basic structures:
		- Marketplace `market.` zset
		- User `users:{user_id}` hash
		- Inventory `inventory:{user_id}` set
	'''

	def __init__(self, conn = Redis().conn):
		self.conn = conn

	# Try to acquire for 10 seconds before giving up and returning
	def acquire_lock(self, lockname, acquire_timeout=10):
		identifier = str(uuid.uuid4())
		end = time.time() + acquire_timeout
		while time.time() < end:
			acq = self.conn.setnx(self.get_lock_key(lockname), identifier)
			if acq:
				return identifier
			time.sleep(.001)
		return False

	def release_lock(self, lockname, lockvalue):
		pipeline = self.conn.pipeline(True)
		lockkey = self.get_lock_key(lockname)
		while True:
			try:
				pipeline.watch(lockkey)
				lockvaluesaved = pipeline.get(lockkey)
				if pipeline.get(lockkey).decode('utf-8') == lockvalue:
					pipeline.multi()
					pipeline.delete(lockkey)
					pipeline.execute()
					return True # Lock consistent, released successfully
				pipeline.unwatch()
				break
			except redis.exceptions.WatchError:
				pass
		return False # Lost the lock


'''
Market place simulator to demonstrate locks
'''
class MarketplaceSim():
	def __init__(self):
		self.simple_lock = SimpleLock()

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

		# print(buyer, seller, item, buyer_inventory, market, end)

		locked = self.simple_lock.acquire_lock(market, 30)
		if not locked:
			return False

		pipeline = self.conn.pipeline(True)
		try:
			while time.time() < end:
				print (time.time(), end)
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
			self.simple_lock.release_lock(market, locked)
