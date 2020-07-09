import redis

class RecentContactAc:
	def __init__(self):
		self.conn = redis.Redis(host='localhost', port=6379, db=0)

	@staticmethod
	def ac_list(user):
		return 'recent:' + user

	def add_update_contact(self, user, contact):
		ac_list = self.ac_list(user)
		transaction = self.conn.pipeline(True)
		transaction.lrem(ac_list, value=contact, count=0)
		transaction.lpush(ac_list, contact)
		transaction.ltrim(ac_list, 0, 99)
		transaction.execute()

	def remove_contact(self, user, contact):
		ac_list = self.ac_list(user)
		self.conn.lrem(ac_list, contact)

	def fetch_autocomplete_list(self, user, prefix):
		ac_list = self.ac_list(user)
		items = self.conn.lrange(ac_list, 0, -1)
		return [item for item in items if item.decode('ascii').lower().startswith(prefix)]
