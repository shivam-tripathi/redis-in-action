import redis
import uuid

class AddressBook:
	# Ordered list of encoded characters: '`abcdefghijklmnopqrstuvwxyz{'
	valid_characters = [chr(i) for i in range(ord('a') - 1, ord('z') + 2)]

	def __init__(self):
		self.conn = redis.Redis(host='localhost', port=6379, db=0)

	def get_zset_name(self, guild):
		return 'members:' + guild

	def find_prefix_range(self, prefix):
		pos = self.valid_characters.index(prefix[-1]) # Position of last character
		# begin: shift last character by one and add '{' in the end
		begin = prefix[:-1] + self.valid_characters[pos - 1] + self.valid_characters[-1]
		# end:
		end = prefix + self.valid_characters[-1]
		return begin, end

	def autocomplete_on_prefix(self, guild, prefix):
		start, end = find_prefix_range(prefix)

		# To avoid two users using same start and end keys (which are inserted and then removed later)
		identifier = uuid.uuid4()
		start += identifier
		end += identifier
		zset_name = get_zset_name(guild)
		self.conn.zadd(zset_name, start, 0, end, 0)
		pipeline = self.conn.pipeline(True) # transaction

		# Loop till success
		while True:
			try:
				pipeline.watch(zset_name) # After watching, pipeline gets into immediate mode
				start_index = pipeline.zrank(zset_name, start)
				end_index = pipeline.zrank(zset_name, end)
				stop_index = min(start_index + 9, end_index - 2) # Some random range
				pipeline.multi() # Start buffering again
				# First delete start, end because we donot want them in our results
				pipeline.zrem(zset_name, start, end)
				# Fetch results after deletion
				pipeline.zrange(zset_name, start_index, stop_index)
				items = pipeline.execute()[-1]
				break
			except e:
				continue # Retry on error
			finally:
				pipeline.reset()

	def join_guild(self, guild, user):
		self.conn.zadd(get_zset_name(guild), user, 0)

	def leave_guild(self, guild, user):
		self.conn.zrem(get_zset_name(guild), user)


address_book = AddressBook()
print(address_book.find_prefix_range('aba'))
