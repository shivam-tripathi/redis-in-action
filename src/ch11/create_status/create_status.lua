local user_key = KEYS[1]
local status_counter = KEYS[2]

local login = redis.call('hget', user_key, 'login')
if not login then
	return 0
end

local id = redis.call('incr', status_counter)
local status_key = string.format('status:%s', id)

redis.call('hset', status_key, 'login', login, 'id', id, unpack(ARGV))
redis.call('hincrby', user_key, 'posts', 1)
return status_key