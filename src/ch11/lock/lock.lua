local lock_key = KEYS[1]
local lock_timeout = ARGV[1]
local identifier = ARGV[2]

if redis.call('exists', lock_key) == 0 then
	return redis.call('setex', lock_key, lock_timeout, identifier)
end
return 0