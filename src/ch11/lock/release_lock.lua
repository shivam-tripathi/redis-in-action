local lock_name = KEYS[1]
local identifer = ARGV[1]
if redis.call('get', lock_name) == identifer then
	return redis.call('del', lock_name)
end
return 0