--set volume
redis.call("select","2")
local z = redis.call("exists",KEYS[1])
if z == 1 then
 local v = redis.call("get",KEYS[1])
 redis.call("set",KEYS[1],KEYS[2]+v)
else
 redis.call("set",KEYS[1],KEYS[2])
end
return KEYS[1]