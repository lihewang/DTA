--pop job
redis.call("select",KEYS[1])
local z = redis.call("lpop",KEYS[2])
if (type(z) == "boolean" and not z) or z == "done" then
 redis.call("rpush", KEYS[2], "done")
end
return z