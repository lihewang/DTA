--msa volume
redis.call("select","2")
local z = redis.call("exists",KEYS[1])
local y = redis.call("exists",KEYS[2])
local vc = 0
local vp = 0
if z == 1 then
 vc = redis.call("get",KEYS[1])
end
if y == 1 then
 vp = redis.call("get",KEYS[2])
end
local v = vp*(1-KEYS[3])+vc*KEYS[3]
redis.call("set",KEYS[1],v)
return vc .. "," .. vp .. "," .. v
