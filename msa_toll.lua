--msa volume
redis.call("select","7")
local z = redis.call("exists",KEYS[1])
local vp = 0
if z == 1 then
 vp = redis.call("get",KEYS[1])
end
local v = vp*(1-KEYS[3])+KEYS[2]*KEYS[3]
redis.call("set",KEYS[1],v)
return vp .. "," .. v 
