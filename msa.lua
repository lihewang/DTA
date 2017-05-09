--msa volume
redis.call("select","2")
local z = redis.call("exists",KEYS[1])
if z == 1 then
 local vc = redis.call("get",KEYS[1])
 local vp = redis.call("get",KEYS[2])
 local v = vp*(1-1/KEYS[3])+vc/KEYS[3]
 redis.call("set",KEYS[1],v)
 return "c=" .. vc .. ",p=" .. vp .. ",v=" .. v
end
return "Not Exists"