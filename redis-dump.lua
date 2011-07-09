#!/usr/bin/env lua
--------------------------------------------------------------------
-- usage : ./redis-mport.lua [inPutFile] [ip] [port] [whichDB]
-- function: export the redis db data to a lua data file.
------------------------------------------------------------------

require 'redis'

local seri_string = function (str)
	return ("%q"):format(self);
end

--The table like array in C
local seri_itable = function(t)
	local res = "{";
    for k, v in pairs(t) do 
    	res = ('%s"%s",'):format(res, v)
    end

	res = ('%s}'):format(res); 
	return res;
end

--The table is hash Table
local seri_stable = function(t)
	local res = "{";
	for k,v in pairs(t)do
    	res = ('%s"%s",'):format(res, k)
    	res = ('%s"%s",'):format(res, v)
	end
		
	res = ('%s}'):format(res); 
	return res;
end

--The table's element is also a table ,and both are array like C
local seri_itable2 = function(t)
	local res = "{";
    for k, v in pairs(t) do 
    	for k1,v1 in pairs(v) do
    		res = ('%s%s,'):format(res, v1)
		end
    end

	res = ('%s}'):format(res); 
	return res;
end


print("+++++++++++++++++++++++++++++++");
print("++++++++  DUMP DATA start ...++");
print("+++++++++++++++++++++++++++++++");

-- init database and dump file 
local ip   = arg[1] or '127.0.0.1';
local port = arg[2] or 6379;
local which= arg[3] or 0; 
local fileName = arg[4] or 'dump.rds'

-- Make sure not loose data
local dumpFile = io.open(fileName,"r");
if nil ~= dumpFile then 
	print("The dump file ["..fileName.."] is exist, rewrite it ? Y:N")
	local usrChar = io.read();
	if usrChar ~= "Y" and usrChar ~= "y" then
  		dumpFile:close(); 
		return;
	end
  dumpFile:close(); 
end

local dumpFile = io.open(fileName,"w");

--connecting the database
local redis = Redis.connect(ip,port);
redis:select(which);


--get all keys
local count = 1;
local keys = redis:keys('*');
for i,key in pairs(keys) do
    io.write(".");
	if count >= 20 then
		io.write("\n");
		count = 1;
	end			
	
    dumpFile:write("Entry {\n");
	
	dumpFile:write("key=");
	dumpFile:write(("%q,\n"):format(key));
	
	local valueType = redis:type(key);
	local value;
	
	if valueType == "string" then
	 	value = redis:get(key);
	elseif valueType == "set" then
		value = redis:smembers(key);
		value = seri_itable(value);
	elseif valueType == "zset" then
		value = redis:zrange(key,0,-1,'withscores');
		value = seri_itable2(value);
    elseif valueType == "list" then
		value = redis:lrange(key,0,-1);   
		value = seri_itable(value);
	elseif valueType == "hash" then
		value = redis:hgetall(key);
		value = seri_stable(value);
	end
	

	dumpFile:write("type=");
	dumpFile:write(("%q,\n"):format(valueType));

	dumpFile:write("value=");
	dumpFile:write(("%q,\n"):format(value));

	dumpFile:write("}\n\n");

	count = count + 1;
end
	
dumpFile:close();
redis:quit();

print("");
print("+++++++++++++++++++++++++++++++");
print("++++++++  DUMP DATA SUCCESS  ++");
print("+++++++++++++++++++++++++++++++");
