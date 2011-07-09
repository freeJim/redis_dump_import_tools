#!/usr/bin/env lua
--------------------------------------------------------------------
-- usage : ./redis-import.lua dataFile [ip] [port] [whichDB] 
-- function: import the lua data file dumped by redis-dump.lua 
-- to the redis DB.
------------------------------------------------------------------

require 'redis'

local redis = nil;
local count = 1;

------------------------------------------------------------------------
-- 将序列化的字符串加载到内存中，生成lua对象
-- @param self  被处理字符串
-- @return lua对象
------------------------------------------------------------------------
local unseri = function (self)
     if not self then
         return nil
     end
    local func = loadstring(("return %s"):format(self))
     if not func then
         error(("unserialize fails %s %s"):format(debug.traceback(), self))
     end
     return func()
end

function import_set(key,t)
	for k,v in ipairs(t) do
		redis:sadd(key,v);
	end
end

function import_list(key,t)
	for k,v in ipairs(t) do
		redis:rpush(key,v);
	end
end

function import_zset(key,t)
	for i=1,#t,2 do
		redis:zadd(key,t[i+1],t[i]);	
	end
end

function import_hash(key,t)
	for i=1,#t,2 do
		redis:hset(key,t[i],t[i+1]);	
	end
end

function import(key,type,value)
	if type == "string" then
    	redis:set(key,value); 
	else
		local t = unseri(value);

		if type == "set" then
			import_set(key,t);
		elseif type == "zset" then
			import_zset(key,t);
		elseif type == "hash"  then
			import_hash(key,t);
		elseif type == "list"  then
			import_list(key,t);
		else 
			print "[ERROR:] UNKNOWN TYPE";
	    	return;
		end
	end	
end

function Entry(self)
	local key  = "";
	local type = "";
	local value= "";

	io.write(".");
	if count >= 20 then
		io.write("\n");
		count = 1;
	end
	count = count + 1;
  
	for k,v in pairs(self) do
	--	print(k,v)
		if k == "key" then
			key = v;
		elseif k == "type" then
			type = v;
		elseif k == "value" then
			value = v;
		else 
			print "[ERROR:] UNKNOWN DATA";
			return;		
		end
	end
    
	import(key,type,value);
end

print("+++++++++++++++++++++++++++++++");
print("++++++  IMPORT DATA start ...++");
print("+++++++++++++++++++++++++++++++");

-- init database and data file 
local ip   = arg[2] or '127.0.0.1';
local port = arg[3] or 6379;
local which= arg[4] or 0; 
local fileName = arg[1] or 'dump.rds'


--connecting the database
redis = Redis.connect(ip,port);
redis:select(which);

dofile(fileName);
	
redis:quit();

print("");
print("+++++++++++++++++++++++++++++++");
print("++++++++  DUMP DATA SUCCESS  ++");
print("+++++++++++++++++++++++++++++++");

