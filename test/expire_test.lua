-- =========================================================================
-- expire_bin.lua tests
-- =========================================================================

local eb = require('expire_bin');
local EXP_ID = "expbin_ttl";
local EXP_DATA = "data";

local function is_expbin_test(rec)
	local meth = "is_expbin_test";
	local badBin = list();
	local goodBin = map { expbin_ttl = 20, data = 12};
	if (eb.is_expbin(goodBin) == false) then
		info ("[EXPIREBIN TEST] <%s> goodBin is %s", meth, tostring(eb.is_expbin(goodBin)));
		return false;
	end
	if (eb.is_expbin(badBin) == true) then
		info("[EXPIREBIN TEST] <%s> badBin is %s", meth, tostring(eb.is_expbin(badBin)));
		return false;
	end
	return true;
end

local function valid_time_test(rec)
	local meth = "valid_time_test";
	local pass = true;
	if (eb.valid_time(-1, 0) == false) then
		info("[EXPIREBIN TEST] <%s>  -1, 0 failed", meth);
		pass = false;
	end
	if (eb.valid_time(20, 10) == true) then
		info("[EXPIREBIN TEST] <%s>  20, 10 failed", meth);
		pass = false;
	end
	if (eb.valid_time(200, 0) == false) then
		info("[EXPIREBIN TEST] <%s>  200, 0 failed", meth);
		pass = false;
	end
	if (eb.valid_time(200, 1000) == false) then
		info("[EXPIREBIN TEST] <%s>  200, 1000 failed", meth);
		pass = false;
	end
	return pass;
end

local function not_expired_test(rec)
	local meth = "not_expired_test";
	local pass = true;
	if (eb.not_expired(os.time() - 10) == true) then
		info("[EXPIREBIN TEST] <%s>  past time test failed");
		pass = false;
	end
	if (eb.not_expired(os.time() + 10) == false) then
		info("[EXPIREBIN TEST] <%s>  future time test failed");
		pass = false;
	end
	return pass;
end

local function get_bin_test(rec)
	local meth = "get_bin_test";
	local pass = true;
	local goodBin = map { expbin_ttl = os.time() + 100, data = 12 };
	local badBin1 = map { expbin_ttl = os.time() - 10, data = 120 };
	local badBin2 = map { expbin = os.time(), data = "leet" };
	if (eb.get_bin(goodBin) ~= 12) then
		info("[EXPIREBIN TEST] <%s> false negative", meth)
		pass = false;
	end
	if (eb.get_bin(badBin1) ~= nil) then
		info("[EXPIREBIN TEST] <%s> false positive", meth)
		pass = false;
	end
	if (eb.get_bin(badBin2) ~= badBin2) then 
		info("[EXPIREBIN TEST] <%s> false positive 2", meth)
		pass = false;
	end
	return pass;
end

local function put_test(rec)
	local meth = "put_test";
	if (eb.put(rec, "TEST", 123, 100, false) == 1) then
		info("[EXPIREBIN TEST] <%s> put failed", meth);
		return false;
	end
	info ("EXPIREBIN TEST] <%s> rec bin is %s", meth, tostring(rec));
	return true;
end

local function get_test(rec)
	local meth = "get_test";
	-- to be implemented
	return nil;
end

function test(rec) 
	-- export the module
	info("[BEGIN] EXPIREBIN TEST");
	info("[EXPIREBIN TEST] is_expbin_test %s", tostring(is_expbin_test(rec)));
	info("[EXPIREBIN TEST] valid_time_test %s", tostring(valid_time_test(rec)));
	info("[EXPIREBIN TEST] not_expire_test %s", tostring(not_expired_test(rec)));
	info("[EXPIREBIN TEST] get_bin_test %s", tostring(get_bin_test(rec)));
	info("[EXPIREBIN TEST] get_test %s", tostring(get_test(rec)));
	info("[EXIT] EXPIREBIN TEST");
	return true;
end

