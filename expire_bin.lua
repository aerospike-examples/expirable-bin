-- =========================================================================
-- UDF Bin Expiration Module
-- =========================================================================

-- This module provides basic functionality for bin level data expiration for the 
-- Aerospike database. This module uses special UDF bins that are specific to this
-- module and the bin expiration functionality will not be supported if used with
-- normal read/write operations. Also, this module does not fully remove data
-- from an expired bin for performance reasons so the data may still be accessible
-- through normal read/write operations. 

-- =========================================================================
-- USAGE
-- =========================================================================
--
-- Call the function through a client.
-- AerospikeClient as = new AerospikeClient(...);
-- as.execute(policy, key, "expire_bin", "get_expbin", bin);
-- as.execute(policy, key, "expire_bin", "put_expbin", bin, val, bin_ttl);

-- =========================================================================
-- Debug Flags
-- =========================================================================
local GP;
local F = true;

-- =========================================================================
-- Config Variables
-- =========================================================================
-- Set to true if you want data to persist after the bin expires
local EXP_ID = "expbin_ttl";
local EXP_DATA = "data";
-- =========================================================================
-- Module Initialization
-- =========================================================================


-- =========================================================================
-- get_expbin(): Get bin from record
-- =========================================================================
-- 
-- USAGE: as.execute(policy, key, "expire_bin", "get_expbin", bin);
--
-- Params:
-- (*) rec: record to retrieve bin from
-- (*) bin: bin names to retrieve from
--
-- Return:
-- 1 = error
-- list containing each respective bin value = success
-- =========================================================================
function get_expbin(rec, ...)
	local meth = "get_expbin";
	GP=F and debug("[BEGIN]<%s> bin:<%s>", meth, tostring(bin));
	if aerospike:exists(rec) then
		local return_list = list();
		-- Iterate through every bin request 
		for x=1,arg[n] do
			local bin_map = rec[arg[x]];
			return_list[x] = get_bin(bin_map);
		end
		GP=F and debug("<%s> Returning bin list");
		return return_list;
	else
		GP=F and debug("<%s> Record does not exist.", meth);
	end
end

local function get_bin(bin_map)
	local meth = "get_bin";
	if (bin_map and bin_map[EXP_ID]) then
		local bin_ttl = bin_map[EXP_ID];
		if (bin_ttl == 0xFFFFFFFF or os.time() <= bin_ttl) then
			return bin_map[EXP_DATA];
		else
			GP=F and debug("<%s> Bin has expired, returning nil", meth);
			return nil;
		end
	else
		GP=F and debug("<%s> Bin is not an expbin.", meth);
		return bin_map;
	end
end
-- =========================================================================
-- put_expbin(): Store bin to record
-- =========================================================================
--
-- USAGE: as.execute(policy, key, "expire_bin", "put_expbin", bin, val, bin_ttl, exp_create);
--
-- Params:
-- (*) rec: record to retrieve bin from
-- (*) bin: bin name 
-- (*) val: Value to store in bin
-- (*) bin_ttl: Bin TTL given in seconds or -1 to disable expiration
-- (*) exp_bin: set to true to create new exp_bins
--
-- Return:
-- 1 = error
-- 0 = success
-- =========================================================================
function put_expbin(rec, bin, val, bin_ttl, exp_create)
	local meth = "put_expbin";
	GP=F and debug("[BEGIN]<%s> bin:%s value:%s ttl:%s", meth, bin, tostring(val), tostring(bin_ttl));
	if aerospike:exists(rec) then
		-- create expbin
		-- create normal bin
		-- update/set expbin
		if (rec[bin] == nil and not exp_create ) then
			GP=F and debug("%s : bin doesn't exist and expire bin creation disabled")
			rec[bin] = val;
			return 0;
		end
		local map_bin = rec[bin];
		if ( map_bin == nil or map_bin[EXP_ID] == nil) then
			map_bin = map();
		end 
		-- check that times are correct
		if (bin_ttl == -1) then
			if (record.ttl(rec) ~= -1) then
				GP=F and debug ("%s record ttl and bin ttl conflict", meth);
				return -1;
			end
		end
		if (record.ttl(rec) ~= -1) then
			if ( bin_ttl > record.ttl(rec)) then
				GP=F and debug ("%s record ttl and bin ttl conflict", meth);
				return -1;
			end
		end

		map_bin[EXP_ID] = bin_ttl;
		map_bin[EXP_DATA] = val;
		rec[bin] = map_bin;
		aerospike:update(rec);
	end
	GP=F and debug("%s : Record doesn't exist", meth);
	return 1;
end

-- =========================================================================
-- put_bins(): Store bin to record
-- =========================================================================
--
-- USAGE: as.execute(policy, key, "expire_bin", "put_expbin", bin, val, bin_ttl, exp_create);
--
-- Params:
-- (*) rec: record to retrieve bin from
-- (*) bin: bin name 
-- (*) val: Value to store in bin
-- (*) bin_ttl: Bin TTL given in seconds or -1 to disable expiration
-- (*) exp_bin: set to true to create new exp_bins
--
-- Return:
-- 1 = error
-- 0 = success
-- =========================================================================
function put_bins(rec, exp_create, bin_ttl, ...)
	local meth = "put_bins";
	-- api in progress
end

-- =========================================================================
-- touch(): Modify the bin's TTL
-- =========================================================================
--
-- USAGE: as.execute(policy, key, "expire_bin", "touch_expbin", bin, bin_ttl);
--
-- Params:
-- (*) rec: record to retrieve bin from
-- (*) bin: bin name 
-- (*) bin_ttl: Bin TTL given in seconds or -1 to disable expiration
--
-- Return:
-- 0 = success
-- 1 = error
-- =========================================================================
function touch_expbin(rec, bin_ttl, ...)
	local meth = "touch_expbin";
	GP=F and debug("[BEGIN]<%s> bin:%s bin_ttl:%s", meth, bin, tostring(bin_ttl));
	if aerospike:exists(rec) then
		if (record.ttl(rec) < bin_ttl) then
			GP=F and debug("%s : Record TTL is less than Bin TTL", meth);
			return 1;
		end
		local temp_list = rec[bin];
		if (temp_list) then -- TODO: Implement a better list check
			temp_list[1] = os.time() + bin_ttl;
			rec[bin] = temp_list;
			return 0;
		end
		GP=F and debug("expire_bin.%s : Bin is empty", meth);
	end
	GP=F and debug("expire_bin.%s : Record doesn't exist", meth);
	return 1;
end

-- =========================================================================
-- clean_bin(): Empty expired bins
-- =========================================================================
--
-- USAGE:
-- try {
-- 	final AerospikeClient client = new AerospikeClient(host, port);

-- 	ScanPolicy scanPolicy = new ScanPolicy();
-- 	client.scanAll(scanPolicy, namespace, set, new ScanCallback() {
-- 		public void scanCallback(Key key, Record record) throws AerospikeException {
-- 			client.execute(new WritePolicy(), key, "expire_bin", "clean_bin", record.bins.keySet());
-- 		}
-- 	}, new String[] {});
-- } catch (AerospikeException e) {
	
-- }
--
-- Params:
-- (*) rec: record to retrieve bin from
-- (*) bin: bin to clean 
--
-- Return:
-- 0 = success
-- 1 = error
-- =========================================================================
function clean_expbin(rec, ...)
	local meth = "clean_expbins";
	GP=F and debug("[BEGIN]<%s> bin:%s ", meth, bin);
	if aerospike:exists(rec) then
		local temp_bin = rec[bin];
		if (temp_bin) then
			if (temp_bin[1] < os.time()) then
				rec[bin] = nil;
				aerospike:update(rec);
				GP=F and debug("expire_bin.%s : Bin expired, erasing bin", meth);
				return 0;
			end
			GP=F and debug("expire_bin.%s : Bin hasn't expired, skipping record", meth);
			return 0;
		end
		GP=F and debug("expire_bin.%s : Bin doesn't exist", meth);
	end
	GP=F and debug("expire_bin.%s : Record doesn't exist", meth);
	return 1;
end

return exports;