-- Copyright 2014 Aerospike, Inc.

-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at

--    http://www.apache.org/licenses/LICENSE-2.0

-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

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
-- as.execute(policy, key, "expire_bin", "get", bin);
-- as.execute(policy, key, "expire_bin", "put", bin, val, bin_ttl);

-- =========================================================================
-- Debug Flags
-- =========================================================================
local GP;
local F = true;

-- =========================================================================
-- Config Variables
-- =========================================================================
local EXP_ID = "expbin_ttl";
local EXP_DATA = "data";

-- =========================================================================
-- Module Initialization
-- =========================================================================


-- =========================================================================
-- get(): Get bin from record
-- =========================================================================
-- 
-- USAGE: as.execute(policy, key, "expire_bin", "get", bin);
--
-- Params:
-- (*) rec: record to retrieve bin from
-- (*) bin: bin names to retrieve from
--
-- Return:
-- 1 = error
-- list containing each respective bin value = success
-- =========================================================================
function get(rec, ...)
	local meth = "get";
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
-- put(): Store bin to record
-- =========================================================================
--
-- USAGE: as.execute(policy, key, "expire_bin", "put", bin, val, bin_ttl, exp_create);
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
function put(rec, bin, val, bin_ttl, exp_create)
	local meth = "put";
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
				return 1;
			end
		end
		if (record.ttl(rec) ~= -1) then
			if ( bin_ttl > record.ttl(rec)) then
				GP=F and debug ("%s record ttl and bin ttl conflict", meth);
				return 1;
			end
		end

		if (bin_ttl ~= -1) then
			map_bin[EXP_ID] = bin_ttl + os.time();
		else
			map_bin[EXP_ID] = bin_ttl;
		end
		map_bin[EXP_DATA] = val;
		rec[bin] = map_bin;
		aerospike:update(rec);
		return 0
	end
	GP=F and debug("%s : Record doesn't exist", meth);
	return 1;
end

-- =========================================================================
-- puts(): Store bin to record
-- =========================================================================
--
-- USAGE: as.execute(policy, key, "expire_bin", "put", record_maps);
--
-- Params:
-- (*) rec: record to create/update bin to
-- (*) record_maps: list of maps containing the following fields
-- 	(*) bin: bin name 
-- 	(*) val: Value to store in bin
-- 	(*) bin_ttl: (optional) if provided, expire_bin will be created if none exists
--
-- Return:
-- 1 = error
-- 0 = success
-- =========================================================================
function puts(rec, ...)
	local meth = "puts";
	GP=F and debug("[BEGIN]<%s> bin:%s value:%s ttl:%s", meth);
	if aerospike:exists(rec) then
		for x=1,args[n] do
			local map_bin = args[x];
			local bin_ttl = map_bin["bin_ttl"];
			local exp_create = false;
			if (bin_ttl == nil) then
				bin_ttl = -1;
			else 
				exp_create = true;
			end
			return put(rec, map_bin["bin"], map_bin["val"], bin_ttl, exp_create);
		end
	end
	GP=F and debug("%s : Record doesn't exist", meth);
	return 1;
end

-- =========================================================================
-- touch(): Modify the bin's TTL
-- =========================================================================
--
-- USAGE: as.execute(policy, key, "expire_bin", "touch", bin, bin_maps);
--
-- Params:
-- (*) rec: record to retrieve bin from
-- (*) bin_maps: list of bin maps containing the following
-- 	(*) bin: bin names 
-- 	(*) bin_ttl: Bin TTL given in seconds or -1 to disable expiration
--
-- Return:
-- 0 = success
-- 1 = error
-- =========================================================================
function touch(rec, ...)
	local meth = "touch";
	GP=F and debug("[BEGIN]<%s> bin:%s bin_ttl:%s", meth, bin, tostring(bin_ttl));
	if aerospike:exists(rec) then
		for x=1,args[n] do
			local bin_map = args[x];
			local bin_name = bin_map["bin"]
			if (record.ttl(rec) < bin_map["bin_ttl"]) then -- TODO: CHECK THIS
				GP=F and debug("%s : Record TTL is less than Bin TTL for bin %s", meth, bin_name);
			else
				local rec_map = rec[bin_name];
				if (rec_map ~= nil) then
					if (rec_map[EXP_ID] == nil) then
						GP=F and debug("%s : Bin %s is not an expirable bin", meth, bin_name);
					else
						if (bin_map["bin_ttl"] ~= -1) then
							rec_map[EXP_ID] = bin_map["bin_ttl"] + os.time();
						else
							rec_map[EXP_ID] = -1;
						end
					end
				else
					GP=F and debug("expire_bin.%s : Bin %s is empty", meth, bin_name);
				end
			end	
		end
		aerospike:update(rec);
		return 0;
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
function clean(rec, ...)
	local meth = "clean";
	GP=F and debug("[BEGIN]<%s> bin:%s ", meth, bin);
	if aerospike:exists(rec) then
		for x=1,args[n] do
			local bin = args[x];
			local temp_bin = rec[bin];
			if (temp_bin and temp_bin[EXP_ID]) then
				if (temp_bin[EXP_ID] < os.time()) then -- TODO: CHECK THIS
					rec[bin] = nil;
					GP=F and debug("expire_bin.%s : Bin %s expired, erasing bin", meth, bin);
				end
				GP=F and debug("expire_bin.%s : Bin %s hasn't expired, skipping record", meth, bin);
			else	
				GP=F and debug("expire_bin.%s : Bin doesn't exist", meth);
			end
		end
		aerospike:update(rec);
		return 0;
	else
		GP=F and debug("expire_bin.%s : Record doesn't exist", meth);
		return 1;
	end
end

-- =========================================================================
-- Module export
-- =========================================================================
return {
	get   = get,
	put   = put,
	puts  = puts,
	touch = touch,
	clean = clean
}