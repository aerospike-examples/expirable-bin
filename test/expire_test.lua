-- Test of the expire_bin module

function expire_bin_test(rec)
	local meth = "expire_bin_test";
	-- Import the module into this script
	local expbin = require('expire_bin');

	-- Insert bin 
	expbin.put(rec, "jenny", 123, 100);

	-- Create or update record on db
	if aerospike:exists(rec) then
  		aerospike:update(rec)
	else
  		aerospike:create(rec)
	end

	-- Get bin
	local return_val = expbin.get(rec, "jenny");

	-- Wait 100 seconds
	local wait_time = os.time() + 100;
	while os.time() < wait_time do
		-- nothing
	end

	-- Get bin again
	local no_val = expbin.get(rec, "jenny");

	-- change bin_ttl
	

end

