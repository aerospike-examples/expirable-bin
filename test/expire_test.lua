-- =========================================================================
-- expire_bin.lua tests
-- =========================================================================

function test(rec) 
	-- export the module
	local eb = require('expire_bin');

	if (eb == nil) then
		log.info("[EXPIRE BIN TEST] module could not be imported.");
	else
		log.info("[EXPIRE BIN TEST] module successfuly imported");
	end

	if aerospike:exists(rec) then
		local rl = eb.get(rec, "test1", "test2", "test3");
		log.info("[EXPIRE BIN TEST] Return List: %s", tostring(rl));

		eb.put(rec, "test3", 48, 24, true);
		eb.puts(rec, map {bin = "test1", val = 12, bin_ttl = 10}, map {bin = "test2", val = 23, bin_ttl = 10});
		rl = eb.get(rec, "test1", "test2", "test3");
		log.info("EXPIRE BIN TEST] Return List after put: %s", tostring(rl));

		local expire_time = os.time() + 12;
		while os.time() < expire_time do
			-- nothing
			log.info("waiting");
		end
		rl = eb.get(rec, "test1", "test2", "test3");
		log.info("EXPIRE BIN TEST] Return List after wait: %s", tostring(rl));
	else
		log.info("[EXPIRE BIN TEST] record doesn't exist");
		aerospike:create(rec);
	end
end
