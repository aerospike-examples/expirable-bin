#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_error.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>
#include <aerospike/as_scan.h>
#include <aerospike/as_key.h>

#define UDF_MODULE "expire_bin"
#define LOG(_fmt, _args...) { printf(_fmt "\n", ## _args); fflush(stdout); }

as_status
as_expbin_get(aerospike* as, as_error* err, const as_policy_apply* policy, const as_key* key, as_list* arglist, as_val** result) 
{
	as_status rc = aerospike_key_apply(as, err, policy, key, UDF_MODULE, "get", arglist, result);
	if (rc != AEROSPIKE_OK) {
		LOG("as_expbin_get() returned %d - %s", err->code, err->message);
	}
	return rc;
}

as_status
as_expbin_put(aerospike* as, as_error* err, const as_policy_apply* policy, const as_key* key, as_list* arglist, as_val** result) 
{
	as_status rc = aerospike_key_apply(as, err, policy, key, UDF_MODULE, "put", arglist, result);
	if (rc != AEROSPIKE_OK) {
		LOG("as_expbin_put() returned %d - %s", err->code, err->message);
	}
	return rc;
}

as_status 
as_expbin_puts(aerospike* as, as_error* err, const as_policy_apply* policy, const as_key* key, as_list* arglist, as_val** result) 
{
	as_status rc = aerospike_key_apply(as, err, policy, key, UDF_MODULE, "puts", arglist, result);
	if (rc != AEROSPIKE_OK) {
		LOG("as_expbin_puts() returned %d - %s", err->code, err->message);
	}
	return rc;
}

as_status 
as_expbin_touch(aerospike* as, as_error* err, const as_policy_apply* policy, const as_key* key, as_list* arglist, as_val** result) 
{
	as_status rc = aerospike_key_apply(as, err, policy, key, UDF_MODULE, "touch", arglist, result);
	if (rc != AEROSPIKE_OK) {
		LOG("as_expbin_touch() returned %d - %s", err->code, err->message);	
	}
	return rc;
}

as_status 
as_expbin_clean(aerospike* as, as_error* err, const char* ns, const char* set,  const as_policy_apply* policy, as_list* arglist, as_val** result) 
{
	as_scan scan;
	as_scan_init(&scan, ns, set);
	as_scan_select_inita(&scan, as_list_size(arglist)); 
	if (as_scan_apply_each(&scan, UDF_MODULE, "clean", arglist) != true) {
		LOG("as_expbin_clean() failed");
	}
	as_scan_destroy(&scan);
	return AEROSPIKE_OK;
}

int
main(int argc, char* argv[]) 
{
	aerospike as;
	as_config config;
	as_error err;
	as_status rc;
	as_config_init(&config);
	as_config_add_host(&config, "127.0.0.1", 3000);
	aerospike_init(&as, &config);

	printf("Connecting to Aerospike server...\n");
	if ( aerospike_connect(&as, &err) != AEROSPIKE_OK ) {
		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
	}
	printf("Connected!\n");

	as_policy_apply policy;
	as_policy_apply_init(&policy);
	
	as_key key1, key2, key3;
	
	printf("Creating expire bins...");
	
	if (as_key_init_str(&key1, "test", "expireBin", "eb1") == NULL ||
	    as_key_init_str(&key2, "test", "expireBin", "eb2") == NULL ||
		as_key_init_str(&key3, "test", "expireBin", "eb3") == NULL) {
		printf("Keys were not initiated.\n");
		exit(1);
	}
	as_arraylist arglist;
	as_list * l = (as_list *) as_arraylist_inita(&arglist, 4);

	as_val result;
	rc = as_expbin_put(as, err, key1, arglist, &result);
	if (rc != AEROSPIKE_OK) {
		exit(1);
	}
	
	aerospike_close(&as, &err);
	aerospike_destroy(&as);

	return 0;
}
