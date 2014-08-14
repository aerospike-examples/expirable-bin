/*
 * Copyright 2014 Aerospike, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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


/*
 * Attempt to retrieve values from list of bins. The bins
 * can be expire bins or normal bins.
 *
 * \param as The aerospike instance to use for this operation.
 * \param err The as_error to be populated if an error occurs.
 * \param policy The policy to use for this operation. If NULL, then the default policy will be used.
 * \param key The key of the record
 * \param arglist The list of bin names to retrieve values from.
 * \param result A list of bin values respective to the list of bin names passedin. if a bin is expired or empty, the
 *        corresponding index in the list will be NULL.
 * \return AEROSPIKE_OK if successful, otherwise an error.
 */
as_status
as_expbin_get(aerospike* as, as_error* err, const as_policy_apply* policy, const as_key* key, as_list* arglist, as_val** result)
{
	as_status rc = aerospike_key_apply(as, err, policy, key, UDF_MODULE, "get", arglist, result);
	if (rc != AEROSPIKE_OK) {
		LOG("as_expbin_get() returned %d - %s", err->code, err->message);
	}
	return rc;
}

/*
 * Create or update expire bins. If the create flag is set to true,
 * 	all newly created bins will be expire bins otherwise, only normal bins will be created
 * 	and existing expire bins will be updated. Note: existing expire bins will not be converted
 * 	into normal bins if create flag is off.
 *
 * \param as The aerospike instance to use for this operation.
 * \param err The as_error to be populated if an error occurs.
 * \param policy The policy to use for this operation. If NULL, then the default policy will be used.
 * \param key The key of the record
 * \param arglist The as_list containing the following:
 *   - bin name - char* with bin name
 *   - val - value to put inside bin
 *   - bin_ttl - expiration time in seconds or -1 for no expiration
 *   - create_flag: 0 to create expire bins, 1 to create normal bins
 * \param result - 0 if successfully written, 1 otherwise
 *
 * \return AEROSPIKE_OK if successful, otherwise an error.
 */
as_status
as_expbin_put(aerospike* as, as_error* err, const as_policy_apply* policy, const as_key* key, as_list* arglist, as_val** result)
{
	as_status rc = aerospike_key_apply(as, err, policy, key, UDF_MODULE, "put", arglist, result);
	if (rc != AEROSPIKE_OK) {
		LOG("as_expbin_put() returned %d - %s", err->code, err->message);
	}
	return rc;
}

/* Batch create or update expire bins for a given key. Use the as_map
 *		{'bin' : bin_name, 'val' : bin_value, 'bin_ttl' : ttl}
 * to store each put operation. Omit the bin_ttl to turn bin creation off.
 *
 * \param as The aerospike instance to use for this operation.
 * \param err The as_error to be populated if an error occurs.
 * \param policy The policy to use for this operation. If NULL, then the default policy will be used.
 * \param key The key of the record
 * \param arglist The list of as_maps in the following form: {'bin' : bin_name, 'val' : bin_value, 'bin_ttl' : ttl}
 * \param result - 0 if all ops succeed, 1 otherwise
 * 
 * \return AEROSPIKE_OK if successful, otherwise an error
 */
as_status 
as_expbin_puts(aerospike* as, as_error* err, const as_policy_apply* policy, const as_key* key, as_list* arglist, as_val** result) 
{
	as_status rc = aerospike_key_apply(as, err, policy, key, UDF_MODULE, "puts", arglist, result);
	if (rc != AEROSPIKE_OK) {
		LOG("as_expbin_puts() returned %d - %s", err->code, err->message);
	}
	return rc;
}

/*
 * Batch update the bin TTLs. Us this method to change or reset the bin TTL of
 * multiple bins in a record. Use a dict to store each touch operation.
 *
 * \param as The aerospike instance to use for this operation.
 * \param err The as_error to be populated if an error occurs.
 * \param policy The policy to use for this operation. If NULL, then the default policy will be used. 
 * \param key The key of the record
 * \param arglist The list of as_maps in the following form: {'bin' : bin_name, 'bin_ttl' : ttl}
 * \param result - 0 if all ops succeed, 1 otherwise
 * 
 * \return AEROSPIKE_OK if successful, otherwise an error
 */
as_status 
as_expbin_touch(aerospike* as, as_error* err, const as_policy_apply* policy, const as_key* key, as_list* arglist, as_val** result) 
{
	as_status rc = aerospike_key_apply(as, err, policy, key, UDF_MODULE, "touch", arglist, result);
	if (rc != AEROSPIKE_OK) {
		LOG("as_expbin_touch() returned %d - %s", err->code, err->message);	
	}
	return rc;
}

/*
 * Get the time bin will expire in seconds.
 *
 * \param as The aerospike instance to use for this operation.
 * \param err The as_error to be populated if an error occurs.
 * \param policy The policy to use for this operation. If NULL, then the default policy will be used. 
 * \param key The key of the record
 * \param arglist the bin name to check
 * \param result bin time to expire in seconds
 * 
 * \return AEROSPIKE_OK if successful, otherwise an error
 */
as_status 
as_expbin_ttl(aerospike* as, as_error* err, const as_policy_apply* policy, const as_key* key, as_list* arglist, as_val** result) 
{
	as_status rc = aerospike_key_apply(as, err, policy, key, UDF_MODULE, "ttl", arglist, result);
	if (rc != AEROSPIKE_OK) {
		LOG("as_expbin_ttl() returned %d - %s", err->code, err->message);	
	}
	return rc;
}

/* 
 * Clear out the expired bins on a scan of the database
 * 
 * \param as The aerospike instance to use for this operation.
 * \param ns The namespace to clean
 * \param set The set name to clean
 * \param arglist The list of bin names to clean.
 *
 * \return AEROSPIKE_OK if successful, otherwise an error
 */
as_status
as_expbin_clean(aerospike* as, const char* ns, const char* set, as_list* arglist)
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

//==========================================================
// Expire Bin Example
//  
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
		exit(1);
	}
	printf("Connected!\n");

	
	as_key key1, key2, key3;
	
	printf("Creating expire bins...");
	
	if (as_key_init_str(&key1, "test", "expireBin", "eb1") == NULL ||
	    as_key_init_str(&key2, "test", "expireBin", "eb2") == NULL ||
		as_key_init_str(&key3, "test", "expireBin", "eb3") == NULL) {
		printf("Keys were not initiated.\n");
		exit(1);
	}

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 4);
	as_arraylist_append_str(&arglist, "TestBin");
	as_arraylist_append_str(&arglist, "Hello World!");
	as_arraylist_append_int64(&arglist, -1);
	as_arraylist_append_int64(&arglist, 0);

	as_val* result = NULL;

	rc = as_expbin_put(&as, &err, NULL, &key1, (as_list*)&arglist, &result);
	if (rc != AEROSPIKE_OK) {
		exit(1);
	}
	
	aerospike_close(&as, &err);
	aerospike_destroy(&as);

	return 0;
}
