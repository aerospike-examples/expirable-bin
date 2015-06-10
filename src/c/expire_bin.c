/*******************************************************************************
 * Copyright 2008-2015 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/


//==========================================================
// Includes
//

#include <errno.h>

#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/aerospike_udf.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_record_iterator.h>


//==========================================================
// Constants
//

#define UDF_MODULE "expire_bin"
#define UDF_USER_PATH "../../"
#define LOG(_fmt, _args...) { printf(_fmt "\n", ## _args); fflush(stdout); }

const char UDF_FILE_PATH[] = UDF_USER_PATH UDF_MODULE ".lua";

// Namespace, Set, and Key	
const char DEFAULT_NAMESPACE[] = "test";
const char DEFAULT_SET[]       = "expireBin";
const char DEFAULT_KEY_STR[]   = "testKey";

// Based on current server limit
char eb_namespace[32]; 
char eb_set[64];
char eb_key_str[1024];

aerospike as;
as_key testKey;
as_config config;
as_error err;
as_val* result;
as_string val;
as_arraylist arglist;
as_hashmap map1, map2; 


//==========================================================
// Forward Declarations
//

as_val* as_expbin_get(aerospike* as, as_error* err, as_policy_apply* policy, as_key* key, as_list* arglist, as_val* result);
void as_expbin_put(aerospike* as, as_error* err, as_policy_apply* policy, as_key* key, char* bin, as_val* val, uint64_t bin_ttl, as_val* result);
void as_expbin_puts(aerospike* as, as_error* err, as_policy_apply* policy, as_key* key, as_list* arglist, as_val* result);
void as_expbin_touch(aerospike* as, as_error* err, as_policy_apply* policy, as_key* key, as_list* arglist, as_val* result);
as_val* as_expbin_ttl(aerospike* as, as_error* err, as_policy_apply* policy, as_key* key, char* bin_name, as_val* result);
void as_expbin_clean(aerospike* as, as_error* err, as_policy_scan* policy, as_scan* scan, as_list* binlist);
as_hashmap create_bin_map(char* bin_name, char* val, int64_t bin_ttl);

bool register_udf(aerospike* p_as, const char* udf_file_path);
void cleanup(aerospike* as, as_error* err, as_policy_remove* policy, as_key* key);
void example_dump_record(const as_record* p_rec);
void example_cleanup(aerospike* p_as);
void example_remove_test_records(aerospike* p_as);
void example_remove_test_record(aerospike* p_as);

void exp_example(void);
void touch_example(void);
void get_example(void);


//==========================================================
// Expire Bin C Example
//  

int
main(int argc, char* argv[]) 
{
	LOG("This is a demo of the expirable bin module for C:");

	strcpy(eb_namespace, DEFAULT_NAMESPACE);
	strcpy(eb_set, DEFAULT_SET);
	strcpy(eb_key_str, DEFAULT_KEY_STR);

	as_config_init(&config);
	as_config_add_host(&config, "127.0.0.1", 3000);
	aerospike_init(&as, &config);

	LOG("\nConnecting to Aerospike server...");
	
	if (aerospike_connect(&as, &err) != AEROSPIKE_OK) {
		LOG("error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
		exit(1);
	}

	LOG("Connected!");
	
	// Start clean.
	if (as_key_init_str(&testKey, eb_namespace, eb_set, eb_key_str) == NULL) {
		LOG("Key was not initiated");
		exit(1);
	}
	
	aerospike_key_remove(&as, &err, NULL, &testKey);

	LOG("\nRegistering UDF...");

	if (! register_udf(&as, UDF_FILE_PATH)) {
		LOG("Error registering UDF!")
		cleanup(&as, &err, NULL, &testKey);
		exit(-1);
	}

	LOG("UDF registered!");

	LOG("\nInserting expire bins...");

	// Example 1: validates the basic bin expiration.
	exp_example();

	// Example 2: validates the basic bin expiration after using 'touch'.
	touch_example();

	// Example 3: shows the difference between normal 'get' and 'eb.get'.
	get_example();

	aerospike_close(&as, &err);
	aerospike_destroy(&as);

	return 0;
}

/*
 * Attempt to retrieve values from list of bins. The bins
 * can be expire bins or normal bins.
 *
 * \param as      - The aerospike instance to use for this operation.
 * \param err     - The as_error to be populated if an error occurs.
 * \param policy  - The policy to use for this operation. If NULL, then the default policy will be used.
 * \param key     - The key of the record.
 * \param arglist - The list of bin names to retrieve values from.
 * \param result  - A list of bin values respective to the list of bin names passed in. 
 *                  If a bin is expired or empty, the corresponding index in the list will be NULL.
 * \return        - result if successful, an error otherwise.
 */
as_val* 
as_expbin_get(aerospike* as, as_error* err, as_policy_apply* policy, as_key* key, as_list* arglist, as_val* result)
{
	as_status rc = aerospike_key_apply(as, err, policy, key, UDF_MODULE, "get", arglist, &result);
	
	if (rc != AEROSPIKE_OK) {
		LOG("as_expbin_get() returned %d - %s", err->code, err->message);
		exit(1);
	}
	return result;
}

/*
 * Create or update expire bins. If bin_ttl is not NULL, all newly created bins
 * will be expire bins, otherwise, only normal bins will be created and existing 
 * expire bins will be updated. Note: existing expire bins will not be converted
 * into normal bins if bin_ttl is NULL.
 *
 * \param as      - The aerospike instance to use for this operation.
 * \param err     - The as_error to be populated if an error occurs.
 * \param policy  - The policy to use for this operation. If NULL, then the default policy will be used.
 * \param key     - The key of the record.
 * \param bin     - Bin name.
 * \param val     - Bin value.
 * \param bin_ttl - Expiration time in seconds or -1 for no expiration.
 * \param result  - 0 if successfully written, 1 otherwise.
 * \return        - void if successful, an error otherwise.
 */
void
as_expbin_put(aerospike* as, as_error* err, as_policy_apply* policy, as_key* key, char* bin, as_val* val, uint64_t bin_ttl, as_val* result)
{
	as_arraylist arglist;
	as_arraylist_inita(&arglist, 4);
	as_arraylist_append_str(&arglist, bin);
	as_arraylist_append(&arglist, val);
	as_arraylist_append_int64(&arglist, bin_ttl);

	as_status rc = aerospike_key_apply(as, err, policy, key, UDF_MODULE, "put", (as_list*)&arglist, &result);
	
	if (rc != AEROSPIKE_OK) {
		LOG("as_expbin_put() returned %d - %s", err->code, err->message);
		exit(1);
	}
}

/* Batch create or update expire bins for a given key. Use the as_map:
 * {'bin' : bin_name, 'val' : bin_value, 'bin_ttl' : ttl} to store each put operation.
 * Omit the bin_ttl to turn bin creation off.
 *
 * \param as      - The aerospike instance to use for this operation.
 * \param err     - The as_error to be populated if an error occurs.
 * \param policy  - The policy to use for this operation. If NULL, then the default policy will be used.
 * \param key     - The key of the record.
 * \param arglist - The list of as_maps in the following form: {'bin' : bin_name, 'val' : bin_value, 'bin_ttl' : ttl}.
 * \param result  - 0 if all ops succeed, 1 otherwise.
 * \return        - void if successful, an error otherwise.
 */
void 
as_expbin_puts(aerospike* as, as_error* err, as_policy_apply* policy, as_key* key, as_list* arglist, as_val* result) 
{
	as_status rc = aerospike_key_apply(as, err, policy, key, UDF_MODULE, "puts", arglist, &result);
	
	if (rc != AEROSPIKE_OK) {
		LOG("as_expbin_puts() returned %d - %s", err->code, err->message);
		exit(1);
	}
}

/*
 * Batch update the bin TTLs. Us this method to change or reset the bin TTL of
 * multiple bins in a record. 
 *
 * \param as      - The aerospike instance to use for this operation.
 * \param err     - The as_error to be populated if an error occurs.
 * \param policy  - The policy to use for this operation. If NULL, then the default policy will be used.
 * \param key     - The key of the record.
 * \param arglist - The list of bin names.
 * \param result  - 0 if all ops succeed, 1 otherwise.
 * \return        - void if successful, an error otherwise.
 */
void 
as_expbin_touch(aerospike* as, as_error* err, as_policy_apply* policy, as_key* key, as_list* arglist, as_val* result) 
{
	as_status rc = aerospike_key_apply(as, err, policy, key, UDF_MODULE, "touch", arglist, &result);
	
	if (rc != AEROSPIKE_OK) {
		LOG("as_expbin_touch() returned %d - %s", err->code, err->message);	
		exit(1);
	}
}

/*
 * Get the time bin will expire in seconds.
 *
 * \param as      - The aerospike instance to use for this operation.
 * \param err     - The as_error to be populated if an error occurs.
 * \param policy  - The policy to use for this operation. If NULL, then the default policy will be used.
 * \param key     - The key of the record.
 * \param arglist - The bin name to check.
 * \param result  - Bin time to expire in seconds.
 * \return        - result if successful, an error otherwise.
 */
as_val*
as_expbin_ttl(aerospike* as, as_error* err, as_policy_apply* policy, as_key* key, char* bin_name, as_val* result) 
{
	as_arraylist arglist;
	as_arraylist_inita(&arglist, 1);
	as_arraylist_append_str(&arglist, bin_name);

	as_status rc = aerospike_key_apply(as, err, policy, key, UDF_MODULE, "ttl", (as_list*) &arglist, &result);
	
	if (rc != AEROSPIKE_OK) {
		LOG("as_expbin_ttl() returned %d - %s", err->code, err->message);
		exit(1);	
	}
	return result;
}

/* 
 * Perform a background scan and remove all expired bins.
 * 
 * \param as      - The aerospike instance to use for this operation.
 * \param err     - The as_error to be populated if an error occurs.
 * \param policy  - The policy to use for this operation. If NULL, then the default policy will be used.
 * \param scan    - as_scan to execute scan on.
 * \param binlist - List of bins to clean.
 * \return        - void if successful, an error otherwise.
 */
void
as_expbin_clean(aerospike* as, as_error* err, as_policy_scan* policy, as_scan* scan, as_list* binlist)
{
	uint64_t scan_id = 0;
	as_scan_init(scan, eb_namespace, eb_set);

	if (as_scan_apply_each(scan, UDF_MODULE, "clean", binlist) != true) {
		LOG("UDF apply failed");
		exit(1);
	}

	as_status rc = aerospike_scan_background(as, err, policy, scan, &scan_id);
	aerospike_scan_wait(as, err, NULL, scan_id, 0);

	if (rc != AEROSPIKE_OK) {
		LOG("as_expbin_clean() returned %d - %s", err->code, err->message);
		exit(1);
	} 
}

/*
 * Generate maps for use with batch put and touch operations.
 *
 * \param bin_name - name of bin to perform op on.
 * \param val      - value of bin.
 * \param bin_ttl  - bin_ttl for bin (-1 for no expiration, 0 to create normal bin).
 * \return         - as_hashmap.
 */
as_hashmap 
create_bin_map(char* bin_name, char* val, int64_t bin_ttl) 
{
	as_hashmap map;
	as_hashmap_init(&map, 1);
	as_stringmap_set_str((as_map *) &map, "bin", bin_name);
	as_stringmap_set_str((as_map *) &map, "val", val);
	as_stringmap_set_int64((as_map *) &map, "bin_ttl", bin_ttl);

	return map;
}

//==========================================================
// Helpers
//

// Register a UDF function in the database.

bool
register_udf(aerospike* p_as, const char* udf_file_path)
{
	FILE* file = fopen(udf_file_path, "r");

	if (! file) {
		// If we get here it's likely that we're not running the example from
		// the right directory - the specific example directory.
		LOG("cannot open script file %s : %s", udf_file_path, strerror(errno));
		return false;
	}

	// Read the file's content into a local buffer.

	uint8_t* content = (uint8_t*)malloc(1024 * 1024);

	if (! content) {
		LOG("script content allocation failed");
		return false;
	}

	uint8_t* p_write = content;
	int read = (int)fread(p_write, 1, 512, file);
	int size = 0;

	while (read) {
		size += read;
		p_write += read;
		read = (int)fread(p_write, 1, 512, file);
	}

	fclose(file);

	// Wrap the local buffer as an as_bytes object.
	as_bytes udf_content;
	as_bytes_init_wrap(&udf_content, content, size, true);

	as_error err;
	as_string base_string;
	const char* base = as_basename(&base_string, udf_file_path);

	// Register the UDF file in the database cluster.
	if (aerospike_udf_put(p_as, &err, NULL, base, AS_UDF_TYPE_LUA,
			&udf_content) == AEROSPIKE_OK) {
		// Wait for the system metadata to spread to all nodes.
		aerospike_udf_put_wait(p_as, &err, NULL, base, 100);
	}
	else {
		LOG("aerospike_udf_put() returned %d - %s", err.code, err.message);
	}

	as_string_destroy(&base_string);

	// This frees the local buffer.
	as_bytes_destroy(&udf_content);

	return err.code == AEROSPIKE_OK;
}

// Remove the record from database, and disconnect from cluster.

void
cleanup(aerospike* as, as_error* err, as_policy_remove* policy, as_key* testKey)
{
	// Clean up the database. Note that with database "storage-engine device"
	// configurations, this record may come back to life if the server is re-
	// started. That's why this example that want to start clean removes the 
	// record at the beginning.
	
	// Remove the record from the database.
	aerospike_key_remove(as, err, NULL, testKey);

	// Disconnect from the database cluster and clean up the aerospike object.
	aerospike_close(as, err);
	aerospike_destroy(as);
}

static void
example_dump_bin(const as_bin* p_bin)
{
	if (! p_bin) {
		LOG("  null as_bin object");
		return;
	}

	char* val_as_str = as_val_tostring(as_bin_get_value(p_bin));

	LOG("  %s: %s", as_bin_get_name(p_bin), val_as_str);

	free(val_as_str);
}

void
example_dump_record(const as_record* p_rec)
{
	if (! p_rec) {
		LOG("  null as_record object");
		return;
	}

	if (p_rec->key.valuep) {
		char* key_val_as_str = as_val_tostring(p_rec->key.valuep);
		free(key_val_as_str);
	}

	as_record_iterator it;
	as_record_iterator_init(&it, p_rec);

	while (as_record_iterator_has_next(&it)) {
		example_dump_bin(as_record_iterator_next(&it));
	}

	as_record_iterator_destroy(&it);
}

//------------------------------------------------
// Remove the test record from database, and
// disconnect from cluster.
//
void
example_cleanup(aerospike* p_as)
{
	// Clean up the database. Note that with database "storage-engine device"
	// configurations, this record may come back to life if the server is re-
	// started. That's why examples that want to start clean remove the test
	// record at the beginning.
	example_remove_test_record(p_as);

	// Note also example_remove_test_records() is not called here - examples
	// using multiple records call that from their own cleanup utilities.

	as_error err;

	// Disconnect from the database cluster and clean up the aerospike object.
	aerospike_close(p_as, &err);
	aerospike_destroy(p_as);
}

//------------------------------------------------
// Remove the test record from the database.
//
void
example_remove_test_record(aerospike* p_as)
{
	as_error err;

	// Try to remove the test record from the database. If the example has not
	// inserted the record, or it has already been removed, this call will
	// return as_status AEROSPIKE_ERR_RECORD_NOT_FOUND - which we just ignore.
	aerospike_key_remove(p_as, &err, NULL, &testKey);
}

//------------------------------------------------
// Remove multiple-record examples' test records
// from the database.
//
void
example_remove_test_records(aerospike* p_as)
{
	as_error err;

	if (as_key_init_str(&testKey, eb_namespace, eb_set, eb_key_str) == NULL) {
		LOG("Key was not initiated");
		exit(1);
	}

	aerospike_key_remove(p_as, &err, NULL, &testKey);
}

void 
exp_example(void) {
	as_string_init(&val, "Hello World.", false);
	as_expbin_put(&as, &err, NULL, &testKey, "TestBin1", (as_val*)&val, -1, result);
	LOG("TestBin 1 inserted");
	
	as_string_init(&val, "I don't expire.", false);
	as_expbin_put(&as, &err, NULL, &testKey, "TestBin2", (as_val*)&val, 8, result);
	LOG("TestBin 2 inserted");

	as_string_init(&val, "I will expire soon.", false);
	as_expbin_put(&as, &err, NULL, &testKey, "TestBin3", (as_val*)&val, 5, result);
	LOG("TestBin 3 inserted");

	LOG("Getting expire bins...");
	as_arraylist_inita(&arglist, 5);
	as_arraylist_append_str(&arglist, "TestBin1");
	as_arraylist_append_str(&arglist, "TestBin2");
	as_arraylist_append_str(&arglist, "TestBin3");

	result = as_expbin_get(&as, &err, NULL, &testKey, (as_list*)&arglist, result);
	LOG("%s", as_val_tostring(result));

	LOG("Getting bin TTLs...");
	result = as_expbin_ttl(&as, &err, NULL, &testKey, "TestBin1", result); 
	LOG("TestBin 1 TTL: %s", as_val_tostring(result));
	result = as_expbin_ttl(&as, &err, NULL, &testKey, "TestBin2", result); 
	LOG("TestBin 2 TTL: %s", as_val_tostring(result));
	result = as_expbin_ttl(&as, &err, NULL, &testKey, "TestBin3", result); 
	LOG("TestBin 3 TTL: %s", as_val_tostring(result));

	LOG("Waiting for TestBin 3 to expire...");
	sleep(6);

	LOG("Getting expire bins again...");
	as_arraylist_inita(&arglist, 3);
	as_arraylist_append_str(&arglist, "TestBin1");
	as_arraylist_append_str(&arglist, "TestBin2");
	as_arraylist_append_str(&arglist, "TestBin3");

	result = as_expbin_get(&as, &err, NULL, &testKey, (as_list*)&arglist, result);
	LOG("%s", as_val_tostring(result));
}

void
touch_example(void) {
	LOG("\nChanging expiration time for TestBin 1 and TestBin 2...");

	as_arraylist_inita(&arglist, 2);

	map1 = create_bin_map("TestBin1", "Hello World.", 3); 
	as_val_reserve((as_map *)&map1);
	as_arraylist_append(&arglist, (as_val *)((as_map *)&map1));

	map2 = create_bin_map("TestBin2", "I don't expire.", -1); 
	as_val_reserve((as_map *)&map2);
	as_arraylist_append(&arglist, (as_val *)((as_map *)&map2));

	as_expbin_touch(&as, &err, NULL, &testKey, (as_list*)&arglist, result);

	LOG("Getting bin TTLs...");
	result = as_expbin_ttl(&as, &err, NULL, &testKey, "TestBin1", result); 
	LOG("TestBin 1 TTL: %s", as_val_tostring(result));
	result = as_expbin_ttl(&as, &err, NULL, &testKey, "TestBin2", result); 
	LOG("TestBin 2 TTL: %s", as_val_tostring(result));

	LOG("Waiting for TestBin 1 to expire...");
	sleep(4);

	LOG("Getting expire bins again...");
	as_arraylist_inita(&arglist, 3);
	as_arraylist_append_str(&arglist, "TestBin1");
	as_arraylist_append_str(&arglist, "TestBin2");
	as_arraylist_append_str(&arglist, "TestBin3");

	result = as_expbin_get(&as, &err, NULL, &testKey, (as_list*)&arglist, result);
	LOG("%s", as_val_tostring(result));
}

void
get_example(void) {
	LOG("\nInserting expire bins...");
	as_arraylist_inita(&arglist, 2);

	map1 = create_bin_map("TestBin4", "Good Morning.", 5); 
	as_val_reserve((as_map *)&map1);
	as_arraylist_append(&arglist, (as_val *)((as_map *)&map1));

	map2 = create_bin_map("TestBin5", "Good Night.", 5); 
	as_val_reserve((as_map *)&map2);
	as_arraylist_append(&arglist, (as_val *)((as_map *)&map2));

	as_expbin_puts(&as, &err, NULL, &testKey, (as_list*)&arglist, result);
	LOG("TestBin 4 & 5 inserted");

	LOG("Sleeping for 6 seconds (TestBin 4 & 5 will expire)...");
	sleep(6);

	// Read the record using 'eb.get' after it expires, showing it's gone
	LOG("Getting TestBin 4 & 5 using 'eb interface'...");
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_str(&arglist, "TestBin4");
	as_arraylist_append_str(&arglist, "TestBin5");

	result = as_expbin_get(&as, &err, NULL, &testKey, (as_list*)&arglist, result);
	LOG("%s", as_val_tostring(result));
		
	// Read the record using normal 'get' after it expires, showing it's persistent
	LOG("Getting TestBin 4 & 5 using 'normal get'...");

	// Select bins 4 and 5 to read.
	const char* two_bins[] = {"TestBin4", "TestBin5", NULL};

	as_record* p_rec = NULL;

	// Read only these two bins of the test record from the database.
	if (aerospike_key_select(&as, &err, NULL, &testKey, two_bins, &p_rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_select() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	// Log the result and recycle the as_record object.
	example_dump_record(p_rec);
	as_record_destroy(p_rec);
	p_rec = NULL;

	LOG("Cleaning bins...");
	as_arraylist_inita(&arglist, 5);
	as_arraylist_append_str(&arglist, "TestBin1");
	as_arraylist_append_str(&arglist, "TestBin2");
	as_arraylist_append_str(&arglist, "TestBin3");
	as_arraylist_append_str(&arglist, "TestBin4");
	as_arraylist_append_str(&arglist, "TestBin5");

	as_scan scan;
	LOG("Scan in progress...");
	as_expbin_clean(&as, &err, NULL, &scan, (as_list*) &arglist);
	LOG("Scan completed!");

	LOG("Checking expire bins again using 'eb interface'...");
	as_arraylist_inita(&arglist, 5);
	as_arraylist_append_str(&arglist, "TestBin1");
	as_arraylist_append_str(&arglist, "TestBin2");
	as_arraylist_append_str(&arglist, "TestBin3");
	as_arraylist_append_str(&arglist, "TestBin4");
	as_arraylist_append_str(&arglist, "TestBin5");

	result = as_expbin_get(&as, &err, NULL, &testKey, (as_list*) &arglist, result);
	LOG("%s", as_val_tostring(result));

	LOG("Checking expire bins again using 'normal get'...");
	// Select all previously created bins to read.
	const char* all_bins[] = {"TestBin1", "TestBin2", "TestBin3", "TestBin4", "TestBin5", NULL};

	// Read all these bins of the test record from the database.
	if (aerospike_key_select(&as, &err, NULL, &testKey, all_bins, &p_rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_select() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	// Log the result and recycle the as_record object.
	example_dump_record(p_rec);
	as_record_destroy(p_rec);
	p_rec = NULL;
}