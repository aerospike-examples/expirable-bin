#!/usr/bin/env python

# Copyright 2014 Aerospike, Inc

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import aerospike
import time

MODULE_NAME = "expire_bin"
GET_OP = "get"
PUT_OP = "put"
BATCH_PUT_OP = "puts"
TTL_OP = "ttl"
TOUCH_OP = "touch"
CLEAN_OP = "clean"
CITRUSLEAF_EPOCH = 1262304000

class ExpireBin:
	def __init__(self, client):
		"""Initialize the ExpireBin module

		Args:
			client -- aerospike client to perform ops
		"""

		self.client = client

	def get(self, policy, key, *bins):
		"""Attempt to retrieve values from list of bins. The bins
		can be expire bins or normal bins.

		Args:
			policy -- policy to use for op
			key -- tuple (namespace, set, record name)
			*bins -- one or more bin names to retrieve values from

		Returns:
			record dict: A record with each bin value mapped to the bin name.
			If the bin value expired or does not exist, there will not
			be an entry in the dict

		Raises:
			Exception: Exception with details of server error.
		"""
		rv = self.client.apply(key, MODULE_NAME, GET_OP, list(bins), policy)
		if not type(rv) == dict:
			raise Exception("Get operation failed, return {0} instead of map".format(type(rv)))
		return rv

	def put(self, policy, key, bin, val, bin_ttl):
		"""Create or update expire bins. If bin_ttl is not None,
		all newly created bins will be expire bins otherwise, only normal bins will be created
		Note: existing expire bins will not be converted into normal bins if bin_ttl is None.

		Args:
			policy -- policy to use for op
			key -- tuple (namespace, set, record name)
			bin -- bin name
			val -- value to store into bin
			bin_ttl -- expiration time in seconds or -1 for no expiration

		Returns:
			int: 0 if success, 1 otherwise

		Raises:
			Exception: Exception with details of server error.
		"""
		return self.client.apply(key, MODULE_NAME, PUT_OP, [bin, val, bin_ttl], policy)

	def puts(self, policy, key, *binMaps):
		"""Batch create or update expire bins for a given key. Use the dict
			{'bin' : bin_name, 'val' : bin_value, 'bin_ttl' : ttl}
		to store each put operation. Omit the bin_ttl to turn bin creation off.

		Args:
			policy -- policy to use for op
			key -- tuple (namespace, set, record name)
			*binMaps -- dict {'bin' : bin_name, 'val' : bin_val, 'bin_ttl' : ttl}

		Returns:
			int: 0 if all ops succeeded, 1 if error occurred

		Raises:
			Exception: Exception with details of server error.

		"""
		return self.client.apply(key, MODULE_NAME, BATCH_PUT_OP, list(binMaps), policy)

	def touch(self, policy, key, *mapBins):
		"""Batch update the bin TTLs. Us this method to change or reset the bin TTL of
		multiple bins in a record. Use a dict to store each touch operation.

		Args:
			policy -- policy to use for op
			key -- tuple (namespace, set, record name)
			*mapBins -- dict {'bin' : bin_name, 'bin_ttl' : ttl}

		Returns:
			int: 0 on success of ALL touch operations, 1 if a failure occurs

		Raises:
			Exception: Exception with details of server error.
		"""
		return self.client.apply(key, MODULE_NAME, TOUCH_OP, list(mapBins), policy)

	def clean(self, policy, scan, *bins):
		"""Clear out the expired bins on a scan of the database

		Args:
			policy -- policy to use for op
			scan -- Scan object to run bin clean on
			*bins -- bin names to clean out

		Raises:
			Exception: Exception with details of server error.
		"""
		raise NotImplementedError("Scan UDF not yet implemented in python client")
		#def callback((key, meta, record)):
		#	self.client.apply(key, MODULE_NAME, "does_not_exist", list(bins), self.policy)
		#scan.foreach(callback)

	def ttl(self, policy, key, bin):
		"""Get the time bin will expire in seconds.

		Args:
			policy -- policy to use for op
			key -- tuple (namespace, set, record name)
			bin -- bin name

		Returns:
			int: Time to expire in seconds

		Raises:
			Exception: Exception with details of server error.
		"""
		return self.client.apply(key, MODULE_NAME, TTL_OP, [bin], policy)

def main():
	config = { 'hosts' : [ ('127.0.0.1', 3000) ]}
	policy = { "timeout" : 2000 }
	testClient = aerospike.client(config).connect()
	try:
		print "Registering UDF..."
		filename = "../../expire_bin.lua"
		udf_type = 0
		testClient.udf_put(policy, filename, udf_type)
		print "UDF Registered!"
	except Exception as e:
		print "Error registering lua function: {0}".format(e)

	eb = ExpireBin(testClient)
	key = ("test", "expireBin", "eb")

	print "Creating expire bins..."

	print "TestBin 1: {0}".format("Inserted!" if eb.put(policy, key, "TestBin1", "Hello World", -1) is 0 else "Insertion Failed")
	print "TestBin 2: {0}".format("Inserted!" if eb.put(policy, key, "TestBin2", "I don't expire", -1) is 0 else "Insertion Failed")
	print "TestBin 3: {0}".format("Inserted!" if eb.put(policy, key, "TestBin3", "I will expire soon", 5 ) is 0 else "Insertion Failed")
	print "TestBin 4 & 5: {0}".format("Inserted!" if eb.puts(policy, key, {'bin' : "TestBin4", 'val' : "Good Morning.", 'bin_ttl' : 100}, {'bin' : "TestBin5", 'val' : "Good Night."})  is 0 else "Insertion Failed")


	print "Getting expire bins..."

	print "TestBins: {0}".format(eb.get(policy, key, "TestBin1", "TestBin2", "TestBin3", "TestBin4", "TestBin5"))

	print "Current time is: {0}\nGetting bin TTLs...".format(time.time() - CITRUSLEAF_EPOCH)
	print "TestBin 1 TTL: {0}".format(eb.ttl(policy, key, "TestBin1"))
	print "TestBin 2 TTL: {0}".format(eb.ttl(policy, key, "TestBin2"))
	print "TestBin 3 TTL: {0}".format(eb.ttl(policy, key, "TestBin3"))
	print "TestBin 4 TTL: {0}".format(eb.ttl(policy, key, "TestBin4"))
	print "TestBin 5 TTL: {0}".format(eb.ttl(policy, key, "TestBin5"))


	print "Waiting for TestBin 3 to expire..."

	time.sleep(10)

	print "Getting expire bins again..."

	print "TestBins: {0}".format(eb.get(policy, key, "TestBin1", "TestBin2", "TestBin3", "TestBin4", "TestBin5"))

	print "Changing expiration times..."

	eb.touch(policy, key, {'bin' : "TestBin1", 'bin_ttl' : 10}, {'bin' : "TestBin4", 'bin_ttl' : 5});

	print "Getting bin TTLs again..."

	print "TestBin 1 TTL: {0}".format(eb.ttl(policy, key, "TestBin1"))
	print "TestBin 2 TTL: {0}".format(eb.ttl(policy, key, "TestBin2"))
	print "TestBin 3 TTL: {0}".format(eb.ttl(policy, key, "TestBin3"))
	print "TestBin 4 TTL: {0}".format(eb.ttl(policy, key, "TestBin4"))
	print "TestBin 5 TTL: {0}".format(eb.ttl(policy, key, "TestBin5"))


	# scan udf not yet implemented in python client
	print "Cleaning bins..."

	testScan = testClient.scan("test", "expireBin")
	try:
		eb.clean(policy, testScan, "TestBin1", "TestBin2", "TestBin3", "TestBin4", "TestBin5")
	except NotImplementedError as e:
		print "Scan UDF not implemented in current version of python client."

	testClient.close()
if __name__ == "__main__":
	main()


