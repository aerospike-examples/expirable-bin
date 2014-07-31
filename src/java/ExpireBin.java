//Copyright 2014 Aerospike, Inc
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.RegisterTask;


public class ExpireBin {
	static final String MODULE_NAME = "expire_bin";
	public AerospikeClient client;
	public WritePolicy policy;

	/**
	 * Initialize ExpireBin object with suitable client and policy
	 * 
	 * @param client - client to perform operations on
	 * @param policy - configuration parameters for expire bin ops
	 */
	public ExpireBin(AerospikeClient client, WritePolicy policy) {
		this.policy = policy;
		this.client = client;
	}

	/**
	 * Try to get values from the expire bin
	 * 
	 * @param - key to get from
	 * @param bins -  list of bin names to attempt to get from
	 * @return list containing respective values from each bin, or null if
	 * expired or bin doesn't exist
	 * @throws AerospikeException 
	 */
	public Object get(Key key, String ... bins) throws AerospikeException {
		Value[] valueBins = new Value[bins.length];
		int count = 0;
		for (String bin : bins) {
			valueBins[count] = Value.get(bin);
			count++;
		}
		return client.execute(policy, key, MODULE_NAME, "get", valueBins);
	}

	/**
	 * Create or update expire bins. If the expCreate flag is set to true, 
	 * all newly created bins will be expire bins otherwise, only normal bins will be created
	 * and existing expire bins will be updated. Note: existing expire bins will not be converted
	 * into normal bins if create flag is off.
	 * 
	 * @param key - Record key to apply operation on
	 * @param binName - Bin name to create or update
	 * @param val - bin value
	 * @param binTTL - expiration time in seconds or -1 for no expiration
	 * @param expCreate - set true to create expire bins, false to create normal bins
	 * @return 0 in success, 1 if error
	 * @throws AerospikeException 
	 */
	public Integer put(Key key, String binName, Value val, int binTTL, boolean expCreate) throws AerospikeException {
		int create;
		if (expCreate == false) {
			create = 1;
		}else {
			create = 0;
		}
		return (Integer) client.execute(policy, key, MODULE_NAME, "put", Value.get(binName), val, Value.get(binTTL), Value.get(create));
	}

	/**
	 * Batch create or update expire bins for a given key. Use the PutBin map
	 * wrapper to store each put operation. Don't supply a binTTL to turn bin 
	 * creation off. 
	 * 
	 * @param key - Record key to store bins
	 * @param mapBins - list of PutBins containing operation arguments
	 * @return - 0 if all bins succeeded, 1 if failure
	 * @throws AerospikeException 
	 */
	public Object puts(Key key, PutBin ... mapBins) throws AerospikeException {
		Value[] valueMaps = new Value[mapBins.length];
		int count = 0;
		for (PutBin map : mapBins) {
			valueMaps[count] = Value.get(map);
			count++;
		}
		return client.execute(policy, key, MODULE_NAME, "puts", valueMaps);
	}

	/**
	 * Batch update the bin TTLs. Use this method to change or reset 
	 * the bin TTL of multiple bins in a record. 
	 * 
	 * @param key - Record key
	 * @param mapBins - list of TouchBins containing operation arguments
	 * @return 0 on success of all touch operations, 1 if a failure occurs
	 * @throws AerospikeException 
	 */
	public Object touch(Key key, TouchBin ... mapBins) throws AerospikeException {
		Value[] valueMaps = new Value[mapBins.length];
		int count = 0;
		for (Map map : mapBins) {
			valueMaps[count] = Value.get(map);
			count++;
		}
		return client.execute(policy, key, MODULE_NAME, "touch", valueMaps);
	}

	/**
	 * Perform a scan of the database and clear out expired expire bins
	 * 
	 * @param scan - scan policy containing which records should be scanned
	 * @param namespace - namespace of server to scan
	 * @param set - set of server to scan
	 * @param bins - list of bins to scan
	 * @throws AerospikeException 
	 */
	public ExecuteTask clean(Policy policy, Statement statement, String ... bins) throws AerospikeException {
		final Value[] valueBins = new Value[bins.length];
		int count = 0;
		for (String bin : bins) {
			valueBins[count] = Value.get(bin);
			count++;
		}
		return client.execute(policy, statement, MODULE_NAME, "clean", valueBins);
	}

	/**
	 * Get time bin will expire in seconds.
	 * 
	 * @param key - Record key
	 * @param bin - Bin Name
	 * @return time in seconds bin will expire, -1 or null if it doesn't expire
	 * @throws AerospikeException 
	 */
	public Integer ttl(Key key, String bin) throws AerospikeException {
		return (Integer)(client.execute(policy, key, MODULE_NAME, "ttl", Value.get(bin)));
	}

	public class PutBin extends HashMap<Value, Value> {
		/**
		 * Map wrapper to store batch put operations
		 */
		private static final long serialVersionUID = 1L;

		
		/**
		 * Map to create / update a bin with bin creation turned on
		 * 
		 * @param binName
		 * @param val
		 * @param binTTL
		 */
		public PutBin(String binName, Value val, int binTTL) {
			super();
			this.put(Value.get("bin"), Value.get(binName));
			this.put(Value.get("bin_ttl"), Value.get(binTTL));
			this.put(Value.get("val"), val);
		}
		
		/**
		 * Map to create / update a bin with bin creation turned off
		 * 
		 * @param binName
		 * @param val
		 */
		public PutBin(String binName, Value val) {
			super();
			this.put(Value.get("bin"), Value.get(binName));
			this.put(Value.get("val"), val);
		}
	}
	

	public class TouchBin extends HashMap<Value, Value> {
		/**
		 * Map wrapper to store touch operations
		 */
		private static final long serialVersionUID = 1L;
		
		/**
		 * Map to perform a touch bin operation
		 * 
		 * @param binName - name of bin to touch
		 * @param binTTL - new TTL for bin
		 */
		public TouchBin(String binName, int binTTL) {
			super();
			this.put(Value.get("bin"), Value.get(binName));
			this.put(Value.get("bin_ttl"), Value.get(binTTL));
		}
	}
	
	public static void main(String[] args) {
		AerospikeClient testClient = null;
		System.out.println("This is a demo of the expirable bin module for Java");
		try {
			System.out.println("Connecting to server...");
			testClient = new AerospikeClient("127.0.0.1", 3000);
			System.out.println("Connected!");

			ExpireBin eb = new ExpireBin(testClient, new WritePolicy());
			System.out.println("Creating expire bins...");
			Key testKey1 = null, testKey2 = null, testKey3 = null;
			testKey1 = new Key("test", "expireBin", "eb1");
			testKey2 = new Key("test", "expireBin", "eb2");
			testKey3 = new Key("test", "expireBin", "eb3");

			System.out.println(((Integer)(eb.put(testKey1, "TestBin", Value.get("Hello World"), -1, true)) == 0 ? "TestBin 1 inserted." : "TestBin 1 not inserted" ));
			System.out.println((Integer)eb.put(testKey2, "TestBin", Value.get("I don't expire"), -1, true) == 0 ? "TestBin 2 inserted" : "TestBin 2 not inserted");
			System.out.println((Integer)eb.put(testKey3, "TestBin", Value.get("I will expire soon"), 5, true) == 0 ? "TestBin 3 inserted" : "TestBin 3 not inserted");

			System.out.println("Getting expire bins...");

			System.out.println("TestBin 1: " + eb.get(testKey1, "TestBin").toString());
			System.out.println("TestBin 2: " + eb.get(testKey2, "TestBin").toString());
			System.out.println("TestBin 3: " + eb.get(testKey3, "TestBin").toString());

			System.out.println("Current time is: " + (System.currentTimeMillis() / 1000) + "\nGetting bin TTLs...");
			System.out.println("TestBin 1 TTL: " + eb.ttl(testKey1, "TestBin"));
			System.out.println("TestBin 2 TTL: " + eb.ttl(testKey2, "TestBin"));
			System.out.println("TestBin 3 TTL: " + eb.ttl(testKey3, "TestBin"));


			System.out.println("Waiting for TestBin 3 to expire...");

			try {
				TimeUnit.SECONDS.sleep(10);
			} catch(InterruptedException ex) {
				Thread.currentThread().interrupt();
			}

			System.out.println("Getting expire bins again...");

			System.out.println("TestBin 1: " + eb.get(testKey1, "TestBin").toString());
			System.out.println("TestBin 2: " + eb.get(testKey2, "TestBin").toString());
			System.out.println("TestBin 3: " + eb.get(testKey3, "TestBin").toString());

			System.out.println("Cleaning bins...");
			Statement stmt = new Statement();
			stmt.setNamespace("test");
			stmt.setSetName("expireBin");
			eb.clean(new WritePolicy(), stmt, "TestBin");
		} catch (AerospikeException e) {
			e.printStackTrace();
			System.exit(1);
		}



	}

}
