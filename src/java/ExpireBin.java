import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;


public class ExpireBin {
	static final String MODULE_NAME = "expire_bin";
	public AerospikeClient client;
	public WritePolicy policy;
	
	public ExpireBin(AerospikeClient client, WritePolicy policy) {
		this.policy = policy;
		this.client = client;
	}
	
	public Object get(Key key, String ... bins) {
		Value[] valueBins = new Value[bins.length];
		int count = 0;
		for (String bin : bins) {
			valueBins[count] = Value.get(bin);
			count++;
		}
		try {
			return client.execute(policy, key, MODULE_NAME, "get", valueBins);
		} catch (AerospikeException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public Object put(Key key, String binName, Value val, int binTTL, int expCreate) {
		try {
			return client.execute(policy, key, MODULE_NAME, "put", Value.get(binName), val, Value.get(binTTL), Value.get(expCreate));
		} catch (AerospikeException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public Object puts(Key key, Map ... mapBins) {
		Value[] valueMaps = new Value[mapBins.length];
		int count = 0;
		for (Map map : mapBins) {
			valueMaps[count] = Value.get(map);
			count++;
		}
		try {
			return client.execute(policy, key, MODULE_NAME, "puts", valueMaps);
		} catch (AerospikeException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public Object touch(Key key, Map ... mapBins) {
		Value[] valueMaps = new Value[mapBins.length];
		int count = 0;
		for (Map map : mapBins) {
			valueMaps[count] = Value.get(map);
			count++;
		}
		try {
			return client.execute(policy, key, MODULE_NAME, "touch", valueMaps);
		} catch (AerospikeException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public void clean(ScanPolicy scan, String namespace, String set, String ... bins) {
		final Value[] valueBins = new Value[bins.length];
		int count = 0;
		for (String bin : bins) {
			valueBins[count] = Value.get(bin);
			count++;
		}
		try {
			client.scanAll(scan, namespace, set, new ScanCallback() {
				public void scanCallback(Key key, Record record) throws AerospikeException {
					client.execute(new WritePolicy(), key, MODULE_NAME, "clean", valueBins);
				}
			}, new String[] {});
		} catch (AerospikeException e) {
			e.printStackTrace();
		}
	}
	
	public Integer ttl(Key key, String bin) {
		try {
			return (Integer)(client.execute(policy, key, MODULE_NAME, "ttl", Value.get(bin)));
		} catch (AerospikeException e) {
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		AerospikeClient testClient = null;
		System.out.println("This is a demo of the expirable bin module for Java");
		try {
			System.out.println("Connecting to server...");
			testClient = new AerospikeClient("127.0.0.1", 3000);
			System.out.println("Connected!");
		} catch (AerospikeException e) {
			e.printStackTrace();
			System.exit(1);
		}
		ExpireBin eb = new ExpireBin(testClient, new WritePolicy());
		System.out.println("Creating expire bins...");
		Key testKey1 = null, testKey2 = null, testKey3 = null;
		try {
			testKey1 = new Key("test", "expireBin", "eb1");
			testKey2 = new Key("test", "expireBin", "eb2");
			testKey3 = new Key("test", "expireBin", "eb3");
		} catch (AerospikeException e) {
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println(((Integer)(eb.put(testKey1, "TestBin", Value.get("Hello World"), -1, 0)) == 0 ? "TestBin 1 inserted." : "TestBin 1 not inserted" ));
		System.out.println((Integer)eb.put(testKey2, "TestBin", Value.get("I don't expire"), -1, 0) == 0 ? "TestBin 2 inserted" : "TestBin 2 not inserted");
		System.out.println((Integer)eb.put(testKey3, "TestBin", Value.get("I will expire soon"), 5, 0) == 0 ? "TestBin 3 inserted" : "TestBin 3 not inserted");

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
		eb.clean(new ScanPolicy(), "test", "expireBin", "TestBin");
		
		
		
	}

}
