import java.util.Map;

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
					client.execute(new WritePolicy(), key, "MODULE_NAME", "clean_bin", valueBins);
				}
			}, new String[] {});
		} catch (AerospikeException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
