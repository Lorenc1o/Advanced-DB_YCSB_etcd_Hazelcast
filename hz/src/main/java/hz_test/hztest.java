package hz_test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import com.hazelcast.map.IMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;



public class hztest {
	  public static void main1(String[] args) {
	    Config hzConfig = new Config();
	    hzConfig.setClusterName("hazelcast"); 

	    
	    HazelcastInstance hz = Hazelcast.newHazelcastInstance(hzConfig);
	    HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(hzConfig);
	  }
	  
	  public static int read(String table, String key, Set<String> fields, HashMap<String,String> result){
			try {
				for(String f : fields) {
					CompletableFuture<GetResponse> futureResponse = 
							kvClient.get(ByteSequence.fromString(table + "." + key + "." + f));
					GetResponse response = futureResponse.get();
					for(KeyValue kv:response.getKvs())
			        {
			            result.put(kv.getKey().toString(), kv.getValue().toString());
			            System.out.println(kv.getKey().toString() +"=" + kv.getValue().toString());
			        }
				}
			    return result.isEmpty() ? 0 : 1;
			} catch (InterruptedException e) {
				e.printStackTrace();
				return 0;
			} catch (ExecutionException e) {
				e.printStackTrace();
				return 0;
			}
		    //return result.isEmpty() ? Status.ERROR : Status.OK;
			
			//for (Map.Entry<Integer, String> entry : myMap.entrySet()) {
		   // System.out.println(entry.getKey() + " " + entry.getValue())
		   // }
			//https://docs.hazelcast.com/hazelcast/5.2/data-structures/reading-a-map
			
	}
	  public static int insert(String table, String key, HashMap<String,String> values) {
			for(String k : values.keySet()) {		
				try {
					kvClient.put(ByteSequence.fromString(table + "." + key + "." + k), 
							ByteSequence.fromString(values.get(k))).get();
				} catch (InterruptedException e) {
					e.printStackTrace();
					return 0;
				} catch (ExecutionException e) {
					e.printStackTrace();
					return 0;
				}
			}		
			return 1;
		}
		
		public static int delete(String table, String key) {
			ByteSequence fullkey = ByteSequence.fromString(table+"."+key);
	        DeleteOption option = DeleteOption.newBuilder().withPrefix(fullkey).build();
	        
	        try {
				return kvClient.delete(fullkey, option).get().getDeleted() > 0 ? 1 : 0;
			} catch (InterruptedException e) {
				e.printStackTrace();
				return 0;
			} catch (ExecutionException e) {
				e.printStackTrace();
				return 0;
			}
		}
		
		//TODO is it necessary to check existence?
		public static int update(String table, String key, HashMap<String,String> values) {
			ByteSequence fullkey = ByteSequence.fromString(table+"."+key);
	        GetOption option = GetOption.newBuilder().withPrefix(fullkey).build();
			try {
				return kvClient.get(fullkey, option).get().getCount() > 0 ? insert(table, key, values) : 0;
			} catch (InterruptedException e) {
				e.printStackTrace();
				return 0;
			} catch (ExecutionException e) {
				e.printStackTrace();
				return 0;
			}
		}
		
		public static int scan(String table, String startkey, 
				int recordcount, Set<String> fields, 
				Vector<HashMap<String,String>> result) {
			String key;
			for(int i=0; i<recordcount; i++) {
				key = startkey + i;
				if(read(table,key,fields,result.get(i))==0) return 0;	
			}
			return 1;
		}

	    public static void main(String[] args) throws ExecutionException, InterruptedException {

	        init();        

	        String table = "test_table";
	        String key = "test_key";
	        
	        HashMap<String,String> values = new HashMap<String,String>();
	        values.put("field1", "v1");
	        values.put("field2", "v2");
	        
	        System.out.println(insert(table, key,values));
	        
	        Set<String> fields = new HashSet<String>();
	        fields.add("field1");
	        fields.add("field2");
	        HashMap<String,String> result = new HashMap<String,String>();
	        
	        System.out.println(read(table,key,fields,result));
	        
	        System.out.println(delete(table,key));
	        
	        result = new HashMap<String,String>();
	        System.out.println(read(table,key,fields,result));	  
}