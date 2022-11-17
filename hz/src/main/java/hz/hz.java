package hz;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.hazelcast.multimap.MultiMap;
import com.hazelcast.query.Predicates;

public class hz {
	
	private static HazelcastInstance hz;
	private static final int MAP = 1;
    private int pollTimeoutMs = 50;
    private boolean async = false;
    
    private static final ReentrantLock _lock = new ReentrantLock();
    private HashMap<String, IMap<String, Map<String, String>>> mapMap = new HashMap<String, IMap<String, Map<String, String>>>();
	
	public static void init() {		
		// Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
        hz = HazelcastClient.newHazelcastClient();
	}
	
	
	public static int read(String table, String key, Set<String> fields, HashMap<String, String> result) {
		ConcurrentMap<String, Map<String, String>> distributedMap = hz.getMap(table);
        Map<String, String> resultMap = distributedMap.get(key);
        result.putAll(resultMap);
        return 1;
	}
	
	public static int insert(String table, String key, HashMap<String,String> values) throws InterruptedException, ExecutionException, TimeoutException {
		IMap<String, Map<String, String>> distributedMap = hz.getMap(table);
        Future<Map<String, String>> future = (Future<Map<String, String>>) distributedMap.putAsync(key, values);
        future.get(50, TimeUnit.MINUTES);
		return 1;
	}
	
	public static int delete(String table, String key) {
		ConcurrentMap<String, Map<String, String>> distributedMap = hz.getMap(table);
        distributedMap.remove(key);
		return 1;
	}
	
	
	public int update(String table, String key, HashMap<String,String> values) throws InterruptedException, ExecutionException, TimeoutException {
		IMap<String, Map<String, String>> distributedMap = hz.getMap(table);
        if (values != null && values.size() > 0) {
            Map<String, String> resultMap = distributedMap.get(key);
            Iterator<String> iter = values.keySet().iterator();
            String k = null;
            while (iter.hasNext()) {
                k = iter.next();
                resultMap.put(k, values.get(k));
            }
            if (this.async) {
            	Future<Map<String, String>> future = (Future<Map<String, String>>) distributedMap.putAsync(key, values);
                future.get(50,TimeUnit.MINUTES);

            } else {
                distributedMap.put(key, resultMap);
            }
        }
        return 1;
	}
	
	
	public static int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String,String>> result) {
		String key;
		for(int i=0; i<recordcount; i++) {
			key = startkey + i;
			if(read(table,key,fields,result.get(i))==0) return 0;	
		}
		return 1;
	}
	
    public static void main(String[] args) {

       init();
       
       String table = "test_table";
       String key = "test_key";
       
       HashMap<String,String> values = new HashMap<String,String>();
       values.put("field1", "v1");
       values.put("field2", "v2");
       
       try {
		System.out.println(insert(table, key,values));
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (ExecutionException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (TimeoutException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
       
       Set<String> fields = new HashSet<String>();
       fields.add("field1");
       fields.add("field2");
       HashMap<String,String> result = new HashMap<String,String>();
       
       System.out.println(read(table,key,fields,result));
       
       System.out.println(delete(table,key));
       
              

	}
       
   
    
    }
   

