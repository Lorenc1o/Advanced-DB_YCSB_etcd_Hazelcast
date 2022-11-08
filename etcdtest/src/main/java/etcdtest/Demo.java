package etcdtest;
import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.DeleteResponse;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.options.DeleteOption;
import com.coreos.jetcd.options.GetOption;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import java.util.*;

public class Demo {
	private static Client client;
	private static KV kvClient;
	
    public static final String HOST_PROPERTY = "etcd.host";
    public static final String PORT_PROPERTY = "etcd.port";
    public static final String PASSWORD_PROPERTY = "etcd.password";
    public static final String CLUSTER_PROPERTY = "etcd.cluster";
    public static final String TIMEOUT_PROPERTY = "etcd.timeout";

    public static final String INDEX_KEY = "_indices";

	public static void init() {		
		client = Client.builder().endpoints("http://localhost:2379").build();
		kvClient=client.getKVClient();
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

}
