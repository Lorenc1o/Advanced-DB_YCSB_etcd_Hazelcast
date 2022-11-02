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

//At the moment, this is a Demo and does not work as is
//it works in a properly created java project

public class Demo {
	private static Client client;
	private static KV kvClient;
	
	public static void init() {
		client = Client.builder().endpoints("http://localhost:2379").build();
		kvClient=client.getKVClient();
	}

//Read	
	public static int read(String table, String key, Set<String> fields, HashMap<String,String> result) throws InterruptedException, ExecutionException {
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
	    //return result.isEmpty() ? Status.ERROR : Status.OK;

	}

//Insert	
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
//Insert	
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

//Main function for testing
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

