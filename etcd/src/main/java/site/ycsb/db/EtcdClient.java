package site.ycsb.db;

import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.ByteIterator;
import site.ycsb.StringByteIterator;
import site.ycsb.Status;
import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.options.DeleteOption;
import com.coreos.jetcd.options.GetOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;


/**
 * YCSB binding for <a href="https://etcd.io/">etcd</a>.
 *
 * See {@code etcd/README.md} for details.
 */
public class EtcdClient extends DB{
  private static Client client;
  private static KV kvClient;
  
  public void init() throws DBException{
    client = Client.builder().endpoints("http://localhost:2379").build();
    kvClient=client.getKVClient();
  }
  
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result){
    if(table==""){
      table="usertable";
    }
    CompletableFuture<GetResponse> futureResponse = new CompletableFuture<GetResponse>();
    try {
      if(fields != null){
        for(String f : fields) {
          futureResponse = kvClient.get(ByteSequence.fromString(table + "." + key + "." + f));
        }
      }else{
        ByteSequence fullkey = ByteSequence.fromString(table+"."+key);
        GetOption option = GetOption.newBuilder().withPrefix(fullkey).build();
        futureResponse = kvClient.get(fullkey, option);
      }
      GetResponse response = futureResponse.get();
      for(KeyValue kv:response.getKvs()){
        result.put(kv.getKey().toString(), new StringByteIterator(kv.getValue().toString()));
      }      
      return result.isEmpty() ? Status.ERROR : Status.OK;
    } catch (InterruptedException e) {
      e.printStackTrace();
      return Status.ERROR;
    } catch (ExecutionException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }
  
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    Map<String, String> strValues = StringByteIterator.getStringMap(values);
    for(String k : strValues.keySet()) {    
      try {
        kvClient.put(ByteSequence.fromString(table + "." + key + "." + k), 
            ByteSequence.fromString(strValues.get(k))).get();
      } catch (InterruptedException e) {
        e.printStackTrace();
        return Status.ERROR;
      } catch (ExecutionException e) {
        e.printStackTrace();
        return Status.ERROR;
      }
    }    
    return Status.OK;
  }
  
  @Override
  public Status delete(String table, String key) {
    if(table==""){
      table="usertable";
    }
    ByteSequence fullkey = ByteSequence.fromString(table+"."+key);
    DeleteOption option = DeleteOption.newBuilder().withPrefix(fullkey).build();
    
    try {
      return kvClient.delete(fullkey, option).get().getDeleted() > 0 ? Status.OK : Status.ERROR;
    } catch (InterruptedException e) {
      e.printStackTrace();
      return Status.ERROR;
    } catch (ExecutionException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }
  
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    ByteSequence fullkey = ByteSequence.fromString(table+"."+key);
    GetOption option = GetOption.newBuilder().withPrefix(fullkey).build();
    try {
      return kvClient.get(fullkey, option).get().getCount() > 0 ? insert(table, key, values) : Status.ERROR;
    } catch (InterruptedException e) {
      e.printStackTrace();
      return Status.ERROR;
    } catch (ExecutionException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }
  
  @Override
  public Status scan(String table, String startkey, 
      int recordcount, Set<String> fields, 
      Vector<HashMap<String, ByteIterator>> result) {
    ByteSequence fullkey = ByteSequence.fromString(table+"."+startkey);
    GetOption option = GetOption.newBuilder().withLimit(recordcount).withPrefix(fullkey).build();
    try {
      GetResponse response = kvClient.get(fullkey, option).get();
      for(KeyValue kv:response.getKvs()){
        HashMap<String, ByteIterator> map = new HashMap<String, ByteIterator>();
        map.put(kv.getKey().toString(), new StringByteIterator(kv.getValue().toString()));
        result.add(map);
      }
      return Status.OK;
    } catch (InterruptedException e) {
      e.printStackTrace();
      return Status.ERROR;
    } catch (ExecutionException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public void cleanup(){   
    client.close();
  }
}
