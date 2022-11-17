package site.ycsb.db;

import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.ByteIterator;
import site.ycsb.StringByteIterator;
import site.ycsb.Status;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;

import com.hazelcast.client.*;
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


public class HazelcastClient {
    public static void main(String[] args) {
        // Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
        HazelcastInstance hz = HazelcastClient.newHazelcastClient();
        // Get the Distributed Map from Cluster.
       
        // Shutdown this Hazelcast client
       // hz.shutdown();
    }
}

public class HazelcastClient extends DB{
  private static Client client;
  private static KV kvClient;
  
  public void init() throws DBException{
    client = Client.builder().endpoints("http://localhost:2379").build();
    kvClient=client.getKVClient();
  }
  
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result){
    try {
      for(String f : fields) {
        CompletableFuture<GetResponse> futureResponse = 
            kvClient.get(ByteSequence.fromString(table + "." + key + "." + f));
        GetResponse response = futureResponse.get();
        for(KeyValue kv:response.getKvs()){
          result.put(kv.getKey().toString(), new StringByteIterator(kv.getValue().toString()));
        }
      }
      return result.isEmpty() ? Status.ERROR : Status.OK;
    } catch (InterruptedException e) {
      e.printStackTrace();
      return Status.ERROR;
    } catch (ExecutionException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    //return result.isEmpty() ? Status.ERROR : Status.OK;

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
  
  //TODO is it necessary to check existence?
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
    String key;
    for(int i=0; i<recordcount; i++) {
      key = startkey + i;
      if(read(table, key, fields, result.get(i))==Status.ERROR){
        return Status.ERROR;
      }
    }
    return Status.OK;
  }
}
