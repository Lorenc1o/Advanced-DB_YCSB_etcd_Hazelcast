package site.ycsb.db;

import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.ByteIterator;
import site.ycsb.StringByteIterator;
import site.ycsb.Status;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentMap;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

/**
 * YCSB binding for <a href="https://hazelcast.com/">hazelcast</a>.
 *
 * See {@code hazelcast/README.md} for details.
 */
public class HazelcastDBClient extends DB{
  private static HazelcastInstance hz;

  @Override
  public void init() throws DBException{
    Config confYCSB = new Config();
    confYCSB.setClusterName("YCSB-hz");
    hz = Hazelcast.newHazelcastInstance(confYCSB);
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if(table==""){
      table="usertable";
    }
    try{
      IMap<String, Map<String, String>> myMap = hz.getMap(table);
      Map<String, String> resultMap = myMap.get(key);
      if(fields != null){
        for(String f : fields) {
          result.put(f, new StringByteIterator(resultMap.get(f)));
        }
      } else if(resultMap != null){
        for(Map.Entry<String, String> entry : resultMap.entrySet()) {
          result.put(entry.getKey(), new StringByteIterator(entry.getValue()));
        }
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values){
    Map<String, String> strValues = StringByteIterator.getStringMap(values);
    IMap<String, Map<String, String>> myMap = hz.getMap(table);
    try{
      myMap.put(key, strValues);
      return Status.OK;
    }catch(Exception e){
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    ConcurrentMap<String, Map<String, String>> myMap = hz.getMap(table);
    try{
      myMap.remove(key);
      return Status.OK;
    }catch(Exception e){
      e.printStackTrace();
      return Status.ERROR;
    }
  }
  
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values){
    return insert(table, key, values);
  }
  
  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    IMap<String, Map<String, String>> myMap = hz.getMap(table);
    int count = 0;
    try{
      for (String key : myMap.keySet()) {
        if(count >= recordcount){
          break;
        }
        if(key.compareTo(startkey) >= 0){
          count++;
          HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
          Map<String, String> res = myMap.get(key);
          StringByteIterator.putAllAsByteIterators(values, res);
          result.add(values);
        }
      }
      return Status.OK;
    } catch(Exception e){
      e.printStackTrace();
      return Status.ERROR;
    }
  }
}

