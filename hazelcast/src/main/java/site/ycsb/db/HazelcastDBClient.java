package site.ycsb.db.hazelcast;

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
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import java.util.concurrent.Future;

/**
 * YCSB binding for <a href="https://hazelcast.com/">hazelcast</a>.
 *
 * See {@code hazelcast/README.md} for details.
 */
public class HazelcastDBClient extends DB{
  private static HazelcastInstance hz;
  private boolean async = false;

  public void init() throws DBException{
    hz = HazelcastClient.newHazelcastClient();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    IMap<String, HashMap<String, String>> map = hz.getMap(table);
    Map<String, String> resultMap = map.get(key);
    StringByteIterator.putAllAsByteIterators(result, resultMap);
    return Status.OK;
  }
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values){
    Map<String, String> strValues = StringByteIterator.getStringMap(values);
    IMap<String, Map<String, String>> distributedMap = hz.getMap(table);
    Future<Map<String, String>> future = (Future<Map<String, String>>) distributedMap.putAsync(key, strValues);
    return Status.OK;
  }
  @Override
  public Status delete(String table, String key) {
    ConcurrentMap<String, Map<String, String>> distributedMap = hz.getMap(table);
    distributedMap.remove(key);
    return Status.OK;
  }
  
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values){
    IMap<String, Map<String, String>> distributedMap = hz.getMap(table);
    if (values != null && values.size() > 0) {
      Map<String, String> resultMap = distributedMap.get(key);
      StringByteIterator.putAllAsStrings(resultMap, values);
      Future<Map<String, String>> future = (Future<Map<String, String>>) distributedMap.putAsync(key, resultMap);
    }
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    String key;
    for (int i = 0; i < recordcount; i++) {
      key = startkey + i;
      if (read(table, key, fields, result.get(i)) == Status.ERROR) {
        return Status.ERROR;
      }
    }
    return Status.OK;
  }
}
