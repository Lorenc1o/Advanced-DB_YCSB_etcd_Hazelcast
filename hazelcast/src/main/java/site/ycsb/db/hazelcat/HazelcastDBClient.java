package site.ycsb.db.hazelcast;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * bullshit.
 */

public class HazelcastDBClient {
  private static HazelcastInstance hz;
  private boolean async = false;

  public static void init() {
    HazelcastInstance hzl = HazelcastClient.newHazelcastClient();
  }

  public static int read(String table, String key, Set<String> fields, HashMap<String, String> result) {
    ConcurrentMap<String, Map<String, String>> distributedMap = hz.getMap(table);
    Map<String, String> resultMap = distributedMap.get(key);
    result.putAll(resultMap);
    return 1;
  }

  public static int insert(String table, String key, HashMap<String, String> values)
  throws InterruptedException, ExecutionException, TimeoutException {
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

  public int update(String table, String key, HashMap<String, String> values)
          throws InterruptedException, ExecutionException, TimeoutException {
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
        Future<Map<String, String>> future = (Future<Map<String, String>>)
            distributedMap.putAsync(key, values);
        future.get(50, TimeUnit.MINUTES);
      } else {
        distributedMap.put(key, resultMap);
      }
    }
    return 1;
  }

  public static int scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, String>> result) {
    String key;
    for (int i = 0; i < recordcount; i++) {
      key = startkey + i;
      if (read(table, key, fields, result.get(i)) == 0) {
        return 0;
      }
    }
    return 1;
  }
}
