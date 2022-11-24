package org.example;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Launch {
  public static void main(String[] args) {
    Config ycsb = new Config();
    ycsb.setClusterName("YCSB-hz");

    HazelcastInstance hz = Hazelcast.newHazelcastInstance(ycsb);
  }
}