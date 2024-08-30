package com.google.cloud.teleport.v2.spanner.migrations.shard;

import java.util.HashMap;
import java.util.Map;

public class ShardingContext {

    private Map<String,Map<String,String>> streamToDbAndShardMap;

    public ShardingContext() {
        this.streamToDbAndShardMap = new HashMap<String, Map<String, String>>();
    }

    public ShardingContext(Map<String, Map<String, String>> streamToDbAndShardMap) {
        this.streamToDbAndShardMap = streamToDbAndShardMap;
    }

    public Map<String, Map<String, String>> getStreamToDbAndShardMap() {
        return streamToDbAndShardMap;
    }

    @Override
    public String toString() {
        return "ShardingContext{" +
                "streamToDbAndShardMap=" + streamToDbAndShardMap +
                '}';
    }
}
