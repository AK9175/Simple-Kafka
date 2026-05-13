package com.simplekafka.common.log;

import java.util.List;
import java.util.Map;

public class ClusterMetadata {
    public final Map<String, byte[]> topics;           // name → UUID
    public final Map<String, String> topicNamesById;   // UUID_hex → name
    public final Map<String, List<PartitionInfo>> partitions;

    public ClusterMetadata(Map<String, byte[]> topics, Map<String, String> topicNamesById,
                           Map<String, List<PartitionInfo>> partitions) {
        this.topics = topics;
        this.topicNamesById = topicNamesById;
        this.partitions = partitions;
    }
}
