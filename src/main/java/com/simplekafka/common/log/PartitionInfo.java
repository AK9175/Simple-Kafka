package com.simplekafka.common.log;

import java.util.List;

public class PartitionInfo {
    public final int partitionId;
    public final int leaderId;
    public final int leaderEpoch;
    public final List<Integer> replicas;
    public final List<Integer> isr;

    public PartitionInfo(int partitionId, int leaderId, int leaderEpoch,
                         List<Integer> replicas, List<Integer> isr) {
        this.partitionId = partitionId;
        this.leaderId = leaderId;
        this.leaderEpoch = leaderEpoch;
        this.replicas = replicas;
        this.isr = isr;
    }

    // bytes this partition entry occupies in the DescribeTopicPartitions response wire format
    public int serializedSize() {
        // fixed: error_code(2) + partition_index(4) + leader_id(4) + leader_epoch(4)
        //      + 3 empty COMPACT_ARRAYs (eligible_lr, last_known_elr, offline) (3)
        //      + partition TAG_BUFFER (1)
        // variable: replica_nodes length(1) + N*4, isr_nodes length(1) + M*4
        return 20 + replicas.size() * 4 + isr.size() * 4;
    }
}
