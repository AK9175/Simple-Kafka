package com.simplekafka.common.requests;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FetchRequest {
    public final List<FetchTopic> topics;

    public FetchRequest(List<FetchTopic> topics) {
        this.topics = topics;
    }

    public static class FetchTopic {
        public final byte[] topicId;
        public final List<FetchPartition> partitions;

        public FetchTopic(byte[] topicId, List<FetchPartition> partitions) {
            this.topicId = topicId;
            this.partitions = partitions;
        }
    }

    public static class FetchPartition {
        public final int partitionIndex;
        public final long fetchOffset;

        public FetchPartition(int partitionIndex, long fetchOffset) {
            this.partitionIndex = partitionIndex;
            this.fetchOffset = fetchOffset;
        }
    }

    public static FetchRequest readFrom(DataInputStream in) throws IOException {
        in.skipBytes(24);  // client_id(null=2) + headerTag(1) + max_wait(4) + min_bytes(4) + max_bytes(4) + isolation(1) + session_id(4) + session_epoch(4)

        int topicCount = in.readByte() - 1;
        List<FetchTopic> topics = new ArrayList<>();

        for (int i = 0; i < topicCount; i++) {
            byte[] topicId = new byte[16];
            in.readFully(topicId);

            int partCount = in.readByte() - 1;
            List<FetchPartition> partitions = new ArrayList<>();

            for (int j = 0; j < partCount; j++) {
                int partitionIndex = in.readInt();    // 4 bytes
                in.skipBytes(4);                      // current_leader_epoch
                long fetchOffset = in.readLong();     // 8 bytes
                in.skipBytes(4 + 8 + 4 + 1);         // last_fetched_epoch + log_start_offset + partition_max_bytes + TAG_BUFFER
                partitions.add(new FetchPartition(partitionIndex, fetchOffset));
            }

            in.skipBytes(1);  // topic TAG_BUFFER
            topics.add(new FetchTopic(topicId, partitions));
        }

        in.skipBytes(3);  // forgotten_topics(1) + rack_id(1) + TAG_BUFFER(1)
        return new FetchRequest(topics);
    }
}
