package com.simplekafka.common.requests;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProduceRequest {
    public final List<TopicData> topics;

    public ProduceRequest(List<TopicData> topics) {
        this.topics = topics;
    }

    public static class PartitionProduceData {
        public final int partitionIndex;
        public final byte[] records;  // raw record batch bytes, may be empty

        public PartitionProduceData(int partitionIndex, byte[] records) {
            this.partitionIndex = partitionIndex;
            this.records = records;
        }
    }

    public static class TopicData {
        public final String topicName;
        public final List<PartitionProduceData> partitions;

        public TopicData(String topicName, List<PartitionProduceData> partitions) {
            this.topicName = topicName;
            this.partitions = partitions;
        }
    }

    public static ProduceRequest readFrom(DataInputStream in) throws IOException {
        in.skipBytes(3);  // client_id null (NULLABLE_STRING = 2 bytes) + header TAG_BUFFER (1 byte)

        // transactional_id: COMPACT_NULLABLE_STRING (null = 0x00)
        int transactionalIdLen = readUnsignedVarint(in) - 1;
        if (transactionalIdLen > 0) in.skipBytes(transactionalIdLen);

        in.skipBytes(2);  // acks (INT16)
        in.skipBytes(4);  // timeout_ms (INT32)

        int topicCount = readUnsignedVarint(in) - 1;
        List<TopicData> topics = new ArrayList<>();
        for (int i = 0; i < topicCount; i++) {
            int nameLen = readUnsignedVarint(in) - 1;
            byte[] nameBytes = new byte[nameLen];
            in.readFully(nameBytes);
            String topicName = new String(nameBytes);

            int partCount = readUnsignedVarint(in) - 1;
            List<PartitionProduceData> partitions = new ArrayList<>();
            for (int j = 0; j < partCount; j++) {
                int partitionIndex = in.readInt();
                in.skipBytes(4);  // partition_leader_epoch (v10+)

                int recordsLen = readUnsignedVarint(in) - 1;
                byte[] records = new byte[0];
                if (recordsLen > 0) {
                    records = new byte[recordsLen];
                    in.readFully(records);
                }
                in.skipBytes(1);  // partition TAG_BUFFER
                partitions.add(new PartitionProduceData(partitionIndex, records));
            }
            in.skipBytes(1);  // topic TAG_BUFFER
            topics.add(new TopicData(topicName, partitions));
        }
        in.skipBytes(1);  // request TAG_BUFFER
        return new ProduceRequest(topics);
    }

    private static int readUnsignedVarint(DataInputStream in) throws IOException {
        int result = 0, shift = 0, b;
        do {
            b = in.readByte() & 0xFF;
            result |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }
}
