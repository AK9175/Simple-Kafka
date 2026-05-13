package com.simplekafka.common.responses;

import com.simplekafka.common.log.PartitionInfo;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class TopicMetadata {
    private final String topicName;
    private final short errorCode;
    private final byte[] topicId;
    private final List<PartitionInfo> partitions;

    public TopicMetadata(String topicName, short errorCode, byte[] topicId, List<PartitionInfo> partitions) {
        this.topicName = topicName;
        this.errorCode = errorCode;
        this.topicId = topicId;
        this.partitions = partitions;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeShort(errorCode);
        out.writeByte(topicName.length() + 1);
        out.writeBytes(topicName);
        out.write(topicId);
        out.writeByte(0);
        out.writeByte(partitions.size() + 1);

        for (PartitionInfo p : partitions) {
            out.writeShort(0);
            out.writeInt(p.partitionId);
            out.writeInt(p.leaderId);
            out.writeInt(p.leaderEpoch);
            out.writeByte(p.replicas.size() + 1);
            for (int r : p.replicas) out.writeInt(r);
            out.writeByte(p.isr.size() + 1);
            for (int r : p.isr) out.writeInt(r);
            out.writeByte(1);
            out.writeByte(1);
            out.writeByte(1);
            out.writeByte(0);
        }

        out.writeInt(0);
        out.writeByte(0);
    }

    public int getSerializedSize() {
        int size = 26 + topicName.length();
        for (PartitionInfo p : partitions) {
            size += p.serializedSize();
        }
        return size;
    }
}
