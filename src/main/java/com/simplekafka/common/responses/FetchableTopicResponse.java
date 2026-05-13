package com.simplekafka.common.responses;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class FetchableTopicResponse {
    private final byte[] topicId;
    private final List<PartitionData> partitions;

    public FetchableTopicResponse(byte[] topicId, List<PartitionData> partitions) {
        this.topicId = topicId;
        this.partitions = partitions;
    }

    public int getSerializedSize() {
        int size = 18;  // topicId(16) + array_length(1) + tagged_fields(1)
        for (PartitionData p : partitions) size += p.getSerializedSize();
        return size;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.write(topicId);
        out.writeByte(partitions.size() + 1);
        for (PartitionData partition : partitions) {
            partition.writeTo(out);
        }
        out.writeByte(0);
    }
}
