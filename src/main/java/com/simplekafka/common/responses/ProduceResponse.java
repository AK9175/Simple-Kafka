package com.simplekafka.common.responses;

import com.simplekafka.common.requests.ProduceRequest;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ProduceResponse {
    public static class PartitionResult {
        public final short errorCode;
        public final long baseOffset;

        public PartitionResult(short errorCode, long baseOffset) {
            this.errorCode = errorCode;
            this.baseOffset = baseOffset;
        }
    }

    private final int correlationId;
    private final List<ProduceRequest.TopicData> topics;
    private final Map<String, Map<Integer, PartitionResult>> results;

    public ProduceResponse(int correlationId, List<ProduceRequest.TopicData> topics, Map<String, Map<Integer, PartitionResult>> results) {
        this.correlationId = correlationId;
        this.topics = topics;
        this.results = results;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        int messageSize = 4 + 1;
        messageSize += 1;
        for (ProduceRequest.TopicData topic : topics) {
            messageSize += 1 + topic.topicName.length();
            messageSize += 1;
            messageSize += topic.partitions.size() * (4 + 2 + 8 + 8 + 8 + 1 + 1 + 1);
            messageSize += 1;
        }
        messageSize += 4 + 1;

        out.writeInt(messageSize);
        out.writeInt(correlationId);
        out.writeByte(0);

        out.writeByte(topics.size() + 1);
        for (ProduceRequest.TopicData topic : topics) {
            out.writeByte(topic.topicName.length() + 1);
            out.writeBytes(topic.topicName);

            out.writeByte(topic.partitions.size() + 1);
            for (ProduceRequest.PartitionProduceData p : topic.partitions) {
                PartitionResult result = results.getOrDefault(topic.topicName, Map.of())
                    .getOrDefault(p.partitionIndex, new PartitionResult((short) 3, -1L));
                out.writeInt(p.partitionIndex);
                out.writeShort(result.errorCode);
                out.writeLong(result.baseOffset);
                out.writeLong(-1);
                out.writeLong(-1);
                out.writeByte(1);
                out.writeByte(0);
                out.writeByte(0);
            }
            out.writeByte(0);
        }
        out.writeInt(0);
        out.writeByte(0);
        out.flush();
    }
}
