package com.simplekafka.common.responses;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class DescribeTopicPartitionsResponse {
    private static final int BASE_MESSAGE_SIZE = 12;
    private final int correlationId;
    private final List<TopicMetadata> topics;

    public DescribeTopicPartitionsResponse(int correlationId, List<TopicMetadata> topics) {
        this.correlationId = correlationId;
        this.topics = topics;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        int messageSize = BASE_MESSAGE_SIZE;
        for (TopicMetadata topic : topics) {
            messageSize += topic.getSerializedSize();
        }
        out.writeInt(messageSize);
        out.writeInt(correlationId);
        out.writeByte(0);
        out.writeInt(0);
        out.writeByte(topics.size() + 1);
        for (TopicMetadata topic : topics) {
            topic.writeTo(out);
        }
        out.writeByte(0xFF);
        out.writeByte(0);
        out.flush();
    }
}
