package com.simplekafka.common.requests;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DescribeTopicPartitionsRequest {
    public final List<String> topicNames;

    public DescribeTopicPartitionsRequest(List<String> topicNames) {
        this.topicNames = topicNames;
    }

    public static DescribeTopicPartitionsRequest readFrom(DataInputStream in) throws IOException {
        in.skipBytes(in.readShort());
        in.skipBytes(1);
        int topicCount = in.readByte() - 1;
        List<String> topicNames = new ArrayList<>();
        for (int i = 0; i < topicCount; i++) {
            int nameLen = in.readByte() - 1;
            byte[] nameBytes = new byte[nameLen];
            in.readFully(nameBytes);
            topicNames.add(new String(nameBytes));
            in.skipBytes(1);
        }
        in.skipBytes(6);
        return new DescribeTopicPartitionsRequest(topicNames);
    }
}
