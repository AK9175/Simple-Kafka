package com.simplekafka.broker.server;

import com.simplekafka.common.log.ClusterMetadata;
import com.simplekafka.common.log.PartitionInfo;
import com.simplekafka.common.log.PartitionLog;
import com.simplekafka.common.requests.*;
import com.simplekafka.common.responses.*;
import java.nio.charset.StandardCharsets;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaApis {
    private static final short API_VERSIONS = 18;
    private static final short FETCH = 1;
    private static final short PRODUCE = 0;
    private static final short DESCRIBE_TOPIC_PARTITIONS = 75;

    public static void handle(RequestHeader header, DataInputStream in, DataOutputStream out, ClusterMetadata metadata) throws IOException {
        switch (header.apiKey) {
            case API_VERSIONS -> handleApiVersions(header, in, out);
            case FETCH -> handleFetch(header, in, out, metadata);
            case PRODUCE -> handleProduce(header, in, out, metadata);
            case DESCRIBE_TOPIC_PARTITIONS -> handleDescribeTopicPartitions(header, in, out, metadata);
            default -> System.err.println("Unknown api_key: " + header.apiKey);
        }
    }

    private static void handleApiVersions(RequestHeader header, DataInputStream in, DataOutputStream out) throws IOException {
        in.skipBytes(header.messageSize - 8);
        short errorCode = (header.apiVersion < 0 || header.apiVersion > 4) ? (short) 35 : 0;
        List<ApiVersionEntry> entries = List.of(
            new ApiVersionEntry((short) 18, (short) 0, (short) 4),
            new ApiVersionEntry((short) 1, (short) 0, (short) 16),
            new ApiVersionEntry((short)0, (short) 0, (short) 11),
            new ApiVersionEntry((short) 75, (short) 0, (short) 0)
        );
        new ApiVersionsResponse(header.correlationId, errorCode, entries).writeTo(out);
    }

    private static void handleProduce(RequestHeader header, DataInputStream in, DataOutputStream out, ClusterMetadata metadata) throws IOException {
        ProduceRequest request = ProduceRequest.readFrom(in);

        Map<String, Map<Integer, ProduceResponse.PartitionResult>> results = new HashMap<>();
        for (ProduceRequest.TopicData topic : request.topics) {
            Map<Integer, ProduceResponse.PartitionResult> partitionResults = new HashMap<>();
            List<PartitionInfo> partitions = metadata.partitions.getOrDefault(topic.topicName, List.of());
            for (ProduceRequest.PartitionProduceData p : topic.partitions) {
                boolean valid = metadata.topics.containsKey(topic.topicName)
                    && partitions.stream().anyMatch(pi -> pi.partitionId == p.partitionIndex);
                if (!valid) {
                    partitionResults.put(p.partitionIndex, new ProduceResponse.PartitionResult((short) 3, -1L));
                } else {
                    long baseOffset = PartitionLog.writeBatch(topic.topicName, p.partitionIndex, p.records);
                    partitionResults.put(p.partitionIndex, new ProduceResponse.PartitionResult((short) 0, baseOffset));
                }
            }
            results.put(topic.topicName, partitionResults);
        }
        new ProduceResponse(header.correlationId, request.topics, results).writeTo(out);
    }

    private static void handleFetch(RequestHeader header, DataInputStream in, DataOutputStream out, ClusterMetadata metadata) throws IOException {
        FetchRequest fetchRequest = FetchRequest.readFrom(in);
        List<FetchableTopicResponse> responses = new ArrayList<>();

        for (FetchRequest.FetchTopic topic : fetchRequest.topics) {
            String topicName = metadata.topicNamesById.get(
                new String(topic.topicId, StandardCharsets.ISO_8859_1));

            List<PartitionData> partitions = new ArrayList<>();
            for (FetchRequest.FetchPartition p : topic.partitions) {
                byte[] records = new byte[0];
                if (topicName != null) {
                    records = PartitionLog.readBatch(topicName, p.partitionIndex, p.fetchOffset);
                }
                partitions.add(new PartitionData(p.partitionIndex, (short) 0, records));
            }
            responses.add(new FetchableTopicResponse(topic.topicId, partitions));
        }

        new FetchResponse(header.correlationId, (byte) 0, 0, (short) 0, 0, responses, (byte) 0).writeTo(out);
    }

    private static void handleDescribeTopicPartitions(RequestHeader header, DataInputStream in, DataOutputStream out, ClusterMetadata metadata) throws IOException {
        DescribeTopicPartitionsRequest request = DescribeTopicPartitionsRequest.readFrom(in);
        List<TopicMetadata> topicMetadata = new ArrayList<>();
        for (String topicName : request.topicNames) {
            if (metadata.topics.containsKey(topicName)) {
                List<PartitionInfo> partitions = metadata.partitions.getOrDefault(topicName, List.of());
                topicMetadata.add(new TopicMetadata(topicName, (short) 0, metadata.topics.get(topicName), partitions));
            } else {
                topicMetadata.add(new TopicMetadata(topicName, (short) 3, new byte[16], List.of()));
            }
        }
        new DescribeTopicPartitionsResponse(header.correlationId, topicMetadata).writeTo(out);
    }
}
