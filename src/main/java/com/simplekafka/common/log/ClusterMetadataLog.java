package com.simplekafka.common.log;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterMetadataLog {
    private static final String LOG_DIR =
        "/tmp/kraft-combined-logs/__cluster_metadata-0";
    private static final int TOPIC_RECORD_TYPE = 2;
    private static final int PARTITION_RECORD_TYPE = 1;

    public static ClusterMetadata load() throws IOException {
        Map<String, byte[]> topics = new HashMap<>();
        Map<String, String> topicNamesById = new HashMap<>();
        Map<String, List<PartitionInfo>> partitionsByTopicName = new HashMap<>();

        File dir = new File(LOG_DIR);
        if (!dir.exists()) return new ClusterMetadata(topics, topicNamesById, partitionsByTopicName);

        File[] logFiles = dir.listFiles((d, name) -> name.endsWith(".log"));
        if (logFiles == null) return new ClusterMetadata(topics, topicNamesById, partitionsByTopicName);

        for (File file : logFiles) {
            System.err.println("Parsing metadata log: " + file.getName());
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
                while (in.available() > 0) {
                    parseBatch(in, topics, topicNamesById, partitionsByTopicName);
                }
            }
            System.err.println("Finished parsing: " + file.getName()
                + " — found " + topics.size() + " topic(s)");
        }
        return new ClusterMetadata(topics, topicNamesById, partitionsByTopicName);
    }

    private static void parseBatch(DataInputStream in, Map<String, byte[]> topics,
                                   Map<String, String> topicNamesById,
                                   Map<String, List<PartitionInfo>> partitionsByTopicName) throws IOException {
        in.skipBytes(8);                   // base_offset
        in.readInt();                      // batch_length
        in.skipBytes(4);                   // partition_leader_epoch
        in.skipBytes(1);                   // magic
        in.skipBytes(4);                   // crc
        in.skipBytes(2);                   // attributes
        in.skipBytes(4);                   // last_offset_delta
        in.skipBytes(8);                   // base_timestamp
        in.skipBytes(8);                   // max_timestamp
        in.skipBytes(8);                   // producer_id
        in.skipBytes(2);                   // producer_epoch
        in.skipBytes(4);                   // base_sequence
        int recordsCount = in.readInt();   // records_count

        for (int i = 0; i < recordsCount; i++) {
            parseRecord(in, topics, topicNamesById, partitionsByTopicName);
        }
    }

    private static void parseRecord(DataInputStream in, Map<String, byte[]> topics,
                                    Map<String, String> topicNamesById,
                                    Map<String, List<PartitionInfo>> partitionsByTopicName) throws IOException {
        readZigzagVarint(in);              // record length (skip)
        in.skipBytes(1);                   // attributes
        readZigzagVarint(in);              // timestamp_delta (skip)
        readZigzagVarint(in);              // offset_delta (skip)
        int keyLength = readZigzagVarint(in);
        if (keyLength > 0) in.skipBytes(keyLength);
        int valueLength = readZigzagVarint(in);
        byte[] value = new byte[valueLength];
        in.readFully(value);
        readUnsignedVarint(in);            // headers_count (skip)

        parseValue(value, topics, topicNamesById, partitionsByTopicName);
    }

    private static void parseValue(byte[] value, Map<String, byte[]> topics,
                                   Map<String, String> topicNamesById,
                                   Map<String, List<PartitionInfo>> partitionsByTopicName) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(value));
        in.skipBytes(1);                   // frame_version
        int type = in.readByte();          // record type

        if (type == TOPIC_RECORD_TYPE) {
            in.skipBytes(1);               // version
            short nameLength = in.readShort();
            byte[] nameBytes = new byte[nameLength];
            in.readFully(nameBytes);
            String topicName = new String(nameBytes);

            byte[] topicId = new byte[16];
            in.readFully(topicId);

            topics.put(topicName, topicId);
            topicNamesById.put(new String(topicId, StandardCharsets.ISO_8859_1), topicName);

        } else if (type == PARTITION_RECORD_TYPE) {
            in.skipBytes(1);               // version
            int partitionId = in.readInt();

            byte[] topicId = new byte[16];
            in.readFully(topicId);

            List<Integer> replicas = readCompactArrayInt32(in);
            List<Integer> isr      = readCompactArrayInt32(in);
            skipCompactArrayInt32(in);     // removingReplicas
            skipCompactArrayInt32(in);     // addingReplicas
            int leaderId    = in.readInt();
            in.skipBytes(1);               // leaderRecoveryState
            int leaderEpoch = in.readInt();

            String topicName = topicNamesById.get(new String(topicId, StandardCharsets.ISO_8859_1));
            if (topicName != null) {
                partitionsByTopicName
                    .computeIfAbsent(topicName, k -> new ArrayList<>())
                    .add(new PartitionInfo(partitionId, leaderId, leaderEpoch, replicas, isr));
            }
        }
    }

    private static List<Integer> readCompactArrayInt32(DataInputStream in) throws IOException {
        int count = readUnsignedVarint(in) - 1;
        List<Integer> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(in.readInt());
        }
        return list;
    }

    private static void skipCompactArrayInt32(DataInputStream in) throws IOException {
        int count = readUnsignedVarint(in) - 1;
        if (count > 0) in.skipBytes(count * 4);
    }

    private static int readZigzagVarint(DataInputStream in) throws IOException {
        int raw = readUnsignedVarint(in);
        return (raw >>> 1) ^ -(raw & 1);
    }

    private static int readUnsignedVarint(DataInputStream in) throws IOException {
        int result = 0;
        int shift = 0;
        int b;
        do {
            b = in.readByte() & 0xFF;
            result |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }
}
