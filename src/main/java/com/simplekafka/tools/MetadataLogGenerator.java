package com.simplekafka.tools;

import java.io.*;
import java.util.zip.CRC32C;

/**
 * Generates a sample Kafka cluster metadata log file containing one TopicRecord and one PartitionRecord.
 * Run this to create /tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log
 */
public class MetadataLogGenerator {

    public static void main(String[] args) throws IOException {
        generateMetadataLog();
        generatePartitionLog("foo", 0, "hello from kafka");
        generatePartitionLog("foo", 0, "second message");
        generatePartitionLog("foo", 0, "third message");
        generatePartitionLog("foo", 1, "hello from partition 1");
    }

    public static void generateMetadataLog() throws IOException {
        String topicName = "foo";
        byte[] topicId = new byte[]{
            (byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef,
            (byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef,
            (byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef,
            (byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef
        };

        byte[] fileBytes = buildBatch(topicName, topicId);

        String path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
        new File(path).getParentFile().mkdirs();
        try (FileOutputStream fos = new FileOutputStream(path)) {
            fos.write(fileBytes);
        }

        System.out.println("Generated: " + path);
        System.out.println("Topic:     " + topicName);
        System.out.println("Topic ID:  deadbeefdeadbeefdeadbeefdeadbeef");
        System.out.println("Partitions: 0, 1");
    }

    // tracks next baseOffset per "topicName-partition" so appended batches have correct offsets
    private static final java.util.Map<String, Long> nextOffset = new java.util.HashMap<>();

    public static void generatePartitionLog(String topicName, int partition, String message) throws IOException {
        String key = topicName + "-" + partition;
        long baseOffset = nextOffset.getOrDefault(key, 0L);
        nextOffset.put(key, baseOffset + 1);

        byte[] valueBytes = message.getBytes();

        ByteArrayOutputStream bodyOut = new ByteArrayOutputStream();
        DataOutputStream body = new DataOutputStream(bodyOut);
        body.writeByte(0);
        writeZigzagVarint(body, 0);               // timestamp_delta
        writeZigzagVarint(body, 0);               // offset_delta
        writeZigzagVarint(body, -1);              // key = null
        writeZigzagVarint(body, valueBytes.length);
        body.write(valueBytes);
        writeUnsignedVarint(body, 0);             // headers_count
        byte[] bodyBytes = bodyOut.toByteArray();

        ByteArrayOutputStream recOut = new ByteArrayOutputStream();
        DataOutputStream rec = new DataOutputStream(recOut);
        writeZigzagVarint(rec, bodyBytes.length);
        rec.write(bodyBytes);
        byte[] recordBytes = recOut.toByteArray();

        ByteArrayOutputStream acOut = new ByteArrayOutputStream();
        DataOutputStream ac = new DataOutputStream(acOut);
        ac.writeShort(0);    // attributes
        ac.writeInt(0);      // last_offset_delta
        ac.writeLong(0);     // base_timestamp
        ac.writeLong(0);     // max_timestamp
        ac.writeLong(-1);    // producer_id
        ac.writeShort(-1);   // producer_epoch
        ac.writeInt(-1);     // base_sequence
        ac.writeInt(1);      // records_count
        ac.write(recordBytes);
        byte[] afterCrc = acOut.toByteArray();

        CRC32C crc32c = new CRC32C();
        crc32c.update(afterCrc);
        int crc = (int) crc32c.getValue();

        ByteArrayOutputStream hdrOut = new ByteArrayOutputStream();
        DataOutputStream hdr = new DataOutputStream(hdrOut);
        hdr.writeInt(0);     // partition_leader_epoch
        hdr.writeByte(2);    // magic
        hdr.writeInt(crc);
        hdr.write(afterCrc);
        byte[] batchHeader = hdrOut.toByteArray();

        ByteArrayOutputStream fileOut = new ByteArrayOutputStream();
        DataOutputStream file = new DataOutputStream(fileOut);
        file.writeLong(baseOffset);             // base_offset (increments per message)
        file.writeInt(batchHeader.length);      // batch_length
        file.write(batchHeader);

        String path = "/tmp/kraft-combined-logs/" + topicName + "-" + partition + "/00000000000000000000.log";
        new File(path).getParentFile().mkdirs();
        try (FileOutputStream fos = new FileOutputStream(path, baseOffset > 0)) {  // append if not first
            fos.write(fileOut.toByteArray());
        }
        System.out.println("Generated: " + path + "  offset=" + baseOffset + "  message='" + message + "'");
    }

    static byte[] buildBatch(String topicName, byte[] topicId) throws IOException {
        byte[] topicValue  = buildTopicRecordValue(topicName, topicId);
        byte[] topicRecord = buildRecord(topicValue, 0);
        byte[] part0Value  = buildPartitionRecordValue(0, topicId);
        byte[] part0Record = buildRecord(part0Value, 1);
        byte[] part1Value  = buildPartitionRecordValue(1, topicId);
        byte[] part1Record = buildRecord(part1Value, 2);

        ByteArrayOutputStream records = new ByteArrayOutputStream();
        records.write(topicRecord);
        records.write(part0Record);
        records.write(part1Record);
        byte[] allRecords = records.toByteArray();

        byte[] afterCrc    = buildAfterCrc(allRecords, 3);
        byte[] batchHeader = buildBatchHeader(afterCrc);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeLong(0);                    // base_offset
        dos.writeInt(batchHeader.length);    // batch_length
        dos.write(batchHeader);
        return out.toByteArray();
    }

    private static byte[] buildTopicRecordValue(String topicName, byte[] topicId) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeByte(1);                          // frame_version
        dos.writeByte(2);                          // type = TopicRecord
        dos.writeByte(0);                          // version
        dos.writeShort(topicName.length());        // topic_name length (INT16)
        dos.writeBytes(topicName);                 // topic_name bytes
        dos.write(topicId);                        // topic_id UUID (16 bytes)
        dos.writeByte(0);                          // tagged_fields
        return out.toByteArray();
    }

    private static byte[] buildPartitionRecordValue(int partitionId, byte[] topicId) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeByte(1);                          // frame_version
        dos.writeByte(1);                          // type = PartitionRecord
        dos.writeByte(0);                          // version
        dos.writeInt(partitionId);                 // partitionId (INT32)
        dos.write(topicId);                        // topicId UUID (16 bytes)
        writeUnsignedVarint(dos, 2);               // replicas: COMPACT_ARRAY, 1 element
        dos.writeInt(1);                           //   replica node id = 1
        writeUnsignedVarint(dos, 2);               // isr: COMPACT_ARRAY, 1 element
        dos.writeInt(1);                           //   isr node id = 1
        writeUnsignedVarint(dos, 1);               // removingReplicas: empty COMPACT_ARRAY
        writeUnsignedVarint(dos, 1);               // addingReplicas: empty COMPACT_ARRAY
        dos.writeInt(1);                           // leader = 1
        dos.writeByte(0);                          // leaderRecoveryState
        dos.writeInt(0);                           // leaderEpoch
        dos.writeInt(0);                           // partitionEpoch
        dos.writeByte(0);                          // tagged_fields
        return out.toByteArray();
    }

    private static byte[] buildRecord(byte[] valueBytes, int offsetDelta) throws IOException {
        ByteArrayOutputStream bodyOut = new ByteArrayOutputStream();
        DataOutputStream body = new DataOutputStream(bodyOut);
        body.writeByte(0);                         // attributes
        writeZigzagVarint(body, 0);                // timestamp_delta
        writeZigzagVarint(body, offsetDelta);      // offset_delta
        writeZigzagVarint(body, -1);               // key_length (null)
        writeZigzagVarint(body, valueBytes.length);// value_length
        body.write(valueBytes);
        writeUnsignedVarint(body, 0);              // headers_count
        byte[] bodyBytes = bodyOut.toByteArray();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        writeZigzagVarint(dos, bodyBytes.length);  // record length
        dos.write(bodyBytes);
        return out.toByteArray();
    }

    private static byte[] buildAfterCrc(byte[] recordBytes, int recordsCount) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeShort(0);                         // attributes
        dos.writeInt(recordsCount - 1);            // last_offset_delta
        dos.writeLong(0);                          // base_timestamp
        dos.writeLong(0);                          // max_timestamp
        dos.writeLong(-1);                         // producer_id
        dos.writeShort(-1);                        // producer_epoch
        dos.writeInt(-1);                          // base_sequence
        dos.writeInt(recordsCount);                // records_count
        dos.write(recordBytes);
        return out.toByteArray();
    }

    private static byte[] buildBatchHeader(byte[] afterCrc) throws IOException {
        CRC32C crc32c = new CRC32C();
        crc32c.update(afterCrc);
        int crc = (int) crc32c.getValue();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeInt(0);                           // partition_leader_epoch
        dos.writeByte(2);                          // magic
        dos.writeInt(crc);                         // crc32c
        dos.write(afterCrc);
        return out.toByteArray();
    }

    static void writeZigzagVarint(DataOutputStream out, int n) throws IOException {
        writeUnsignedVarint(out, (n << 1) ^ (n >> 31));
    }

    static void writeUnsignedVarint(DataOutputStream out, int n) throws IOException {
        while ((n & 0xFFFFFF80) != 0) {
            out.writeByte((n & 0x7F) | 0x80);
            n >>>= 7;
        }
        out.writeByte(n & 0x7F);
    }
}
