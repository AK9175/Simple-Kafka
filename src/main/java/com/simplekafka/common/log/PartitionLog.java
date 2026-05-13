package com.simplekafka.common.log;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PartitionLog {
    private static final String LOG_BASE_DIR = "/tmp/kraft-combined-logs";
    private static final Map<String, Long> nextOffsets = new HashMap<>();

    public static long writeBatch(String topicName, int partitionIndex, byte[] records) throws IOException {
        String key = topicName + "-" + partitionIndex;
        long baseOffset = nextOffsets.getOrDefault(key, 0L);

        // overwrite the base_offset (first 8 bytes) with the assigned log offset
        ByteBuffer.wrap(records).putLong(0, baseOffset);

        // read last_offset_delta to know how many offsets this batch consumes
        // layout: base_offset(8) + batch_length(4) + epoch(4) + magic(1) + crc(4) + attributes(2) = 23 bytes before last_offset_delta
        int lastOffsetDelta = ByteBuffer.wrap(records, 23, 4).getInt();
        nextOffsets.put(key, baseOffset + lastOffsetDelta + 1);

        String path = LOG_BASE_DIR + "/" + topicName + "-" + partitionIndex + "/00000000000000000000.log";
        new File(path).getParentFile().mkdirs();
        try (FileOutputStream fos = new FileOutputStream(path, true)) {
            fos.write(records);
        }
        return baseOffset;
    }

    public static byte[] readBatch(String topicName, int partitionIndex, long fetchOffset) throws IOException {
        File dir = new File(LOG_BASE_DIR + "/" + topicName + "-" + partitionIndex);
        if (!dir.exists()) return new byte[0];

        File[] logFiles = dir.listFiles((d, name) -> name.endsWith(".log"));
        if (logFiles == null || logFiles.length == 0) return new byte[0];
        Arrays.sort(logFiles);

        ByteArrayOutputStream result = new ByteArrayOutputStream();
        for (File logFile : logFiles) {
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(logFile)))) {
                while (in.available() > 0) {
                    long baseOffset = in.readLong();
                    int batchLength = in.readInt();
                    byte[] batchBody = new byte[batchLength];
                    in.readFully(batchBody);

                    // epoch(4) + magic(1) + crc(4) + attributes(2) = 11 bytes before last_offset_delta
                    DataInputStream body = new DataInputStream(new ByteArrayInputStream(batchBody));
                    body.skipBytes(4 + 1 + 4 + 2);
                    int lastOffsetDelta = body.readInt();

                    // include this batch if it contains or comes after fetchOffset
                    if (baseOffset + lastOffsetDelta >= fetchOffset) {
                        DataOutputStream dos = new DataOutputStream(result);
                        dos.writeLong(baseOffset);
                        dos.writeInt(batchLength);
                        dos.write(batchBody);
                    }
                }
            }
        }
        return result.toByteArray();
    }
}
