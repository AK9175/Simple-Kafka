package com.simplekafka.common.responses;

import java.io.DataOutputStream;
import java.io.IOException;

public class PartitionData {
    private final int partitionIndex;
    private final short errorCode;
    private final byte[] records;

    public PartitionData(int partitionIndex, short errorCode, byte[] records) {
        this.partitionIndex = partitionIndex;
        this.errorCode = errorCode;
        this.records = records;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(partitionIndex);
        out.writeShort(errorCode);
        out.writeLong(0);   // high_watermark
        out.writeLong(0);   // last_stable_offset
        out.writeLong(0);   // log_start_offset
        out.writeByte(0);   // aborted_transactions: empty COMPACT_ARRAY
        out.writeInt(-1);   // preferred_read_replica

        if (records.length == 0) {
            out.writeByte(0);
        } else {
            writeUnsignedVarint(out, records.length + 1);
            out.write(records);
        }

        out.writeByte(0);   // tagged_fields
    }

    public int getSerializedSize() {
        int recordsFieldSize = records.length == 0
                ? 1
                : varintByteCount(records.length + 1) + records.length;
        return 36 + recordsFieldSize;
    }

    private static void writeUnsignedVarint(DataOutputStream out, int n) throws IOException {
        while ((n & 0xFFFFFF80) != 0) {
            out.writeByte((n & 0x7F) | 0x80);
            n >>>= 7;
        }
        out.writeByte(n & 0x7F);
    }

    private static int varintByteCount(int n) {
        int count = 1;
        while ((n & 0xFFFFFF80) != 0) { n >>>= 7; count++; }
        return count;
    }
}
