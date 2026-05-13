package com.simplekafka.common.responses;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class ApiVersionsResponse {
    private static final int BASE_MESSAGE_SIZE = 12;
    private final int correlationId;
    private final short errorCode;
    private final List<ApiVersionEntry> entries;

    public ApiVersionsResponse(int correlationId, short errorCode, List<ApiVersionEntry> entries) {
        this.correlationId = correlationId;
        this.errorCode = errorCode;
        this.entries = entries;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        if (errorCode != 0) {
            out.writeInt(6);
            out.writeInt(correlationId);
            out.writeShort(errorCode);
        } else {
            out.writeInt(BASE_MESSAGE_SIZE + (entries.size() * 7));
            out.writeInt(correlationId);
            out.writeShort(errorCode);
            out.writeByte(entries.size() + 1);
            for (ApiVersionEntry entry : entries) {
                entry.writeTo(out);
            }
            out.writeInt(0);
            out.writeByte(0);
        }
        out.flush();
    }
}
