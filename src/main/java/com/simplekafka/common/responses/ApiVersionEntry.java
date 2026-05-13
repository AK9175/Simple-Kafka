package com.simplekafka.common.responses;

import java.io.DataOutputStream;
import java.io.IOException;

public class ApiVersionEntry {
    final short apiKey;
    final short minVersion;
    final short maxVersion;

    public ApiVersionEntry(short apiKey, short minVersion, short maxVersion) {
        this.apiKey = apiKey;
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeShort(apiKey);
        out.writeShort(minVersion);
        out.writeShort(maxVersion);
        out.writeByte(0);
    }
}
