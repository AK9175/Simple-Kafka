package com.simplekafka.common.requests;

import java.io.DataInputStream;
import java.io.IOException;

public class RequestHeader {
    public final int messageSize;
    public final short apiKey;
    public final short apiVersion;
    public final int correlationId;

    private RequestHeader(int messageSize, short apiKey, short apiVersion, int correlationId) {
        this.messageSize = messageSize;
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
    }

    public static RequestHeader readFrom(DataInputStream in) throws IOException {
        int messageSize = in.readInt();
        short apiKey = in.readShort();
        short apiVersion = in.readShort();
        int correlationId = in.readInt();
        return new RequestHeader(messageSize, apiKey, apiVersion, correlationId);
    }
}
