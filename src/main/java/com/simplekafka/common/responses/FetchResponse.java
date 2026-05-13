package com.simplekafka.common.responses;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class FetchResponse {
    private final int correlationId;
    private final byte headerTaggedFields;
    private final int throttleTime;
    private final short errorCode;
    private final int sessionId;
    private final List<FetchableTopicResponse> responses;
    private final byte taggedFields;

    public FetchResponse(int correlationId, byte headerTaggedFields, int throttleTime, short errorCode, int sessionId, List<FetchableTopicResponse> responses, byte taggedFields) {
        this.correlationId = correlationId;
        this.headerTaggedFields = headerTaggedFields;
        this.throttleTime = throttleTime;
        this.errorCode = errorCode;
        this.sessionId = sessionId;
        this.responses = responses;
        this.taggedFields = taggedFields;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        int messageSize = 17;
        for (FetchableTopicResponse r : responses) messageSize += r.getSerializedSize();
        out.writeInt(messageSize);
        out.writeInt(correlationId);
        out.writeByte(headerTaggedFields);
        out.writeInt(throttleTime);
        out.writeShort(errorCode);
        out.writeInt(sessionId);
        out.writeByte(responses.size() + 1);
        for (FetchableTopicResponse response : responses) {
            response.writeTo(out);
        }
        out.writeByte(taggedFields);
        out.flush();
    }
}
