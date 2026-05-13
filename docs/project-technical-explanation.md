# SimpleKafka — Project Technical Explanation

---

## Overview

This project implements a working Kafka broker from scratch in Java, speaking the real Kafka wire protocol on TCP port 9092. Real Kafka clients can connect to it and it responds correctly. The implementation covers four core APIs: `ApiVersions`, `Fetch`, `Produce`, and `DescribeTopicPartitions`.

---

## Project Structure

```
src/main/java/com/simplekafka/
  broker/
    network/
      SocketServer.java          ← TCP server, accepts connections
    server/
      Main.java                  ← Entry point, starts server
      KafkaApis.java             ← Request dispatcher and handlers
  common/
    log/
      ClusterMetadata.java       ← In-memory cluster state
      ClusterMetadataLog.java    ← Parses __cluster_metadata log
      PartitionInfo.java         ← Partition metadata bean
      PartitionLog.java          ← Reads/writes partition log files
    requests/
      RequestHeader.java
      DescribeTopicPartitionsRequest.java
      FetchRequest.java
      ProduceRequest.java
    responses/
      ApiVersionEntry.java
      ApiVersionsResponse.java
      DescribeTopicPartitionsResponse.java
      TopicMetadata.java
      FetchResponse.java
      FetchableTopicResponse.java
      PartitionData.java
      ProduceResponse.java
  tools/
    MetadataLogGenerator.java    ← Test data generator
```

---

## Startup Flow

```
Main.main()
  │
  ├── ClusterMetadataLog.load()
  │     reads /tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log
  │     parses TopicRecord + PartitionRecord entries
  │     returns ClusterMetadata (topics map, topicNamesById map, partitions map)
  │
  └── SocketServer.start()
        opens ServerSocket on port 9092
        for each connection → new thread → connection loop
          reads RequestHeader
          dispatches to KafkaApis.handle()
```

---

## TCP Server (SocketServer)

Uses a cached thread pool — one thread per client connection. Each thread runs a loop:

```java
while (true) {
    RequestHeader header = RequestHeader.readFrom(in);
    KafkaApis.handle(header, in, out, metadata);
}
```

The connection stays open until the client disconnects (persistent TCP connection, same as real Kafka).

---

## Kafka Wire Protocol Implementation

Every request and response follows this binary frame:

```
[4 bytes message_size] [N bytes payload]
```

Request payload:
```
api_key(2) + api_version(2) + correlation_id(4) + client_id(NULLABLE_STRING) + TAG_BUFFER + body
```

Response payload:
```
correlation_id(4) + TAG_BUFFER + body
```

### Encoding Details

**COMPACT_ARRAY** — element count encoded as `N+1` (unsigned varint). Value `1` means empty array, `2` means 1 element, etc. This allows `0` to represent null.

**VARINT (unsigned)** — variable-length encoding. Each byte uses 7 bits of data and 1 continuation bit. Numbers < 128 fit in 1 byte.

**ZIGZAG VARINT (signed)** — maps signed integers to unsigned: `(n << 1) ^ (n >> 31)`. Allows small negative numbers to be encoded compactly.

**NULLABLE_STRING** — INT16 length (-1 for null) followed by UTF-8 bytes. Used in request headers.

**COMPACT_STRING** — (length+1) as unsigned varint followed by UTF-8 bytes. Used in flexible API versions.

---

## RequestHeader

```java
int messageSize   // total bytes that follow
short apiKey      // which API (0=Produce, 1=Fetch, 18=ApiVersions, 75=Describe)
short apiVersion  // version of the API
int correlationId // echoed in response for client-side request matching
```

The `client_id` and `TAG_BUFFER` that follow in the actual wire format are NOT parsed by `RequestHeader` — each request handler is responsible for skipping them (they are consumed as part of the first `skipBytes()` call in each request parser).

---

## ClusterMetadata and Log Parsing

### ClusterMetadataLog
Parses the KRaft metadata log at `/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log`.

The file contains RecordBatches. Each record inside has a `frame_version` byte, a `type` byte, then type-specific fields:

**TopicRecord (type=2):**
```
frame_version(1) + type(1=2) + version(1) + topic_name_length(INT16) + topic_name + topic_uuid(16) + TAG_BUFFER
```

**PartitionRecord (type=1):**
```
frame_version(1) + type(1=1) + version(1) + partition_id(INT32) + topic_uuid(16)
+ replicas(COMPACT_ARRAY INT32) + isr(COMPACT_ARRAY INT32)
+ removingReplicas(COMPACT_ARRAY) + addingReplicas(COMPACT_ARRAY)
+ leader_id(INT32) + leaderRecoveryState(INT8) + leader_epoch(INT32) + partition_epoch(INT32)
+ TAG_BUFFER
```

### ClusterMetadata
Three maps built during log parsing:

```java
Map<String, byte[]> topics           // "foo" → UUID bytes
Map<String, String> topicNamesById   // ISO-8859-1 UUID string → "foo"
Map<String, List<PartitionInfo>> partitions  // "foo" → [PartitionInfo(0,...), PartitionInfo(1,...)]
```

`topicNamesById` uses ISO-8859-1 encoding of the raw 16 UUID bytes as the key — this allows byte-array UUIDs from Fetch requests to be looked up directly without hex conversion.

### PartitionInfo
```java
int partitionId
int leaderId
int leaderEpoch
List<Integer> replicas
List<Integer> isr
```

Used in `DescribeTopicPartitions` responses and `Produce` validation.

---

## API Implementations

### ApiVersions (key=18)

Skips the remainder of the request body (`messageSize - 8` bytes — the 8 already consumed are apiKey + apiVersion + correlationId).

Returns a list of `ApiVersionEntry` records, each with:
- `api_key` (INT16)
- `min_version` (INT16)
- `max_version` (INT16)
- `TAG_BUFFER` (1 byte)

Advertises support for:
- ApiVersions (18): v0–v4
- Fetch (1): v0–v16
- Produce (0): v0–v11
- DescribeTopicPartitions (75): v0–v0

If `apiVersion > 4`, returns error code `35` (UNSUPPORTED_VERSION) with an empty entries list.

---

### DescribeTopicPartitions (key=75)

**Request parsing:**
Reads a COMPACT_ARRAY of topic names (COMPACT_STRING each). Skips the cursor field (for pagination — not implemented).

**Handler logic:**
```java
for each topicName in request:
    if metadata.topics.containsKey(topicName):
        return TopicMetadata(name, errorCode=0, uuid, partitions)
    else:
        return TopicMetadata(name, errorCode=3, zeroUUID, emptyPartitions)
```

Error code `3` = `UNKNOWN_TOPIC_OR_PARTITION`.

**Response (per topic):**
```
error_code(INT16) + name_length(varint) + name + topic_uuid(16) + is_internal(INT8)
+ partitions COMPACT_ARRAY:
    error_code(INT16) + partition_index(INT32) + leader_id(INT32) + leader_epoch(INT32)
    + replica_nodes COMPACT_ARRAY(INT32) + isr_nodes COMPACT_ARRAY(INT32)
    + eligible_leader_replicas(empty) + last_known_elr(empty) + offline_replicas(empty)
    + TAG_BUFFER
+ topic_authorized_operations(INT32) + TAG_BUFFER
```

---

### Fetch (key=1)

**Request parsing (v16):**
Skips: `client_id(2) + headerTag(1) + max_wait(4) + min_bytes(4) + max_bytes(4) + isolation(1) + session_id(4) + session_epoch(4)` = 24 bytes.

Then reads:
- Topics COMPACT_ARRAY: each topic has a `topicId` (16 bytes UUID)
- Partitions COMPACT_ARRAY: each partition has `partitionIndex(INT32)` + `currentLeaderEpoch(INT32, skipped)` + `fetchOffset(INT64)` + other fields

**UUID to topic name resolution:**
```java
String topicName = metadata.topicNamesById.get(
    new String(topic.topicId, StandardCharsets.ISO_8859_1));
```

Uses ISO-8859-1 encoding to convert the raw UUID bytes to a string key for the map lookup.

**Disk read (PartitionLog.readBatch):**
Scans log files in sorted order. For each RecordBatch, reads `last_offset_delta` from byte position 11 of the batch body (after epoch+magic+crc+attributes = 4+1+4+2 = 11 bytes).

Includes a batch if: `baseOffset + lastOffsetDelta >= fetchOffset`

This returns ALL batches from `fetchOffset` onward, concatenated — supporting multi-message responses.

**Response (per partition):**
```
partition_index(INT32) + error_code(INT16)
+ high_watermark(INT64) + last_stable_offset(INT64) + log_start_offset(INT64)
+ aborted_transactions(empty COMPACT_ARRAY) + preferred_read_replica(INT32=-1)
+ records COMPACT_NULLABLE_BYTES (varint length N+1, then N bytes of batch data)
+ TAG_BUFFER
```

Response size is computed dynamically using `getSerializedSize()` methods — necessary because records bytes vary in length.

---

### Produce (key=0)

**Request parsing (v11):**
Skips: `client_id(2) + headerTag(1)` = 3 bytes.

Then reads:
- `transactional_id` (COMPACT_NULLABLE_STRING — null = varint 0)
- `acks` (INT16, skipped)
- `timeout_ms` (INT32, skipped)
- Topics COMPACT_ARRAY:
  - `name` (COMPACT_STRING)
  - Partitions COMPACT_ARRAY:
    - `partition_index` (INT32)
    - `partition_leader_epoch` (INT32, skipped — added in v10)
    - `records` (COMPACT_NULLABLE_BYTES) — **captured as raw bytes**

Uses a proper multi-byte `readUnsignedVarint()` for record lengths — essential because production batches easily exceed 127 bytes.

**Handler logic:**
```java
for each topic in request:
    for each partition in topic:
        valid = topic exists in metadata AND partition index exists in metadata
        if not valid:
            result = PartitionResult(errorCode=3, baseOffset=-1)
        else:
            baseOffset = PartitionLog.writeBatch(topicName, partitionIndex, records)
            result = PartitionResult(errorCode=0, baseOffset)
```

**Disk write (PartitionLog.writeBatch):**
```java
long baseOffset = nextOffsets.getOrDefault(key, 0L);
ByteBuffer.wrap(records).putLong(0, baseOffset);   // overwrite base_offset
int lastOffsetDelta = ByteBuffer.wrap(records, 23, 4).getInt();
nextOffsets.put(key, baseOffset + lastOffsetDelta + 1);
new FileOutputStream(path, true).write(records);   // append=true
return baseOffset;
```

Key points:
- `nextOffsets` is a static in-memory map tracking the next available offset per partition
- The producer sends `base_offset=0` (relative). The broker overwrites the first 8 bytes with the actual assigned offset
- `lastOffsetDelta` at byte 23 tells us how many offsets this batch covers — the next batch starts at `baseOffset + lastOffsetDelta + 1`
- `FileOutputStream(path, true)` — `true` means append mode, preserving existing log content

**Response (per partition):**
```
partition_index(INT32) + error_code(INT16) + base_offset(INT64)
+ log_append_time_ms(INT64=-1) + log_start_offset(INT64=-1)
+ record_errors(empty COMPACT_ARRAY) + error_message(null) + TAG_BUFFER
```

---

## RecordBatch — The Universal Format

RecordBatch is the only format used for storing and transferring messages — even single messages use this format with `records_count=1`.

```
base_offset(8)       ← offset of first record; broker overwrites this on Produce
batch_length(4)      ← byte count of everything below
partition_epoch(4)
magic(1)             ← always 2
crc32c(4)            ← checksum of everything below this field
attributes(2)        ← compression codec, timestamp type
last_offset_delta(4) ← num_records - 1; broker uses to assign offset range
base_timestamp(8)
max_timestamp(8)
producer_id(8)       ← -1 if not transactional
producer_epoch(2)    ← -1 if not transactional
base_sequence(4)     ← -1 if not idempotent
records_count(4)
records[]
  length (zigzag varint)
  attributes(1)
  timestamp_delta (zigzag varint)
  offset_delta (zigzag varint)   ← actual offset = base_offset + offset_delta
  key_length (zigzag varint)     ← -1 for null
  key (bytes)
  value_length (zigzag varint)
  value (bytes)
  headers_count (unsigned varint)
```

CRC32C is computed over everything from `attributes` onward. The broker can validate this on Produce to detect corrupt batches.

The broker treats the batch as an **opaque byte array** — it never deserializes individual records. On Produce it just overwrites `base_offset` and appends. On Fetch it reads raw bytes and returns them. This is how real Kafka works too.

---

## MetadataLogGenerator

A developer tool that pre-populates the test log files. It generates:
- `__cluster_metadata-0/00000000000000000000.log` — one RecordBatch containing:
  - TopicRecord for "foo" with UUID `deadbeefdeadbeefdeadbeefdeadbeef`
  - PartitionRecord for partition 0 (leader=1, replicas=[1], isr=[1])
  - PartitionRecord for partition 1 (leader=1, replicas=[1], isr=[1])
- `foo-0/00000000000000000000.log` — 3 batches at offsets 0, 1, 2
- `foo-1/00000000000000000000.log` — 1 batch at offset 0

Generates RecordBatches correctly from scratch: builds records → wraps in batch → computes CRC32C → writes header. The CRC32C is computed using Java's built-in `java.util.zip.CRC32C` class.

Multiple messages to the same partition use `append=true` on FileOutputStream, tracked via a static `nextOffset` map.

---

## Key Design Decisions

**Opaque batch passthrough** — The broker never deserializes records inside batches. Produce bytes flow straight to disk; Fetch bytes flow straight from disk to the network. This is both simpler and matches how real Kafka works.

**ISO-8859-1 UUID keys** — Kafka UUIDs are 16 raw bytes. To use them as map keys, they are converted to strings using ISO-8859-1 (1 byte per char, lossless). This avoids hex conversion overhead and keeps the map lookup O(1).

**Dynamic response sizing** — FetchResponse and ProduceResponse sizes vary based on record content. `getSerializedSize()` methods compute the exact byte count before writing, allowing the correct `message_size` frame to be written first.

**Static offset tracking** — `PartitionLog.nextOffsets` is a static map. On broker restart, it resets to 0 — meaning produced messages may collide with existing offsets in pre-existing log files. For a dev/test environment this is acceptable since logs are regenerated fresh. A production broker would scan existing log files at startup to initialize offsets.

**Per-request skipBytes pattern** — RequestHeader only reads `messageSize + apiKey + apiVersion + correlationId`. Each request parser then skips `client_id + headerTag` and any other fields it doesn't need. This avoids coupling the header parser to the request body format.

---

## Testing Approach

All testing was done with raw TCP connections using `nc` (netcat):

```bash
echo -n '<hex bytes>' | xxd -r -p | nc -w 2 localhost 9092 | hexdump -C
```

Request bytes were constructed manually using Python's `struct` module:
- `struct.pack('>HHI', api_key, api_version, correlation_id)` for headers
- RecordBatch bytes constructed with correct CRC32C (computed via lookup table)
- Zigzag and unsigned varint encoding for record fields

Responses were validated by parsing the hexdump output field by field against the Kafka protocol specification.

---

## What Is Not Implemented

- **CreateTopics** — topics are pre-populated in the metadata log by MetadataLogGenerator
- **Consumer group protocol** — JoinGroup, SyncGroup, Heartbeat, OffsetCommit, OffsetFetch
- **Replication** — inter-broker data replication, ISR management
- **KRaft consensus** — Raft leader election, log replication across controller nodes
- **Metadata API** — broker discovery for multi-broker setups
- **Log segments** — only one segment file per partition, no rolling
- **Index files** — no .index or .timeindex files, linear scan only
- **Authentication/TLS** — no SASL or SSL support
- **Transactions** — no exactly-once semantics
- **Compression** — batches are always uncompressed
- **Startup offset recovery** — nextOffsets map initializes at 0, not from existing log
