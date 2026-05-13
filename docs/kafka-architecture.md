# Apache Kafka — Complete Technical Reference for Interviews

---

## Table of Contents
1. What is Kafka and Why Does It Exist
2. Core Concepts Deep Dive
3. Physical Storage Architecture
4. RecordBatch Wire Format
5. Producer Internals
6. Consumer Internals
7. Replication Protocol
8. KRaft Consensus
9. Kafka Wire Protocol
10. Controller and Broker Lifecycle
11. Consumer Groups and Rebalancing
12. Exactly-Once Semantics
13. Performance Architecture
14. Log Compaction
15. Common Interview Questions with Answers

---

## 1. What is Kafka and Why Does It Exist

### The Problem Before Kafka

Before event streaming platforms, systems communicated via point-to-point integrations:

```
                  ┌─────────────────────────────────────────────┐
                  │           BEFORE KAFKA                      │
                  │                                             │
  ┌──────────┐    │  ┌──────────┐    ┌──────────┐              │
  │ Service A│────┼──► Service B│    │ Service C│              │
  └──────────┘    │  └────┬─────┘    └─────▲────┘              │
                  │       │                │                    │
  ┌──────────┐    │       └────────────────┘                    │
  │ Service D│────┼──────────────────────────► Service E        │
  └──────────┘    │  Point-to-point = N² connections            │
                  └─────────────────────────────────────────────┘

  Problems:
  - Each producer knows about each consumer (tight coupling)
  - No replay — if consumer is down, messages are lost
  - No backpressure control
  - Schema changes require coordinating all services
```

### Kafka's Solution: Decoupled Log

```
                  ┌──────────────────────────────────────────────────┐
                  │                  WITH KAFKA                      │
                  │                                                  │
  ┌──────────┐    │  ┌─────────────────────────────────┐            │
  │ Service A│────┼──►                                 ├──► Consumer1│
  └──────────┘    │  │         KAFKA BROKER            │            │
                  │  │                                 ├──► Consumer2│
  ┌──────────┐    │  │  topic: orders                  │            │
  │ Service B│────┼──►  [0][1][2][3][4][5]...          ├──► Consumer3│
  └──────────┘    │  │                                 │            │
                  │  │  topic: payments                │            │
  ┌──────────┐    │  │  [0][1][2][3]...                │            │
  │ Service C│────┼──►                                 │            │
  └──────────┘    │  └─────────────────────────────────┘            │
                  │                                                  │
                  │  Producers don't know about consumers            │
                  │  Consumers can replay from any offset            │
                  │  Multiple consumers get same data independently  │
                  └──────────────────────────────────────────────────┘
```

### Core Design Principles
- **Append-only log** — immutable, ordered, persistent
- **Pull-based consumption** — consumers control their pace
- **Retention-based** — data stays for a configured period regardless of consumption
- **Distributed** — partitioned across brokers for scale
- **Replicated** — copies across brokers for fault tolerance

---

## 2. Core Concepts Deep Dive

### Topics and Partitions

```
  Topic: "orders"   num_partitions=3   replication_factor=2

  ┌─────────────────────────────────────────────────────────────┐
  │ Partition 0                                                 │
  │  offset: [0]─►[1]─►[2]─►[3]─►[4]─►[5]─►[6]  (append only)│
  └─────────────────────────────────────────────────────────────┘
  ┌─────────────────────────────────────────────────────────────┐
  │ Partition 1                                                 │
  │  offset: [0]─►[1]─►[2]─►[3]                                │
  └─────────────────────────────────────────────────────────────┘
  ┌─────────────────────────────────────────────────────────────┐
  │ Partition 2                                                 │
  │  offset: [0]─►[1]─►[2]─►[3]─►[4]─►[5]                     │
  └─────────────────────────────────────────────────────────────┘

  KEY POINTS:
  - Messages within a partition are STRICTLY ORDERED
  - No ordering guarantee ACROSS partitions
  - Each partition is an independent ordered log
  - Partitions are the unit of parallelism
```

### Offset — The Core Primitive

```
  Partition 0 of topic "orders":

  offset:   0          1          2          3          4
          ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐
          │order │  │order │  │order │  │order │  │order │
          │#1001 │  │#1002 │  │#1003 │  │#1004 │  │#1005 │
          └──────┘  └──────┘  └──────┘  └──────┘  └──────┘

  Consumer A committed offset 2 → will fetch from offset 3 next restart
  Consumer B committed offset 4 → fully caught up
  Consumer C at offset 0       → reading from beginning (replay)

  PROPERTIES:
  - Monotonically increasing, never resets
  - Unique within a partition (not globally)
  - Offset 5 in partition 0 ≠ Offset 5 in partition 1
  - Consumers track their own offsets independently
```

### Broker, Leader, Follower

```
  3-broker cluster, topic "orders" with 3 partitions, replication=2

  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
  │    BROKER 1     │    │    BROKER 2     │    │    BROKER 3     │
  │                 │    │                 │    │                 │
  │ P0 ★ LEADER    │    │ P0   follower   │    │                 │
  │                 │    │                 │    │                 │
  │ P1   follower   │    │ P1 ★ LEADER    │    │ P1   follower   │
  │                 │    │                 │    │                 │
  │                 │    │ P2   follower   │    │ P2 ★ LEADER    │
  └─────────────────┘    └─────────────────┘    └─────────────────┘

  ★ = Leader for that partition (handles ALL reads and writes)
  - = Follower (replicates from leader, ready for failover)

  EACH BROKER:
  - Is leader for some partitions
  - Is follower for others
  - Load is spread across the cluster
```

### ISR — In-Sync Replicas

```
  Partition 0: Leader=Broker1, Replicas=[1,2,3], ISR=[1,2,3]

  ┌──────────┐  replicate  ┌──────────┐
  │ Broker 1 │────────────►│ Broker 2 │  LEO=offset 100  ← in ISR
  │ (Leader) │             └──────────┘
  │ LEO=100  │  replicate  ┌──────────┐
  └──────────┘────────────►│ Broker 3 │  LEO=offset 85   ← BEHIND!
                            └──────────┘

  If Broker 3 is more than replica.lag.time.max.ms (default 30s) behind:
  → Removed from ISR
  → ISR becomes [1, 2]
  → acks=all now only requires Broker 1 + Broker 2

  High Watermark (HW) = min(LEO across ISR members)
  HW = min(100, 100) = 100  (Broker 3 not in ISR so excluded)
  Consumers can only read up to HW
```

---

## 3. Physical Storage Architecture

### Directory Layout

```
/kafka-logs/
  ├── __cluster_metadata-0/          ← KRaft metadata log
  │   ├── 00000000000000000000.log
  │   ├── 00000000000000000000.index
  │   └── 00000000000000000000.timeindex
  │
  ├── orders-0/                      ← topic "orders", partition 0
  │   ├── 00000000000000000000.log   ← segment 1: offsets 0 to ~999999
  │   ├── 00000000000000000000.index ← offset→byte position index
  │   ├── 00000000000000000000.timeindex
  │   ├── 00000000001000000000.log   ← segment 2: starts at offset 1000000
  │   ├── 00000000001000000000.index
  │   └── 00000000001000000000.timeindex
  │
  ├── orders-1/
  └── orders-2/
```

### Log Segments

```
  Partition directory: orders-0/

  ┌──────────────────────────────────────────────────────────┐
  │ 00000000000000000000.log  (segment 1, base offset = 0)   │
  │                                                          │
  │ [RecordBatch offset=0][RecordBatch offset=5]...          │
  │  → 1GB or 7 days old → ROLLED to new segment            │
  └──────────────────────────────────────────────────────────┘
  ┌──────────────────────────────────────────────────────────┐
  │ 00000000001000000000.log  (segment 2, base offset=1M)    │
  │                                                          │
  │ [RecordBatch offset=1000000][RecordBatch offset=1000003] │
  └──────────────────────────────────────────────────────────┘
  ┌──────────────────────────────────────────────────────────┐
  │ 00000000002000000000.log  (segment 3, ACTIVE - current)  │
  └──────────────────────────────────────────────────────────┘

  To find offset 1500000:
  1. Binary search filenames: 0, 1000000, 2000000 → segment 2
  2. Binary search .index file → byte position 48392
  3. Seek to that position, scan forward to offset 1500000
```

### Index File Format

```
  00000000000000000000.index
  ┌─────────────────────────────────────────────┐
  │  relative_offset(4) │ file_position(4)      │
  ├─────────────────────────────────────────────┤
  │       0             │       0               │ ← first entry
  │     100             │    4832               │ ← offset 100 is at byte 4832
  │     200             │    9741               │
  │     300             │   14205               │
  │      ...            │     ...               │
  └─────────────────────────────────────────────┘

  "Sparse" = not every offset, every Nth entry (log.index.interval.bytes default 4096)
  relative_offset = actual_offset - base_offset (saves 4 bytes per entry)

  Lookup offset 250:
  1. Binary search index: 200 ≤ 250 < 300, so start at byte position 9741
  2. Scan .log file forward from byte 9741 until offset 250
```

### TimeIndex File

```
  00000000000000000000.timeindex
  ┌──────────────────────────────────────────────────────┐
  │  timestamp(8)              │ offset(8)               │
  ├──────────────────────────────────────────────────────┤
  │  1700000000000             │    0                    │
  │  1700000300000             │  500                    │ ← 5 minutes later
  │  1700000600000             │ 1100                    │
  └──────────────────────────────────────────────────────┘

  Used by ListOffsets API: "give me offset for timestamp T"
  → Binary search timeindex → approximate offset → scan log
```

---

## 4. RecordBatch Wire Format (Magic v2)

This is the ONLY format used for all messages in Kafka — on disk and on the wire.

```
RecordBatch:
┌─────────────────────────────────────────────────────────────────┐
│ base_offset          (INT64, 8 bytes)                           │
│   → offset of the FIRST record in this batch                    │
│   → producer sends 0; broker overwrites with actual log offset  │
├─────────────────────────────────────────────────────────────────┤
│ batch_length         (INT32, 4 bytes)                           │
│   → byte length of everything BELOW this field                  │
├─────────────────────────────────────────────────────────────────┤
│ partition_leader_epoch (INT32, 4 bytes)                         │
│   → controller epoch when this batch was written                │
├─────────────────────────────────────────────────────────────────┤
│ magic                (INT8, 1 byte) = 2                         │
│   → format version; 0,1 are old pre-2.0 formats                 │
├─────────────────────────────────────────────────────────────────┤
│ crc                  (UINT32, 4 bytes)                          │
│   → CRC32C of EVERYTHING below this field                       │
│   → CRC32C (Castagnoli) not CRC32 (different polynomial)        │
├═════════════════════════════════════════════════════════════════╡
│ attributes           (INT16, 2 bytes)                           │
│   bits 0-2: compression (0=none,1=gzip,2=snappy,3=lz4,4=zstd)  │
│   bit    3: timestamp type (0=create,1=log_append)              │
│   bit    4: is_transactional                                    │
│   bit    5: is_control_batch                                    │
│   bit    6: has_delete_horizon_ms                               │
├─────────────────────────────────────────────────────────────────┤
│ last_offset_delta    (INT32, 4 bytes)                           │
│   → (num_records - 1)                                           │
│   → broker uses: next_batch_base_offset = base + delta + 1      │
├─────────────────────────────────────────────────────────────────┤
│ base_timestamp       (INT64, 8 bytes)                           │
├─────────────────────────────────────────────────────────────────┤
│ max_timestamp        (INT64, 8 bytes)                           │
├─────────────────────────────────────────────────────────────────┤
│ producer_id          (INT64, 8 bytes) = -1 if not transactional │
├─────────────────────────────────────────────────────────────────┤
│ producer_epoch       (INT16, 2 bytes) = -1 if not transactional │
├─────────────────────────────────────────────────────────────────┤
│ base_sequence        (INT32, 4 bytes) = -1 if not idempotent    │
├─────────────────────────────────────────────────────────────────┤
│ records_count        (INT32, 4 bytes)                           │
├─────────────────────────────────────────────────────────────────┤
│ records[]                                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ length          (ZIGZAG VARINT) ← signed, variable len  │   │
│  │ attributes      (INT8) = 0                              │   │
│  │ timestamp_delta (ZIGZAG VARINT) ← delta from base_ts    │   │
│  │ offset_delta    (ZIGZAG VARINT) ← 0,1,2,... per record  │   │
│  │ key_length      (ZIGZAG VARINT) ← -1 for null key       │   │
│  │ key             (BYTES)                                  │   │
│  │ value_length    (ZIGZAG VARINT)                          │   │
│  │ value           (BYTES)                                  │   │
│  │ headers_count   (UNSIGNED VARINT)                        │   │
│  │ headers[]       (key=COMPACT_STRING, val=COMPACT_BYTES)  │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘

Actual offset of record[i] = base_offset + offset_delta[i]

Example batch with 3 records at base_offset=100:
  record[0]: offset_delta=0 → actual offset 100
  record[1]: offset_delta=1 → actual offset 101
  record[2]: offset_delta=2 → actual offset 102
  last_offset_delta = 2
  next batch base_offset = 100 + 2 + 1 = 103
```

### VARINT Encoding

```
  UNSIGNED VARINT (used for array lengths, header counts):
  Each byte: [continuation_bit (1)] [7 data bits]
  If continuation_bit=1, more bytes follow

  Examples:
  0         → 0x00           (1 byte)
  127       → 0x7F           (1 byte)
  128       → 0x80 0x01      (2 bytes) — bit layout: 10000000 00000001
  300       → 0xAC 0x02      (2 bytes) — 300 = 0b100101100

  ZIGZAG VARINT (used for signed fields like offset_delta, key_length):
  Maps signed → unsigned: (n << 1) ^ (n >> 31)
  -1 → 1, 1 → 2, -2 → 3, 2 → 4, -3 → 5, 3 → 6 ...
  Small negative numbers (like -1 for null key) encode compactly

  Why zigzag? Regular two's complement -1 = 0xFFFFFFFF needs 5 varint bytes.
  Zigzag -1 = 1 needs only 1 byte.
```

---

## 5. Producer Internals

### Producer Architecture

```
  Application Code
       │
       │ producer.send(record)
       ▼
  ┌─────────────────────────────────────────────────────────┐
  │                   PRODUCER CLIENT                       │
  │                                                         │
  │  ┌──────────────┐    ┌────────────────────────────────┐ │
  │  │ Partitioner  │    │       RecordAccumulator         │ │
  │  │              │    │                                 │ │
  │  │ if key:      │    │  partition 0: [batch1][batch2]  │ │
  │  │  murmur2(key)│    │  partition 1: [batch1]          │ │
  │  │  % numParts  │    │  partition 2: [batch1][batch2]  │ │
  │  │              │    │                                 │ │
  │  │ if no key:   │    │  Each batch fills up to         │ │
  │  │  sticky      │    │  batch.size (default 16KB)      │ │
  │  │  round-robin │    │  or linger.ms expires           │ │
  │  └──────────────┘    └────────────────────────────────┘ │
  │                                      │                  │
  │                          ┌───────────▼──────────────┐   │
  │                          │       Sender Thread       │   │
  │                          │                           │   │
  │                          │  Groups ready batches     │   │
  │                          │  by leader broker         │   │
  │                          │  Sends ProduceRequest     │   │
  │                          └───────────────────────────┘   │
  └─────────────────────────────────────────────────────────┘
                                │
         ┌──────────────────────┼─────────────────────┐
         ▼                      ▼                     ▼
    Broker 1               Broker 2              Broker 3
  (leads P0)             (leads P1)            (leads P2)
```

### Partitioning Strategies

```
  1. KEY-BASED (most common for ordering):
     key = "user-123"
     hash = murmur2("user-123") = 0x7A3F...
     partition = abs(hash) % numPartitions = 2
     → ALL events for user-123 ALWAYS go to partition 2 → ordered

  2. ROUND-ROBIN (no key, max throughput):
     record 1 → partition 0
     record 2 → partition 1
     record 3 → partition 2
     record 4 → partition 0  (cycles)

  3. STICKY BATCHING (default since Kafka 2.4, no key):
     Fill partition 0 until batch is full OR linger.ms expires
     Then switch to partition 1
     → Better batching efficiency than pure round-robin

  4. CUSTOM PARTITIONER:
     Implement org.apache.kafka.clients.producer.Partitioner
     Example: route VIP customers to dedicated partition
```

### Producer Batching and Throughput

```
  WITHOUT batching (linger.ms=0):
  send("msg1") → ProduceRequest → network RTT → next send
  send("msg2") → ProduceRequest → network RTT → next send
  → Throughput limited by RTT (~1000 msg/s at 1ms RTT)

  WITH batching (linger.ms=5, batch.size=1MB):
  ┌─────────────────────────────────┐
  │ 5ms window: accumulate records  │
  │ msg1, msg2, msg3, ... msg10000  │
  └─────────────────────────────────┘
         → single ProduceRequest
  → Throughput: millions of msg/s

  Key configs:
  batch.size        default 16384  (16KB) — max bytes per batch
  linger.ms         default 0      — wait time for more records
  buffer.memory     default 33554432 (32MB) — total producer buffer
  compression.type  default none   — gzip/snappy/lz4/zstd
  max.request.size  default 1MB    — max single request size
```

### Acks and Durability

```
  acks=0 (fire and forget):
  Producer ──────────────────────────────► Broker (no response)
  Fastest, zero durability guarantees

  acks=1 (leader ack):
  Producer ────────────────────────────► Leader
           ◄── OK (leader wrote to disk) ──
  Fast, loses data if leader crashes before replication

  acks=-1 / acks=all (ISR ack):
  Producer ──────────────────────────► Leader
                                         │ replicate
                                    ┌────▼────┐
                                    │Follower1│ ack
                                    └─────────┘
                                    ┌─────────┐
                                    │Follower2│ ack
                                    └─────────┘
           ◄── OK (all ISR confirmed) ──
  Slowest, strongest durability. Required for exactly-once.
```

### Idempotent Producer

```
  Problem: Network error during Produce → producer retries → DUPLICATE

  With enable.idempotence=true:
  Broker assigns:  producer_id = 42
  Producer tracks: sequence_number per partition (starts at 0)

  send batch 1: {producer_id=42, sequence=0, data="msg1"}
  → broker writes, seq[42][P0] = 0
  network error!
  retry:  {producer_id=42, sequence=0, data="msg1"}
  → broker sees seq=0 already written for pid=42 → DUPLICATE DETECTED → ignore
  → returns success (idempotent)

  seq_number increments per batch, broker rejects:
  - seq < expected: duplicate
  - seq > expected + 1: out-of-order (error)
```

---

## 6. Consumer Internals

### Consumer Architecture

```
  ┌──────────────────────────────────────────────────────────┐
  │                    CONSUMER CLIENT                       │
  │                                                          │
  │  poll(Duration timeout)                                  │
  │       │                                                  │
  │       ▼                                                  │
  │  ┌─────────────────────────────────────────────────────┐ │
  │  │              Fetcher                                 │ │
  │  │                                                      │ │
  │  │  prefetch buffer:                                    │ │
  │  │  partition 0 @ offset 50: [batch][batch]             │ │
  │  │  partition 2 @ offset 30: [batch]                    │ │
  │  │                                                      │ │
  │  │  Sends FetchRequest to each partition's leader       │ │
  │  │  max.poll.records (default 500) per poll()           │ │
  │  └─────────────────────────────────────────────────────┘ │
  │                                                          │
  │  ┌─────────────────────────────────────────────────────┐ │
  │  │           ConsumerCoordinator                        │ │
  │  │  - JoinGroup / SyncGroup                             │ │
  │  │  - Heartbeat (every heartbeat.interval.ms=3s)        │ │
  │  │  - OffsetCommit / OffsetFetch                        │ │
  │  └─────────────────────────────────────────────────────┘ │
  └──────────────────────────────────────────────────────────┘
```

### Fetch Request Deep Dive

```
  FetchRequest (v16):
  ┌──────────────────────────────────────────────────────┐
  │ max_wait_ms       (INT32)  ← wait up to N ms         │
  │ min_bytes         (INT32)  ← don't return < N bytes  │
  │ max_bytes         (INT32)  ← don't return > N bytes  │
  │ isolation_level   (INT8)   ← 0=read_uncommitted      │
  │ session_id        (INT32)  ← incremental fetch state │
  │ session_epoch     (INT32)                            │
  │ topics:                                              │
  │   topic_id        (UUID 16 bytes)                    │
  │   partitions:                                        │
  │     partition_index        (INT32)                   │
  │     current_leader_epoch   (INT32)                   │
  │     fetch_offset           (INT64)  ← start here     │
  │     last_fetched_epoch     (INT32)                   │
  │     log_start_offset       (INT64)                   │
  │     partition_max_bytes    (INT32)                   │
  └──────────────────────────────────────────────────────┘

  FetchResponse returns RecordBatches from fetch_offset onward,
  up to partition_max_bytes, only up to High Watermark.

  Long polling: if no data, broker waits up to max_wait_ms
  for data to arrive before returning empty response.
  → Avoids tight polling loop, reduces CPU/network overhead.
```

### Consumer Offset Tracking

```
  __consumer_offsets (internal topic, 50 partitions by default)

  Consumer Group "payment-service":
  ┌─────────────────────────────────────────────────────────┐
  │  key:   group="payment-service" + topic="orders" + P=0  │
  │  value: offset=142, metadata="", timestamp=...          │
  │                                                         │
  │  key:   group="payment-service" + topic="orders" + P=1  │
  │  value: offset=98                                       │
  │                                                         │
  │  key:   group="payment-service" + topic="orders" + P=2  │
  │  value: offset=211                                      │
  └─────────────────────────────────────────────────────────┘

  On consumer restart:
  1. OffsetFetch request to coordinator
  2. Coordinator reads from __consumer_offsets
  3. Consumer resumes from committed offsets

  auto.offset.reset (when no committed offset exists):
  - earliest: start from offset 0
  - latest:   start from current end (skip existing messages)
  - none:     throw exception
```

---

## 7. Replication Protocol

### Leader Epoch and Log Divergence Prevention

```
  WITHOUT leader epoch (old behavior):
  Broker 2 (leader, epoch 1): offsets [0,1,2,3,4,5]
  Broker 3 (follower):        offsets [0,1,2,3]    ← fell behind
  Broker 2 crashes
  Broker 3 elected leader (epoch 2)
  Broker 3 writes: offset 4 = "new data"

  Broker 2 restarts, tries to rejoin:
  Its offset 4 = "old data from epoch 1"
  Conflict! Log divergence!

  WITH leader epoch:
  Each record batch stores partition_leader_epoch
  When Broker 2 rejoins, it asks: "what is the end offset for epoch 1?"
  Leader says: "epoch 1 ended at offset 3"
  Broker 2 truncates its log to offset 3, re-replicates from there
  No divergence!
```

### Replication Flow in Detail

```
  Producer writes "order#1001" to Partition 0

  Step 1: Producer → Leader (Broker 1)
  ┌─────────┐  ProduceRequest(acks=-1)  ┌─────────┐
  │Producer │ ──────────────────────────► Broker 1 │ LEO: 99→100
  └─────────┘                           └────┬────┘
                                             │
  Step 2: Leader appends to local log        │ HW still at 99
  offset 100: "order#1001"                   │
                                             │
  Step 3: Followers replicate                │
  ┌─────────┐  FetchRequest(offset=100) ◄────┤
  │ Broker 2│ ─────────────────────────────► │ returns batch at offset 100
  │ LEO:99  │ ◄────────────────────────────  │
  │ LEO:100 │ ──ack─────────────────────────►│
  └─────────┘                               │
  ┌─────────┐  FetchRequest(offset=100) ◄────┤
  │ Broker 3│ ─────────────────────────────► │
  │ LEO:99  │ ◄────────────────────────────  │
  │ LEO:100 │ ──ack─────────────────────────►│
  └─────────┘                               │
                                             │
  Step 4: All ISR at LEO=100                 │
  HW advances to 100                         │
                                             │
  Step 5: Leader responds to producer        │
  ┌─────────┐  ProduceResponse(offset=100) ◄─┘
  │Producer │
  └─────────┘

  Step 6: Consumers can now see offset 100 (up to HW)
```

### High Watermark and Log End Offset

```
  Partition state on each broker:

  Leader (Broker 1):
  ┌────────────────────────────────────────────────────┐
  │ Log: [0][1][2][3][4][5][6][7][8][9][10]            │
  │                                         ▲   ▲      │
  │                                        HW  LEO     │
  │ HW=9: consumers can read up to offset 9            │
  │ LEO=10: latest byte written (offset 10 not yet    │
  │         replicated to all ISR)                     │
  └────────────────────────────────────────────────────┘

  Follower (Broker 2):
  ┌────────────────────────────────────────────────────┐
  │ Log: [0][1][2][3][4][5][6][7][8]                   │
  │                                  ▲                 │
  │                                 LEO=9              │
  │ (behind leader by 1 batch, still in ISR)           │
  └────────────────────────────────────────────────────┘

  HW = min(leader LEO, follower LEOs in ISR)
     = min(10, 9) = 9
```

---

## 8. KRaft Consensus Protocol

### Raft State Machine

```
  ┌──────────────────────────────────────────────────────────┐
  │                    RAFT NODE STATES                      │
  │                                                          │
  │         timeout, no heartbeat                           │
  │  FOLLOWER ─────────────────────► CANDIDATE              │
  │     ▲                               │                   │
  │     │  discovers higher term        │ receives majority  │
  │     │  or valid leader              │ votes              │
  │     │                               ▼                   │
  │     └──────────────────────────── LEADER                │
  │              steps down if                              │
  │              sees higher term                           │
  └──────────────────────────────────────────────────────────┘

  Terms:
  ┌──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┐
  │  TERM 1  │   TERM 2   │  TERM 3  │ ...
  │ Leader=1 │ Leader=2   │ Leader=1 │
  └──┴──┴──┴──┴──┴──┴──┴──┴──┴──┴──┘

  Every election increments term. Node only votes ONCE per term.
  Randomized election timeout (150–300ms) prevents split votes.
```

### Leader Election Walkthrough

```
  3-node KRaft controller quorum: N1, N2, N3

  Initial state: N1 is leader (term=5)

  N1 crashes.

  N2: election timer fires after 175ms (random)
  N2: currentTerm = 6, state = CANDIDATE, voted for self
  N2: sends VoteRequest{term=6, lastLogIndex=100, lastLogTerm=5} to N1, N3

  N3: receives VoteRequest
      term 6 > my term 5? YES → update term, become FOLLOWER
      have I voted in term 6? NO
      is N2's log at least as up-to-date as mine? YES (same lastLogIndex)
      → grant vote: VoteResponse{term=6, granted=true}

  N2: receives vote from N3 → has 2/3 votes → MAJORITY → becomes LEADER term=6
  N2: sends BeginQuorumEpoch{term=6} to all → prevents new elections
  N2: starts sending Fetch (heartbeat) to followers

  N1 restarts later:
  N1: receives message with term=6 > its term=5 → becomes FOLLOWER
  N1: fetches missed log entries from N2 (using Fetch API)
```

### Log Replication in KRaft

```
  Client → N2 (leader): CreateTopics("orders", numPartitions=3)

  N2 appends to its log (UNCOMMITTED):
  ┌────────────────────────────────────────────────────────┐
  │ index=101: TopicRecord{name="orders", uuid=XYZ}        │ ← uncommitted
  │ index=102: PartitionRecord{id=0, topic=XYZ, leader=1}  │ ← uncommitted
  │ index=103: PartitionRecord{id=1, topic=XYZ, leader=2}  │ ← uncommitted
  │ index=104: PartitionRecord{id=2, topic=XYZ, leader=3}  │ ← uncommitted
  └────────────────────────────────────────────────────────┘
  commitIndex = 100 (previous state)

  N2 sends FetchResponse to followers (replication):
  N3 appends indices 101-104 to its log
  N3 responds to N2's next Fetch: "my LEO is now 104"

  N2: 2/3 nodes (N2+N3) have index 104 → MAJORITY → COMMIT
  commitIndex = 104

  N2 applies committed entries to state machine:
  → ClusterMetadata.addTopic("orders", XYZ)
  → mkdir /kafka-logs/orders-0, orders-1, orders-2

  N2 sends response to client: CreateTopicsResponse{error=0, topicId=XYZ}

  N1 catches up on next heartbeat cycle.
```

### KRaft Metadata Record Types

```
  __cluster_metadata log record types:

  ┌──────────────────────────────────────────────────────────────┐
  │ Type  │ ID │ Fields                                          │
  ├──────────────────────────────────────────────────────────────┤
  │TopicRecord       │ 2 │ name, topicId(UUID)                  │
  │PartitionRecord   │ 1 │ partitionId, topicId, replicas[],    │
  │                  │   │ isr[], leader, leaderEpoch           │
  │BrokerReg         │19 │ brokerId, listeners[], rack          │
  │ConfigRecord      │12 │ resourceType, resourceName, configs[]│
  │PartitionChange   │ 5 │ partitionId, topicId, leader, isr[]  │
  │RemoveTopic       │ 3 │ topicId                              │
  │ProducerIds       │22 │ brokerId, nextProducerId             │
  │AccessControl     │ 6 │ ACL entries                          │
  └──────────────────────────────────────────────────────────────┘

  Each stored as a Record inside a RecordBatch:
  frame_version(1) + record_type(1) + schema_version(1) + fields...
```

### Snapshot in KRaft

```
  Problem: New node joins cluster. Metadata log has 10 million entries.
  Replaying all 10M entries takes too long.

  Solution: Snapshot
  ┌─────────────────────────────────────────────────┐
  │ 00000000000500000000-0000000001.checkpoint       │
  │                                                 │
  │ Complete state at log offset 500000000:         │
  │ - All topics with their configs                 │
  │ - All partitions with leader/replicas/isr       │
  │ - All broker registrations                      │
  │ - All ACLs                                      │
  └─────────────────────────────────────────────────┘

  New node: FetchSnapshot → downloads checkpoint
            Then fetch log entries from offset 500000001 onward
  → Much faster than replaying full log
```

---

## 9. Kafka Wire Protocol

### Request/Response Frame Structure

```
  ┌──────────────────────────────────────────────────────────┐
  │                    TCP STREAM                            │
  │                                                          │
  │  ┌────────┬──────────────────────────────────────────┐   │
  │  │ 4 bytes│         N bytes (= message_size)         │   │
  │  │message │                                          │   │
  │  │  size  │  REQUEST or RESPONSE PAYLOAD             │   │
  │  └────────┴──────────────────────────────────────────┘   │
  │                                                          │
  │  Requests and responses share the same TCP connection    │
  │  Matched by correlation_id                               │
  └──────────────────────────────────────────────────────────┘

  REQUEST PAYLOAD:
  ┌─────────────────────────────────────────────────────────┐
  │ api_key         (INT16)  ← which API                    │
  │ api_version     (INT16)  ← which version                │
  │ correlation_id  (INT32)  ← client assigns, echoed back  │
  │ client_id       (NULLABLE_STRING) ← for logging/quotas  │
  │ TAG_BUFFER      (varint 0 = empty)                      │
  │ [request body fields...]                                │
  └─────────────────────────────────────────────────────────┘

  RESPONSE PAYLOAD:
  ┌─────────────────────────────────────────────────────────┐
  │ correlation_id  (INT32)  ← matches request              │
  │ TAG_BUFFER      (varint 0 = empty)                      │
  │ [response body fields...]                               │
  └─────────────────────────────────────────────────────────┘
```

### Flexible vs Non-Flexible Versions

```
  For each API, starting at some version, it becomes "flexible":
  - Arrays use COMPACT_ARRAY (varint N+1) instead of INT32 length
  - Strings use COMPACT_STRING instead of INT16 length prefix
  - TAG_BUFFER added to end of each struct
  - Allows backward-compatible field additions via tagged fields

  Example ApiVersions:
  v0-v3: non-flexible
  v4+:   flexible (uses COMPACT_ARRAY, TAG_BUFFER)

  Non-flexible array: INT32 count, then elements
  Flexible array:     varint (count+1), then elements
                      0 = null array, 1 = empty, 2 = one element
```

### Complete API Table

```
  ┌─────────────────────────────────────┬─────┬──────────────────┐
  │ API Name                            │  ID │ Flexible Version │
  ├─────────────────────────────────────┼─────┼──────────────────┤
  │ Produce                             │   0 │ v9+              │
  │ Fetch                               │   1 │ v12+             │
  │ ListOffsets                         │   2 │ v6+              │
  │ Metadata                            │   3 │ v9+              │
  │ LeaderAndIsr                        │   4 │ v4+              │
  │ StopReplica                         │   5 │ v3+              │
  │ UpdateMetadata                      │   6 │ v6+              │
  │ ControlledShutdown                  │   7 │ v3+              │
  │ OffsetCommit                        │   8 │ v8+              │
  │ OffsetFetch                         │   9 │ v6+              │
  │ FindCoordinator                     │  10 │ v3+              │
  │ JoinGroup                           │  11 │ v6+              │
  │ Heartbeat                           │  12 │ v4+              │
  │ LeaveGroup                          │  13 │ v4+              │
  │ SyncGroup                           │  14 │ v4+              │
  │ DescribeGroups                      │  15 │ v5+              │
  │ ListGroups                          │  16 │ v3+              │
  │ SaslHandshake                       │  17 │ —                │
  │ ApiVersions                         │  18 │ v3+              │
  │ CreateTopics                        │  19 │ v5+              │
  │ DeleteTopics                        │  20 │ v4+              │
  │ InitProducerId                      │  22 │ v2+              │
  │ AddPartitionsToTxn                  │  24 │ v3+              │
  │ TxnOffsetCommit                     │  28 │ v3+              │
  │ DescribeConfigs                     │  32 │ v4+              │
  │ AlterConfigs                        │  33 │ v2+              │
  │ SaslAuthenticate                    │  36 │ v2+              │
  │ CreatePartitions                    │  37 │ v2+              │
  │ Vote                                │  52 │ v0+              │
  │ BeginQuorumEpoch                    │  53 │ v0+              │
  │ EndQuorumEpoch                      │  54 │ v0+              │
  │ FetchSnapshot                       │  59 │ v0+              │
  │ DescribeTopicPartitions             │  75 │ v0+              │
  └─────────────────────────────────────┴─────┴──────────────────┘
```

---

## 10. Controller and Broker Lifecycle

### Broker Startup Sequence

```
  Broker boots up:

  1. Load local KRaft metadata log snapshot + tail entries
     → Build in-memory ClusterMetadata from committed entries

  2. Register with KRaft controller:
     → Append BrokerRegistrationRecord to metadata log
     → Controller acknowledges registration

  3. Receive UpdateMetadata from controller:
     → "Here are all current partition leaders/followers"
     → Broker knows which partitions it leads/follows

  4. For each partition it leads:
     → Open log segment files
     → Initialize LEO from last batch's base_offset + last_offset_delta + 1
     → Start accepting Produce/Fetch requests for these partitions

  5. For each partition it follows:
     → Start FetchRequest loop to leader (replication)

  6. Start accepting client connections on port 9092
```

### Partition Leader Failover

```
  Normal operation:
  Producer → Broker2 (leader for P0) → replicates to Broker1, Broker3

  Broker2 crashes (no heartbeat for session.timeout.ms):

  Controller detects failure:
  ┌──────────────────────────────────────────────────────────────┐
  │ 1. Remove Broker2 from cluster                               │
  │ 2. For each partition Broker2 led:                           │
  │    - Pick new leader from ISR (prefer highest LEO)           │
  │    - Update ISR list (remove Broker2)                        │
  │ 3. Append PartitionChangeRecord{P0, leader=Broker1} to log   │
  │ 4. Send LeaderAndIsr{P0, leader=Broker1} to Broker1          │
  │ 5. Broadcast UpdateMetadata to all brokers                   │
  └──────────────────────────────────────────────────────────────┘

  Broker1 receives LeaderAndIsr:
  → Transition P0 from FOLLOWER to LEADER
  → Accept writes to P0

  Producers: get NotLeaderOrFollower error
  → Fetch metadata → find Broker1 is new leader → reconnect
  → Total downtime: ~5-30 seconds (configurable)
```

### Unclean Leader Election

```
  All ISR members for P0 are down. Only out-of-sync Broker4 is alive.

  unclean.leader.election.enable=false (default):
  → P0 stays offline until an ISR member comes back
  → Data safety guaranteed (no data loss)
  → Availability sacrificed

  unclean.leader.election.enable=true:
  → Broker4 elected as leader even though behind
  → P0 comes back online immediately
  → Messages that Broker4 didn't replicate are LOST
  → Use only when availability > consistency
```

---

## 11. Consumer Groups and Rebalancing

### Group Coordinator

```
  __consumer_offsets has 50 partitions.
  For group "payment-service":
  coordinator_partition = abs(murmur2("payment-service")) % 50 = 23
  Broker that leads __consumer_offsets partition 23 = COORDINATOR

  All JoinGroup/SyncGroup/Heartbeat/OffsetCommit for "payment-service"
  go to this one coordinator broker.
```

### Full Rebalance Protocol

```
  Topic "orders" has 6 partitions.
  Consumer group "payment-service" has 3 consumers: C1, C2, C3.

  Phase 1: JoinGroup (all consumers)
  ┌─────┐ JoinGroup{group="payment-service", memberId=""} ┌────────────┐
  │ C1  │ ─────────────────────────────────────────────► │Coordinator │
  │ C2  │ ─────────────────────────────────────────────► │            │
  │ C3  │ ─────────────────────────────────────────────► │            │
  └─────┘                                                │            │
                                                         │ Waits for  │
                                                         │ all to join│
                                                         │ or timeout │
                                                         │            │
  Coordinator picks LEADER (e.g. C1):                   │            │
  C1 ◄── JoinGroupResponse{leader=C1, members=[C1,C2,C3]}│            │
  C2 ◄── JoinGroupResponse{follower, generation=5}       │            │
  C3 ◄── JoinGroupResponse{follower, generation=5}       │            │

  Phase 2: SyncGroup (leader assigns partitions)
  C1 runs partition assignment algorithm:
    C1 → P0, P1
    C2 → P2, P3
    C3 → P4, P5

  ┌─────┐ SyncGroup{assignments={C1:[P0,P1], C2:[P2,P3]...}} ┌────────┐
  │ C1  │ ─────────────────────────────────────────────────► │Coord.  │
  │ C2  │ ─────────────────────────────────────────────────► │        │
  │ C3  │ ─────────────────────────────────────────────────► │        │
  └─────┘                                                    └────────┘

  C1 ◄── SyncGroupResponse{assignment=[P0,P1]}
  C2 ◄── SyncGroupResponse{assignment=[P2,P3]}
  C3 ◄── SyncGroupResponse{assignment=[P4,P5]}

  Phase 3: Consumption + Heartbeat
  Each consumer fetches from its assigned partitions.
  Each sends Heartbeat every heartbeat.interval.ms (default 3s).
  If coordinator doesn't hear from a consumer within session.timeout.ms
  (default 45s) → triggers rebalance.
```

### Stop-the-World vs Incremental Rebalance

```
  EAGER (Stop-the-World) rebalance:
  ┌─────────────────────────────────────────────────────┐
  │ C4 joins group                                      │
  │ All consumers REVOKE all their partitions           │ ← PAUSE
  │ All consumers JoinGroup                             │
  │ Leader assigns partitions to C1,C2,C3,C4           │
  │ All consumers resume                                │ ← RESUME
  │                                                     │
  │ Gap: 10–30 seconds of no consumption               │
  └─────────────────────────────────────────────────────┘

  COOPERATIVE (Incremental) rebalance (CooperativeStickyAssignor):
  ┌─────────────────────────────────────────────────────┐
  │ C4 joins group                                      │
  │ Round 1: Each consumer keeps its partitions         │
  │          C4 gets assigned nothing yet               │
  │ Round 2: C1 revokes P1, C2 revokes P3              │ ← only 2 revoked
  │          C4 gets P1 and P3                          │
  │                                                     │
  │ Gap: Only revoked partitions pause, others continue │
  └─────────────────────────────────────────────────────┘
```

---

## 12. Exactly-Once Semantics

### Three Layers

```
  Layer 1: Idempotent Producer (within session, single partition)
  Layer 2: Transactional Producer (across partitions)
  Layer 3: read-process-write loop with transactions
```

### Transactional Produce + Consume Loop

```
  Stream Processor: read from "orders", write to "processed-orders"

  ┌─────────────────────────────────────────────────────────────────┐
  │                                                                 │
  │  producer = new KafkaProducer({transactional.id="processor-1"}) │
  │  producer.initTransactions()                                    │
  │  // → InitProducerId request → broker assigns pid=42, epoch=3   │
  │                                                                 │
  │  while (true) {                                                 │
  │    records = consumer.poll()                                    │
  │    producer.beginTransaction()                                  │
  │                                                                 │
  │    for record in records:                                       │
  │      result = process(record)                                   │
  │      producer.send("processed-orders", result)                  │
  │                                                                 │
  │    // Include consumer offsets IN the transaction:              │
  │    producer.sendOffsetsToTransaction(                           │
  │      {orders-P0: offset 101},                                   │
  │      consumer.groupMetadata()                                   │
  │    )                                                            │
  │                                                                 │
  │    producer.commitTransaction()                                 │
  │    // → writes COMMIT marker to processed-orders log            │
  │    // → writes committed offsets to __consumer_offsets          │
  │    // → both atomically, or both rolled back                    │
  │  }                                                              │
  └─────────────────────────────────────────────────────────────────┘

  If crash between send and commit:
  → Coordinator finds open transaction → marks as ABORT on timeout
  → Consumer with isolation.level=read_committed never sees aborted records
  → Processor restarts, re-reads from last committed offset, reprocesses
  → Idempotent producer deduplicates the retry
  → Net result: exactly-once processing
```

### Transaction Markers

```
  processed-orders partition 0 log:
  ┌──────────────────────────────────────────────────────────────┐
  │ offset 50: RecordBatch{is_transactional=1, pid=42} "result1" │
  │ offset 51: RecordBatch{is_transactional=1, pid=42} "result2" │
  │ offset 52: COMMIT MARKER{pid=42, epoch=3}                    │ ← visible
  └──────────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────────┐
  │ offset 53: RecordBatch{is_transactional=1, pid=42} "result3" │
  │ offset 54: ABORT MARKER{pid=42, epoch=3}                     │ ← invisible
  └──────────────────────────────────────────────────────────────┘

  Consumer with read_committed:
  - Sees offsets 50, 51, 52 (committed transaction)
  - Skips offset 53, 54 (aborted transaction)
  - Sees offset 55+ (next transaction)
```

---

## 13. Performance Architecture

### Zero-Copy with sendfile()

```
  WITHOUT zero-copy (traditional):
  ┌──────────────────────────────────────────────────────────┐
  │                    NORMAL READ+SEND                      │
  │                                                          │
  │  Disk → [kernel page cache]                             │
  │          ↓ copy 1                                       │
  │         [kernel read buffer]                            │
  │          ↓ copy 2 (kernel → userspace)                  │
  │         [JVM heap buffer]                               │
  │          ↓ copy 3 (userspace → kernel)                  │
  │         [kernel socket buffer]                          │
  │          ↓ copy 4 (kernel → NIC)                        │
  │         [NIC DMA buffer] → Network                      │
  │                                                         │
  │  4 copies, 2 context switches, 2 syscalls               │
  └──────────────────────────────────────────────────────────┘

  WITH zero-copy (sendfile syscall):
  ┌──────────────────────────────────────────────────────────┐
  │                    ZERO-COPY sendfile()                  │
  │                                                          │
  │  Disk → [kernel page cache]                             │
  │          ↓ DMA (hardware)                               │
  │         [NIC DMA buffer] → Network                      │
  │                                                         │
  │  2 copies (both DMA), 0 userspace involvement           │
  │  Data NEVER enters JVM heap                             │
  └──────────────────────────────────────────────────────────┘

  Result: 10x+ throughput improvement for Fetch path
  This is why Kafka can serve the SAME data to 100 consumers
  with almost no incremental CPU cost (page cache hit, DMA copy)
```

### Page Cache Strategy

```
  Broker machine: 64GB RAM
  ┌──────────────────────────────────────────────────────────┐
  │ JVM Heap: 6GB (Kafka recommends small heap!)             │
  │                                                          │
  │ OS Page Cache: ~58GB ← THIS is Kafka's real "cache"      │
  │                                                          │
  │ Recent writes sit in page cache → reads are cache hits   │
  │ Most consumer lag is small → consumers read from cache   │
  │ No GC pressure on cached data (it's in OS, not JVM)      │
  └──────────────────────────────────────────────────────────┘

  Why small JVM heap?
  Large heap → long GC pauses → broker appears unresponsive
  → Followers think leader is down → unnecessary failovers
  6-8GB heap is typical. All throughput comes from page cache.
```

### Sequential I/O Performance

```
  Random I/O (traditional database pattern):
  ┌──────────────────────────────────────┐
  │ HDD Random read: ~100 IOPS           │ ~0.4MB/s
  │ SSD Random read: ~100,000 IOPS       │ ~400MB/s
  └──────────────────────────────────────┘

  Sequential I/O (Kafka's append-only pattern):
  ┌──────────────────────────────────────┐
  │ HDD Sequential read: ~200MB/s        │ 500x better than random
  │ SSD Sequential read: ~3,000MB/s      │ 7x better than random
  └──────────────────────────────────────┘

  Kafka on HDD can match SSD random I/O patterns.
  This is why Kafka can run on spinning disks cost-effectively.
```

### Compression Impact

```
  Compression happens at RecordBatch level:
  ┌────────────────────────────────────────────────────────┐
  │ Without compression:                                   │
  │ 1000 JSON records × 500 bytes = 500KB on wire/disk     │
  │                                                        │
  │ With snappy compression:                               │
  │ 1000 JSON records × 500 bytes → ~100KB (5x ratio)     │
  │ Less network bandwidth, less disk I/O, more throughput │
  │                                                        │
  │ Compression happens ONCE by producer                   │
  │ Decompression happens ONCE by consumer                 │
  │ Broker stores and forwards compressed bytes as-is      │
  │ (zero-copy of compressed data)                         │
  └────────────────────────────────────────────────────────┘

  lz4 = best throughput/ratio balance (recommended)
  zstd = best compression ratio (Kafka 2.1+)
  gzip = CPU expensive, good ratio, avoid for high throughput
```

---

## 14. Log Compaction

### How Compaction Works

```
  Topic: user-preferences (cleanup.policy=compact)

  Before compaction:
  ┌──────────────────────────────────────────────────────────┐
  │ offset 0: key="user:1" value="theme:dark"                │
  │ offset 1: key="user:2" value="theme:light"               │
  │ offset 2: key="user:1" value="theme:blue"  ← overwrites  │
  │ offset 3: key="user:3" value="theme:dark"                │
  │ offset 4: key="user:2" value=null          ← tombstone   │
  │ offset 5: key="user:1" value="theme:red"   ← overwrites  │
  └──────────────────────────────────────────────────────────┘

  After compaction:
  ┌──────────────────────────────────────────────────────────┐
  │ offset 3: key="user:3" value="theme:dark"                │
  │ offset 5: key="user:1" value="theme:red"   ← latest      │
  │                                                          │
  │ user:2 tombstoned → deleted after retention period       │
  └──────────────────────────────────────────────────────────┘

  Consumer reading from offset 0 gets "final state" for each key.
  Used for: CDC (Change Data Capture), event sourcing, caches.
```

### Compaction Thread

```
  Log cleaner thread runs in background:

  1. Find "dirtiest" segment (highest ratio of redundant keys)
  2. Build offset map: key → highest offset (in-memory hash map)
  3. Re-read segments from beginning
  4. Copy records to new segment ONLY if:
     - Record's offset == highest offset for that key (latest)
     - AND key is not tombstoned
  5. Replace old segments with compacted segment
  6. Delete tombstone records after delete.retention.ms

  Active (latest) segment is NEVER compacted.
  Consumers reading old offsets get "compacted away" records skipped.
```

---

## 15. Common Interview Questions with Detailed Answers

---

**Q: How does Kafka guarantee message ordering?**

Within a single partition, messages are strictly ordered by offset. The broker only appends to a partition, never inserts or reorders. When a consumer reads from offset N, it always gets N, N+1, N+2... in that order.

Across partitions, there is NO ordering guarantee. If you need all events for a given entity to be ordered, use a message key — the partitioner hashes the key deterministically, so all events for `user-123` always go to the same partition, preserving order for that user.

If you need strict global ordering across everything, use a single partition — but this eliminates parallelism.

---

**Q: What is the difference between at-most-once, at-least-once, and exactly-once?**

**At-most-once**: Commit offset before processing. If the consumer crashes after commit but before processing, those messages are lost. No duplicates, but possible loss.

**At-least-once**: Commit offset after processing. If the consumer crashes after processing but before committing, it will reprocess on restart. No loss, but possible duplicates.

**Exactly-once**: Use idempotent producers (`enable.idempotence=true`) and transactional producers (`transactional.id`). The producer deduplicates retries using `producer_id + sequence_number`. Consumer offsets are committed atomically inside the transaction. Combined with `isolation.level=read_committed`, consumers never see partial results.

---

**Q: What happens when a broker fails?**

1. Controller detects the broker is down (missed heartbeats, ZK/KRaft session expiry).
2. Controller identifies all partitions where the failed broker was the leader.
3. For each such partition, controller picks a new leader from the ISR.
4. Controller writes `PartitionChangeRecord` to the KRaft log (replicated to other controllers).
5. Controller sends `LeaderAndIsr` to the new leader broker.
6. Controller broadcasts `UpdateMetadata` to all remaining brokers.
7. Producers/consumers get `NotLeaderOrFollower` errors, refresh metadata, reconnect to the new leader.

If `unclean.leader.election.enable=false` (default) and all ISR members are down, the partition goes offline. No data loss but availability is sacrificed. With `true`, an out-of-sync replica becomes leader — data loss possible but partition comes back online.

---

**Q: What is the High Watermark and why does it matter?**

The High Watermark (HW) is the highest offset that has been replicated to ALL ISR members. Consumers can only read messages up to the HW.

This prevents consumers from reading data that might be lost if the leader crashes. If a consumer read offset 100 but only the leader has it (not replicated), and the leader crashes, that data is gone — but the consumer already processed it. The HW ensures consumers only see committed (replicated) data.

---

**Q: Explain the consumer group rebalancing protocol.**

Rebalancing is triggered when: a consumer joins or leaves the group, a consumer crashes (heartbeat timeout), topic partition count changes.

1. All consumers send `JoinGroup` to the group coordinator.
2. Coordinator waits for all members or timeout, picks one as group leader.
3. Coordinator sends member list to the group leader.
4. Group leader runs partition assignment algorithm (RangeAssignor, StickyAssignor, etc.).
5. Group leader sends assignments via `SyncGroup`.
6. Each consumer receives its assigned partitions and starts fetching.

In eager rebalancing (default for most assignors), all partitions are revoked at step 1 — causing a "stop the world" pause. CooperativeStickyAssignor does incremental rebalancing — only partitions that need to move are revoked.

---

**Q: How does Kafka achieve such high throughput?**

1. **Sequential disk I/O**: Kafka only appends. Sequential writes are 500x faster than random writes on HDDs.

2. **Zero-copy**: The `sendfile()` syscall transfers data from page cache directly to NIC via DMA, bypassing JVM heap entirely.

3. **Batching**: Producer batches multiple records into one network request and one disk write. Amortizes syscall and network RTT overhead.

4. **Page cache**: Kafka relies on OS page cache. Recent data is already in RAM. No double-caching (unlike databases that maintain their own buffer pool).

5. **Compression**: Compressed batches mean less network and disk I/O, with compression/decompression only at producer and consumer.

6. **Async I/O**: Producers don't wait for disk before returning — they wait for replication acknowledgment, which happens in parallel.

---

**Q: What is the difference between `log.retention.bytes` and `log.segment.bytes`?**

`log.retention.bytes` = total size limit for the entire partition log (all segments). When exceeded, oldest segments are deleted.

`log.segment.bytes` = size of a SINGLE segment file. When the active segment reaches this size, it is "rolled" — closed, and a new active segment is opened.

Example: `log.retention.bytes=10GB`, `log.segment.bytes=1GB`
→ A partition can have up to 10 segments of 1GB each.
→ When segment 11 starts, segment 1 is deleted.

---

**Q: What is the role of `__consumer_offsets` and `__cluster_metadata`?**

`__consumer_offsets`: Internal topic (50 partitions) that stores committed consumer group offsets. When a consumer commits offset N for partition P in group G, this is written as a record keyed by `(group, topic, partition)`. On restart, consumers read from here to resume.

`__cluster_metadata`: KRaft metadata log. Single partition. Stores all cluster state as a Raft-replicated log — topic creations, partition changes, broker registrations, config changes. Replaces ZooKeeper. The controller quorum (3–5 nodes) maintains this log via Raft consensus.

---

**Q: What is a "sticky" partition strategy and why is it better?**

Without a key, old Kafka used pure round-robin — record 1 to P0, record 2 to P1, record 3 to P2. This means each batch sent to a broker might contain records from many partitions, resulting in small batches.

Sticky partitioning (default since Kafka 2.4): stick to one partition until the batch is full OR `linger.ms` expires, then switch. This fills batches more efficiently → larger batches → better compression → fewer network requests → higher throughput. The trade-off is slightly higher latency for the first record in each batch.

---

**Q: How does log compaction differ from log retention?**

**Retention** (`cleanup.policy=delete`): Time-based or size-based deletion of entire segments. All records are deleted when old enough. Used for event logs, activity data, finite retention.

**Compaction** (`cleanup.policy=compact`): Keeps the latest record per key indefinitely (unless tombstoned). Old values for the same key are deleted during background compaction. Used for changelogs, database state replication, caches. A new consumer reading from offset 0 gets the current state of all keys.

Both can be combined: `cleanup.policy=delete,compact` — compact AND apply time-based retention.

---

**Q: What is the difference between `session.timeout.ms` and `heartbeat.interval.ms`?**

`heartbeat.interval.ms` (default 3s): How often the consumer sends a heartbeat to the coordinator. Should be 1/3 of `session.timeout.ms`.

`session.timeout.ms` (default 45s): If the coordinator doesn't receive a heartbeat within this window, it considers the consumer dead and triggers rebalancing.

`max.poll.interval.ms` (default 5 minutes): Maximum time between `poll()` calls. If processing takes longer than this, the consumer is considered dead even if heartbeats are being sent (heartbeats run in a background thread).

Typical tuning: for slow-processing consumers, increase `max.poll.interval.ms` or reduce `max.poll.records`.

---

**Q: Explain the difference between KRaft and ZooKeeper mode.**

| Aspect | ZooKeeper Mode | KRaft Mode |
|--------|---------------|------------|
| External dependency | Yes (ZooKeeper cluster) | No |
| Controller failover | 30–60 seconds | < 1 second |
| Partition limit | ~200K (ZK bottleneck) | Millions |
| Metadata storage | ZooKeeper znodes | `__cluster_metadata` Raft log |
| Operations | Two systems to manage | Single system |
| Snapshots | ZK snapshots | KRaft checkpoints |

KRaft uses Raft consensus internally. The controller quorum (typically 3 nodes) replicates metadata changes before committing. Data brokers receive metadata via the `UpdateMetadata` API.
