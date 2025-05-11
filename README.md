# FlyQ

A high-performance, distributed messaging system inspired by Apache Kafka, written in Rust. Focused on simplicity, observability, and performance.

## Roadmap

### Stage 1 – MVP: Single-Node, Append-Only Log (Completed)
- [x] Message and Partition structs
- [x] Disk-backed append-only log with segment rotation
- [x] Sparse in-memory and file-based index
- [x] Minimal CLI for `produce` and `consume`

### Stage 2 – Multi-Partition Support (Completed)
- [x] Topic abstraction with multiple partitions
- [x] Round-robin and key-based partitioning
- [x] Consumer group offset tracking (in-memory + JSON persistence)

### Stage 3 – Networking & Runtime Optimization (In Progress)
- [x] TCP server for produce/consume
- [x] Rust client SDK
- [x] Simple binary wire protocol with framing, version, checksums
- [ ] Offset commit batching:
  - Dirty flag on commit
  - Manual `flush()` API
  - Optional auto-flush interval
- [ ] Runtime retention and access control:
  - Segment retention (time/size-based)
  - Watermark APIs
  - Authentication hooks
  - Partition health APIs

### Stage 4 – Indexing Rework: MVP Fixes
- [ ] Replace in-memory `BTreeMap` with compact on-disk format
- [ ] Persistent memory-mapped index files
- [ ] Recovery guarantees (no stale or misaligned index)
- [ ] Forward-only scan guarantees with correct segment boundaries
- [ ] Test coverage for crash-safe recovery, rotation, and re-indexing

### Stage 5 – Indexing Optimization & Strategy
- [ ] Pluggable index strategies per topic/partition
- [ ] Backward scan support for tailing consumers
- [ ] Timestamp-based seek support
- [ ] Secondary indexing (e.g. by headers or custom fields)
- [ ] Index compaction and garbage collection
- [ ] CLI/metrics observability for index health and density

### Stage 6 – Broker Coordination (Multi-Node)
- [ ] Metadata management via `openraft`
- [ ] Partition leadership and replication

### Stage 7 – Delivery Guarantees
- [ ] Producer acknowledgments and retries
- [ ] Durable offset storage
- [ ] Idempotent produce with deduplication

## Current Features
- Segment rotation with sparse indexing
- `stream_from_offset` API for direct reads
- Message routing with round-robin and key-awareness
- Consumer groups with offset tracking
- `StoredRecord` log format: `[len][offset][message]`
- Clean serialization model (`serialize_body`, `serialize_with_len`)
- Comprehensive error handling (`EngineError`, `DeserializeError`, `ProtocolError`)

## Getting Started
Documentation will be available soon.

## Contributing
Not currently accepting external contributions. Feature suggestions may be submitted via GitHub issues.

## License
TBD (Likely Apache 2.0 or MIT/Apache dual-license)
