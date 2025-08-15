# FlyQ

A high-performance, distributed messaging system inspired by Apache Kafka, written in Rust. Focused on simplicity, observability, and performance.

## Roadmap

### Stage 0 – Design Principles
- Log-centric, segment-first architecture
- Sparse indexing with forward-only scan guarantees
- Clean separation between storage, protocol, and coordination
- Pluggable strategies (indexing, retention, compaction)

### Stage 1 – MVP: Single-Node, Append-Only Log (✔ Stable)
- [x] Message and Partition structs
- [x] Disk-backed append-only log with segment rotation
- [x] Sparse in-memory and file-based index
- [x] Minimal CLI for `produce` and `consume`

### Stage 2 – Multi-Partition Support (✔ Stable)
- [x] Topic abstraction with multiple partitions
- [x] Round-robin and key-based partitioning
- [x] Consumer group offset tracking (in-memory + JSON persistence)

### Stage 3 – Networking & Runtime Optimization (✔ Complete)
- [x] TCP server for produce/consume
- [x] Rust client SDK
- [x] Simple binary wire protocol with framing, version, checksums
- [x] Offset commit batching
- [x] Segment retention policies:
  - [x] Time-based cleanup (configurable retention duration)
  - [x] Size-based cleanup (configurable retention bytes)
  - [x] Background cleanup loop with memory-safe file deletion
  - [x] Drop-based cleanup to prevent race conditions
- [x] Runtime visibility:
  - [x] Watermark APIs (low, high, log end offset)
  - [x] Consumer lag tracking across topics and partitions
  - [x] Partition health endpoints (segment count, size, watermarks)
  - [x] Monitoring tools and example implementations

### Stage 4 – Indexing Rework: MVP Fixes 
- [ ] Replace in-memory `BTreeMap` with compact on-disk format
- [ ] Persistent memory-mapped index files
- [ ] Recovery guarantees (no stale or misaligned index)
- [ ] Forward-only scan guarantees with correct segment boundaries
- [ ] Robust test coverage for crash recovery, rotation, and re-indexing

### Stage 5 – Indexing Optimization & Strategy
- [ ] Pluggable index strategies per topic/partition
- [ ] Backward scan support for tailing consumers
- [ ] Timestamp-based seek support
- [ ] Secondary indexing (e.g. by headers or custom fields)
- [ ] Index compaction and garbage collection
- [ ] Index visibility via CLI and metrics (density, staleness, gaps)

### Stage 6 – Broker Coordination (Multi-Node)
- [ ] Metadata management via `openraft`
- [ ] Partition leadership and replication
- [ ] Basic authentication scaffolding
- [ ] Pluggable request validation layer

### Stage 7 – Delivery Guarantees
- [ ] Producer acknowledgments and retries
- [ ] Durable offset storage
- [ ] Idempotent produce with deduplication
- [ ] Replace JSON offset file with internal `__consumer_offsets` topic
  - Append offset commits as records to a log
  - Use standard segment and index engine for durability
  - Enable log compaction for latest-offset retention

### Stage 8 – Observability & Tooling (Planned)
- [ ] Prometheus metrics and exporter integration
- [ ] Segment/index compaction diagnostics
- [ ] Consumer lag and partition health dashboards

## Current Features
- **Segment Management**: Rotation with sparse indexing and configurable size limits
- **Retention Policies**: Time-based and size-based automatic cleanup with background processing
- **Memory Safety**: Drop-based file deletion preventing race conditions with active readers
- **Message Streaming**: `stream_from_offset` API for direct reads with forward-only guarantees
- **Partitioning**: Round-robin and key-based message routing across multiple partitions
- **Consumer Groups**: Offset tracking with in-memory and JSON persistence
- **Wire Protocol**: Binary framing with version control and checksums
- **Storage Format**: `StoredRecord` log format: `[len][offset][message]`
- **Serialization**: Clean model with `serialize_body` and `serialize_with_len`
- **Error Handling**: Comprehensive error types (`EngineError`, `DeserializeError`, `ProtocolError`)
- **Configuration**: TOML-based broker configuration for retention and operational settings
- **Runtime Observability**: Watermark tracking, consumer lag monitoring, and partition health metrics
- **Monitoring Tools**: Real-time monitoring example with lag alerts and health dashboards

## Configuration

FlyQ supports TOML-based configuration for retention policies and operational settings. Create a `flyq.toml` file:

```toml
# Segment size limit (bytes) before rotation
segment_max_bytes = 1073741824  # 1 GiB

# Time-based retention - keep data newer than this duration
retention = "7d"  # 7 days (supports: 60s, 5m, 1h, 7d)

# Size-based retention - total bytes per partition (optional)
retention_bytes = 10737418240  # 10 GiB, set to null to disable

# Background cleanup interval
cleanup_interval = "60s"  # 1 minute
```

### Retention Policies

FlyQ automatically manages disk space through two retention mechanisms:

1. **Time-based retention**: Removes segments older than the configured `retention` duration
2. **Size-based retention**: When enabled via `retention_bytes`, removes oldest segments when partition size exceeds the limit

Both policies work together and respect the active segment (never deleted) to ensure data integrity.

## Getting Started

### Running the Server

```bash
# Build the project
cargo build --release

# Run with default configuration
./target/release/flyQ --base-dir ./data --port 8080

# Run with custom configuration
./target/release/flyQ --base-dir ./data --port 8080 --config flyq.toml
```

### Basic Usage

```bash
# The server will automatically create topics and manage retention
# Client SDK and CLI tools are available in the flyq-client crate
```

## Contributing
Not currently accepting external contributions. Feature suggestions may be submitted via GitHub issues.

## License
TBD (Likely Apache 2.0 or MIT/Apache dual-license)
