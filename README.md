FlyQ
A high-performance, distributed messaging system inspired by Apache Kafka, written in Rust.
Focused on simplicity, observability, and performance.

🚧 Project Status
FlyQ is under active development. We are currently building Stage 3 (basic networking + runtime optimization). The engine supports multiple partitions, sparse indexing, and consumer group offset tracking with disk persistence.

🗺️ Roadmap
✅ Stage 1: MVP – Single-Node, Append-Only Log
1.1 – Message and Partition Structs

1.2 – Disk-Backed Append-Only Log with Segment Rotation

1.3 – Sparse In-Memory and File Index

1.4 – CLI for Produce/Consume (basic)

✅ Stage 2: Multi-Partition Support
2.1 – Topic Abstraction with Multiple Partitions

2.2 – Round-Robin and Key-Based Partitioning

2.3 – Consumer Group Offset Tracking (with JSON persistence)

🛠️ Stage 3: Networking & Runtime Optimizations
3.1 – TCP or HTTP Server for Produce/Consume APIs

3.2 – Rust Client SDK

3.3 – Simple Wire Protocol (Binary or JSON)

3.4 – Offset Commit Batching

Track dirty state per commit

Manual flush() API

Optional: auto-flush interval / shutdown hook

3.5 – Runtime Retention, Visibility & Access Control

3.5.1 – Segment Retention (time/size-based)

3.5.2 – Watermark API (low/high watermark per partition)

3.5.3 – Authentication Hooks for Server APIs

3.5.4 – Partition Health & Metadata API

🚦 Stage 4: Broker Coordination (Multi-Node)
4.1 – Metadata Management with openraft

4.2 – Partition Leadership & Replication Protocol

🔁 Stage 5: Delivery Guarantees
5.1 – Producer Acknowledgments & Retries

5.2 – Durable Offset Storage via Internal Topic

5.3 – Idempotent Produce API for Deduplication

🔧 Stage 6: Dev Experience & Admin
6.1 – Prometheus Metrics for Log, Segment, Partition

6.2 – Web UI for Topics, Offsets, and Message Browser

6.3 – WASM Plugin System for Inline Filters / Transforms

⚙️ Stage 7: Platform Extensions
7.1 – Multi-Tenant Namespace Isolation

7.2 – Pluggable Storage Backends (RocksDB, Redb, Parquet)

7.3 – Native gRPC/QUIC APIs

7.4 – Log Time Travel, Snapshot Reads

7.5 – Embedded Mode for Edge/Mobile Use

7.6 – GitOps-style Declarative Topic/Partition Config

🧪 Stage 8: Advanced Delivery Semantics
8.1 – Exactly-Once Semantics with Producer IDs

8.2 – Cross-Partition Transactional Messaging

✅ Current Achievements
Segment rotation, sparse indexing, and backfilled recovery

stream_from_offset() API over multiple segments

Per-topic partitioned log layout with topic abstraction

Message routing via round-robin or key hashing

Consumer groups with offset tracking and JSON persistence

Integration tests for segment recovery, message replay, and offset commits

Clean error model with DeserializeError and EngineError

📦 Getting Started
(To be added soon — will include CLI and dev guide)

🤝 Contributing
(Opening soon. If you’re interested, raise an issue or PR.)

⚖️ License
To be finalized (likely Apache 2.0 or MIT/Apache dual).

