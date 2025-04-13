# Kafka Alternative (Name TBD)

A high-performance, distributed messaging system inspired by Apache Kafka, built with Rust.

## Project Status

### ✅ Stage 1: MVP – Single-Node, Append-Only Log (Completed)
- **1.1** – Message and Partition Structs: Implemented cleanly
- **1.2** – Disk-Backed Append-Only Log: Segments backed by files with rotation support
- **1.3** – In-Memory and Persisted Sparse Index: Works with index interval, backfilled on recovery
- **1.4** – CLI for Produce/Consume: To be implemented (may defer until after basic networking)

### 🔜 Stage 2: Multi-Partition Support
- **2.1** – Topic Abstraction with Multiple Partitions
- **2.2** – Round-Robin or Key-Based Partitioning
- **2.3** – Consumer Group Offset Tracking

### 🛠️ Stage 3: Basic Networking
- **3.1** – TCP or HTTP Server for Produce/Consume
- **3.2** – Rust Client SDK
- **3.3** – Simple Binary or JSON Protocol

### 🚦 Stage 4: Broker Coordination (Multi-Node)
- **4.1** – Use openraft for Metadata Management
- **4.2** – Partition Leadership & Replication

### 🔁 Stage 5: Reliability and Delivery Guarantees
- **5.1** – Producer Acknowledgments & Retries
- **5.2** – Consumer Offset Persistence
- **5.3** – Idempotency for Deduplication

### 🔧 Stage 6: Dev Experience & Admin
- **6.1** – Prometheus Metrics
- **6.2** – Web UI for Monitoring Topics & Offsets
- **6.3** – WASM Filter/Transformation Plugins

### ⚙️ Stage 7: Post-MVP – Platform/DX
- **7.1** – Namespaces / Multi-Tenant Isolation
- **7.2** – Pluggable Storage Backends (RocksDB, Redb, Parquet)
- **7.3** – Native gRPC/QUIC APIs
- **7.4** – Log Time Travel & Snapshot Reads
- **7.5** – Embedded Mode for Edge/Mobile
- **7.6** – GitOps-style Declarative Config

### 🧪 Stage 8: Advanced Delivery Semantics
- **8.1** – Exactly-Once with Producer IDs & Dedup
- **8.2** – Transactional Messaging Across Partitions

## Current Achievements

✅ **Log Segments**  
Implemented rotation, naming, and cleanup

✅ **Sparse Indexing**  
Index written every n messages, with fallback recovery

✅ **Recovery Path**  
Loads index, streams tail for unknowns, backfills last_offset

✅ **Stream API**  
`stream_from_offset` is clean and segment-aware

✅ **Tests**  
Added for missing index, partial index, and recovery logic

✅ **Error Handling**  
Solid error handling with `DeserializeError`, backpressure planned

## Getting Started

*(To be added as development progresses)*

## Contributing

*(To be added as project opens for contributions)*

## License

*(To be determined)*
