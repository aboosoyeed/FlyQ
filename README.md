# FlyQ

*A high-performance, distributed messaging system inspired by Apache Kafka, written in Rust.*  
Focused on simplicity, observability, and performance.

---

## 🧭 Roadmap

### ✅ Stage 1 – MVP: Single-Node, Append-Only Log

- ✅ **1.1** Message and Partition structs
- ✅ **1.2** Disk-backed append-only log with segment rotation
- ✅ **1.3** Sparse in-memory and file-based index
- ✅ **1.4** Minimal CLI for `produce` and `consume`

---

### ✅ Stage 2 – Multi-Partition Support

- ✅ **2.1** Topic abstraction with multiple partitions
- ✅ **2.2** Round-robin and key-based partitioning
- ✅ **2.3** Consumer group offset tracking (in-memory + JSON persistence)

---

### 🛠️ Stage 3 – Networking & Runtime Optimization

- 🔄 **3.1** TCP or HTTP server for produce/consume
- 🔄 **3.2** Rust client SDK
- 🔄 **3.3** Simple binary or JSON wire protocol
- 🔄 **3.4** Offset commit batching  
  _Includes:_  
  &nbsp;&nbsp;&nbsp;&nbsp;• Dirty flag on commit  
  &nbsp;&nbsp;&nbsp;&nbsp;• Manual `flush()` API  
  &nbsp;&nbsp;&nbsp;&nbsp;• Optional auto-flush interval or shutdown hook
- 🔄 **3.5** Runtime retention, visibility & access control  
  _Includes:_  
  &nbsp;&nbsp;&nbsp;&nbsp;• 3.5.1 Segment retention (time/size-based)  
  &nbsp;&nbsp;&nbsp;&nbsp;• 3.5.2 Watermark APIs (low/high per partition)  
  &nbsp;&nbsp;&nbsp;&nbsp;• 3.5.3 Authentication hooks for server APIs  
  &nbsp;&nbsp;&nbsp;&nbsp;• 3.5.4 Partition metadata & health APIs

---

### 🚦 Stage 4 – Broker Coordination (Multi-Node)

- 🔄 **4.1** Metadata management via `openraft`
- 🔄 **4.2** Partition leadership and replication

---

### 🔁 Stage 5 – Delivery Guarantees

- 🔄 **5.1** Producer acknowledgments and retry support
- 🔄 **5.2** Durable offset storage via internal topic
- 🔄 **5.3** Idempotent produce with deduplication

---

### 🔧 Stage 6 – Dev Experience & Admin

- 🔄 **6.1** Prometheus metrics
- 🔄 **6.2** Web UI for topics, partitions, and offsets
- 🔄 **6.3** WASM plugin support for transform/filter pipelines

---

### ⚙️ Stage 7 – Platform Extensions

- 🔄 **7.1** Namespace & multi-tenant isolation
- 🔄 **7.2** Pluggable storage backends (RocksDB, Redb, Parquet)
- 🔄 **7.3** Native gRPC / QUIC APIs
- 🔄 **7.4** Log time-travel & snapshot reads
- 🔄 **7.5** Embedded mode for edge/mobile devices
- 🔄 **7.6** GitOps-style declarative configuration

---

### 🧪 Stage 8 – Advanced Delivery Semantics

- 🔄 **8.1** Exactly-once delivery with producer IDs
- 🔄 **8.2** Transactional messaging across partitions

---

## ✅ Current Highlights

- Segment rotation, sparse indexing, and recovery
- Clean log stream API (`stream_from_offset`)
- Round-robin and key-aware routing
- Consumer groups with persisted offset tracking
- Integration tests for segment replay and offset recovery
- `DeserializeError` and `EngineError` based error model

---

## 📦 Getting Started

*(Coming soon – CLI usage, API docs, setup instructions)*

---

## 🤝 Contributing

*(Soon to open. For now, feel free to raise an issue or suggest features.)*

---

## ⚖️ License

*(To be determined — likely Apache 2.0 or dual MIT/Apache.)*
