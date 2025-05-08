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
- [ ] TCP or HTTP server for produce/consume  
- [ ] Rust client SDK  
- [ ] Simple binary or JSON wire protocol  
- [ ] Offset commit batching:  
  - Dirty flag on commit  
  - Manual `flush()` API  
  - Optional auto-flush interval  
- [ ] Runtime retention and access control:  
  - Segment retention (time/size-based)  
  - Watermark APIs  
  - Authentication hooks  
  - Partition health APIs  

### Stage 4 – Broker Coordination (Multi-Node)
- [ ] Metadata management via `openraft`  
- [ ] Partition leadership and replication  

### Stage 5 – Delivery Guarantees
- [ ] Producer acknowledgments and retries  
- [ ] Durable offset storage  
- [ ] Idempotent produce with deduplication  

## Current Features
- Segment rotation with sparse indexing  
- `stream_from_offset` API  
- Key-aware message routing  
- Consumer groups with persisted offsets  
- Comprehensive error model (`EngineError`, `DeserializeError`)  

## Getting Started
Documentation will be available soon.

## Contributing
Not currently accepting external contributions. Feature suggestions may be submitted via GitHub issues.

## License
TBD (Likely Apache 2.0 or MIT/Apache dual-license)