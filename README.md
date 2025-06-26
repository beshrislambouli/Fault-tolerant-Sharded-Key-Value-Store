# 🧠 Distributed Sharded Key-Value Store

A distributed, fault-tolerant, and linearizable key-value store built from scratch using the [Raft consensus algorithm](https://raft.github.io/). The system supports replication, snapshotting, sharding, and dynamic reconfiguration for high availability and scalability.

## 🚀 Features

- **Raft-Based Replication**
  - Implements log replication, leader election, and persistent state storage.
  - Ensures consistency across replicas with majority quorum.
  - Recovers state after crashes and partitions.

- **Replicated Key-Value Store**
  - Supports `Get` and `Put` operations with at-most-once semantics.
  - Guarantees linearizability for all client requests.

- **Snapshotting for Log Compaction**
  - Automatically generates snapshots to truncate logs.
  - Enables efficient state recovery and reduces disk usage.

- **Sharded Architecture**
  - Keys are partitioned into shards handled by independent Raft-replicated server groups.
  - Enables parallel request handling and scales with the number of groups.

- **Dynamic Reconfiguration**
  - A centralized controller migrates shards between groups to balance load and support cluster resizing.
  - Maintains linearizability and availability throughout configuration changes.

## 🧪 Resilience

The system was tested under a wide range of failure scenarios:
- Server crashes and restarts
- Network partitions and dropped messages
- Duplicate and delayed client requests
- Shard movements during reconfiguration

## 🛠️ Tech Stack

- **Language**: Go
- **Persistence**: Custom snapshot + disk-based log serialization
- **Communication**: RPC over TCP
- **Testing**: Fault-injection and randomized execution

## 📌 Status

✅ Linearizable  
✅ Fault-tolerant  
✅ Scalable  
✅ Snapshot-enabled  
✅ Sharded  
✅ Dynamically reconfigurable  

---

Built for real-world fault tolerance, scalability, and correctness.
