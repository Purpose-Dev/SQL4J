# ADP - Module `sql4j-memory`

## Context

The `sql4j-memory` module is responsible for off-heap memory management in SQL4J.
It abstract pages, slot directories, and allocation strategies, enabling efficient in-memory columnar storage.
Current implementation provides a foundation (`OffHeapBuffer`, `PageLayout`, `SlotDirectory`), but several components
are incomplete or lack architectural clarity.<br />
This document defines **decisions, rationale, and next steps**.

---

## 1. Incomplete or Missing Components

### 1.1 PageMeta

* **Decision**: Introduce a `PageMeta` structure that stores page-level metadata (usable size, fragmentation ratio,
  version, compaction state).
* **Rationale**: Without explicit metadata, page management requires expensive scanning. Metadata enables **O(1)**
  access to occupancy and fragmentation, which is crucial for allocation, compaction, and recovery.

---

### 1.2 MemoryPool

* **Decision**: Extend `MemoryPool` to support multiple segments, occupancy tracking, and runtime metrics.
* **Rationale**: Current design only wraps `ByteBuffer.allocateDirect`. For real workloads, SQL4J must:<br />
    1. Manage several memory regions.
    2. Track free vs. used pages.
    3. Expose memory statistics to operators and the planner. This provides predictable performance and prevents memory
       fragmentation from escalating.

---

### 1.3 PageManager

* **Decision**: Add `pin/unpin` operations and a replacement policy (initially **Clock** or **LRU**).
* **Rationale**: Pinning ensures that actively used pages are not evicted during execution. Replacement policies allow
  controlled eviction under memory pressure. Without this, concurrent queries risk corrupting memory state.

---

### 1.4 File-backed Pages

* **Decision**: Implement `mapFileAsPages` using memory-mapped files.
* **Rationale**: SQL4J will eventually persist data. File-mapped pages unify memory and persistence under a single
  interface, reducing complexity while leveraging the OS page cache.

---

### 1.5 WAL / TxManager Integration

* **Decision**: Leave WAL and `TxManager` integration for `sql4j-mvcc`, but add hooks in `PageManager` for journaling.
* **Rationale**: WAL is essential for durability and recovery, but premature coupling complicates memory APIs. Designing
  hooks now avoids refactoring later.

---

## 2. Improvements Required

### 2.1 Compaction

* **Decision**: Redesign `SlotDirectory` compaction to perform **in-place relocation** of slots with minimal copying.
* **Rationale**: Current compaction risks high CPU overhead due to unnecessary data movement. An incremental,
  slot-by-slot strategy reduces fragmentation without full rewrites.

---

### 2.2 Metrics

* **Decision**: Expose runtime metrics (`MemoryStats`) for allocation count, fragmentation, page occupancy, and
  pin/unpin usage.
* **Rationale**: Observability is mandatory for diagnosing query performance regressions and memory pressure issues.

---

### 2.3 Concurrency

* **Decision**: Introduce ZIO `Ref` / `Semaphore` wrappers for concurrent access to shared pages.
* **Rationale**: Without controlled access, concurrent fibers can corrupt page state. ZIO primitives ensure composable
  and purely functional concurrency control.

---

### 2.4 OffHeapBuffer Safety

* **Decision**: Integrate `OffHeapBuffer` with ZIO `Scope` to guarantee deterministic release.
* **Rationale**: Current best-effort `release()` is unsafe for long-lived processes. Scopes align with ZIO resource
  management, ensuring no leaks.

---

### 2.5 PageLayout Specification

* **Decision**: Formalize page layout as a binary contract:
    * **Header** (PageMeta, pin count, flags)
    * **Slot directory** (offsets, lengths)
    * **Data region** (tuples in serialized form)
* **Rationale**: Explicit layout definition prevents inconsistencies across modules and allows direct unsafe memory
  operations without ambiguity.

---

## 3. Testing Strategy

### Unit Tests

* **MemoryPool**: allocation, release, exhaustion handling.
* **PageManager**: lifecycle (`allocate → pin/unpin → free`).
* **SlotDirectory**: insertion, deletion, compaction under load.
* **OffHeapBuffer**: alignment correctness, release safety.
* **PageLayout**: read/write roundtrip for tuples.

### Integration Tests

* Multipage workloads with allocation pressure.
* Fragmentation + compaction benchmarks.
* Concurrent fiber access validation.
* File-mapped pages with persistence guarantees.

**Rationale**: Ensuring correctness at both micro (unit) and macro (integration) levels prevents silent corruption,
which is the primary risk in low-level memory management.

---

## 4. Roadmap & Priorities

1. **Finalize Core Structures**: `PageMeta`, `PageLayout`, `SlotDirectory`.
   → foundation for correctness.
2. **Strengthen Memory Management**: extend `MemoryPool` and `PageManager` with pin/unpin, replacement policies,
   metrics.
   → enables concurrency and safe eviction.
3. **Optimize Compaction & Fragmentation**.
   → prevents long-term performance degradation.
4. **Add Robust Testing**.
   → unit + property-based + integration coverage.
5. **Integrate File Mapping & WAL Hooks**.
   → prepares for persistence and durability.

---
