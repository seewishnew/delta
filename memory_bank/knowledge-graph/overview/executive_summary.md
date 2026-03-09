---
title: "Executive Summary"
tags: [overview, executive, L1, delta-lake]
layer: L1
last_updated: 2026-03-02
related:
  - "[[architecture/system_map]]"
  - "[[architecture/module_dependencies]]"
  - "[[_index/module_manifest]]"
---

# Executive Summary

---

## What Delta Lake Is

Delta Lake is an **open-source transactional storage layer** that adds ACID guarantees, scalable metadata handling, time travel, schema enforcement, and Change Data Feed to data lakes built on commodity object stores (S3, GCS, Azure ADLS) or distributed filesystems (HDFS). It stores table metadata as an append-only **transaction log** (`_delta_log/`) co-located with Parquet data files, enabling snapshot isolation for reads, serializable writes via Optimistic Concurrency Control (OCC), and full history replay to any prior version.

Delta Lake's architecture separates the **protocol** (the specification of what a valid Delta table looks like and how writers must behave) from the **engine** (the query processing system that executes reads and writes). This separation, realized through the `delta-kernel-api` library with its `Engine` SPI, allows Apache Spark, Apache Flink, Python clients, and custom JVM engines to all read and write the same Delta tables through a single authoritative protocol implementation.

---

## Target Users and Use Cases

| User / Use Case | How Delta Lake Helps |
|---|---|
| **Data engineers** building ETL pipelines | ACID writes, schema enforcement, schema evolution, generated columns |
| **Analytics engineers** querying large datasets | Time travel (`VERSION AS OF`, `TIMESTAMP AS OF`), data skipping (min/max/nullCount statistics), liquid clustering (Z-order, Hilbert) |
| **Streaming ingestion teams** | `DeltaSource` / `DeltaSink` structured streaming integration; Change Data Feed (CDF) for downstream propagation |
| **Platform teams** managing data lakes on S3/ADLS/GCS | Pluggable LogStore per cloud (HDFS, S3, Azure, GCS, IBM COS, Oracle Cloud); DynamoDB-backed multi-cluster S3 |
| **Organizations sharing data cross-org** | Delta Sharing protocol: pre-signed URL vending; no direct cloud credential sharing |
| **Teams using multiple query engines** | UniForm (Universal Format): read same Parquet files as Iceberg or Hudi — no data copying |
| **Flink users** | Kernel-based Flink Table API connector with incremental V2 checkpoint writing; no Spark dependency |
| **Python / Spark Connect users** | `delta.tables.DeltaTable` Python API with transparent classic/Connect mode dispatch |
| **Enterprise with Unity Catalog** | Coordinated commits via UC REST API; server-side scan planning; catalog-managed table lifecycle |

---

## Technology Stack

| Layer | Technology | Notes |
|---|---|---|
| Primary language | Scala 2.13.17 | All Spark/shared modules; 2.12 dropped |
| Protocol/Kernel API | Java 11 | `delta-kernel-api`, `delta-kernel-defaults`, `delta-storage`, `delta-flink` |
| Build tool | SBT (Scala Build Tool) | Monorepo; single root `build.sbt`; 23 SBT sub-projects |
| Execution engine (primary) | Apache Spark 4.0.1, 4.1.0 | `provided`; Spark 4.2.0-SNAPSHOT defined but unreleased |
| Execution engine (streaming) | Apache Flink 2.0.1 | `provided`; no Spark dependency in Flink connector |
| Data format | Apache Parquet | All data files and checkpoints |
| Log format | Newline-delimited JSON (`.json`) + Parquet (checkpoints) | Transaction log actions |
| RPC / IDL | Proto3 + gRPC (grpc-java 1.62.2 / protobuf-java 3.25.1) | Spark Connect extension protocol |
| Storage abstraction | Apache Hadoop FileSystem API (Hadoop 3.4.2) | LogStore and DefaultEngine I/O |
| Python bindings | Python 3 + PySpark | `delta-spark` PyPI package |
| Deletion vectors | RoaringBitmap (roaringbitmap library) | Off-heap compressed bitmaps for row-level soft deletes |
| Catalog (optional) | Unity Catalog REST API | Coordinated commits, server-side planning, credential vending |
| External coordinator (S3) | AWS DynamoDB | Multi-cluster S3 commit serialization |
| UniForm — Iceberg | Apache Iceberg 1.10.1 (shaded) | Metadata-only Delta→Iceberg conversion |
| UniForm — Hudi | Apache Hudi (provided, Spark 4.0.1 only) | Delta→Hudi timeline conversion |
| Test fixtures | ScalaTest 3.2.15, JMH (benchmarks), WireMock (UC mocking) | All modules |

---

## Architectural Pattern Summary

Delta Lake follows a **protocol-first, engine-agnostic layered monolith** pattern. The architecture has four distinct layers:

1. **Protocol Layer** (`PROTOCOL.md` + `delta-kernel-api` internals): The canonical specification of the Delta table format — action types, log replay rules, checkpoint formats, OCC commit protocol, table features — is both documented in `PROTOCOL.md` and implemented once in `delta-kernel-api` with zero runtime dependencies. All other layers depend on this specification.

2. **Storage Layer** (`delta-storage`): A minimal SPI defining atomic file writes (`LogStore`) and catalog-managed commit coordination (`CommitCoordinatorClient`). Concrete implementations target each cloud/filesystem. This layer knows nothing about Spark or Flink.

3. **Kernel Layer** (`delta-kernel-api` + `delta-kernel-defaults` + `delta-kernel-unitycatalog`): The engine-agnostic read/write API. The `Engine` SPI lets each connector provide its own I/O implementation; all protocol logic is centralized in `kernel-api`. `DefaultEngine` provides a Hadoop+Parquet reference implementation used by Flink and by Spark-v2.

4. **Connector Layer** (`delta-spark`, `delta-flink`, `delta-sharing-spark`, `delta-connect-*`, `delta-iceberg`, `delta-hudi`, `python`): Engine-specific implementations that build on the layers below. Spark's connector (`delta-spark`) is the most feature-complete and handles all writes; the Flink connector is the cleanest Kernel adoption; Delta Sharing adds cross-org distribution; UniForm adds Iceberg/Hudi interoperability; Spark Connect enables remote thin clients.

The architecture exhibits **two coexisting Spark read paths**: a classic v1 path (DeltaLog-based, Spark-integrated) and a v2 path (Kernel-backed, DV-aware). Both are aggregated into the single published `delta-spark` artifact. This dual-path is a known architectural constraint stemming from the incremental migration from a Spark-coupled design to the engine-agnostic Kernel design.

---

## Subsystems

| Subsystem | KG Document |
|---|---|
| System Map (full dependency and data flow overview) | [[architecture/system_map]] |
| Module Dependencies (artifact matrix, SBT graph) | [[architecture/module_dependencies]] |
| Protocol / Transaction Log | [[protocol/transaction_log]] |
| Table Features Registry | [[protocol/table_features]] |
| Kernel Module (API + protocol impl) | [[modules/kernel]] |
| Spark Module (v1 + v2 + unified) | [[modules/spark]] |
| Spark Commands (DML deep-dive) | [[modules/spark/commands]] |
| Storage Module (LogStore + CommitCoordinator) | [[modules/storage]] |
| Spark Connect Module | [[modules/spark-connect]] |
| Delta Sharing Module | [[modules/sharing]] |
| Python Module | [[modules/python]] |
| Flink Connector | [[modules/connectors/flink]] |
| UniForm — Iceberg | [[modules/connectors/uniform-iceberg]] |
| UniForm — Hudi | [[modules/connectors/uniform-hudi]] |
| Connectors Overview (Flink, contribs, golden-tables) | [[modules/connectors]] |
| Data Models (cross-cutting) | [[cross_cutting/data_models]] |
| Shared Interfaces & IDL (cross-cutting) | [[cross_cutting/interfaces_idl]] |
| Shared Utilities (cross-cutting) | [[cross_cutting/shared_utilities]] |
| Build System | [[dev/build_system]] |
| Protocol RFCs | [[protocol/rfcs]] |

---

## Key Architectural Observations

### What Works Well

1. **Protocol-first design with PROTOCOL.md as ground truth** — Every engine implementation (Spark, Flink, Kernel) references the same protocol document. Changes to the protocol require an RFC and explicit `TableFeature` registration, preventing silent breakage.

2. **`delta-kernel-api` zero-dependency design** — The entire protocol implementation (log replay, OCC, DVs, data skipping, checkpointing) is available to any JVM engine without dragging in Spark. The Flink connector demonstrates this cleanly.

3. **LogStore pluggability** — The abstraction is thin (5 methods) but sufficient to support HDFS, S3/GCS/Azure/IBM/Oracle with their wildly different atomicity primitives, each with appropriate guarantees.

4. **UniForm metadata-only conversion** — Generating Iceberg/Hudi metadata from Delta metadata without data duplication is architecturally elegant. The constraint set (`IcebergCompatV2`, column mapping, no DVs) is explicitly enforced at write time rather than silently violated.

5. **Spark Connect extension-point design** — Delta's proto IDL uses `google.protobuf.Any` extension fields, making Delta's Connect support entirely additive. No changes to Spark's core schema were needed.

### Fragility / Architectural Debt

> [!WARNING] Dual v1/v2 Spark read path is a maintenance burden
> The `delta-spark-v1` and `delta-spark-v2` connectors implement overlapping log replay and snapshot state for the Spark execution environment. As the protocol evolves (new actions, new table features), both paths must be kept current. The v2 path is not yet feature-complete for all v1 write features. Source: [[modules/spark]]

> [!WARNING] `delta-contribs` depends on `delta-spark`, not `delta-storage`
> Community LogStore implementations (IBM COS, Oracle Cloud) extend `HadoopFileSystemLogStore` which lives in `delta-spark-v1`, forcing a full Spark connector dependency for what should be a pure storage-layer extension. Source: [[modules/connectors]]

> [!WARNING] Both UniForm Iceberg AND UniForm Hudi are disabled for Spark 4.1.0 (the default build target)
> `delta-hudi` is only compiled and published for Spark 4.0.1. `delta-iceberg` UniForm support is also disabled for Spark 4.1.0 — `CrossSparkVersions.scala` line 279 explicitly sets `supportIceberg = false` for the `spark41` spec. Users on Spark 4.1.0 (the default build target) cannot use either UniForm format. Source: [[modules/connectors/uniform-hudi]], [[modules/connectors/uniform-iceberg]], [[architecture/module_dependencies]]

> [!WARNING] Vacuum returns no metrics in Python Spark Connect mode
> `DeltaTable.vacuum()` in Spark Connect mode (Python) discards execution metrics, returning `None` instead of an empty DataFrame. The classic PySpark path returns a metrics DataFrame. Source: [[modules/python]], [[modules/spark-connect]]

> [!WARNING] No deep-clone support over Spark Connect
> `CloneTable` in the Connect protocol only supports shallow clone (`is_shallow=true`). Deep clone is not supported via Connect. Source: [[modules/spark-connect]]

> [!WARNING] `UCTokenBasedRestClient` does not implement schema conversion
> The Unity Catalog REST client (`UCTokenBasedRestClient.java`) explicitly notes that schema conversion from Delta schema string to UC `ColumnInfo` objects is **not implemented** (`UCTokenBasedRestClient.java:292-294`). Source: [[modules/storage]]

---

## Build Snapshot

| Property | Value |
|---|---|
| Current version | `4.1.0-SNAPSHOT` |
| Supported Spark versions | 4.0.1, 4.1.0 |
| Default Spark version | 4.1.0 |
| Scala version | 2.13.17 (2.12 dropped) |
| Java target | JVM 11 (17 for Spark 4.x modules) |
| Published artifacts | `delta-spark`, `delta-kernel-api`, `delta-kernel-defaults`, `delta-storage`, `delta-storage-s3-dynamodb`, `delta-flink`, `delta-sharing-spark`, `delta-iceberg`, `delta-hudi`, `delta-contribs` |

---

## Known Gaps and Unresolved Issues

The following areas were noted as either not fully documented or as known architectural limitations identified during this KG pipeline run:

### Documentation Gaps
| Gap | Reason | Priority |
|---|---|---|
| Spark `test/` suites structure | Test infrastructure is large; test files not explored | Low |
| `kernel/examples/kernel-examples/` detailed examples | Maven project; examples walked at high level only | Low |
| `protocol_rfcs/accepted/` full RFC deep-dives | RFCs summarized; full per-RFC analysis not done | Medium |
| `dev/` tooling scripts in detail | Pre-commit hooks, CI helper scripts surveyed at high level | Low |
| CI/CD matrix in detail | Workflows listed; individual step details not fully explored | Low |

### Architectural Limitations (Logged from Pipeline)
| Issue | Source Document |
|---|---|
| `DeltaCatalog`/`DeltaTable` in the Flink module are internal SPI interfaces — not Flink framework `Catalog`/`DynamicTableSource` implementations. The extent of Flink Table API integration surface is unclear. | [[modules/connectors/flink]] |
| `UCTokenBasedRestClient` does not implement Delta schema → UC `ColumnInfo` conversion | [[modules/storage]] |
| Vacuum metrics are discarded in Python Spark Connect mode | [[modules/spark-connect]], [[modules/python]] |
| Both UniForm formats (Iceberg + Hudi) are disabled for Spark 4.1.0 | [[modules/connectors/uniform-iceberg]], [[modules/connectors/uniform-hudi]] |
| v1/v2 dual read path maintenance burden as protocol evolves | [[modules/spark]] |

---

## How to Keep This KG Current

Run the `codebase-knowledge-graph` agent in **incremental mode** after substantial changesets. Key signals to watch for:
- New table features added to `TableFeature.scala` or `tablefeatures/` → update `protocol/table_features.md`
- New action types in `actions/` → update `protocol/actions.md`
- Protocol changes in `PROTOCOL.md` → update `protocol/transaction_log.md`
- New module additions to `build.sbt` → update manifest and add new module doc
- Kernel API additions (`Table`, `Snapshot`, `Transaction`, `Engine`) → update `modules/kernel.md`
- New DML commands in `spark/commands/` → update `modules/spark/commands.md`
