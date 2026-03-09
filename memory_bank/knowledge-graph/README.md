# Delta Lake OSS — Knowledge Graph

> **Last updated**: 2026-03-02  
> **Pipeline**: Full generation, depth-3, 35 documents  
> **Repo**: `/Users/vishnuc/OSS/delta.git/main`  
> **Obsidian vault**: Open `memory_bank/knowledge-graph/` as an Obsidian vault for wiki-links and graph view.

---

## Start Here

| Document | What it covers |
|---|---|
| [Executive Summary](overview/executive_summary.md) | What Delta Lake is, tech stack, architecture overview, known issues |
| [System Map](architecture/system_map.md) | Full subsystem map, data/control plane flows, key architectural decisions |
| [Module Dependencies](architecture/module_dependencies.md) | Published artifact matrix, SBT project graph, cross-Spark build |

---

## Protocol Layer (L2)

| Document | What it covers |
|---|---|
| [Transaction Log Protocol](protocol/transaction_log.md) | MVCC architecture, log format, snapshot construction, commit protocol |
| [Action Types](protocol/actions.md) | All 10 action types with field-level schemas and JSON examples |
| [Checkpoint Formats](protocol/checkpoints.md) | V1/V2 checkpoint specs, sidecar files, `_last_checkpoint` |
| [Table Features](protocol/table_features.md) | All 25+ reader/writer features with dependencies and descriptions |
| [Protocol RFCs](protocol/rfcs.md) | 13 RFCs — accepted, in-flight, and rejected design proposals |

---

## Module Documentation (L3)

### Core Modules

| Document | What it covers |
|---|---|
| [Delta Kernel](modules/kernel.md) | Engine-agnostic Java API, Engine SPI, protocol impl, DefaultEngine, UC integration, benchmarks |
| [Delta Spark](modules/spark.md) | DeltaLog, OCC, Snapshot, streaming (DeltaSource), catalog V2, coordinated commits, data skipping, public API |
| [Spark Commands](modules/spark/commands.md) | All DML/DDL: DELETE, UPDATE, MERGE, OPTIMIZE, VACUUM, RESTORE, CLONE, CONVERT — with algorithm details |
| [Delta Storage](modules/storage.md) | LogStore interface + all impls (HDFS/S3/Azure/GCS), CommitCoordinator API, UC commit coordinator, DynamoDB S3 store |

### Connector Modules

| Document | What it covers |
|---|---|
| [Spark Connect](modules/spark-connect.md) | Proto3 IDL (commands + relations), client-side planner, server-side plugin, end-to-end flow |
| [Delta Sharing](modules/sharing.md) | Virtual filesystem, sharing client, CDF streaming, pre-signed URL architecture |
| [Python API](modules/python.md) | Classic PySpark + Spark Connect dual-mode, DeltaTable API, plan serialization |
| [Flink Connector](modules/connectors/flink.md) | Flink Table API (DeltaCatalog/DeltaTable internal SPI), kernel-based checkpointing |
| [UniForm — Iceberg](modules/connectors/uniform-iceberg.md) | Delta→Iceberg metadata conversion, IcebergCompatV1/V2 protocol features, async converter |
| [UniForm — Hudi](modules/connectors/uniform-hudi.md) | Delta→Hudi timeline conversion, REPLACE_COMMIT_ACTION, Spark 4.0.1 only |
| [Community Connectors](modules/connectors.md) | delta-contribs: IBM COS, Oracle Cloud LogStore implementations |
| [Golden Tables](modules/connectors/golden-tables.md) | Test fixture corpus (130+ tables), GoldenTableUtils, how tests use them |

---

## Cross-Cutting Concerns (L2)

| Document | What it covers |
|---|---|
| [Data Models](cross_cutting/data_models.md) | ColumnarBatch/ColumnVector/Row (kernel), DataFrame/Dataset (Spark), type system mapping |
| [Shared Interfaces & IDL](cross_cutting/interfaces_idl.md) | Engine SPI, LogStore SPI, CommitCoordinatorClient SPI, Delta Connect proto IDL |
| [Shared Utilities](cross_cutting/shared_utilities.md) | SchemaUtils, CoordinatedCommitsUtils, JsonUtils, GoldenTableUtils, Scala implicits |

---

## Development (L2)

| Document | What it covers |
|---|---|
| [Build System](dev/build_system.md) | SBT multi-project, cross-Spark build, testing, publishing, MiMa, benchmarks, CI/CD |

---

## Index Files

| Document | What it covers |
|---|---|
| [Module Manifest](_index/module_manifest.md) | Full SBT project map with dependency order, key files, depth-3 components |
| [Change Log](_index/change_log.md) | KG pipeline history, corrections applied, known gaps |
| [File Map](_index/file_map.md) | Source file → KG doc mapping |
| [Tag Index](_index/tag_index.md) | Obsidian tag registry |

---

## Knowledge Graph Structure

```
memory_bank/knowledge-graph/
├── README.md                          ← This file
├── overview/
│   └── executive_summary.md           ← L1: Start here
├── architecture/
│   ├── system_map.md                  ← L2: Full architecture
│   └── module_dependencies.md         ← L2: Artifact matrix + SBT graph
├── protocol/
│   ├── transaction_log.md             ← L2: Protocol spec
│   ├── actions.md                     ← L2: Action types
│   ├── checkpoints.md                 ← L2: Checkpoint formats
│   ├── table_features.md              ← L2: Feature registry (25+)
│   └── rfcs.md                        ← L2: Design proposals (13 RFCs)
├── modules/
│   ├── kernel.md                      ← L3: Kernel API + defaults + UC
│   ├── spark.md                       ← L3: Spark connector (full depth-3)
│   ├── spark/
│   │   └── commands.md                ← L3: DML commands deep-dive
│   ├── storage.md                     ← L3: LogStore + CommitCoordinator
│   ├── spark-connect.md               ← L3: Spark Connect integration
│   ├── sharing.md                     ← L3: Delta Sharing
│   ├── python.md                      ← L3: Python API
│   ├── connectors.md                  ← L3: Community connectors
│   └── connectors/
│       ├── flink.md                   ← L3: Flink connector
│       ├── uniform-iceberg.md         ← L3: UniForm Iceberg
│       ├── uniform-hudi.md            ← L3: UniForm Hudi
│       └── golden-tables.md           ← L4: Test fixtures
├── cross_cutting/
│   ├── data_models.md                 ← L2: Type system + data model
│   ├── interfaces_idl.md              ← L2: All SPIs + proto IDL
│   └── shared_utilities.md            ← L2: Shared utility classes
├── dev/
│   └── build_system.md                ← L2: Build + CI + benchmarks
└── _index/
    ├── module_manifest.md             ← Decomposer output
    ├── change_log.md                  ← Pipeline history
    ├── file_map.md                    ← Source → KG doc map
    ├── tag_index.md                   ← Tag registry
    ├── decomposition_review_01.md     ← Review artifacts
    ├── adjudication_decompose_01.md
    ├── detail_review_batch1.md
    ├── detail_review_batch2.md
    ├── arch_review_01.md
    └── adjudication_arch_01.md
```

---

> **Maintenance**: Run `codebase-knowledge-graph` in incremental mode after substantial changesets.
> Watch for: new table features, new DML commands, kernel API additions, new `build.sbt` modules.
