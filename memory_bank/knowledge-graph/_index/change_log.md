---
title: "Knowledge Graph — Change Log"
tags: [index, change-log]
layer: L1
last_updated: 2026-03-02
related:
  - "[[../overview/executive_summary]]"
  - "[[file_map]]"
---

# Knowledge Graph — Change Log

#index #change-log

---

## 2026-03-02 — Full Re-generation (Depth-3)

**Scope**: Full re-generation of the Delta Lake OSS knowledge graph at `/Users/vishnuc/OSS/delta.git/main`. Previous KG (2026-03-01) was torn down and rebuilt from scratch with greater depth and coverage.

**Pipeline run**: Full generation, depth-3, all gap areas covered.

**Modules analyzed**: All 23 SBT sub-projects plus protocol_rfcs and examples:
- `build.sbt` (1710 lines) — complete module and dependency map
- `PROTOCOL.md` (194 KB) — Delta Transaction Log Protocol specification
- `kernel/kernel-api/` — full depth-3 (public API + engine SPI + internals)
- `kernel/kernel-defaults/` — DefaultEngine implementation
- `kernel/unitycatalog/` — UC commit coordinator (NEW: was a gap)
- `kernel/kernel-benchmarks/` — JMH benchmarks (NEW)
- `spark/src/main/scala/org/apache/spark/sql/delta/` — full depth-3 across 13 components
- `spark/src/main/scala/org/apache/spark/sql/delta/commands/` — dedicated deep-dive doc (NEW)
- `spark/src/main/scala/org/apache/spark/sql/delta/streaming/` — streaming internals (NEW: was a gap)
- `spark/src/main/scala/org/apache/spark/sql/delta/catalog/` — DeltaCatalog V2 (NEW: was a gap)
- `spark/src/main/scala/org/apache/spark/sql/delta/uniform/` — UniForm helpers (NEW)
- `spark/src/main/scala/org/apache/spark/sql/delta/serverSidePlanning/` — UC SSP (NEW)
- `spark/v2/` — delta-spark-v2 kernel-backed reader (explored in depth)
- `spark-unified/` — published facade
- `storage/` — LogStore + CommitCoordinator + UCCommitCoordinator (expanded)
- `storage-s3-dynamodb/` — DynamoDB multi-cluster store (NEW: was a gap)
- `sharing/` — Delta Sharing connector (expanded)
- `spark-connect/` — proto schema + client + server (expanded)
- `flink/` — Flink Table API connector (corrected: NOT DataStream API)
- `iceberg/` — UniForm Iceberg conversion (NEW: was a gap)
- `hudi/` — UniForm Hudi conversion (NEW: was a gap)
- `python/` — Python DeltaTable API (expanded)
- `contribs/` — Community LogStore contributions (NEW: was a gap)
- `benchmarks/` — Spark-level performance benchmarks (NEW: was a gap)
- `protocol_rfcs/` — 13 RFCs documented (NEW: was a gap)
- `kernel/examples/kernel-examples/` — Maven examples (NEW)

**Documents created** (35 total):

| Document | Layer | Status |
|---|---|---|
| `overview/executive_summary.md` | L1 | NEW |
| `architecture/system_map.md` | L2 | NEW |
| `architecture/module_dependencies.md` | L2 | NEW |
| `cross_cutting/data_models.md` | L2 | NEW |
| `cross_cutting/interfaces_idl.md` | L2 | NEW |
| `cross_cutting/shared_utilities.md` | L2 | NEW |
| `modules/kernel.md` | L3 | REPLACED (expanded) |
| `modules/spark.md` | L3 | REPLACED (expanded, full depth-3) |
| `modules/spark/commands.md` | L3 | NEW (dedicated DML deep-dive) |
| `modules/storage.md` | L3 | REPLACED (expanded, UC coord added) |
| `modules/spark-connect.md` | L3 | REPLACED (expanded) |
| `modules/sharing.md` | L3 | REPLACED (expanded) |
| `modules/python.md` | L3 | REPLACED (expanded, Connect parity table) |
| `modules/connectors.md` | L3 | REPLACED (contribs documented) |
| `modules/connectors/flink.md` | L3 | NEW (corrected: Table API, not DataStream) |
| `modules/connectors/golden-tables.md` | L4 | NEW |
| `modules/connectors/uniform-iceberg.md` | L3 | NEW |
| `modules/connectors/uniform-hudi.md` | L3 | NEW |
| `protocol/transaction_log.md` | L2 | REPLACED (from PROTOCOL.md) |
| `protocol/actions.md` | L2 | REPLACED (from PROTOCOL.md) |
| `protocol/checkpoints.md` | L2 | REPLACED (from PROTOCOL.md) |
| `protocol/table_features.md` | L2 | REPLACED (25+ features) |
| `protocol/rfcs.md` | L2 | NEW (13 RFCs) |
| `dev/build_system.md` | L2 | REPLACED (benchmarks added) |
| `_index/module_manifest.md` | L1 | NEW (full re-generation, all fixes applied) |
| `_index/file_map.md` | L1 | Retained |
| `_index/tag_index.md` | L1 | Retained (needs update) |

**Pipeline artifacts** (review/adjudication docs):
- `_index/decomposition_review_01.md`
- `_index/adjudication_decompose_01.md`
- `_index/detail_review_batch1.md`
- `_index/detail_review_batch2.md`
- `_index/arch_review_01.md`
- `_index/adjudication_arch_01.md`

**Corrections applied** (from review + adjudication pipeline):
- M1: Spark version corrected (4.0.1 + 4.1.0 only, not 3.x)
- M2-M3: Kernel-defaults non-existent key_files corrected
- M4: Flink module description corrected (Table API, not DataStream API)
- M5: SBT diagram arrow direction clarified
- m1-m7: Minor manifest fixes (path corrections, missing deps, duplicate entries)
- Arch-1: Fabricated `CommitCoordinatorClientHandler` removed from Engine SPI docs
- Arch-2: Fabricated `SparkEngine` class removed from all docs
- Arch-3: UniForm Iceberg cross-Spark support table corrected (disabled for Spark 4.1.0)
- Arch-4: delta-connect modules correctly marked as published Maven artifacts
- Arch-5: Fabricated `LogStoreBasedCommitCoordinatorClient` removed
- Detail-1: Hudi swallowed HoodieException count corrected (2→3)

**Known gaps remaining**:
- Spark `test/` suite structure not documented
- `protocol_rfcs/accepted/` individual deep-dives not done
- Flink `DeltaCatalog`/`DeltaTable` are internal SPI interfaces — extent of Flink Table API integration unclear (flagged in flink.md)

---

## Stale Documentation Monitor

> [!NOTE] How to Keep This KG Up To Date
> Run the `codebase-knowledge-graph` agent in **incremental mode** after substantial changesets.
>
> Key things to watch for:
> - New table features → `protocol/table_features.md`
> - New action types → `protocol/actions.md`
> - Protocol changes in `PROTOCOL.md` → `protocol/transaction_log.md`
> - New module additions to `build.sbt` → update manifest + add module doc
> - Kernel API additions → `modules/kernel.md`
> - New DML commands → `modules/spark/commands.md`

---

## 2026-03-01 — Initial Indexing (Superseded)

_This run was superseded by the 2026-03-02 full re-generation. The initial run covered 24 documents with known gaps. All gaps have been addressed in the 2026-03-02 run._
