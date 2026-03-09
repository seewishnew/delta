# Active Context — Delta Lake OSS

## Last Updated
2026-03-02

## Current Task: Knowledge Graph Full Re-generation ✅ COMPLETE

### Summary
Full re-generation of the Delta Lake OSS knowledge graph at `memory_bank/knowledge-graph/`. 35 documents at depth-3, covering all modules including previously-documented gaps. Full review and adjudication pipeline was run with 12+ corrections applied.

### Knowledge Graph Location
`/Users/vishnuc/OSS/delta.git/main/memory_bank/knowledge-graph/`

### Entry Point
`memory_bank/knowledge-graph/README.md`

---

### What Was Done (2026-03-02 Full Re-generation)

**Phase 0 — Orientation**: Confirmed full re-generation mode, all gaps covered, depth-3.

**Phase 1 — Decomposition**: 
- `codebase-decomposer` produced full manifest with 23 SBT sub-projects + 5 depth-3 component breakdowns
- `kg-decomposition-reviewer` found 4 MAJOR + 5 MINOR issues (Spark version claims, non-existent key_files, Flink module description)
- `kg-feedback-verifier` confirmed all 12 findings, resolved 2 escalations
- All fixes applied (Spark 3.x removed, key_files corrected, Flink description corrected, diagram captions fixed)

**Phase 2 — Explorer Loop** (14 distinct KG doc targets, run in 4 parallel batches):
- `modules/storage.md` — LogStore + CommitCoordinator + UCCommitCoordinator + DynamoDB
- `modules/connectors/golden-tables.md` — 130+ test fixture tables
- `dev/build_system.md` — SBT, cross-Spark build, CI/CD, benchmarks
- `protocol/rfcs.md` — 13 RFCs (5 accepted, 7 in-flight, 1 rejected)
- `modules/kernel.md` — full depth-3 (API, engine SPI, protocol impl, DefaultEngine, UC, benchmarks, examples)
- `modules/connectors/flink.md` — Flink Table API connector (corrected: DeltaCatalog/DeltaTable are internal SPI)
- `modules/connectors/uniform-iceberg.md` — async converter, IcebergCompatV1/V2, shaded JAR, SSP
- `modules/connectors/uniform-hudi.md` — REPLACE_COMMIT_ACTION, Spark 4.0.1 only, 3 swallowed exceptions
- `modules/spark.md` — full depth-3 (13 components: core, actions, streaming, catalog, coordinatedcommits, files, schema, skipping, uniform, SSP, sql-parser, public-api + v2 + unified)
- `modules/spark/commands.md` — dedicated DML deep-dive (17 commands, 3 Mermaid diagrams)
- `modules/spark-connect.md` — proto3 IDL, client planner, server plugin
- `modules/sharing.md` — virtual FS, BlockManager-backed log, DeltaSource wrapper
- `modules/python.md` — classic + Connect dual-mode, full parity table, plan serialization
- `modules/connectors.md` — IBM COS + Oracle Cloud community LogStore impls
- `protocol/transaction_log.md` — full protocol spec from PROTOCOL.md
- `protocol/actions.md` — all 10 action types
- `protocol/checkpoints.md` — V1/V2 checkpoint formats
- `protocol/table_features.md` — 25+ features

**Phase 2 Reviews**: 
- `kg-detail-reviewer` ran on all 13 explorer outputs in 2 batches
- No CRITICAL issues; 1 MAJOR (hudi: 3 swallowed exceptions, not 2) fixed directly
- 3 MINOR issues noted (line number offsets, one field clarification)

**Phase 3 — Architecture**:
- `codebase-architect` produced: `architecture/system_map.md`, `architecture/module_dependencies.md`, `overview/executive_summary.md` (stub), `cross_cutting/data_models.md`, `cross_cutting/interfaces_idl.md`, `cross_cutting/shared_utilities.md`
- `kg-arch-reviewer` found 4 MAJOR + 3 MINOR issues (fabricated classes, wrong publish status, wrong UniForm cross-Spark support)
- `kg-feedback-verifier` confirmed all 7 findings
- All fixes applied: removed SparkEngine, CommitCoordinatorClientHandler, LogStoreBasedCommitCoordinatorClient; fixed UniForm Spark 4.1.0 table; fixed connect module publish status

**Phase 4 — Executive Summary**:
- Completed executive summary (35-doc TOC, tech stack, architecture pattern, known gaps, maintenance guide)
- Updated `change_log.md` with full pipeline history
- Created `README.md` entry point

---

### Documents Created (35 total)

**L1**: `overview/executive_summary.md`, `_index/module_manifest.md`  
**L2**: `architecture/system_map.md`, `architecture/module_dependencies.md`, `cross_cutting/data_models.md`, `cross_cutting/interfaces_idl.md`, `cross_cutting/shared_utilities.md`, `protocol/transaction_log.md`, `protocol/actions.md`, `protocol/checkpoints.md`, `protocol/table_features.md`, `protocol/rfcs.md`, `dev/build_system.md`  
**L3**: `modules/kernel.md`, `modules/spark.md`, `modules/spark/commands.md`, `modules/storage.md`, `modules/spark-connect.md`, `modules/sharing.md`, `modules/python.md`, `modules/connectors.md`, `modules/connectors/flink.md`, `modules/connectors/uniform-iceberg.md`, `modules/connectors/uniform-hudi.md`  
**L4**: `modules/connectors/golden-tables.md`

---

### Key Architectural Findings (from this run)

1. **Spark versions**: Only 4.0.1 and 4.1.0 are supported. Spark 3.x was dropped. 4.2.0-SNAPSHOT is defined but not in `ALL_SPECS`.
2. **Flink connector**: `DeltaCatalog`/`DeltaTable` are internal SPI **interfaces**, not Flink framework implementations. The module's 14 files provide foundation layer and kernel utilities.
3. **delta-spark-v2 uses `DefaultEngine`**: There is no `SparkEngine` class. `SparkTable.java` and `SparkScan.java` call `DefaultEngine.create(hadoopConf)` directly from `kernel-defaults`.
4. **UniForm disabled for 4.1.0**: Both Iceberg AND Hudi UniForm are disabled for the default build target (Spark 4.1.0). Only Spark 4.0.1 supports both UniForm formats.
5. **3 swallowed HoodieExceptions**: Not 2 as previously documented.
6. **UC schema conversion unimplemented**: `UCTokenBasedRestClient.java:292-294` explicitly notes that Delta schema → UC `ColumnInfo` conversion is not implemented.
7. **delta-connect modules ARE published**: All 3 connect modules have `releaseSettings` with `publishArtifact := true`.

---

### Known Gaps Remaining

- Spark `test/` suite structure not documented
- `protocol_rfcs/accepted/` individual RFC deep-dives at full depth
- Flink `DeltaCatalog`/`DeltaTable` SPI extent of Flink Table API integration needs more investigation
- `dev/` tooling scripts in full detail
- CI/CD individual step details

## Previous Tasks
- **2026-03-01**: Initial knowledge graph creation (24 docs, known gaps — superseded by 2026-03-02 full re-generation)
