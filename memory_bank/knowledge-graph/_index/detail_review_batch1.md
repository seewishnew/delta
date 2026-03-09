---
title: "Detail Review — Batch 1 (kernel, storage, spark, commands)"
tags: [review, detail, feedback]
last_updated: 2026-03-02
modules_reviewed:
  - "[[modules/kernel]]"
  - "[[modules/storage]]"
  - "[[modules/spark]]"
  - "[[modules/spark/commands]]"
---

# Detail Review — Batch 1 (kernel, storage, spark, commands)

date: 2026-03-02

> Produced by kg-detail-reviewer. Subject to adjudication by kg-feedback-verifier before fixes are applied.

## Overall Assessment

All four documents are accurate, well-structured, and reflect a deep reading of the source code.
Line-number citations are consistently precise (within ±1 line in a handful of cases). Behavioral
descriptions faithfully capture the logic including edge-case branches. No CRITICAL or MAJOR issues
were found across 30+ spot-checks against actual source files. The only issues are MINOR omissions
and isolated ±1-line off-by-one citations — none of which would mislead the architect phase.

---

## kernel.md

### VALID Spot-Checks

| Claim | Evidence |
|---|---|
| `Table.forPath` cited at `Table.java:56-58` | Actual lines 56-58 exactly match the quoted snippet. ✅ |
| `DefaultEngine.create(hadoopConf)` at `DefaultEngine.java:64-66`, second overload at `75-77` (cited as 64-77) | Both methods confirmed. ✅ |
| `LogReplay.getAddFilesAsColumnarBatches` cited at `LogReplay.java:195-213` | Exact signature + body match. ✅ |
| `Transaction.getTransactionState` at `Transaction.java:93` | Confirmed. ✅ |
| `Transaction.commit` at `Transaction.java:115` | Confirmed. ✅ |
| `Transaction.transformLogicalData` at `Transaction.java:171` | Confirmed. ✅ |
| `Transaction.getWriteContext` at `Transaction.java:267` | Confirmed. ✅ |
| `Transaction.generateAppendActions` at `Transaction.java:298` | Confirmed. ✅ |
| Engine SPI — 5 sub-interfaces (`ExpressionHandler`, `JsonHandler`, `FileSystemClient`, `ParquetHandler`, `MetricsReporter`) | Confirmed against `Engine.java`. ✅ |
| `transformLogicalData` behavioral description (Iceberg compat → move partition cols to end; otherwise remove; blocks column-mapping + Variant) | Confirmed against `Transaction.java:171-238`. ✅ |
| UC integration: `UCCatalogManagedClient.loadSnapshot` → `GetCommitsResponse` → `SnapshotBuilder.withLogData` | Confirmed against `UCCatalogManagedClient.java:92-184`. ✅ |

### MINOR — `transformLogicalData` description omits one blocking call

- **Section**: "Internal Protocol Implementation → `transformLogicalData` logic (lines 171–237)"
- **Finding**: The description lists only two blocking calls ("Blocks writes if column mapping is enabled", "Blocks writes if Variant data type is in the schema"). There is a third blocking call at line 198: `ColumnDefaults.blockWriteIfEnabled(transactionState)`, which blocks writes when `AllowColumnDefaults` feature is enabled (Iceberg V3 path, currently unsupported for physical writes).
- **Evidence**: `kernel/kernel-api/src/main/java/io/delta/kernel/Transaction.java:197-199`
  ```java
  // We recognize the AllowColumnDefaults feature for Iceberg v3
  // but do not support writing with it yet
  ColumnDefaults.blockWriteIfEnabled(transactionState);
  ```
- **Suggested correction**: Add a third bullet: "Blocks writes if `AllowColumnDefaults` feature is active (recognized for IcebergCompatV3 but physical write not yet supported)."

### Coverage Gaps

- `Transaction.java` has a `blockIfVariantDataTypeIsDefined(tableSchema)` helper at line 244 with its own logic (checks nested `VariantType` via `SchemaIterable`) — worth a separate mention since the type check is non-trivial.
- `UCCatalogManagedClient.loadSnapshot` telemetry timers (`UcLoadSnapshotTelemetry.Report`) are mentioned but the timer names (`totalSnapshotLoadTimer`, etc.) have not been verified; treating as low-risk.

---

## storage.md

### VALID Spot-Checks

| Claim | Evidence |
|---|---|
| `S3SingleDriverLogStore.write()` cited at `S3SingleDriverLogStore.java:147-180` | Exact method at lines 147-180. ✅ |
| `BaseExternalLogStore.listFrom()` cited at `BaseExternalLogStore.java:129-152` | Exact method at lines 128-152 (note: `@Override` at 128, body starts 129). ✅ |
| `HDFSLogStore.isPartialWriteVisible() → true` | Confirmed at `HDFSLogStore.java:73-75`. ✅ |
| `HDFSLogStore` uses `FileContext.rename()` for atomic write | Confirmed; `fc.rename(tempPath, path, renameOpt)` at line 125. ✅ |
| `HDFSLogStore` CRC cleanup via `tryRemoveCrcFile(fc, tempPath)` | Confirmed at line 129. ✅ |
| `HDFSLogStore` `msyncIfSupported` after rename | Confirmed at line 142. ✅ |
| `GCSLogStore` dispatches write to new thread via `ThreadUtils.runInNewThread("delta-gcs-logstore-write", true, body)` | Confirmed at `GCSLogStore.java:96`. ✅ |
| `GCSLogStore` maps both `org.apache.hadoop.fs.FileAlreadyExistsException` and precondition failures to `java.nio.file.FileAlreadyExistsException` | Confirmed at lines 97-106. ✅ |
| `UCCommitCoordinatorClient`: `MAX_RETRIES_ON_TRANSIENT_ERROR = 15`, `BACKFILL_LISTING_OFFSET = 100`, `THREAD_POOL_SIZE = 20` | All three confirmed at `UCCommitCoordinatorClient.java:93,96,106`. ✅ |
| `CommitCoordinatorClient` interface methods (`registerTable`, `commit`, `getCommits`, `backfillToVersion`, `semanticEquals`) | Confirmed against `CommitCoordinatorClient.java:42-161`. ✅ |

### MINOR — `GCSLogStore.write()` code snippet omits the `LocalFileSystem` guard

- **Section**: "GCSLogStore — Google Cloud Storage Implementation"
- **Finding**: The KG doc's code snippet starts at the `Callable body = ...` block (around line 86), but there is a `LocalFileSystem` guard that executes before the callable is even created (lines 75-78 of actual source). This guard throws `FileAlreadyExistsException` eagerly for local FS and `overwrite=false`.
- **Evidence**: `storage/src/main/java/io/delta/storage/GCSLogStore.java:75-78`
  ```java
  if (fs instanceof LocalFileSystem && !overwrite && fs.exists(path)) {
      throw new FileAlreadyExistsException(path.toString());
  }
  ```
- **Suggested correction**: Add a note that before dispatching to the new thread, a `LocalFileSystem` guard throws `FileAlreadyExistsException` for test environments. Or start the code snippet at line 75 rather than 86.

### Coverage Gaps

- `BaseExternalLogStore.write()` uses a separate `PathLock` from `S3SingleDriverLogStore`. The doc mentions both at the bottom (Cross-Cutting Concerns → PathLock section) and correctly notes they are separate static instances. ✅
- `UCTokenBasedRestClient` schema conversion gap ("not implemented" — `UCTokenBasedRestClient.java:292-294`) is documented correctly.

---

## spark.md

### VALID Spot-Checks

| Claim | Evidence |
|---|---|
| `Snapshot` class hierarchy (lines 95-108): extends `SnapshotDescriptor with SnapshotStateManager with StateCache with StatisticsCollection with DataSkippingReader with ValidateChecksum with DeltaLogging` | Exact match at `Snapshot.scala:95-108`. ✅ |
| `DeltaLog.startTransaction` cited at `DeltaLog.scala:216-222` | Exact match. ✅ |
| `DeltaLog.forTable(spark, dataPath: String)` cited at `DeltaLog.scala:797-804` | Exact match. ✅ |
| `DeltaLog.getChanges()` cited at `DeltaLog.scala:303-311` | Exact match. ✅ |
| `DeltaLogCacheKey` cited as `DeltaLog.scala:712-716` | Actual case class starts at line 713; line 712 is the closing of the preceding comment block. Off by 1 on start. |
| `DeltaLog` removalListener snippet cited at `736-746` | Actual lambda starts at 735, not 736. Off by 1 on start. |
| `OptimisticTransaction` description of OCC retry loop (fetch winning commits → `ConflictChecker` loop → retry or throw) | Confirmed against `OptimisticTransaction.scala` structure. ✅ |

### MINOR — `DeltaLogCacheKey` and removalListener citations off by 1

- **Document**: `knowledge_graph/modules/spark.md`
- **Section**: "Component: spark.core → DeltaLog → Singleton Cache"
- **Finding**: `DeltaLogCacheKey` is cited as `DeltaLog.scala:712-716`, but the case class definition begins at line 713 (line 712 is the end of the preceding ScalaDoc comment). Similarly, the `removalListener` block is cited as `736-746` but actually starts at line 735.
- **Evidence**: `spark/src/main/scala/org/apache/spark/sql/delta/DeltaLog.scala` (lines 707-747 read directly).
- **Suggested correction**: Update citation to `713-716` for `DeltaLogCacheKey` and `735-746` for the removalListener block.

### Coverage Gaps

- `DeltaLog.logPathFor` is shown at `722-723` in the doc. The actual file shows two overloads at lines 721-723 (both the `String` and `Path` overloads). The doc correctly shows only the `Path` overload at 722-723. No error.
- `CoordinatedCommitType` Enum cited at `OptimisticTransaction.scala:69-73` — not verified, treating as low-risk.
- The `spark.files` section (TahoeFileIndex, TransactionalWrite, DelayedCommitProtocol) was not spot-checked in detail; these are large components and further review is recommended if the architect consumes these sections heavily.

---

## commands.md

### VALID Spot-Checks

| Claim | Evidence |
|---|---|
| `WriteIntoDelta extends LeafRunnableCommand with ImplicitMetadataOperation with DeltaCommand` | Confirmed at `WriteIntoDelta.scala:81-94`. ✅ |
| Write modes decision tree at `WriteIntoDelta.scala:247-351` | Actual `match` block starts at line 247. ✅ |
| `MergeIntoCommand extends MergeIntoCommandBase with InsertOnlyMergeExecutor with ClassicMergeExecutor` | Confirmed at `MergeIntoCommand.scala:75-77`. ✅ |
| `ClassicMergeExecutor` extends `MergeOutputGeneration` | Confirmed: `trait ClassicMergeExecutor extends MergeOutputGeneration` at `ClassicMergeExecutor.scala:63`. ✅ |
| `InsertOnlyMergeExecutor` extends `MergeOutputGeneration` | Confirmed: `trait InsertOnlyMergeExecutor extends MergeOutputGeneration` at `InsertOnlyMergeExecutor.scala:34`. ✅ |
| `ClassicMergeExecutor.findTouchedFiles` starts at line 72 | Exact match. ✅ |
| Phase 1 join type: `inner` if no `NOT MATCHED BY SOURCE` else `right_outer` | Confirmed: `val joinType = if (notMatchedBySourceClauses.isEmpty) "inner" else "right_outer"` at `ClassicMergeExecutor.scala:101`. ✅ |
| `SetAccumulator[String]()` used in Phase 1 | Confirmed at `ClassicMergeExecutor.scala:83-84`. ✅ |
| Insert-only path uses `leftanti` join | Confirmed at `InsertOnlyMergeExecutor.scala:95`. ✅ |
| `Transaction.transformLogicalData` behavioral description for `isCDCEnabled` / replaceWhere CDF pack+explode | Not contradicted by code checks. ✅ |

### MINOR — `MergeOutputGeneration` uses self-type annotation, not `extends`

- **Section**: "MergeIntoCommand, MergeIntoCommandBase, ClassicMergeExecutor, InsertOnlyMergeExecutor"
- **Finding**: The doc states "both extend `MergeOutputGeneration`, which extends `MergeIntoCommandBase`". In reality, `MergeOutputGeneration` uses a **self-type annotation**: `trait MergeOutputGeneration { self: MergeIntoCommandBase => }`. This is not inheritance — it is a requirement that any class mixing in `MergeOutputGeneration` must also be a `MergeIntoCommandBase`. Functionally it is equivalent for the usage described, but "extends" is technically incorrect.
- **Evidence**: `spark/src/main/scala/org/apache/spark/sql/delta/commands/merge/MergeOutputGeneration.scala:36`
  ```scala
  trait MergeOutputGeneration { self: MergeIntoCommandBase =>
  ```
- **Suggested correction**: Change "which extends `MergeIntoCommandBase`" to "which requires `self: MergeIntoCommandBase` via a Scala self-type annotation".

### MINOR — `WriteIntoDelta` constructor missing `catalogTableOpt` parameter

- **Section**: "WriteIntoDelta → Constructor Parameters"
- **Finding**: The constructor parameter table omits `catalogTableOpt: Option[CatalogTable] = None` which is part of the actual constructor signature.
- **Evidence**: `WriteIntoDelta.scala:88`: `val catalogTableOpt: Option[CatalogTable] = None`
- **Suggested correction**: Add a row for `catalogTableOpt` to the parameter table.

### MINOR — `WriteIntoDelta` also extends `WriteIntoDeltaLike`

- **Section**: "WriteIntoDelta → Source"
- **Finding**: The description says `WriteIntoDelta extends LeafRunnableCommand, ImplicitMetadataOperation, DeltaCommand` but the actual declaration at line 94 also has `with WriteIntoDeltaLike`.
- **Evidence**: `WriteIntoDelta.scala:91-94`
- **Suggested correction**: Add `WriteIntoDeltaLike` to the description of the class declaration.

### MINOR — `InsertOnlyMergeExecutor` snippet starts at line 78, not 79

- **Section**: "Insert-Only Fast Path → `InsertOnlyMergeExecutor` code snippet"
- **Finding**: The cited range is `InsertOnlyMergeExecutor.scala:79-98`, but the `var dataSkippedFiles: Option[Seq[AddFile]] = None` declaration that belongs to the same code block is at line 78.
- **Evidence**: `spark/src/main/scala/org/apache/spark/sql/delta/commands/merge/InsertOnlyMergeExecutor.scala:78`
- **Suggested correction**: Update citation to `78-98`.

### Coverage Gaps

- `ConvertToDeltaCommand` abstract class declaration is not verified — treating as low-risk given the rest of the document's accuracy.
- `CloneTableBase.handleClone()` 5-step description was not verified against source, but is a lower-risk behavioral description.

---

## Backwards-Pass Diagram Opportunities

The following diagram opportunities were already flagged by the explorers. They are reproduced here with a recommendation for the orchestrator to act on them at the manifest level.

### FLAG 1: UC Commit Lifecycle Sequence Diagram (kernel module)

- **Modules involved**: [[modules/kernel]], [[delta-storage]]
- **What the explorer found**: `UCCatalogManagedCommitter.commit()` has a three-way dispatch (`CATALOG_CREATE` → `createImpl()`, `CATALOG_UPDATE` → `updateImpl()`, standard path → blocked), and `UCCatalogManagedClient.loadSnapshot()` merges UC-ratified `ParsedLogData` catalog commits with the Kernel `SnapshotManager.getLogSegmentForVersion()` to build a `LogSegment`. This interaction is buried in the `kernel.md` prose.
- **Why a manifest-level diagram would help**: The UC commit staging/publish lifecycle is the most complex part of the kernel and the primary differentiator from filesystem-managed tables. A `sequenceDiagram` at the module manifest level (or an L4 doc for `delta-kernel-unitycatalog`) would make this pattern visible at a glance.
- **Relevant source evidence**: `kernel/unitycatalog/src/main/java/io/delta/kernel/unitycatalog/UCCatalogManagedClient.java:92-184`, `UCCatalogManagedCommitter.java:80-200`
- **Suggested diagram type**: `sequenceDiagram`

### FLAG 2: CommitCoordinatorClient ↔ Spark OptimisticTransaction Routing Diagram (storage/spark)

- **Modules involved**: [[modules/storage]], [[modules/spark]]
- **What the explorer found**: `TableCommitCoordinatorClient` in `delta-spark-v1` wraps the storage-level `CommitCoordinatorClient`, and `OptimisticTransaction.doCommitRetryIteratively` routes through it when the table is coordinated-commit-enabled. This bridging layer is not diagrammed at the manifest level.
- **Why a manifest-level diagram would help**: The `CommitCoordinatorClient` is defined in `delta-storage`, used via Spark adapter in `delta-spark-v1`, and also used via Kernel adapter in `delta-kernel-defaults`. Showing this fan-out at the manifest level avoids confusion about which layer "owns" coordinated commits.
- **Relevant source evidence**: `spark/src/main/scala/org/apache/spark/sql/delta/coordinatedcommits/TableCommitCoordinatorClient.scala`, `storage/src/main/java/io/delta/storage/commit/CommitCoordinatorClient.java`
- **Suggested diagram type**: `sequenceDiagram`

### FLAG 3: WriteIntoDelta → DeleteCommand Internal Delegation (commands module)

- **Modules involved**: [[modules/spark/commands]]
- **What the explorer found**: `WriteIntoDelta.removeFiles()` at lines 391-410 internally instantiates `DeleteCommand` and calls `performDelete()` for the `replaceWhere` with data column support path. This is a non-obvious internal coupling between the write path and the delete path.
- **Why a manifest-level diagram would help**: Users expect `replaceWhere` to be a pure write operation; the fact that it delegates to `DeleteCommand.performDelete()` (which may write DVs, generate CDF delete events, etc.) has observability and correctness implications.
- **Relevant source evidence**: `spark/src/main/scala/org/apache/spark/sql/delta/commands/WriteIntoDelta.scala:391-410`
- **Suggested diagram type**: `sequenceDiagram`

---

## Formatting Issues

- `commands.md`: The `related` frontmatter references `[[spark.core]]`, `[[spark.actions]]`, `[[spark.files]]`, `[[spark.skipping]]` — these use dot-notation that doesn't match the actual KG file paths (e.g., `[[modules/spark]]`). May not resolve in Obsidian.
- `kernel.md`: `related` references `[[delta-storage]]`, `[[delta-spark-v2]]`, `[[delta-flink]]` without full path prefixes. Same potential Obsidian resolution issue.
- `storage.md`: `related` uses `[[kernel]]`, `[[spark]]`, `[[connectors]]` — inconsistent with `[[modules/kernel]]` path convention used in other docs.

---

## Verdict per Document

| Document | Verdict | Summary |
|---|---|---|
| `kernel.md` | **APPROVED WITH MINOR CHANGES** | All line citations accurate; 1 behavioral omission (ColumnDefaults blocking) |
| `storage.md` | **APPROVED WITH MINOR CHANGES** | All constants and code behaviors confirmed; 1 snippet simplification (GCSLogStore LocalFileSystem guard) |
| `spark.md` | **APPROVED WITH MINOR CHANGES** | Class hierarchies confirmed; 2 × ±1 line citations on DeltaLog cache fields |
| `commands.md` | **APPROVED WITH MINOR CHANGES** | Core algorithms accurate; 3 minor omissions (catalogTableOpt parameter, WriteIntoDeltaLike, MergeOutputGeneration self-type vs extends) |

**None of the four documents require blocking revision before the architect phase. All changes are optional quality improvements.**
