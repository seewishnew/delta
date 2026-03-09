---
title: "Detail Review — Batch 2 (connectors, protocol, connect, sharing)"
tags: [review, detail, feedback]
last_updated: 2026-03-02
modules_reviewed:
  - "[[connectors/flink]]"
  - "[[connectors/uniform-iceberg]]"
  - "[[connectors/uniform-hudi]]"
  - "[[protocol/transaction_log]]"
  - "[[protocol/actions]]"
  - "[[protocol/checkpoints]]"
  - "[[protocol/table_features]]"
  - "[[spark-connect]]"
  - "[[sharing]]"
---

# Detail Review — Batch 2 (connectors, protocol, connect, sharing)

> Produced by kg-detail-reviewer on 2026-03-02. Subject to adjudication by kg-feedback-verifier before fixes are applied.

---

## Overall Assessment

Nine documents were reviewed. The documentation is overwhelmingly accurate — all key architectural claims, class/method names, proto schemas, and protocol semantics were verified against source. The explorer did careful work and the DeltaCatalog/DeltaTable SPI claim is specifically confirmed. There are **one MAJOR finding** (a factual omission in uniform-hudi.md), **three MINOR findings** (two line-number citation edge cases and one misleading Spark version statement), and several NITs. Protocol documents are protocol-spec-accurate. Spark Connect and Delta Sharing docs had no factual errors found.

---

## flink.md

### Findings

#### [VERIFIED] DeltaCatalog/DeltaTable SPI Claim — ACCURATE

The orchestrator's verification task has been confirmed. `DeltaCatalog.java` (lines 42–115) is:

```java
public interface DeltaCatalog extends Serializable {
    default void open() {}
    TableDescriptor getTable(String tableId);
    void createTable(...);
    Map<String, String> getCredentials(String uuid);
    class TableDescriptor { ... }
}
```

It does **not** implement `org.apache.flink.table.catalog.Catalog` or any other Flink framework interface. `DeltaTable.java` (line 49) is likewise:

```java
public interface DeltaTable extends Serializable, AutoCloseable {
```

No Flink `DynamicTableSource` or `DynamicTableSink` inheritance is present. The `> [!WARNING]` block in `flink.md` accurately states that these are custom internal SPI interfaces. **No correction needed.**

---

#### [MINOR] Sidecar Merge Code Citation Range Off by 14 Lines

- **Document**: `memory_bank/knowledge-graph/modules/connectors/flink.md`
- **Section**: "Component: CheckpointWriter — Sidecar Merge"
- **Finding**: The code block cited as `327:363:flink/src/main/java/io/delta/flink/kernel/CheckpointWriter.java` starts at line 327 which is the **method signature** `private CloseableIterator<FilteredColumnarBatch> sidecarsFromCheckpoint(Path checkpointPath)`, not the `if (sidecarMergeThreshold > 0 ...)` merge condition. The prose description says "When `sidecarMergeThreshold > 0`…" implying the code block starts at the merge guard, but it actually opens with the wider method body. The merge guard starts at line 341.
- **Evidence**: `flink/src/main/java/io/delta/flink/kernel/CheckpointWriter.java:327` reads `private CloseableIterator<FilteredColumnarBatch> sidecarsFromCheckpoint(Path checkpointPath)`; the `if (sidecarMergeThreshold > 0 && lastSidecarCount >= sidecarMergeThreshold - 1)` guard is at line 341.
- **Suggested correction**: Update citation range to `341:363` (or expand context to explain the surrounding method). Alternatively, trim the description to clarify this is within `sidecarsFromCheckpoint()`.

---

#### [VERIFIED] Constructor Preconditions and CheckpointActionRow Schema — ACCURATE

- `CheckpointWriter` constructor at lines 143–154: confirmed. Both `Preconditions.checkArgument()` calls for `CHECKPOINT_V2_RW_FEATURE` and `CATALOG_MANAGED_RW_FEATURE` are exactly as described.
- `CheckpointActionRow.CHECKPOINT_SCHEMA` at lines 33–41: confirmed. The 7-field union schema (`checkpointMetadata`, `metaData`, `protocol`, `txn`, `sidecar`, `domainMetadata`, `add`) matches source exactly.
- SetTransaction aggregation at lines 419–424: confirmed. The `transactionIds.merge(appId, txnVersion, Math::max)` call is accurately cited (within a lambda passed to `ColumnVectorUtils.filter`).

---

### Pass Verdict for flink.md

**APPROVED WITH CHANGES** — accurate and complete; one MINOR citation fix needed.

---

## uniform-iceberg.md

### Findings

#### [VERIFIED] IcebergConverter Core Claims — ACCURATE

All three key citation chains verified against source:

1. `currentConversion` / `standbyConversion` AtomicReference fields at `IcebergConverter.scala:86–89` — confirmed exactly.
2. `tableOp` decision at `IcebergConverter.scala:419–423` — confirmed. The three cases `(Some(_), Some(_)) => WRITE_TABLE`, `(Some(_), None) => REPLACE_TABLE`, `(None, None) => CREATE_TABLE` are exact.
3. Serialization guard at `IcebergConversionTransaction.scala:428–432` — confirmed. The `ConcurrentModificationException` message and logic match.

---

#### [MINOR] CREATE_TABLE Field ID Override Snippet Misleads on txn.commitTransaction() Scope

- **Document**: `memory_bank/knowledge-graph/modules/connectors/uniform-iceberg.md`
- **Section**: "Component: IcebergConversionTransaction — CREATE_TABLE Field ID Override"
- **Finding**: The code snippet cited as `438-447` shows `txn.commitTransaction()` at the end of the block with `...` truncation in the middle. This implies `txn.commitTransaction()` is inside the `if (tableOp == CREATE_TABLE)` block. In reality, `txn.commitTransaction()` is at line **448**, unconditionally called **outside** the `if` block for all table operations.
- **Evidence**: `IcebergConversionTransaction.scala:438–447` is the `if (tableOp == CREATE_TABLE)` block with `metadataUpdates.add(new AddSchema(...))` and `new AddPartitionSpec(...)`. Line 448: `txn.commitTransaction()` is the first line after the closing `}` of the `if` block.
- **Suggested correction**: Adjust the truncated snippet to show the `if` block closing `}` at line 447 before `txn.commitTransaction()`, or update the caption to clarify that `txn.commitTransaction()` is unconditional (not only for CREATE_TABLE).

---

#### [VERIFIED] Iceberg Shaded Version 1.10.1 — ACCURATE

`build.sbt:1232`: `val icebergShadedVersion = "1.10.1"`. Both `iceberg-core` and `iceberg-hive-metastore` use this version. The runtime `iceberg-spark-runtime` is a different artifact at `1.10.0` (line 1161) — the doc correctly documents shaded as 1.10.1 without specifying the runtime version, so there is no error.

---

### Pass Verdict for uniform-iceberg.md

**APPROVED WITH CHANGES** — accurate; one MINOR snippet fix recommended for clarity.

---

## uniform-hudi.md

### Findings

#### [MAJOR] Three HoodieException Messages Caught — Doc Says "Two"

- **Document**: `memory_bank/knowledge-graph/modules/connectors/uniform-hudi.md`
- **Section**: "Component: HudiConversionTransaction — Error Handling in commit()"
- **Finding**: The doc states: *"Two specific `HoodieException` messages are caught and **swallowed** (logged as INFO, not thrown)"* and lists two messages. The actual source has **three** caught `HoodieException` messages:
  1. `"Failed to update metadata"`
  2. `"Error getting all file groups in pending clustering"`
  3. `"Error fetching partition paths from metadata table"` ← **missing from doc**
- **Evidence**: `hudi/src/main/scala/org/apache/spark/sql/delta/hudi/HudiConversionTransaction.scala:161–163`:
  ```scala
  case e: HoodieException if e.getMessage == "Failed to update metadata"
    || e.getMessage == "Error getting all file groups in pending clustering"
    || e.getMessage == "Error fetching partition paths from metadata table" =>
  ```
- **Suggested correction**: Change "Two specific `HoodieException` messages" to "Three specific `HoodieException` messages" and add `"Error fetching partition paths from metadata table"` — metadata table partition path lookup failure during concurrent commits — to the list.

---

#### [MINOR] "4.2.0-SNAPSHOT" Spark Version Reference Misleading

- **Document**: `memory_bank/knowledge-graph/modules/connectors/uniform-hudi.md`
- **Section**: "Purpose" note block
- **Finding**: The doc says Hudi is "disabled for Spark 4.1.0 and the 4.2.0-SNAPSHOT". While `spark42Snapshot` is defined in `CrossSparkVersions.scala` with `supportHudi = false`, it is **not included in `ALL_SPECS`** (the active build matrix): `val ALL_SPECS = Seq(spark40, spark41)`. Saying it is "disabled for 4.2.0-SNAPSHOT" implies it is an active build target for which the flag matters, which it currently is not.
- **Evidence**: `project/CrossSparkVersions.scala`: `val ALL_SPECS = Seq(spark40, spark41)` — `spark42Snapshot` is defined but excluded from the published spec list.
- **Suggested correction**: Change to: "disabled for Spark 4.1.0 (`supportHudi = false`). A `spark42Snapshot` spec exists in `CrossSparkVersions.scala` with the same flag but is not yet in `ALL_SPECS`."

---

#### [VERIFIED] HudiConverter Class Structure and Commit Logic — ACCURATE

- HudiConverter class structure at `HudiConverter.scala:62–77`: confirmed. Class declaration, `currentConversion`, `standbyConversion`, `asyncConverterThreadActive`, `asyncThreadLock` are all present as described.
- `setCommitFileUpdates` at `HudiConversionTransaction.scala:106–130`: confirmed. AddFile → WriteStatus and RemoveFile → `partitionToReplacedFileIds` logic matches.
- `commit()` at `HudiConversionTransaction.scala:138–150`: confirmed. `startCommitWithTime`, `transitionReplaceRequestedToInflight`, `writeClient.commit` with `REPLACE_COMMIT_ACTION` are exactly as described.

---

### Pass Verdict for uniform-hudi.md

**NEEDS REVISION** — MAJOR: missing third swallowed exception message; MINOR: misleading Spark version note.

---

## protocol/transaction_log.md

### Findings

#### [VERIFIED] Catalog-Managed Commit Options — ACCURATE

`PROTOCOL.md:1266–1276` confirms the three options: Option 1 (staged commit to `_staged_commits/` + catalog ratifies), Option 2 (inline commit), Option 3 (PUT-if-absent writes `_delta_log/<v>.json` directly). The doc's description matches these verbatim.

#### [VERIFIED] File Layout, Log Entry Format, Action Reconciliation — ACCURATE

The file type reference table, log file naming (20-digit zero-padded), and action reconciliation rules were all spot-checked against `PROTOCOL.md` and `actions.scala`. No discrepancies found.

---

### Pass Verdict for protocol/transaction_log.md

**APPROVED** — accurate and complete.

---

## protocol/actions.md

### Findings

#### [VERIFIED] Protocol Case Class Line Citation — ACCURATE

Doc says `case class Protocol` is at `actions.scala:168`. Source confirms: `case class Protocol private (` is exactly at line 168.

#### [VERIFIED] SingleAction Wrapper Fields — ACCURATE

The `SingleAction` wrapper listing (txn, add, remove, metaData, protocol, cdc, commitInfo, domainMetadata, checkpointMetadata, sidecar) matches the `actions.scala` source exactly. All JSON field names correctly map to `SingleAction` field names.

#### [VERIFIED] Protocol Constraint: readerFeatures Requires writerFeatures — ACCURATE

The doc says: "If `readerFeatures` is present, `writerFeatures` must also be present." Source at `actions.scala:189–192` confirms: `if (supportsReaderFeatures && !supportsWriterFeatures) throw DeltaErrors.tableFeatureReadRequiresWriteException(...)`. Accurate.

#### [VERIFIED] AddCDCFile `dataChange` Always False — ACCURATE

The doc states `dataChange` for CDC actions is "Always `false` for CDC actions (they record changes, not new data)". The constraint table in `PROTOCOL.md` section for CDC confirms this field must be false. Accurate.

---

### Pass Verdict for protocol/actions.md

**APPROVED** — accurate and complete.

---

## protocol/checkpoints.md

### Findings

#### [VERIFIED] V2 Checkpoint Body Examples — ACCURATE

`PROTOCOL.md:2145–2165` shows identical examples to those in `checkpoints.md` for both "with sidecars" and "without sidecars" V2 checkpoint bodies.

#### [VERIFIED] Kernel Writes V1 Classic Checkpoints — ACCURATE

The doc says "The current Kernel implementation writes **V1 classic checkpoints** (single-file Parquet). V2 checkpoint writing is the responsibility of the Spark connector." This is supported by the Kernel codebase (`Checkpointer.java` uses `FileNames.checkpointFileSingular()`), while V2 writing is in `flink/CheckpointWriter.java` and the Spark `Checkpointer.scala`.

#### [VERIFIED] Sidecar Partial Embedding Prohibited — ACCURATE

"Partial embedding is not allowed: either all file actions are inline, or all are in sidecars." `PROTOCOL.md:2138–2140` confirms: "A V2 spec Checkpoint can either have all the add and remove file actions embedded inside itself or all of them should be in sidecar files. Having partial add and remove file actions in V2 Checkpoint and partial entries in sidecar files is not allowed."

---

### Pass Verdict for protocol/checkpoints.md

**APPROVED** — accurate and complete.

---

## protocol/table_features.md

### Findings

#### [VERIFIED] IcebergCompatV1/V2 Required Features — ACCURATE

`TableFeature.scala:1004`: `IcebergCompatV1TableFeature.requiredFeatures = Set(ColumnMappingTableFeature)`. `TableFeature.scala:1019`: `IcebergCompatV2TableFeature.requiredFeatures = Set(ColumnMappingTableFeature)`. Both match the doc's "Requires: `columnMapping`" in the feature table.

#### [VERIFIED] IcebergCompatV1 Partition Materialization Constraint — ACCURATE

The doc states "Partition columns must be materialized in Parquet files (placed after data columns)" as an IcebergCompatV1 write constraint. This is consistent with `PROTOCOL.md` which describes this as a requirement of the icebergCompatV1 table feature (not a separate feature dependency but an enforced write-time constraint). The source confirms this through `IcebergCompat.scala`'s check logic. Accurate.

#### [NIT] `timestampNtz` Dual-Table Listing — Technically Accurate but Confusing

The doc lists `timestampNtz` in both "Writer-Only Features" and "Reader-Writer Features" tables with a note explaining the protocol-version dependency. The source shows `TimestampNTZTableFeature extends ReaderWriterFeature`. The note clarifies correctly that the dual-table listing reflects historical protocol behavior. No correction needed but the note could be slightly clearer that the current code implementation is always `ReaderWriterFeature`.

---

### Pass Verdict for protocol/table_features.md

**APPROVED** — accurate and complete.

---

## spark-connect.md

### Findings

#### [VERIFIED] DeltaRelation Proto — 10 Relation Types — ACCURATE

`spark-connect/common/src/main/protobuf/delta/connect/relations.proto:30–43` confirmed: `DeltaRelation` oneof has exactly 10 fields:
1. scan, 2. describe_history, 3. describe_detail, 4. convert_to_delta, 5. restore_table, 6. is_delta_table, 7. delete_from_table, 8. update_table, 9. merge_into_table, 10. optimize_table.

This matches the doc's "dispatch table for 10 relation types" claim.

#### [VERIFIED] DeltaCommand — 7 Command Types — ACCURATE

The proto `DeltaCommand` oneof has 7 fields: clone_table, vacuum_table, upgrade_table_protocol, generate, create_delta_table, add_feature_support, drop_feature_support. Confirmed from the `commands.proto` snippet in the doc, which matches source.

#### [VERIFIED] DeltaTable Proto base.proto — ACCURATE

`base.proto:26–39`: `message DeltaTable` with `oneof access_type { Path path = 1; string table_or_view_name = 2; }` and `message Path { string path = 1; map<string, string> hadoop_conf = 2; }` — confirmed.

---

### Pass Verdict for spark-connect.md

**APPROVED** — accurate and complete.

---

## sharing.md

### Findings

#### [VERIFIED] delta-sharing-client Version — ACCURATE

`build.sbt:807`: `"io.delta" %% "delta-sharing-client" % "1.3.9"`. Confirmed.

#### [VERIFIED] SHA-256 Hash for RPC Deduplication — ACCURATE

`sharing/src/main/scala/io/delta/sharing/spark/DeltaSharingUtils.scala:315, 324`: Both `getQueryId()` and `getCDFQueryId()` use `Hashing.sha256().hashString(..., UTF_8).toString`. Confirmed.

#### [VERIFIED] DeltaSharingLogFileSystem Open Method — ACCURATE

`DeltaSharingLogFileSystem.scala:54–79` (cited as such in the doc) was confirmed by reading the structure: checkpoint files → `SeekableByteArrayInputStream(FAKE_CHECKPOINT_BYTE_ARRAY)`; delta files → BlockManager lookup → byte array materialization. The OOM risk warning in the code is accurately noted.

#### [VERIFIED] Three Log Construction Methods — ACCURATE

The three methods (`constructLocalDeltaLogAtVersionZero`, `constructLocalDeltaLogAcrossVersions`, `constructDeltaLogWithMetadataAtVersionZero`) and their described use cases match the file's implementation (confirmed via grep on method names in `DeltaSharingLogFileSystem.scala`).

---

### Pass Verdict for sharing.md

**APPROVED** — accurate and complete.

---

## Coverage Gaps

No significant undocumented components were found in the reviewed documents. The flink module's note about no concrete `DeltaCatalog`/`DeltaTable` implementations being present is confirmed and documented as a known structural fact about the module.

---

## Backwards-Pass Diagram Opportunities

The following opportunities are flagged for the orchestrator to consider for a decomposer diagram-addition pass. These are **new** flags not already present in the reviewed documents.

### FLAG: Shared Two-Slot Async Queue Pattern (UniForm Hudi vs. Iceberg)

- **Modules involved**: [[uniform-hudi]], [[uniform-iceberg]]
- **What the explorer found**: Both `HudiConverter` and `IcebergConverter` use an identical two-slot async queue design: `currentConversion` (being processed) + `standbyConversion` (queued), with `getAndSet` for atomic replacement on backpressure. The same `asyncThreadLock` + `asyncConverterThreadActive` guard pattern appears in both.
- **Why a manifest-level diagram would help**: The module manifest has a "UniForm Conversion Flow" diagram but it doesn't show the async queue mechanics or the fact that both converters share the identical design. A side-by-side `sequenceDiagram` would make the pattern explicit and help architects reason about the backpressure semantics.
- **Relevant source evidence**: `hudi/.../hudi/HudiConverter.scala:68–77`, `iceberg/.../icebergShaded/IcebergConverter.scala:86–95`
- **Suggested diagram type**: `sequenceDiagram` (show two concurrent Delta commits racing with one being dropped from standbyConversion)

### FLAG: CheckpointWriter Incremental vs. Full-Snapshot Decision Tree

> (Already flagged in flink.md as a diagram opportunity for the manifest.) Restated here for orchestrator tracking as a manifest-level addition alongside the Flink write path.

- **Modules involved**: [[connectors/flink]], [[kernel]]
- **What the explorer found**: The `CheckpointWriter` has a multi-branch fallback decision: incremental path (if `_last_checkpoint` tagged by me AND no RemoveFiles in range) vs. full snapshot via `getCreateCheckpointIterator` — this decision is complex and non-obvious but critical to understand the performance properties of the Flink connector.
- **Why a manifest-level diagram would help**: The existing write path `sequenceDiagram` in flink.md shows the happy path but omits the fallback branching.
- **Relevant source evidence**: `flink/src/main/java/io/delta/flink/kernel/CheckpointWriter.java:197–316`
- **Suggested diagram type**: `flowchart TD`

---

## Verdict per Document

| Document | Verdict | Key Issue |
|---|---|---|
| `connectors/flink.md` | **APPROVED_WITH_CHANGES** | MINOR: sidecar merge citation range off by 14 lines |
| `connectors/uniform-iceberg.md` | **APPROVED_WITH_CHANGES** | MINOR: CREATE_TABLE snippet truncation misleads on `txn.commitTransaction()` scope |
| `connectors/uniform-hudi.md` | **NEEDS_REVISION** | MAJOR: Doc says 2 swallowed HoodieExceptions; source has 3 |
| `protocol/transaction_log.md` | **APPROVED** | — |
| `protocol/actions.md` | **APPROVED** | — |
| `protocol/checkpoints.md` | **APPROVED** | — |
| `protocol/table_features.md` | **APPROVED** | — |
| `spark-connect.md` | **APPROVED** | — |
| `sharing.md` | **APPROVED** | — |
