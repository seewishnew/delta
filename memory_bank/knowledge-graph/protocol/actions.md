---
title: "Action Types Reference"
tags: [protocol, actions, delta-protocol, schema]
layer: L2
last_updated: 2026-03-02
source_files:
  - "PROTOCOL.md"
  - "spark/src/main/scala/org/apache/spark/sql/delta/actions/actions.scala"
related:
  - "[[transaction_log]]"
  - "[[checkpoints]]"
  - "[[table_features]]"
  - "[[spark]]"
---

# Action Types Reference

## Purpose

Delta actions are the atomic units of state change in the transaction log. Every commit file is a sequence of newline-delimited JSON objects, each encoding exactly one action. This document provides a complete reference for every action type defined by the Delta protocol, including field schemas, JSON representations, and the constraints governing their use.

---

## Action Overview

| Action name | JSON field | Category | Appears in commits | Appears in checkpoints |
|---|---|---|---|---|
| `Protocol` | `protocol` | Protocol | ✓ | ✓ |
| `Metadata` | `metaData` | Metadata | ✓ | ✓ |
| `AddFile` | `add` | File | ✓ | ✓ |
| `RemoveFile` | `remove` | File | ✓ | ✓ (tombstones only) |
| `AddCDCFile` | `cdc` | File | ✓ | ✗ |
| `CommitInfo` | `commitInfo` | Provenance | ✓ | ✗ |
| `SetTransaction` | `txn` | Transaction | ✓ | ✓ |
| `DomainMetadata` | `domainMetadata` | Metadata | ✓ | ✓ |
| `CheckpointMetadata` | `checkpointMetadata` | Checkpoint | ✗ | ✓ (V2 only) |
| `SidecarFile` | `sidecar` | Checkpoint | ✗ | ✓ (V2 only) |

---

## Action Schemas

### Protocol

Controls which reader and writer versions are required to access the table. Upgrading the protocol blocks older clients.

**JSON field**: `protocol`

| Field | Type | Required | Description |
|---|---|---|---|
| `minReaderVersion` | Int | ✓ | Minimum reader version required |
| `minWriterVersion` | Int | ✓ | Minimum writer version required |
| `readerFeatures` | Array[String] | Only when `minReaderVersion=3` | Reader table features clients must implement |
| `writerFeatures` | Array[String] | Only when `minWriterVersion=7` | Writer table features clients must implement |

**Constraints**:
- `readerFeatures` must be present if and only if `minReaderVersion=3`.
- `writerFeatures` must be present if and only if `minWriterVersion=7`.
- If `readerFeatures` is present, `writerFeatures` must also be present (reader features require writer features).
- Clients must silently ignore unrecognized fields in actions — this is the forward-compatibility contract.

**JSON examples**:

```json
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
```

```json
{
  "protocol": {
    "minReaderVersion": 3,
    "minWriterVersion": 7,
    "readerFeatures": ["columnMapping","deletionVectors"],
    "writerFeatures": ["columnMapping","deletionVectors","rowTracking","domainMetadata"]
  }
}
```

**Scala implementation**: `case class Protocol` in `spark/src/main/scala/org/apache/spark/sql/delta/actions/actions.scala:168`

The `Protocol` class enforces at construction time that `supportsReaderFeatures == readerFeatures.isDefined`. The `Action.supportedProtocolVersion()` method builds the maximum supported protocol by including all features in `TableFeature.allSupportedFeaturesMap`.

---

### Metadata (`metaData`)

Changes the table schema, partition spec, or configuration. At most one `metaData` action per commit. Every table must have a `metaData` action in its first commit (version 0).

**JSON field**: `metaData`

| Field | Type | Required | Description |
|---|---|---|---|
| `id` | GUID String | ✓ | Globally unique table identifier; must not change after table creation |
| `name` | String | optional | User-provided table name |
| `description` | String | optional | User-provided description |
| `format` | Format struct | ✓ | Encoding spec; `{"provider":"parquet","options":{}}` in practice |
| `schemaString` | JSON String | ✓ | Delta schema JSON (Spark SQL subset); see Schema Serialization in `PROTOCOL.md` |
| `partitionColumns` | Array[String] | ✓ | Column names used for partitioning (empty = unpartitioned) |
| `createdTime` | Long (ms epoch) | optional | When this metadata action was created |
| `configuration` | Map[String,String] | ✓ | Table properties (e.g. `delta.appendOnly`, `delta.enableChangeDataFeed`) |

**Format Specification**:

| Field | Type | Description |
|---|---|---|
| `provider` | String | `"parquet"` (other formats for legacy/future; API-level only allows parquet) |
| `options` | Map[String,String] | Format options; empty in practice |

**JSON example**:
```json
{
  "metaData": {
    "id": "af23c9d7-fff1-4a5a-a2c8-55c59bd782aa",
    "format": {"provider": "parquet", "options": {}},
    "schemaString": "{\"type\":\"struct\",\"fields\":[...]}",
    "partitionColumns": ["date"],
    "configuration": {"delta.enableChangeDataFeed": "true"},
    "createdTime": 1515491537026
  }
}
```

---

### AddFile (`add`)

Records a data file being added to the table. Combined with `remove` actions and deletion vectors, `add` actions constitute the complete file inventory of the table.

**JSON field**: `add`

| Field | Type | Required | Description |
|---|---|---|---|
| `path` | String (URI) | ✓ | Relative (from table root) or absolute path; URI-encoded per RFC 2396 |
| `partitionValues` | Map[String,String] | ✓ | Partition column → serialized value; empty map for unpartitioned |
| `size` | Long | ✓ | Data file size in bytes |
| `modificationTime` | Long (ms epoch) | ✓ | When the file was created/modified |
| `dataChange` | Boolean | ✓ | `false` = rearrangement only (e.g. compaction); streaming can skip these |
| `stats` | JSON String | optional | Per-file statistics: `numRecords`, `tightBounds`, `minValues`, `maxValues`, `nullCount` |
| `tags` | Map[String,String] | optional | Arbitrary metadata about the file |
| `deletionVector` | DeletionVectorDescriptor | optional | DV reference if rows are soft-deleted; null/absent if no DV |
| `baseRowId` | Long | optional | Row ID of the first row; used for Row Tracking |
| `defaultRowCommitVersion` | Long | optional | First commit version this file was added |
| `clusteringProvider` | String | optional | Name of clustering implementation (e.g. `"liquid"`) |

**Deletion Vector Descriptor Schema** (nested struct when `deletionVector` is present):

| Field | Type | Description |
|---|---|---|
| `storageType` | String (`'u'`,`'i'`,`'p'`) | `u`=relative UUID-named file, `i`=inline base85, `p`=absolute path |
| `pathOrInlineDv` | String | UUID (with optional prefix), inline bytes, or absolute path depending on `storageType` |
| `offset` | Option[Int] | Byte offset into DV file; absent for inline |
| `sizeInBytes` | Int | Raw serialized size of the DV |
| `cardinality` | Long | Number of rows logically removed |

**Primary key** for log replay: `(path, deletionVector.uniqueId)` where `uniqueId` is derived as `<storageType><pathOrInlineDv>[@<offset>]`, or `NULL` when no DV.

**JSON example (partitioned + row tracking)**:
```json
{
  "add": {
    "path": "date=2017-12-10/part-00000-3935a07c.c000.snappy.parquet",
    "partitionValues": {"date": "2017-12-10"},
    "size": 841454,
    "modificationTime": 1512909768000,
    "dataChange": true,
    "baseRowId": 4071,
    "defaultRowCommitVersion": 41,
    "stats": "{\"numRecords\":1,\"minValues\":{...},\"maxValues\":{...}}"
  }
}
```

**JSON example (with Deletion Vector)**:
```json
{
  "add": {
    "path": "part-00000.parquet",
    "partitionValues": {},
    "size": 1200000,
    "modificationTime": 1512909768000,
    "dataChange": false,
    "deletionVector": {
      "storageType": "u",
      "pathOrInlineDv": "ab^-aqEH.-t@S}K{vb[*k^",
      "offset": 4,
      "sizeInBytes": 40,
      "cardinality": 6
    }
  }
}
```

---

### RemoveFile (`remove`)

Records logical removal of a data file. Remove actions become _tombstones_ that persist in the snapshot until VACUUM expires them. They are stored in checkpoints without `stats` or `tags` (those are only needed in commit files).

**JSON field**: `remove`

| Field | Type | Required | Description |
|---|---|---|---|
| `path` | String (URI) | ✓ | Same encoding as `add.path` |
| `deletionTimestamp` | Long (ms epoch) | optional | When the removal occurred; used for VACUUM expiry |
| `dataChange` | Boolean | ✓ | `false` = records also covered by a concurrent `add` in the same commit |
| `extendedFileMetadata` | Boolean | optional | When `true`, `partitionValues`, `size`, `tags` are present |
| `partitionValues` | Map[String,String] | optional | Present when `extendedFileMetadata=true` |
| `size` | Long | optional | Present when `extendedFileMetadata=true` |
| `stats` | JSON String | optional | Statistics (only in commit files, **not** in checkpoints) |
| `tags` | Map[String,String] | optional | Only in commit files |
| `deletionVector` | DeletionVectorDescriptor | optional | DV that was associated with this file at removal time |
| `baseRowId` | Long | optional | Row Tracking base row ID |
| `defaultRowCommitVersion` | Long | optional | Row Tracking default commit version |

**JSON example**:
```json
{
  "remove": {
    "path": "part-00001-9abcdef.snappy.parquet",
    "deletionTimestamp": 1515488792485,
    "dataChange": true
  }
}
```

> [!NOTE]
> A tombstone expires when `currentTime > deletionTimestamp + retentionThreshold`. The default retention threshold is 7 days. VACUUM deletes expired tombstones and their physical files.

---

### AddCDCFile (`cdc`)

Records a Change Data Capture file for a given commit. CDC files contain a `_change_type` column (`insert`, `update_preimage`, `update_postimage`, `delete`). Requires the `changeDataFeed` table feature.

**JSON field**: `cdc`

| Field | Type | Required | Description |
|---|---|---|---|
| `path` | String (URI) | ✓ | Path to CDC file (under `_change_data/`) |
| `partitionValues` | Map[String,String] | ✓ | Partition values |
| `size` | Long | ✓ | File size in bytes |
| `dataChange` | Boolean | ✓ | Always `false` for CDC actions (they record changes, not new data) |
| `tags` | Map[String,String] | optional | Arbitrary metadata |

**Behavior**:
- When a commit version has `cdc` actions, CDC readers must use **only** the CDC files for that version (not derive changes from `add`/`remove`).
- If no `cdc` actions in a version, CDC readers treat `add` actions as inserts and `remove` actions as deletes.
- **Not preserved in checkpoints**.

**JSON example**:
```json
{
  "cdc": {
    "path": "_change_data/cdc-00001-c123.snappy.parquet",
    "partitionValues": {},
    "size": 1213,
    "dataChange": false
  }
}
```

---

### CommitInfo (`commitInfo`)

Optional provenance metadata about the commit. Contains operation type, user, timestamp, parameters. When `catalogManaged` is active, `commitInfo` must include a `txnId` field. When `inCommitTimestamp` is active, `commitInfo` must be the **first action** in the commit and must include `inCommitTimestamp`.

**JSON field**: `commitInfo`

Fields are implementation-defined free-form JSON. Common fields observed in practice:

| Field | Description |
|---|---|
| `timestamp` | Wall-clock timestamp (ms epoch) at commit time |
| `userId` / `userName` | Who performed the operation |
| `operation` | Operation name (`WRITE`, `DELETE`, `MERGE`, `OPTIMIZE`, etc.) |
| `operationParameters` | Map of operation-specific parameters |
| `inCommitTimestamp` | Long (ms epoch) — monotonically increasing; required when `inCommitTimestamp` feature active |
| `txnId` | Unique transaction ID string; required when `catalogManaged` active |

**JSON example**:
```json
{
  "commitInfo": {
    "timestamp": 1515491537026,
    "userId": "100121",
    "operation": "WRITE",
    "operationParameters": {"mode": "Append"},
    "inCommitTimestamp": 1515491537042
  }
}
```

**Not preserved in checkpoints.** Log compaction files also drop `commitInfo`.

---

### SetTransaction (`txn`)

Records idempotency state for streaming writers and other incremental processors. Stored per `appId`; only the latest `version` per `appId` is retained in a snapshot.

**JSON field**: `txn`

| Field | Type | Required | Description |
|---|---|---|---|
| `appId` | String | ✓ | Unique identifier for the writing application/stream |
| `version` | Long | ✓ | Application-specific progress indicator (semantics up to the app) |
| `lastUpdated` | Long (ms epoch) | optional | When this `txn` action was recorded |

**Use case**: The Delta streaming sink writes `(appId=streamId, version=batchId)` alongside data. On retry, if `batchId` ≤ the recorded version, the write is skipped (idempotency).

**JSON example**:
```json
{
  "txn": {
    "appId": "3ba13872-2d47-4e17-86a0-21afd2a22395",
    "version": 364475
  }
}
```

---

### DomainMetadata (`domainMetadata`)

Stores named configuration blobs used by table features (e.g. Row Tracking high watermark, clustering column list). Requires `domainMetadata` writer feature.

**JSON field**: `domainMetadata`

| Field | Type | Required | Description |
|---|---|---|---|
| `domain` | String | ✓ | Domain name; `delta.*` = system-controlled, anything else = user-controlled |
| `configuration` | String | ✓ | Arbitrary JSON string; feature-specific semantics |
| `removed` | Boolean | ✓ | When `true`, acts as a tombstone to logically delete the domain |

**System-controlled domains** (names starting with `delta.`):
- `delta.rowTracking` — stores `rowIdHighWaterMark`
- `delta.clustering` — stores `clusteringColumns` list

Writers must preserve all domains (even unknown ones) and must not allow users to modify system-controlled domains.

**JSON example**:
```json
{
  "domainMetadata": {
    "domain": "delta.rowTracking",
    "configuration": "{\"rowIdHighWaterMark\": 99999}",
    "removed": false
  }
}
```

---

### CheckpointMetadata (`checkpointMetadata`)

Present only in V2 checkpoints. Describes the checkpoint version and carries optional tags. **Not allowed in commit files.**

**JSON field**: `checkpointMetadata`

| Field | Type | Required | Description |
|---|---|---|---|
| `version` | Long | ✓ | The table version this checkpoint represents |
| `tags` | Map[String,String] | optional | Arbitrary metadata about the checkpoint |

**JSON example**:
```json
{
  "checkpointMetadata": {
    "version": 364475,
    "tags": {}
  }
}
```

Required by the `v2Checkpoint` table feature. All non-file actions (protocol, metadata, txn, domainMetadata) must be in the V2 checkpoint body itself; file actions (add/remove) may be in sidecar files.

---

### SidecarFile (`sidecar`)

Present only in V2 checkpoints. References a Parquet sidecar file that holds `add` and `remove` file actions. **Not allowed in commit files.**

**JSON field**: `sidecar`

| Field | Type | Required | Description |
|---|---|---|---|
| `path` | String (URI) | ✓ | File name (without directory); resolved relative to `_delta_log/_sidecars/` |
| `sizeInBytes` | Long | ✓ | Size of the sidecar Parquet file |
| `modificationTime` | Long (ms epoch) | ✓ | Creation timestamp |
| `tags` | Map[String,String] | optional | Arbitrary metadata |

Sidecar files contain only `add` and `remove` entries stored as Parquet struct columns. A V2 checkpoint either puts all file actions in sidecars, or puts all of them inline in the checkpoint body — mixing is not allowed.

**JSON example**:
```json
{
  "sidecar": {
    "path": "016ae953-37a9-438e-8683-9a9a4a79a395.parquet",
    "sizeInBytes": 2304522,
    "modificationTime": 1512909768000,
    "tags": {}
  }
}
```

---

## SingleAction Wrapper

In the Scala implementation, each action is serialized to / deserialized from a `SingleAction` wrapper case class that has one field per action type. All other fields are null.

```scala
// spark/src/main/scala/org/apache/spark/sql/delta/actions/actions.scala
case class SingleAction(
    txn: SetTransaction = null,
    add: AddFile = null,
    remove: RemoveFile = null,
    metaData: Metadata = null,
    protocol: Protocol = null,
    cdc: AddCDCFile = null,
    commitInfo: CommitInfo = null,
    domainMetadata: DomainMetadata = null,
    checkpointMetadata: CheckpointMetadata = null,
    sidecar: SidecarFile = null
) {
  def unwrap: Action = ...
}
```

The JSON field names (e.g. `"metaData"`, `"add"`) match the field names in `SingleAction`. Parquet checkpoints store one row per action with all unused columns null.

---

## Action Reconciliation Summary

See [[transaction_log]] for the full log replay algorithm. Quick reference:

| Action | Reconciliation |
|---|---|
| `protocol` | Last one wins |
| `metaData` | Last one wins |
| `txn` | Last `version` per `appId` |
| `domainMetadata` | Last per `domain`; `removed=true` is a tombstone |
| `add` + `remove` | Last per `(path, dvId)`; intersection of `add` and `remove` sets must be empty |
| `commitInfo` | Not reconciled; only the target version's `commitInfo` is surfaced |
| `cdc` | Not in snapshot; used only by CDC readers per-version |
| `checkpointMetadata`, `sidecar` | Not in log replay; V2 checkpoint-only actions |
