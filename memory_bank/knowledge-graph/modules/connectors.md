---
title: "Delta Connectors Module (Flink, Hive, Golden Tables, Contribs)"
tags: [module, connectors, flink, hive, iceberg, compatibility, contribs, logstore]
layer: L3
last_updated: 2026-03-02
source_files:
  - "flink/src/main/java/io/delta/flink/table/DeltaCatalog.java"
  - "flink/src/main/java/io/delta/flink/table/DeltaTable.java"
  - "flink/src/main/java/io/delta/flink/kernel/CheckpointWriter.java"
  - "connectors/golden-tables/src/main/scala/io/delta/golden/GoldenTableUtils.scala"
  - "contribs/src/main/scala/io/delta/storage/IBMCOSLogStore.scala"
  - "contribs/src/main/scala/io/delta/storage/OracleCloudLogStore.scala"
related:
  - "[[../architecture/system_map]]"
  - "[[kernel]]"
  - "[[spark]]"
  - "[[storage]]"
---

# Delta Connectors Module

Module:: DeltaConnectors  
#module #connectors #flink #iceberg #compatibility

## Table of Contents
- [[#Overview|Overview]]
- [[#Delta Flink Connector|Delta Flink Connector]]
- [[#Iceberg Compatibility|Iceberg Compatibility]]
- [[#Hudi Compatibility|Hudi Compatibility]]
- [[#Golden Tables|Golden Tables]]
- [[#Related Documents|Related Documents]]

---

## Overview

The connectors section covers Delta integrations with non-Spark engines and compatibility layers. The primary active connector in this directory is the **Delta Flink connector** (`flink/`). The `connectors/` directory historically contained standalone connectors (Hive, standalone reader), but these are now largely superseded by Kernel-based implementations. The `golden-tables/` sub-project provides test tables used for cross-engine compatibility validation.

---

## Delta Flink Connector

**Location**: `flink/`  
**Artifact**: `delta-flink-2.0.1-<version>.jar` (assembly JAR)  
**Language**: Java  
**Built on**: Delta Kernel (`kernel-defaults`) — not on Spark

### Architecture

The Flink connector uses Delta Kernel as its protocol implementation layer, similar in spirit to how custom engines would use Kernel. It does NOT depend on Spark.

```
Flink Job
  └── DeltaCatalog (Flink Catalog impl)
        └── DeltaTable (Flink Table impl)
              └── Delta Kernel (kernel-defaults)
                    └── DefaultEngine (Hadoop + Parquet)
```

### Key Classes

#### `DeltaCatalog` (`io.delta.flink.table.DeltaCatalog`)
Implements Flink's `Catalog` interface. Provides:
- Table discovery and registration
- Schema inference from Delta metadata
- Integration with Unity Catalog for catalog-managed tables

#### `DeltaTable` (Flink) (`io.delta.flink.table.DeltaTable`)
Flink table implementation backed by a Delta table. Bridges Flink's table API to Delta Kernel.

#### `SnapshotCacheManager`
Caches Delta `Snapshot` instances to avoid redundant log replay during repeated Flink task executions.

#### `CheckpointWriter` (`io.delta.flink.kernel.CheckpointWriter`)
Handles writing Delta checkpoints from within Flink jobs. Uses Kernel's checkpoint infrastructure.

#### `CheckpointActionRow`
Represents a single action row in a checkpoint file being written.

#### `ColumnVectorUtils`
Utility class for converting between Flink's `ColumnVector` types and Delta Kernel's `ColumnVector`.

#### `Conf` / `TableConf`
Configuration classes for the Flink connector. `TableConf` holds per-table configuration.

#### `CredentialManager`
Manages cloud storage credentials for the Flink connector (e.g., S3 access keys, Azure credentials).

#### `MetricListener`
Implements Delta Kernel's `MetricsReporter` interface to publish Kernel metrics to Flink's metric system.

#### `ExceptionUtils`
Utility class for wrapping Kernel/IO exceptions into Flink-compatible exceptions.

### Dependencies
```
Runtime: flink-core, flink-table-common, flink-streaming-java (provided)
         delta-kernel-defaults (embedded)
         unitycatalog-client (for UC integration)
         hadoop-aws (S3 access)
         caffeine (caching)
         failsafe (retry logic)
```

### Build Notes
- Assembly JAR bundles all dependencies (except provided Flink and Hadoop jars)
- `kernel-api` JAR is referenced as an unmanaged JAR (symlinked) rather than via Maven dep
- Tests use Flink's mini-cluster (`flink-test-utils`) + WireMock for UC API mocking

---

## Iceberg Compatibility

**Location**: `iceberg/` and `icebergShaded/`  
**Artifact**: `delta-iceberg` (part of delta-spark)

### Purpose
Allows Delta tables to be read by Iceberg-compatible engines (Trino, Athena, etc.) without data duplication. The feature is called **UniForm** (Universal Format).

### IcebergCompatV1
- Enforces Iceberg V1 format constraints on Delta data files
- Generates Iceberg table metadata alongside Delta metadata
- Table feature: `icebergCompatV1`

### IcebergCompatV2
- Extends V1 with Iceberg V2 (row-level deletes) support
- Requires column mapping (`name` mode) for field ID mapping
- Requires no partition evolution
- Table feature: `icebergCompatV2`
- Kernel code: `IcebergCompatV2MetadataValidatorAndUpdater`

### IcebergCompatV3
- Extends V2 with additional constraints
- Table feature: `icebergCompatV3`
- Kernel code: `IcebergCompatV3MetadataValidatorAndUpdater`

### `UniversalFormat` (Spark)
`UniversalFormat.scala` in the Spark module orchestrates:
1. Converting Delta commits to Iceberg/Hudi format metadata
2. Writing Iceberg manifests alongside Delta commits
3. The `ProvidesUniFormConverters` trait in `DeltaLog` provides access

### `icebergShaded`
A shaded Iceberg runtime JAR used to avoid Iceberg dependency version conflicts in Spark environments that may have their own Iceberg installation.

---

## Hudi Compatibility

**Location**: `hudi/`

### Purpose
Allows Delta tables to be read by Hudi-compatible tools. Similar to Iceberg compatibility but for the Apache Hudi format. Less mature than Iceberg compatibility.

---

## Golden Tables

**Location**: `connectors/golden-tables/`  
**Artifact**: `golden-tables` (test-only, not released)

### Purpose
`GoldenTables` generates a fixed set of Delta tables with known contents, schema, and configurations. These tables are used as test fixtures across all connectors to validate:
- Correct protocol reading across versions
- Schema evolution correctness
- Partition handling
- Column type coverage
- Edge cases (empty tables, single-file tables, etc.)

### `GoldenTableUtils`
Utility methods for:
- Writing golden tables to a temp directory
- Asserting expected contents match actual contents
- Delta log inspection utilities

### Usage in Tests
Connector tests (Flink, standalone, Kernel tests) depend on `goldenTables % "test"` to access pre-built table fixtures.

---

---

## delta-contribs — Community LogStore Contributions

**Module**: `delta-contribs`  
**SBT artifact**: `delta-contribs` (published)  
**Source path**: `contribs/src/main/scala/io/delta/storage/`  
**Language**: Scala 2.13  
**Depends on**: `delta-spark` (the full unified facade, not just `delta-storage`)  
**Version**: `4.1.0`

### Overview

`delta-contribs` is a community contribution module that ships `LogStore` implementations for cloud storage systems not covered by the core `delta-storage` artifact. The separation from `delta-storage` serves two purposes:

1. **Stability boundary**: The core `delta-storage` artifact guarantees `@Stable` API surface; contributed stores carry `@Unstable` annotations and may change without binary-compatibility guarantees.
2. **Support model**: Community-maintained stores receive best-effort support rather than the first-class support given to the AWS S3/Azure/GCS stores in `delta-storage`.

Both stores extend `HadoopFileSystemLogStore` (from `delta-spark`'s internal package `org.apache.spark.sql.delta.storage`), which provides the base Hadoop FS-based LogStore contract.

### Why `delta-spark` Dependency (Not Just `delta-storage`)

The parent class `HadoopFileSystemLogStore` lives in `delta-spark-v1` (package `org.apache.spark.sql.delta.storage`), not in `delta-storage`. This is a notable asymmetry: `delta-storage` defines the public `LogStore` interface, but the Hadoop FS concrete base class is inside the Spark module. The contribs must therefore depend on the full `delta-spark` artifact, which is heavier than strictly necessary for the LogStore contract alone.

> [!WARNING] Unverified
> No `README.md` was found in `contribs/` to confirm the contribution model or documentation. The information below is inferred from source code and class-level ScalaDoc.

---

### Community Store: `IBMCOSLogStore`

**File**: `contribs/src/main/scala/io/delta/storage/IBMCOSLogStore.scala`  
**Class**: `io.delta.storage.IBMCOSLogStore`  
**Annotation**: `@Unstable`  
**Target system**: IBM Cloud Object Storage (COS)

#### Storage Assumptions

From the class-level ScalaDoc:
- Write on COS is **all-or-nothing** (whether or not overwrite is set)
- Write is **atomic** *only when* the Stocator connector v1.1.1+ is used and `fs.cos.atomic.write = true` is configured
- List-after-write is **consistent**

#### Startup Guard

The constructor immediately asserts the required Hadoop configuration:

```scala
// contribs/src/main/scala/io/delta/storage/IBMCOSLogStore.scala:52-55
assert(initHadoopConf.getBoolean("fs.cos.atomic.write", false) == true,
  "'fs.cos.atomic.write' must be set to true to use IBMCOSLogStore " +
  "in order to enable atomic write")
```

Failing to set this property causes an `AssertionError` at instantiation time, preventing silent data corruption from non-atomic writes.

#### Write Implementation

```scala
// contribs/src/main/scala/io/delta/storage/IBMCOSLogStore.scala:60-85
override def write(path, actions, overwrite, hadoopConf): Unit = {
  val fs = path.getFileSystem(hadoopConf)
  val exists = fs.exists(path)
  if (exists && overwrite == false) {
    throw new FileAlreadyExistsException(path.toString)
  } else {
    val stream = fs.create(path, overwrite)
    try {
      actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(stream.write)
      stream.close()
    } catch {
      case e: IOException if isPreconditionFailure(e) =>
        if (fs.exists(path)) throw new FileAlreadyExistsException(path.toString)
        else throw new IllegalStateException("Failed due to concurrent write", e)
    }
  }
}
```

When `overwrite=false` and the path exists, throws `FileAlreadyExistsException` immediately (no write attempted). When Stocator's precondition check fails mid-write (concurrent conflict), the code re-checks existence and re-throws the appropriate exception: `FileAlreadyExistsException` if the file was concurrently committed, or `IllegalStateException` otherwise.

The `isPreconditionFailure()` helper walks the full exception causal chain (via Guava `Throwables.getCausalChain`) looking for the Stocator message `"At least one of the preconditions you specified did not hold"`.

#### Key Properties

| Property | Value |
|----------|-------|
| `isPartialWriteVisible` | `false` — COS + Stocator writes are atomic |
| `invalidateCache` | No-op |
| `shouldUseRenameToWriteCheckpoint` (test) | `false` — checkpoints are written directly |
| Required Hadoop config | `fs.cos.atomic.write = true` |

---

### Community Store: `OracleCloudLogStore`

**File**: `contribs/src/main/scala/io/delta/storage/OracleCloudLogStore.scala`  
**Class**: `io.delta.storage.OracleCloudLogStore`  
**Annotation**: `@Unstable`  
**Target system**: Oracle Cloud Infrastructure (OCI) Object Storage (via `BmcFilesystem`)

#### Storage Assumptions

From the class-level ScalaDoc:
- **Atomic rename** without overwrite is atomic on OCI's `BmcFilesystem`
- List-after-write is **consistent**

#### Write Implementation

Entirely delegates to the inherited `writeWithRename` method from `HadoopFileSystemLogStore`:

```scala
// contribs/src/main/scala/io/delta/storage/OracleCloudLogStore.scala:47-57
override def write(path, actions, overwrite, hadoopConf): Unit = {
  writeWithRename(path, actions, overwrite, hadoopConf)
}
```

`writeWithRename` (in the base class) implements the standard put-if-absent pattern:
1. Write content to a temporary file
2. Atomically rename temp → destination (using `fs.rename()`)
3. If rename fails and destination exists, throw `FileAlreadyExistsException`

When `overwrite=true`, the rename is performed without the existence check, meaning partial files are potentially visible to concurrent readers.

#### Key Properties

| Property | Value |
|----------|-------|
| `isPartialWriteVisible` | `true` — overwrite=true writes not atomic; partial visibility possible |
| `invalidateCache` | No-op |
| `shouldUseRenameToWriteCheckpoint` (test) | `true` — checkpoint written via rename |
| Required Hadoop config | OCI BmcFilesystem driver on classpath |

---

### IBMCOSLogStore vs. OracleCloudLogStore: Design Comparison

| Dimension | IBMCOSLogStore | OracleCloudLogStore |
|-----------|----------------|---------------------|
| Write atomicity mechanism | COS native atomic write (Stocator) | Atomic rename (BmcFilesystem) |
| Conflict detection | Stocator precondition failure + exist check | Rename failure |
| `isPartialWriteVisible` | `false` | `true` |
| Startup validation | Assert `fs.cos.atomic.write=true` | None |
| Base method used | Direct `fs.create()` + stream write | `writeWithRename()` (inherited) |
| Code complexity | ~50 lines (precondition exception handling) | ~20 lines (pure delegation) |

---

### Relationship to `delta-storage` Core Stores

The `delta-storage` artifact ships production-hardened stores for the major cloud platforms:
- `S3SingleDriverLogStore` — single-cluster S3, uses auxiliary file trick for atomicity
- `AzureLogStore` — Azure ADLS Gen2, uses blob-level conditional write
- `GCSLogStore` — Google Cloud Storage
- `HDFSLogStore` / `HadoopFileSystemLogStore` — HDFS and compatible filesystems with rename semantics

The contribs stores target platforms with sufficiently good storage primitives that no auxiliary coordination layer is needed (like the DynamoDB table needed by `delta-storage-s3-dynamodb`). They are contributed by platform users who operate the infrastructure and can verify the storage guarantees firsthand.

---

### Configuration

To use a contrib store, set:

```
spark.delta.logStore.class = io.delta.storage.IBMCOSLogStore
# or
spark.delta.logStore.class = io.delta.storage.OracleCloudLogStore
```

For path-scheme-based configuration (preferred since Delta 1.0):
```
spark.delta.logStore.cos.impl  = io.delta.storage.IBMCOSLogStore
spark.delta.logStore.oci.impl  = io.delta.storage.OracleCloudLogStore
```

---

### Test Coverage

| File | Type | What's tested |
|------|------|---------------|
| `IBMCOSLogStoreSuite` | Unit (extends `LogStoreSuiteBase`) | LogStore contract tests via `FakeFileSystem`; asserts `fs.cos.atomic.write=true` is required |
| `OracleCloudLogStoreSuite` | Unit (extends `LogStoreSuiteBase`) | LogStore contract tests; rename-based write checkpoint behavior |

Both test suites use `FakeFileSystem` (from `delta-spark` test utils) to simulate the cloud filesystem without real network access. `LogStoreSuiteBase` provides the shared test contract all LogStore implementations must satisfy.

---

## Related Documents

- [[kernel]] — Kernel is used by Flink connector directly
- [[spark]] — Iceberg/Hudi compatibility is implemented in the Spark module
- [[storage]] — Core LogStore interface and built-in implementations; delta-contribs extends these patterns
- [[../architecture/system_map]] — Overall system
- [[../concepts/z_ordering_clustering]] — OPTIMIZE/clustering (used in Flink too)
