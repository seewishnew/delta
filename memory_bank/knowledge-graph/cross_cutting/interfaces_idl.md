---
title: "Shared Interfaces & IDL"
tags: [cross-cutting, interfaces, spi, idl, protobuf, grpc, L2]
layer: L2
last_updated: 2026-03-02
related:
  - "[[architecture/system_map]]"
  - "[[modules/kernel]]"
  - "[[modules/storage]]"
  - "[[modules/spark-connect]]"
  - "[[cross_cutting/data_models]]"
---

# Shared Interfaces & IDL

#cross-cutting #L2 #interfaces #spi #idl #grpc #protobuf

## Overview

Delta Lake exposes four layers of pluggable interfaces:

1. **Engine SPI** (`delta-kernel-api`): The connector-facing service provider interface that isolates all I/O from protocol logic. Connectors implement this to use the Kernel.
2. **LogStore SPI** (`delta-storage`): The storage-system-facing interface for atomic file writes and listings. One implementation per cloud/filesystem.
3. **CommitCoordinatorClient SPI** (`delta-storage`): The catalog-facing interface for coordinated (catalog-managed) commit routing. Optional; only used for UC-managed tables.
4. **Delta Connect IDL** (`delta-connect-common`): The Proto3 / gRPC interface definition for the Spark Connect extension protocol. Client and server both depend on this.

These interfaces span multiple modules and define the primary extension points for Delta Lake.

---

## 1. Engine SPI (`delta-kernel-api`)

### Purpose

The `Engine` interface is the fundamental inversion-of-control mechanism in `delta-kernel-api`. Instead of the Kernel calling filesystems and Parquet readers directly, it delegates all I/O through the `Engine` SPI — which the connector provides. This makes `delta-kernel-api` have zero runtime dependencies.

### Interface Definition

```
// kernel/kernel-api/src/main/java/io/delta/kernel/engine/Engine.java
public interface Engine {
    ExpressionHandler    getExpressionHandler();
    JsonHandler          getJsonHandler();
    FileSystemClient     getFileSystemClient();
    ParquetHandler       getParquetHandler();
    default List<MetricsReporter> getMetricsReporters() { return emptyList(); }
}
```

Source: [[modules/kernel]] §§ "Engine SPI"

### Sub-Interfaces

#### `FileSystemClient`

| Method | Contract |
|---|---|
| `listFrom(filePath)` | List entries in the same directory ≥ filePath (lexicographic UTF-8 order; **results must be sorted**) |
| `resolvePath(path)` | Resolve relative/URI path to fully-qualified string |
| `readFiles(requests)` | Stream byte contents for `FileReadRequest`s (path + optional byte range) |
| `mkdirs(path)` | Create directory and parents |
| `delete(path)` | Delete a file (not directory); returns false if not found |
| `getFileStatus(path)` | Return `FileStatus` (path, size, modification time) |
| `copyFileAtomically(src, dest, overwrite)` | Atomic copy — either fully written or absent |

> [!WARNING] `listFrom` ordering invariant
> Delta log reconstruction depends on lexicographic ordering of delta and checkpoint files in `_delta_log/`. Any `FileSystemClient` implementation that returns an unsorted listing will produce incorrect snapshots silently.

#### `JsonHandler`

| Method | Contract |
|---|---|
| `parseJson(jsonStringVector, outputSchema, selectionVector)` | Parse a `ColumnVector` of JSON strings into a `ColumnarBatch` with the requested schema |
| `readJsonFiles(fileIter, physicalSchema, predicate)` | Read NDJSON log files into `ColumnarBatch`es |
| `writeJsonFileAtomically(filePath, data, overwrite)` | Serialize `Row` iterator as NDJSON; atomic all-or-nothing write |

> [!NOTE] Null serialization asymmetry
> Struct fields with null values must NOT be written to JSON. Map entries with null values MUST be written. These rules mirror the Delta protocol JSON serialization spec and must be implemented correctly.

#### `ParquetHandler`

| Method | Contract |
|---|---|
| `readParquetFiles(fileIter, physicalSchema, predicate)` | Read Parquet files; column matching: by field ID → case-sensitive name → case-insensitive name; ROW_INDEX metadata column populated when requested |
| `writeParquetFiles(directoryPath, dataIter, statsColumns)` | Write columnar batches to Parquet files; collect min/max/null-count stats |
| `writeParquetFileAtomically(filePath, data)` | Write exactly one Parquet file atomically — used for checkpoints |

#### `ExpressionHandler`

| Method | Contract |
|---|---|
| `getEvaluator(inputSchema, expression, outputType)` | Create a reusable `ExpressionEvaluator` for scalar evaluation |
| `getPredicateEvaluator(inputSchema, predicate)` | Create a reusable `PredicateEvaluator` returning a boolean selection vector |
| `createSelectionVector(values[], from, to)` | Wrap a boolean array sub-range into a `ColumnVector` selection vector |

#### `MetricsReporter`

Single method: `report(MetricsReport metricsReport)`. Used by Kernel to emit performance telemetry. `DefaultEngine` provides `LoggingMetricsReporter` (SLF4J). Connectors can implement custom reporters.

### Discovery and Instantiation

The `Engine` SPI is not discovered via classpath scanning. It is **directly instantiated** by the connector:

```java
// All connectors (delta-flink, delta-spark-v2, custom): use DefaultEngine from kernel-defaults
Engine engine = DefaultEngine.create(hadoopConf);

// Tests: custom Engine implementation passed to Table.forPath(engine, path)
```

Source: [[modules/kernel]] §§ "DefaultEngine"

### Concrete Implementations

| Implementation | Module | Description |
|---|---|---|
| `DefaultEngine` | `delta-kernel-defaults` | Hadoop `FileSystem` + Apache Parquet. Used by **both** `delta-flink` AND `delta-spark-v2` (via `DefaultEngine.create(hadoopConf)`), as well as benchmarks and integration tests. |
| Test `Engine` impls | various test modules | Mock engines for unit testing specific kernel behaviors. |

---

## 2. LogStore SPI (`delta-storage`)

### Purpose

`LogStore` abstracts the atomic file write semantics required by the Delta commit protocol. Different cloud storage systems have different primitive operations for achieving mutual exclusion and atomic visibility; `LogStore` normalizes these into a single interface.

### Interface Definition

```java
// storage/src/main/java/io/delta/storage/LogStore.java
public abstract class LogStore {
    public LogStore(Configuration initHadoopConf)  // required reflective constructor

    public abstract CloseableIterator<String> read(Path path, Configuration conf) throws IOException;
    public abstract void write(Path path, Iterator<String> actions, Boolean overwrite, Configuration conf) throws IOException;
    public abstract Iterator<FileStatus> listFrom(Path path, Configuration conf) throws IOException;
    public abstract Path resolvePathOnPhysicalStorage(Path path, Configuration conf) throws IOException;
    public abstract Boolean isPartialWriteVisible(Path path, Configuration conf) throws IOException;
}
```

Source: [[modules/storage]] §§ "LogStore"

### Three Correctness Guarantees

| Guarantee | What it means |
|---|---|
| **Atomic visibility** | If `isPartialWriteVisible()` = false, the file must not become visible to readers until fully written |
| **Mutual exclusion** | `write(overwrite=false)` must throw `FileAlreadyExistsException` if the file already exists (only one concurrent writer can "win") |
| **Consistent listing** | Once a file is written, all future `listFrom()` calls must include it |

### Discovery and Instantiation

`LogStore` implementations are discovered via **Hadoop configuration** properties, instantiated by reflection using the `Configuration`-arg constructor:

| Config Key | Description |
|---|---|
| `spark.delta.logStore.class` | Global default LogStore class (legacy, single-scheme) |
| `spark.delta.logStore.<scheme>.impl` | Per-scheme override (e.g., `spark.delta.logStore.s3a.impl`) |

Default scheme-to-implementation mappings:
- `s3`, `s3a`, `s3n` → `S3SingleDriverLogStore`
- `gs` → `GCSLogStore`
- `abfs`, `abfss`, `adl`, `wasb`, `wasbs` → `AzureLogStore`
- `hdfs`, `viewfs` → `HDFSLogStore`
- `file` → `LocalLogStore`

Both the Spark connector (`LogStoreProvider` in `delta-spark-v1`) and the Kernel (`DefaultEngine` in `kernel-defaults`) use the same configuration precedence rules.

### Concrete Implementations

| Class | Module | Mechanism | `isPartialWriteVisible` |
|---|---|---|---|
| `HDFSLogStore` | `delta-storage` | `FileContext.rename()` (atomic HDFS) + HDFS Observer NameNode msync | `true` |
| `AzureLogStore` | `delta-storage` | `FileSystem.rename()` (atomic ADLS) | `true` |
| `GCSLogStore` | `delta-storage` | GCS native precondition PUT (HTTP 412); written in new thread to avoid interruption | `false` |
| `S3SingleDriverLogStore` | `delta-storage` | JVM-local `PathLock` + all-or-nothing S3 PUT | `false` |
| `LocalLogStore` | `delta-storage` | `synchronized(this)` + atomic rename (test use only) | `true` |
| `S3DynamoDBLogStore` | `delta-storage-s3-dynamodb` | DynamoDB conditional PutItem (cross-JVM mutex) + S3 copy | `false` |
| `IBMCOSLogStore` | `delta-contribs` | Stocator precondition atomic write (`fs.cos.atomic.write=true` required) | `false` |
| `OracleCloudLogStore` | `delta-contribs` | BmcFilesystem atomic rename | `true` |

Source: [[modules/storage]] §§ "LogStore Class Hierarchy", [[modules/connectors]] §§ "Community LogStore"

---

## 3. CommitCoordinatorClient SPI (`delta-storage`)

### Purpose

`CommitCoordinatorClient` enables **coordinated commits**: a mode where a third-party service (e.g., Unity Catalog) owns version assignment, commit staging, and backfill instead of the filesystem's put-if-absent semantics. This is an opt-in feature; tables without the `catalogManaged` table feature use `LogStore` directly.

### Interface Definition

```java
// storage/src/main/java/io/delta/storage/commit/CommitCoordinatorClient.java
public interface CommitCoordinatorClient {
    Map<String, String> registerTable(Path logPath, Optional<TableIdentifier> tableId,
        long currentVersion, AbstractMetadata meta, AbstractProtocol proto);

    CommitResponse commit(LogStore ls, Configuration conf, TableDescriptor td,
        long version, Iterator<String> actions, UpdatedActions ua) throws CommitFailedException;

    GetCommitsResponse getCommits(TableDescriptor td, Long startVersion, Long endVersion);

    void backfillToVersion(LogStore ls, Configuration conf, TableDescriptor td,
        long version, Long lastKnownBackfilled) throws IOException;

    boolean semanticEquals(CommitCoordinatorClient other);
}
```

Source: [[modules/storage]] §§ "CommitCoordinatorClient Interface"

### Key DTOs

| DTO | Key Fields | Notes |
|---|---|---|
| `Commit` | `version`, `fileStatus`, `commitTimestamp` | A ratified commit (staged or published) |
| `CommitResponse` | Wraps `Commit` | Returned by `commit()` |
| `GetCommitsResponse` | `commits: List<Commit>`, `latestTableVersion` | Unbackfilled commits from coordinator; `latestTableVersion=-1` if none ratified |
| `UpdatedActions` | `commitInfo`, `newMetadata`, `newProtocol`, `oldMetadata`, `oldProtocol` | Passed alongside commit actions |
| `TableDescriptor` | `logPath`, `tableIdentifier` (optional), `tableConf` (opaque map) | Identifies table to coordinator |
| `CommitFailedException` | `retryable: bool`, `conflict: bool` | Controls retry semantics (4 combinations) |

### Discovery and Instantiation

`CommitCoordinatorClient` implementations are discovered by **name** (the `delta.coordinatedCommits.commitCoordinator-preview` table property value):

- Spark: `CommitCoordinatorProvider.getCommitCoordinator(spark, name, conf)` uses a registry of named builders (`UCCommitCoordinatorBuilder`).
- Kernel: `DefaultEngine` in `kernel-defaults` routes coordinated commits through `UCCatalogManagedCommitter` (for UC-managed tables); the Kernel Engine SPI itself has no `CommitCoordinatorClient` discovery method.

### Concrete Implementations

| Class | Module | Description |
|---|---|---|
| `UCCommitCoordinatorClient` | `delta-storage` | Routes commits through Unity Catalog REST API (`UCTokenBasedRestClient`). Used by Spark connector via `UCCommitCoordinatorBuilder`. |
| `UCCatalogManagedClient` / `UCCatalogManagedCommitter` | `delta-kernel-unitycatalog` | Kernel-level UC integration. `UCCatalogManagedClient` wraps `SnapshotBuilder` with UC-staged commits; `UCCatalogManagedCommitter` implements `Committer` + `CatalogCommitter`. |
| `InMemoryCommitCoordinator` | `delta-spark-v1` (test) | In-process coordinator for unit tests. |
| `AbstractBatchBackfillingCommitCoordinatorClient` | `delta-spark-v1` | Abstract Spark-side base with batch backfill logic; subclassed by UC builder. |

Source: [[modules/storage]] §§ "CommitCoordinator", [[modules/kernel]] §§ "Unity Catalog Integration"

---

## 4. Delta Connect IDL (Proto3 / gRPC)

### Purpose

The Delta Connect IDL defines the **wire protocol** for Delta operations over the Spark Connect gRPC channel. It is the formal contract between the `delta-connect-client` (Python/Scala thin clients) and `delta-connect-server` (Spark-side plugin).

### Location

```
spark-connect/common/src/main/protobuf/delta/connect/
├── base.proto      — DeltaTable identifier message
├── commands.proto  — DeltaCommand (7 fire-and-forget operations)
└── relations.proto — DeltaRelation (10 data-returning operations)
```

Generated Java stubs: `spark-connect/common/src/main/java/` (auto-generated by `sbt protoc`; not in KG scope).
Generated Python stubs: `python/delta/connect/proto/` (auto-generated; not in KG scope).

Source: [[modules/spark-connect]] §§ "Proto Schema"

### `base.proto` — Table Identifier

```proto
// spark-connect/common/src/main/protobuf/delta/connect/base.proto
message DeltaTable {
  oneof access_type {
    Path path = 1;                  // filesystem path + per-request Hadoop conf
    string table_or_view_name = 2;  // catalog name or delta.`path`
  }
  message Path {
    string path = 1;
    map<string, string> hadoop_conf = 2;  // per-request cloud FS credentials
  }
}
```

### `commands.proto` — Fire-and-Forget Operations

`DeltaCommand` is a `oneof` wrapping 7 command types:

| Command | Description |
|---|---|
| `CloneTable` | Shallow clone of a source table to a target path; supports time travel |
| `VacuumTable` | VACUUM — remove unreferenced files; optional `retention_hours` |
| `UpgradeTableProtocol` | Set minimum reader/writer version on the table's Protocol action |
| `Generate` | Generate manifest files (e.g., `"symlink_format_manifest"`) |
| `CreateDeltaTable` | CREATE / CREATE IF NOT EXISTS / REPLACE / CREATE OR REPLACE with full column DDL |
| `AddFeatureSupport` | Add a table feature by name |
| `DropFeatureSupport` | Drop a table feature; optional `truncate_history` flag |

### `relations.proto` — Data-Returning Operations

`DeltaRelation` is a `oneof` wrapping 10 relation types. These are modeled as **relations** (not commands) because they return execution metrics as a DataFrame:

| Relation | Description |
|---|---|
| `Scan` | Table scan / `DeltaTable.toDF` — returns the table's data |
| `DescribeHistory` | Returns commit history DataFrame |
| `DescribeDetail` | Returns table detail info |
| `ConvertToDelta` | Convert Parquet table to Delta format |
| `RestoreTable` | Restore to prior version or timestamp |
| `IsDeltaTable` | Returns single-boolean DataFrame |
| `DeleteFromTable` | DML delete; target is a `spark.connect.Relation` (supports aliases) |
| `UpdateTable` | DML update with assignment list |
| `MergeIntoTable` | Full MERGE INTO: matched/not-matched/not-matched-by-source action lists + schema evolution flag |
| `OptimizeTable` | Compaction or Z-order; `zorder_columns` list |

### Extension Point Mechanism

Delta's proto messages are packed into `google.protobuf.Any` and placed in the `extension` field of Spark Connect's `Relation` or `Command` messages:

```
spark.connect.Relation.extension → Any(delta.connect.DeltaRelation)
spark.connect.Command.extension  → Any(delta.connect.DeltaCommand)
```

This is **entirely additive** — no changes to Spark's core proto schema.

### Discovery and Instantiation (Server Side)

The server plugins are registered via Spark configuration:

```
spark.connect.extensions.relation.classes = org.apache.spark.sql.connect.delta.DeltaRelationPlugin
spark.connect.extensions.command.classes  = org.apache.spark.sql.connect.delta.DeltaCommandPlugin
```

Spark Connect's plugin chain calls each registered `RelationPlugin.transform()` / `CommandPlugin.process()` with the raw proto bytes; Delta's plugins check `is(classOf[proto.DeltaRelation])` and return `Optional.empty()` if not a Delta message (allowing other plugins to handle it).

### Proto Parsing Safety

All server-side proto deserialization uses `cis.setRecursionLimit(recursionLimit)` sourced from `SparkEnv.get.conf.get(Connect.CONNECT_GRPC_MARSHALLER_RECURSION_LIMIT)`. This matches how Spark Connect itself parses plans, preventing stack overflows on deeply nested plans.

### Vendored Proto Copy

Delta's client module cannot import Spark Connect's proto classes directly as a regular compile dependency. It vendors a copy of Spark Connect's proto definitions under `spark-connect/common/src/main/protobuf/spark/connect/` for compilation. At runtime, `ImplicitProtoConversions` serializes between the two generated-class hierarchies (Delta's copy ↔ Spark's live classes) via a byte-level round-trip.

> [!NOTE] Client recursion limit gap
> `ImplicitProtoConversions` does not currently apply recursion limits on the client-to-server conversion path. There are `TODO` comments acknowledging this. Source: [[modules/spark-connect]] §§ "Recursion Limit on Proto Parsing"

Source: [[modules/spark-connect]]

---

## 5. Flink Connector SPIs (`delta-flink`)

`delta-flink` defines two additional internal SPIs (not published as stable APIs):

### `DeltaCatalog` Interface

```java
// flink/src/main/java/io/delta/flink/table/DeltaCatalog.java
public interface DeltaCatalog extends Serializable {
    default void open() {}
    TableDescriptor getTable(String tableId);
    void createTable(String tableId, StructType schema, List<String> partitions, Map<String,String> props);
    Map<String, String> getCredentials(String uuid);
}
```

- Not a Flink `org.apache.flink.table.catalog.Catalog` implementation — it is Delta's internal catalog abstraction.
- `getCredentials(uuid)` separates credential management from table resolution; credentials are keyed by stable UUID rather than table path (so renames don't require credential refetch).

### `DeltaTable` Interface

```java
// flink/src/main/java/io/delta/flink/table/DeltaTable.java
public interface DeltaTable extends Serializable, AutoCloseable {
    String getId();
    StructType getSchema();
    List<String> getPartitionColumns();
    void open();
    Optional<Snapshot> commit(CloseableIterable<Row> actions, String appId, long txnId, Map<String,String> props);
    void refresh();
    CloseableIterator<Row> writeParquet(String pathSuffix, CloseableIterator<FilteredColumnarBatch> data, Map<String, Literal> partitionValues) throws IOException;
}
```

Source: [[modules/connectors/flink]]

---

## Interface Comparison

| Interface | Layer | Zero-dep? | Discovery mechanism | Primary consumer |
|---|---|---|---|---|
| `Engine` SPI | Kernel | Yes (kernel-api has 0 runtime deps) | Direct instantiation by connector | delta-kernel-api (Flink, spark-v2, custom engines) |
| `FileSystemClient` | Kernel (sub-SPI) | Yes | Part of `Engine` impl | All Kernel I/O |
| `JsonHandler` | Kernel (sub-SPI) | Yes | Part of `Engine` impl | Log reading/writing |
| `ParquetHandler` | Kernel (sub-SPI) | Yes | Part of `Engine` impl | Data file I/O, checkpoints |
| `ExpressionHandler` | Kernel (sub-SPI) | Yes | Part of `Engine` impl | Data skipping predicate evaluation |
| `LogStore` | Storage | No (Hadoop required) | Hadoop config + reflection | delta-spark-v1, delta-kernel-defaults |
| `CommitCoordinatorClient` | Storage | No (network required for UC) | Name registry (Spark) or Engine SPI (Kernel) | delta-spark-v1 (UC tables), delta-kernel-unitycatalog |
| Delta Connect IDL | Transport | n/a | Spark config keys | delta-connect-client (Python/Scala), delta-connect-server |
| `DeltaCatalog` (Flink) | Flink connector | Yes | Direct instantiation | delta-flink implementations |
| `DeltaTable` (Flink) | Flink connector | Yes | Resolved via DeltaCatalog | delta-flink Flink sink |

---

## Related Documents

- [[cross_cutting/data_models]] — `ColumnarBatch`, `ColumnVector`, `FilteredColumnarBatch` types used in these interfaces
- [[cross_cutting/shared_utilities]] — `CoordinatedCommitsUtils`, adapters used with these interfaces
- [[modules/kernel]] — Full Engine SPI contract, DefaultEngine implementation
- [[modules/storage]] — Full LogStore + CommitCoordinatorClient implementation details
- [[modules/spark-connect]] — Delta Connect IDL, client/server plugin wiring
- [[modules/connectors/flink]] — Flink DeltaCatalog + DeltaTable SPI detail
- [[architecture/system_map]] — How these interfaces fit into the overall architecture
