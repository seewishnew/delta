---
title: "golden-tables"
tags: [module, test-fixture, connectors, golden-tables, parquet, delta-log]
layer: L4
last_updated: 2026-03-02
source_files:
  - "connectors/golden-tables/src/main/scala/io/delta/golden/GoldenTableUtils.scala"
  - "connectors/golden-tables/src/test/scala/io/delta/golden/GoldenTables.scala"
  - "connectors/golden-tables/src/main/resources/golden/"
related:
  - "[[module_manifest]]"
  - "[[kernel]]"
  - "[[spark]]"
  - "[[connectors/flink]]"
---

# golden-tables

## Purpose

`golden-tables` is a **test fixture module** that ships a curated corpus of pre-built Delta table
snapshots (binary Parquet data + `_delta_log` JSON/Parquet log files) as JAR resources. It has no
production runtime code and is never shipped as part of any published artifact. Its sole job is to
give `delta-kernel-defaults`, `delta-spark-v2`, and other test suites a stable, hermetic set of
real Delta tables covering edge cases that are impractical to generate live in every test run —
corrupted logs, specific checkpoint formats, special-character paths, timezone-variant data,
deletion vectors, column mapping modes, type widening, V2 checkpoints, and more.

The module follows a **generate-once, commit-binary** workflow:
- A special Spark-based test class (`GoldenTables.scala`) generates all tables on demand.
- The generated binary assets live in `src/main/resources/golden/` and are committed to version
  control.
- Consumer modules declare a `test`-scoped SBT dependency on `goldenTables` and access table paths
  via `GoldenTableUtils`.

---

## Source

```
connectors/golden-tables/
├── src/
│   ├── main/
│   │   ├── scala/io/delta/golden/
│   │   │   └── GoldenTableUtils.scala      ← classpath resolver (L4 detail below)
│   │   └── resources/golden/               ← ~90+ committed binary Delta table dirs
│   └── test/
│       └── scala/io/delta/golden/
│           └── GoldenTables.scala          ← Spark-backed table generator (run once)
```

---

## Public Interface

| Symbol | Type | Description |
|---|---|---|
| `GoldenTableUtils.goldenTablePath(name: String): String` | object method | Returns the absolute filesystem path of a named golden table from the classpath JAR resource |
| `GoldenTableUtils.goldenTableFile(name: String): File` | object method | Returns a `java.io.File` handle to the same resource |
| `GoldenTableUtils.allTableNames(): Seq[String]` | object method | Returns sorted relative paths of every directory under `golden/` that contains a `_delta_log` sub-directory |

---

## Key Dependencies

- **`io.delta:delta-spark:3.3.2` (test only)**: The generator (`GoldenTables.scala`) uses
  `DeltaLog`, `DeltaTable`, `OptimisticTransaction`, and related Spark Delta APIs to write the
  tables. This dependency is pinned to a released version of Delta Spark, *not* the in-repo
  snapshot, to ensure the generated table format is stable and representative of what external
  engines must read.
- **Apache Spark SQL (test only)**: Required by the generator for `SparkSession`,
  `SharedSparkSession`, `QueryTest`, and DataFrame APIs.
- **ScalaTest (test only)**: Generator is a ScalaTest `QueryTest`.
- **commons-io (test only)**: `FileUtils.copyDirectory` for copying previously generated table dirs
  (the generator re-uses prior outputs as base states for incremental scenarios).

---

## Modules That Depend On This

| Consumer | SBT scope | How used |
|---|---|---|
| `delta-kernel-defaults` | `test` | `DeltaTableReadsSuite`, `DeltaTableWritesSuite`, `DeltaTableReadsSuite`, `LogReplaySuite`, `DeletionVectorSuite`, `ScanSuite`, `PartitionPruningSuite`, `TableChangesSuite`, `CreateCheckpointSuite`, `SnapshotReportSuite`, and Parquet reader/writer suites all call `goldenTablePath(name)` to resolve table paths for read-path correctness tests. |
| `delta-spark-v2` | `test` | `SparkGoldenTableTest.java` iterates over `allTableNames()` to verify DSv2 kernel-backed reads produce identical results to the v1 (Spark-native) read path. |
| `kernel/examples/kernel-examples` | manual/integration | `ReadIntegrationTestSuite.java` accepts the golden-tables root dir as a CLI argument and runs sanity row-count checks against key tables to verify staged/released Kernel artifacts. |

---

## SBT Project Declaration

```scala
// build.sbt:1457-1474
lazy val goldenTables = (project in file("connectors/golden-tables"))
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .settings(
    name := "golden-tables",
    commonSettings,
    skipReleaseSettings,
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "commons-io" % "commons-io" % "2.8.0" % "test",
      "io.delta" %% "delta-spark" % "3.3.2" % "test",
      "org.apache.spark" %% "spark-sql" % defaultSparkVersion % "test",
      "org.apache.spark" %% "spark-catalyst" % defaultSparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-core" % defaultSparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % defaultSparkVersion % "test" classifier "tests"
    )
  )
```

`skipReleaseSettings` ensures this sub-project is never published to Maven. Both
`delta-kernel-defaults` (line 459) and `delta-spark-v2` (line 928) declare:

```scala
.dependsOn(goldenTables % "test")
```

---

## GoldenTableUtils — Component Detail

**Source**: `connectors/golden-tables/src/main/scala/io/delta/golden/GoldenTableUtils.scala` — lines 1–49

### Classpath Resolution Strategy

```scala
// GoldenTableUtils.scala:21-48
object GoldenTableUtils {
  lazy val classLoader = GoldenTableUtils.getClass.getClassLoader()
  lazy val goldenResourceURL = classLoader.getResource("golden")

  def goldenTablePath(name: String): String = {
    classLoader.getResource(s"golden/$name").getPath
  }

  def goldenTableFile(name: String): File = {
    new File(classLoader.getResource(s"golden/$name").getFile)
  }

  def allTableNames(): Seq[String] = {
    val root = new File(goldenResourceURL.getFile)
    def loop(dir: File): Seq[File] = {
      val children = Option(dir.listFiles()).getOrElse(Array.empty[File])
      val subdirs = children.filter(_.isDirectory)
      val here = if (new File(dir, "_delta_log").isDirectory) Seq(dir) else Seq.empty[File]
      here ++ subdirs.flatMap(loop)
    }
    val rootPath = root.toPath
    loop(root).map(f => rootPath.relativize(f.toPath).toString).sorted
  }
}
```

> [!NOTE] Why classpath resolution works across modules
> When `goldenTables` is declared as a `test`-scoped SBT dependency, SBT adds the
> `connectors/golden-tables/src/main/resources/` directory to the consuming module's test
> classpath. `classLoader.getResource("golden/$name")` then resolves to an absolute path into that
> directory on the local filesystem — not a JAR entry — which is why `.getPath` returns a usable
> `String` that Hadoop, Parquet, and kernel readers can open directly.
>
> `allTableNames()` recursively walks the resource root and identifies any sub-directory that
> contains a `_delta_log` child — this correctly handles the `hive/` sub-directory which nests
> multiple tables at depth 2.

### Call Graph

- **Called by**: `DeltaTableReadsSuite`, `LogReplaySuite`, `DeletionVectorSuite`, `ScanSuite`,
  `PartitionPruningSuite`, and all other kernel-defaults test suites via the import
  `import io.delta.golden.GoldenTableUtils.goldenTablePath`
- **Called by (Java)**: `SparkGoldenTableTest.java` via
  `GoldenTableUtils$.MODULE$.goldenTablePath(name)` (Scala companion object access from Java)
- **Called by (Java)**: `ReadIntegrationTestSuite.java` accepts the golden table root as a
  constructor argument and does not use `GoldenTableUtils` directly — the dir path is passed in
  from `run-kernel-examples.py`

---

## GoldenTables Generator — Component Detail

**Source**: `connectors/golden-tables/src/test/scala/io/delta/golden/GoldenTables.scala` — lines 1–1708

### Generation Workflow

```bash
# Regenerate all tables:
GENERATE_GOLDEN_TABLES=1 build/sbt 'goldenTables/test'

# Regenerate a single table (by test name):
GENERATE_GOLDEN_TABLES=1 build/sbt 'goldenTables/testOnly *GoldenTables -- -z "tbl_name"'
```

The generator checks `sys.env.contains("GENERATE_GOLDEN_TABLES")` at runtime. When the env var is
absent, all `generateGoldenTable` calls become no-ops (the ScalaTest framework registers them as
empty tests that instantly pass). When the env var is set, each call deletes the existing directory
and runs the Spark-based generator closure:

```scala
// GoldenTables.scala:93-102
private def generateGoldenTable(name: String,
    createTableFile: String => File = createGoldenTableFile)(generator: String => Unit): Unit = {
  if (shouldGenerateGoldenTables) {
    test(name) {
      val tablePath = createTableFile(name)
      JavaUtils.deleteRecursively(tablePath)
      generator(tablePath.getCanonicalPath)
    }
  }
}
```

### SparkConf Setup

The generator always sets:
- `spark.sql.extensions = io.delta.sql.DeltaSparkSessionExtension` — enables Delta DDL/DML SQL
- `spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog`
- `spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs = false` — suppresses `_SUCCESS`
  files that would pollute the golden table dirs
- `TimeZone.setDefault(America/Los_Angeles)` + `Locale.setDefault(Locale.US)` — baseline for
  timezone-sensitive tables; individual table generators override as needed

### Incremental copy pattern

Several tables build on earlier ones via `copyDir(src, dest)` before appending more writes. This
models realistic DML history where the golden table encodes state at a specific point mid-evolution:

```scala
// Example: snapshot series
generateGoldenTable("snapshot-data1") { tablePath =>
  copyDir("snapshot-data0", "snapshot-data1")           // starts from prior snapshot
  writeData((0 until 10).map(x => (x, s"data-1-$x")), "append", tablePath)
}
```

---

## Table Catalog

130+ table directories are committed under `src/main/resources/golden/`. Below is the full
enumeration organized by functional area.

### DeltaLog / Protocol Correctness

| Table name | Purpose |
|---|---|
| `checkpoint` | 15-commit log with a V1 checkpoint at version 10; tests basic checkpoint reading |
| `corrupted-last-checkpoint` | Valid log with empty/corrupted `_last_checkpoint` file |
| `corrupted-last-checkpoint-kernel` | Corrupted `_last_checkpoint` with overwrite + appends; kernel-specific variant |
| `deltalog-commit-info` | Commit with `CommitInfo` fields (job, notebook, cluster, txnId, etc.) |
| `deltalog-getChanges` | 3-commit log with `AddFile`, `AddCDCFile`, `RemoveFile`, `SetTransaction` |
| `deltalog-invalid-protocol-version` | Protocol `readerVersion=99` — should fail protocol check |
| `deltalog-state-reconstruction-without-metadata` | Log with no `Metadata` action — should fail state reconstruction |
| `deltalog-state-reconstruction-without-protocol` | Log with no `Protocol` action |
| `deltalog-state-reconstruction-from-checkpoint-missing-metadata` | Checkpoint stripped of `Metadata` action |
| `deltalog-state-reconstruction-from-checkpoint-missing-protocol` | Checkpoint stripped of `Protocol` action |
| `log-replay-latest-metadata-protocol` | Schema evolves across two commits + protocol upgrade; tests that log replay returns the latest metadata/protocol |
| `log-replay-dv-key-cases` | Table with deletion vectors and multiple DV key variants |
| `log-replay-special-characters` | Log with `special p@#h` in `AddFile` path (URI-encoded) |
| `log-replay-special-characters-a` | Add + remove of URI-encoded path |
| `log-replay-special-characters-b` | Add only of URI-encoded path |
| `versions-not-contiguous` | `00000000000000000001.json` manually deleted — should fail version gap check |
| `commit-info-containing-arbitrary-operationParams-types` | `CommitInfo.operationParameters` with mixed JSON types (from OPTIMIZE ZORDER) |
| `basic-with-vacuum-protocol-check-feature` | Table with `vacuumProtocolCheck` writer feature set |
| `only-checkpoint-files` | Checkpoint-interval-1 table where all commits are checkpointed |

### Snapshot / Time Travel

| Table name | Purpose |
|---|---|
| `snapshot-data0` | Initial snapshot (10 rows, 2 cols) |
| `snapshot-data1` | Appended to `snapshot-data0` |
| `snapshot-data2` | Overwritten `snapshot-data1` |
| `snapshot-data2-deleted` | Deleted `data-2-*` rows from `snapshot-data3` |
| `snapshot-data3` | Appended to `snapshot-data2` |
| `snapshot-repartitioned` | Repartitioned to 2 files (dataChange=false) |
| `snapshot-vacuumed` | Vacuumed with 0 retention — all older files removed |
| `update-deleted-directory` | 10 AddFiles then a checkpoint — original dir deleted post-generation |
| `time-travel-start` | Single commit with fixed timestamp `1540415658000` |
| `time-travel-start-start20` | Adds a commit 20 minutes after `start` |
| `time-travel-start-start20-start40` | Adds a commit 40 minutes after `start` |
| `time-travel-schema-changes-a` | Baseline for time travel with schema evolution |
| `time-travel-schema-changes-b` | Schema changed (added `part` col with `mergeSchema`) |
| `time-travel-partition-changes-a` | Partitioned by `part5` |
| `time-travel-partition-changes-b` | Overwritten with `part2` partitioning — different partition scheme |

### Data Reader — Primitive and Complex Types

| Table name | Purpose |
|---|---|
| `data-reader-primitives` | 11 rows, all scalar types (int, long, byte, short, boolean, float, double, string, binary, decimal) |
| `data-reader-partition-values` | Multi-column partition schema; all scalar types as partition cols |
| `data-reader-array-primitives` | Arrays of each primitive type |
| `data-reader-array-complex-objects` | Nested arrays, list-of-maps, list-of-structs |
| `data-reader-map` | Maps with various key/value type combinations |
| `data-reader-nested-struct` | 2-level nested struct |
| `data-reader-nullable-field-invalid-schema-key` | Array with null elements; edge-case for nullable fields |
| `data-reader-escaped-chars` | Partition directory with `+`, `%`, `!`, `"`, `#` chars |
| `data-reader-absolute-paths-escaped-chars` | Absolute paths with URI-encoded characters |
| `data-reader-timestamp_ntz` | `TIMESTAMP_NTZ` values in data + partition columns |
| `data-reader-timestamp_ntz-id-mode` | Same as above with column mapping mode=id |
| `data-reader-timestamp_ntz-name-mode` | Same with column mapping mode=name |
| `data-reader-date-types-UTC` | Timestamp + Date written in UTC timezone |
| `data-reader-date-types-Iceland` | Written in Iceland timezone |
| `data-reader-date-types-PST` | Written in PST |
| `data-reader-date-types-America` (America/Los_Angeles) | |
| `data-reader-date-types-Etc` (Etc/GMT+9) | |
| `data-reader-date-types-Asia` (Asia/Beirut) | |
| `data-reader-date-types-JST` | Written in JST |

### Decimal Types

| Table name | Purpose |
|---|---|
| `basic-decimal-table` | 4 rows, 4 decimal encodings (string partition, INT32, INT64, FIXED_LEN_BYTE_ARRAY) |
| `basic-decimal-table-legacy` | Same using Parquet legacy format |
| `decimal-various-scale-precision` | Wide schema with decimals at all precision/scale combinations (0–38) |
| `124-decimal-decode-bug` | Regression: `large_decimal` value `1000000` DecimalType(10,0) — #124 bug |
| `parquet-decimal-type` | 99,998 rows, 3 decimal encodings (INT32, INT64, FIXED_LEN_BYTE_ARRAY) |
| `parquet-decimal-dictionaries` | 1M rows with dictionary-encoded decimals (alias for `parquet-decimal-dictionaries-v2`) |
| `parquet-decimal-dictionaries-v1` | Parquet writer v1 (no dictionary for FIXED_LEN_BYTE_ARRAY) |
| `parquet-decimal-dictionaries-v2` | Parquet writer v2 (dictionary encoding supported) |

### Parquet Format

| Table name | Purpose |
|---|---|
| `parquet-all-types` | 200 rows, all Delta-supported types including `TIMESTAMP_NTZ`, nested struct, arrays, maps with NULLs; standard format |
| `parquet-all-types-legacy-format` | Same with `spark.sql.parquet.writeLegacyFormat=true` |

### Timestamp Encoding Variants

| Table name | Purpose |
|---|---|
| `kernel-timestamp-INT96` | Timestamps encoded as Parquet INT96 |
| `kernel-timestamp-TIMESTAMP_MICROS` | Timestamps as INT64 with TIMESTAMP_MICROS annotation |
| `kernel-timestamp-TIMESTAMP_MILLIS` | Timestamps as INT64 with TIMESTAMP_MILLIS annotation |
| `kernel-timestamp-PST` | Same data written in PST timezone |
| `kernel-timestamp-partition-col-ISO8601` | Timestamp partition column serialized in ISO8601 string format |

### Checkpoint Formats

| Table name | Purpose |
|---|---|
| `checkpoint` | V1 single-file Parquet checkpoint |
| `multi-part-checkpoint` | V1 multi-part checkpoint (`00000000000000000001.checkpoint.0000000001.0000000002.parquet` etc.) |
| `v2-checkpoint-json` | V2 checkpoint with JSON top-level file + `_sidecars/` directory |
| `v2-checkpoint-parquet` | V2 checkpoint with Parquet top-level file + `_sidecars/` |

### Column Mapping

| Table name | Purpose |
|---|---|
| `table-with-columnmapping-mode-id` | All Delta-supported types, column mapping mode=id |
| `table-with-columnmapping-mode-name` | Same with mode=name |
| `table-with-icebegCompatV2Enabled` | Column mapping mode=id + `enableIcebergCompatV2=true` |
| `data-skipping-basic-stats-all-types-columnmapping-id` | Stats table with column mapping id |
| `data-skipping-basic-stats-all-types-columnmapping-name` | Stats table with column mapping name |
| `data-reader-timestamp_ntz-id-mode` | TIMESTAMP_NTZ with column mapping id |
| `data-reader-timestamp_ntz-name-mode` | TIMESTAMP_NTZ with column mapping name |

### Deletion Vectors

| Table name | Purpose |
|---|---|
| `dv-partitioned-with-checkpoint` | Partitioned table (50 rows, 3 cols), DVs enabled, 15 deletes across 9 commits, final checkpoint |
| `dv-with-columnmapping` | Same but with `columnMapping.mode=name` |
| `log-replay-dv-key-cases` | 50-row table, 3 DV deletes across different DV key scenarios |

### Data Skipping

| Table name | Purpose |
|---|---|
| `data-skipping-basic-stats-all-types` | Stats for all stat-eligible types (int, long, byte, short, float, double, string, date, timestamp, decimal) |
| `data-skipping-basic-stats-all-types-checkpoint` | Same but with checkpoint triggered |
| `data-skipping-basic-stats-all-types-columnmapping-id` | Stats + column mapping id |
| `data-skipping-basic-stats-all-types-columnmapping-name` | Stats + column mapping name |
| `data-skipping-change-stats-collected-across-versions` | `dataSkippingNumIndexedCols` changes from all → 1 → 0 across versions |
| `data-skipping-partition-and-data-column` | Partitioned table where both partition and data filters can be applied |

### Type Widening

| Table name | Purpose |
|---|---|
| `type-widening` | Columns widened: `byte→long`, `int→long`, `float→double`, `byte→double`, `short→double`, `int→double`, `date→timestamp_ntz`, plus decimal-to-decimal widening |
| `type-widening-nested` | Type widening applied inside nested struct, map key/value, and array elements |

### Log Store / File I/O

| Table name | Purpose |
|---|---|
| `log-store-read` | Raw delta log files with custom text content (used by `ReadOnlyLogStoreSuite`) |
| `log-store-listFrom` | Files at indices 1–3 (index 0 missing) for `listFrom` boundary testing |
| `no-delta-log-folder` | Plain Parquet directory with no `_delta_log` — tests graceful failure |
| `canonicalized-paths-normal-a/b` | `AddFile` with `file:` scheme, `RemoveFile` without — path canonicalization test |
| `canonicalized-paths-special-a/b` | Same with URI-encoded special characters |

### Log Replay — DML History

| Table name | Purpose |
|---|---|
| `basic-with-inserts-deletes-checkpoint` | 14 commits: range appends + range deletes + checkpoint |
| `basic-with-inserts-updates` | 100-row table + `UPDATE SET str='N/A' WHERE id < 50` |
| `basic-with-inserts-merge` | 100-row table + full MERGE (matched update, not-matched insert, not-matched-by-source delete) |
| `basic-with-inserts-overwrite-restore` | Append → overwrite → `RESTORE TABLE TO VERSION AS OF 1` |
| `delete-re-add-same-file-different-transactions` | Same file path added, removed, re-added in separate transactions |

### Hive Connector Tables

Under the nested `hive/` subdirectory (11 sub-tables accessed as `hive/<name>`):

| Table name | Purpose |
|---|---|
| `hive/deltatbl-partition-prune` | Partitioned by `date`, `city`; used by `SparkGoldenTableTest` DSv2 scan + runtime filter tests |
| `hive/deltatbl-non-partitioned` | Simple 2-column, non-partitioned table |
| `hive/deltatbl-partitioned` | Partitioned by `c2` |
| `hive/deltatbl-schema-match` | 3 columns, partitioned by `b` |
| `hive/deltatbl-not-allow-write` | Table used to verify write-rejection paths |
| `hive/deltatbl-special-chars-in-partition-column` | Partition value with `+ =%` chars |
| `hive/deltatbl-map-types-correctly` | All Hive-compatible types (byte, binary, boolean, int, long, string, float, double, short, date, timestamp, decimal, array, map, struct) |
| `hive/deltatbl-column-names-case-insensitive` | Column names `FooBar`, `BarFoo` — case-insensitive access test |
| `hive/deltatbl-deleted-path` | Table after its directory was externally deleted |
| `hive/deltatbl-incorrect-format-config` | Table for format config mismatch testing |
| `hive/deltatbl-touch-files-needed-for-partitioned` | Partitioned; tests "touch files" behavior |

### Miscellaneous / Regression

| Table name | Purpose |
|---|---|
| `125-iterator-bug` | 12 appends of varying sizes — regression for iterator bug #125 |
| `124-decimal-decode-bug` | Regression for decimal decode overflow bug #124 |

---

## Usage Patterns

### Pattern 1 — Scala test import (kernel-defaults)

```scala
// e.g., DeltaTableReadsSuite.scala:25
import io.delta.golden.GoldenTableUtils.goldenTablePath

test("read primitives") {
  checkTable(
    path = goldenTablePath("data-reader-primitives"),
    expectedAnswer = (0 until 10).map(i => TestRow(...)) :+ TestRow.nullRow
  )
}
```

`goldenTablePath` is statically imported as a method — no object prefix needed. The returned path
string is passed directly to `Table.forPath(engine, path)` in the Kernel read path.

### Pattern 2 — Java test via Scala companion object (spark-v2)

```java
// SparkGoldenTableTest.java:651-655
private String goldenTablePath(String name) {
    return GoldenTableUtils$.MODULE$.goldenTablePath(name);
}
private List<String> getAllGoldenTableNames() {
    return scala.collection.JavaConverters.seqAsJavaList(GoldenTableUtils$.MODULE$.allTableNames());
}
```

Java accesses the Scala `object` via its generated `$` companion class.

### Pattern 3 — allTableNames() sweep (spark-v2 compatibility test)

```java
// SparkGoldenTableTest.java:578-621
@Test
public void testAllGoldenTables() {
    List<String> tableNames = getAllGoldenTableNames();
    List<String> unsupportedTables = Arrays.asList(
        "canonicalized-paths-normal-a", ... // known-unsupported list
    );
    for (String tableName : tableNames) {
        if (unsupportedTables.contains(tableName)) continue;
        if (hasOnlyDeltaLogSubdir(tablePath)) continue;  // corrupted/metadata-only tables
        // Compare v1 (Spark) vs v2 (kernel-backed DSv2) reads
        Dataset<Row> df  = spark.sql("SELECT * FROM `spark_catalog`.`delta`.`" + tablePath + "`");
        Dataset<Row> df2 = spark.sql("SELECT * FROM `dsv2`.`delta`.`" + tablePath + "`");
        assertEquals(df.schema(), df2.schema());
        checkAnswer(df2, df.collectAsList());
    }
}
```

This is the primary **cross-engine compatibility gate**: for every readable golden table, the DSv2
kernel-backed path must return identical schema and data as the v1 Spark path.

### Pattern 4 — External integration test (kernel-examples)

```java
// ReadIntegrationTestSuite.java:36-45
public ReadIntegrationTestSuite(String goldenTableDir) {
    this.goldenTableDir = goldenTableDir;  // passed from CLI, not via classpath
}
// run-kernel-examples.py passes the golden-tables resource dir explicitly
```

This usage bypasses `GoldenTableUtils` — the dir is passed as a command-line argument to enable
testing against a released/staged JAR without needing the SBT test classpath.

---

## Adding New Golden Tables

1. **Write the generator** in `GoldenTables.scala` using `generateGoldenTable("my-table-name") { tablePath => ... }`.
   - For a table under `hive/`, use `generateGoldenTable("my-table", createHiveGoldenTableFile) { ... }`.
   - If your table builds on an existing one, use `copyDir(src, dest)` before modifying it.

2. **Run generation**:
   ```bash
   GENERATE_GOLDEN_TABLES=1 build/sbt 'goldenTables/testOnly *GoldenTables -- -z "my-table-name"'
   ```

3. **Package golden-tables** so the resources are available to consuming modules:
   ```bash
   build/sbt 'goldenTables/compile'  # or: goldenTables/package
   ```

4. **Commit all binary artifacts** under `connectors/golden-tables/src/main/resources/golden/my-table-name/`.

5. **Reference in tests** via `goldenTablePath("my-table-name")`.

> [!NOTE] Why the binary assets are committed
> Delta table files (Parquet + JSON logs) are deterministic given fixed seeds and SparkConf, but
> re-generating them from scratch on every CI run would require a full Spark session and would be
> slow and fragile (timezone differences, Parquet writer version changes). Committing the output
> provides a hermetic corpus that any engine can read against without a Spark dependency.

---

## Test Coverage

The module itself has no test-coverage target — `GoldenTables.scala` is a *generator*, not a test
suite. Coverage for the fixture's correctness comes indirectly from the consuming test suites:
- `DeltaTableReadsSuite` (28 `goldenTablePath` calls): comprehensive correctness coverage of the
  Kernel read path against golden table data
- `LogReplaySuite` (16 calls): log replay, state reconstruction, checkpoint reading
- `SparkGoldenTableTest.testAllGoldenTables()`: cross-engine schema/data equality sweep
- `DeletionVectorSuite`, `ScanSuite`, `PartitionPruningSuite`, `TableChangesSuite`: targeted
  DV/scan/CDF feature correctness

Notable gap: there is no automated test that verifies all committed table binaries are bit-for-bit
reproducible from `GoldenTables.scala` — if the generator changes, the binary assets must be
manually regenerated and committed.

---

## Diagram Opportunity Flag

> FLAG FOR ORCHESTRATOR: The `testAllGoldenTables()` sweep in `SparkGoldenTableTest` implements a
> cross-engine read parity check: v1 (Spark DeltaLog) vs v2 (Kernel-backed DSv2). This binary
> comparison pattern warrants a small `sequenceDiagram` in the `delta-spark-v2` module's KG doc
> showing how the two read paths are exercised in parallel against the same golden table corpus.
> Relevant files: `spark/v2/src/test/java/io/delta/spark/internal/v2/read/SparkGoldenTableTest.java:578-621`.

---

## Related Documents

- [[module_manifest]] — SBT project graph; golden-tables is leaf node #1
- [[kernel]] — `delta-kernel-defaults` is the primary consumer
- [[spark]] — `delta-spark-v2` uses golden tables for DSv2 compatibility tests
- [[connectors/flink]] — Flink tests do not currently use golden-tables (they use kernel-defaults
  test resources directly)
- [[protocol/checkpoints]] — V1/V2/multi-part checkpoint golden tables encode concrete examples of
  the checkpoint format spec
- [[concepts/deletion_vectors]] — `dv-*` tables encode DV-format binary assets
- [[concepts/column_mapping]] — `*-columnmapping-*` and `*-id-mode`/`*-name-mode` tables
