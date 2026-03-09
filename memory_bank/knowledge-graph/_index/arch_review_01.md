---
title: "Architecture Review 01"
tags: [review, architecture, feedback, L1, L2]
last_updated: 2026-03-02
---

# Architecture Review 01

> Produced by `kg-arch-reviewer`. Subject to adjudication by `kg-feedback-verifier` before fixes are applied.
> Date: 2026-03-02

---

## Overall Assessment

The six L1/L2 architecture documents are structurally coherent and correctly describe the dominant design decisions of the Delta Lake monorepo: protocol-first layering, the engine-agnostic Kernel design, pluggable LogStore, UniForm metadata conversion, and Spark Connect extensibility. The dependency graphs, data flow sequences, and most subsystem descriptions are accurate and well-grounded in the L3/L4 explorer documents and source code.

However, three **MAJOR** factual errors were found through direct source verification:

1. A fabricated interface (`CommitCoordinatorClientHandler`) is shown as a 6th method of the `Engine` SPI that does not exist in `Engine.java`.
2. A fabricated class (`SparkEngine`) is shown as the concrete `Engine` SPI implementation in `delta-spark-v2` — no such class exists; the module uses `DefaultEngine` directly.
3. The cross-Spark support table incorrectly marks Iceberg UniForm as "Yes" for Spark 4.1.0 — source confirms it is disabled.

One additional MAJOR factual error concerns the publish status of `delta-connect-*` artifacts: they **are** published to Maven (they have `releaseSettings`), yet all three are described as "(internal, not published standalone)".

These errors, if unaddressed, would mislead readers about (a) the shape of the Engine SPI, (b) how `delta-spark-v2` works, and (c) which Delta features are available to end-users on the default Spark build.

---

## Findings

### [MAJOR] Engine SPI contains a fabricated `CommitCoordinatorClientHandler` method

- **Document**: `memory_bank/knowledge-graph/cross_cutting/interfaces_idl.md`
- **Section**: "1. Engine SPI — Interface Definition"
- **Finding**: The code snippet shown for `Engine.java` includes a 6th method:
  ```java
  default CommitCoordinatorClientHandler getCommitCoordinatorClientHandler() { ... }
  ```
  This method and the `CommitCoordinatorClientHandler` interface do not exist in the source. The actual `Engine.java` has exactly five members: `getExpressionHandler()`, `getJsonHandler()`, `getFileSystemClient()`, `getParquetHandler()`, `getMetricsReporters()`.
- **Evidence**:
  - Source: `kernel/kernel-api/src/main/java/io/delta/kernel/engine/Engine.java` — only 5 methods, no `getCommitCoordinatorClientHandler` (verified by direct read).
  - `ls kernel/kernel-api/src/main/java/io/delta/kernel/engine/` — no `CommitCoordinatorClientHandler.java` file exists.
  - `grep -r CommitCoordinatorClientHandler kernel/` — zero matches in any `.java` file.
- **Consequence**: A reader implementing the Engine SPI would search for a non-existent interface. The sub-interfaces count throughout the document ("5 sub-interfaces" in `active_context.md`) is also now inconsistent with the 6-item list in `interfaces_idl.md`.
- **Suggested correction**: Remove `getCommitCoordinatorClientHandler()` from the `Engine.java` interface snippet. Update the sub-interfaces section accordingly. If coordinated-commit support via the Engine is a future planned extension, add a `> [!NOTE] Not yet in Engine SPI` callout rather than showing it as present.

---

### [MAJOR] `SparkEngine` named as Engine SPI implementation in `delta-spark-v2` — class does not exist

- **Documents**: `memory_bank/knowledge-graph/architecture/system_map.md` (Engine-Agnostic Design Pattern diagram), `memory_bank/knowledge-graph/cross_cutting/interfaces_idl.md` ("Concrete Implementations" table)
- **Section**: "Engine-Agnostic Design Pattern — The Kernel + Engine SPI" and interfaces_idl §"Concrete Implementations"
- **Finding**:
  - `system_map.md` diagram shows `SE[SparkEngine\ndelta-spark-v2]` as a named Engine SPI implementation.
  - `interfaces_idl.md` table entry: `SparkEngine (internal) | delta-spark-v2 | Wraps Spark's native vectorized Parquet reader and Spark's JSON handler.`
  - Neither a class nor any reference to `SparkEngine` exists in `spark/v2/`.
- **Evidence**:
  - `Glob *Engine*.java` across the entire repo returns exactly: `Engine.java`, `DefaultEngine.java`, `DefaultEngineErrors.java`, `KernelEngineException.java`. No `SparkEngine.java`.
  - `SparkTable.java` (line 147): `Engine kernelEngine = DefaultEngine.create(this.hadoopConf);`
  - `SparkScan.java` (line 270, 281): `final Engine tableEngine = DefaultEngine.create(hadoopConf);`
  - `PathBasedSnapshotManager.java` (line 42): `this(tablePath, DefaultEngine.create(requireNonNull(hadoopConf, "hadoopConf is null")));`
  - `delta-spark-v2` uses `DefaultEngine` from `delta-kernel-defaults` directly — not a custom Spark-optimized Engine.
- **Consequence**: A reader would believe that `delta-spark-v2` provides Spark-native vectorized I/O through the Engine SPI when it actually uses the same Hadoop+Parquet `DefaultEngine` as Flink does. The architectural insight about the dual read path is slightly misleading.
- **Suggested correction**: In `system_map.md` Engine diagram, replace `SE[SparkEngine\ndelta-spark-v2]` with `SE[DefaultEngine\n(also used by spark-v2)]`. In `interfaces_idl.md`, remove the `SparkEngine (internal)` row from the Concrete Implementations table or replace it with an accurate note explaining that `delta-spark-v2` uses `DefaultEngine` from `kernel-defaults`.

---

### [MAJOR] Cross-Spark UniForm support table incorrectly shows Iceberg = "Yes" for Spark 4.1.0

- **Document**: `memory_bank/knowledge-graph/architecture/module_dependencies.md`
- **Section**: "Build Cross-Spark Version Support"
- **Finding**: The table shows:
  ```
  | 4.1.0 | Supported + published (default) | Yes | No | spark41 spec; default build target |
  ```
  The "Yes" for Iceberg UniForm on Spark 4.1.0 is incorrect.
- **Evidence**:
  - `project/CrossSparkVersions.scala` lines 275–283:
    ```scala
    private val spark41 = SparkVersionSpec(
      fullVersion = "4.1.0",
      ...
      supportIceberg = false,
      supportHudi = false,
      ...
    )
    ```
  - `build.sbt` lines 1170–1174: `Compile / skip := !supportIceberg, publish / skip := !supportIceberg`
  - Both `supportIceberg` AND `supportHudi` are `false` for `spark41`.
- **Consequence**: Users and documentation readers on the **default** Delta build (Spark 4.1.0) would believe Iceberg UniForm is available when it is not. `delta-iceberg` is only compiled and published for `spark40` (4.0.1). This is a critical correctness issue for users making version choices.
- **Suggested correction**: Update the table to:
  ```
  | 4.1.0 | Supported + published (default) | **No** | No | spark41 spec; default build target; UniForm disabled |
  ```
  Add a `> [!WARNING]` note beneath the table: "Both UniForm Iceberg and UniForm Hudi are disabled for the default build (Spark 4.1.0). UniForm is only available when building against Spark 4.0.1."

---

### [MAJOR] `delta-connect-*` artifacts described as "not published standalone" — contradicted by `build.sbt`

- **Document**: `memory_bank/knowledge-graph/architecture/module_dependencies.md`
- **Section**: "Published Artifact Matrix"
- **Finding**: All three connect modules show "(internal, not published standalone)" in the Maven Coordinate column:
  - `delta-connect-common | (internal, not published standalone)`
  - `delta-connect-client | (internal, not published standalone)`
  - `delta-connect-server | (internal, not published standalone)`
- **Evidence**:
  - `build.sbt` lines 167–192 (`connectCommon`): has `releaseSettings` → `publishArtifact := true`
  - `build.sbt` lines 194–240 (`connectClient`): has `releaseSettings`
  - `build.sbt` lines 242–290 (`connectServer`): has `releaseSettings`
  - `build.sbt` line 170: `name := "delta-connect-common"` with `releaseSettings`
  - Module manifest's `Build System Overview` section lists `delta-connect-*` under Published artifacts.
  - `active_context.md` "Published artifacts": includes `delta-connect-*`.
- **Consequence**: A reader would believe that the Connect protocol JARs must be obtained by building from source; they are actually available as Maven artifacts.
- **Suggested correction**: Update the Published Artifact Matrix to include proper Maven coordinates for all three connect modules. Based on `build.sbt` naming patterns: `io.delta:delta-connect-common_<spark>_<scala>:<version>`, `io.delta:delta-connect-client_<spark>_<scala>:<version>`, `io.delta:delta-connect-server_<spark>_<scala>:<version>`.

---

### [MINOR] `module_dependencies.md` references non-existent class `LogStoreBasedCommitCoordinatorClient`

- **Document**: `memory_bank/knowledge-graph/architecture/module_dependencies.md`
- **Section**: "2. delta-kernel-api — The Protocol Specification in Code" (cross-cutting foundation)
- **Finding**: Text states: "`DefaultFileSystemClient` wraps `LogStore`; `LogStoreBasedCommitCoordinatorClient` adapts it to Engine SPI"
- **Evidence**:
  - `grep -r LogStoreBasedCommitCoordinatorClient kernel/` → zero matches in any `.java` file.
  - No class with this name exists in `kernel-defaults` or `delta-storage`.
  - `kernel/kernel-defaults/src/main/java/io/delta/kernel/defaults/engine/` contains: `DefaultEngine`, `DefaultFileSystemClient`, `DefaultJsonHandler`, `DefaultParquetHandler`, `DefaultExpressionHandler` — no commit coordinator adapter.
- **Consequence**: Readers of the cross-cutting foundation section cannot locate this described class.
- **Suggested correction**: Remove the reference to `LogStoreBasedCommitCoordinatorClient`. The accurate description is that `DefaultEngine` wraps `LogStoreProvider` (internal) for file I/O; commit coordinator support in Kernel is handled through `CommitCoordinatorClient` in `delta-storage` via separate Kernel-internal pathways (not through a named adapter in `DefaultEngine`).

---

### [MINOR] `executive_summary.md` WARNING omits that Iceberg UniForm is also disabled for Spark 4.1.0

- **Document**: `memory_bank/knowledge-graph/overview/executive_summary.md`
- **Section**: "Fragility / Architectural Debt" — UniForm Hudi warning
- **Finding**: The warning reads: "`delta-hudi` is only compiled and published for Spark 4.0.1. It is disabled for Spark 4.1.0 and the unreleased 4.2.0-SNAPSHOT. Users on Spark 4.1.0 cannot use UniForm Hudi." — Iceberg is not mentioned.
- **Evidence**: `CrossSparkVersions.scala`: `spark41.supportIceberg = false` (same file, line 279). Both Iceberg and Hudi are disabled for the default build.
- **Suggested correction**: Extend the WARNING to cover both formats:
  > `delta-iceberg` and `delta-hudi` are only compiled and published for Spark 4.0.1. Both are disabled for Spark 4.1.0 (the default build target). UniForm (both Iceberg and Hudi) is unavailable to users on the default Spark 4.1.0 build.

---

### [MINOR] `interfaces_idl.md` Overview says "three layers" but lists four

- **Document**: `memory_bank/knowledge-graph/cross_cutting/interfaces_idl.md`
- **Section**: "Overview"
- **Finding**: The opening paragraph says "Delta Lake exposes **three** layers of pluggable interfaces" but then immediately lists 4 numbered items: Engine SPI, LogStore SPI, CommitCoordinatorClient SPI, Delta Connect IDL.
- **Suggested correction**: Change "three layers" to "four layers" (or list only the three storage/kernel-level SPIs, placing the IDL in a separate "transport contracts" category).

---

## Missing Coverage

No modules from the manifest are entirely absent from the architecture docs. All 23 modules are at least referenced in the system map or module dependency graph. The following minor gap was noted:

- `delta-kernel-unitycatalog` does not appear in the `module_dependencies.md` **Published Artifact Matrix** (it is documented as "internal" correctly in the module manifest, so no matrix entry is required — noting for completeness).
- `delta-spark-unitycatalog` (integration tests only) is correctly omitted from the Published Artifact Matrix.
- `benchmarks` and `protocol_rfcs` are correctly omitted.

---

## Diagram Verdict

| Diagram | Document | Verdict | Key Issues |
|---|---|---|---|
| ASCII layered architecture | `system_map.md` | Accurate | None |
| Full dependency graph (Mermaid `graph TD`) | `system_map.md` | Accurate | None |
| Write Path sequence | `system_map.md` | Accurate | None |
| Read Path (Kernel, `graph LR`) | `system_map.md` | Accurate | None |
| Control Plane / OCC diagram | `system_map.md` | Accurate | None |
| Engine-Agnostic Design Pattern | `system_map.md` | **Minor issues** | `SparkEngine` node should be `DefaultEngine (also spark-v2)`; `SE` label is inaccurate (see MAJOR finding #2) |
| LogStore implementations | `system_map.md` | Accurate | None |
| Coordinated Commits | `system_map.md` | Accurate | None |
| UniForm conversion | `system_map.md` | Accurate | `HiveCatalog` confirmed present in `IcebergTransactionUtils.scala` |
| Delta Sharing sequence | `system_map.md` | Accurate | None |
| Spark Connect sequence | `system_map.md` | Accurate | None |
| SBT dependency graph (Mermaid) | `module_dependencies.md` | Accurate | None |
| Kernel↔Spark data model bridge | `data_models.md` | Accurate | None |
| Cross-module utility dependency map | `shared_utilities.md` | Accurate | None |

---

## Consistency Flags

| Source | Architecture Doc Claims | L3/L4 or Source Reality |
|---|---|---|
| `Engine.java` (source) | `interfaces_idl.md` lists `CommitCoordinatorClientHandler` as a 6th Engine method | Only 5 methods exist; no such interface |
| `SparkScan.java`, `SparkTable.java` (source) | `interfaces_idl.md` and `system_map.md` list `SparkEngine` as Engine implementation | Both use `DefaultEngine.create(hadoopConf)` |
| `CrossSparkVersions.scala` line 279 (source) | `module_dependencies.md` table: Iceberg UniForm "Yes" for Spark 4.1.0 | `supportIceberg = false` for spark41 |
| `build.sbt` `releaseSettings` on all three connect modules (source) | `module_dependencies.md`: connect modules "(internal, not published standalone)" | All three have `publishArtifact := true` via `releaseSettings` |
| `CrossSparkVersions.scala` line 279 (source) | `executive_summary.md` WARNING omits Iceberg UniForm disabled status | Iceberg also disabled for Spark 4.1.0 |

---

## Pass Verdict

- [ ] PASS — architecture documentation accurately represents the system
- [ ] PASS WITH MINOR ISSUES — accurate overall; minor corrections before finalising executive summary
- [x] **REQUEST FIXES** — significant inaccuracies that must be corrected before the orchestrator writes the executive summary

**Rationale**: Three of the four MAJOR findings are factual errors about the codebase (non-existent class, non-existent interface method, incorrect feature availability table) that would mislead engineers using these documents. The fourth (publish status of connect modules) would mislead consumers trying to use the artifact. These should be corrected in the architecture documents before the executive summary is finalised.

**Priority order for fixes**:
1. `module_dependencies.md` — Iceberg UniForm "Yes" → "No" for Spark 4.1.0 (highest user impact)
2. `module_dependencies.md` — connect module publish status corrected
3. `interfaces_idl.md` + `system_map.md` — remove `CommitCoordinatorClientHandler` from Engine SPI
4. `interfaces_idl.md` + `system_map.md` — replace `SparkEngine` with `DefaultEngine (also used by spark-v2)`
5. `executive_summary.md` — extend UniForm WARNING to cover Iceberg
6. `module_dependencies.md` — remove `LogStoreBasedCommitCoordinatorClient` reference
7. `interfaces_idl.md` — fix "three layers" → "four layers"
