---
title: "KG Feedback Adjudication — Architecture Review 01"
tags: [adjudication, feedback, review, architecture]
last_updated: 2026-03-02
source_review: "memory_bank/knowledge-graph/_index/arch_review_01.md"
---

# KG Feedback Adjudication — Architecture Review 01

> Source review: `arch_review_01.md`
> Adjudicated by: kg-feedback-verifier
> Date: 2026-03-02

---

## Summary

The arch-reviewer's feedback is uniformly well-grounded. All seven findings have been verified against the actual source code and all seven are confirmed. No findings were refuted. The three MAJOR factual errors the reviewer flagged (fabricated `CommitCoordinatorClientHandler` Engine method, fabricated `SparkEngine` class, incorrect Iceberg UniForm availability for Spark 4.1.0) are definitively confirmed by direct source reads. The fourth MAJOR finding (connect module publish status) is confirmed by `releaseSettings` in `build.sbt`. Both MINOR factual findings (non-existent `LogStoreBasedCommitCoordinatorClient` class, UniForm WARNING gap in executive summary) are also confirmed. The "three layers" counting error in `interfaces_idl.md` is confirmed. No producer counter-arguments were submitted. All seven items require producer fixes before the architecture documents are considered accurate.

---

## Adjudicated Findings

### Finding 1: `CommitCoordinatorClientHandler` as 6th Engine SPI method [MAJOR]

- **Reviewer's claim**: `interfaces_idl.md` shows a 6th Engine method `getCommitCoordinatorClientHandler()` and a `CommitCoordinatorClientHandler` sub-interface that do not exist in source.
- **What I verified**:
  - Read `kernel/kernel-api/src/main/java/io/delta/kernel/engine/Engine.java` in full (64 lines).
  - Read `kernel/kernel-defaults/src/main/java/io/delta/kernel/defaults/engine/DefaultEngine.java` lines 1–57.
  - Ran Glob `**/*Engine*.java` across the entire repo.
  - The KG document `cross_cutting/interfaces_idl.md` lines 44–48, 101–103, 241.
- **Evidence**:
  - `Engine.java` has exactly **5 members**: `getExpressionHandler()`, `getJsonHandler()`, `getFileSystemClient()`, `getParquetHandler()`, `getMetricsReporters()`. Line 64 is the closing brace; there is no 6th method.
  - Glob `**/*Engine*.java` returns exactly 4 files: `Engine.java`, `DefaultEngine.java`, `DefaultEngineErrors.java`, `KernelEngineException.java`. No `CommitCoordinatorClientHandler.java` exists.
  - `DefaultEngine.java` implements only the 5 methods above; no commit coordinator method is present.
  - `interfaces_idl.md` line 47: `default CommitCoordinatorClientHandler getCommitCoordinatorClientHandler() { ... }` — fabricated.
  - `interfaces_idl.md` line 103: "Implemented by `DefaultEngine` in `kernel-defaults` via `LogStoreBasedCommitCoordinatorClient`" — references two non-existent entities.
  - `interfaces_idl.md` line 241: "Kernel: `CommitCoordinatorClientHandler.getCommitCoordinatorClient(name, conf)` in the Engine SPI" — fabricated API call.
- **Ruling**: ✅ Confirmed
- **Action**: Fix required — three locations in `interfaces_idl.md`

**Fix instruction**:
1. `interfaces_idl.md` line 47: Remove the entire line `default CommitCoordinatorClientHandler getCommitCoordinatorClientHandler() { ... }` from the Engine.java code snippet.
2. `interfaces_idl.md` section `#### CommitCoordinatorClientHandler` (lines 101–103): Remove this entire sub-section. The `CommitCoordinatorClientHandler` sub-interface does not exist in the Engine SPI. If coordinated commit support via the Engine is a planned future addition, replace with a `> [!NOTE] CommitCoordinatorClient support is not (yet) part of the Engine SPI. Coordinated commits in Kernel are handled through internal pathways, not through Engine-exposed interfaces.`
3. `interfaces_idl.md` line 241: Replace `CommitCoordinatorClientHandler.getCommitCoordinatorClient(name, conf)` with an accurate description: e.g., "Kernel resolves `CommitCoordinatorClient` implementations internally (not via the Engine SPI)."

---

### Finding 2: `SparkEngine` as Engine SPI implementation in `delta-spark-v2` [MAJOR]

- **Reviewer's claim**: `system_map.md` and `interfaces_idl.md` list `SparkEngine` as a named Engine SPI implementation in `delta-spark-v2`. No such class exists.
- **What I verified**:
  - Grep `SparkEngine` across `spark/` directory — zero matches.
  - Glob `**/*Engine*.java` across entire repo — only 4 results, none named `SparkEngine`.
  - Grep `DefaultEngine.create` in `spark/` directory.
  - The KG document `interfaces_idl.md` Concrete Implementations table; `system_map.md` Engine-Agnostic Design Pattern diagram.
- **Evidence**:
  - Grep `SparkEngine` in `spark/` → no matches anywhere in the repository.
  - `spark/v2/src/main/java/io/delta/spark/internal/v2/catalog/SparkTable.java` line 147: `Engine kernelEngine = DefaultEngine.create(this.hadoopConf);`
  - `spark/v2/src/main/java/io/delta/spark/internal/v2/read/SparkScan.java` lines 270, 281: `final Engine tableEngine = DefaultEngine.create(hadoopConf);`
  - `spark/v2/src/main/java/io/delta/spark/internal/v2/read/SparkMicroBatchStream.java` line 174: `this.engine = DefaultEngine.create(hadoopConf);`
  - `spark/v2/src/main/java/io/delta/spark/internal/v2/snapshot/PathBasedSnapshotManager.java` line 42: `DefaultEngine.create(requireNonNull(hadoopConf, ...))`
  - All four call sites in `delta-spark-v2` use `DefaultEngine` from `delta-kernel-defaults` — no custom Spark Engine implementation exists.
- **Ruling**: ✅ Confirmed
- **Action**: Fix required — two documents

**Fix instruction**:
1. `system_map.md` Engine-Agnostic Design Pattern diagram: Replace the `SE[SparkEngine\ndelta-spark-v2]` node with `SE[DefaultEngine\n(kernel-defaults;\nalso used by spark-v2)]`. The diagram should make clear that `delta-spark-v2` does not provide a custom Engine; it reuses `DefaultEngine`.
2. `interfaces_idl.md` Concrete Implementations table: Remove the `SparkEngine (internal) | delta-spark-v2 | Wraps Spark's native vectorized Parquet reader and Spark's JSON handler.` row entirely. Replace with a note or a row accurately describing the situation: `DefaultEngine | delta-kernel-defaults | Hadoop+Parquet reference implementation; used by delta-flink AND delta-spark-v2 (both use DefaultEngine.create(hadoopConf) directly).`

---

### Finding 3: Iceberg UniForm marked "Yes" for Spark 4.1.0 [MAJOR]

- **Reviewer's claim**: `module_dependencies.md` cross-Spark version table incorrectly shows Iceberg UniForm = "Yes" for Spark 4.1.0.
- **What I verified**:
  - Read `project/CrossSparkVersions.scala` lines 265–290.
  - Read `build.sbt` lines 1165–1180.
  - The KG document `module_dependencies.md` cross-Spark support table row for 4.1.0.
- **Evidence**:
  - `CrossSparkVersions.scala` line 279: `supportIceberg = false` for `spark41`.
  - `CrossSparkVersions.scala` line 280: `supportHudi = false` for `spark41`.
  - `build.sbt` lines 1170–1174: `Compile / skip := !supportIceberg`, `publish / skip := !supportIceberg` — the entire `delta-iceberg` module is skipped for compilation and publishing when `supportIceberg = false`.
  - The `module_dependencies.md` table row `| 4.1.0 | Supported + published (default) | Yes | No | ...` is directly contradicted by source.
- **Ruling**: ✅ Confirmed
- **Action**: Fix required — `module_dependencies.md`

**Fix instruction**:
In `module_dependencies.md` cross-Spark version support table, update the Spark 4.1.0 row:
- Change Iceberg UniForm column from `Yes` to `**No**`
- Update the Notes column to include: `spark41 spec; default build target; both UniForm Iceberg and Hudi disabled`
- Add the following `[!WARNING]` block immediately beneath the table:
  > `> [!WARNING] UniForm (both Iceberg and Hudi) is disabled for the default Spark 4.1.0 build. `delta-iceberg` and `delta-hudi` are compiled and published only for Spark 4.0.1. Users on Spark 4.1.0 (the default build target) cannot use either UniForm format. Source: `project/CrossSparkVersions.scala` lines 275–284, `build.sbt` lines 1170–1174.`

---

### Finding 4: `delta-connect-*` modules described as "not published standalone" [MAJOR]

- **Reviewer's claim**: `module_dependencies.md` Published Artifact Matrix shows all three connect modules as "(internal, not published standalone)" — this is contradicted by `build.sbt`.
- **What I verified**:
  - Read `build.sbt` lines 167–290 (full `connectCommon`, `connectClient`, `connectServer` project definitions).
  - Read `build.sbt` lines 1590–1605 (`releaseSettings` definition).
  - The KG document `module_dependencies.md` Published Artifact Matrix rows for connect modules.
- **Evidence**:
  - `build.sbt` line 173: `connectCommon` has `releaseSettings`.
  - `build.sbt` line 200: `connectClient` has `releaseSettings`.
  - `build.sbt` line 249: `connectServer` has `releaseSettings`.
  - `build.sbt` line 1590–1592: `lazy val releaseSettings = Seq(publishMavenStyle := true, publishArtifact := true, ...)`.
  - None of the three connect project definitions contain `publish / skip := true` or `skipReleaseSettings`. (Compare: `deltaSuiteGenerator` at line 298 explicitly uses `skipReleaseSettings` with comment "Internal module - not published to Maven".)
  - `module_dependencies.md` lines 29–31: All three show `(internal, not published standalone)` — factually wrong.
- **Ruling**: ✅ Confirmed
- **Action**: Fix required — `module_dependencies.md`

**Fix instruction**:
In `module_dependencies.md` Published Artifact Matrix, replace the three connect module rows with accurate Maven coordinates matching the naming pattern from `build.sbt` (names: `delta-connect-common`, `delta-connect-client`, `delta-connect-server`, all cross-versioned):

| Module | Maven Coordinate | Description | Key Dependencies |
|---|---|---|---|
| `delta-connect-common` | `io.delta:delta-connect-common_<spark>_<scala>:<version>` | Protobuf IDL + gRPC stubs for Delta Connect protocol | spark-connect-common (provided) |
| `delta-connect-client` | `io.delta:delta-connect-client_<spark>_<scala>:<version>` | Client-side Spark Connect DeltaTable planner | spark-connect-client-jvm (provided) |
| `delta-connect-server` | `io.delta:delta-connect-server_<spark>_<scala>:<version>` | Server-side Spark Connect plugin (RelationPlugin + CommandPlugin) | Spark Connect (provided) + delta-spark |

---

### Finding 5: Non-existent class `LogStoreBasedCommitCoordinatorClient` in `module_dependencies.md` [MINOR]

- **Reviewer's claim**: `module_dependencies.md` references a class `LogStoreBasedCommitCoordinatorClient` that doesn't exist in source.
- **What I verified**:
  - Grep `LogStoreBasedCommitCoordinatorClient` across entire repository.
  - Read `kernel/kernel-defaults/src/main/java/io/delta/kernel/defaults/engine/DefaultEngine.java`.
- **Evidence**:
  - Grep across the entire repo returns zero matches in any `.java` or `.scala` file. The class name appears only in KG documentation files (`module_dependencies.md`, `interfaces_idl.md`, `storage.md`) and in the review/adjudication reports — never in source code.
  - `DefaultEngine.java` wraps a `FileIO`/`HadoopFileIO` abstraction. It implements the 5 Engine SPI methods. No commit coordinator adapter exists as a named class in `kernel-defaults`.
  - Note: `storage.md` line 817 and line 859 also reference this non-existent class — those are out of scope for this adjudication (L3 document, not L2 architecture doc) but should be flagged for a follow-up L3 fix pass.
- **Ruling**: ✅ Confirmed
- **Action**: Fix required — `module_dependencies.md` (and flagged for follow-up in `storage.md`)

**Fix instruction**:
In `module_dependencies.md` section "2. delta-kernel-api — The Protocol Specification in Code", replace:
> `DefaultFileSystemClient` wraps `LogStore`; `LogStoreBasedCommitCoordinatorClient` adapts it to Engine SPI

With an accurate description:
> `DefaultEngine` provides Hadoop-based I/O via `HadoopFileIO`; coordinated commit support is handled through Kernel-internal pathways in `delta-storage` (not through a named adapter class in `DefaultEngine`).

**Ancillary flag (out of scope for this adjudication but must be tracked)**: `storage.md` lines 817 and 859 also reference `LogStoreBasedCommitCoordinatorClient`. This should be corrected in the next L3 detail-review pass for the storage module.

---

### Finding 6: `executive_summary.md` WARNING omits Iceberg UniForm disabled for Spark 4.1.0 [MINOR]

- **Reviewer's claim**: The executive summary WARNING only mentions Hudi being disabled for 4.1.0; it silently omits that Iceberg is also disabled.
- **What I verified**:
  - Read `memory_bank/knowledge-graph/overview/executive_summary.md` — grep for UniForm/Hudi/Iceberg context.
  - `project/CrossSparkVersions.scala` lines 279–280.
- **Evidence**:
  - `executive_summary.md` lines 133–134: `> [!WARNING] UniForm Hudi disabled for Spark 4.1.0+` — mentions only `delta-hudi`.
  - `CrossSparkVersions.scala` line 279: `supportIceberg = false` for `spark41` — both formats are disabled.
  - `executive_summary.md` line 60–61: The technology stack table correctly shows `delta-iceberg` and `delta-hudi` as "Spark 4.0.1 only" in the Notes column (line 61: `(Spark 4.0.1 only)`), but the WARNING block only surfaced Hudi — the higher-profile omission is Iceberg, since most users are more likely to pursue Iceberg interop than Hudi.
- **Ruling**: ✅ Confirmed
- **Action**: Fix required — `executive_summary.md`

**Fix instruction**:
In `executive_summary.md`, replace the existing WARNING block (lines 133–134):
```
> [!WARNING] UniForm Hudi disabled for Spark 4.1.0+
> `delta-hudi` is only compiled and published for Spark 4.0.1. It is disabled for Spark 4.1.0 and the unreleased 4.2.0-SNAPSHOT. Users on Spark 4.1.0 cannot use UniForm Hudi. Source: [[modules/connectors/uniform-hudi]]
```
With:
```
> [!WARNING] UniForm (Iceberg and Hudi) disabled for Spark 4.1.0+
> Both `delta-iceberg` and `delta-hudi` are only compiled and published for Spark 4.0.1. Both are disabled for Spark 4.1.0 (the default build target) and the unreleased 4.2.0-SNAPSHOT. Users on the default Spark 4.1.0 build cannot use UniForm in either format. Source: `project/CrossSparkVersions.scala` lines 279–280; [[modules/connectors/uniform-iceberg]], [[modules/connectors/uniform-hudi]]
```

---

### Finding 7: `interfaces_idl.md` Overview says "three layers" but lists four [MINOR]

- **Reviewer's claim**: `interfaces_idl.md` Overview paragraph says "three layers" but immediately lists 4 numbered items.
- **What I verified**:
  - Grep `three layers` in `interfaces_idl.md`.
- **Evidence**:
  - `interfaces_idl.md` line 20: "Delta Lake exposes **three** layers of pluggable interfaces:" followed by 4 numbered items: Engine SPI, LogStore SPI, CommitCoordinatorClient SPI, Delta Connect IDL.
- **Ruling**: ✅ Confirmed
- **Action**: Fix required — `interfaces_idl.md`

**Fix instruction**:
In `interfaces_idl.md` line 20, change:
> Delta Lake exposes **three** layers of pluggable interfaces:

To:
> Delta Lake exposes **four** layers of pluggable interfaces:

(All four are legitimately "layers" — the Delta Connect IDL is a separate transport-level contract from the storage-level SPIs, but grouping it as a fourth layer is accurate and consistent with how the document is structured.)

---

## Action List for Producer

The following is the prioritised, confirmed action list for the architecture document producer (`codebase-architect`). All items are confirmed by source code verification. No items are disputed or refuted.

1. **[CRITICAL] `module_dependencies.md` — Fix Iceberg UniForm availability for Spark 4.1.0**
   - Change Iceberg UniForm column for Spark 4.1.0 row from `Yes` to `No`
   - Add `[!WARNING]` block noting both Iceberg and Hudi are disabled for 4.1.0 (the default build)
   - Source evidence: `project/CrossSparkVersions.scala` lines 275–284; `build.sbt` lines 1170–1174

2. **[MAJOR] `module_dependencies.md` — Correct `delta-connect-*` publish status**
   - Replace `(internal, not published standalone)` with proper Maven coordinates for all three modules
   - Source evidence: `build.sbt` lines 173, 200, 249, 1590–1592

3. **[MAJOR] `interfaces_idl.md` — Remove fabricated `CommitCoordinatorClientHandler` from Engine SPI**
   - Remove line 47 (`getCommitCoordinatorClientHandler()`) from the Engine.java snippet
   - Remove the `#### CommitCoordinatorClientHandler` sub-section (lines 101–103) or replace with a `[!NOTE]` clarifying it does not exist in the Engine SPI
   - Fix line 241 reference to `CommitCoordinatorClientHandler.getCommitCoordinatorClient()`
   - Source evidence: `Engine.java` (64 lines, 5 methods only); Glob `**/*Engine*.java` (4 files, none named CommitCoordinatorClientHandler)

4. **[MAJOR] `interfaces_idl.md` + `system_map.md` — Replace fabricated `SparkEngine` with `DefaultEngine`**
   - `system_map.md`: Replace `SE[SparkEngine\ndelta-spark-v2]` node with `SE[DefaultEngine\n(kernel-defaults;\nalso used by spark-v2)]`
   - `interfaces_idl.md` Concrete Implementations table: Remove `SparkEngine (internal)` row; add accurate `DefaultEngine` note covering both Flink and spark-v2 usage
   - Source evidence: Grep `SparkEngine` → zero matches; `SparkTable.java` line 147, `SparkScan.java` lines 270/281, `SparkMicroBatchStream.java` line 174 all use `DefaultEngine.create()`

5. **[MINOR] `executive_summary.md` — Extend UniForm WARNING to cover Iceberg**
   - Extend the existing Hudi-only WARNING to cover both `delta-iceberg` and `delta-hudi` disabled for 4.1.0
   - Source evidence: `CrossSparkVersions.scala` line 279 (`supportIceberg = false`)

6. **[MINOR] `module_dependencies.md` — Remove `LogStoreBasedCommitCoordinatorClient` reference**
   - Replace the fabricated class name with an accurate description of how `DefaultEngine` handles commit coordination
   - Source evidence: Grep across entire repo → zero Java/Scala source matches; `DefaultEngine.java` inspected

7. **[MINOR] `interfaces_idl.md` — Fix "three layers" count to "four layers"**
   - Line 20: `three` → `four`
   - Source evidence: The same document lists 4 numbered items immediately after

---

## Refuted / Invalid Findings

None. All seven findings raised by the reviewer are confirmed by direct source code verification.

---

## Escalations (User Input Required)

None. All findings are resolvable from available source files without ambiguity.

---

## Ancillary Flags (Out of Scope — Track for Future Passes)

The following issues were surfaced during verification but are outside the scope of the L2 architecture documents under review. They should be addressed in future L3 detail-review passes:

- **`storage.md` lines 817, 859**: References to non-existent class `LogStoreBasedCommitCoordinatorClient`. These are in the L3 storage module document, not the L2 architecture docs. Flag for correction in next L3 review pass for the storage module.
