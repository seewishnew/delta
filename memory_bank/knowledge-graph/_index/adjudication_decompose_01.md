# Adjudication Report — Decomposition Review 01
date: 2026-03-02

> Source review: `knowledge_graph/_index/decomposition_review_01.md`
> Adjudicated by: kg-feedback-verifier

---

## Adjudication Summary

The reviewer's feedback is well-grounded. All four MAJOR findings and all five MINOR findings were
verified against source code and confirmed accurate. Every factual claim traced to a real file
(or absence of a file) in the repository. Line numbers cited by the reviewer have minor
off-by-one errors in two cases (lines 459 vs 460, 1077 vs 1078) but the underlying claims are
correct in both instances. Both escalations are genuine ambiguities requiring orchestrator input —
neither can be resolved by reading source code alone. No reviewer claims were refuted.

---

## Adjudicated Findings

---

### [MAJOR] Finding M1: Build System Overview — Spark Version Support Is Wrong

**Reviewer's claim**: The manifest states "Spark 3.x, 4.0, 4.1, 4.2 via CrossSparkVersions.scala".
Spark 3.x is not supported; 4.2 is a SNAPSHOT not in `ALL_SPECS`; actual supported targets are
only 4.0.1 and 4.1.0.

**Ground truth verification**:
- Read `project/CrossSparkVersions.scala` lines 265–307.
- Three `SparkVersionSpec` objects defined: `spark40` (4.0.1, line 265), `spark41` (4.1.0, line
  275), `spark42Snapshot` (4.2.0-SNAPSHOT, line 286).
- `val ALL_SPECS = Seq(spark40, spark41)` — line 307. `spark42Snapshot` is explicitly excluded
  from `ALL_SPECS`.
- `val DEFAULT = spark41` — line 301.
- `val MASTER: Option[SparkVersionSpec] = None` — line 304.
- No `spark35` or any 3.x spec exists anywhere in the file.
- The comments in the file itself use "3.5" as a hypothetical example in documentation prose,
  which may have confused the original decomposer.

**Ruling**: ✅ Confirmed

**Evidence**: `project/CrossSparkVersions.scala` lines 265–307 show exactly two supported release
targets (4.0.1, 4.1.0). The 3.x claim is factually false. The 4.2 claim is misleading (SNAPSHOT,
not in ALL_SPECS).

**Action**: FIX

**Instructions for producer**: In the "Build System Overview" section, replace:
```
- **Cross-version support**: Spark 3.x, 4.0, 4.1, 4.2 via `CrossSparkVersions.scala`
```
with:
```
- **Cross-version support**: Spark 4.0.1 and 4.1.0 (`ALL_SPECS` in `CrossSparkVersions.scala`);
  Spark 4.2.0-SNAPSHOT defined (`spark42Snapshot`) but excluded from `ALL_SPECS` and not
  released; Spark 3.x support was dropped prior to this branch.
- **Default build target**: Spark 4.1.0 (`DEFAULT = spark41`)
```
Also update the External Dependencies table row for Apache Spark (currently "3.x – 4.2 (provided)")
to: "4.0.1, 4.1.0 (provided); 4.2.0-SNAPSHOT (defined, unreleased)".

---

### [MAJOR] Finding M2: `delta-kernel-defaults` key_file `GsonSerializationHelper.java` Does Not Exist

**Reviewer's claim**: No `GsonSerializationHelper.java` exists anywhere; the correct file is
`JsonUtils.java`.

**Ground truth verification**:
- `ls kernel/kernel-defaults/src/main/java/io/delta/kernel/defaults/internal/json/` →
  output: `JsonUtils.java` (the only file in this directory).
- `find kernel/kernel-defaults/src -name "*Gson*"` → empty result (confirmed by absence of
  any output from the command).

**Ruling**: ✅ Confirmed

**Evidence**: The directory contains exactly one file: `JsonUtils.java`. `GsonSerializationHelper.java`
does not exist anywhere in the repository.

**Action**: FIX

**Instructions for producer**: In the `delta-kernel-defaults` `key_files` list, replace:
```
src/main/java/io/delta/kernel/defaults/internal/json/GsonSerializationHelper.java
```
with:
```
src/main/java/io/delta/kernel/defaults/internal/json/JsonUtils.java
```

---

### [MAJOR] Finding M3: `delta-kernel-defaults` key_file `HadoopFileSystemClient.java` Does Not Exist

**Reviewer's claim**: `HadoopFileSystemClient.java` does not exist in the `hadoopio/` directory.
The actual `FileSystemClient` implementation is `DefaultFileSystemClient.java` in the parent
`engine/` directory.

**Ground truth verification**:
- `ls kernel/kernel-defaults/src/main/java/io/delta/kernel/defaults/engine/hadoopio/` → output:
  `HadoopFileIO.java`, `HadoopInputFile.java`, `HadoopOutputFile.java`,
  `HadoopPositionOutputStream.java`, `HadoopSeekableInputStream.java`. No
  `HadoopFileSystemClient.java`.
- `ls kernel/kernel-defaults/src/main/java/io/delta/kernel/defaults/engine/` → output:
  `DefaultEngine.java`, `DefaultExpressionHandler.java`, **`DefaultFileSystemClient.java`**,
  `DefaultJsonHandler.java`, `DefaultParquetHandler.java`, `fileio/`, `hadoopio/`,
  `LoggingMetricsReporter.java`, `package-info.java`. Confirms `DefaultFileSystemClient.java`
  exists in the `engine/` parent directory.

**Ruling**: ✅ Confirmed

**Evidence**: `HadoopFileSystemClient.java` does not exist anywhere; `DefaultFileSystemClient.java`
exists at `engine/DefaultFileSystemClient.java`.

**Action**: FIX

**Instructions for producer**: In the `delta-kernel-defaults` `key_files` list, replace:
```
src/main/java/io/delta/kernel/defaults/engine/hadoopio/HadoopFileSystemClient.java
```
with:
```
src/main/java/io/delta/kernel/defaults/engine/DefaultFileSystemClient.java
```

---

### [MAJOR] Finding M4: `delta-flink` Description Claims Non-Existent DataStream API Classes

**Reviewer's claim**: No `DeltaSource.java` or `DeltaSink.java` exists in the flink module.
The module is a Flink **Table API** connector, not a DataStream connector.

**Ground truth verification**:
- `find flink/src/main/java -name "DeltaSource.java" -o -name "DeltaSink.java"` → empty result.
- `ls flink/src/main/java/io/delta/flink/` → `Conf.java`, `kernel/`, `table/`.
- `ls flink/src/main/java/io/delta/flink/table/` → `CredentialManager.java`, `DeltaCatalog.java`,
  `DeltaTable.java`, `ExceptionUtils.java`, `MetricListener.java`, `SnapshotCacheManager.java`,
  `TableConf.java`.
- `ls flink/src/main/java/io/delta/flink/kernel/` → `CheckpointActionRow.java`,
  `CheckpointWriter.java`, `ColumnVectorUtils.java`.

**Ruling**: ✅ Confirmed

**Evidence**: The `DeltaSource` and `DeltaSink` DataStream API classes do not exist. The module
contains only Table API integration classes (`DeltaCatalog`, `DeltaTable`) and kernel-backed
checkpoint utilities. The reviewer's alternative description accurately matches the actual file
structure.

**Action**: FIX

**Instructions for producer**: In the `delta-flink` module entry, replace the `description` field:
```
description: Apache Flink connector for Delta, built directly on the Kernel API (no Spark
dependency). Provides `DeltaSource` and `DeltaSink` Flink DataStream API connectors, a Flink
Table API integration (`flink/table/`), and kernel-backed operators (`flink/kernel/`). Uses
`DefaultEngine` from kernel-defaults for I/O.
```
with:
```
description: Apache Flink Table API connector for Delta, built directly on the Kernel API (no
Spark dependency). Provides `DeltaCatalog` and `DeltaTable` Flink Catalog/Table API classes
(`flink/table/`), kernel-backed checkpoint and column-vector utilities (`flink/kernel/`), and
credential management for Unity Catalog. Uses `DefaultEngine` from kernel-defaults for I/O. No
DataStream API (`DeltaSource`/`DeltaSink`) classes are present in this module.
```

---

### [MAJOR] Finding M5: SBT Dependency Diagram Legend Arrow Direction Is Misleading

**Reviewer's claim**: The caption "Solid arrows = compile-time .dependsOn()" implies `A --> B`
means `A.dependsOn(B)`, but the diagram arrows flow in the opposite direction (from dependency to
dependent), so the caption is actively misleading.

**Ground truth verification**:
- `build.sbt` line 926: `kernelDefaults.dependsOn(storage)` — KD depends on STG.
  Diagram edge: `STG --> KD` — arrow points FROM STG TO KD.
  Under the caption's reading ("arrows = .dependsOn()"), `STG --> KD` would mean STG depends on
  KD. That is the opposite of the actual dependency.
- `build.sbt` lines 503–505: `spark.dependsOn(sparkV1).dependsOn(sparkV2).dependsOn(storage)` —
  SP depends on STG. Diagram edge: `STG --> SP` — again, arrow points FROM dependency TO
  dependent.
- Pattern is consistent throughout: arrows flow foundation → consumer (upward), not
  dependent → dependency.

**Ruling**: ✅ Confirmed

**Evidence**: The diagram's arrows consistently go FROM prerequisite TO consumer. The caption "Solid
arrows = compile-time .dependsOn()" is semantically backwards — a reader naturally parses this as
"A depends on B" when seeing `A --> B`, but the diagram means the reverse.

**Action**: FIX

**Instructions for producer**: In the "SBT Project Dependency Graph" section, replace the
diagram caption:
```
_Solid arrows = compile-time .dependsOn(). Dashed arrows = JAR-level dependency (kernel-api is
consumed as a compiled JAR by kernel-defaults and flink, not via .dependsOn source)._
```
with:
```
_Arrows show the "depended-upon-by" direction: A → B means B depends on A (arrows flow from
foundation modules up to consumers). Dashed arrows = JAR-level dependency: kernel-api is consumed
as a compiled JAR by kernel-defaults and flink via `unmanagedJars`, not via SBT `.dependsOn()`
source linking._
```

---

### [MINOR] Finding m1: `delta-kernel-api` key_file `Transaction.java` Path Is Incorrect

**Reviewer's claim**: `Transaction.java` is at `io/delta/kernel/Transaction.java`, not
`io/delta/kernel/transaction/Transaction.java`.

**Ground truth verification**:
- `find kernel/kernel-api/src -name "Transaction.java"` → output:
  `kernel/kernel-api/src/main/java/io/delta/kernel/Transaction.java`.
- `ls kernel/kernel-api/src/main/java/io/delta/kernel/transaction/` → output:
  `CreateTableTransactionBuilder.java`, `DataLayoutSpec.java`,
  `ReplaceTableTransactionBuilder.java`, `UpdateTableTransactionBuilder.java`.
  (`Transaction.java` is NOT in this subdirectory.)

**Ruling**: ✅ Confirmed

**Evidence**: `Transaction.java` is at the top-level `io/delta/kernel/` package. The `transaction/`
subdirectory contains only builder classes, as the reviewer stated.

**Action**: FIX

**Instructions for producer**: In the `delta-kernel-api` `key_files` list, replace:
```
src/main/java/io/delta/kernel/transaction/Transaction.java
```
with:
```
src/main/java/io/delta/kernel/Transaction.java
```

---

### [MINOR] Finding m2: `delta-kernel-unitycatalog` `depends_on` Omits kernel-api Unmanaged JAR

**Reviewer's claim**: `build.sbt` lines 1019–1022 show `kernelUnityCatalog` adds kernel-api as
an unmanaged compile-time dependency, not listed in `depends_on`.

**Ground truth verification**:
- `build.sbt` lines 1021–1022 (within the `kernelUnityCatalog` settings block starting at line
  1007):
  ```
  Compile / unmanagedJars += (kernelApi / Compile / packageBin).value,
  Test / unmanagedJars += (kernelApi / Compile / packageBin).value,
  ```
  Also line 1024: `Compile / compile := (Compile / compile).dependsOn(kernelApi / Compile / packageBin).value`
- This is a real compile-time dependency on kernel-api via unmanaged JAR.
- Manifest `depends_on` for `delta-kernel-unitycatalog` currently lists only:
  `[delta-kernel-defaults (test), delta-storage]`.

**Ruling**: ✅ Confirmed

**Evidence**: `build.sbt` lines 1021–1022 explicitly add kernel-api compiled JAR to both
`Compile/unmanagedJars` and `Test/unmanagedJars`. This is a compile-scope dependency not
reflected in the manifest.

**Action**: FIX

**Instructions for producer**: In the `delta-kernel-unitycatalog` `depends_on` field, add:
```
delta-kernel-api (JAR, unmanaged compile+test)
```
Resulting `depends_on` should be:
```
[delta-kernel-api (JAR, unmanaged compile+test), delta-kernel-defaults (test), delta-storage]
```

---

### [MINOR] Finding m3: `delta-spark-v2` `depends_on` Omits `golden-tables` Test Dependency

**Reviewer's claim**: `build.sbt` line 460 shows `.dependsOn(goldenTables % "test")` for sparkV2.

**Ground truth verification**:
- `build.sbt` line 459 (reviewer cited 460, off-by-one): `.dependsOn(goldenTables % "test")`
  within the `sparkV2` project block starting at line 455. The claim is factually correct; only
  the line number is slightly off.
- Manifest `depends_on` for `delta-spark-v2` currently lists:
  `[delta-spark-v1-filtered, delta-kernel-defaults, delta-kernel-unitycatalog]`.
  `golden-tables` is absent.

**Ruling**: ✅ Confirmed (line number off-by-one, claim valid)

**Evidence**: `build.sbt` line 459 confirms `goldenTables % "test"` dependency.

**Action**: FIX

**Instructions for producer**: In the `delta-spark-v2` `depends_on` field, add:
```
golden-tables (test)
```

---

### [MINOR] Finding m4: `delta-storage-s3-dynamodb` `depends_on` Omits `delta-spark` Test Dependency

**Reviewer's claim**: `build.sbt` line 1078 shows `.dependsOn(spark % "test->test")` for
storageS3DynamoDB.

**Ground truth verification**:
- `build.sbt` line 1077 (reviewer cited 1078, off-by-one): `.dependsOn(spark % "test->test")`
  within the `storageS3DynamoDB` block starting at line 1075. Claim is correct; line number
  differs by 1.
- Manifest `depends_on` for `delta-storage-s3-dynamodb` currently lists:
  `[delta-storage, AWS DynamoDB SDK]`. `delta-spark` test dependency is absent.

**Ruling**: ✅ Confirmed (line number off-by-one, claim valid)

**Evidence**: `build.sbt` line 1077 confirms `spark % "test->test"` dependency.

**Action**: FIX

**Instructions for producer**: In the `delta-storage-s3-dynamodb` `depends_on` field, add:
```
delta-spark (test)
```

---

### [MINOR] Finding m5: `contribs` Listed Twice in Dependency Order

**Reviewer's claim**: Items 13 and 24 in the Dependency Order section both describe the `contribs`
module (`delta-contribs`). There is only one SBT project.

**Ground truth verification**:
- `build.sbt` grep for `lazy val contribs` → one result: line 659.
- Manifest items 13 and 24:
  - Item 13: `` `delta-contribs` (contribs) — community LogStore contribs; depends on delta-spark ``
  - Item 24: `` `contribs` — community LogStore contributions ``
  These are the same module.

**Ruling**: ✅ Confirmed

**Evidence**: One `contribs` SBT project at `build.sbt:659`. Duplicate entry at item 24 is
erroneous.

**Action**: FIX

**Instructions for producer**: Remove item 24 (`` `contribs` — community LogStore contributions ``)
from the Dependency Order list. Renumber subsequent items if applicable (items after 24 do not
exist in the current manifest — item 24 is already the last entry, so simple deletion suffices).

---

### [MINOR] Finding m6: `docs/` Directory Not Mentioned in Generated/Vendored Exclusions

**Reviewer's claim**: `docs/` exists at repo root as an Astro-based documentation website and
should be listed in the exclusions.

**Ground truth verification**:
- `ls /docs/` at repo root → `apis/`, `astro.config.mjs`, `environment.yml`,
  `eslint.config.mjs`, `generate_docs.py`, `package.json`, `pnpm-lock.yaml`, `public/`,
  `README.md`, `scripts/`, `src/`, `tsconfig.json`.
- Confirmed: this is a full documentation website build with its own `package.json`, `pnpm-lock.yaml`,
  Astro config, and `generate_docs.py` script.
- The current exclusions section does not mention `docs/`.

**Ruling**: ✅ Confirmed

**Evidence**: `docs/` is a frontend documentation website build system (Astro). It has no
production Delta source code and should be excluded from KG scope.

**Action**: FIX

**Instructions for producer**: Add the following entry to the "Generated / Vendored (excluded
from KG)" section:
```
- `docs/` — Astro-based documentation website (`astro.config.mjs`, `pnpm-lock.yaml`,
  `generate_docs.py`); no production Delta source code
```

---

### [MINOR] Finding m7: Multiple Modules Share `connectors.md` as kg_doc_target

**Reviewer's claim**: Five modules (`golden-tables`, `delta-contribs`, `delta-iceberg`,
`delta-hudi`, `delta-flink`) all map to `connectors.md`, creating heterogeneous scoping risk for
a single explorer invocation.

**Ground truth verification**:
- Verified in manifest: all five modules have `kg_doc_target: memory_bank/knowledge-graph/modules/connectors.md`.
- The five modules span radically different technology domains: Flink Table API (Java, Kernel-based),
  Iceberg UniForm (Scala, complex shaded deps), Hudi UniForm (Scala), community LogStore contribs
  (Scala), and golden test fixtures (Scala, binary resources).
- This is a judgment call about explorer invocation strategy, not a factual error.
- The reviewer's concern is grounded: forcing a single explorer to cover this scope is either
  unworkable or will produce a shallow document for all five modules.

**Ruling**: ✅ Valid (judgment call, reviewer's reasoning is well-grounded)

**Evidence**: Five modules with genuinely different tech stacks and 0 shared code all mapping to
one target document is a structural planning deficiency that will degrade explorer output quality.

**Action**: FIX

**Instructions for producer**: Split `connectors.md` into three targeted kg_doc_targets:
1. `memory_bank/knowledge-graph/modules/connectors_flink.md` — for `delta-flink` only.
2. `memory_bank/knowledge-graph/modules/connectors_uniform.md` — for `delta-iceberg` and
   `delta-hudi`.
3. Keep `memory_bank/knowledge-graph/modules/connectors.md` — for `golden-tables` and
   `delta-contribs` (both are small support modules that make sense together).

Update `kg_doc_target` in each of the five module entries accordingly.

---

## Escalations for Orchestrator

### Escalation E1: `kernel/examples/kernel-examples/` Uses Maven, Not SBT

**What was determined**:
- `kernel/examples/kernel-examples/` contains `pom.xml` at the directory root — it is a
  standalone Maven project with no SBT relationship.
- `examples/` (repo root) is a non-SBT directory with `cheat_sheet/`, `python/`, `scala/`,
  `README.md` — end-user reference materials.
- The manifest groups both under a single `examples` module entry with
  `kg_doc_target: modules/kernel.md`.

**What remains unresolved**:
The two example areas serve different audiences (end-user notebooks vs. kernel API integration
patterns) and use different build systems (no build vs. Maven). It is unclear whether:
(a) Both should be covered by the kernel explorer (current state), or
(b) `kernel/examples/kernel-examples/` deserves its own manifest entry, or
(c) Maven-based code should be excluded from KG scope entirely (like generated protobuf stubs).

**Question for orchestrator/user**: Should `kernel/examples/kernel-examples/` (Maven-based Java
kernel API examples) be documented in the KG? If yes, should it be a separate manifest entry, or
annotated within the existing `examples` entry as a distinct sub-section requiring Maven
awareness? If no, should it be added to the Generated/Vendored exclusions?

---

### Escalation E2: `delta-spark-v1` `decomposition_candidate: true` Flag Intent Is Unclear

**What was determined**:
- The manifest already provides a 13-component depth-3 breakdown for `delta-spark-v1`
  (spark.core, spark.actions, spark.commands, spark.streaming, spark.catalog,
  spark.coordinatedcommits, spark.files, spark.schema, spark.skipping, spark.uniform,
  spark.serverSidePlanning, spark.v2-interop, spark.sql-parser, spark.public-api).
- `decomposition_candidate: true` is set with the note "150+ source files, ~12 distinct
  internal sub-domains."
- The `spark.commands` component in particular contains sub-packages: `merge/`, `optimize/`,
  `cdc/`, `backfill/`, `columnmapping/`, `convert/` — each with multiple command classes.

**What remains unresolved**:
It is ambiguous whether the `decomposition_candidate` flag is asking for:
(a) A further nested decomposer pass on `spark.commands` specifically (and possibly
    `spark.coordinatedcommits` and `spark.streaming`), or
(b) The existing 13-component depth-3 breakdown is already considered the decomposition result,
    and the flag should now be cleared.

**Question for orchestrator/user**: Is the 13-component depth-3 breakdown in the manifest
sufficient for scoping explorer invocations on `delta-spark-v1`, or should a second decomposer
pass be run on `spark.commands` (and possibly other large components) before explorer invocations
begin? If sufficient, please clear `decomposition_candidate: true` to avoid confusion.

---

## Action List for Producer

Ordered by priority. All FIX items are confirmed against source code.

1. **[MAJOR] M1** — Correct Build System Overview Spark version claim.
   Replace "Spark 3.x, 4.0, 4.1, 4.2" with accurate version support. Also update External
   Dependencies table row for Apache Spark. See Finding M1 instructions.

2. **[MAJOR] M2** — Fix `delta-kernel-defaults` key_file: replace non-existent
   `GsonSerializationHelper.java` → `JsonUtils.java`. See Finding M2 instructions.

3. **[MAJOR] M3** — Fix `delta-kernel-defaults` key_file: replace non-existent
   `hadoopio/HadoopFileSystemClient.java` → `engine/DefaultFileSystemClient.java`.
   See Finding M3 instructions.

4. **[MAJOR] M4** — Correct `delta-flink` description: remove false `DeltaSource`/`DeltaSink`
   DataStream API claim; replace with accurate Table API description. See Finding M4 instructions.

5. **[MAJOR] M5** — Fix SBT dependency diagram legend caption to clarify arrow direction
   (arrows flow FROM dependency TO consumer, not the reverse). See Finding M5 instructions.

6. **[MINOR] m1** — Fix `delta-kernel-api` key_file path for `Transaction.java`:
   `kernel/Transaction.java` (not `kernel/transaction/Transaction.java`).

7. **[MINOR] m2** — Add `delta-kernel-api (JAR, unmanaged compile+test)` to
   `delta-kernel-unitycatalog` `depends_on`.

8. **[MINOR] m3** — Add `golden-tables (test)` to `delta-spark-v2` `depends_on`.

9. **[MINOR] m4** — Add `delta-spark (test)` to `delta-storage-s3-dynamodb` `depends_on`.

10. **[MINOR] m5** — Remove duplicate `contribs` entry (item 24) from Dependency Order list.

11. **[MINOR] m6** — Add `docs/` to Generated/Vendored exclusions.

12. **[MINOR] m7** — Split `connectors.md` kg_doc_target into three targeted documents
    (`connectors_flink.md`, `connectors_uniform.md`, `connectors.md`).

---

## Refuted / Invalid Findings

None. All reviewer findings were confirmed against source code.

---

## Summary

- **Total findings reviewed**: 13 (4 MAJOR + 5 MINOR + 2 ESCALATIONS + 2 ESCALATIONS)
- **Valid**: 12 | **Invalid**: 0 | **Partially valid**: 0 (two findings had off-by-one line
  numbers but correct claims — ruled Confirmed)
- **Actions**: FIX(12) | SKIP(0) | ESCALATE(2)
- **Reviewer assessment**: High quality. Every factual claim was accurate. The two off-by-one
  line number discrepancies (m3: line 459 vs 460; m4: line 1077 vs 1078) are immaterial — the
  build.sbt context is unambiguous and the cited lines are adjacent to the actual lines. The
  judgment call on `connectors.md` multi-target scoping is well-reasoned and confirmed valid.
  Both escalations identify genuine ambiguities that cannot be resolved without orchestrator input.
