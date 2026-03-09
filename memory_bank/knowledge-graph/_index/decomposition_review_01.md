---
title: "Decomposition Review 01"
tags: [review, decomposition, feedback]
last_updated: 2026-03-02
pass: full
---

# Decomposition Review 01
date: 2026-03-02

> Produced by kg-decomposition-reviewer. Subject to adjudication by kg-feedback-verifier before fixes are applied.

---

## Overall Assessment

The manifest is structurally sound and covers all 24 SBT sub-projects. Module groupings are
logical, boundary choices are defensible, and the dependency order is correct at a high level.
The manifest demonstrates a thorough survey of the build system. However, several factual
errors were found by cross-checking against the actual repository: the Spark version support
claim in the Build System Overview is materially wrong (Spark 3.x is absent; 4.2 is a SNAPSHOT
not a supported target); two `key_files` entries point to Java classes that do not exist; the
`delta-flink` description references `DeltaSource`/`DeltaSink` DataStream classes that were
never added to this module; and the SBT dependency diagram legend is directionally ambiguous.
These are fixable before explorer invocations begin.

---

## CRITICAL Issues (blockers)

None identified. No structural omissions that would cause an explorer agent to miss an entire
module or produce wrong scoping.

---

## MAJOR Issues (important but not blocking)

### [MAJOR] Build System Overview: Spark Version Support Is Wrong

- **Location in manifest**: "Build System Overview" section, line `- **Cross-version support**: Spark 3.x, 4.0, 4.1, 4.2 via CrossSparkVersions.scala`
- **Finding**: The claim that Spark 3.x and 4.2 are supported cross-build targets is factually incorrect for the current `main` branch.
- **Evidence**:
  - `project/CrossSparkVersions.scala` lines 265–306 define exactly three `SparkVersionSpec` objects: `spark40` (4.0.1), `spark41` (4.1.0), `spark42Snapshot` (4.2.0-SNAPSHOT).
  - `val ALL_SPECS = Seq(spark40, spark41)` — 4.2 is NOT in `ALL_SPECS`; spark42Snapshot is unused in build artifacts.
  - `val DEFAULT = spark41` — default is Spark 4.1, not 3.x.
  - No `spark35` or any 3.x spec exists in the file.
  - Published artifact names from build docs show `delta-spark_4.0_2.13` and `delta-spark_4.1_2.13` only.
- **Suggested correction**: Change to `- **Cross-version support**: Spark 4.0.1 and 4.1.0 (ALL_SPECS); 4.2.0-SNAPSHOT defined but not released; Spark 3.x was dropped prior to this branch`.

---

### [MAJOR] `delta-kernel-defaults` key_file `GsonSerializationHelper.java` Does Not Exist

- **Location in manifest**: `delta-kernel-defaults` → `key_files` → `src/main/java/io/delta/kernel/defaults/internal/json/GsonSerializationHelper.java`
- **Finding**: No file with this name exists anywhere in the repository.
- **Evidence**:
  - `ls kernel/kernel-defaults/src/main/java/io/delta/kernel/defaults/internal/json/` → only `JsonUtils.java` is present.
  - `find kernel/kernel-defaults/src -name "*Gson*"` → empty result.
- **Suggested correction**: Replace `GsonSerializationHelper.java` with `JsonUtils.java` (path: `src/main/java/io/delta/kernel/defaults/internal/json/JsonUtils.java`).

---

### [MAJOR] `delta-kernel-defaults` key_file `HadoopFileSystemClient.java` Does Not Exist

- **Location in manifest**: `delta-kernel-defaults` → `key_files` → `src/main/java/io/delta/kernel/defaults/engine/hadoopio/HadoopFileSystemClient.java`
- **Finding**: The class `HadoopFileSystemClient` does not exist in the `hadoopio/` directory. The directory contains Iceberg-style file I/O wrappers.
- **Evidence**:
  - `ls kernel/kernel-defaults/src/main/java/io/delta/kernel/defaults/engine/hadoopio/` → `HadoopFileIO.java`, `HadoopInputFile.java`, `HadoopOutputFile.java`, `HadoopPositionOutputStream.java`, `HadoopSeekableInputStream.java`.
  - The Hadoop `FileSystemClient` implementation is `DefaultFileSystemClient.java` in the parent `engine/` directory, not in `hadoopio/`.
- **Suggested correction**: Replace with `src/main/java/io/delta/kernel/defaults/engine/DefaultFileSystemClient.java` (the actual `FileSystemClient` impl).

---

### [MAJOR] `delta-flink` Description Claims Non-Existent DataStream API Classes

- **Location in manifest**: `delta-flink` → `description` → "Provides `DeltaSource` and `DeltaSink` Flink DataStream API connectors"
- **Finding**: No `DeltaSource.java` or `DeltaSink.java` class exists in the flink module. The description is wrong about the connector type.
- **Evidence**:
  - `find flink/src/main/java -name "DeltaSource.java" -o -name "DeltaSink.java"` → empty result.
  - `ls flink/src/main/java/io/delta/flink/` → `Conf.java`, `kernel/`, `table/`.
  - The module contains only `kernel/` (checkpoint writers, table operations) and `table/` (Flink Table API: `DeltaCatalog.java`, `DeltaTable.java`, `CredentialManager.java`, `SnapshotCacheManager.java`).
  - This is a Flink **Table API** connector, not a DataStream API connector. There is no `DeltaSource`/`DeltaSink` here.
- **Suggested correction**: Revise description to: "Apache Flink Table API connector for Delta, built directly on the Kernel API (no Spark dependency). Provides `DeltaCatalog` and `DeltaTable` Flink Catalog/Table API classes (`flink/table/`), kernel-backed checkpoint and column-vector utilities (`flink/kernel/`), and credential management for Unity Catalog. Uses `DefaultEngine` from kernel-defaults for I/O." Remove references to DeltaSource/DeltaSink DataStream API.

---

### [MAJOR] SBT Dependency Diagram Legend Direction Is Ambiguous / Misleading

- **Location in manifest**: "SBT Project Dependency Graph" Mermaid diagram caption: _"Solid arrows = compile-time .dependsOn()"_
- **Finding**: The caption implies `A --> B` means A `.dependsOn(B)` (A depends on B), but the diagram is drawn in the **opposite** direction — arrows flow from the dependency to the dependent (foundation→consumer). Under the caption's reading, `STG --> KD` would mean storage depends on kernel-defaults, which is wrong.
- **Evidence**:
  - `build.sbt:924` → `kernelDefaults.dependsOn(storage)` — kernel-defaults depends on storage, yet the arrow is `STG --> KD` (storage points to kernel-defaults).
  - `build.sbt:503` → `spark.dependsOn(sparkV1).dependsOn(sparkV2).dependsOn(storage)` — spark-unified depends on storage, yet the arrow is `STG --> SP` (storage points to spark-unified).
  - The actual arrow semantics are: `A --> B` means "B depends on A" (A is a prerequisite of B), i.e., the **dependency flow** direction (bottom-up), not the `.dependsOn()` source→target direction.
- **Suggested correction**: Replace caption with: _"Arrows show the 'depended-upon-by' direction: A → B means B depends on A (arrows flow from foundation modules up to consumers). Dashed arrows = JAR-level dependency consumed via unmanaged classpath (kernel-api)."_

---

## MINOR Issues (nice to fix)

### [MINOR] `delta-kernel-api` key_file `Transaction.java` Path Is Incorrect

- **Location in manifest**: `delta-kernel-api` → `key_files` → `src/main/java/io/delta/kernel/transaction/Transaction.java`
- **Finding**: `Transaction.java` lives at the top level of the `io.delta.kernel` package, not inside a `transaction/` subdirectory. The `transaction/` subdirectory contains builder classes, not `Transaction.java` itself.
- **Evidence**:
  - `find kernel/kernel-api/src -name "Transaction.java"` → `kernel/kernel-api/src/main/java/io/delta/kernel/Transaction.java`.
  - `ls kernel/kernel-api/src/main/java/io/delta/kernel/transaction/` → `CreateTableTransactionBuilder.java`, `DataLayoutSpec.java`, `ReplaceTableTransactionBuilder.java`, `UpdateTableTransactionBuilder.java`.
- **Suggested correction**: Change path to `src/main/java/io/delta/kernel/Transaction.java`.

---

### [MINOR] `delta-kernel-unitycatalog` `depends_on` Omits kernel-api Unmanaged JAR

- **Location in manifest**: `delta-kernel-unitycatalog` → `depends_on: [delta-kernel-defaults (test), delta-storage]`
- **Finding**: `build.sbt:1007-1040` shows that `kernelUnityCatalog` also adds the kernel-api compiled JAR as an unmanaged dependency via `Compile / unmanagedJars += (kernelApi / Compile / packageBin).value`. This is a compile-time dependency on kernel-api, not listed in the manifest.
- **Evidence**: `build.sbt` lines 1019–1022:
  ```
  Compile / unmanagedJars += (kernelApi / Compile / packageBin).value,
  Test / unmanagedJars += (kernelApi / Compile / packageBin).value,
  ```
- **Suggested correction**: Add `delta-kernel-api (JAR, unmanaged compile)` to `depends_on`.

---

### [MINOR] `delta-spark-v2` `depends_on` Omits `golden-tables` Test Dependency

- **Location in manifest**: `delta-spark-v2` → `depends_on: [delta-spark-v1-filtered, delta-kernel-defaults, delta-kernel-unitycatalog]`
- **Finding**: `build.sbt:455` shows `.dependsOn(goldenTables % "test")` — golden-tables is a test-scope dependency not listed in the manifest.
- **Evidence**: `build.sbt` line 460: `.dependsOn(goldenTables % "test")`.
- **Suggested correction**: Add `golden-tables (test)` to `depends_on`.

---

### [MINOR] `delta-storage-s3-dynamodb` `depends_on` Omits `delta-spark` Test Dependency

- **Location in manifest**: `delta-storage-s3-dynamodb` → `depends_on: [delta-storage, AWS DynamoDB SDK]`
- **Finding**: `build.sbt:1075` shows `.dependsOn(spark % "test->test")` — the spark connector is a test-scope dependency.
- **Evidence**: `build.sbt` line 1078: `.dependsOn(spark % "test->test")`.
- **Suggested correction**: Add `delta-spark (test)` to `depends_on`.

---

### [MINOR] `contribs` Listed Twice in Dependency Order

- **Location in manifest**: Dependency Order section, items 13 and 24.
- **Finding**: Item 13 says `` `delta-contribs` (contribs) — community LogStore contribs; depends on delta-spark `` and item 24 says `` `contribs` — community LogStore contributions ``. These are the same module described twice.
- **Evidence**: There is one `contribs/` directory and one `contribs` SBT project (`lazy val contribs = (project in file("contribs"))`). Confirmed by `build.sbt:659`.
- **Suggested correction**: Remove item 24 from the dependency order list.

---

### [MINOR] `docs/` Directory Not Mentioned in Generated/Vendored Exclusions

- **Location in manifest**: "Generated / Vendored (excluded from KG)" section
- **Finding**: The `docs/` directory exists at repo root and contains a documentation website build (`astro.config.mjs`, `pnpm-lock.yaml`, `package.json`, `generate_docs.py`, `scripts/`). It is neither a code module nor listed in the exclusions.
- **Evidence**: `ls /docs/` → `apis/`, `astro.config.mjs`, `eslint.config.mjs`, `package.json`, `pnpm-lock.yaml`, `generate_docs.py`, `public/`, `scripts/`.
- **Suggested correction**: Add `docs/` to the Generated/Vendored exclusions section with a note: "`docs/` — Astro-based documentation website; no production source code".

---

### [MINOR] Multiple Modules Share `connectors.md` as kg_doc_target — Explorer Scoping Risk

- **Location in manifest**: `golden-tables`, `delta-contribs`, `delta-iceberg`, `delta-hudi`, `delta-flink` all target `memory_bank/knowledge-graph/modules/connectors.md`.
- **Finding**: Five distinct modules — spanning 3 different technologies (Flink, Iceberg/Hudi UniForm, golden test fixtures, Scala contribs) — are mapped to a single explorer document. This forces a single codebase-explorer invocation to cover very heterogeneous ground, or requires the orchestrator to manually split the scope.
- **Evidence**: Five `kg_doc_target: memory_bank/knowledge-graph/modules/connectors.md` entries verified in manifest.
- **Suggested correction**: Consider splitting into `connectors.md` (Flink), `uniform.md` (iceberg + hudi), and keeping `connectors.md` for golden-tables + contribs. Or at minimum, annotate in each module entry that the explorer should only document that module's section of the doc.

---

## Escalations (require orchestrator/user decision)

### [ESCALATION] `kernel/examples/kernel-examples/` Uses Maven, Not SBT

- **Context**: The `examples` module entry groups `examples/` (top-level, end-user notebooks and Scala examples) with `kernel/examples/kernel-examples/` (Java API examples). The `kernel/examples/kernel-examples/` project contains only a `pom.xml` — it is a standalone Maven project, not an SBT sub-project.
- **Question**: Should the explorer covering `examples` be expected to document Maven-based code? Is there a separate build/CI path for `kernel/examples/` that should be surfaced in the manifest? The two example areas arguably have different target audiences (end users vs. kernel API users) and different build systems.
- **Suggested resolution**: Confirm whether a single `examples` module entry with `kg_doc_target: modules/kernel.md` is appropriate, or whether `kernel/examples/` deserves its own entry pointing to a `modules/kernel.md#examples` anchor.

---

### [ESCALATION] `delta-spark-v1` Decomposition Candidate Already Has 13 Depth-3 Components Listed

- **Context**: The manifest flags `delta-spark-v1` as a `decomposition_candidate` with the note "~150+ source files, ~12 distinct internal sub-domains." However, the manifest already provides a 13-component depth-3 breakdown (spark.core, spark.actions, spark.commands, spark.streaming, spark.catalog, spark.coordinatedcommits, spark.files, spark.schema, spark.skipping, spark.uniform, spark.serverSidePlanning, spark.v2-interop, spark.sql-parser, spark.public-api).
- **Question**: Is the decomposition candidate flag calling for a *further* nested decomposer pass within some of these 13 components (e.g., `spark.commands` with its many sub-commands), or is the existing depth-3 breakdown sufficient for explorer scoping?
- **Suggested resolution**: Clarify intent. If the depth-3 breakdown is sufficient, clear the `decomposition_candidate: true` flag. If `spark.commands` (which has merge/, optimize/, cdc/, backfill/, columnmapping/, convert/ sub-packages) needs its own sub-decomposition, flag *that component* specifically.

---

## Verdict: APPROVED_WITH_CHANGES

The manifest is ready to proceed to codebase-explorer invocations **after** applying the following fixes:

**Must-fix before explorer begins:**
1. Correct Build System Overview Spark version claim (MAJOR — affects downstream accuracy of all Spark module docs)
2. Fix `delta-kernel-defaults` key_files: replace non-existent `GsonSerializationHelper.java` → `JsonUtils.java` and `HadoopFileSystemClient.java` → `DefaultFileSystemClient.java` (MAJOR — broken file references will mislead explorers)
3. Correct `delta-flink` description to remove false `DeltaSource`/`DeltaSink` DataStream claims (MAJOR — will propagate incorrect API information into L3 docs)
4. Fix SBT diagram legend wording to clarify arrow direction (MAJOR — ambiguous legend makes the diagram actively misleading)

**Can be fixed concurrently with explorer runs:**
5. Fix `Transaction.java` key_file path (MINOR)
6. Add missing `depends_on` entries for kernel-unitycatalog (kernel-api JAR), spark-v2 (golden-tables test), storage-s3-dynamodb (spark test)
7. Remove duplicate `contribs` entry (#24) from dependency order
8. Add `docs/` to Generated/Vendored exclusions
9. Address connectors.md multi-module collision (MINOR — current state is workable but noisy)
