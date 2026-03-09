---
title: "Source File → Knowledge Graph Document Map"
tags: [index, file-map]
layer: L1
last_updated: 2026-03-01
related:
  - "[[../000-index]]"
  - "[[tag_index]]"
---

# Source File → Knowledge Graph Document Map

#index #file-map

This map links key source files to their corresponding knowledge graph documentation.

---

## Build & Configuration

| Source File | KG Document |
|-------------|-------------|
| `build.sbt` | [[../dev/build_system]], [[../architecture/module_dependencies]] |
| `version.sbt` | [[../dev/build_system]] |
| `PROTOCOL.md` | [[../protocol/transaction_log]], [[../protocol/actions]], [[../protocol/checkpoints]], [[../protocol/table_features]] |
| `project/CrossSparkVersions.scala` | [[../dev/build_system]] |
| `project/Mima.scala` | [[../dev/build_system]] |
| `project/Checkstyle.scala` | [[../dev/build_system]] |
| `scalastyle-config.xml` | [[../dev/build_system]] |

---

## Kernel Module (`kernel/`)

| Source File | KG Document |
|-------------|-------------|
| `kernel/kernel-api/src/main/java/io/delta/kernel/Table.java` | [[../modules/kernel]] |
| `kernel/kernel-api/src/main/java/io/delta/kernel/Snapshot.java` | [[../modules/kernel]], [[../concepts/snapshot]] |
| `kernel/kernel-api/src/main/java/io/delta/kernel/Transaction.java` | [[../modules/kernel]], [[../concepts/transaction]] |
| `kernel/kernel-api/src/main/java/io/delta/kernel/Scan.java` | [[../modules/kernel]] |
| `kernel/kernel-api/src/main/java/io/delta/kernel/ScanBuilder.java` | [[../modules/kernel]] |
| `kernel/kernel-api/src/main/java/io/delta/kernel/engine/Engine.java` | [[../modules/kernel]] |
| `kernel/kernel-api/src/main/java/io/delta/kernel/engine/ExpressionHandler.java` | [[../modules/kernel]] |
| `kernel/kernel-api/src/main/java/io/delta/kernel/engine/JsonHandler.java` | [[../modules/kernel]] |
| `kernel/kernel-api/src/main/java/io/delta/kernel/engine/FileSystemClient.java` | [[../modules/kernel]] |
| `kernel/kernel-api/src/main/java/io/delta/kernel/engine/ParquetHandler.java` | [[../modules/kernel]] |
| `kernel/kernel-api/src/main/java/io/delta/kernel/data/ColumnarBatch.java` | [[../modules/kernel]] |
| `kernel/kernel-api/src/main/java/io/delta/kernel/data/ColumnVector.java` | [[../modules/kernel]] |
| `kernel/kernel-api/src/main/java/io/delta/kernel/data/Row.java` | [[../modules/kernel]] |
| `kernel/kernel-api/src/main/java/io/delta/kernel/expressions/` | [[../modules/kernel]] |
| `kernel/kernel-api/src/main/java/io/delta/kernel/types/` | [[../modules/kernel]] |
| `kernel/kernel-defaults/src/main/java/io/delta/kernel/defaults/engine/DefaultEngine.java` | [[../modules/kernel]] |
| `kernel/kernel-defaults/src/main/java/io/delta/kernel/defaults/engine/DefaultParquetHandler.java` | [[../modules/kernel]] |
| `kernel/kernel-defaults/src/main/java/io/delta/kernel/defaults/engine/DefaultJsonHandler.java` | [[../modules/kernel]] |
| `kernel/kernel-defaults/src/main/java/io/delta/kernel/defaults/engine/DefaultFileSystemClient.java` | [[../modules/kernel]] |

---

## Spark Module (`spark/`)

| Source File | KG Document |
|-------------|-------------|
| `spark/src/main/scala/org/apache/spark/sql/delta/DeltaLog.scala` | [[../modules/spark]], [[../concepts/snapshot]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/Snapshot.scala` | [[../modules/spark]], [[../concepts/snapshot]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/SnapshotManagement.scala` | [[../concepts/snapshot]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/DeltaConfig.scala` | [[../modules/spark]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/DeltaTable.scala` | [[../modules/spark]], [[../modules/python]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/DeltaAnalysis.scala` | [[../modules/spark]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/DeltaErrors.scala` | [[../modules/spark]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/TableFeature.scala` | [[../modules/spark]], [[../protocol/table_features]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/actions/` | [[../protocol/actions]], [[../modules/spark]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/commands/` | [[../modules/spark]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/commands/DeleteCommand.scala` | [[../concepts/deletion_vectors]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/commands/MergeIntoCommand.scala` | [[../modules/spark]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/commands/OptimizeTableCommand.scala` | [[../concepts/z_ordering_clustering]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/commands/cdc/` | [[../concepts/change_data_feed]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/deletionvectors/` | [[../concepts/deletion_vectors]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/zorder/` | [[../concepts/z_ordering_clustering]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/clustering/` | [[../concepts/z_ordering_clustering]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/DeltaColumnMapping.scala` | [[../concepts/column_mapping]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/sources/DeltaSource.scala` | [[../modules/spark]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/coordinatedcommits/` | [[../modules/spark]], [[../modules/storage]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/storage/` | [[../modules/storage]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/stats/` | [[../concepts/snapshot]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/skipping/` | [[../concepts/snapshot]] |
| `spark/src/main/scala/org/apache/spark/sql/delta/uniform/` | [[../modules/connectors]] |

---

## Storage Module (`storage/`)

| Source File | KG Document |
|-------------|-------------|
| `storage/src/main/java/io/delta/storage/LogStore.java` | [[../modules/storage]] |
| `storage/src/main/java/io/delta/storage/HDFSLogStore.java` | [[../modules/storage]] |
| `storage/src/main/java/io/delta/storage/S3SingleDriverLogStore.java` | [[../modules/storage]] |
| `storage/src/main/java/io/delta/storage/AzureLogStore.java` | [[../modules/storage]] |
| `storage/src/main/java/io/delta/storage/GCSLogStore.java` | [[../modules/storage]] |
| `storage/src/main/java/io/delta/storage/commit/CommitCoordinatorClient.java` | [[../modules/storage]] |
| `storage/src/main/java/io/delta/storage/commit/uccommitcoordinator/` | [[../modules/storage]] |
| `storage-s3-dynamodb/` | [[../modules/storage]] |

---

## Spark Connect (`spark-connect/`)

| Source File | KG Document |
|-------------|-------------|
| `spark-connect/common/src/main/protobuf/delta/connect/commands.proto` | [[../modules/spark-connect]] |
| `spark-connect/common/src/main/protobuf/delta/connect/relations.proto` | [[../modules/spark-connect]] |
| `spark-connect/server/src/main/scala/io/delta/connect/DeltaCommandPlugin.scala` | [[../modules/spark-connect]] |
| `spark-connect/server/src/main/scala/io/delta/connect/DeltaRelationPlugin.scala` | [[../modules/spark-connect]] |
| `spark-connect/client/src/main/scala/io/delta/connect/tables/DeltaTable.scala` | [[../modules/spark-connect]] |

---

## Sharing Module (`sharing/`)

| Source File | KG Document |
|-------------|-------------|
| `sharing/src/main/scala/io/delta/sharing/spark/DeltaSharingDataSource.scala` | [[../modules/sharing]] |
| `sharing/src/main/scala/io/delta/sharing/spark/DeltaSharingLogFileSystem.scala` | [[../modules/sharing]] |
| `sharing/src/main/scala/io/delta/sharing/spark/DeltaFormatSharingSource.scala` | [[../modules/sharing]] |
| `sharing/src/main/scala/io/delta/sharing/spark/DeltaSharingCDFUtils.scala` | [[../modules/sharing]], [[../concepts/change_data_feed]] |
| `sharing/src/main/scala/io/delta/sharing/spark/model.scala` | [[../modules/sharing]] |

---

## Flink Module (`flink/`)

| Source File | KG Document |
|-------------|-------------|
| `flink/src/main/java/io/delta/flink/table/DeltaCatalog.java` | [[../modules/connectors]] |
| `flink/src/main/java/io/delta/flink/table/DeltaTable.java` | [[../modules/connectors]] |
| `flink/src/main/java/io/delta/flink/kernel/CheckpointWriter.java` | [[../modules/connectors]], [[../protocol/checkpoints]] |

---

## Python Module (`python/`)

| Source File | KG Document |
|-------------|-------------|
| `python/delta/tables.py` | [[../modules/python]] |
| `python/delta/__init__.py` | [[../modules/python]] |

---

## Iceberg / Hudi (`iceberg/`, `hudi/`)

| Source File | KG Document |
|-------------|-------------|
| `iceberg/` | [[../modules/connectors]] |
| `hudi/` | [[../modules/connectors]] |

---

## Related Documents

- [[../000-index]] — Root index
- [[tag_index]] — All tags used in the knowledge graph
- [[change_log]] — History of KG updates
