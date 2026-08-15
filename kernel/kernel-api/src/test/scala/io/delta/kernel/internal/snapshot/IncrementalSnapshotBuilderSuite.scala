/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.internal.snapshot

import java.io.{ByteArrayInputStream, FileNotFoundException}
import java.net.URI
import java.util
import java.util.{Collections, Optional}

import scala.collection.JavaConverters._
import scala.collection.mutable

import io.delta.kernel.{FileActionKey, IncrementalReplay, SnapshotBuilder, TableManager}
import io.delta.kernel.Snapshot.ChecksumWriteMode
import io.delta.kernel.commit.Committer
import io.delta.kernel.data.{ColumnarBatch, ColumnVector, MapValue, Row}
import io.delta.kernel.engine._
import io.delta.kernel.expressions.{Column, Expression, ExpressionEvaluator, Literal, PartitionValueExpression, Predicate, PredicateEvaluator, ScalarExpression}
import io.delta.kernel.internal.{InternalScanFileUtils, SnapshotImpl}
import io.delta.kernel.internal.actions.{AddFile, CommitInfo, DeletionVectorDescriptor, Metadata, Protocol, RemoveFile, SingleAction}
import io.delta.kernel.internal.checksum.CRCInfo
import io.delta.kernel.internal.data.{GenericColumnVector, GenericRow}
import io.delta.kernel.internal.files.{ParsedCatalogCommitData, ParsedLogData}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.table.SnapshotBuilderImpl
import io.delta.kernel.internal.util.{ColumnMapping, FileNames, JsonUtils, Utils, VectorUtils}
import io.delta.kernel.metrics.{MetricsReport, SnapshotReport}
import io.delta.kernel.statistics.DataFileStatistics
import io.delta.kernel.test.{ActionUtils, BaseMockExpressionHandler}
import io.delta.kernel.types._
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import com.fasterxml.jackson.databind.JsonNode
import org.scalatest.funsuite.AnyFunSuite

/**
 * Green regression and I/O oracle for retained-snapshot refresh.
 *
 * The counting engine makes the performance contract observable: a correct snapshot produced by
 * rereading old commit contents is still a contract failure. CRC replay and incremental scans are
 * covered by later layers.
 */
class IncrementalSnapshotBuilderSuite extends AnyFunSuite with ActionUtils {
  private val tablePath = new Path("/incremental/table")
  private val logPath = new Path(tablePath, "_delta_log")
  private val tableSchema = new StructType().add("id", IntegerType.INTEGER)
  private val protocol = new Protocol(1, 2)

  private case class IoTrace(
      listStarts: Seq[String],
      listResults: Seq[Seq[String]],
      fileStatusProbes: Seq[String],
      rawFileReads: Seq[String],
      jsonReads: Seq[String],
      jsonReadCalls: Seq[Seq[String]],
      parquetReads: Seq[String]) {
    def commitJsonReads: Seq[String] = jsonReads.filter(FileNames.isCommitFile)
    def crcReads: Seq[String] = jsonReads.filter(FileNames.isChecksumFile)
    def checkpointReads: Seq[String] =
      parquetReads ++ jsonReads.filter(FileNames.isCheckpointFile)
    def contentReads: Seq[String] = rawFileReads ++ jsonReads ++ parquetReads
    def lastCheckpointAccesses: Seq[String] =
      (fileStatusProbes ++ contentReads).filter(path =>
        new Path(path).getName == "_last_checkpoint")
    def isEmpty: Boolean =
      listStarts.isEmpty && fileStatusProbes.isEmpty && contentReads.isEmpty &&
        jsonReadCalls.isEmpty
  }

  /**
   * In-memory engine whose storage listing and every content-opening API are independently
   * recorded. Rows are registered by path, so staged commits can intentionally differ from the
   * published commit at the same version.
   */
  private class CountingEngine private (
      initialFiles: Seq[FileStatus],
      initialRows: Map[String, Seq[Row]],
      initialCrcs: Map[String, CRCInfo]) {
    def this() = this(Seq.empty, Map.empty, Map.empty)

    private val visibleFiles = mutable.ArrayBuffer(initialFiles: _*)
    private val actionRows = mutable.Map(initialRows.toSeq: _*)
    private val crcInfos = mutable.Map(initialCrcs.toSeq: _*)

    private val listStarts = mutable.ArrayBuffer.empty[String]
    private val listResults = mutable.ArrayBuffer.empty[Seq[String]]
    private val fileStatusProbes = mutable.ArrayBuffer.empty[String]
    private val rawFileReads = mutable.ArrayBuffer.empty[String]
    private val jsonReads = mutable.ArrayBuffer.empty[String]
    private val jsonReadCalls = mutable.ArrayBuffer.empty[Seq[String]]
    private val parquetReads = mutable.ArrayBuffer.empty[String]
    private val metricReports = mutable.ArrayBuffer.empty[MetricsReport]
    private var jsonHasNextFailure = Option.empty[RuntimeException]
    private var jsonNextFailure = Option.empty[RuntimeException]
    private var jsonParseFailure = Option.empty[RuntimeException]
    private var jsonCloseFailure = Option.empty[java.io.IOException]
    private var predicateConstructionFailure = Option.empty[RuntimeException]
    private var predicateFailure = Option.empty[RuntimeException]
    private var jsonDelegateClosed = false
    private var jsonBatchSize = Int.MaxValue

    private val fileSystemClient = new FileSystemClient {
      override def listFrom(filePath: String): CloseableIterator[FileStatus] = {
        val parent = new Path(filePath).getParent
        val result = visibleFiles
          .filter(status => new Path(status.getPath).getParent == parent)
          .filter(_.getPath.compareTo(filePath) >= 0)
          .sortBy(_.getPath)
          .toSeq
        listStarts += filePath
        listResults += result.map(_.getPath)
        Utils.toCloseableIterator(result.iterator.asJava)
      }

      override def resolvePath(path: String): String = path

      override def readFiles(
          readRequests: CloseableIterator[FileReadRequest])
          : CloseableIterator[ByteArrayInputStream] = {
        val requests = consume(readRequests)
        rawFileReads ++= requests.map(_.getPath)
        Utils.toCloseableIterator(
          requests.map(_ => new ByteArrayInputStream(Array.emptyByteArray)).iterator.asJava)
      }

      override def mkdirs(path: String): Boolean =
        throw new UnsupportedOperationException("writes are not expected")

      override def delete(path: String): Boolean =
        throw new UnsupportedOperationException("writes are not expected")

      override def getFileStatus(path: String): FileStatus = {
        fileStatusProbes += path
        visibleFiles.find(_.getPath == path).getOrElse(throw new FileNotFoundException(path))
      }

      override def copyFileAtomically(
          srcPath: String,
          destPath: String,
          overwrite: Boolean): Unit =
        throw new UnsupportedOperationException("writes are not expected")
    }

    private val jsonHandler = new JsonHandler {
      override def parseJson(
          jsonStringVector: ColumnVector,
          outputSchema: StructType,
          selectionVector: Optional[ColumnVector]): ColumnarBatch = {
        jsonParseFailure.foreach(throw _)
        parsedJsonBatch(jsonStringVector, outputSchema)
      }

      override def readJsonFiles(
          fileIter: CloseableIterator[FileStatus],
          physicalSchema: StructType,
          predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
        val files = consume(fileIter)
        jsonReads ++= files.map(_.getPath)
        jsonReadCalls += files.map(_.getPath)
        val batches = files.flatMap { file =>
          val (rows, sourceSchema) = crcInfos.get(file.getPath) match {
            case Some(crc) => (Seq(crc.toRow), CRCInfo.CRC_FILE_SCHEMA)
            case None => (actionRows.getOrElse(file.getPath, Seq.empty), SingleAction.FULL_SCHEMA)
          }
          val rowGroups = if (rows.isEmpty) Seq(rows) else rows.grouped(jsonBatchSize).toSeq
          rowGroups.map(projectedBatch(_, sourceSchema, physicalSchema))
        }
        val delegate = Utils.toCloseableIterator(batches.iterator.asJava)
        new CloseableIterator[ColumnarBatch] {
          override def hasNext: Boolean = {
            jsonHasNextFailure.foreach(throw _)
            delegate.hasNext
          }

          override def next(): ColumnarBatch = {
            jsonNextFailure.foreach(throw _)
            delegate.next()
          }

          override def close(): Unit = {
            jsonDelegateClosed = true
            try delegate.close()
            finally jsonCloseFailure.foreach(throw _)
          }
        }
      }

      override def writeJsonFileAtomically(
          filePath: String,
          data: CloseableIterator[Row],
          overwrite: Boolean): Unit =
        throw new UnsupportedOperationException("writes are not expected")
    }

    private val parquetHandler = new ParquetHandler {
      override def readParquetFiles(
          fileIter: CloseableIterator[FileStatus],
          physicalSchema: StructType,
          predicate: Optional[Predicate]): CloseableIterator[FileReadResult] = {
        val files = consume(fileIter)
        parquetReads ++= files.map(_.getPath)
        val results = files.map { file =>
          val batch = projectedBatch(
            actionRows.getOrElse(file.getPath, Seq.empty),
            SingleAction.FULL_SCHEMA,
            physicalSchema)
          new FileReadResult(batch, file.getPath)
        }
        Utils.toCloseableIterator(results.iterator.asJava)
      }

      override def writeParquetFiles(
          directoryPath: String,
          dataIter: CloseableIterator[io.delta.kernel.data.FilteredColumnarBatch],
          statsColumns: util.List[io.delta.kernel.expressions.Column])
          : CloseableIterator[io.delta.kernel.utils.DataFileStatus] =
        throw new UnsupportedOperationException("writes are not expected")

      override def writeParquetFileAtomically(
          filePath: String,
          data: CloseableIterator[io.delta.kernel.data.FilteredColumnarBatch]): Unit =
        throw new UnsupportedOperationException("writes are not expected")
    }

    val engine: Engine = new Engine {
      override def getExpressionHandler: ExpressionHandler = new BaseMockExpressionHandler {
        override def getEvaluator(
            inputSchema: StructType,
            expression: Expression,
            outputType: DataType): ExpressionEvaluator = {
          val literal = expression.asInstanceOf[Literal]
          new ExpressionEvaluator {
            override def eval(input: ColumnarBatch): ColumnVector =
              columnVector(
                Seq.fill(input.getSize)(literal.getValue.asInstanceOf[AnyRef]),
                outputType)
            override def close(): Unit = {}
          }
        }

        override def getPredicateEvaluator(
            inputSchema: StructType,
            predicate: Predicate): PredicateEvaluator = {
          predicateConstructionFailure.foreach(throw _)
          new PredicateEvaluator {
            override def eval(
                input: ColumnarBatch,
                existingSelection: Optional[ColumnVector]): ColumnVector = {
              predicateFailure.foreach(throw _)
              val selected = (0 until input.getSize).map { rowId =>
                val alreadySelected = !existingSelection.isPresent ||
                  (!existingSelection.get.isNullAt(rowId) &&
                    existingSelection.get.getBoolean(rowId))
                Boolean.box(
                  alreadySelected && evaluatePredicate(
                    predicate,
                    input,
                    rowId) == Boolean.box(true))
              }
              columnVector(selected, BooleanType.BOOLEAN)
            }
          }
        }

        override def createSelectionVector(
            values: Array[Boolean],
            from: Int,
            to: Int): ColumnVector =
          columnVector(values.slice(from, to).map(Boolean.box).toSeq, BooleanType.BOOLEAN)
      }
      override def getJsonHandler: JsonHandler = jsonHandler
      override def getFileSystemClient: FileSystemClient = fileSystemClient
      override def getParquetHandler: ParquetHandler = parquetHandler
      override def getMetricsReporters: util.List[MetricsReporter] =
        Collections.singletonList(new MetricsReporter {
          override def report(metricsReport: MetricsReport): Unit =
            metricReports += metricsReport
        })
    }

    def putCommit(version: Long, rows: Seq[Row]): FileStatus = {
      val status = FileStatus.of(FileNames.deltaFile(logPath, version), 1, version * 10)
      put(status, rows)
      status
    }

    def putStagedCommit(version: Long, rows: Seq[Row]): FileStatus = {
      val status = FileStatus.of(FileNames.stagedCommitFile(logPath, version), 1, version * 10 + 1)
      actionRows.put(status.getPath, rows)
      status
    }

    def putCheckpoint(version: Long, rows: Seq[Row]): FileStatus = {
      val status = FileStatus.of(
        FileNames.checkpointFileSingular(logPath, version).toString,
        1,
        version * 10)
      put(status, rows)
      status
    }

    def putCompaction(startVersion: Long, endVersion: Long, rows: Seq[Row]): FileStatus = {
      val status = FileStatus.of(
        FileNames.logCompactionPath(logPath, startVersion, endVersion).toString,
        1,
        endVersion * 10)
      put(status, rows)
      status
    }

    def putCrc(version: Long, crcInfo: CRCInfo): FileStatus = {
      val status = FileStatus.of(FileNames.checksumFile(logPath, version).toString, 1, version * 10)
      visibleFiles += status
      crcInfos.put(status.getPath, crcInfo)
      status
    }

    def putMalformedCrc(version: Long): FileStatus = {
      val status = FileStatus.of(FileNames.checksumFile(logPath, version).toString, 1, version * 10)
      visibleFiles += status
      status
    }

    def removeVisible(path: String): Unit = {
      visibleFiles --= visibleFiles.filter(_.getPath == path)
    }

    def clearIo(): Unit = {
      listStarts.clear()
      listResults.clear()
      fileStatusProbes.clear()
      rawFileReads.clear()
      jsonReads.clear()
      jsonReadCalls.clear()
      parquetReads.clear()
    }

    def clearReports(): Unit = metricReports.clear()

    def failJsonHasNext(error: RuntimeException): Unit =
      jsonHasNextFailure = Some(error)

    def failJsonNext(error: RuntimeException): Unit =
      jsonNextFailure = Some(error)

    def failJsonParse(error: RuntimeException): Unit =
      jsonParseFailure = Some(error)

    def failJsonClose(error: java.io.IOException): Unit =
      jsonCloseFailure = Some(error)

    def failPredicateConstruction(error: RuntimeException): Unit =
      predicateConstructionFailure = Some(error)

    def failPredicateEvaluation(error: RuntimeException): Unit =
      predicateFailure = Some(error)

    def isJsonDelegateClosed: Boolean = jsonDelegateClosed

    def withJsonBatchSize(size: Int): CountingEngine = {
      require(size > 0, "JSON batch size must be positive")
      jsonBatchSize = size
      this
    }

    def reports: Seq[SnapshotReport] =
      metricReports.collect { case report: SnapshotReport => report }.toSeq

    def trace: IoTrace =
      IoTrace(
        listStarts.toSeq,
        listResults.toSeq,
        fileStatusProbes.toSeq,
        rawFileReads.toSeq,
        jsonReads.toSeq,
        jsonReadCalls.toSeq,
        parquetReads.toSeq)

    // Snapshot the currently visible files, actions, and CRCs into an independent engine.
    def fork(): CountingEngine =
      new CountingEngine(visibleFiles.toSeq, actionRows.toMap, crcInfos.toMap)

    private def put(status: FileStatus, rows: Seq[Row]): Unit = {
      visibleFiles += status
      actionRows.put(status.getPath, rows)
    }
  }

  private def consume[T](iter: CloseableIterator[T]): Seq[T] = {
    try {
      val values = mutable.ArrayBuffer.empty[T]
      while (iter.hasNext) values += iter.next()
      values.toSeq
    } finally {
      iter.close()
    }
  }

  private def projectedBatch(
      rows: Seq[Row],
      sourceSchema: StructType,
      outputSchema: StructType): ColumnarBatch = {
    val columns = (0 until outputSchema.length()).map { outputOrdinal =>
      val outputField = outputSchema.at(outputOrdinal)
      val sourceOrdinal = sourceSchema.indexOf(outputField.getName)
      val values = rows.map { row =>
        if (sourceOrdinal < 0 || row.isNullAt(sourceOrdinal)) {
          null
        } else {
          valueAt(row, sourceOrdinal, outputField.getDataType)
        }
      }
      columnVector(values, outputField.getDataType)
    }
    columnarBatch(outputSchema, columns, rows.size)
  }

  private def columnarBatch(
      schema: StructType,
      columns: Seq[ColumnVector],
      size: Int): ColumnarBatch = {
    new ColumnarBatch {
      override def getSchema: StructType = schema
      override def getColumnVector(ordinal: Int): ColumnVector = columns(ordinal)
      override def getSize: Int = size
      override def withNewColumn(
          ordinal: Int,
          columnSchema: StructField,
          columnVector: ColumnVector): ColumnarBatch = {
        require(ordinal >= 0 && ordinal <= schema.length())
        val newFields = schema.fields().asScala.patch(ordinal, Seq(columnSchema), 0).asJava
        columnarBatch(
          new StructType(newFields),
          columns.patch(ordinal, Seq(columnVector), 0),
          size)
      }
      override def withDeletedColumnAt(ordinal: Int): ColumnarBatch = {
        require(ordinal >= 0 && ordinal < schema.length())
        val remainingFields =
          (0 until schema.length()).filterNot(_ == ordinal).map(schema.at).asJava
        columnarBatch(
          new StructType(remainingFields),
          columns.patch(ordinal, Nil, 1),
          size)
      }
    }
  }

  /**
   * GenericColumnVector assumes every struct slot is non-null when extracting children. Log action
   * columns are sparse by definition, so the harness needs a null-aware struct vector.
   */
  private def columnVector(values: Seq[AnyRef], dataType: DataType): ColumnVector = dataType match {
    case structType: StructType =>
      new ColumnVector {
        override def getDataType: DataType = structType
        override def getSize: Int = values.size
        override def close(): Unit = {}
        override def isNullAt(rowId: Int): Boolean = values(rowId) == null
        override def getChild(ordinal: Int): ColumnVector = {
          val childType = structType.at(ordinal).getDataType
          val childValues = values.map {
            case null => null
            case row: Row if row.isNullAt(ordinal) => null
            case row: Row => valueAt(row, ordinal, childType)
            case other =>
              throw new IllegalArgumentException(s"Expected Row for struct vector, found $other")
          }
          columnVector(childValues, childType)
        }
      }
    case _ => new GenericColumnVector(values.asJava, dataType)
  }

  private def parsedJsonBatch(
      jsonStringVector: ColumnVector,
      outputSchema: StructType): ColumnarBatch = {
    val roots = (0 until jsonStringVector.getSize).map { rowId =>
      if (jsonStringVector.isNullAt(rowId)) null
      else JsonUtils.mapper().readTree(jsonStringVector.getString(rowId))
    }
    val columns = outputSchema.fields().asScala.map { field =>
      columnVector(
        roots.map(root =>
          if (root == null) null else jsonValue(root.get(field.getName), field.getDataType)),
        field.getDataType)
    }
    columnarBatch(outputSchema, columns.toSeq, roots.size)
  }

  private def jsonValue(node: JsonNode, dataType: DataType): AnyRef = {
    if (node == null || node.isNull) {
      return null
    }
    dataType match {
      case structType: StructType =>
        val values = new util.HashMap[Integer, Object]()
        structType.fields().asScala.zipWithIndex.foreach { case (field, ordinal) =>
          values.put(ordinal, jsonValue(node.get(field.getName), field.getDataType))
        }
        new GenericRow(structType, values)
      case _: BooleanType => Boolean.box(node.asBoolean())
      case _: IntegerType | _: DateType => Int.box(node.asInt())
      case _: LongType | _: TimestampType | _: TimestampNTZType => Long.box(node.asLong())
      case _: FloatType => Float.box(node.floatValue())
      case _: DoubleType => Double.box(node.doubleValue())
      case _: StringType => node.asText()
      case other =>
        throw new UnsupportedOperationException(s"Unsupported JSON fixture type: $other")
    }
  }

  private def evaluatePredicate(
      predicate: Predicate,
      input: ColumnarBatch,
      rowId: Int): java.lang.Boolean = {
    val name = predicate.getName.toUpperCase(java.util.Locale.ROOT)
    val children = predicate.getChildren.asScala
    name match {
      case "ALWAYS_TRUE" => Boolean.box(true)
      case "ALWAYS_FALSE" => Boolean.box(false)
      case "AND" =>
        sqlAnd(
          evaluateExpression(children.head, input, rowId).asInstanceOf[java.lang.Boolean],
          evaluateExpression(children(1), input, rowId).asInstanceOf[java.lang.Boolean])
      case "OR" =>
        sqlOr(
          evaluateExpression(children.head, input, rowId).asInstanceOf[java.lang.Boolean],
          evaluateExpression(children(1), input, rowId).asInstanceOf[java.lang.Boolean])
      case "=" =>
        val left = evaluateExpression(children.head, input, rowId)
        val right = evaluateExpression(children(1), input, rowId)
        if (left == null || right == null) null else Boolean.box(left == right)
      case "<" | "<=" | ">" | ">=" =>
        val left = evaluateExpression(children.head, input, rowId)
        val right = evaluateExpression(children(1), input, rowId)
        if (left == null || right == null) {
          null
        } else {
          val comparison = compareFixtureValues(left, right)
          Boolean.box(name match {
            case "<" => comparison < 0
            case "<=" => comparison <= 0
            case ">" => comparison > 0
            case ">=" => comparison >= 0
          })
        }
      case other =>
        throw new UnsupportedOperationException(s"Unsupported fixture predicate: $other")
    }
  }

  private def sqlAnd(
      left: java.lang.Boolean,
      right: java.lang.Boolean): java.lang.Boolean = {
    if (left == Boolean.box(false) || right == Boolean.box(false)) Boolean.box(false)
    else if (left == null || right == null) null
    else Boolean.box(true)
  }

  private def sqlOr(
      left: java.lang.Boolean,
      right: java.lang.Boolean): java.lang.Boolean = {
    if (left == Boolean.box(true) || right == Boolean.box(true)) Boolean.box(true)
    else if (left == null || right == null) null
    else Boolean.box(false)
  }

  private def evaluateExpression(
      expression: Expression,
      input: ColumnarBatch,
      rowId: Int): AnyRef = expression match {
    case predicate: Predicate => evaluatePredicate(predicate, input, rowId)
    case literal: Literal => literal.getValue.asInstanceOf[AnyRef]
    case column: Column => columnValue(input, column.getNames, rowId)
    case partitionValue: PartitionValueExpression =>
      val serialized = evaluateExpression(partitionValue.getInput, input, rowId)
      if (serialized == null) {
        null
      } else {
        partitionValue.getDataType match {
          case _: IntegerType => Int.box(serialized.toString.toInt)
          case _: LongType => Long.box(serialized.toString.toLong)
          case _: StringType => serialized.toString
          case other =>
            throw new UnsupportedOperationException(
              s"Unsupported partition fixture type: $other")
        }
      }
    case scalar: ScalarExpression =>
      val values = scalar.getChildren.asScala.map(evaluateExpression(_, input, rowId))
      scalar.getName.toUpperCase(java.util.Locale.ROOT) match {
        case "ELEMENT_AT" => mapValue(values.head.asInstanceOf[MapValue], values(1).toString)
        case "COALESCE" => values.find(_ != null).orNull
        case other =>
          throw new UnsupportedOperationException(s"Unsupported fixture scalar: $other")
      }
    case other => throw new UnsupportedOperationException(s"Unsupported fixture expression: $other")
  }

  private def columnValue(
      input: ColumnarBatch,
      names: Array[String],
      rowId: Int): AnyRef = {
    var schema = input.getSchema
    var vector = input.getColumnVector(schema.indexOf(names.head))
    names.tail.foreach { name =>
      schema = vector.getDataType.asInstanceOf[StructType]
      vector = vector.getChild(schema.indexOf(name))
    }
    valueFromVector(vector, rowId)
  }

  private def valueFromVector(vector: ColumnVector, rowId: Int): AnyRef = {
    if (vector.isNullAt(rowId)) {
      return null
    }
    vector.getDataType match {
      case _: BooleanType => Boolean.box(vector.getBoolean(rowId))
      case _: IntegerType | _: DateType => Int.box(vector.getInt(rowId))
      case _: LongType | _: TimestampType | _: TimestampNTZType => Long.box(vector.getLong(rowId))
      case _: FloatType => Float.box(vector.getFloat(rowId))
      case _: DoubleType => Double.box(vector.getDouble(rowId))
      case _: StringType => vector.getString(rowId)
      case _: MapType => vector.getMap(rowId)
      case other => throw new UnsupportedOperationException(s"Unsupported fixture vector: $other")
    }
  }

  private def mapValue(map: MapValue, key: String): AnyRef = {
    if (map == null) {
      return null
    }
    (0 until map.getSize)
      .find(index => map.getKeys.getString(index) == key)
      .map(index => valueFromVector(map.getValues, index))
      .orNull
  }

  private def compareFixtureValues(left: AnyRef, right: AnyRef): Int = (left, right) match {
    case (leftNumber: Number, rightNumber: Number) =>
      java.lang.Double.compare(leftNumber.doubleValue(), rightNumber.doubleValue())
    case (leftValue: String, rightValue: String) => leftValue.compareTo(rightValue)
    case _ => throw new UnsupportedOperationException(s"Cannot compare $left and $right")
  }

  private def projectedRow(row: Row, outputSchema: StructType): Row = {
    val values = new util.HashMap[Integer, Object]()
    outputSchema.fields().asScala.zipWithIndex.foreach { case (field, outputOrdinal) =>
      val sourceOrdinal = row.getSchema.indexOf(field.getName)
      if (sourceOrdinal >= 0 && !row.isNullAt(sourceOrdinal)) {
        values.put(outputOrdinal, valueAt(row, sourceOrdinal, field.getDataType))
      }
    }
    new GenericRow(outputSchema, values)
  }

  private def valueAt(row: Row, ordinal: Int, dataType: DataType): AnyRef = dataType match {
    case _: BooleanType => Boolean.box(row.getBoolean(ordinal))
    case _: ByteType => Byte.box(row.getByte(ordinal))
    case _: ShortType => Short.box(row.getShort(ordinal))
    case _: IntegerType | _: DateType => Int.box(row.getInt(ordinal))
    case _: LongType | _: TimestampType | _: TimestampNTZType => Long.box(row.getLong(ordinal))
    case _: FloatType => Float.box(row.getFloat(ordinal))
    case _: DoubleType => Double.box(row.getDouble(ordinal))
    case _: StringType => row.getString(ordinal)
    case _: BinaryType => row.getBinary(ordinal)
    case _: DecimalType => row.getDecimal(ordinal)
    case outputType: StructType => projectedRow(row.getStruct(ordinal), outputType)
    case _: ArrayType => row.getArray(ordinal)
    case _: MapType => row.getMap(ordinal)
    case other => throw new UnsupportedOperationException(s"Unsupported fixture type: $other")
  }

  private def metadata(label: String): Metadata =
    testMetadata(tableSchema, tblProps = Map("fixture.state" -> label))

  private def protocolAction(value: Protocol = protocol): Row =
    SingleAction.createProtocolSingleAction(value.toRow)

  private def metadataAction(value: Metadata): Row =
    SingleAction.createMetadataSingleAction(value.toRow)

  private def addAction(
      path: String,
      deletionVector: Option[DeletionVectorDescriptor] = None): Row = {
    val add = AddFile.createAddFileRow(
      tableSchema,
      path,
      VectorUtils.stringStringMapValue(Collections.emptyMap()),
      10,
      100,
      true,
      Optional.ofNullable(deletionVector.orNull),
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.empty())
    SingleAction.createAddFileSingleAction(add)
  }

  private def deletionVector(id: String): DeletionVectorDescriptor =
    new DeletionVectorDescriptor("i", id, Optional.empty(), id.length, 1)

  private val filterSchema =
    new StructType().add("p", StringType.STRING).add("id", IntegerType.INTEGER)
  private val filterMetadata = testMetadata(filterSchema, partitionCols = Seq("p"))

  private def filteredAddAction(
      path: String,
      partition: String,
      minId: Int,
      maxId: Int): Row = {
    val idColumn = new Column("id")
    val stats = new DataFileStatistics(
      10,
      Collections.singletonMap(idColumn, Literal.ofInt(minId)),
      Collections.singletonMap(idColumn, Literal.ofInt(maxId)),
      Collections.singletonMap[Column, java.lang.Long](idColumn, Long.box(0L)),
      Optional.empty())
    partitionedAddAction(
      filterSchema,
      path,
      "p",
      partition,
      Optional.of(stats))
  }

  private def statsLessFilteredAddAction(path: String, partition: String): Row =
    partitionedAddAction(
      filterSchema,
      path,
      "p",
      partition,
      Optional.empty())

  private def partitionedAddAction(
      schema: StructType,
      path: String,
      partitionKey: String,
      partition: String,
      stats: Optional[DataFileStatistics]): Row = {
    val add = AddFile.createAddFileRow(
      schema,
      path,
      VectorUtils.stringStringMapValue(Collections.singletonMap(partitionKey, partition)),
      10,
      100,
      true,
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      stats)
    SingleAction.createAddFileSingleAction(add)
  }

  private def removeAction(
      path: String,
      deletionVector: Option[DeletionVectorDescriptor] = None): Row = {
    val values = new util.HashMap[Integer, Object]()
    values.put(RemoveFile.FULL_SCHEMA.indexOf("path"), path)
    values.put(RemoveFile.FULL_SCHEMA.indexOf("dataChange"), Boolean.box(true))
    deletionVector.foreach(value =>
      values.put(RemoveFile.FULL_SCHEMA.indexOf("deletionVector"), value.toRow))
    SingleAction.createRemoveFileSingleAction(new GenericRow(RemoveFile.FULL_SCHEMA, values))
  }

  private def initialActions(label: String): Seq[Row] =
    Seq(protocolAction(), metadataAction(metadata(label)), addAction("base.parquet"))

  private def crcAt(
      version: Long,
      label: String,
      tableSizeBytes: Long = 10,
      numFiles: Long = 1): CRCInfo =
    new CRCInfo(
      version,
      metadata(label),
      protocol,
      tableSizeBytes,
      numFiles,
      Optional.empty(),
      Optional.empty(),
      Optional.empty())

  private def commitWithOperation(operation: String, actions: Row*): Seq[Row] = {
    val commitInfo = new CommitInfo(
      Optional.empty(),
      1L,
      Optional.of("test-engine"),
      Optional.of(operation),
      Collections.emptyMap(),
      Optional.of(true),
      Optional.of("txn"),
      Collections.emptyMap())
    SingleAction.createCommitInfoSingleAction(commitInfo.toRow) +: actions
  }

  private def writeCommit(actions: Row*): Seq[Row] =
    commitWithOperation("WRITE", actions: _*)

  private def coldSnapshot(
      counting: CountingEngine,
      version: Long,
      logData: Seq[ParsedLogData] = Seq.empty,
      configure: SnapshotBuilder => SnapshotBuilder = identity): SnapshotImpl = {
    var builder = TableManager.loadSnapshot(tablePath.toString).atVersion(version)
    if (logData.nonEmpty) {
      builder = builder.withLogData(logData.asJava)
    }
    configure(builder).build(counting.engine).asInstanceOf[SnapshotImpl]
  }

  private def coldLatestSnapshot(
      counting: CountingEngine,
      logData: Seq[ParsedLogData] = Seq.empty,
      configure: SnapshotBuilder => SnapshotBuilder = identity): SnapshotImpl = {
    var builder = TableManager.loadSnapshot(tablePath.toString)
    if (logData.nonEmpty) {
      builder = builder.withLogData(logData.asJava)
    }
    configure(builder).build(counting.engine).asInstanceOf[SnapshotImpl]
  }

  private def from(
      counting: CountingEngine,
      base: SnapshotImpl,
      version: Option[Long] = None,
      logData: Seq[ParsedLogData] = Seq.empty,
      configure: SnapshotBuilder => SnapshotBuilder = identity): SnapshotImpl = {
    var builder: SnapshotBuilder = TableManager.builderFrom(tablePath.toString, base)
    version.foreach(v => builder = builder.atVersion(v))
    if (logData.nonEmpty) {
      builder = builder.withLogData(logData.asJava)
    }
    try {
      configure(builder).build(counting.engine).asInstanceOf[SnapshotImpl]
    } finally {
      assertNoLastCheckpoint(counting.trace)
    }
  }

  private def listedStartVersion(path: String): Long =
    new Path(path).getName.takeWhile(_.isDigit).toLong

  private def assertSingleListFrom(trace: IoTrace, expectedVersion: Long): Unit = {
    assert(
      trace.listStarts.map(listedStartVersion) == Seq(expectedVersion),
      s"expected exactly one LIST from version $expectedVersion: $trace")
  }

  private def assertNoLastCheckpoint(trace: IoTrace): Unit = {
    assert(
      trace.lastCheckpointAccesses.isEmpty,
      s"builderFrom must not probe or open _last_checkpoint: $trace")
  }

  private def assertCrcRefreshIo(
      trace: IoTrace,
      listFromVersion: Long,
      crcPaths: Seq[String],
      commitVersions: Seq[Long],
      checkpointPaths: Seq[String] = Seq.empty): Unit = {
    assertSingleListFrom(trace, listFromVersion)
    assertNoLastCheckpoint(trace)
    assert(trace.fileStatusProbes.isEmpty)
    assert(trace.crcReads == crcPaths)
    assert(trace.commitJsonReads.map(FileNames.deltaVersion).sorted == commitVersions.sorted)
    assert(trace.checkpointReads == checkpointPaths)
  }

  private def checkpointActions(label: String, addVersions: Seq[Long]): Seq[Row] =
    Seq(protocolAction(), metadataAction(metadata(label)), addAction("base.parquet")) ++
      addVersions.map(version => addAction(s"v$version.parquet"))

  private def scanPaths(snapshot: SnapshotImpl, engine: Engine): Seq[String] = {
    val batches = snapshot.getScanBuilder.build().getScanFiles(engine)
    try {
      val paths = mutable.ArrayBuffer.empty[String]
      while (batches.hasNext) {
        val rows = batches.next().getRows
        try {
          while (rows.hasNext) {
            paths += InternalScanFileUtils.getAddFileStatus(rows.next()).getPath
          }
        } finally {
          rows.close()
        }
      }
      paths.sorted.toSeq
    } finally {
      batches.close()
    }
  }

  private def scanFileSemantics(
      snapshot: SnapshotImpl,
      engine: Engine,
      filter: Option[Predicate] = None): Seq[(FileStatus, AddFile)] = {
    val scanBuilder = snapshot.getScanBuilder
    filter.foreach(scanBuilder.withFilter)
    val batches = scanBuilder.build().getScanFiles(engine)
    try {
      val files = mutable.ArrayBuffer.empty[(FileStatus, AddFile)]
      while (batches.hasNext) {
        val rows = batches.next().getRows
        try {
          while (rows.hasNext) {
            val row = rows.next()
            val status = InternalScanFileUtils.getAddFileStatus(row)
            val add = new AddFile(row.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL))
            files += status -> add
          }
        } finally {
          rows.close()
        }
      }
      files.sortBy(_._1.getPath).toSeq
    } finally {
      batches.close()
    }
  }

  private def assertEquivalent(
      actual: SnapshotImpl,
      actualEngine: Engine,
      expected: SnapshotImpl,
      expectedEngine: Engine): Unit = {
    // Build intent is intentionally omitted: promotion tests assert wasBuiltAsLatest separately.
    assert(actual.getVersion == expected.getVersion)
    assert(actual.getProtocol == expected.getProtocol)

    val actualMetadata = actual.getMetadata
    val expectedMetadata = expected.getMetadata
    assert(actualMetadata.getSchema == expectedMetadata.getSchema)
    assert(actualMetadata.getFormat == expectedMetadata.getFormat)
    assert(
      VectorUtils.toJavaList(actualMetadata.getPartitionColumns) ==
        VectorUtils.toJavaList(expectedMetadata.getPartitionColumns))
    assert(actualMetadata.getConfiguration == expectedMetadata.getConfiguration)
    assert(actualMetadata == expectedMetadata)

    val actualSegment = actual.getLogSegment
    val expectedSegment = expected.getLogSegment
    assert(actualSegment.getLogPath == expectedSegment.getLogPath)
    assert(actualSegment.getVersion == expectedSegment.getVersion)
    assert(actualSegment.getCheckpointVersionOpt == expectedSegment.getCheckpointVersionOpt)
    assert(actualSegment.getDeltas == expectedSegment.getDeltas)
    assert(actualSegment.getCompactions == expectedSegment.getCompactions)
    assert(actualSegment.getCheckpoints == expectedSegment.getCheckpoints)
    assert(actualSegment.getDeltaFileAtEndVersion == expectedSegment.getDeltaFileAtEndVersion)
    assert(
      actualSegment.getMaxPublishedDeltaVersion == expectedSegment.getMaxPublishedDeltaVersion)
    assert(actualSegment.getLastSeenChecksum == expectedSegment.getLastSeenChecksum)

    // getCurrentCrcInfo is authoritative only at the snapshot version. CRCInfo equality covers all
    // fields available in this revision, including P&M, stats, transactions, DV counts, and files.
    assert(actual.getCurrentCrcInfo == expected.getCurrentCrcInfo)
    assert(scanPaths(actual, actualEngine) == scanPaths(expected, expectedEngine))
  }

  private def assertMatchesCold(
      actual: SnapshotImpl,
      counting: CountingEngine,
      version: Long,
      logData: Seq[ParsedLogData] = Seq.empty,
      configure: SnapshotBuilder => SnapshotBuilder = identity): Unit = {
    val coldEngine = counting.fork()
    val expected = coldSnapshot(coldEngine, version, logData, configure)
    assertEquivalent(actual, counting.engine, expected, coldEngine.engine)
  }

  /**
   * Compares observable snapshot behavior when cold and incremental loading intentionally choose
   * different physical segment representations. This must remain narrower than assertMatchesCold:
   * it excludes segment structure while retaining final P&M, CRC, and complete AddFile semantics.
   */
  private def assertBehaviorMatchesCold(
      actual: SnapshotImpl,
      counting: CountingEngine,
      version: Long): Unit = {
    val coldEngine = counting.fork()
    val expected = coldSnapshot(coldEngine, version)

    assert(actual.getVersion == expected.getVersion)
    assert(actual.getProtocol == expected.getProtocol)
    assert(actual.getMetadata == expected.getMetadata)
    assert(actual.getCurrentCrcInfo == expected.getCurrentCrcInfo)
    assert(
      scanFileSemantics(actual, counting.engine) ==
        scanFileSemantics(expected, coldEngine.engine))
  }

  private def incrementalPaths(snapshot: SnapshotImpl, counting: CountingEngine): Seq[String] = {
    val scan = snapshot.getIncrementalScanBuilder(0).build(counting.engine).get()
    try {
      val batches = scan.getLiveAddBatches
      val paths = mutable.ArrayBuffer.empty[String]
      while (batches.hasNext) {
        val rows = batches.next().getRows
        try {
          while (rows.hasNext) {
            paths += InternalScanFileUtils.getAddFileStatus(rows.next()).getPath
          }
        } finally {
          rows.close()
        }
      }
      paths.toSeq
    } finally {
      scan.close()
    }
  }

  private def consumeLivePaths(scan: io.delta.kernel.IncrementalScan): Seq[String] = {
    val paths = mutable.ArrayBuffer.empty[String]
    val batches = scan.getLiveAddBatches
    while (batches.hasNext) {
      val rows = batches.next().getRows
      try {
        while (rows.hasNext) {
          paths += InternalScanFileUtils.getAddFileStatus(rows.next()).getPath
        }
      } finally {
        rows.close()
      }
    }
    paths.toSeq
  }

  private def keyOf(add: AddFile): FileActionKey =
    new FileActionKey(
      new URI(add.getPath),
      add.getDeletionVector.map[String](_.getUniqueId))

  test("file-action identity preserves raw URI spelling in full and incremental scans") {
    val dotted = new FileActionKey(new URI("a/./b"), Optional.empty())
    val plain = new FileActionKey(new URI("a/b"), Optional.empty())
    assert(dotted.getPath.toString == "a/./b")
    assert(plain.getPath.toString == "a/b")
    assert(dotted != plain)

    val counting = new CountingEngine
    counting.putCommit(0, Seq(protocolAction(), metadataAction(metadata("v0"))))
    counting.putCommit(1, Seq(addAction("a/./b"), addAction("a/b")))
    val target = coldSnapshot(counting, 1)

    assert(scanFileSemantics(target, counting.fork().engine).map(_._2.getPath) == Seq(
      "a/./b",
      "a/b"))
    val incremental = target.getIncrementalScanBuilder(0).build(counting.fork().engine).get()
    assert(consumeLivePaths(incremental).size == 2)
    assert(incremental.finish().getLiveAdds.asScala.map(_.getPath.toString) == Set("a/./b", "a/b"))
  }

  test("incremental scan rejects invalid ranges before I/O") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, Seq(addAction("v1.parquet")))
    val target = coldSnapshot(counting, 1)
    val scanEngine = counting.fork()

    Seq(1L, 2L).foreach { baseVersion =>
      val error = intercept[IllegalArgumentException] {
        target.getIncrementalScanBuilder(baseVersion).build(scanEngine.engine)
      }
      assert(error.getMessage.contains("must be less than target version"))
    }
    assert(scanEngine.trace.isEmpty)
  }

  test("incremental scan returns empty when retained deltas do not cover its start") {
    val counting = new CountingEngine
    counting.putCommit(2, initialActions("v2"))
    counting.putCheckpoint(2, checkpointActions("v2", Seq.empty))
    counting.putCommit(3, Seq(addAction("v3.parquet")))
    val target = coldSnapshot(counting, 3)
    val scanEngine = counting.fork()

    assert(!target.getIncrementalScanBuilder(0).build(scanEngine.engine).isPresent)
    assert(scanEngine.trace.isEmpty)
  }

  test("incremental scan clips raw commits and reconciles Add and Remove actions newest-first") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    val commit1 = counting.putCommit(1, Seq(addAction("a.parquet")))
    val commit2 =
      counting.putCommit(2, Seq(removeAction("a.parquet"), addAction("b.parquet")))
    val commit3 = counting.putCommit(3, Seq(addAction("c.parquet")))
    val target = coldSnapshot(counting, 3)
    val scanEngine = counting.fork()

    val scan = target.getIncrementalScanBuilder(0).build(scanEngine.engine).get()
    val paths =
      try {
        val batches = scan.getLiveAddBatches
        val result = mutable.ArrayBuffer.empty[String]
        while (batches.hasNext) {
          val rows = batches.next().getRows
          try {
            while (rows.hasNext) {
              result += InternalScanFileUtils.getAddFileStatus(rows.next()).getPath
            }
          } finally {
            rows.close()
          }
        }
        result.toSeq
      } finally {
        scan.close()
      }

    assert(paths == Seq(s"$tablePath/c.parquet", s"$tablePath/b.parquet"))
    assert(scanEngine.trace.jsonReadCalls == Seq(Seq(
      commit3.getPath,
      commit2.getPath,
      commit1.getPath)))
    assert(scanEngine.trace.listStarts.isEmpty)
    assert(scanEngine.trace.fileStatusProbes.isEmpty)
    assert(scanEngine.trace.checkpointReads.isEmpty)
  }

  test("incremental scan keeps only the newest duplicate Add") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, Seq(addAction("duplicate.parquet")))
    counting.putCommit(2, Seq(addAction("duplicate.parquet")))
    val target = coldSnapshot(counting, 2)
    val scan = target.getIncrementalScanBuilder(0).build(counting.fork().engine).get()

    val paths = mutable.ArrayBuffer.empty[String]
    val batches = scan.getLiveAddBatches
    while (batches.hasNext) {
      val rows = batches.next().getRows
      try {
        while (rows.hasNext) {
          paths += InternalScanFileUtils.getAddFileStatus(rows.next()).getPath
        }
      } finally {
        rows.close()
      }
    }
    val summary = scan.finish()

    assert(paths == Seq(s"$tablePath/duplicate.parquet"))
    assert(summary.getLiveAdds.asScala.map(_.getPath.toString) == Set("duplicate.parquet"))
    assert(summary.getRemoves.isEmpty)
    assert(summary.getDuplicateAdds.isEmpty)
  }

  test("incremental scan reconciles the same path and deletion vector newest-first") {
    val dv = deletionVector("same")
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(
      1,
      Seq(addAction("remove-wins.parquet", Some(dv)), removeAction("add-wins.parquet", Some(dv))))
    counting.putCommit(
      2,
      Seq(removeAction("remove-wins.parquet", Some(dv)), addAction("add-wins.parquet", Some(dv))))
    val scan = coldSnapshot(counting, 2)
      .getIncrementalScanBuilder(0)
      .build(counting.fork().engine)
      .get()

    assert(consumeLivePaths(scan) == Seq(s"$tablePath/add-wins.parquet"))
    val summary = scan.finish()
    assert(summary.getLiveAdds.asScala.map(_.getPath.toString) == Set("add-wins.parquet"))
    assert(summary.getRemoves.asScala.map(_.getPath.toString) == Set("remove-wins.parquet"))
  }

  test("incremental scan keeps the same path with different deletion vectors distinct") {
    val oldDv = deletionVector("old")
    val newDv = deletionVector("new")
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, Seq(addAction("data.parquet", Some(oldDv))))
    counting.putCommit(2, Seq(addAction("data.parquet", Some(newDv))))
    val scan = coldSnapshot(counting, 2)
      .getIncrementalScanBuilder(0)
      .build(counting.fork().engine)
      .get()

    assert(consumeLivePaths(scan).size == 2)
    val summary = scan.finish()
    assert(summary.getLiveAdds.asScala.map(_.getDeletionVectorId.get()) ==
      Set(oldDv.getUniqueId, newDv.getUniqueId))
    assert(summary.getRemoves.isEmpty)
  }

  test("incremental scan reports old-DV removal and new-DV Add independently") {
    val oldDv = deletionVector("old")
    val newDv = deletionVector("new")
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(
      1,
      Seq(removeAction("data.parquet", Some(oldDv)), addAction("data.parquet", Some(newDv))))
    val scan = coldSnapshot(counting, 1)
      .getIncrementalScanBuilder(0)
      .build(counting.fork().engine)
      .get()

    assert(consumeLivePaths(scan) == Seq(s"$tablePath/data.parquet"))
    val summary = scan.finish()
    assert(summary.getLiveAdds.asScala.map(_.getDeletionVectorId.get()) == Set(newDv.getUniqueId))
    assert(summary.getRemoves.asScala.map(_.getDeletionVectorId.get()) == Set(oldDv.getUniqueId))
  }

  test("incremental scan finishAgainstBase matches the complete file-action key") {
    val oldDv = deletionVector("old")
    val newDv = deletionVector("new")
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, Seq(addAction("data.parquet", Some(newDv))))
    val target = coldSnapshot(counting, 1)
    val oldKey = new FileActionKey(new URI("data.parquet"), Optional.of(oldDv.getUniqueId))
    val newKey = new FileActionKey(new URI("data.parquet"), Optional.of(newDv.getUniqueId))

    val oldOnly = target.getIncrementalScanBuilder(0).build(counting.fork().engine).get()
    assert(oldOnly.finishAgainstBase(Set(oldKey).contains).getDuplicateAdds.isEmpty)

    val exact = target.getIncrementalScanBuilder(0).build(counting.fork().engine).get()
    assert(exact.finishAgainstBase(Set(
      oldKey,
      newKey).contains).getDuplicateAdds == Set(newKey).asJava)
  }

  test("incremental scan includes staged commits from the retained log tail") {
    val counting = new CountingEngine
    counting.putCommit(
      0,
      Seq(
        protocolAction(protocolWithCatalogManagedSupport),
        metadataAction(metadata("v0")),
        addAction("base.parquet")))
    val staged1 = counting.putStagedCommit(1, Seq(addAction("staged-1.parquet")))
    val staged2 = counting.putStagedCommit(2, Seq(addAction("staged-2.parquet")))
    val logData = Seq(staged1, staged2).map(ParsedCatalogCommitData.forFileStatus)
    val target = coldSnapshot(counting, 2, logData, _.withMaxCatalogVersion(2))
    val scanEngine = counting.fork()

    assert(incrementalPaths(target, scanEngine) ==
      Seq(s"$tablePath/staged-2.parquet", s"$tablePath/staged-1.parquet"))
    assert(scanEngine.trace.jsonReadCalls == Seq(Seq(staged2.getPath, staged1.getPath)))
    assert(scanEngine.trace.listStarts.isEmpty)
    assert(scanEngine.trace.checkpointReads.isEmpty)
  }

  test("incremental scan finish drains unread action batches") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, Seq(addAction("a.parquet")))
    counting.putCommit(2, Seq(addAction("b.parquet")))
    counting.putCommit(3, Seq(removeAction("a.parquet"), addAction("c.parquet")))
    val target = coldSnapshot(counting, 3)
    val scan = target.getIncrementalScanBuilder(0).build(counting.fork().engine).get()

    val batches = scan.getLiveAddBatches
    assert(batches.hasNext)
    val firstRows = batches.next().getRows
    try {
      assert(firstRows.hasNext)
      assert(
        InternalScanFileUtils.getAddFileStatus(firstRows.next()).getPath ==
          s"$tablePath/c.parquet")
    } finally {
      firstRows.close()
    }

    val summary = scan.finish()
    assert(summary.getBaseVersion == 0)
    assert(summary.getTargetVersion == 3)
    assert(summary.getLiveAdds.asScala.map(_.getPath.toString) == Set("b.parquet", "c.parquet"))
    assert(summary.getRemoves.asScala.map(_.getPath.toString) == Set("a.parquet"))
  }

  test("incremental scan finishAgainstBase classifies re-added files") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(
      1,
      Seq(addAction("brand-new.parquet"), addAction("re-added.parquet")))
    val target = coldSnapshot(counting, 1)
    val scan = target.getIncrementalScanBuilder(0).build(counting.fork().engine).get()

    val summary = scan.finishAgainstBase(_.getPath.toString == "re-added.parquet")

    assert(
      summary.getLiveAdds.asScala.map(_.getPath.toString) ==
        Set("brand-new.parquet", "re-added.parquet"))
    assert(summary.getDuplicateAdds.asScala.map(_.getPath.toString) == Set("re-added.parquet"))
    assert(summary.getRemoves.isEmpty)
  }

  test("incremental scan applies partition and data filters with Scan semantics") {
    val partitionFilter = new Predicate("=", new Column("p"), Literal.ofString("keep"))
    val dataFilter = new Predicate("=", new Column("id"), Literal.ofInt(5))
    val mixedFilter = new Predicate("AND", partitionFilter, dataFilter)
    val counting = new CountingEngine
    counting.putCommit(0, Seq(protocolAction(), metadataAction(filterMetadata)))
    counting.putCommit(
      1,
      Seq(
        filteredAddAction("both.parquet", "keep", 4, 6),
        filteredAddAction("partition-only.parquet", "keep", 10, 20),
        filteredAddAction("data-only.parquet", "drop", 4, 6),
        filteredAddAction("neither.parquet", "drop", 10, 20)))
    val target = coldSnapshot(counting, 1)

    val cases = Seq(
      (partitionFilter, Set("both.parquet", "partition-only.parquet"), false),
      (dataFilter, Set("both.parquet", "data-only.parquet"), true),
      (mixedFilter, Set("both.parquet"), true))
    val allAddIdentities =
      Set("both.parquet", "partition-only.parquet", "data-only.parquet", "neither.parquet")

    cases.foreach { case (filter, expected, hasRemainingFilter) =>
      val scan = target
        .getIncrementalScanBuilder(0)
        .withFilter(filter)
        .build(counting.fork().engine)
        .get()
      val paths = consumeLivePaths(scan).map(path => new Path(path).getName).toSet
      val summary = scan.finish()
      assert(paths == expected)
      assert(summary.getLiveAdds.asScala.map(_.getPath.toString) == allAddIdentities)
      assert(scan.getRemainingFilter.isPresent == hasRemainingFilter)
    }
  }

  test("incremental scan keeps stats-less Adds under a data filter") {
    val dataFilter = new Predicate("=", new Column("id"), Literal.ofInt(5))
    val counting = new CountingEngine
    counting.putCommit(0, Seq(protocolAction(), metadataAction(filterMetadata)))
    counting.putCommit(
      1,
      Seq(
        statsLessFilteredAddAction("no-stats.parquet", "keep"),
        filteredAddAction("excluded.parquet", "keep", 10, 20)))
    val scan = coldSnapshot(counting, 1)
      .getIncrementalScanBuilder(0)
      .withFilter(dataFilter)
      .build(counting.fork().engine)
      .get()

    assert(consumeLivePaths(scan) == Seq(s"$tablePath/no-stats.parquet"))
    val summary = scan.finish()
    assert(summary.getLiveAdds.asScala.map(_.getPath.toString) ==
      Set("excluded.parquet", "no-stats.parquet"))
    assert(scan.getRemainingFilter == Optional.of(dataFilter))
  }

  test("incremental scan filter composes with finishAgainstBase") {
    val dataFilter = new Predicate("=", new Column("id"), Literal.ofInt(5))
    val counting = new CountingEngine
    counting.putCommit(0, Seq(protocolAction(), metadataAction(filterMetadata)))
    counting.putCommit(
      1,
      Seq(
        filteredAddAction("existing.parquet", "keep", 4, 6),
        filteredAddAction("new.parquet", "keep", 4, 6),
        filteredAddAction("excluded.parquet", "keep", 10, 20)))
    val scan = coldSnapshot(counting, 1)
      .getIncrementalScanBuilder(0)
      .withFilter(dataFilter)
      .build(counting.fork().engine)
      .get()
    val existingKey = new FileActionKey(new URI("existing.parquet"), Optional.empty())

    val summary = scan.finishAgainstBase(Set(existingKey).contains)

    assert(summary.getLiveAdds.asScala.map(_.getPath.toString) ==
      Set("excluded.parquet", "existing.parquet", "new.parquet"))
    assert(summary.getDuplicateAdds == Set(existingKey).asJava)
    assert(summary.getRemoves.isEmpty)
  }

  test("incremental scan results are independent of JSON action batch size") {
    val dataFilter = new Predicate("=", new Column("id"), Literal.ofInt(5))
    val counting = new CountingEngine
    counting.putCommit(0, Seq(protocolAction(), metadataAction(filterMetadata)))
    counting.putCommit(
      1,
      Seq(
        filteredAddAction("a.parquet", "keep", 4, 6),
        filteredAddAction("excluded.parquet", "keep", 10, 20),
        removeAction("removed.parquet"),
        filteredAddAction("b.parquet", "keep", 4, 6)))
    val target = coldSnapshot(counting, 1)

    val results = Seq(Int.MaxValue, 2, 1).map { batchSize =>
      val scanEngine = counting.fork().withJsonBatchSize(batchSize)
      val scan = target
        .getIncrementalScanBuilder(0)
        .withFilter(dataFilter)
        .build(scanEngine.engine)
        .get()
      val paths = consumeLivePaths(scan).map(path => new Path(path).getName).toSet
      val summary = scan.finish()
      (
        paths,
        summary.getLiveAdds.asScala.toSet,
        summary.getRemoves.asScala.toSet,
        summary.getDuplicateAdds.asScala.toSet)
    }

    assert(results.distinct.size == 1)
    assert(results.head._1 == Set("a.parquet", "b.parquet"))
    assert(results.head._3.map(_.getPath.toString) == Set("removed.parquet"))
  }

  test("incremental scan filters staged commits from the retained log tail") {
    val dataFilter = new Predicate("=", new Column("id"), Literal.ofInt(5))
    val counting = new CountingEngine
    counting.putCommit(
      0,
      Seq(
        protocolAction(protocolWithCatalogManagedSupport),
        metadataAction(filterMetadata)))
    val staged1 = counting.putStagedCommit(
      1,
      Seq(
        filteredAddAction("included.parquet", "keep", 4, 6),
        filteredAddAction("excluded.parquet", "keep", 10, 20)))
    val staged2 =
      counting.putStagedCommit(2, Seq(statsLessFilteredAddAction("no-stats.parquet", "keep")))
    val logData = Seq(staged1, staged2).map(ParsedCatalogCommitData.forFileStatus)
    val target = coldSnapshot(counting, 2, logData, _.withMaxCatalogVersion(2))
    val scanEngine = counting.fork()
    val scan = target
      .getIncrementalScanBuilder(0)
      .withFilter(dataFilter)
      .build(scanEngine.engine)
      .get()

    assert(consumeLivePaths(scan).map(path => new Path(path).getName).toSet ==
      Set("included.parquet", "no-stats.parquet"))
    assert(scan.finish().getLiveAdds.asScala.map(_.getPath.toString) ==
      Set("excluded.parquet", "included.parquet", "no-stats.parquet"))
    assert(scanEngine.trace.jsonReadCalls == Seq(Seq(staged2.getPath, staged1.getPath)))
  }

  test("filtered incremental reconciliation matches a normal target scan") {
    val dataFilter = new Predicate("=", new Column("id"), Literal.ofInt(5))
    val counting = new CountingEngine
    counting.putCommit(
      0,
      Seq(
        protocolAction(),
        metadataAction(filterMetadata),
        filteredAddAction("base-stays.parquet", "keep", 4, 6),
        filteredAddAction("base-removed.parquet", "keep", 4, 6),
        filteredAddAction("base-pruned-readd.parquet", "keep", 4, 6),
        filteredAddAction("base-excluded.parquet", "keep", 10, 20)))
    counting.putCommit(
      1,
      Seq(
        filteredAddAction("base-stays.parquet", "keep", 4, 6),
        removeAction("base-removed.parquet"),
        removeAction("base-pruned-readd.parquet"),
        filteredAddAction("transient.parquet", "keep", 4, 6)))
    counting.putCommit(
      2,
      Seq(
        removeAction("transient.parquet"),
        filteredAddAction("base-pruned-readd.parquet", "keep", 10, 20),
        filteredAddAction("target-new.parquet", "keep", 4, 6),
        filteredAddAction("target-excluded.parquet", "keep", 10, 20),
        statsLessFilteredAddAction("target-no-stats.parquet", "keep")))
    val base = coldSnapshot(counting, 0)
    val target = coldSnapshot(counting, 2)
    val baseKeys = scanFileSemantics(base, counting.fork().engine, Some(dataFilter))
      .map { case (_, add) => keyOf(add) }
      .toSet
    val expectedTargetKeys = scanFileSemantics(target, counting.fork().engine, Some(dataFilter))
      .map { case (_, add) => keyOf(add) }
      .toSet
    val scan = target
      .getIncrementalScanBuilder(0)
      .withFilter(dataFilter)
      .build(counting.fork().engine)
      .get()

    val streamedAdds = consumeLivePaths(scan)
      .map(path =>
        new FileActionKey(new URI(new Path(path).getName), Optional.empty()))
      .toSet
    val summary = scan.finishAgainstBase(baseKeys.contains)
    val baseMask = summary.getRemoves.asScala ++ summary.getDuplicateAdds.asScala
    val reconciled = (baseKeys -- baseMask) ++ streamedAdds

    assert(reconciled == expectedTargetKeys)
    assert(summary.getDuplicateAdds.asScala.map(_.getPath.toString) ==
      Set("base-pruned-readd.parquet", "base-stays.parquet"))
    assert(summary.getRemoves.asScala.map(_.getPath.toString) ==
      Set("base-removed.parquet", "transient.parquet"))
  }

  test("incremental scan applies logical partition filters to column-mapped Adds") {
    val mappedMetadata = ColumnMapping.updateColumnMappingMetadataIfNeeded(
      testMetadata(
        filterSchema,
        partitionCols = Seq("p"),
        tblProps = Map("delta.columnMapping.mode" -> "name")),
      true).get()
    val physicalPartitionName =
      ColumnMapping.getPhysicalName(mappedMetadata.getSchema.get("p"))
    val counting = new CountingEngine
    counting.putCommit(
      0,
      Seq(
        protocolAction(new Protocol(2, 5)),
        metadataAction(mappedMetadata)))
    counting.putCommit(
      1,
      Seq(
        partitionedAddAction(
          mappedMetadata.getSchema,
          "included.parquet",
          physicalPartitionName,
          "keep",
          Optional.empty()),
        partitionedAddAction(
          mappedMetadata.getSchema,
          "excluded.parquet",
          physicalPartitionName,
          "drop",
          Optional.empty())))
    val target = coldSnapshot(counting, 1)
    val partitionFilter = new Predicate("=", new Column("p"), Literal.ofString("keep"))
    val scan = target
      .getIncrementalScanBuilder(0)
      .withFilter(partitionFilter)
      .build(counting.fork().engine)
      .get()

    assert(consumeLivePaths(scan) == Seq(s"$tablePath/included.parquet"))
    assert(scan.finish().getLiveAdds.asScala.map(_.getPath.toString) ==
      Set("excluded.parquet", "included.parquet"))
    assert(!scan.getRemainingFilter.isPresent)
  }

  test("incremental scan prunes Adds only and marks filtered newest keys seen") {
    val filter = new Predicate("=", new Column("id"), Literal.ofInt(5))
    val counting = new CountingEngine
    counting.putCommit(0, Seq(protocolAction(), metadataAction(filterMetadata)))
    counting.putCommit(
      1,
      Seq(
        filteredAddAction("duplicate.parquet", "keep", 4, 6),
        filteredAddAction("excluded.parquet", "keep", 10, 20)))
    counting.putCommit(
      2,
      Seq(
        filteredAddAction("duplicate.parquet", "keep", 10, 20),
        removeAction("removed.parquet")))
    val target = coldSnapshot(counting, 2)
    val scan = target
      .getIncrementalScanBuilder(0)
      .withFilter(filter)
      .build(counting.fork().engine)
      .get()

    assert(consumeLivePaths(scan).isEmpty)
    val summary = scan.finish()
    assert(summary.getLiveAdds.asScala.map(_.getPath.toString) ==
      Set("duplicate.parquet", "excluded.parquet"))
    assert(summary.getRemoves.asScala.map(_.getPath.toString) == Set("removed.parquet"))
  }

  test("incremental scan validates unknown filter columns before JSON reads") {
    val counting = new CountingEngine
    counting.putCommit(0, Seq(protocolAction(), metadataAction(filterMetadata)))
    counting.putCommit(1, Seq(filteredAddAction("a.parquet", "keep", 4, 6)))
    val target = coldSnapshot(counting, 1)
    val scanEngine = counting.fork()
    val unknownFilter = new Predicate("=", new Column("missing"), Literal.ofInt(5))

    intercept[IllegalArgumentException] {
      target.getIncrementalScanBuilder(0).withFilter(unknownFilter).build(scanEngine.engine)
    }
    assert(scanEngine.trace.isEmpty)
  }

  test("incremental scan closes JSON actions when eager evaluator construction fails") {
    val filter = new Predicate("=", new Column("id"), Literal.ofInt(5))
    val counting = new CountingEngine
    counting.putCommit(0, Seq(protocolAction(), metadataAction(filterMetadata)))
    counting.putCommit(1, Seq(filteredAddAction("a.parquet", "keep", 4, 6)))
    val target = coldSnapshot(counting, 1)
    val scanEngine = counting.fork()
    val evaluatorFailure = new IllegalStateException("injected evaluator construction failure")
    val closeFailure = new java.io.IOException("injected JSON close failure")
    scanEngine.failPredicateConstruction(evaluatorFailure)
    scanEngine.failJsonClose(closeFailure)

    val thrown = intercept[RuntimeException] {
      target.getIncrementalScanBuilder(0).withFilter(filter).build(scanEngine.engine)
    }

    assert(thrown.getCause eq evaluatorFailure)
    assert(thrown.getSuppressed.toSeq == Seq(closeFailure))
    assert(scanEngine.isJsonDelegateClosed)
  }

  test("incremental scan rejects repeated and post-terminal lifecycle calls") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, Seq(addAction("a.parquet")))
    val target = coldSnapshot(counting, 1)
    def newScan() = target.getIncrementalScanBuilder(0).build(counting.fork().engine).get()

    val borrowed = newScan()
    borrowed.getLiveAddBatches
    intercept[IllegalStateException] {
      borrowed.getLiveAddBatches
    }
    borrowed.close()

    val finished = newScan()
    finished.finish()
    intercept[IllegalStateException] {
      finished.finish()
    }
    intercept[IllegalStateException] {
      finished.getLiveAddBatches
    }
    finished.close()

    val closed = newScan()
    closed.close()
    intercept[IllegalStateException] {
      closed.getLiveAddBatches
    }
    intercept[IllegalStateException] {
      closed.finish()
    }
  }

  test("incremental scan poisons failures and closes its JSON iterator") {
    val dataFilter = new Predicate("=", new Column("id"), Literal.ofInt(5))
    val counting = new CountingEngine
    counting.putCommit(0, Seq(protocolAction(), metadataAction(filterMetadata)))
    counting.putCommit(1, Seq(filteredAddAction("a.parquet", "keep", 4, 6)))
    val target = coldSnapshot(counting, 1)

    Seq("hasNext", "next", "parse", "filter").foreach { failurePoint =>
      val scanEngine = counting.fork()
      val injected = new IllegalStateException(s"injected $failurePoint failure")
      failurePoint match {
        case "hasNext" => scanEngine.failJsonHasNext(injected)
        case "next" => scanEngine.failJsonNext(injected)
        case "parse" => scanEngine.failJsonParse(injected)
        case "filter" => scanEngine.failPredicateEvaluation(injected)
      }
      val scan = target
        .getIncrementalScanBuilder(0)
        .withFilter(dataFilter)
        .build(scanEngine.engine)
        .get()
      val batches = scan.getLiveAddBatches

      withClue(s"failurePoint=$failurePoint: ") {
        assert(intercept[RuntimeException] {
          if (failurePoint == "parse" || failurePoint == "filter") {
            assert(batches.hasNext)
            batches.next()
          } else {
            batches.hasNext
          }
        }.getMessage.contains(failurePoint))
      }
      assert(scanEngine.isJsonDelegateClosed)
      val readAfterFailure = intercept[IllegalStateException] {
        batches.hasNext
      }
      assert(readAfterFailure.getMessage.contains("previously failed"))
      val finishAfterFailure = intercept[IllegalStateException] {
        scan.finish()
      }
      assert(finishAfterFailure.getMessage.contains("previously failed"))
    }
  }

  test("Rust case A: same explicit target returns the identical snapshot without I/O") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCrc(0, crcAt(0, "v0"))
    val base = coldSnapshot(counting, 0)
    counting.clearIo()

    val result = from(counting, base, version = Some(0))
    val trace = counting.trace

    assert(
      trace.isEmpty && (result eq base),
      s"expected same reference and zero I/O, got trace=$trace, sameRef=${result eq base}")
  }

  test("Rust case A: same target ignores a checkpoint discovered after the base") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, Seq(addAction("v1.parquet")))
    val base = coldSnapshot(counting, 1)
    val baseSegment = base.getLogSegment
    assert(!baseSegment.getCheckpointVersionOpt.isPresent)

    counting.putCheckpoint(1, checkpointActions("v0", Seq(1L)))
    counting.clearIo()

    val result = from(counting, base, version = Some(1))
    val trace = counting.trace

    assert(result eq base)
    assert(result.getLogSegment eq baseSegment)
    assert(!result.getLogSegment.getCheckpointVersionOpt.isPresent)
    assert(trace.isEmpty, s"same-target reuse must perform zero I/O after checkpoint write: $trace")
  }

  test("Rust case B: backward target is rejected before any I/O") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, Seq(addAction("v1.parquet")))
    val base = coldSnapshot(counting, 1)
    counting.clearIo()

    val error = intercept[IllegalArgumentException] {
      from(counting, base, version = Some(0))
    }

    assert(error.getMessage.contains("before base snapshot version 1"))
    assert(counting.trace.isEmpty)
  }

  test("Rust case A: maxCatalogVersion at the base is an effective same target") {
    val counting = new CountingEngine
    counting.putCommit(
      0,
      Seq(
        protocolAction(protocolWithCatalogManagedSupport),
        metadataAction(metadata("v0")),
        addAction("base.parquet")))
    val stagedV1 = counting.putStagedCommit(1, Seq(addAction("staged-v1.parquet")))
    val catalogData = Seq(ParsedCatalogCommitData.forFileStatus(stagedV1))
    val base =
      coldLatestSnapshot(counting, catalogData, _.withMaxCatalogVersion(1))
    counting.clearIo()

    val result = from(
      counting,
      base,
      logData = catalogData,
      configure = _.withMaxCatalogVersion(1))

    assert(result eq base)
    assert(
      counting.trace.isEmpty,
      s"effective same target must perform zero I/O: ${counting.trace}")
  }

  test("Rust case A: same-target catalog refresh preserves time-travel build intent") {
    val counting = new CountingEngine
    counting.putCommit(
      0,
      Seq(
        protocolAction(protocolWithCatalogManagedSupport),
        metadataAction(metadata("v0")),
        addAction("base.parquet")))
    val stagedV1 = counting.putStagedCommit(1, Seq(addAction("staged-v1.parquet")))
    val catalogData = Seq(ParsedCatalogCommitData.forFileStatus(stagedV1))
    val base =
      coldSnapshot(counting, 1, catalogData, _.withMaxCatalogVersion(2))
    assert(!base.wasBuiltAsLatest)
    counting.clearIo()

    val result = from(
      counting,
      base,
      logData = catalogData,
      configure = _.withMaxCatalogVersion(base.getVersion))

    assert(result eq base)
    assert(!result.wasBuiltAsLatest)
    assert(
      counting.trace.isEmpty,
      s"same-target catalog refresh must perform zero I/O: ${counting.trace}")
  }

  test("Rust case B: maxCatalogVersion below the base is rejected before I/O") {
    val counting = new CountingEngine
    counting.putCommit(
      0,
      Seq(
        protocolAction(protocolWithCatalogManagedSupport),
        metadataAction(metadata("v0")),
        addAction("base.parquet")))
    val stagedV1 = counting.putStagedCommit(1, Seq(addAction("staged-v1.parquet")))
    val catalogData = Seq(ParsedCatalogCommitData.forFileStatus(stagedV1))
    val base =
      coldLatestSnapshot(counting, catalogData, _.withMaxCatalogVersion(1))
    counting.clearIo()

    val error = intercept[IllegalArgumentException] {
      from(counting, base, configure = _.withMaxCatalogVersion(0))
    }

    assert(
      error.getMessage.contains("0") &&
        error.getMessage.contains("1") &&
        error.getMessage.toLowerCase.contains("before"),
      s"diagnostic must identify effective target 0 and base version 1: ${error.getMessage}")
    assert(counting.trace.isEmpty)
  }

  test("Rust case C.1: maxCatalogVersion above reachable source data is rejected") {
    val counting = new CountingEngine
    counting.putCommit(
      0,
      Seq(
        protocolAction(protocolWithCatalogManagedSupport),
        metadataAction(metadata("v0")),
        addAction("base.parquet")))
    val stagedV1 = counting.putStagedCommit(1, Seq(addAction("staged-v1.parquet")))
    val catalogData = Seq(ParsedCatalogCommitData.forFileStatus(stagedV1))
    val base =
      coldLatestSnapshot(counting, catalogData, _.withMaxCatalogVersion(1))
    counting.clearIo()

    val error = intercept[RuntimeException] {
      from(counting, base, configure = _.withMaxCatalogVersion(3))
    }
    val trace = counting.trace

    assert(
      error.getMessage.contains("3") &&
        error.getMessage.contains("1") &&
        error.getMessage.toLowerCase.contains("available"),
      s"diagnostic must identify effective target 3 and base version 1: ${error.getMessage}")
    assertSingleListFrom(trace, 1)
    assert(trace.fileStatusProbes.isEmpty)
    assert(trace.contentReads.isEmpty)
  }

  test("Rust case C.1: unavailable explicit target over a truly empty incremental listing") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, Seq(addAction("v1.parquet")))
    counting.putCheckpoint(1, checkpointActions("v0", Seq(1L)))
    val base = coldSnapshot(counting, 1)
    counting.clearIo()

    val error = intercept[RuntimeException] {
      from(counting, base, version = Some(3))
    }
    val trace = counting.trace

    // Error checks validate semantic requested/base/missing-version payload, not copied Rust prose.
    assert(
      error.getMessage.contains("3") &&
        error.getMessage.contains("1") &&
        error.getMessage.toLowerCase.contains("available"),
      s"diagnostic must identify requested version 3 and base version 1: ${error.getMessage}")
    assertSingleListFrom(trace, 2)
    assert(trace.listResults == Seq(Seq.empty))
    assert(trace.fileStatusProbes.isEmpty)
    assert(trace.contentReads.isEmpty)
  }

  test("Rust case C.2: empty latest listing promotes a time-travel base without mutation") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, Seq(addAction("v1.parquet")))
    counting.putCheckpoint(1, checkpointActions("v0", Seq(1L)))
    val base = coldSnapshot(counting, 1)
    assert(!base.wasBuiltAsLatest)
    counting.clearIo()

    val result = from(counting, base)
    val trace = counting.trace

    assertMatchesCold(result, counting, 1)
    assert(result.getVersion == base.getVersion)
    assert(!(result eq base), "promotion must not mutate the time-travel base snapshot")
    assert(result.wasBuiltAsLatest)
    assert(!base.wasBuiltAsLatest)
    assertSingleListFrom(trace, 2)
    assert(trace.listResults == Seq(Seq.empty))
    assert(trace.fileStatusProbes.isEmpty)
    assert(trace.contentReads.isEmpty, s"case C.2 must not open log contents: $trace")
  }

  test("Rust case C.1: a CRC-only listing does not make an explicit target reachable") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, Seq(addAction("v1.parquet")))
    counting.putCheckpoint(1, checkpointActions("v0", Seq(1L)))
    val base = coldSnapshot(counting, 1)
    val crc3 = counting.putCrc(3, crcAt(3, "unreachable"))
    counting.clearIo()

    val error = intercept[RuntimeException] {
      from(counting, base, version = Some(3))
    }
    val trace = counting.trace

    assert(
      error.getMessage.contains("3") &&
        error.getMessage.contains("1") &&
        error.getMessage.toLowerCase.contains("available"),
      s"CRC-only case C.1 must identify requested 3 and base 1: ${error.getMessage}")
    assertSingleListFrom(trace, 2)
    assert(trace.listResults == Seq(Seq(crc3.getPath)))
    assert(trace.fileStatusProbes.isEmpty)
    assert(trace.contentReads.isEmpty, s"case C.1 must not open the unrelated CRC: $trace")
  }

  test("Rust case C.2: a CRC-only listing promotes without opening the CRC") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, Seq(addAction("v1.parquet")))
    counting.putCheckpoint(1, checkpointActions("v0", Seq(1L)))
    val base = coldSnapshot(counting, 1)
    val crc2 = counting.putCrc(2, crcAt(2, "unreachable"))
    counting.clearIo()

    val result = from(counting, base)
    val trace = counting.trace

    assertMatchesCold(result, counting, 1)
    assert(!(result eq base))
    assert(result.wasBuiltAsLatest)
    assert(!base.wasBuiltAsLatest)
    assertSingleListFrom(trace, 2)
    assert(trace.listResults == Seq(Seq(crc2.getPath)))
    assert(trace.fileStatusProbes.isEmpty)
    assert(trace.contentReads.isEmpty, s"case C.2 must not open the unrelated CRC: $trace")
  }

  test("Rust case E: nonempty unchanged listing reuses an already-latest base") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, Seq(addAction("v1.parquet")))
    val base = coldLatestSnapshot(counting)
    assert(base.wasBuiltAsLatest)
    counting.clearIo()

    val result = from(counting, base)
    val trace = counting.trace

    assert(result eq base)
    assert(result.wasBuiltAsLatest)
    assertSingleListFrom(trace, 1)
    assert(trace.listResults.size == 1 && trace.listResults.head.nonEmpty)
    assert(trace.fileStatusProbes.isEmpty)
    assert(trace.contentReads.isEmpty, s"case E must not open unchanged log contents: $trace")
  }

  test("Rust case E: nonempty unchanged listing promotes a time-travel base") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, Seq(addAction("v1.parquet")))
    val base = coldSnapshot(counting, 1)
    assert(!base.wasBuiltAsLatest)
    counting.clearIo()

    val result = from(counting, base)
    val trace = counting.trace

    assertMatchesCold(result, counting, 1)
    assert(!(result eq base))
    assert(result.wasBuiltAsLatest)
    assert(!base.wasBuiltAsLatest)
    assertSingleListFrom(trace, 1)
    assert(trace.listResults.size == 1 && trace.listResults.head.nonEmpty)
    assert(trace.fileStatusProbes.isEmpty)
    assert(trace.contentReads.isEmpty, s"case E promotion must not open log contents: $trace")
  }

  test("Rust case D.1: a checkpoint ahead of the base rebuilds from that checkpoint") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCheckpoint(0, checkpointActions("v0", Seq.empty))
    counting.putCommit(1, Seq(addAction("v1.parquet")))
    val base = coldSnapshot(counting, 1)

    counting.putCommit(
      2,
      Seq(metadataAction(metadata("v2")), addAction("v2.parquet")))
    counting.putCommit(3, Seq(addAction("v3.parquet")))
    val checkpoint3 = counting.putCheckpoint(3, checkpointActions("v2", 1L to 3L))
    val commit4 = counting.putCommit(4, Seq(addAction("v4.parquet")))
    counting.clearIo()

    val result = from(counting, base, version = Some(4))
    val refreshTrace = counting.trace

    assertMatchesCold(result, counting, 4)
    assert(result.getLogSegment.getCheckpointVersionOpt == Optional.of(3L))
    assertSingleListFrom(refreshTrace, 1)
    assert(refreshTrace.checkpointReads == Seq(checkpoint3.getPath))
    assert(refreshTrace.commitJsonReads == Seq(commit4.getPath))
    assert(
      refreshTrace.commitJsonReads.forall(FileNames.deltaVersion(_) > 3),
      s"checkpoint@3 subsumes all older commit JSON: $refreshTrace")
    assert(refreshTrace.crcReads.isEmpty)
  }

  test("Rust case D.2: checkpoint behind the base advances lineage without regressing P&M") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCheckpoint(0, checkpointActions("v0", Seq.empty))
    counting.putCommit(1, Seq(addAction("v1.parquet")))
    counting.putCommit(
      2,
      Seq(metadataAction(metadata("v2")), addAction("v2.parquet")))
    counting.putCommit(3, Seq(addAction("v3.parquet")))
    val base = coldSnapshot(counting, 3)

    counting.putCheckpoint(1, checkpointActions("v0", Seq(1L)))
    counting.putCommit(4, Seq(addAction("v4.parquet")))
    counting.clearIo()

    val result = from(counting, base, version = Some(4))
    val refreshTrace = counting.trace

    assertMatchesCold(result, counting, 4)
    assert(result.getLogSegment.getCheckpointVersionOpt == Optional.of(1L))
    assert(result.getTableProperties.get("fixture.state") == "v2")
    assert(
      refreshTrace.commitJsonReads.map(FileNames.deltaVersion) == Seq(4L),
      s"only the new tail may be opened for P&M replay: $refreshTrace")
    assertSingleListFrom(refreshTrace, 1)
    assert(refreshTrace.checkpointReads.isEmpty)
  }

  test("Rust case D.2: checkpoint at S1 advances through only the new commit tail") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCheckpoint(0, checkpointActions("v0", Seq.empty))
    counting.putCommit(1, Seq(addAction("v1.parquet")))
    counting.putCommit(2, Seq(addAction("v2.parquet")))
    val base = coldSnapshot(counting, 2)

    counting.putCheckpoint(2, checkpointActions("v0", 1L to 2L))
    val commit3 = counting.putCommit(3, Seq(addAction("v3.parquet")))
    val commit4 = counting.putCommit(4, Seq(addAction("v4.parquet")))
    counting.clearIo()

    val result = from(counting, base, version = Some(4))
    val refreshTrace = counting.trace

    assertMatchesCold(result, counting, 4)
    assert(result.getLogSegment.getCheckpointVersionOpt == Optional.of(2L))
    assert(refreshTrace.commitJsonReads.sorted == Seq(commit3.getPath, commit4.getPath))
    assert(
      refreshTrace.checkpointReads.isEmpty,
      s"checkpoint@S1 cannot change P&M already resolved by the base: $refreshTrace")
    assert(refreshTrace.crcReads.isEmpty)
  }

  test("Rust case D.2/E: checkpoint at the base improves lineage with zero content reads") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, Seq(addAction("v1.parquet")))
    counting.putCommit(
      2,
      Seq(metadataAction(metadata("v2")), addAction("v2.parquet")))
    counting.putCommit(3, Seq(addAction("v3.parquet")))
    val base = coldSnapshot(counting, 3)

    counting.putCheckpoint(3, checkpointActions("v2", 1L to 3L))
    counting.clearIo()

    val result = from(counting, base)
    val refreshTrace = counting.trace

    assertMatchesCold(result, counting, 3)
    assert(result.getLogSegment.getCheckpointVersionOpt == Optional.of(3L))
    assert(result.getTableProperties.get("fixture.state") == "v2")
    assert(
      refreshTrace.contentReads.isEmpty,
      s"base P&M already subsumes an equal-version checkpoint: $refreshTrace")
    assertSingleListFrom(refreshTrace, 1)
    assert(refreshTrace.listResults.size == 1 && refreshTrace.listResults.head.nonEmpty)
    assert(refreshTrace.fileStatusProbes.isEmpty)
  }

  test("Rust resolve_crc_file: a newly listed CRC supersedes the base segment CRC") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCheckpoint(0, checkpointActions("v0", Seq.empty))
    (1L to 3L).foreach(version =>
      counting.putCommit(version, Seq(addAction(s"v$version.parquet"))))
    counting.putCrc(2, crcAt(2, "base-crc"))
    val base = coldSnapshot(counting, 3)

    counting.putCheckpoint(1, checkpointActions("v0", Seq(1L)))
    val newlyListedCrc = counting.putCrc(3, crcAt(3, "new-crc"))
    counting.clearIo()

    val result = from(counting, base)
    val trace = counting.trace

    assert(result.getLogSegment.getCheckpointVersionOpt == Optional.of(1L))
    assert(result.getLogSegment.getLastSeenChecksum == Optional.of(newlyListedCrc))
    assert(result.getLogSegment.getLastSeenChecksum.get.getPath == newlyListedCrc.getPath)
    assert(trace.fileStatusProbes.isEmpty)
    assert(trace.contentReads.isEmpty, s"CRC file selection must remain structural: $trace")
  }

  test("Rust resolve_crc_file: retain a base CRC at or above the selected checkpoint") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCheckpoint(0, checkpointActions("v0", Seq.empty))
    (1L to 3L).foreach(version =>
      counting.putCommit(version, Seq(addAction(s"v$version.parquet"))))
    val baseCrc = counting.putCrc(2, crcAt(2, "base-crc"))
    val base = coldSnapshot(counting, 3)

    counting.removeVisible(baseCrc.getPath)
    counting.putCheckpoint(1, checkpointActions("v0", Seq(1L)))
    counting.clearIo()

    val result = from(counting, base)
    val trace = counting.trace

    assert(result.getLogSegment.getCheckpointVersionOpt == Optional.of(1L))
    assert(result.getLogSegment.getLastSeenChecksum == Optional.of(baseCrc))
    assert(result.getLogSegment.getLastSeenChecksum.get.getPath == baseCrc.getPath)
    assert(trace.fileStatusProbes.isEmpty)
    assert(trace.contentReads.isEmpty, s"retaining a valid base CRC must not reopen it: $trace")
  }

  test("Rust resolve_crc_file: drop a base CRC below the selected checkpoint") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCheckpoint(0, checkpointActions("v0", Seq.empty))
    (1L to 3L).foreach(version =>
      counting.putCommit(version, Seq(addAction(s"v$version.parquet"))))
    val baseCrc = counting.putCrc(1, crcAt(1, "base-crc"))
    val base = coldSnapshot(counting, 3)

    counting.removeVisible(baseCrc.getPath)
    counting.putCheckpoint(2, checkpointActions("v0", 1L to 2L))
    counting.clearIo()

    val result = from(counting, base)
    val trace = counting.trace

    assert(result.getLogSegment.getCheckpointVersionOpt == Optional.of(2L))
    assert(!result.getLogSegment.getLastSeenChecksum.isPresent)
    assert(trace.fileStatusProbes.isEmpty)
    assert(
      trace.contentReads.isEmpty,
      s"a below-checkpoint base CRC must be dropped unopened: $trace")
  }

  test("plan step 7: an eligible base compaction is retained exactly once") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCheckpoint(0, checkpointActions("v0", Seq.empty))
    counting.putCommit(1, Seq(addAction("v1.parquet")))
    counting.putCommit(2, Seq(addAction("v2.parquet")))
    val baseCompaction =
      counting.putCompaction(1, 2, Seq(addAction("v1.parquet"), addAction("v2.parquet")))
    val base = coldSnapshot(counting, 2)

    val commit3 = counting.putCommit(3, Seq(addAction("v3.parquet")))
    counting.clearIo()

    val result = from(counting, base, version = Some(3))
    val refreshTrace = counting.trace
    val compactions = result.getLogSegment.getCompactions.asScala.toSeq

    assertMatchesCold(result, counting, 3)
    assert(compactions == Seq(baseCompaction))
    assert(compactions.count(_.getPath == baseCompaction.getPath) == 1)
    assert(
      refreshTrace.jsonReads == Seq(commit3.getPath),
      s"refresh must open only the new commit tail, not retained files: $refreshTrace")
    assert(refreshTrace.checkpointReads.isEmpty)
    assert(refreshTrace.fileStatusProbes.isEmpty)
  }

  test("plan step 7: a base compaction covered by a newer checkpoint is trimmed") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCheckpoint(0, checkpointActions("v0", Seq.empty))
    counting.putCommit(1, Seq(addAction("v1.parquet")))
    counting.putCommit(2, Seq(addAction("v2.parquet")))
    counting.putCommit(3, Seq(addAction("v3.parquet")))
    counting.putCompaction(1, 2, Seq(addAction("v1.parquet"), addAction("v2.parquet")))
    val base = coldSnapshot(counting, 3)

    counting.putCheckpoint(2, checkpointActions("v0", 1L to 2L))
    counting.clearIo()

    val result = from(counting, base)
    val refreshTrace = counting.trace

    assertMatchesCold(result, counting, 3)
    assert(result.getLogSegment.getCheckpointVersionOpt == Optional.of(2L))
    assert(result.getLogSegment.getCompactions.isEmpty)
    assert(
      result.getLogSegment.getDeltas.asScala.map(file => FileNames.deltaVersion(file.getPath)) ==
        Seq(3L))
    assert(
      refreshTrace.contentReads.isEmpty,
      s"checkpoint-only lineage improvement must not reopen log contents: $refreshTrace")
    assert(refreshTrace.fileStatusProbes.isEmpty)
  }

  test("plan step 6: a newly listed boundary-spanning compaction is dropped") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCheckpoint(0, checkpointActions("v0", Seq.empty))
    counting.putCommit(1, Seq(addAction("v1.parquet")))
    counting.putCommit(2, Seq(addAction("v2.parquet")))
    val base = coldSnapshot(counting, 2)

    val commit3 = counting.putCommit(3, Seq(addAction("v3.parquet")))
    counting.putCompaction(
      1,
      3,
      Seq(addAction("v1.parquet"), addAction("v2.parquet"), addAction("v3.parquet")))
    counting.clearIo()

    val result = from(counting, base, version = Some(3))
    val refreshTrace = counting.trace

    assertBehaviorMatchesCold(result, counting, 3)
    assert(result.getLogSegment.getCompactions.isEmpty)
    assert(
      result.getLogSegment.getDeltas.asScala.map(file => FileNames.deltaVersion(file.getPath)) ==
        (1L to 3L))
    assert(
      refreshTrace.jsonReads == Seq(commit3.getPath),
      s"refresh must open only the new commit tail, not the spanning compaction: $refreshTrace")
    assert(refreshTrace.checkpointReads.isEmpty)
    assert(refreshTrace.fileStatusProbes.isEmpty)
  }

  test("plan step 6: a newly listed compaction entirely above the base is retained") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCheckpoint(0, checkpointActions("v0", Seq.empty))
    counting.putCommit(1, Seq(addAction("v1.parquet")))
    counting.putCommit(2, Seq(addAction("v2.parquet")))
    val base = coldSnapshot(counting, 2)

    counting.putCommit(3, Seq(addAction("v3.parquet")))
    counting.putCommit(
      4,
      Seq(metadataAction(metadata("v4")), addAction("v4.parquet")))
    val newCompaction = counting.putCompaction(
      3,
      4,
      Seq(
        addAction("v3.parquet"),
        metadataAction(metadata("v4")),
        addAction("v4.parquet")))
    counting.clearIo()

    val result = from(counting, base, version = Some(4))
    val refreshTrace = counting.trace
    val compactions = result.getLogSegment.getCompactions.asScala.toSeq

    assertMatchesCold(result, counting, 4)
    assert(compactions == Seq(newCompaction))
    assert(compactions.count(_.getPath == newCompaction.getPath) == 1)
    assert(
      refreshTrace.jsonReads == Seq(newCompaction.getPath),
      s"refresh must replay the eligible tail compaction without reopening raw commits: " +
        refreshTrace)
    assert(refreshTrace.checkpointReads.isEmpty)
    assert(refreshTrace.fileStatusProbes.isEmpty)
  }

  test("Rust case F: a checkpoint-free tail retains the base segment and replays only the tail") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(
      1,
      Seq(metadataAction(metadata("v1")), addAction("v1.parquet")))
    counting.putCommit(2, Seq(addAction("v2.parquet")))
    val base = coldSnapshot(counting, 2)

    counting.putCommit(3, Seq(addAction("v3.parquet")))
    counting.putCommit(4, Seq(addAction("v4.parquet")))
    counting.clearIo()

    val result = from(counting, base, version = Some(4))
    val refreshTrace = counting.trace

    assertMatchesCold(result, counting, 4)
    assert(!result.getLogSegment.getCheckpointVersionOpt.isPresent)
    assert(
      result.getLogSegment.getDeltas.asScala.map(f => FileNames.deltaVersion(f.getPath)) ==
        (0L to 4L))
    assert(
      refreshTrace.commitJsonReads.map(FileNames.deltaVersion).sorted == Seq(3L, 4L),
      s"old commits must remain structural only, not be reopened: $refreshTrace")
    assertSingleListFrom(refreshTrace, 1)
    assert(refreshTrace.checkpointReads.isEmpty)
  }

  Seq(
    ("protocol and metadata", true, true),
    ("protocol only", true, false),
    ("metadata only", false, true),
    ("neither protocol nor metadata", false, false)).foreach {
    case (label, changeProtocol, changeMetadata) =>
      test(s"Rust case F tail P&M replay: $label") {
        val counting = new CountingEngine
        counting.putCommit(0, initialActions("base"))
        counting.putCommit(1, Seq(addAction("v1.parquet")))
        val base = coldSnapshot(counting, 1)

        val tailProtocol = new Protocol(2, 5)
        val tailMetadata = metadata("tail")
        val tailActions =
          Seq(
            Option.when(changeProtocol)(protocolAction(tailProtocol)),
            Option.when(changeMetadata)(metadataAction(tailMetadata))).flatten ++
            Seq(addAction("v2.parquet"))
        val commit2 = counting.putCommit(2, tailActions)
        counting.clearIo()

        val result = from(counting, base, version = Some(2))
        val refreshTrace = counting.trace

        assertMatchesCold(result, counting, 2)
        assert(result.getProtocol == (if (changeProtocol) tailProtocol else protocol))
        assert(
          result.getTableProperties.get("fixture.state") ==
            (if (changeMetadata) "tail" else "base"))
        assertSingleListFrom(refreshTrace, 1)
        assert(
          refreshTrace.commitJsonReads == Seq(commit2.getPath),
          s"tail replay must open exactly (S1=1, S2=2], not reuse base blindly: $refreshTrace")
        assert(refreshTrace.checkpointReads.isEmpty)
        assert(refreshTrace.crcReads.isEmpty)
      }
  }

  test("provided protocol and metadata skip incremental tail P&M replay") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("base"))
    counting.putCommit(1, Seq(addAction("v1.parquet")))
    val base = coldSnapshot(counting, 1)

    val tailProtocol = new Protocol(2, 5)
    counting.putCommit(
      2,
      Seq(
        protocolAction(tailProtocol),
        metadataAction(metadata("tail")),
        addAction("v2.parquet")))
    val providedMetadata = metadata("provided")
    counting.clearIo()

    val result = from(
      counting,
      base,
      version = Some(2),
      configure = _.withProtocolAndMetadata(protocol, providedMetadata))
    val refreshTrace = counting.trace

    assert(result.getProtocol == protocol)
    assert(result.getMetadata == providedMetadata)
    assertSingleListFrom(refreshTrace, 1)
    assert(
      refreshTrace.contentReads.isEmpty,
      s"provided P&M must bypass incremental tail content reads: $refreshTrace")
  }

  test("Rust corruption guard: a reconstructable listing below the base is rejected") {
    val counting = new CountingEngine
    val commits = (0L to 5L).map { version =>
      if (version == 0) {
        counting.putCommit(version, initialActions("v0"))
      } else {
        counting.putCommit(version, Seq(addAction(s"v$version.parquet")))
      }
    }
    val base = coldSnapshot(counting, 5)

    counting.removeVisible(commits(4).getPath)
    counting.removeVisible(commits(5).getPath)
    counting.clearIo()

    val error = intercept[RuntimeException] {
      from(counting, base)
    }
    val trace = counting.trace
    assert(
      error.getMessage.contains("3") &&
        error.getMessage.contains("5") &&
        error.getMessage.toLowerCase.contains("older"),
      s"diagnostic must identify newest listed version 3 and base version 5: ${error.getMessage}")
    assertSingleListFrom(trace, 1)
    assert(trace.contentReads.isEmpty)
  }

  test("Rust corruption guard: a gap in the new tail is rejected") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, Seq(addAction("v1.parquet")))
    val base = coldSnapshot(counting, 1)

    counting.putCommit(3, Seq(addAction("v3.parquet")))
    counting.clearIo()

    val error = intercept[RuntimeException] {
      from(counting, base, version = Some(3))
    }
    val trace = counting.trace
    assert(
      error.getMessage.contains("1") &&
        error.getMessage.contains("2") &&
        error.getMessage.contains("3") &&
        (error.getMessage.toLowerCase.contains("missing") ||
          error.getMessage.toLowerCase.contains("contiguous")),
      s"diagnostic must identify base 1, missing 2, and requested 3: ${error.getMessage}")
    assertSingleListFrom(trace, 1)
    assert(trace.contentReads.isEmpty)
  }

  test("catalog log data wins over a published commit at the same version") {
    val counting = new CountingEngine
    counting.putCommit(
      0,
      Seq(
        protocolAction(protocolWithCatalogManagedSupport),
        metadataAction(metadata("v0")),
        addAction("base.parquet")))
    counting.putCommit(1, Seq(addAction("v1.parquet")))
    val base = coldSnapshot(
      counting,
      1,
      configure = _.withMaxCatalogVersion(1))

    val publishedV2 = counting.putCommit(2, Seq(addAction("published-v2.parquet")))
    val stagedV2 = counting.putStagedCommit(2, Seq(addAction("staged-v2.parquet")))
    val catalogData = Seq(ParsedCatalogCommitData.forFileStatus(stagedV2))
    counting.clearIo()

    val result = from(
      counting,
      base,
      version = Some(2),
      logData = catalogData,
      configure = _.withMaxCatalogVersion(2))
    val refreshTrace = counting.trace

    assertMatchesCold(
      result,
      counting,
      2,
      logData = catalogData,
      configure = _.withMaxCatalogVersion(2))
    assert(
      result.getLogSegment.getDeltas.asScala
        .find(file => FileNames.deltaVersion(file.getPath) == 2)
        .exists(_.getPath == stagedV2.getPath))
    assertSingleListFrom(refreshTrace, 1)
    assert(refreshTrace.commitJsonReads.contains(stagedV2.getPath))
    assert(!refreshTrace.commitJsonReads.contains(publishedV2.getPath))
    assert(scanPaths(result, counting.engine).exists(_.endsWith("staged-v2.parquet")))
    assert(!scanPaths(result, counting.engine).exists(_.endsWith("published-v2.parquet")))
  }

  test("catalog tail preserves the exact staged end status and published watermark") {
    val counting = new CountingEngine
    counting.putCommit(
      0,
      Seq(
        protocolAction(protocolWithCatalogManagedSupport),
        metadataAction(metadata("v0")),
        addAction("base.parquet")))
    counting.putCommit(1, Seq(addAction("published-v1.parquet")))
    val base = coldLatestSnapshot(counting, configure = _.withMaxCatalogVersion(1))

    val stagedV2 = counting.putStagedCommit(2, Seq(addAction("staged-v2.parquet")))
    val catalogData = Seq(ParsedCatalogCommitData.forFileStatus(stagedV2))
    counting.clearIo()

    val result = from(
      counting,
      base,
      logData = catalogData,
      configure = _.withMaxCatalogVersion(2))
    val refreshTrace = counting.trace
    val coldEngine = counting.fork()
    val expected =
      coldLatestSnapshot(coldEngine, catalogData, _.withMaxCatalogVersion(2))

    assertEquivalent(result, counting.engine, expected, coldEngine.engine)
    assert(result.getLogSegment.getDeltaFileAtEndVersion == stagedV2)
    assert(result.getLogSegment.getDeltaFileAtEndVersion.getPath == stagedV2.getPath)
    assert(result.getLogSegment.getMaxPublishedDeltaVersion == Optional.of(1L))
    assert(refreshTrace.commitJsonReads == Seq(stagedV2.getPath))
    assertSingleListFrom(refreshTrace, 1)
  }

  test("distinct preloaded-source path wins over available storage and catalog conflicts") {
    val counting = new CountingEngine
    val commit0 = counting.putCommit(0, initialActions("v0"))
    val base = coldSnapshot(counting, 0)
    val preloadedV1 = counting.putCommit(
      1,
      Seq(metadataAction(metadata("preloaded-wins")), addAction("preloaded-v1.parquet")))
    val storageCheckpointV1 = counting.putCheckpoint(
      1,
      Seq(
        protocolAction(),
        metadataAction(metadata("storage-loses")),
        addAction("base.parquet"),
        addAction("storage-v1.parquet")))
    val catalogV1 = counting.putStagedCommit(
      1,
      Seq(metadataAction(metadata("catalog-loses")), addAction("catalog-v1.parquet")))
    val catalogData: Seq[ParsedLogData] =
      Seq(ParsedCatalogCommitData.forFileStatus(catalogV1))
    val preloaded = Seq(commit0, preloadedV1).map(ParsedLogData.forFileStatus)
    counting.clearIo()

    // SnapshotFactory Path A is the explicit preloaded-segment path; it bypasses storage listing.
    val builder = TableManager
      .builderFrom(tablePath.toString, base)
      .atVersion(1)
      .withLogData(catalogData.asJava)
      .asInstanceOf[SnapshotBuilderImpl]
      .withPreloadedLogSegment(preloaded.asJava)
    val result = builder.build(counting.engine)
    val refreshTrace = counting.trace

    assert(result.getVersion == 1)
    assert(result.getTableProperties.get("fixture.state") == "preloaded-wins")
    assert(refreshTrace.listStarts.isEmpty)
    assert(refreshTrace.fileStatusProbes.isEmpty)
    assert(refreshTrace.rawFileReads.isEmpty)
    assertNoLastCheckpoint(refreshTrace)
    assert(refreshTrace.commitJsonReads.toSet == Set(commit0.getPath, preloadedV1.getPath))
    assert(!refreshTrace.commitJsonReads.contains(catalogV1.getPath))
    assert(!refreshTrace.checkpointReads.contains(storageCheckpointV1.getPath))
    assert(scanPaths(result, counting.engine).exists(_.endsWith("preloaded-v1.parquet")))
    assert(!scanPaths(result, counting.engine).exists(_.endsWith("storage-v1.parquet")))
    assert(!scanPaths(result, counting.engine).exists(_.endsWith("catalog-v1.parquet")))
  }

  test("C1=100 S1=109 S2=112 lists overlap but opens only commits 110 through 112") {
    // Anti-cheat fixture:
    // - incremental LIST starts at C1 + 1 = 101 and observes commit inventory 101..112;
    // - the combined segment retains one status for every commit 101..112;
    // - P&M replay opens only the true tail (S1, S2] = 110..112.
    val counting = new CountingEngine
    counting.putCommit(100, initialActions("v100"))
    counting.putCheckpoint(100, checkpointActions("v100", Seq.empty))
    (101L to 109L).foreach { version =>
      counting.putCommit(version, Seq(addAction(s"v$version.parquet")))
    }
    val base = coldSnapshot(counting, 109)

    (110L to 112L).foreach { version =>
      counting.putCommit(version, Seq(addAction(s"v$version.parquet")))
    }
    counting.clearIo()

    val result = from(counting, base, version = Some(112))
    val refreshTrace = counting.trace

    assertMatchesCold(result, counting, 112)
    assert(
      refreshTrace.commitJsonReads.map(FileNames.deltaVersion).sorted == (110L to 112L),
      s"P&M replay reopened retained commits: $refreshTrace")
    assertSingleListFrom(refreshTrace, 101)
    assert(
      refreshTrace.listResults.flatten
        .filter(FileNames.isCommitFile)
        .map(FileNames.deltaVersion)
        .sorted == (101L to 112L))
    assert(refreshTrace.checkpointReads.isEmpty)
    assert(refreshTrace.crcReads.isEmpty)
    assert(result.getLogSegment.getCheckpointVersionOpt == Optional.of(100L))
    assert(
      result.getLogSegment.getDeltas.asScala.map(f => FileNames.deltaVersion(f.getPath)) ==
        (101L to 112L))
  }

  test("IncrementalReplay is one validated budget value and builders reject null") {
    assert(IncrementalReplay.disabled() == IncrementalReplay.upToCommits(0))
    assert(IncrementalReplay.unlimited() != IncrementalReplay.disabled())
    intercept[IllegalArgumentException] {
      IncrementalReplay.upToCommits(-1)
    }
    val builder = TableManager.loadSnapshot(tablePath.toString)
    val error = intercept[NullPointerException] {
      builder.withIncrementalCrcReplay(null)
    }
    assert(error.getMessage == "incrementalReplay is null")
  }

  Seq(
    ("disabled", IncrementalReplay.disabled(), false),
    ("exact budget", IncrementalReplay.upToCommits(2), true),
    ("over budget", IncrementalReplay.upToCommits(1), false),
    ("unlimited", IncrementalReplay.unlimited(), true)).foreach {
    case (label, replay, expectTargetCrc) =>
      test(s"Rust CRC policy matrix: $label") {
        val counting = new CountingEngine
        counting.putCommit(0, initialActions("v0"))
        counting.putCommit(1, writeCommit(addAction("v1.parquet")))
        val crc1 = counting.putCrc(
          1,
          crcAt(1, "v0", tableSizeBytes = 20, numFiles = 2))
        val base = coldSnapshot(counting, 1)
        counting.putCommit(2, writeCommit(addAction("v2.parquet")))
        counting.putCommit(3, writeCommit(addAction("v3.parquet")))
        counting.clearIo()

        val result = from(
          counting,
          base,
          version = Some(3),
          configure = _.withIncrementalCrcReplay(replay))
        val crc = result.getCurrentCrcInfo
        val trace = counting.trace

        assert(crc.isPresent == expectTargetCrc)
        if (expectTargetCrc) {
          assert(crc.get.getVersion == 3)
          assert(crc.get.getNumFiles == 4)
          assert(crc.get.getTableSizeBytes == 40)
        }
        assert(result.getTableProperties.get("fixture.state") == "v0")
        assertCrcRefreshIo(trace, 1, Seq.empty, Seq(2, 3))
      }
  }

  test("unsupported incremental CRC operation falls back to tail P&M replay") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, writeCommit(addAction("v1.parquet")))
    val crc1 = counting.putCrc(
      1,
      crcAt(1, "v0", tableSizeBytes = 20, numFiles = 2))
    val base = coldSnapshot(counting, 1)
    assert(base.getCurrentCrcInfo.get.getVersion == 1)

    counting.putCommit(
      2,
      commitWithOperation("RESTORE", addAction("v2.parquet")))
    counting.putCommit(
      3,
      writeCommit(metadataAction(metadata("tail")), addAction("v3.parquet")))
    counting.clearIo()

    val result = from(
      counting,
      base,
      version = Some(3),
      configure = _.withIncrementalCrcReplay(IncrementalReplay.unlimited()))
    val trace = counting.trace
    val commitVersions = trace.commitJsonReads.map(FileNames.deltaVersion)

    assert(!result.getCurrentCrcInfo.isPresent)
    assert(result.getTableProperties.get("fixture.state") == "tail")
    assertSingleListFrom(trace, 1)
    assert(
      trace.crcReads.isEmpty,
      s"in-memory CRC@1 must avoid reopening ${crc1.getPath}: $trace")
    assert(
      commitVersions.toSet == Set(2L, 3L) && commitVersions.forall(version =>
        version > 1 && version <= 3),
      s"fallback may read only the target tail (1, 3]: $trace")
    assert(trace.checkpointReads.isEmpty)
    assertNoLastCheckpoint(trace)
  }

  test("Rust pick_latest_base_crc: newer disk CRC beats the in-memory base") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, writeCommit(addAction("v1.parquet")))
    counting.putCommit(2, writeCommit(addAction("v2.parquet")))
    counting.putCrc(2, crcAt(2, "v0", tableSizeBytes = 30, numFiles = 3))
    val base = coldSnapshot(counting, 2)

    counting.putCommit(
      3,
      writeCommit(metadataAction(metadata("v3")), addAction("v3.parquet")))
    val diskCrc3 = counting.putCrc(
      3,
      crcAt(3, "v3", tableSizeBytes = 40, numFiles = 4))
    counting.putCommit(4, writeCommit(addAction("v4.parquet")))
    counting.clearIo()

    val result = from(
      counting,
      base,
      version = Some(4),
      configure = _.withIncrementalCrcReplay(IncrementalReplay.unlimited()))
    val crc = result.getCurrentCrcInfo
    val trace = counting.trace

    assert(crc.isPresent && crc.get.getVersion == 4)
    assert(crc.get.getNumFiles == 5)
    assert(crc.get.getTableSizeBytes == 50)
    assert(result.getTableProperties.get("fixture.state") == "v3")
    assertCrcRefreshIo(trace, 1, Seq(diskCrc3.getPath), Seq(4))
  }

  test("Rust pick_latest_base_crc: newer in-memory CRC beats the disk base") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    (1L to 3L).foreach(version =>
      counting.putCommit(version, writeCommit(addAction(s"v$version.parquet"))))
    val crc3 = counting.putCrc(
      3,
      crcAt(3, "v0", tableSizeBytes = 40, numFiles = 4))
    val base = coldSnapshot(counting, 3)
    assert(base.getCurrentCrcInfo.get.getVersion == 3)

    counting.removeVisible(crc3.getPath)
    val diskCrc2 = counting.putCrc(
      2,
      crcAt(2, "v0", tableSizeBytes = 30, numFiles = 3))
    counting.putCommit(4, writeCommit(addAction("v4.parquet")))
    counting.putCommit(5, writeCommit(addAction("v5.parquet")))
    counting.clearIo()

    val result = from(
      counting,
      base,
      version = Some(5),
      configure = _.withIncrementalCrcReplay(IncrementalReplay.unlimited()))
    val crc = result.getCurrentCrcInfo
    val trace = counting.trace

    assert(crc.isPresent && crc.get.getVersion == 5)
    assert(crc.get.getNumFiles == 6)
    assert(crc.get.getTableSizeBytes == 60)
    assertCrcRefreshIo(trace, 1, Seq.empty, Seq(4, 5))
  }

  test("stale CRC stays hidden while explicit stats retain on-demand replay") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCrc(0, crcAt(0, "v0"))
    counting.putCommit(1, writeCommit(addAction("v1.parquet")))
    val base = coldSnapshot(counting, 1)
    counting.putCommit(2, writeCommit(addAction("v2.parquet")))
    counting.clearIo()

    val result = from(counting, base, version = Some(2))
    val buildTrace = counting.trace
    assert(!result.getCurrentCrcInfo.isPresent)
    assertCrcRefreshIo(buildTrace, 1, Seq.empty, Seq(2))

    val statistics = result.getStatistics
    assert(statistics.getIncrementalChecksumLoadCost == Optional.of(2))
    val tableStats = statistics.getTableStats(counting.engine)
    val statsTrace = counting.trace

    assert(tableStats.isPresent)
    assert(tableStats.get.getNumFiles == 3)
    assert(tableStats.get.getTableSizeBytes == 30)
    assert(
      statsTrace.commitJsonReads.drop(buildTrace.commitJsonReads.size)
        .map(FileNames.deltaVersion).sorted == Seq(1L, 2L))
    assert(
      statsTrace.crcReads == buildTrace.crcReads,
      s"explicit stats replay must reuse the retained CRC without reopening it: $statsTrace")
  }

  test("Rust CRC at target supplies P&M and skips tail reads") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, writeCommit(addAction("v1.parquet")))
    val base = coldSnapshot(counting, 1)

    counting.putCommit(
      2,
      writeCommit(metadataAction(metadata("v2")), addAction("v2.parquet")))
    counting.putCommit(3, writeCommit(addAction("v3.parquet")))
    val targetCrc = counting.putCrc(
      3,
      crcAt(3, "v2", tableSizeBytes = 40, numFiles = 4))
    counting.clearIo()

    val result = from(counting, base, version = Some(3))
    val trace = counting.trace

    assert(result.getCurrentCrcInfo.get.getVersion == 3)
    assert(result.getTableProperties.get("fixture.state") == "v2")
    assertCrcRefreshIo(trace, 1, Seq(targetCrc.getPath), Seq.empty)
  }

  test("Rust stale CRC newer than S1 seeds P&M replay after the CRC") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, writeCommit(addAction("v1.parquet")))
    val base = coldSnapshot(counting, 1)

    counting.putCommit(
      2,
      writeCommit(metadataAction(metadata("v2")), addAction("v2.parquet")))
    val staleCrc = counting.putCrc(
      2,
      crcAt(2, "v2", tableSizeBytes = 30, numFiles = 3))
    counting.putCommit(3, writeCommit(addAction("v3.parquet")))
    counting.clearIo()

    val result = from(counting, base, version = Some(3))
    val trace = counting.trace

    assert(!result.getCurrentCrcInfo.isPresent)
    assert(result.getTableProperties.get("fixture.state") == "v2")
    assertCrcRefreshIo(trace, 1, Seq(staleCrc.getPath), Seq(3))
  }

  test("Rust stale CRC at or below S1 cannot overwrite retained P&M") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, writeCommit(addAction("v1.parquet")))
    val staleCrc = counting.putCrc(
      1,
      crcAt(1, "v0", tableSizeBytes = 20, numFiles = 2))
    counting.putCommit(
      2,
      writeCommit(metadataAction(metadata("retained")), addAction("v2.parquet")))
    counting.putCommit(3, writeCommit(addAction("v3.parquet")))
    val base = coldSnapshot(counting, 3)
    assert(base.getTableProperties.get("fixture.state") == "retained")

    counting.putCommit(4, writeCommit(addAction("v4.parquet")))
    counting.putCommit(5, writeCommit(addAction("v5.parquet")))
    counting.clearIo()

    val result = from(counting, base, version = Some(5))
    val trace = counting.trace

    assert(!result.getCurrentCrcInfo.isPresent)
    assert(result.getTableProperties.get("fixture.state") == "retained")
    assertCrcRefreshIo(trace, 1, Seq.empty, Seq(4, 5))
  }

  test("Rust checkpoint-ahead rebuild drops a stale base CRC") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, writeCommit(addAction("v1.parquet")))
    val staleCrc = counting.putCrc(
      1,
      crcAt(1, "v0", tableSizeBytes = 20, numFiles = 2))
    counting.putCommit(2, writeCommit(addAction("v2.parquet")))
    val base = coldSnapshot(counting, 2)

    counting.putCommit(3, writeCommit(addAction("v3.parquet")))
    val checkpoint3 = counting.putCheckpoint(3, checkpointActions("v0", 1L to 3L))
    counting.putCommit(4, writeCommit(addAction("v4.parquet")))
    counting.clearIo()

    val result = from(
      counting,
      base,
      version = Some(4),
      configure = _.withIncrementalCrcReplay(IncrementalReplay.unlimited()))
    val trace = counting.trace

    assert(result.getLogSegment.getCheckpointVersionOpt == Optional.of(3L))
    assert(!result.getCurrentCrcInfo.isPresent)
    assert(!trace.crcReads.contains(staleCrc.getPath))
    assertCrcRefreshIo(trace, 1, Seq.empty, Seq(4), Seq(checkpoint3.getPath))
  }

  test("Rust multi-hop replay then checkpoint rebuild drops the carried CRC") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    val base = coldSnapshot(counting, 0)

    (1L to 3L).foreach(version =>
      counting.putCommit(version, writeCommit(addAction(s"v$version.parquet"))))
    val crc3 = counting.putCrc(
      3,
      crcAt(3, "v0", tableSizeBytes = 40, numFiles = 4))
    counting.clearIo()

    val hop3 = from(counting, base, version = Some(3))
    assert(hop3.getCurrentCrcInfo.get.getVersion == 3)
    assertCrcRefreshIo(counting.trace, 1, Seq(crc3.getPath), Seq.empty)

    counting.putCommit(4, writeCommit(addAction("v4.parquet")))
    counting.putCommit(5, writeCommit(addAction("v5.parquet")))
    val checkpoint5 = counting.putCheckpoint(5, checkpointActions("v0", 1L to 5L))
    counting.putCommit(6, writeCommit(addAction("v6.parquet")))
    counting.clearIo()

    val hop6 = from(
      counting,
      hop3,
      version = Some(6),
      configure = _.withIncrementalCrcReplay(IncrementalReplay.unlimited()))
    val trace = counting.trace

    assert(hop6.getLogSegment.getCheckpointVersionOpt == Optional.of(5L))
    assert(!hop6.getCurrentCrcInfo.isPresent)
    assertCrcRefreshIo(trace, 1, Seq.empty, Seq(6), Seq(checkpoint5.getPath))
  }

  Seq(("fresh", false), ("preloaded", true)).foreach {
    case (construction, usePreloadedSegment) =>
      Seq(
        ("disabled", IncrementalReplay.disabled(), false, ChecksumWriteMode.FULL),
        ("unlimited", IncrementalReplay.unlimited(), true, ChecksumWriteMode.SIMPLE)).foreach {
        case (policy, replay, expectTargetCrc, expectedWriteMode) =>
          test(s"$construction construction honors $policy CRC replay policy") {
            val counting = new CountingEngine
            val commit0 = counting.putCommit(0, initialActions("v0"))
            val commit1 = counting.putCommit(1, writeCommit(addAction("v1.parquet")))
            val staleCrc = counting.putCrc(
              1,
              crcAt(1, "v0", tableSizeBytes = 20, numFiles = 2))
            val commit2 = counting.putCommit(2, writeCommit(addAction("v2.parquet")))
            val commit3 = counting.putCommit(3, writeCommit(addAction("v3.parquet")))
            val preloaded = Seq(commit0, commit1, staleCrc, commit2, commit3)
              .map(ParsedLogData.forFileStatus)

            var builder: SnapshotBuilder =
              TableManager.loadSnapshot(tablePath.toString).atVersion(3)
            if (usePreloadedSegment) {
              builder = builder
                .asInstanceOf[SnapshotBuilderImpl]
                .withPreloadedLogSegment(preloaded.asJava)
            }
            counting.clearIo()

            val result = builder
              .withIncrementalCrcReplay(replay)
              .build(counting.engine)
              .asInstanceOf[SnapshotImpl]
            val trace = counting.trace
            val currentCrc = result.getCurrentCrcInfo

            assert(currentCrc.isPresent == expectTargetCrc)
            if (expectTargetCrc) {
              assert(currentCrc.get.getVersion == 3)
              assert(currentCrc.get.getNumFiles == 4)
              assert(currentCrc.get.getTableSizeBytes == 40)
            }
            assert(result.getStatistics.getChecksumWriteMode == Optional.of(expectedWriteMode))
            assert(trace.crcReads == Seq(staleCrc.getPath))
            assert(
              trace.commitJsonReads.map(FileNames.deltaVersion).sorted == Seq(2L, 3L),
              s"construction must open each safe-tail commit exactly once: $trace")
            assert(trace.checkpointReads.isEmpty)
            if (usePreloadedSegment) {
              assert(trace.listStarts.isEmpty)
              assert(trace.fileStatusProbes.isEmpty)
            }
          }
      }
  }

  test("supplied P&M wins while unlimited replay materializes current stats") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, writeCommit(addAction("v1.parquet")))
    val staleCrc = counting.putCrc(
      1,
      crcAt(1, "crc", tableSizeBytes = 20, numFiles = 2))
    counting.putCommit(
      2,
      writeCommit(metadataAction(metadata("tail")), addAction("v2.parquet")))
    counting.putCommit(3, writeCommit(addAction("v3.parquet")))
    val suppliedMetadata = metadata("supplied")
    counting.clearIo()

    val result = TableManager
      .loadSnapshot(tablePath.toString)
      .atVersion(3)
      .withProtocolAndMetadata(protocol, suppliedMetadata)
      .withIncrementalCrcReplay(IncrementalReplay.unlimited())
      .build(counting.engine)
      .asInstanceOf[SnapshotImpl]
    val tableStats = result.getStatistics.getTableStats(counting.engine)
    val trace = counting.trace

    assert(result.getMetadata == suppliedMetadata)
    assert(result.getTableProperties.get("fixture.state") == "supplied")
    assert(result.getCurrentCrcInfo.get.getVersion == 3)
    assert(tableStats.isPresent)
    assert(tableStats.get.getNumFiles == 4)
    assert(tableStats.get.getTableSizeBytes == 40)
    assert(result.getStatistics.getChecksumWriteMode == Optional.of(ChecksumWriteMode.SIMPLE))
    assert(trace.crcReads == Seq(staleCrc.getPath))
    assert(
      trace.commitJsonReads.map(FileNames.deltaVersion).sorted == Seq(2L, 3L),
      s"supplied P&M must prevent a second tail replay: $trace")
    assert(trace.checkpointReads.isEmpty)
  }

  test("malformed newer disk CRC falls back to and advances the in-memory CRC") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, writeCommit(addAction("v1.parquet")))
    counting.putCrc(1, crcAt(1, "v0", tableSizeBytes = 20, numFiles = 2))
    val base = coldSnapshot(counting, 1)
    assert(base.getCurrentCrcInfo.get.getVersion == 1)

    counting.putCommit(2, writeCommit(addAction("v2.parquet")))
    val malformedCrc = counting.putMalformedCrc(2)
    counting.putCommit(3, writeCommit(addAction("v3.parquet")))
    counting.clearIo()

    val result = from(
      counting,
      base,
      version = Some(3),
      configure = _.withIncrementalCrcReplay(IncrementalReplay.unlimited()))
    val trace = counting.trace

    assert(result.getCurrentCrcInfo.get.getVersion == 3)
    assert(result.getCurrentCrcInfo.get.getNumFiles == 4)
    assert(result.getCurrentCrcInfo.get.getTableSizeBytes == 40)
    assertCrcRefreshIo(trace, 1, Seq(malformedCrc.getPath), Seq(2, 3))
  }

  test("incremental refresh resolves an unresolved base CRC with the current engine") {
    val engineA = new CountingEngine
    engineA.putCommit(0, initialActions("v0"))
    engineA.putCommit(1, writeCommit(addAction("v1.parquet")))
    val crc1 = engineA.putCrc(
      1,
      crcAt(1, "v0", tableSizeBytes = 20, numFiles = 2))
    val base = coldSnapshot(
      engineA,
      1,
      configure = _.withProtocolAndMetadata(protocol, metadata("v0")))
    assert(engineA.trace.crcReads.isEmpty)

    val engineB = engineA.fork()
    engineB.putCommit(2, writeCommit(addAction("v2.parquet")))
    engineA.clearIo()
    engineB.clearIo()

    val result = from(
      engineB,
      base,
      version = Some(2),
      configure = _.withIncrementalCrcReplay(IncrementalReplay.unlimited()))
    val coldEngine = engineB.fork()
    val expected = coldSnapshot(
      coldEngine,
      2,
      configure = _.withIncrementalCrcReplay(IncrementalReplay.unlimited()))

    assert(engineA.trace.crcReads.isEmpty, s"engine A reopened the lazy CRC: ${engineA.trace}")
    assert(engineB.trace.crcReads == Seq(crc1.getPath))
    assert(engineB.trace.commitJsonReads.map(FileNames.deltaVersion) == Seq(2L))
    assertEquivalent(result, engineB.engine, expected, coldEngine.engine)
  }

  test("same-target refresh emits a current build report") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCheckpoint(0, checkpointActions("v0", Seq.empty))
    counting.putCommit(1, writeCommit(addAction("v1.parquet")))
    val base = coldSnapshot(counting, 1)
    val baseReportId = base.getSnapshotReport.getReportUUID
    counting.clearIo()
    counting.clearReports()

    val result = from(counting, base, version = Some(1))
    val reports = counting.reports

    assert(result eq base)
    assert(reports.size == 1)
    assert(reports.head.getReportUUID != baseReportId)
    assert(reports.head.getVersion == Optional.of(1L))
    assert(reports.head.getCheckpointVersion == Optional.of(0L))
  }

  test("unchanged latest refresh promotion emits a current build report") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCheckpoint(0, checkpointActions("v0", Seq.empty))
    counting.putCommit(1, writeCommit(addAction("v1.parquet")))
    val base = coldSnapshot(counting, 1)
    val baseReportId = base.getSnapshotReport.getReportUUID
    counting.clearIo()
    counting.clearReports()

    val result = from(counting, base)
    val reports = counting.reports
    val resultReport = result.getSnapshotReport

    assert(!(result eq base))
    assert(result.wasBuiltAsLatest)
    assert(!base.wasBuiltAsLatest)
    assert(reports.size == 1)
    assert(reports.head.getReportUUID != baseReportId)
    assert(reports.head.getVersion == Optional.of(1L))
    assert(reports.head.getCheckpointVersion == Optional.of(0L))
    assert(reports.head.getReportUUID == resultReport.getReportUUID)
    assert(reports.head.getVersion == resultReport.getVersion)
    assert(reports.head.getCheckpointVersion == resultReport.getCheckpointVersion)
  }

  test("supplied P&M and disabled replay defer a visible CRC during incremental refresh") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(1, writeCommit(addAction("v1.parquet")))
    val base = coldSnapshot(counting, 1)

    counting.putCommit(
      2,
      writeCommit(metadataAction(metadata("tail")), addAction("v2.parquet")))
    val targetCrc = counting.putCrc(
      2,
      crcAt(2, "crc", tableSizeBytes = 30, numFiles = 3))
    val suppliedMetadata = metadata("supplied")
    counting.clearIo()

    val result = from(
      counting,
      base,
      version = Some(2),
      configure = _
        .withProtocolAndMetadata(protocol, suppliedMetadata)
        .withIncrementalCrcReplay(IncrementalReplay.disabled()))
    val buildTrace = counting.trace

    assert(result.getProtocol == protocol)
    assert(result.getMetadata == suppliedMetadata)
    assert(result.getTableProperties.get("fixture.state") == "supplied")
    assertSingleListFrom(buildTrace, 1)
    assert(buildTrace.fileStatusProbes.isEmpty)
    assert(
      buildTrace.contentReads.isEmpty,
      s"build with supplied P&M and disabled replay must only list files: $buildTrace")

    assert(result.getCurrentCrcInfo.get.getVersion == 2)
    assert(counting.trace.crcReads == Seq(targetCrc.getPath))
  }

  // ------------------------------------------------------------------
  // Contract tests for withLoadedCrcInfo + withPreloadedLogSegment APIs
  // ------------------------------------------------------------------

  test("withLoadedCrcInfo at target version supplies CRC " +
    "without disk read") {
    val counting = new CountingEngine
    val commit0 = counting.putCommit(0, initialActions("v0"))
    counting.putCheckpoint(
      0,
      checkpointActions("v0", Seq.empty))
    val commit1 = counting.putCommit(
      1,
      writeCommit(addAction("v1.parquet")))
    val commit2 = counting.putCommit(
      2,
      writeCommit(addAction("v2.parquet")))

    // Construct CRC at version 2 programmatically
    val refCrc = crcAt(
      2,
      "v0",
      tableSizeBytes = 30,
      numFiles = 3)

    // Build using preloaded segment + injected CRC
    val preloaded = Seq(commit0, commit1, commit2)
      .map(ParsedLogData.forFileStatus)
    counting.clearIo()

    val result = TableManager
      .loadSnapshot(tablePath.toString)
      .atVersion(2)
      .withPreloadedLogSegment(preloaded.asJava)
      .withLoadedCrcInfo(refCrc)
      .build(counting.engine)
      .asInstanceOf[SnapshotImpl]
    val trace = counting.trace

    assert(result.getCurrentCrcInfo.isPresent)
    assert(result.getCurrentCrcInfo.get == refCrc)
    assert(
      trace.crcReads.isEmpty,
      "no .crc file should be read when CRC is injected")
    assert(trace.listStarts.isEmpty, "preloaded segment bypasses listing")
  }

  test("stale withLoadedCrcInfo is dropped when version" +
    " is before checkpoint") {
    val counting = new CountingEngine
    counting.putCommit(0, initialActions("v0"))
    counting.putCommit(
      1,
      writeCommit(addAction("v1.parquet")))
    counting.putCommit(
      2,
      writeCommit(addAction("v2.parquet")))
    val checkpoint3 = counting.putCheckpoint(
      3,
      checkpointActions("v3", Seq(1L, 2L)))
    counting.putCommit(
      3,
      writeCommit(
        metadataAction(metadata("v3")),
        addAction("v3.parquet")))
    val commit4 = counting.putCommit(
      4,
      writeCommit(addAction("v4.parquet")))
    val commit5 = counting.putCommit(
      5,
      writeCommit(addAction("v5.parquet")))

    // CRC at version 2 is before checkpoint at 3
    val staleCrc = crcAt(
      2,
      "v0",
      tableSizeBytes = 20,
      numFiles = 2)

    val preloaded = Seq(
      checkpoint3,
      commit4,
      commit5).map(ParsedLogData.forFileStatus)
    counting.clearIo()

    val result = TableManager
      .loadSnapshot(tablePath.toString)
      .atVersion(5)
      .withPreloadedLogSegment(preloaded.asJava)
      .withLoadedCrcInfo(staleCrc)
      .build(counting.engine)
      .asInstanceOf[SnapshotImpl]

    // getEligibleCrc filters: crc.version (2) <
    // checkpointVersion (3) -> ineligible
    assert(
      !result.getCurrentCrcInfo.isPresent ||
        result.getCurrentCrcInfo.get.getVersion != 2,
      "stale CRC at version 2 (before checkpoint@3) " +
        "must not appear on the snapshot")
  }

  test("disk .crc in preloaded segment wins over " +
    "injected when newer") {
    val counting = new CountingEngine
    val commit0 = counting.putCommit(0, initialActions("v0"))
    val commit1 = counting.putCommit(
      1,
      writeCommit(addAction("v1.parquet")))
    val commit2 = counting.putCommit(
      2,
      writeCommit(addAction("v2.parquet")))
    val commit3 = counting.putCommit(
      3,
      writeCommit(addAction("v3.parquet")))

    // Disk CRC at the target version (3)
    val diskCrc3 = counting.putCrc(
      3,
      crcAt(3, "v0", tableSizeBytes = 40, numFiles = 4))
    // Injected CRC at an older version (1)
    val injectedCrc = crcAt(
      1,
      "v0",
      tableSizeBytes = 10,
      numFiles = 1)

    val preloaded =
      Seq(commit0, commit1, commit2, commit3, diskCrc3).map(ParsedLogData.forFileStatus)
    counting.clearIo()

    val result = TableManager
      .loadSnapshot(tablePath.toString)
      .atVersion(3)
      .withPreloadedLogSegment(preloaded.asJava)
      .withLoadedCrcInfo(injectedCrc)
      .build(counting.engine)
      .asInstanceOf[SnapshotImpl]
    val crc = result.getCurrentCrcInfo

    assert(crc.isPresent, "CRC must be present")
    assert(crc.get.getVersion == 3, "disk CRC at target version wins")
    assert(crc.get.getTableSizeBytes == 40)
    assert(crc.get.getNumFiles == 4)
    assert(
      counting.trace.crcReads.contains(diskCrc3.getPath),
      "disk .crc file must be read")
  }

  test("builderFrom(base).withPreloadedLogSegment " +
    "preserves base committer") {
    val counting = new CountingEngine
    val commit0 = counting.putCommit(0, initialActions("v0"))

    val customCommitter = new Committer {
      override def commit(
          engine: Engine,
          actions: CloseableIterator[Row],
          metadata: io.delta.kernel.commit.CommitMetadata): io.delta.kernel.commit.CommitResponse =
        throw new UnsupportedOperationException
    }

    val base = coldSnapshot(counting, 0, configure = _.withCommitter(customCommitter))
    assert(base.getCommitter eq customCommitter)

    val commit1 = counting.putCommit(
      1,
      writeCommit(addAction("v1.parquet")))
    val preloaded = Seq(commit0, commit1)
      .map(ParsedLogData.forFileStatus)
    counting.clearIo()

    val result = TableManager
      .builderFrom(tablePath.toString, base)
      .atVersion(1)
      .withPreloadedLogSegment(preloaded.asJava)
      .build(counting.engine)
      .asInstanceOf[SnapshotImpl]

    assert(result.getCommitter eq customCommitter, "committer from base snapshot must be preserved")
    assert(result.getVersion == 1)
  }

  test("withLoadedCrcInfo provides P&M without " +
    "ProtocolMetadataLogReplay") {
    val counting = new CountingEngine
    val commit0 = counting.putCommit(0, initialActions("v0"))
    val commit1 = counting.putCommit(
      1,
      writeCommit(addAction("v1.parquet")))
    val commit2 = counting.putCommit(
      2,
      writeCommit(addAction("v2.parquet")))

    // CRC at the target version carries P&M
    val crcMetadata = metadata("from-crc")
    val targetCrc = new CRCInfo(
      2,
      crcMetadata,
      protocol,
      20,
      2,
      Optional.empty(),
      Optional.empty(),
      Optional.empty())

    val preloaded = Seq(commit0, commit1, commit2)
      .map(ParsedLogData.forFileStatus)
    counting.clearIo()

    val result = TableManager
      .loadSnapshot(tablePath.toString)
      .atVersion(2)
      .withPreloadedLogSegment(preloaded.asJava)
      .withLoadedCrcInfo(targetCrc)
      .build(counting.engine)
      .asInstanceOf[SnapshotImpl]
    val crc = result.getCurrentCrcInfo

    assert(crc.isPresent)
    assert(crc.get.getVersion == 2)
    assert(crc.get.getProtocol == protocol)
    assert(crc.get.getMetadata == crcMetadata)
    assert(
      result.getTableProperties.get("fixture.state") ==
        "from-crc",
      "P&M from the target-version CRC should be used")
  }
}
