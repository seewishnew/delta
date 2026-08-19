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
package io.delta.spark.internal.v2.write;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.ConcurrentTransactionException;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.kernel.KernelEngineFactory;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.delta.v2.interop.DeltaV2Snapshot$;
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StreamingWrite for DSv2 streaming <b>Append</b>. Spark's {@code V2Writes} rebuilds this per
 * micro-batch; {@link DeltaV2Write} supplies the executor write state ({@link
 * DeltaV2DataWriterFactory}), built from a throwaway transaction whose operation-independent
 * context is serialized into the factory. This class adds only the driver-side commit.
 *
 * <p>{@link #commit} builds its transaction from a freshly reloaded snapshot (so it commits at
 * {@code latest+1}), mirroring V1's per-batch {@code deltaLog.startTransaction()}.
 *
 * <p><b>Idempotency:</b> {@code withTransactionId(queryId, epochId)} records a {@code
 * SetTransaction}; {@link #commit} pre-checks the committed epoch for {@code queryId} and skips a
 * replay, as V1 ({@code txn.txnVersion}) does. A concurrent same-epoch commit that races the
 * pre-check is still caught as {@link ConcurrentTransactionException} and skipped.
 *
 * <p><b>Schema/protocol guard:</b> {@link #commit} fails the query if the reloaded snapshot's
 * schema or protocol has diverged from the write state's, since Kernel does not re-validate the
 * executor-written files at commit. TODO(#7140): rebuild the write state against the new
 * schema/protocol so a compatible change (e.g. an added nullable column) is tolerated instead of
 * forcing a query restart.
 */
class DeltaV2StreamingWrite implements StreamingWrite {

  private static final Logger logger =
      LoggerFactory.getLogger(DeltaV2StreamingWrite.class);

  private final Configuration hadoopConf;
  private final DeltaV2SnapshotManager snapshotManager;
  private final String queryId;
  private final DeltaV2DataWriterFactory dataWriterFactory;
  private final StructType writeSchema;
  private final Protocol writeProtocol;

  /**
   * @param hadoopConf Hadoop configuration per-operation engines are
   *     built from
   * @param initialSnapshot the batch's planned snapshot; write-state
   *     source and guard baseline
   * @param snapshotManager reloads the latest snapshot per epoch
   * @param queryId streaming query id; the transaction application id
   *     for cross-restart idempotency
   * @param dataWriterFactoryBuilder builds the executor write state
   */
  DeltaV2StreamingWrite(
      Configuration hadoopConf,
      Snapshot initialSnapshot,
      DeltaV2SnapshotManager snapshotManager,
      String queryId,
      Function<Transaction, DeltaV2DataWriterFactory> dataWriterFactoryBuilder) {
    this.hadoopConf =
        requireNonNull(hadoopConf, "hadoopConf is null");
    requireNonNull(initialSnapshot, "initialSnapshot is null");
    this.snapshotManager =
        requireNonNull(snapshotManager, "snapshotManager is null");
    this.queryId = requireNonNull(queryId, "queryId is null");
    requireNonNull(
        dataWriterFactoryBuilder,
        "dataWriterFactoryBuilder is null");
    this.writeSchema = initialSnapshot.getSchema();
    this.writeProtocol =
        ((SnapshotImpl) initialSnapshot).getProtocol();
    this.dataWriterFactory =
        KernelEngineFactory.withDefaultEngine(
            hadoopConf,
            engine -> {
              Transaction stateTxn =
                  initialSnapshot
                      .buildUpdateTableTransaction(
                          DeltaV2Write.getEngineInfo(),
                          Operation.STREAMING_UPDATE)
                      .build(engine);
              return dataWriterFactoryBuilder.apply(stateTxn);
            });
  }

  @Override
  public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
    return dataWriterFactory;
  }

  @Override
  public boolean useCommitCoordinator() {
    return false;
  }

  @Override
  public void commit(long epochId, WriterCommitMessage[] messages) {
    KernelEngineFactory.runWithDefaultEngine(
        hadoopConf, engine -> commitEpoch(engine, epochId, messages));
  }

  private void commitEpoch(
      Engine engine, long epochId, WriterCommitMessage[] messages) {
    SnapshotImpl latestSnapshot =
        DeltaV2Snapshot$.MODULE$.borrowKernelSnapshot(
            snapshotManager.loadLatestSnapshot(engine));

    assertSchemaAndProtocolUnchanged(latestSnapshot);

    long committedEpoch =
        ((SnapshotImpl) latestSnapshot)
            .getLatestTransactionVersion(engine, queryId)
            .orElse(-1L);
    if (committedEpoch >= epochId) {
      logger.info(
          "Skipping already committed epoch {} for query {}",
          epochId, queryId);
      return;
    }

    try {
      Transaction txn =
          latestSnapshot
              .buildUpdateTableTransaction(
                  DeltaV2Write.getEngineInfo(),
                  Operation.STREAMING_UPDATE)
              .withTransactionId(queryId, epochId)
              .build(engine);
      CloseableIterable<Row> dataActions =
          DeltaV2WriterCommitMessage.toDataActions(messages);
      long version =
          txn.commit(engine, dataActions).getVersion();
      logger.info(
          "DSv2 streaming epoch {} for query {} committed"
              + " at version {}",
          epochId, queryId, version);
    } catch (ConcurrentTransactionException e) {
      logger.info(
          "Skipping already committed epoch {} for query {}",
          epochId, queryId);
    }
  }

  /** Fails the epoch if the fresh snapshot's schema/protocol diverged from the write's baseline. */
  private void assertSchemaAndProtocolUnchanged(Snapshot latestSnapshot) {
    if (!writeSchema.equals(latestSnapshot.getSchema())) {
      throw new IllegalStateException(
          "DSv2 streaming write to query "
              + queryId
              + " cannot continue: the table schema changed after the stream started. Restart the "
              + "query to pick up the new schema.");
    }
    if (!writeProtocol.equals(((SnapshotImpl) latestSnapshot).getProtocol())) {
      throw new IllegalStateException(
          "DSv2 streaming write to query "
              + queryId
              + " cannot continue: the table protocol changed after the stream started. Restart "
              + "the query to pick up the new protocol.");
    }
  }

  @Override
  public void abort(long epochId, WriterCommitMessage[] messages) {
    logger.warn(
        "DSv2 streaming epoch {} for query {} aborted; {} task message(s) not committed. "
            + "Orphaned data files will be cleaned up by VACUUM.",
        epochId,
        queryId,
        messages != null ? messages.length : 0);
  }
}
