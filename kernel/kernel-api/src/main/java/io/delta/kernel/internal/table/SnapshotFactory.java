/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.kernel.internal.table;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Utils.resolvePath;

import io.delta.kernel.IncrementalReplay;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.checksum.CRCInfo;
import io.delta.kernel.internal.checksum.ChecksumReader;
import io.delta.kernel.internal.checksum.ChecksumUtils;
import io.delta.kernel.internal.commit.DefaultFileSystemManagedTableOnlyCommitter;
import io.delta.kernel.internal.files.ParsedCatalogCommitData;
import io.delta.kernel.internal.files.ParsedCheckpointData;
import io.delta.kernel.internal.files.ParsedChecksumData;
import io.delta.kernel.internal.files.ParsedLogCompactionData;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.internal.files.ParsedPublishedDeltaData;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.lang.ListUtils;
import io.delta.kernel.internal.metrics.SnapshotMetrics;
import io.delta.kernel.internal.metrics.SnapshotQueryContext;
import io.delta.kernel.internal.metrics.SnapshotReportImpl;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.replay.ProtocolMetadataLogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.snapshot.SnapshotManager;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class responsible for creating {@link Snapshot} instances.
 *
 * <p>This factory takes validated parameters from {@link SnapshotBuilderImpl} and orchestrates the
 * actual snapshot creation process. It handles path resolution, log segment loading, and
 * coordinates with various internal components to construct a fully initialized {@link Snapshot}.
 *
 * <p>Note: The {@link SnapshotBuilderImpl} is responsible for receiving and validating all builder
 * parameters, and then passing that information to this factory to actually create the {@link
 * Snapshot}.
 */
public class SnapshotFactory {

  //////////////////////////////////////////
  // Static utility methods and variables //
  //////////////////////////////////////////

  /**
   * Resolves the latest table version that exists at or before the given {@code
   * millisSinceEpochUTC}.
   *
   * <p>Updates the given {@code snapshotQueryCtx} with the resolved version and prints out useful
   * log statements.
   */
  public static long resolveTimestampToSnapshotVersion(
      Engine engine,
      SnapshotQueryContext snapshotQueryCtx,
      SnapshotImpl latestSnapshot,
      long millisSinceEpochUTC,
      List<ParsedLogData> logDatas) {
    List<ParsedCatalogCommitData> parsedCatalogCommits =
        logDatas.stream()
            .filter(logData -> logData instanceof ParsedCatalogCommitData && logData.isFile())
            .map(catalogCommit -> (ParsedCatalogCommitData) catalogCommit)
            .collect(Collectors.toList());

    final long resolvedVersionToLoad =
        snapshotQueryCtx
            .getSnapshotMetrics()
            .computeTimestampToVersionTotalDurationTimer
            .time(
                () ->
                    DeltaHistoryManager.getActiveCommitAtTimestamp(
                            engine,
                            latestSnapshot,
                            latestSnapshot.getLogPath(),
                            millisSinceEpochUTC,
                            true /* mustBeRecreatable */,
                            false /* canReturnLastCommit */,
                            false /* canReturnEarliestCommit */,
                            parsedCatalogCommits)
                        .getVersion());

    snapshotQueryCtx.setResolvedVersion(resolvedVersionToLoad);

    logger.info(
        "{}: Took {} ms to resolve timestamp {} to snapshot version {}",
        latestSnapshot.getPath(),
        snapshotQueryCtx
            .getSnapshotMetrics()
            .computeTimestampToVersionTotalDurationTimer
            .totalDurationMs(),
        millisSinceEpochUTC,
        resolvedVersionToLoad);

    return resolvedVersionToLoad;
  }

  /**
   * Creates a lazy loader for CRC file information. The CRC file is loaded only once when needed.
   *
   * <p>If {@link Lazy#isPresent()} is false, then the CRC file was never attempted to be loaded.
   *
   * <p>If {@link Lazy#isPresent()} is true, then the result is:
   *
   * <ul>
   *   <li>{@code Optional.empty()} if there is no CRC file in this LogSegment, we failed to read
   *       it, or we failed to parse it (e.g. missing required fields)
   *   <li>{@code Optional.of(crcInfo)} if the file exists and was successfully read and parsed
   * </ul>
   */
  public static Lazy<Optional<CRCInfo>> createLazyChecksumFileLoaderWithMetrics(
      Engine engine, Lazy<LogSegment> lazyLogSegment, SnapshotMetrics snapshotMetrics) {
    return new Lazy<>(
        () -> {
          final Optional<FileStatus> crcFileOpt = lazyLogSegment.get().getLastSeenChecksum();
          if (!crcFileOpt.isPresent()) {
            return Optional.empty();
          }
          return snapshotMetrics.loadCrcTotalDurationTimer.time(
              () -> ChecksumReader.tryReadChecksumFile(engine, crcFileOpt.get()));
        });
  }

  private static final Logger logger = LoggerFactory.getLogger(SnapshotFactory.class);

  //////////////////////////////////
  // Member methods and variables //
  //////////////////////////////////

  private final SnapshotBuilderImpl.Context ctx;
  private final Path tablePath;

  SnapshotFactory(Engine engine, SnapshotBuilderImpl.Context ctx) {
    this.ctx = ctx;
    this.tablePath = new Path(resolvePath(engine, ctx.unresolvedPath));
  }

  SnapshotImpl create(Engine engine) {
    final SnapshotQueryContext snapshotCtx = getSnapshotQueryContext();

    try {
      final SnapshotImpl snapshot =
          snapshotCtx
              .getSnapshotMetrics()
              .loadSnapshotTotalTimer
              .time(() -> createSnapshot(engine, snapshotCtx));

      logger.info(
          "[{}] Took {}ms to load snapshot (version = {}) for snapshot query {}",
          tablePath.toString(),
          snapshotCtx.getSnapshotMetrics().loadSnapshotTotalTimer.totalDurationMs(),
          snapshot.getVersion(),
          snapshotCtx.getQueryDisplayStr());

      engine
          .getMetricsReporters()
          .forEach(
              reporter ->
                  reporter.report(
                      ctx.baseSnapshotOpt
                              .filter(baseSnapshot -> baseSnapshot == snapshot)
                              .isPresent()
                          ? SnapshotReportImpl.forSuccess(snapshotCtx)
                          : snapshot.getSnapshotReport()));

      return snapshot;
    } catch (Exception e) {
      snapshotCtx.recordSnapshotErrorReport(engine, e);
      throw e;
    }
  }

  private SnapshotImpl createSnapshot(Engine engine, SnapshotQueryContext snapshotCtx) {
    final Optional<Long> timeTravelVersion = getTargetTimeTravelVersion(engine, snapshotCtx);
    final Optional<Long> effectiveTarget =
        timeTravelVersion.isPresent() ? timeTravelVersion : ctx.maxCatalogVersion;
    final boolean builtAsLatest =
        !ctx.timestampQueryContextOpt.isPresent()
            && (!ctx.versionOpt.isPresent() || ctx.versionOpt.equals(ctx.maxCatalogVersion));

    if (ctx.baseSnapshotOpt.isPresent() && ctx.preloadedLogSegment.isEmpty()) {
      return createIncrementalSnapshot(
          engine, snapshotCtx, ctx.baseSnapshotOpt.get(), effectiveTarget, builtAsLatest);
    }

    final Lazy<LogSegment> lazyLogSegment =
        getLazyLogSegment(engine, snapshotCtx, timeTravelVersion);
    final Lazy<Optional<CRCInfo>> lazyCrcInfo;
    final Optional<CRCInfo> targetCrcInfo;
    if (ctx.protocolAndMetadataOpt.isPresent()
        && ctx.incrementalReplay.equals(IncrementalReplay.disabled())) {
      lazyCrcInfo =
          createLazyChecksumFileLoaderWithMetrics(
              engine, lazyLogSegment, snapshotCtx.getSnapshotMetrics());
      targetCrcInfo = Optional.empty();
    } else {
      final LogSegment logSegment = lazyLogSegment.get();
      lazyCrcInfo =
          selectAndAdvanceCrc(
              engine,
              logSegment,
              ctx.loadedCrcInfo /* inMemoryBase */,
              snapshotCtx.getSnapshotMetrics());
      targetCrcInfo = lazyCrcInfo.get().filter(crc -> crc.getVersion() == logSegment.getVersion());
    }

    Protocol protocol;
    Metadata metadata;

    if (ctx.protocolAndMetadataOpt.isPresent()) {
      protocol = ctx.protocolAndMetadataOpt.get()._1;
      metadata = ctx.protocolAndMetadataOpt.get()._2;
    } else if (targetCrcInfo.isPresent()) {
      protocol = targetCrcInfo.get().getProtocol();
      metadata = targetCrcInfo.get().getMetadata();
    } else {
      ProtocolMetadataLogReplay.Result result =
          ProtocolMetadataLogReplay.loadProtocolAndMetadata(
              engine,
              tablePath,
              lazyLogSegment.get(),
              lazyCrcInfo,
              snapshotCtx.getSnapshotMetrics());
      protocol = result.protocol;
      metadata = result.metadata;
    }

    // We require maxCatalogVersion to be provided for catalogManaged tables. We cannot validate
    // this earlier since we need to first load the protocol.
    validateMaxCatalogVersionPresence(protocol);

    // TODO: When LogReplay becomes static utilities, we can create it inside of SnapshotImpl
    final LogReplay logReplay = new LogReplay(engine, tablePath, lazyLogSegment, lazyCrcInfo);

    return new SnapshotImpl(
        tablePath,
        timeTravelVersion.orElseGet(() -> lazyLogSegment.get().getVersion()),
        lazyLogSegment,
        logReplay,
        protocol,
        metadata,
        ctx.committerOpt.orElse(
            ctx.baseSnapshotOpt
                .map(SnapshotImpl::getCommitter)
                .orElse(DefaultFileSystemManagedTableOnlyCommitter.INSTANCE)),
        snapshotCtx,
        Optional.empty() /* inCommitTimestampOpt */,
        builtAsLatest);
  }

  private SnapshotImpl createIncrementalSnapshot(
      Engine engine,
      SnapshotQueryContext snapshotCtx,
      SnapshotImpl baseSnapshot,
      Optional<Long> effectiveTarget,
      boolean builtAsLatest) {
    if (effectiveTarget.isPresent() && effectiveTarget.get() == baseSnapshot.getVersion()) {
      snapshotCtx.setResolvedVersion(baseSnapshot.getVersion());
      snapshotCtx.setCheckpointVersion(baseSnapshot.getLogSegment().getCheckpointVersionOpt());
      return baseSnapshot;
    }

    final Optional<LogSegment> logSegmentOpt =
        snapshotCtx
            .getSnapshotMetrics()
            .loadLogSegmentTotalDurationTimer
            .time(
                () ->
                    new IncrementalSnapshotLoader(tablePath)
                        .loadLogSegment(engine, baseSnapshot, ctx.logDatas, effectiveTarget));

    if (!logSegmentOpt.isPresent()) {
      snapshotCtx.setResolvedVersion(baseSnapshot.getVersion());
      snapshotCtx.setCheckpointVersion(baseSnapshot.getLogSegment().getCheckpointVersionOpt());
      return builtAsLatest ? baseSnapshot.promoteToBuiltAsLatest(snapshotCtx) : baseSnapshot;
    }

    final LogSegment logSegment = logSegmentOpt.get();
    snapshotCtx.setResolvedVersion(logSegment.getVersion());
    snapshotCtx.setCheckpointVersion(logSegment.getCheckpointVersionOpt());
    final boolean rebuild =
        logSegment
            .getCheckpointVersionOpt()
            .map(checkpointVersion -> checkpointVersion > baseSnapshot.getVersion())
            .orElse(false);
    return createSnapshotFromSegment(
        engine,
        snapshotCtx,
        logSegment,
        ctx.protocolAndMetadataOpt,
        builtAsLatest,
        baseSnapshot,
        rebuild);
  }

  private SnapshotImpl createSnapshotFromSegment(
      Engine engine,
      SnapshotQueryContext snapshotCtx,
      LogSegment logSegment,
      Optional<Tuple2<Protocol, Metadata>> protocolAndMetadataOpt,
      boolean builtAsLatest,
      SnapshotImpl baseSnapshot,
      boolean rebuild) {
    final Lazy<LogSegment> lazyLogSegment = new Lazy<>(() -> logSegment);
    if (!rebuild && logSegment.getVersion() == baseSnapshot.getVersion()) {
      final Optional<CRCInfo> loadedBaseCrc =
          getEligibleCrc(logSegment, baseSnapshot.getLoadedCrcInfo());
      final Lazy<Optional<CRCInfo>> lazyCrcInfo;
      if (getPreferredDiskCrc(logSegment, loadedBaseCrc).isPresent()) {
        lazyCrcInfo =
            new Lazy<>(
                () ->
                    pickLatestBaseCrc(
                        engine, logSegment, loadedBaseCrc, snapshotCtx.getSnapshotMetrics()));
      } else {
        lazyCrcInfo = materializedCrcLazy(loadedBaseCrc);
      }
      final LogReplay logReplay = new LogReplay(engine, tablePath, lazyLogSegment, lazyCrcInfo);
      return new SnapshotImpl(
          tablePath,
          logSegment.getVersion(),
          lazyLogSegment,
          logReplay,
          baseSnapshot.getProtocol(),
          baseSnapshot.getMetadata(),
          ctx.committerOpt.orElse(baseSnapshot.getCommitter()),
          snapshotCtx,
          Optional.empty() /* inCommitTimestampOpt */,
          builtAsLatest);
    }

    final Lazy<Optional<CRCInfo>> lazyCrcInfo;
    final Optional<CRCInfo> baseCrcInfo;
    final Optional<CRCInfo> targetCrcInfo;
    if (protocolAndMetadataOpt.isPresent()
        && ctx.incrementalReplay.equals(IncrementalReplay.disabled())) {
      lazyCrcInfo =
          createLazyChecksumFileLoaderWithMetrics(
              engine, lazyLogSegment, snapshotCtx.getSnapshotMetrics());
      baseCrcInfo = Optional.empty();
      targetCrcInfo = Optional.empty();
    } else {
      lazyCrcInfo =
          selectAndAdvanceCrc(
              engine,
              logSegment,
              rebuild ? Optional.empty() : baseSnapshot.getLoadedCrcInfo(),
              snapshotCtx.getSnapshotMetrics());
      baseCrcInfo = lazyCrcInfo.get();
      targetCrcInfo = baseCrcInfo.filter(crc -> crc.getVersion() == logSegment.getVersion());
    }

    final Protocol protocol;
    final Metadata metadata;
    if (protocolAndMetadataOpt.isPresent()) {
      protocol = protocolAndMetadataOpt.get()._1;
      metadata = protocolAndMetadataOpt.get()._2;
    } else if (targetCrcInfo.isPresent()) {
      protocol = targetCrcInfo.get().getProtocol();
      metadata = targetCrcInfo.get().getMetadata();
    } else if (rebuild) {
      ProtocolMetadataLogReplay.Result result =
          ProtocolMetadataLogReplay.loadProtocolAndMetadata(
              engine, tablePath, logSegment, lazyCrcInfo, snapshotCtx.getSnapshotMetrics());
      protocol = result.protocol;
      metadata = result.metadata;
    } else {
      final Optional<CRCInfo> newerBaseCrc =
          baseCrcInfo.filter(crc -> crc.getVersion() > baseSnapshot.getVersion());
      final long replayAfter =
          newerBaseCrc.map(CRCInfo::getVersion).orElse(baseSnapshot.getVersion());
      final ProtocolMetadataLogReplay.TailResult tailResult =
          ProtocolMetadataLogReplay.loadProtocolAndMetadataFromTail(
              engine,
              tablePath,
              logSegment.segmentAfterVersion(replayAfter),
              snapshotCtx.getSnapshotMetrics());
      protocol =
          tailResult.protocol.orElseGet(
              () -> newerBaseCrc.map(CRCInfo::getProtocol).orElse(baseSnapshot.getProtocol()));
      metadata =
          tailResult.metadata.orElseGet(
              () -> newerBaseCrc.map(CRCInfo::getMetadata).orElse(baseSnapshot.getMetadata()));
    }

    TableFeatures.validateKernelCanReadTheTable(protocol, tablePath.toString());
    validateMaxCatalogVersionPresence(protocol);
    final LogReplay logReplay = new LogReplay(engine, tablePath, lazyLogSegment, lazyCrcInfo);
    return new SnapshotImpl(
        tablePath,
        logSegment.getVersion(),
        lazyLogSegment,
        logReplay,
        protocol,
        metadata,
        ctx.committerOpt.orElse(baseSnapshot.getCommitter()),
        snapshotCtx,
        Optional.empty() /* inCommitTimestampOpt */,
        builtAsLatest);
  }

  private Optional<CRCInfo> pickLatestBaseCrc(
      Engine engine,
      LogSegment logSegment,
      Optional<CRCInfo> inMemoryBase,
      SnapshotMetrics snapshotMetrics) {
    final Optional<CRCInfo> eligibleInMemoryBase = getEligibleCrc(logSegment, inMemoryBase);
    final Optional<FileStatus> preferredDiskCrc =
        getPreferredDiskCrc(logSegment, eligibleInMemoryBase);
    final Optional<CRCInfo> selected =
        preferredDiskCrc
            .flatMap(
                file ->
                    snapshotMetrics.loadCrcTotalDurationTimer.time(
                        () -> ChecksumReader.tryReadChecksumFile(engine, file)))
            .map(Optional::of)
            .orElse(eligibleInMemoryBase);
    return getEligibleCrc(logSegment, selected);
  }

  private Lazy<Optional<CRCInfo>> selectAndAdvanceCrc(
      Engine engine,
      LogSegment logSegment,
      Optional<CRCInfo> inMemoryBase,
      SnapshotMetrics snapshotMetrics) {
    final Optional<CRCInfo> baseCrcInfo =
        pickLatestBaseCrc(engine, logSegment, inMemoryBase, snapshotMetrics);
    Optional<CRCInfo> targetCrcInfo =
        baseCrcInfo.filter(crc -> crc.getVersion() == logSegment.getVersion());
    if (!targetCrcInfo.isPresent()
        && baseCrcInfo.isPresent()
        && ctx.incrementalReplay.allowsAdvancing(
            baseCrcInfo.get().getVersion(), logSegment.getVersion())) {
      try {
        targetCrcInfo = ChecksumUtils.tryBuildCrcIncrementally(engine, logSegment, baseCrcInfo);
      } catch (IOException e) {
        throw new RuntimeException("Failed to advance CRC incrementally", e);
      }
    }
    return materializedCrcLazy(targetCrcInfo.isPresent() ? targetCrcInfo : baseCrcInfo);
  }

  private static Optional<CRCInfo> getEligibleCrc(
      LogSegment logSegment, Optional<CRCInfo> crcInfo) {
    final long targetVersion = logSegment.getVersion();
    final Optional<Long> checkpointVersion = logSegment.getCheckpointVersionOpt();
    return crcInfo
        .filter(crc -> crc.getVersion() <= targetVersion)
        .filter(
            crc -> !checkpointVersion.isPresent() || crc.getVersion() >= checkpointVersion.get());
  }

  private static Optional<FileStatus> getPreferredDiskCrc(
      LogSegment logSegment, Optional<CRCInfo> inMemoryBase) {
    return logSegment
        .getLastSeenChecksum()
        .filter(
            file ->
                !inMemoryBase.isPresent()
                    || FileNames.checksumVersion(file.getPath()) > inMemoryBase.get().getVersion());
  }

  private static Lazy<Optional<CRCInfo>> materializedCrcLazy(Optional<CRCInfo> crcInfo) {
    final Lazy<Optional<CRCInfo>> lazyCrcInfo = new Lazy<>(() -> crcInfo);
    lazyCrcInfo.get();
    return lazyCrcInfo;
  }

  private SnapshotQueryContext getSnapshotQueryContext() {
    if (ctx.versionOpt.isPresent()) {
      return SnapshotQueryContext.forVersionSnapshot(tablePath.toString(), ctx.versionOpt.get());
    }
    if (ctx.timestampQueryContextOpt.isPresent()) {
      return SnapshotQueryContext.forTimestampSnapshot(
          tablePath.toString(), ctx.timestampQueryContextOpt.get()._2);
    }
    return SnapshotQueryContext.forLatestSnapshot(tablePath.toString());
  }

  private Lazy<LogSegment> getLazyLogSegment(
      Engine engine, SnapshotQueryContext snapshotCtx, Optional<Long> timeTravelVersion) {

    // Path A: preloaded log segment bypasses
    // SnapshotManager entirely
    if (!ctx.preloadedLogSegment.isEmpty()) {
      return new Lazy<>(
          () -> {
            final Path logPath = new Path(tablePath, "_delta_log");
            final LogSegment segment =
                buildLogSegmentFromPreloadedData(logPath, ctx.preloadedLogSegment);
            // Fix 3: validate preloaded segment version
            // matches requested version if set
            if (ctx.versionOpt.isPresent()) {
              long segVer = segment.getVersion();
              checkArgument(
                  segVer == ctx.versionOpt.get(),
                  "Preloaded segment version %s does " + "not match requested version %s",
                  segVer,
                  ctx.versionOpt.get());
            }
            snapshotCtx.setResolvedVersion(segment.getVersion());
            snapshotCtx.setCheckpointVersion(segment.getCheckpointVersionOpt());
            return segment;
          });
    }

    // Path B: cold build (existing path)
    return new Lazy<>(
        () -> {
          final LogSegment logSegment =
              snapshotCtx
                  .getSnapshotMetrics()
                  .loadLogSegmentTotalDurationTimer
                  .time(
                      () ->
                          new SnapshotManager(tablePath)
                              .getLogSegmentForVersion(
                                  engine, timeTravelVersion, ctx.logDatas, ctx.maxCatalogVersion));
          snapshotCtx.setResolvedVersion(logSegment.getVersion());
          snapshotCtx.setCheckpointVersion(logSegment.getCheckpointVersionOpt());
          return logSegment;
        });
  }

  private Optional<Long> getTargetTimeTravelVersion(
      Engine engine, SnapshotQueryContext snapshotCtx) {
    if (ctx.timestampQueryContextOpt.isPresent()) {
      return Optional.of(
          resolveTimestampToSnapshotVersion(
              engine,
              snapshotCtx,
              ctx.timestampQueryContextOpt.get()._1,
              ctx.timestampQueryContextOpt.get()._2,
              ctx.logDatas));
    } else if (ctx.versionOpt.isPresent()) {
      return ctx.versionOpt;
    }
    return Optional.empty();
  }

  private void validateMaxCatalogVersionPresence(Protocol protocol) {
    boolean isCatalogManaged = TableFeatures.isCatalogManagedSupported(protocol);
    if (isCatalogManaged) {
      checkArgument(
          ctx.maxCatalogVersion.isPresent(),
          "Must provide maxCatalogVersion for " + "catalogManaged tables");
    } else {
      checkArgument(
          !ctx.maxCatalogVersion.isPresent(),
          "Should not provide maxCatalogVersion for " + "file-system managed tables");
    }
  }

  /**
   * Builds a {@link LogSegment} from pre-parsed log data, bypassing SnapshotManager's file listing
   * entirely. Partitions the data by type, validates contiguity, and constructs the segment.
   */
  private static LogSegment buildLogSegmentFromPreloadedData(
      Path logPath, List<ParsedLogData> data) {
    checkArgument(!data.isEmpty(), "preloadedLogSegment must not be empty");

    // Partition by category class
    final Map<Class<? extends ParsedLogData>, List<ParsedLogData>> partitioned =
        data.stream()
            .collect(
                Collectors.groupingBy(
                    ParsedLogData::getGroupByCategoryClass,
                    LinkedHashMap::new,
                    Collectors.toList()));

    // Extract deltas
    final List<ParsedLogData> rawDeltas =
        partitioned.getOrDefault(ParsedPublishedDeltaData.class, Collections.emptyList());
    final List<FileStatus> deltaFiles =
        rawDeltas.stream()
            .filter(ParsedLogData::isFile)
            .map(ParsedLogData::getFileStatus)
            .collect(Collectors.toList());

    // Extract checkpoints
    final List<FileStatus> checkpointFiles =
        partitioned.getOrDefault(ParsedCheckpointData.class, Collections.emptyList()).stream()
            .map(ParsedLogData::getFileStatus)
            .collect(Collectors.toList());

    // Extract compactions
    final List<FileStatus> compactionFiles =
        partitioned.getOrDefault(ParsedLogCompactionData.class, Collections.emptyList()).stream()
            .map(ParsedLogData::getFileStatus)
            .collect(Collectors.toList());

    // Warning 5: sort deltas by version for contiguity
    deltaFiles.sort(Comparator.comparingLong(f -> FileNames.deltaVersion(new Path(f.getPath()))));

    // Warning 6: pick checksum by max version
    final List<ParsedLogData> checksums =
        partitioned.getOrDefault(ParsedChecksumData.class, Collections.emptyList());
    final Optional<FileStatus> lastSeenChecksum =
        checksums.stream()
            .max(Comparator.comparingLong(ParsedLogData::getVersion))
            .map(ParsedLogData::getFileStatus);

    // Fix 4: allow checkpoint-only segments
    checkArgument(
        !deltaFiles.isEmpty() || !checkpointFiles.isEmpty(),
        "Preloaded segment must contain at least " + "delta files or checkpoint files");

    // Determine version from last delta or checkpoint
    final long version;
    final FileStatus lastDelta;
    if (!deltaFiles.isEmpty()) {
      lastDelta = ListUtils.getLast(deltaFiles);
      version = FileNames.deltaVersion(new Path(lastDelta.getPath()));
    } else {
      lastDelta = null;
      version =
          checkpointFiles.stream()
              .mapToLong(f -> FileNames.getFileVersion(new Path(f.getPath())))
              .max()
              .getAsLong();
    }

    // Determine max published delta version
    final Optional<Long> maxPublishedDeltaVersion =
        rawDeltas.stream()
            .filter(d -> d instanceof ParsedPublishedDeltaData)
            .map(ParsedLogData::getVersion)
            .max(Long::compareTo);

    return new LogSegment(
        logPath,
        version,
        deltaFiles,
        compactionFiles,
        checkpointFiles,
        lastDelta,
        lastSeenChecksum,
        maxPublishedDeltaVersion);
  }
}
