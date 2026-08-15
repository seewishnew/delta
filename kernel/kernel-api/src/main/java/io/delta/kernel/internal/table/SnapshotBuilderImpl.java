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
import static java.util.Objects.requireNonNull;

import io.delta.kernel.IncrementalReplay;
import io.delta.kernel.Snapshot;
import io.delta.kernel.SnapshotBuilder;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.checksum.CRCInfo;
import io.delta.kernel.internal.files.LogDataUtils;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.internal.lang.ListUtils;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.Tuple2;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * An implementation of {@link SnapshotBuilder}.
 *
 * <p>Note: The primary responsibility of this class is to take input, validate that input, and then
 * pass the input to the {@link SnapshotFactory}, which is then responsible for actually creating
 * the {@link Snapshot} instance.
 */
public class SnapshotBuilderImpl implements SnapshotBuilder {

  public static class Context {
    public final String unresolvedPath;
    public Optional<Long> versionOpt = Optional.empty();
    public Optional<Tuple2<SnapshotImpl, Long>> timestampQueryContextOpt = Optional.empty();
    public Optional<Committer> committerOpt = Optional.empty();
    public List<ParsedLogData> logDatas = Collections.emptyList();
    public Optional<Tuple2<Protocol, Metadata>> protocolAndMetadataOpt = Optional.empty();
    public IncrementalReplay incrementalReplay = IncrementalReplay.disabled();
    public Optional<Long> maxCatalogVersion = Optional.empty();
    public Optional<SnapshotImpl> baseSnapshotOpt = Optional.empty();
    public List<ParsedLogData> preloadedLogSegment = Collections.emptyList();
    public Optional<CRCInfo> loadedCrcInfo = Optional.empty();

    public Context(String unresolvedPath) {
      this.unresolvedPath = requireNonNull(unresolvedPath, "unresolvedPath is null");
    }
  }

  private final Context ctx;

  public SnapshotBuilderImpl(String unresolvedPath) {
    ctx = new Context(unresolvedPath);
  }

  ////////////////////////////////////
  // Public SnapshotBuilder Methods //
  ////////////////////////////////////

  @Override
  public SnapshotBuilderImpl atVersion(long version) {
    checkArgument(version >= 0, "version must be >= 0");
    ctx.versionOpt = Optional.of(version);
    return this;
  }

  @Override
  public SnapshotBuilderImpl atTimestamp(long millisSinceEpochUTC, Snapshot latestSnapshot) {
    requireNonNull(latestSnapshot, "latestSnapshot is null");
    checkArgument(latestSnapshot instanceof SnapshotImpl, "latestSnapshot must be a SnapshotImpl");
    ctx.timestampQueryContextOpt =
        Optional.of(new Tuple2<>((SnapshotImpl) latestSnapshot, millisSinceEpochUTC));
    return this;
  }

  @Override
  public SnapshotBuilderImpl withCommitter(Committer committer) {
    ctx.committerOpt = Optional.of(requireNonNull(committer, "committer is null"));
    return this;
  }

  @Override
  public SnapshotBuilderImpl withLogData(List<ParsedLogData> logDatas) {
    ctx.logDatas = requireNonNull(logDatas, "logDatas is null");
    return this;
  }

  @Override
  public SnapshotBuilderImpl withProtocolAndMetadata(Protocol protocol, Metadata metadata) {
    ctx.protocolAndMetadataOpt =
        Optional.of(
            new Tuple2<>(
                requireNonNull(protocol, "protocol is null"),
                requireNonNull(metadata, "metadata is null")));
    return this;
  }

  @Override
  public SnapshotBuilderImpl withIncrementalCrcReplay(IncrementalReplay incrementalReplay) {
    ctx.incrementalReplay = requireNonNull(incrementalReplay, "incrementalReplay is null");
    return this;
  }

  @Override
  public SnapshotBuilderImpl withMaxCatalogVersion(long version) {
    checkArgument(version >= 0, "A valid version must be >= 0");
    ctx.maxCatalogVersion = Optional.of(version);
    return this;
  }

  /** Seeds this builder from an existing snapshot for retained-state incremental replay. */
  public SnapshotBuilderImpl fromSnapshot(Snapshot snapshot) {
    checkArgument(snapshot instanceof SnapshotImpl, "snapshot must be a SnapshotImpl");
    final SnapshotImpl baseSnapshot = (SnapshotImpl) snapshot;
    ctx.baseSnapshotOpt = Optional.of(baseSnapshot);
    return this;
  }

  /** Provides a pre-parsed log segment to bypass SnapshotManager listing entirely. */
  @Override
  public SnapshotBuilderImpl withPreloadedLogSegment(List<ParsedLogData> logSegmentData) {
    ctx.preloadedLogSegment = requireNonNull(logSegmentData);
    return this;
  }

  @Override
  public SnapshotBuilderImpl withLoadedCrcInfo(CRCInfo crcInfo) {
    ctx.loadedCrcInfo = Optional.of(requireNonNull(crcInfo, "crcInfo is null"));
    return this;
  }

  @Override
  public SnapshotImpl build(Engine engine) {
    validateInputOnBuild(engine);
    return new SnapshotFactory(engine, ctx).create(engine);
  }

  ////////////////////////////
  // Private Helper Methods //
  ////////////////////////////

  private void validateInputOnBuild(Engine engine) {
    validateTimestampNotGreaterThanLatestSnapshot(engine);
    validateVersionAndTimestampMutuallyExclusive();
    validateProtocolAndMetadataOnlyIfVersionProvided();
    validateProtocolRead();
    validateBaseSnapshotVersionConstraint();
    // TODO: delta-io/delta#4765 support other types
    LogDataUtils.validateLogDataContainsOnlyRatifiedStagedCommits(ctx.logDatas);
    LogDataUtils.validateLogDataIsSortedContiguous(ctx.logDatas);
    validateMaxCatalogVersionCompatibleWithTimeTravelParams();
    validateLogTailEndsWithMaxCatalogVersionOrVersionToLoad();
  }

  /**
   * Recall the semantics of time-travel by timestamp: "If the provided timestamp is after (strictly
   * greater than) the timestamp of the latest version of the table, snapshot resolution will fail."
   */
  private void validateTimestampNotGreaterThanLatestSnapshot(Engine engine) {
    ctx.timestampQueryContextOpt.ifPresent(
        x -> {
          final long latestSnapshotVersion = x._1.getVersion();
          final long latestSnapshotTimestamp = x._1.getTimestamp(engine);
          final long requestedTimestamp = x._2;

          if (requestedTimestamp > latestSnapshotTimestamp) {
            throw DeltaErrors.timestampAfterLatestCommit(
                ctx.unresolvedPath,
                requestedTimestamp,
                latestSnapshotTimestamp,
                latestSnapshotVersion);
          }
        });
  }

  private void validateVersionAndTimestampMutuallyExclusive() {
    checkArgument(
        !ctx.timestampQueryContextOpt.isPresent() || !ctx.versionOpt.isPresent(),
        "timestamp and version cannot be provided together");
  }

  private void validateProtocolAndMetadataOnlyIfVersionProvided() {
    checkArgument(
        ctx.versionOpt.isPresent() || !ctx.protocolAndMetadataOpt.isPresent(),
        "protocol and metadata can only be provided if a version is provided");
  }

  private void validateProtocolRead() {
    ctx.protocolAndMetadataOpt.ifPresent(
        x -> TableFeatures.validateKernelCanReadTheTable(x._1, ctx.unresolvedPath));
  }

  private void validateBaseSnapshotVersionConstraint() {
    if (ctx.baseSnapshotOpt.isPresent()) {
      Optional<Long> effectiveTarget =
          ctx.versionOpt.isPresent() ? ctx.versionOpt : ctx.maxCatalogVersion;
      if (!effectiveTarget.isPresent()) {
        return;
      }
      long baseVersion = ctx.baseSnapshotOpt.get().getVersion();
      long requestedVersion = effectiveTarget.get();
      checkArgument(
          requestedVersion >= baseVersion,
          String.format(
              "Cannot build incremental snapshot at "
                  + "version %d which is before base "
                  + "snapshot version %d",
              requestedVersion, baseVersion));
    }
  }

  /**
   * For catalog managed tables we cannot time-travel to a version after the max catalog version. We
   * also require that the latestSnapshot provided for timestamp-based queries has the max catalog
   * version.
   */
  private void validateMaxCatalogVersionCompatibleWithTimeTravelParams() {
    ctx.maxCatalogVersion.ifPresent(
        maxVersion -> {
          ctx.versionOpt.ifPresent(
              version ->
                  checkArgument(
                      version <= maxVersion,
                      String.format(
                          "Cannot time-travel to version %s after the max catalog version %s",
                          version, maxVersion)));
          ctx.timestampQueryContextOpt.ifPresent(
              queryContext ->
                  checkArgument(
                      queryContext._1.getVersion() == maxVersion,
                      "The latestSnapshot provided for timestamp-based time-travel queries "
                          + "must have version = maxCatalogVersion"));
        });
  }

  /**
   * When a catalog implementation has provided catalog commits we require that they provide up to
   * and including the version that we will load (which for a latest query is the max catalog
   * version, and for a time-travel-by-version query is the version to load). This is to validate
   * that the catalog has queried and provided sufficient catalog commits to correctly read the
   * table.
   */
  private void validateLogTailEndsWithMaxCatalogVersionOrVersionToLoad() {
    ctx.maxCatalogVersion.ifPresent(
        maxVersion -> {
          if (!ctx.logDatas.isEmpty()) {
            ParsedLogData tailLogData = ListUtils.getLast(ctx.logDatas);
            if (ctx.versionOpt.isPresent()) {
              checkArgument(
                  tailLogData.getVersion() >= ctx.versionOpt.get(),
                  "Provided catalog commits must include versionToLoad for time-travel queries");
            } else {
              checkArgument(
                  maxVersion == tailLogData.getVersion(),
                  "Provided catalog commits must end with max catalog version");
            }
          }
        });
  }
}
