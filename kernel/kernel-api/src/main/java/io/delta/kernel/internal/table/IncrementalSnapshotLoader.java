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

package io.delta.kernel.internal.table;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.InvalidTableException;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.checkpoints.CheckpointInstance;
import io.delta.kernel.internal.checkpoints.Checkpointer;
import io.delta.kernel.internal.files.LogDataUtils;
import io.delta.kernel.internal.files.ParsedCatalogCommitData;
import io.delta.kernel.internal.files.ParsedCheckpointData;
import io.delta.kernel.internal.files.ParsedChecksumData;
import io.delta.kernel.internal.files.ParsedDeltaData;
import io.delta.kernel.internal.files.ParsedLogCompactionData;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.internal.files.ParsedPublishedDeltaData;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.ListUtils;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.FileNames.DeltaLogFileType;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.utils.FileStatus;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Lists and assembles the retained-segment portion of an incremental snapshot refresh.
 *
 * <p>An empty result means the base is unchanged. A present segment is either rooted at a
 * checkpoint above the base or combined with the retained base.
 */
final class IncrementalSnapshotLoader {
  private static final Set<DeltaLogFileType> INCREMENTAL_FILE_TYPES =
      new HashSet<>(
          Arrays.asList(
              DeltaLogFileType.COMMIT,
              DeltaLogFileType.CHECKPOINT,
              DeltaLogFileType.CHECKSUM,
              DeltaLogFileType.LOG_COMPACTION));

  private final Path tablePath;
  private final Path logPath;

  IncrementalSnapshotLoader(Path tablePath) {
    this.tablePath = requireNonNull(tablePath);
    this.logPath = new Path(tablePath, "_delta_log");
  }

  Optional<LogSegment> loadLogSegment(
      Engine engine,
      SnapshotImpl baseSnapshot,
      List<ParsedLogData> catalogLogData,
      Optional<Long> effectiveTarget) {
    requireNonNull(engine, "engine is null");
    requireNonNull(baseSnapshot, "baseSnapshot is null");
    requireNonNull(catalogLogData, "catalogLogData is null");
    requireNonNull(effectiveTarget, "effectiveTarget is null");

    final long baseVersion = baseSnapshot.getVersion();
    if (effectiveTarget.isPresent()) {
      final long target = effectiveTarget.get();
      if (target == baseVersion) {
        return Optional.empty();
      }
      checkArgument(
          target > baseVersion,
          "Cannot build incremental snapshot at version %d which is before base snapshot "
              + "version %d",
          target,
          baseVersion);
    }

    final LogSegment baseSegment = baseSnapshot.getLogSegment();
    final long listingStart = baseSegment.getCheckpointVersionOpt().orElse(0L) + 1;
    final List<FileStatus> listedStatuses =
        DeltaLogActionUtils.listDeltaLogFilesAsIter(
                engine,
                INCREMENTAL_FILE_TYPES,
                tablePath,
                listingStart,
                effectiveTarget,
                true /* mustBeRecreatable */)
            .toInMemoryList();

    final Map<Class<? extends ParsedLogData>, List<ParsedLogData>> partitioned =
        listedStatuses.stream()
            .map(ParsedLogData::forFileStatus)
            .collect(
                Collectors.groupingBy(
                    ParsedLogData::getGroupByCategoryClass,
                    LinkedHashMap::new,
                    Collectors.toList()));
    final List<ParsedPublishedDeltaData> publishedDeltas =
        partitioned.getOrDefault(ParsedPublishedDeltaData.class, Collections.emptyList()).stream()
            .map(ParsedPublishedDeltaData.class::cast)
            .collect(Collectors.toList());
    final List<FileStatus> checkpointFiles = statusesFor(partitioned, ParsedCheckpointData.class);
    final List<FileStatus> checksumFiles = statusesFor(partitioned, ParsedChecksumData.class);
    final List<FileStatus> compactionFiles =
        statusesFor(partitioned, ParsedLogCompactionData.class);

    final List<ParsedDeltaData> allListedDeltas =
        combineDeltasWithCatalogPriority(
            publishedDeltas, catalogLogData, listingStart, effectiveTarget.orElse(Long.MAX_VALUE));
    if (allListedDeltas.isEmpty() && checkpointFiles.isEmpty()) {
      return emptyListingResult(effectiveTarget, baseVersion);
    }

    final Optional<CheckpointInstance> checkpoint =
        latestCompleteCheckpoint(checkpointFiles, effectiveTarget);
    final long checkpointVersion = checkpoint.map(instance -> instance.version).orElse(-1L);
    final List<ParsedDeltaData> deltasAfterCheckpoint =
        allListedDeltas.stream()
            .filter(delta -> delta.getVersion() > checkpointVersion)
            .collect(Collectors.toList());
    final long newVersion =
        deltasAfterCheckpoint.isEmpty()
            ? checkpointVersion
            : ListUtils.getLast(deltasAfterCheckpoint).getVersion();

    if (newVersion < baseVersion) {
      throw new InvalidTableException(
          tablePath.toString(),
          String.format(
              "The newest version in the incremental log listing %d is older than base snapshot "
                  + "version %d",
              newVersion, baseVersion));
    }
    effectiveTarget.ifPresent(
        target -> {
          if (newVersion < target) {
            throw new InvalidTableException(
                tablePath.toString(),
                String.format(
                    "Requested snapshot version %d is not available after base snapshot version "
                        + "%d; latest available version is %d",
                    target, baseVersion, newVersion));
          }
        });
    validateContiguousTail(allListedDeltas, baseVersion, newVersion);

    if (newVersion == baseVersion && !checkpoint.isPresent()) {
      return Optional.empty();
    }

    final List<FileStatus> selectedCheckpointFiles =
        selectCheckpointFiles(checkpointFiles, checkpoint);
    final FileStatus deltaAtEnd =
        findDeltaAtVersion(allListedDeltas, newVersion)
            .orElseGet(
                () ->
                    findDeltaAtVersion(publishedDeltas, checkpointVersion)
                        .orElseThrow(
                            () ->
                                new InvalidTableException(
                                    tablePath.toString(),
                                    "Missing delta file for checkpoint version "
                                        + checkpointVersion)));
    final List<FileStatus> selectedCompactions =
        compactionFiles.stream()
            .filter(
                file -> {
                  Tuple2<Long, Long> range = FileNames.logCompactionVersions(file.getPath());
                  return range._1 > checkpointVersion && range._2 <= newVersion;
                })
            .collect(Collectors.toList());
    final Optional<FileStatus> selectedChecksum =
        checksumFiles.stream()
            .filter(
                file -> {
                  long version = FileNames.checksumVersion(file.getPath());
                  return version >= checkpointVersion && version <= newVersion;
                })
            .max(Comparator.comparingLong(file -> FileNames.checksumVersion(file.getPath())));
    final Optional<Long> maxPublishedVersion =
        publishedDeltas.stream().map(ParsedPublishedDeltaData::getVersion).max(Long::compareTo);

    final LogSegment newlyListedSegment =
        new LogSegment(
            logPath,
            newVersion,
            deltasAfterCheckpoint.stream()
                .map(ParsedLogData::getFileStatus)
                .collect(Collectors.toList()),
            selectedCompactions,
            selectedCheckpointFiles,
            deltaAtEnd,
            selectedChecksum,
            maxPublishedVersion);

    if (checkpointVersion > baseVersion) {
      return Optional.of(newlyListedSegment);
    }
    return Optional.of(baseSegment.combineForIncrementalUpdate(newlyListedSegment, baseVersion));
  }

  private Optional<LogSegment> emptyListingResult(
      Optional<Long> effectiveTarget, long baseVersion) {
    if (effectiveTarget.isPresent()) {
      throw new InvalidTableException(
          tablePath.toString(),
          String.format(
              "Requested snapshot version %d is not available after base snapshot version %d",
              effectiveTarget.get(), baseVersion));
    }
    return Optional.empty();
  }

  private Optional<CheckpointInstance> latestCompleteCheckpoint(
      List<FileStatus> checkpointFiles, Optional<Long> effectiveTarget) {
    final List<CheckpointInstance> instances =
        checkpointFiles.stream()
            .map(file -> new CheckpointInstance(file.getPath()))
            .collect(Collectors.toList());
    final CheckpointInstance upperBound =
        effectiveTarget.map(CheckpointInstance::new).orElse(CheckpointInstance.MAX_VALUE);
    return Checkpointer.getLatestCompleteCheckpointFromList(instances, upperBound);
  }

  private List<FileStatus> selectCheckpointFiles(
      List<FileStatus> checkpointFiles, Optional<CheckpointInstance> checkpoint) {
    if (!checkpoint.isPresent()) {
      return Collections.emptyList();
    }
    final Set<Path> selectedPaths = new HashSet<>(checkpoint.get().getCorrespondingFiles(logPath));
    final List<FileStatus> selected =
        checkpointFiles.stream()
            .filter(file -> selectedPaths.contains(new Path(file.getPath())))
            .collect(Collectors.toList());
    if (selected.size() != selectedPaths.size()) {
      throw new IllegalStateException(
          String.format(
              "Checkpoint at version %d is incomplete: expected %d parts but found %d",
              checkpoint.get().version, selectedPaths.size(), selected.size()));
    }
    return selected;
  }

  private List<ParsedDeltaData> combineDeltasWithCatalogPriority(
      List<ParsedPublishedDeltaData> publishedDeltas,
      List<ParsedLogData> catalogLogData,
      long listingStart,
      long targetVersion) {
    final List<ParsedDeltaData> publishedInRange =
        publishedDeltas.stream()
            .filter(delta -> delta.getVersion() >= listingStart)
            .filter(delta -> delta.getVersion() <= targetVersion)
            .map(ParsedDeltaData.class::cast)
            .collect(Collectors.toList());
    final List<ParsedDeltaData> catalogInRange =
        catalogLogData.stream()
            .filter(data -> data instanceof ParsedCatalogCommitData && data.isFile())
            .filter(data -> data.getVersion() >= listingStart)
            .filter(data -> data.getVersion() <= targetVersion)
            .map(ParsedCatalogCommitData.class::cast)
            .collect(Collectors.toList());
    return LogDataUtils.combinePublishedAndRatifiedDeltasWithCatalogPriority(
        publishedInRange, catalogInRange);
  }

  private void validateContiguousTail(
      List<ParsedDeltaData> listedDeltas, long baseVersion, long newVersion) {
    final List<Long> newTailVersions =
        listedDeltas.stream()
            .map(ParsedDeltaData::getVersion)
            .filter(version -> version > baseVersion)
            .collect(Collectors.toList());
    if (newVersion == baseVersion || newTailVersions.isEmpty()) {
      return;
    }
    long expected = baseVersion + 1;
    for (long actual : newTailVersions) {
      if (actual != expected) {
        throw new InvalidTableException(
            tablePath.toString(),
            String.format(
                "Incremental log tail after base version %d is not contiguous: missing version "
                    + "%d before version %d",
                baseVersion, expected, actual));
      }
      expected++;
    }
    if (expected - 1 != newVersion) {
      throw new InvalidTableException(
          tablePath.toString(),
          String.format(
              "Incremental log tail after base version %d ends at %d instead of %d",
              baseVersion, expected - 1, newVersion));
    }
  }

  private static Optional<FileStatus> findDeltaAtVersion(
      List<? extends ParsedDeltaData> deltas, long version) {
    return deltas.stream()
        .filter(delta -> delta.getVersion() == version)
        .map(ParsedLogData::getFileStatus)
        .findFirst();
  }

  private static List<FileStatus> statusesFor(
      Map<Class<? extends ParsedLogData>, List<ParsedLogData>> partitioned,
      Class<? extends ParsedLogData> category) {
    return partitioned.getOrDefault(category, Collections.emptyList()).stream()
        .map(ParsedLogData::getFileStatus)
        .collect(Collectors.toCollection(ArrayList::new));
  }
}
