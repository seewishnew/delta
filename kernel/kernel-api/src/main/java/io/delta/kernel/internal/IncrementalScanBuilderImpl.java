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
package io.delta.kernel.internal;

import io.delta.kernel.IncrementalScan;
import io.delta.kernel.IncrementalScanBuilder;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelEngineException;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.skipping.DataSkippingPredicate;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructField;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Implementation of {@link IncrementalScanBuilder}. */
public final class IncrementalScanBuilderImpl implements IncrementalScanBuilder {
  private final SnapshotImpl targetSnapshot;
  private final long baseVersion;
  private Optional<Predicate> filter = Optional.empty();

  public IncrementalScanBuilderImpl(SnapshotImpl targetSnapshot, long baseVersion) {
    this.targetSnapshot = Objects.requireNonNull(targetSnapshot, "targetSnapshot is null");
    this.baseVersion = baseVersion;
  }

  @Override
  public IncrementalScanBuilder withFilter(Predicate predicate) {
    if (filter.isPresent()) {
      throw new IllegalArgumentException("There already exists a filter in current builder");
    }
    filter = Optional.of(Objects.requireNonNull(predicate, "predicate is null"));
    return this;
  }

  @Override
  public Optional<IncrementalScan> build(Engine engine) {
    Objects.requireNonNull(engine, "engine is null");
    long targetVersion = targetSnapshot.getVersion();
    if (baseVersion >= targetVersion) {
      throw new IllegalArgumentException(
          String.format(
              "Incremental scan base version (%d) must be less than target version (%d)",
              baseVersion, targetVersion));
    }

    ScanImpl.validateFilterReferences(filter, targetSnapshot.getSchema());
    TableFeatures.validateKernelCanReadTheTable(
        targetSnapshot.getProtocol(), targetSnapshot.getPath());
    Metadata metadata = targetSnapshot.getMetadata();
    Optional<Tuple2<Predicate, Predicate>> partitionAndDataFilters =
        ScanImpl.splitFilters(filter, metadata);
    Optional<Predicate> partitionFilter = ScanImpl.getPartitionFilters(partitionAndDataFilters);
    Optional<Predicate> remainingFilter = ScanImpl.getDataFilters(partitionAndDataFilters);
    Optional<DataSkippingPredicate> dataSkippingFilter =
        ScanImpl.getDataSkippingFilter(remainingFilter, metadata);
    Map<String, StructField> partitionColToStructFieldMap =
        metadata.getSchema().fields().stream()
            .filter(
                field ->
                    metadata
                        .getPartitionColNames()
                        .contains(field.getName().toLowerCase(Locale.ROOT)))
            .collect(
                Collectors.toMap(
                    field -> field.getName().toLowerCase(Locale.ROOT), Function.identity()));

    long startVersion = Math.addExact(baseVersion, 1);
    List<FileStatus> retainedDeltas = targetSnapshot.getLogSegment().getDeltas();
    if (retainedDeltas.isEmpty()
        || FileNames.deltaVersion(retainedDeltas.get(0).getPath()) > startVersion) {
      return Optional.empty();
    }

    List<FileStatus> selected = new ArrayList<>();
    for (FileStatus delta : retainedDeltas) {
      long version = FileNames.deltaVersion(delta.getPath());
      if (version >= startVersion && version <= targetVersion) {
        selected.add(delta);
      }
    }
    Collections.reverse(selected);

    final CloseableIterator<io.delta.kernel.data.ColumnarBatch> actions;
    try {
      actions =
          engine
              .getJsonHandler()
              .readJsonFiles(
                  Utils.toCloseableIterator(selected.iterator()),
                  LogReplay.getAddRemoveReadSchema(true),
                  Optional.empty());
    } catch (IOException e) {
      throw new KernelEngineException("read incremental Delta commit files", e);
    }
    try {
      return Optional.of(
          new IncrementalScanImpl(
              engine,
              targetSnapshot.getDataPath(),
              baseVersion,
              targetVersion,
              remainingFilter,
              partitionFilter,
              partitionColToStructFieldMap,
              dataSkippingFilter,
              metadata.getDataSchema(),
              actions));
    } catch (RuntimeException | Error failure) {
      try {
        actions.close();
      } catch (IOException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    }
  }
}
