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

import static io.delta.kernel.internal.DeltaErrors.wrapEngineException;
import static io.delta.kernel.internal.replay.LogReplay.ADD_FILE_DV_ORDINAL;
import static io.delta.kernel.internal.replay.LogReplay.ADD_FILE_ORDINAL;
import static io.delta.kernel.internal.replay.LogReplay.ADD_FILE_PATH_ORDINAL;
import static io.delta.kernel.internal.replay.LogReplay.REMOVE_FILE_DV_ORDINAL;
import static io.delta.kernel.internal.replay.LogReplay.REMOVE_FILE_ORDINAL;
import static io.delta.kernel.internal.replay.LogReplay.REMOVE_FILE_PATH_ORDINAL;

import io.delta.kernel.FileActionKey;
import io.delta.kernel.IncrementalScan;
import io.delta.kernel.IncrementalScanSummary;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.LogReplayUtils;
import io.delta.kernel.internal.skipping.DataSkippingPredicate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Streaming implementation of {@link IncrementalScan}. */
public final class IncrementalScanImpl implements IncrementalScan {
  private final long baseVersion;
  private final long targetVersion;
  private final Optional<Predicate> remainingFilter;
  private final Set<FileActionKey> seen = new HashSet<>();
  private final Set<FileActionKey> liveAdds = new HashSet<>();
  private final Set<FileActionKey> removes = new HashSet<>();
  private final CloseableIterator<FilteredColumnarBatch> filteredAdds;

  private boolean batchesBorrowed;
  private boolean terminal;
  private boolean closed;
  private RuntimeException failure;

  public IncrementalScanImpl(
      Engine engine,
      Path tableRoot,
      long baseVersion,
      long targetVersion,
      Optional<Predicate> remainingFilter,
      Optional<Predicate> partitionFilter,
      Map<String, StructField> partitionColToStructFieldMap,
      Optional<DataSkippingPredicate> dataSkippingFilter,
      StructType dataSchema,
      CloseableIterator<ColumnarBatch> actions) {
    this.baseVersion = baseVersion;
    this.targetVersion = targetVersion;
    this.remainingFilter = remainingFilter;

    CloseableIterator<FilteredColumnarBatch> filtered =
        deduplicateActions(engine, tableRoot, actions);
    filtered =
        ScanImpl.applyPartitionPruning(
            engine, filtered, partitionFilter, partitionColToStructFieldMap);
    if (dataSkippingFilter.isPresent()) {
      filtered = ScanImpl.applyDataSkipping(engine, filtered, dataSkippingFilter.get(), dataSchema);
    }
    this.filteredAdds = filtered;
  }

  @Override
  public synchronized CloseableIterator<FilteredColumnarBatch> getLiveAddBatches() {
    ensureActive();
    if (batchesBorrowed) {
      throw new IllegalStateException("Live Add batches have already been fetched");
    }
    batchesBorrowed = true;
    return new CloseableIterator<FilteredColumnarBatch>() {
      @Override
      public boolean hasNext() {
        return hasNextLiveAdd();
      }

      @Override
      public FilteredColumnarBatch next() {
        return nextLiveAdd();
      }

      @Override
      public void close() {
        // Borrowed facade: IncrementalScan owns and closes the JSON iterator.
      }
    };
  }

  @Override
  public Optional<Predicate> getRemainingFilter() {
    return remainingFilter;
  }

  @Override
  public synchronized IncrementalScanSummary finish() {
    return finishInternal(Optional.empty());
  }

  @Override
  public synchronized IncrementalScanSummary finishAgainstBase(
      java.util.function.Predicate<FileActionKey> baseContains) {
    return finishInternal(Optional.of(Objects.requireNonNull(baseContains, "baseContains")));
  }

  @Override
  public synchronized void close() {
    if (terminal || closed) {
      return;
    }
    terminal = true;
    closeOwnedIterator();
  }

  private synchronized boolean hasNextLiveAdd() {
    ensureActive();
    try {
      return filteredAdds.hasNext();
    } catch (RuntimeException e) {
      throw poison(e);
    }
  }

  private synchronized FilteredColumnarBatch nextLiveAdd() {
    ensureActive();
    try {
      if (!filteredAdds.hasNext()) {
        throw new NoSuchElementException();
      }
      return filteredAdds.next();
    } catch (RuntimeException e) {
      throw poison(e);
    }
  }

  private IncrementalScanSummary finishInternal(
      Optional<java.util.function.Predicate<FileActionKey>> baseContains) {
    ensureActive();
    try {
      while (filteredAdds.hasNext()) {
        filteredAdds.next();
      }
      Set<FileActionKey> duplicateAdds = new HashSet<>();
      if (baseContains.isPresent()) {
        for (FileActionKey key : liveAdds) {
          if (baseContains.get().test(key)) {
            duplicateAdds.add(key);
          }
        }
      }
      terminal = true;
      closeOwnedIterator();
      return new IncrementalScanSummary(
          baseVersion, targetVersion, liveAdds, removes, duplicateAdds);
    } catch (RuntimeException e) {
      throw poison(e);
    }
  }

  private CloseableIterator<FilteredColumnarBatch> deduplicateActions(
      Engine engine, Path tableRoot, CloseableIterator<ColumnarBatch> actions) {
    return new CloseableIterator<FilteredColumnarBatch>() {
      private Optional<FilteredColumnarBatch> next = Optional.empty();
      private ExpressionEvaluator tableRootGenerator;

      @Override
      public boolean hasNext() {
        prepareNext();
        return next.isPresent();
      }

      @Override
      public FilteredColumnarBatch next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        FilteredColumnarBatch result = next.get();
        next = Optional.empty();
        return result;
      }

      @Override
      public void close() throws IOException {
        try {
          if (tableRootGenerator != null) {
            try {
              tableRootGenerator.close();
            } catch (Exception e) {
              throw new IOException("Failed to close table-root evaluator", e);
            }
          }
        } finally {
          actions.close();
        }
      }

      private void prepareNext() {
        while (!next.isPresent() && actions.hasNext()) {
          ColumnarBatch actionBatch = actions.next();
          ColumnVector addVector = actionBatch.getColumnVector(ADD_FILE_ORDINAL);
          ColumnVector removeVector = actionBatch.getColumnVector(REMOVE_FILE_ORDINAL);
          boolean[] selected = new boolean[actionBatch.getSize()];
          int selectedCount = 0;

          for (int rowId = 0; rowId < actionBatch.getSize(); rowId++) {
            if (!addVector.isNullAt(rowId)) {
              FileActionKey key =
                  LogReplayUtils.getFileActionKey(
                      addVector.getChild(ADD_FILE_PATH_ORDINAL),
                      addVector.getChild(ADD_FILE_DV_ORDINAL),
                      rowId);
              if (seen.add(key)) {
                liveAdds.add(key);
                selected[rowId] = true;
                selectedCount++;
              }
            } else if (!removeVector.isNullAt(rowId)) {
              FileActionKey key =
                  LogReplayUtils.getFileActionKey(
                      removeVector.getChild(REMOVE_FILE_PATH_ORDINAL),
                      removeVector.getChild(REMOVE_FILE_DV_ORDINAL),
                      rowId);
              if (seen.add(key)) {
                removes.add(key);
              }
            }
          }

          if (selectedCount == 0) {
            continue;
          }

          ColumnarBatch addBatch = actionBatch.withDeletedColumnAt(REMOVE_FILE_ORDINAL);
          if (tableRootGenerator == null) {
            final ColumnarBatch inputBatch = addBatch;
            tableRootGenerator =
                wrapEngineException(
                    () ->
                        engine
                            .getExpressionHandler()
                            .getEvaluator(
                                inputBatch.getSchema(),
                                Literal.ofString(tableRoot.toUri().toString()),
                                StringType.STRING),
                    "Get the expression evaluator for the table root");
          }
          final ColumnarBatch inputBatch = addBatch;
          ColumnVector tableRootVector =
              wrapEngineException(
                  () -> tableRootGenerator.eval(inputBatch),
                  "Evaluating the table root expression");
          addBatch =
              addBatch.withNewColumn(
                  1, InternalScanFileUtils.TABLE_ROOT_STRUCT_FIELD, tableRootVector);
          Optional<ColumnVector> selection =
              selectedCount == addBatch.getSize()
                  ? Optional.empty()
                  : Optional.of(
                      wrapEngineException(
                          () ->
                              engine
                                  .getExpressionHandler()
                                  .createSelectionVector(selected, 0, selected.length),
                          "Create selection vector for incremental Add files"));
          next = Optional.of(new FilteredColumnarBatch(addBatch, selection));
        }
      }
    };
  }

  private RuntimeException poison(RuntimeException error) {
    if (failure == null) {
      failure = error;
      closeOwnedIteratorSilently();
    }
    return error;
  }

  private void ensureActive() {
    if (failure != null) {
      throw new IllegalStateException("Incremental scan previously failed", failure);
    }
    if (terminal || closed) {
      throw new IllegalStateException("Incremental scan is already terminal");
    }
  }

  private void closeOwnedIterator() {
    if (closed) {
      return;
    }
    closed = true;
    try {
      filteredAdds.close();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close incremental scan", e);
    }
  }

  private void closeOwnedIteratorSilently() {
    if (!closed) {
      closed = true;
      Utils.closeCloseablesSilently(filteredAdds);
    }
  }
}
