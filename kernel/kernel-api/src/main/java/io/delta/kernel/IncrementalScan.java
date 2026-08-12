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
package io.delta.kernel;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Optional;

/**
 * Resource-owning incremental file-action scan.
 *
 * <p>File actions are deduplicated newest-first by {@link FileActionKey}. An optional scan filter
 * affects only streamed Add rows; Remove identities are always retained for reconciliation.
 */
@Evolving
public interface IncrementalScan extends AutoCloseable {
  /**
   * Returns the deduplicated newest Add rows that survive the optional scan filter.
   *
   * <p>The returned iterator borrows this scan's resources. Call exactly once and consume it before
   * calling {@link #finish()} or {@link #finishAgainstBase(java.util.function.Predicate)}.
   */
  CloseableIterator<FilteredColumnarBatch> getLiveAddBatches();

  Optional<Predicate> getRemainingFilter();

  /**
   * Drains the scan and returns all deduplicated file-action identities.
   *
   * <p>The summary's live Adds include every dedup-selected newest Add identity, including Adds
   * omitted from {@link #getLiveAddBatches()} by the optional scan filter.
   */
  IncrementalScanSummary finish();

  /**
   * Drains the scan and classifies newest Add identities already present in a base result.
   *
   * <p>To reconcile a filtered base result, mask the union of {@link
   * IncrementalScanSummary#getRemoves()} and {@link IncrementalScanSummary#getDuplicateAdds()} from
   * the base, then append the Add rows streamed by {@link #getLiveAddBatches()}. Removes are
   * filter-independent. Without a filter, this preserves the same reconciliation behavior because
   * every newest Add is streamed.
   *
   * @param baseContains whether the filtered base result contains a file-action identity
   */
  IncrementalScanSummary finishAgainstBase(
      java.util.function.Predicate<FileActionKey> baseContains);

  @Override
  void close();
}
