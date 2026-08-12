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
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/** Immutable summary of an incremental scan over {@code (baseVersion, targetVersion]}. */
@Evolving
public final class IncrementalScanSummary {
  private final long baseVersion;
  private final long targetVersion;
  private final Set<FileActionKey> liveAdds;
  private final Set<FileActionKey> removes;
  private final Set<FileActionKey> duplicateAdds;

  public IncrementalScanSummary(
      long baseVersion,
      long targetVersion,
      Set<FileActionKey> liveAdds,
      Set<FileActionKey> removes,
      Set<FileActionKey> duplicateAdds) {
    this.baseVersion = baseVersion;
    this.targetVersion = targetVersion;
    this.liveAdds = immutableCopy(liveAdds, "liveAdds");
    this.removes = immutableCopy(removes, "removes");
    this.duplicateAdds = immutableCopy(duplicateAdds, "duplicateAdds");
  }

  public long getBaseVersion() {
    return baseVersion;
  }

  public long getTargetVersion() {
    return targetVersion;
  }

  /**
   * Returns every dedup-selected newest Add identity.
   *
   * <p>With a filter, this set also includes Adds pruned from the streamed Add batches. Use the
   * streamed rows, rather than this identity set, as the rows appended during reconciliation.
   */
  public Set<FileActionKey> getLiveAdds() {
    return liveAdds;
  }

  /** Returns dedup-selected newest Remove identities. Removes are never filtered. */
  public Set<FileActionKey> getRemoves() {
    return removes;
  }

  /**
   * Returns newest Add identities that {@code baseContains} accepted in {@link
   * IncrementalScan#finishAgainstBase(java.util.function.Predicate)}.
   *
   * <p>These identities must be masked from the base together with {@link #getRemoves()} before
   * appending streamed Add rows.
   */
  public Set<FileActionKey> getDuplicateAdds() {
    return duplicateAdds;
  }

  private static Set<FileActionKey> immutableCopy(Set<FileActionKey> values, String name) {
    return Collections.unmodifiableSet(new HashSet<>(Objects.requireNonNull(values, name)));
  }
}
