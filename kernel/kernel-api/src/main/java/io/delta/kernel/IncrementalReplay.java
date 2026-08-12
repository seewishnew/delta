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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.annotation.Experimental;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * Bounds the number of commits Kernel may replay during snapshot construction to advance a stale
 * checksum to the target version.
 *
 * <p>A checksum already at the target version is used regardless of this policy. {@link
 * #disabled()} and {@code upToCommits(0)} are equivalent. This policy does not govern later,
 * explicit statistics computation.
 */
@Experimental
public final class IncrementalReplay {
  private static final IncrementalReplay DISABLED = new IncrementalReplay(OptionalLong.of(0L));
  private static final IncrementalReplay UNLIMITED = new IncrementalReplay(OptionalLong.empty());

  private final OptionalLong maxCommits;

  private IncrementalReplay(OptionalLong maxCommits) {
    this.maxCommits = maxCommits;
  }

  /** Never advances a stale checksum during snapshot construction. */
  public static IncrementalReplay disabled() {
    return DISABLED;
  }

  /**
   * Advances a stale checksum during construction only when within {@code maxCommits} of target.
   */
  public static IncrementalReplay upToCommits(long maxCommits) {
    checkArgument(maxCommits >= 0, "maxCommits must be >= 0");
    return maxCommits == 0 ? DISABLED : new IncrementalReplay(OptionalLong.of(maxCommits));
  }

  /** Advances a stale checksum during construction regardless of its distance from the target. */
  public static IncrementalReplay unlimited() {
    return UNLIMITED;
  }

  /** Returns whether a checksum may be advanced to {@code targetVersion} under this policy. */
  public boolean allowsAdvancing(long crcVersion, long targetVersion) {
    if (crcVersion == targetVersion) {
      return true;
    }
    return crcVersion < targetVersion
        && (!maxCommits.isPresent() || targetVersion - crcVersion <= maxCommits.getAsLong());
  }

  @Override
  public boolean equals(Object other) {
    return this == other
        || (other instanceof IncrementalReplay
            && maxCommits.equals(((IncrementalReplay) other).maxCommits));
  }

  @Override
  public int hashCode() {
    return Objects.hash(maxCommits);
  }

  @Override
  public String toString() {
    return maxCommits.isPresent()
        ? String.format("IncrementalReplay(upToCommits=%d)", maxCommits.getAsLong())
        : "IncrementalReplay(unlimited)";
  }
}
