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
package io.delta.kernel.exceptions;

import io.delta.kernel.annotation.Evolving;

/**
 * Exception thrown when Kernel cannot find a log file for the requested end version. This can
 * happen when the requested end version does not yet exist in the table.
 *
 * @since 4.1.0
 */
@Evolving
public class EndVersionNotFoundException extends KernelException {

  private final String tablePath;
  private final long endVersionRequested;
  private final long latestAvailableVersion;

  public EndVersionNotFoundException(
      String tablePath, long endVersionRequested, long latestAvailableVersion) {
    super(
        String.format(
            "%s: Requested table changes ending with endVersion=%d but no log file found for "
                + "version %d. Latest available version is %d",
            tablePath, endVersionRequested, endVersionRequested, latestAvailableVersion));
    this.tablePath = tablePath;
    this.endVersionRequested = endVersionRequested;
    this.latestAvailableVersion = latestAvailableVersion;
  }

  /** @return the table path where the end version was not found */
  public String getTablePath() {
    return tablePath;
  }

  /** @return the end version that was requested but not found */
  public long getEndVersionRequested() {
    return endVersionRequested;
  }

  /** @return the latest available version in the table */
  public long getLatestAvailableVersion() {
    return latestAvailableVersion;
  }
}
