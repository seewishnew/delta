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
import java.net.URI;
import java.util.Objects;
import java.util.Optional;

/**
 * Identity of an Add or Remove action: its raw path and optional deletion-vector id.
 *
 * @since 4.4.0
 */
@Evolving
public final class FileActionKey {
  private final URI path;
  private final Optional<String> deletionVectorId;

  public FileActionKey(URI path, Optional<String> deletionVectorId) {
    this.path = Objects.requireNonNull(path, "path is null");
    this.deletionVectorId = Objects.requireNonNull(deletionVectorId, "deletionVectorId is null");
  }

  public URI getPath() {
    return path;
  }

  public Optional<String> getDeletionVectorId() {
    return deletionVectorId;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof FileActionKey)) {
      return false;
    }
    FileActionKey that = (FileActionKey) other;
    return path.equals(that.path) && deletionVectorId.equals(that.deletionVectorId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, deletionVectorId);
  }

  @Override
  public String toString() {
    return "FileActionKey{path=" + path + ", deletionVectorId=" + deletionVectorId + '}';
  }
}
