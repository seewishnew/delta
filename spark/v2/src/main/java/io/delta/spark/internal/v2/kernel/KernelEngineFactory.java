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

package io.delta.spark.internal.v2.kernel;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;

/** Factory for creating the default Kernel {@link Engine} used by the DSv2 connector. */
public final class KernelEngineFactory {

  private KernelEngineFactory() {}

  /**
   * Runs {@code body} against a freshly created default engine and closes that
   * engine before returning, whether {@code body} succeeds or throws.
   *
   * <p>Use this for engine-backed work that completes within a single
   * operation. A Kernel {@link Engine} is a resource, so retaining one on a
   * long-lived object leaks it; scoping creation to the operation keeps
   * ownership with the caller that can also release it.
   *
   * <p>Note the engine must not escape {@code body}: it is closed on return,
   * so anything the caller keeps afterwards has to be independent of it.
   */
  public static <T> T withDefaultEngine(
      Configuration hadoopConf, Function<Engine, T> body) {
    Objects.requireNonNull(hadoopConf, "hadoopConf is null");
    Objects.requireNonNull(body, "body is null");
    Engine engine = createDefaultEngine(hadoopConf);
    Throwable failure = null;
    try {
      return body.apply(engine);
    } catch (RuntimeException | Error e) {
      failure = e;
      throw e;
    } finally {
      closeEngine(engine, failure);
    }
  }

  /**
   * {@link #withDefaultEngine} for work that produces no value.
   *
   * <p>Named differently rather than overloaded: a lambda body whose result
   * is discarded matches both {@code Function} and {@code Consumer}, which
   * the compiler cannot disambiguate.
   */
  public static void runWithDefaultEngine(
      Configuration hadoopConf, Consumer<Engine> body) {
    Objects.requireNonNull(body, "body is null");
    withDefaultEngine(
        hadoopConf,
        engine -> {
          body.accept(engine);
          return null;
        });
  }

  /**
   * Closes {@code engine}, attaching any close failure to
   * {@code primaryFailure} as a suppressed exception so the original error
   * still propagates. Pass {@code null} when no error is in flight.
   *
   * <p>OSS Engine does not expose close(); nothing to clean up.
   */
  public static void closeEngine(Engine engine, Throwable primaryFailure) {
    if (engine == null) {
      return;
    }
    // OSS Engine does not expose close(); nothing to clean up.
  }

  /**
   * Closes {@code engine} with no error in flight. Use the two-argument
   * form from a {@code catch}/{@code finally} that is already unwinding.
   */
  public static void closeEngine(Engine engine) {
    closeEngine(engine, null);
  }

  /** Builds the backend-appropriate default engine. */
  public static Engine createDefaultEngine(Configuration hadoopConf) {
    return DefaultEngine.create(hadoopConf);
  }
}
