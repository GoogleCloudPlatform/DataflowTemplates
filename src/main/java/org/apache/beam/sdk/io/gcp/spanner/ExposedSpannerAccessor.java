/*
 * Copyright (C) 2020 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Spanner;

/**
 * Class is ported from Apache Beam and reexposed.
 * Manages lifecycle of {@link DatabaseClient} and {@link Spanner} instances. */
public class ExposedSpannerAccessor implements AutoCloseable {

  public static ExposedSpannerAccessor create(SpannerConfig spannerConfig) {
    SpannerAccessor accessor = SpannerAccessor.create(spannerConfig);
    return new ExposedSpannerAccessor(accessor);
  }
  
  private final SpannerAccessor accessor;

  private ExposedSpannerAccessor(SpannerAccessor accessor) {
    this.accessor = accessor;
  }

  /** Returns Spanner client for Read/Write operations. */
  public DatabaseClient getDatabaseClient() {
    return accessor.getDatabaseClient();
  }

  /** Returns Spanner client for batch operations. */
  public BatchClient getBatchClient() {
    return accessor.getBatchClient();
  }

  /** Returns Spanner client for Database Administration. */
  public DatabaseAdminClient getDatabaseAdminClient() {
    return accessor.getDatabaseAdminClient();
  }

  @Override
  public void close() {
    accessor.close();
  }
}
