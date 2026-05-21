/*
 * Copyright (C) 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter;

import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import javax.sql.DataSource;

/**
 * Interface for providing {@link DataSource} instances based on a shard identifier. This enables
 * dynamic routing of queries in a multi-shard environment.
 */
public interface DataSourceProvider extends Serializable {
  /**
   * Returns a {@link DataSource} for the given identifier.
   *
   * @param datasourceId The unique ID of the physical shard.
   * @return The DataSource for the shard.
   * @throws IllegalArgumentException if no DataSource is found for the given ID.
   */
  DataSource getDataSource(String datasourceId);

  /**
   * Returns a set of all DataSource identifiers managed by this provider.
   *
   * @return An immutable set of DataSource IDs.
   */
  ImmutableSet<String> getDataSourceIds();
}
