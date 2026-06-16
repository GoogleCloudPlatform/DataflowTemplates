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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import java.io.Serializable;
import javax.sql.DataSource;

/**
 * Interface for managing the lifecycle of {@link DataSource} instances within a Dataflow worker.
 *
 * <p>In a multi-shard migration, a single worker may need to interact with multiple physical
 * shards. This interface provides a centralized mechanism for:
 *
 * <ul>
 *   <li>On-demand initialization of {@link DataSource}s.
 *   <li>Caching of initialized instances to avoid redundant connection pool creation.
 *   <li>Coordinated shutdown of all active data sources during worker teardown.
 * </ul>
 */
public interface DataSourceManager extends Serializable {

  /**
   * Returns a {@link DataSource} for the specified identifier.
   *
   * <p>If the data source for the given ID has not been initialized yet, it will be created using
   * the configured provider and cached for subsequent calls.
   *
   * @param dataSourceId The unique identifier for the physical data source.
   * @return The initialized {@link DataSource} instance.
   */
  DataSource getDatasource(String dataSourceId);

  /**
   * Closes all active data source connections and releases associated resources.
   *
   * <p>This method should be called during the teardown phase of a Dataflow bundle or worker to
   * ensure that connection pools are gracefully shut down and no leaks occur.
   */
  void closeAll();
}
