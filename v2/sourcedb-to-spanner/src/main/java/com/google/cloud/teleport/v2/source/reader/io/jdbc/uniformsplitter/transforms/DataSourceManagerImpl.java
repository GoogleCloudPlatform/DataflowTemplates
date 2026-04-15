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

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.DataSourceProvider;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concrete implementation of {@link DataSourceManager} using {@link AutoValue}.
 *
 * <p>This class is responsible for the thread-safe initialization and caching of {@link DataSource}
 * instances within a Dataflow worker. By using a {@link DataSourceProvider}, it can dynamically
 * create connections to different physical shards as needed by the pipeline's processing logic.
 *
 * <p><b>Note on Future Enhancements for Multi-Shard Connectivity (MySQL & PostgreSQL):</b>
 *
 * <p>Currently, this implementation provides a baseline for managing multiple data sources. For
 * large-scale migrations involving hundreds or thousands of physical shards, the following
 * enhancements should be considered to optimize connectivity for MySQL and PostgreSQL:
 *
 * <ul>
 *   <li><b>Per-Physical Shard Connection Pooling:</b> Instead of a single shared pool or raw
 *       connections, the manager should be enhanced to maintain dedicated, properly sized
 *       connection pools for each distinct physical database instance. This prevents "noisy
 *       neighbor" scenarios where one slow shard exhausts the connection capacity for others.
 *   <li><b>Dialect-Specific Optimization:</b> Future iterations should allow for injecting
 *       dialect-specific connection properties. For example, MySQL might benefit from specific
 *       buffer sizes or SSL configurations, while PostgreSQL might require specific search paths or
 *       statement timeouts.
 *   <li><b>Global Connection Throttling:</b> To avoid overwhelming a single physical database
 *       instance when a Dataflow job scales to hundreds of workers, a mechanism for global
 *       (cross-worker) connection throttling or a centralized connection proxy (like Cloud SQL Auth
 *       Proxy or a custom middle tier) could be integrated into the lookup logic.
 *   <li><b>Dynamic Resource Rebalancing:</b> If some shards are significantly more active than
 *       others, the manager could implement a policy to dynamically shrink idle pools and expand
 *       active ones, respecting the total connection limit of the worker and the source DB.
 * </ul>
 */
@AutoValue
public abstract class DataSourceManagerImpl implements DataSourceManager {
  /**
   * Returns the provider used to create new {@link DataSource} instances.
   *
   * @return The data source provider.
   */
  public abstract DataSourceProvider dataSourceProvider();

  // DataSources are instance-local and handled per-bundle for thread safety.
  // These fields are transient as they are not meant to be serialized; they are re-initialized
  // or checked for nullity during the worker's bundle lifecycle.
  private final transient Map<String, DataSource> dataSources = new ConcurrentHashMap<>();
  private final transient Lock dataSourceLock = new ReentrantLock();
  private static final Logger LOG = LoggerFactory.getLogger(DataSourceManagerImpl.class);

  /**
   * Initializes the Datasource if needed and returns it.
   *
   * <p>This implementation ensures that only one {@link DataSource} is created per shard ID even in
   * a multi-threaded execution environment within the worker. It uses double-checked locking
   * pattern with {@link #dataSourceLock}.
   *
   * @param dataSourceId reference to the datasource.
   * @return The initialized {@link DataSource} for the shard.
   */
  @Override
  public DataSource getDatasource(String dataSourceId) {

    DataSource dataSource = this.dataSources.get(dataSourceId);
    if (dataSource == null) {
      // Note: dataSourceLock is transient. If this manager is used in a context
      // where it was serialized, re-initialization logic would be required.
      // Currently, it is expected to be used within a single worker's bundle lifecycle.
      dataSourceLock.lock();
      try {
        dataSource = this.dataSources.get(dataSourceId);
        if (dataSource == null) {
          dataSource = dataSourceProvider().getDataSource(dataSourceId);
          this.dataSources.put(dataSourceId, dataSource);
        }
      } finally {
        dataSourceLock.unlock();
      }
    }
    return dataSource;
  }

  /**
   * Closes all the active connection during teardown.
   *
   * <p>Iterates through all cached {@link DataSource} instances. If a data source implements {@link
   * Closeable}, it will be closed gracefully. Errors during closure are logged but not re-thrown to
   * avoid interrupting the teardown process.
   *
   * <p>Robustness: This method checks for nullity of {@code dataSourceLock} to safely handle cases
   * where it might be called on a deserialized instance.
   */
  @Override
  public void closeAll() {
    if (dataSourceLock == null) {
      return;
    }
    dataSourceLock.lock();
    try {
      for (var entry : dataSources.entrySet()) {
        DataSource dataSource = entry.getValue();
        if (dataSource instanceof Closeable) {
          try {
            ((Closeable) dataSource).close();
          } catch (IOException e) {
            LOG.warn("Failed to close DataSource {}", entry.getKey(), e);
          }
        }
      }
      dataSources.clear();
    } finally {
      dataSourceLock.unlock();
    }
  }

  /**
   * Returns a new builder for {@link DataSourceManagerImpl}.
   *
   * @return A new builder instance.
   */
  public static Builder builder() {
    return new AutoValue_DataSourceManagerImpl.Builder();
  }

  /** Builder for {@link DataSourceManagerImpl}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /**
     * Sets the {@link DataSourceProvider} for the manager.
     *
     * @param value The provider to set.
     * @return This builder instance.
     */
    public abstract Builder setDataSourceProvider(DataSourceProvider value);

    /**
     * Builds the {@link DataSourceManagerImpl} instance.
     *
     * @return The built instance.
     */
    public abstract DataSourceManagerImpl build();
  }
}
