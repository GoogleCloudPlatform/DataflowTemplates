/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.source;

import static com.google.cloud.teleport.v2.utils.KMSUtils.maybeDecrypt;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSourceProvider
    implements org.apache.beam.sdk.transforms.SerializableFunction<Void, DataSource> {

  private static final Logger LOG = LoggerFactory.getLogger(DataSourceProvider.class);
  private static volatile DataSource dataSource = null;
  // VarHandle provides a strongly typed reference to @dataSource
  // and supports various atomic access modes like the acquire-release semantics.
  private static final VarHandle DATA_SOURCE;
  private final JdbcIO.DataSourceConfiguration config;

  // Bind DATA_SOURCE to dataSource
  static {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      DATA_SOURCE =
          lookup
              .in(DataSourceProvider.class)
              .findStaticVarHandle(DataSourceProvider.class, "dataSource", DataSource.class);
    } catch (ReflectiveOperationException e) {
      // Logger might not be initialized in static scope!
      System.err.println("Error while binding VarHandle: " + e.toString());
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Constructs an instance of DataSourceProvider.
   *
   * @param options Pipeline options.
   */
  public DataSourceProvider(SourceDbToSpannerOptions options) {
    config = getDataSourceConfiguration(options);
  }

  /**
   * Returns a Singleton {@link DataSource} after initializing it if necessary.
   *
   * @see <a href = "https://en.wikipedia.org/wiki/Double-checked_locking#Usage_in_Java"> Idomatic
   *     implementation of Double checked locking pattern.</a>
   */
  @Override
  public DataSource apply(Void input) {
    DataSource localRef = getDataSourceAcquire();
    if (localRef == null) {
      synchronized (this) {
        localRef = getDataSourceAcquire();
        if (localRef == null) {
          localRef = JdbcIO.PoolableDataSourceProvider.of(config).apply(null);
          setDataSourceRelease(localRef);
          LOG.debug("initialize DataSource dataSource {}", localRef);
        }
      }
    }
    return localRef;
  }

  /**
   * Access {@link DataSourceProvider#dataSource} with <code>memory_order_acquire</code> memory
   * ordering semantics.
   *
   * <p>The load operation of {@link DataSourceProvider#dataSource} to the returned value
   * guarantees:
   *
   * <ol>
   *   <li>No reads or writes in the current thread can be reordered before this load.
   *   <li>All writes in other threads that release the same variable are visible in the current
   *       thread.
   * </ol>
   *
   * @see VarHandle
   * @see <a href="https://en.cppreference.com/w/cpp/atomic/memory_order#Release-Acquire_ordering">
   *     <code>Release-Acquire_ordering</code></a>
   */
  private DataSource getDataSourceAcquire() {
    return (DataSource) DATA_SOURCE.getAcquire();
  }

  /**
   * Set {@link DataSourceProvider#dataSource} with <code>memory_order_release</code> memory
   * ordering semantics.
   *
   * <p>The store operation to {@link DataSourceProvider#dataSource} with this memory ordering
   * guarantees:
   *
   * <ol>
   *   <li>No reads or writes in the current thread can be reordered after this store.
   *   <li>All writes in the current thread are visible in other threads that acquire the same
   *       variable.
   * </ol>
   *
   * @see VarHandle
   * @see <a href="https://en.cppreference.com/w/cpp/atomic/memory_order#Release-Acquire_ordering">
   *     <code>Release-Acquire_ordering</code></a>
   */
  private void setDataSourceRelease(DataSource value) {
    DATA_SOURCE.setRelease(value);
  }

  private static JdbcIO.DataSourceConfiguration getDataSourceConfiguration(
      SourceDbToSpannerOptions options) {
    var config =
        JdbcIO.DataSourceConfiguration.create(
                StaticValueProvider.of(options.getJdbcDriverClassName()),
                maybeDecrypt(options.getSourceConnectionURL(), null))
            .withUsername(maybeDecrypt(options.getUsername(), null))
            .withPassword(maybeDecrypt(options.getPassword(), null))
            .withMaxConnections(options.getMaxConnections());

    if (options.getSourceConnectionProperties() != null) {
      config = config.withConnectionProperties(options.getSourceConnectionProperties());
    }
    if (options.getJdbcDriverJars() != null) {
      config = config.withDriverJars(options.getJdbcDriverJars());
    }
    return config;
  }
}
