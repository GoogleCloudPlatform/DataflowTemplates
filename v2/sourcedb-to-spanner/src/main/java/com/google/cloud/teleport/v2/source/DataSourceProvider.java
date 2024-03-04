
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSourceProvider
    implements org.apache.beam.sdk.transforms.SerializableFunction<Void, javax.sql.DataSource> {

  private static final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private static final Lock writeLock = readWriteLock.writeLock();
  private static final Lock readLock = readWriteLock.readLock();
  private static final Logger LOG = LoggerFactory.getLogger(DataSourceProvider.class);
  private static volatile javax.sql.DataSource dataSource = null;
  private final JdbcIO.DataSourceConfiguration config;

  public DataSourceProvider(SourceDbToSpannerOptions options) {
    config = getDataSourceConfiguration(options);
  }

  private static JdbcIO.DataSourceConfiguration getDataSourceConfiguration(
      SourceDbToSpannerOptions options) {
    var config = JdbcIO.DataSourceConfiguration.create(
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

  private javax.sql.DataSource initializeDataSourceIfRequired() {
    // Initialize the connection pool if required.
    javax.sql.DataSource ret = null;
    ret = getDataSource();
    if (ret == null) {
      ret = initializeDataSource();
    }
    return ret;
  }

  private javax.sql.DataSource initializeDataSource() {
    javax.sql.DataSource ret;
    writeLock.lock();
    try {
      // Check if it was already initialized till the write lock was acquired.
      if (dataSource == null) {
        dataSource = JdbcIO.PoolableDataSourceProvider.of(config).apply(null);
        LOG.debug("initializeDataSource dataSource" + dataSource.toString());
      }
      ret = dataSource;
    } finally {
      writeLock.unlock();
    }
    return ret;
  }

  private javax.sql.DataSource getDataSource() {
    javax.sql.DataSource ret = null;
    readLock.lock();
    try {
      ret = dataSource;
    } finally {
      readLock.unlock();
    }
    return ret;
  }

  @Override
  public javax.sql.DataSource apply(Void input) {
    return initializeDataSourceIfRequired();
  }
}
