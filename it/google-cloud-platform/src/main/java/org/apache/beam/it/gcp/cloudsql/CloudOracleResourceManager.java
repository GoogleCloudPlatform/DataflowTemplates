/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.it.gcp.cloudsql;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom class for the Oracle implementation of {@link CloudSqlResourceManager} abstract class.
 *
 * <p>This class leverages the API in the {@link CloudSqlResourceManager}, but assumes a self-hosted
 * Oracle instance on GCE or other provider.
 *
 * <p>The class supports one database (XE), and multiple tables. The database must already exist to
 * use this resource manager.
 *
 * <p>The class is thread-safe.
 */
public class CloudOracleResourceManager extends CloudSqlResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(CloudOracleResourceManager.class);

  private static final int DEFAULT_ORACLE_PORT = 1521;

  private CloudOracleResourceManager(Builder builder) {
    super(builder);

    System.setProperty("oracle.jdbc.timezoneAsRegion", "false");
  }

  public static Builder builder(String testId) {
    return new Builder(testId);
  }

  @Override
  public @NonNull String getJDBCPrefix() {
    return "oracle";
  }

  @Override
  public synchronized @NonNull String getUri() {
    return String.format(
        "jdbc:%s:thin:@%s:%d:%s",
        getJDBCPrefix(), this.getHost(), this.getPort(getJDBCPort()), this.getDatabaseName());
  }

  @Override
  protected @NonNull String getFirstRow(@NonNull String tableName) {
    return "SELECT * FROM " + tableName + " WHERE ROWNUM <= 1";
  }

  /** Builder for {@link CloudOracleResourceManager}. */
  public static final class Builder extends CloudSqlResourceManager.Builder {

    public Builder(String testId) {
      super(testId);

      this.setDatabaseName("xe");
      this.setPort(DEFAULT_ORACLE_PORT);

      // Currently only supports static Oracle instance on GCE
      this.maybeUseStaticInstance();
    }

    public Builder maybeUseStaticInstance() {
      if (System.getProperty("cloudOracleHost") != null) {
        this.setHost(System.getProperty("cloudOracleHost"));
      } else {
        LOG.warn("Missing -DcloudOracleHost.");
      }
      this.useStaticContainer();

      return this;
    }

    @Override
    public @NonNull CloudOracleResourceManager build() {
      return new CloudOracleResourceManager(this);
    }
  }
}
