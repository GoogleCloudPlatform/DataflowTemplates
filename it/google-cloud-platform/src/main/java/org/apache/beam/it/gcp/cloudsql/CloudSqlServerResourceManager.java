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

import com.google.auth.oauth2.GoogleCredentials;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom class for the Microsoft SQL Server implementation of {@link CloudSqlResourceManager}
 * abstract class.
 *
 * <p>The class supports one database, and multiple tables per database object. A database is
 * created when the resource manager first initializes, if one is not specified.
 *
 * <p>The class is thread-safe.
 */
public class CloudSqlServerResourceManager extends CloudSqlResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(CloudSqlServerResourceManager.class);

  public static final int DEFAULT_SQLSERVER_PORT = 1433;
  public static final String DEFAULT_SQLSERVER_USERNAME = "sqlserver";

  private CloudSqlServerResourceManager(Builder builder) {
    super(builder);
  }

  public static Builder builder(String testId) {
    return new Builder(testId);
  }

  @Override
  public @NonNull String getJDBCPrefix() {
    return "sqlserver";
  }

  @Override
  public synchronized @NonNull String getUri() {
    return String.format(
        "jdbc:%s://%s:%d%s;encrypt=false;trustServerCertificate=true;",
        getJDBCPrefix(),
        this.getHost(),
        this.getPort(),
        createdDatabase ? ";DatabaseName=" + this.getDatabaseName() : "");
  }

  @Override
  protected @NonNull String getFirstRow(@NonNull String tableName) {
    return "SELECT TOP 1 * FROM " + tableName;
  }

  @Override
  public void createDatabase(@NonNull String databaseName) {
    super.createDatabase(databaseName);
    LOG.info("Enabling CDC on database '{}'.", databaseName);
    try {
      runSQLUpdate(String.format("EXEC msdb.dbo.gcloudsql_cdc_enable_db '%s'", databaseName));
      LOG.info(
          "Successfully enabled CDC on database {} using gcloudsql_cdc_enable_db.", databaseName);
    } catch (Exception e) {
      LOG.warn(
          "gcloudsql_cdc_enable_db failed ({}), falling back to sys.sp_cdc_enable_db...",
          e.getMessage());
      runSQLUpdate(String.format("USE [%s]; EXEC sys.sp_cdc_enable_db;", databaseName));
      LOG.info("Successfully enabled CDC on database {} using sys.sp_cdc_enable_db.", databaseName);
    }
  }

  @Override
  public void dropDatabase(@NonNull String databaseName) {
    LOG.info("Dropping database using databaseName '{}'.", databaseName);
    this.createdDatabase = false;
    try {
      runSQLUpdate(
          String.format(
              "ALTER DATABASE [%s] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [%s]",
              databaseName, databaseName));
    } catch (Exception e) {
      LOG.warn(
          "Failed to set SINGLE_USER on [{}], attempting direct DROP: {}",
          databaseName,
          e.getMessage());
      runSQLUpdate(String.format("DROP DATABASE [%s]", databaseName));
    }
    LOG.info("Successfully dropped database {}", databaseName);
  }

  /** Builder for {@link CloudSqlServerResourceManager}. */
  public static final class Builder extends CloudSqlResourceManager.Builder {

    public Builder(String testId) {
      super(testId);
    }

    @Override
    protected String getDefaultUsername() {
      return DEFAULT_SQLSERVER_USERNAME;
    }

    @Override
    protected void configureHost() {
      if (System.getProperty("cloudSqlServerHost") != null) {
        this.setHost(System.getProperty("cloudSqlServerHost"));
      } else {
        super.configureHost();
      }
    }

    @Override
    protected void configurePort() {
      if (System.getProperty("cloudProxySqlServerPort") != null) {
        this.setPort(Integer.parseInt(System.getProperty("cloudProxySqlServerPort")));
      } else {
        LOG.warn("Missing -DcloudProxySqlServerPort.");
      }
    }

    @Override
    protected void configureUsername() {
      if (System.getProperty("cloudSqlServerUsername") != null) {
        this.setUsername(System.getProperty("cloudSqlServerUsername"));
      } else {
        LOG.info("-DcloudSqlServerUsername not specified, using default: " + getDefaultUsername());
        this.setUsername(getDefaultUsername());
      }
    }

    @Override
    public Builder maybeUseStaticInstance(String host, int port, String userName, String password) {
      super.maybeUseStaticInstance(host, port, userName, password);
      return this;
    }

    public Builder setProjectId(String projectId) {
      this.projectId = projectId;
      return this;
    }

    public Builder setRegion(String region) {
      this.region = region;
      return this;
    }

    public Builder setCredentials(GoogleCredentials credentials) {
      this.credentials = credentials;
      return this;
    }

    @Override
    public @NonNull CloudSqlServerResourceManager build() {
      return new CloudSqlServerResourceManager(this);
    }
  }
}
