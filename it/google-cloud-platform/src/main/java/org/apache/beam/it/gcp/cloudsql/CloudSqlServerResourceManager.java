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

  private static final int CDC_ENABLE_ATTEMPTS = 5;
  private static final long CDC_ENABLE_BASE_BACKOFF_MILLIS = 2000L;

  /**
   * Serializes CDC enablement across all resource managers in the JVM. The first {@code
   * sp_cdc_enable_table} in a database creates the capture/cleanup jobs in {@code msdb}; when
   * several test databases on the same instance do this concurrently, the {@code sp_add_job} calls
   * deadlock with each other (SQL Server error 1205).
   */
  private static final Object CDC_ENABLE_LOCK = new Object();

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
    } catch (Exception e) {
      LOG.warn(
          "gcloudsql_cdc_enable_db failed ({}), falling back to sys.sp_cdc_enable_db...",
          e.getMessage());
      runSQLUpdate(
          String.format(
              "ALTER DATABASE [%s] SET ALLOW_SNAPSHOT_ISOLATION ON; "
                  + "USE [%s]; EXEC sys.sp_cdc_enable_db;",
              databaseName, databaseName));
    }
    LOG.info("Successfully enabled CDC on database {}", databaseName);
  }

  /**
   * Enables CDC on every user table in the current database that is not yet tracked by CDC, and
   * ensures the CDC capture job is running.
   *
   * <p>Note: {@code @supports_net_changes = 1} requires the table to have a primary key or a unique
   * index. Tables without one will make {@code sp_cdc_enable_table} fail and abort the remaining
   * tables in the batch; use {@link #enableCdcOnTable(String, String, String)} for those.
   */
  public void enableCdcOnAllTables() {
    String enableCdcQuery =
        "DECLARE @schema_name SYSNAME; "
            + "DECLARE @table_name SYSNAME; "
            + "DECLARE table_cursor CURSOR FOR "
            + "SELECT SCHEMA_NAME(schema_id), name FROM sys.tables"
            + " WHERE is_ms_shipped = 0 AND is_tracked_by_cdc = 0; "
            + "OPEN table_cursor; "
            + "FETCH NEXT FROM table_cursor INTO @schema_name, @table_name; "
            + "WHILE @@FETCH_STATUS = 0 "
            + "BEGIN "
            + "    EXEC sys.sp_cdc_enable_table @source_schema = @schema_name,"
            + " @source_name = @table_name, @role_name = NULL, @supports_net_changes = 1; "
            + "    FETCH NEXT FROM table_cursor INTO @schema_name, @table_name; "
            + "END; "
            + "CLOSE table_cursor; "
            + "DEALLOCATE table_cursor;";
    // The cursor only visits tables that are not yet tracked, so re-running the batch after a
    // deadlock resumes with the tables that were not enabled yet.
    runCdcUpdateWithDeadlockRetry(enableCdcQuery);
    ensureCaptureJobRunning();
  }

  /**
   * Enables CDC on a single table with net-changes support, optionally naming the unique index to
   * use as the net-changes key for tables without a primary key.
   *
   * @param schemaName SQL Server schema of the table, e.g. {@code dbo}.
   * @param tableName name of the table.
   * @param uniqueIndexName unique index to use as the net-changes key, or {@code null} to use the
   *     primary key.
   */
  public void enableCdcOnTable(
      @NonNull String schemaName, @NonNull String tableName, String uniqueIndexName) {
    String indexClause =
        uniqueIndexName == null ? "" : String.format(", @index_name = N'%s'", uniqueIndexName);
    runCdcUpdateWithDeadlockRetry(
        String.format(
            "IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'%s'"
                + " AND SCHEMA_NAME(schema_id) = N'%s' AND is_tracked_by_cdc = 1) "
                + "EXEC sys.sp_cdc_enable_table @source_schema = N'%s', @source_name = N'%s',"
                + " @role_name = NULL, @supports_net_changes = 1%s;",
            tableName, schemaName, schemaName, tableName, indexClause));
    ensureCaptureJobRunning();
  }

  /**
   * Runs a CDC-enabling statement while holding {@link #CDC_ENABLE_LOCK}, and retries when SQL
   * Server still picks it as a deadlock victim (error 1205), e.g. because another JVM is enabling
   * CDC on the same instance.
   */
  private void runCdcUpdateWithDeadlockRetry(String sql) {
    synchronized (CDC_ENABLE_LOCK) {
      RuntimeException lastFailure = null;
      for (int attempt = 1; attempt <= CDC_ENABLE_ATTEMPTS; attempt++) {
        try {
          runSQLUpdate(sql);
          return;
        } catch (RuntimeException e) {
          if (!isDeadlock(e)) {
            throw e;
          }
          lastFailure = e;
          long backoffMillis = CDC_ENABLE_BASE_BACKOFF_MILLIS * attempt;
          LOG.warn(
              "Enabling CDC was chosen as a deadlock victim (attempt {}/{}), retrying in {} ms.",
              attempt,
              CDC_ENABLE_ATTEMPTS,
              backoffMillis);
          try {
            Thread.sleep(backoffMillis);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw e;
          }
        }
      }
      throw lastFailure;
    }
  }

  private static boolean isDeadlock(Throwable t) {
    for (Throwable cause = t; cause != null; cause = cause.getCause()) {
      String message = cause.getMessage();
      if (message != null && (message.contains("1205") || message.contains("deadlock"))) {
        return true;
      }
    }
    return false;
  }

  /** Starts the CDC capture job of the current database if it exists and is not running. */
  private void ensureCaptureJobRunning() {
    try {
      String startJobQuery =
          "DECLARE @job_id UNIQUEIDENTIFIER; "
              + "SELECT @job_id = job_id FROM msdb.dbo.cdc_jobs"
              + " WHERE database_id = DB_ID() AND job_type = N'capture'; "
              + "IF @job_id IS NOT NULL AND NOT EXISTS ( "
              + "    SELECT 1 FROM msdb.dbo.sysjobactivity "
              + "    WHERE job_id = @job_id "
              + "      AND session_id = (SELECT MAX(session_id) FROM msdb.dbo.syssessions) "
              + "      AND start_execution_date IS NOT NULL "
              + "      AND stop_execution_date IS NULL "
              + ") "
              + "BEGIN "
              + "    EXEC sys.sp_cdc_start_job @job_type = N'capture'; "
              + "END;";
      runSQLUpdate(startJobQuery);
    } catch (Exception e) {
      LOG.info(
          "Capture job start check encountered an issue (job is likely already running): {}",
          e.getMessage());
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
      if (System.getProperty("cloudProxySqlServerUsername") != null) {
        this.setUsername(System.getProperty("cloudProxySqlServerUsername"));
      } else {
        LOG.info(
            "-DcloudProxySqlServerUsername not specified, using default: " + getDefaultUsername());
        this.setUsername(getDefaultUsername());
      }
    }

    @Override
    public Builder maybeUseStaticInstance(String host, int port, String userName, String password) {
      super.maybeUseStaticInstance(host, port, userName, password);
      return this;
    }

    @Override
    public @NonNull CloudSqlServerResourceManager build() {
      return new CloudSqlServerResourceManager(this);
    }
  }
}
