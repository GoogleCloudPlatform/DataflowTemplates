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

import java.util.List;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom class for the Postgres implementation of {@link CloudSqlResourceManager} abstract class.
 *
 * <p>The class supports one database, and multiple tables per database object. A database is *
 * created when the container first spins up, if one is not given.
 *
 * <p>A schema will also be created with the same name as the DB, unless one is manually set in the
 * builder.
 *
 * <p>The class is thread-safe.
 */
public class CloudPostgresResourceManager extends CloudSqlResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(CloudPostgresResourceManager.class);

  private static final String DEFAULT_POSTGRES_USERNAME = "postgres";

  private String pgSchema;
  private boolean createdSchema;

  private CloudPostgresResourceManager(Builder builder) {
    super(builder);
    this.pgSchema = builder.schema;
    this.createdSchema = false;

    // Set schema set by builder or default to use same name as database
    if (this.pgSchema == null) {
      this.pgSchema = databaseName;
      LOG.info("Creating Postgres schema {}.", this.pgSchema);
      runSQLUpdate(String.format("CREATE SCHEMA IF NOT EXISTS %s", this.pgSchema));
      this.createdSchema = true;
    }
  }

  public static Builder builder(String testId) {
    return new Builder(testId);
  }

  public String getSchema() {
    return this.pgSchema;
  }

  public String getFullTableName(String tableName) {
    return String.format("%s.%s", pgSchema, tableName);
  }

  @Override
  public @NonNull String getJDBCPrefix() {
    return "postgresql";
  }

  @Override
  public boolean createTable(@NonNull String tableName, @NonNull JDBCSchema schema) {
    boolean status = super.createTable(tableName, schema);

    // Set table schema
    runSQLUpdate(String.format("ALTER TABLE %s SET SCHEMA %s", tableName, pgSchema));
    this.createdTables.remove(tableName);
    this.createdTables.add(tableName);

    return status;
  }

  @Override
  public void dropTable(@NonNull String tableName) {
    super.dropTable(getFullTableName(tableName));
  }

  @Override
  public boolean write(String tableName, List<Map<String, Object>> rows) {
    return super.write(getFullTableName(tableName), rows);
  }

  @Override
  public List<Map<String, Object>> readTable(String tableName) {
    return super.readTable(getFullTableName(tableName));
  }

  @Override
  public synchronized List<String> getTableSchema(String tableName) {
    return super.getTableSchema(getFullTableName(tableName));
  }

  @Override
  public synchronized long getRowCount(String tableName) {
    return super.getRowCount(getFullTableName(tableName));
  }

  @Override
  public void dropDatabase(@NonNull String databaseName) {
    LOG.info("Dropping database using databaseName '{}'.", databaseName);

    this.createdDatabase = false;
    runSQLUpdate(String.format("DROP DATABASE %s WITH (force)", databaseName));

    LOG.info("Successfully dropped database {}", databaseName);
  }

  @Override
  public void cleanupAll() {
    // Cleanup table schema if using a static DB with non-static tables
    if (this.usingCustomDb && this.createdSchema) {
      LOG.info("Attempting to drop Postgres schema {}.", this.pgSchema);
      try {
        runSQLUpdate(String.format("DROP SCHEMA %s CASCADE", this.pgSchema));
        LOG.info("Postgres schema successfully cleaned up.");
      } catch (Exception e) {
        throw new CloudSqlResourceManagerException("Failed to drop Postgres schema.", e);
      }
    } else {
      LOG.info("Not dropping pre-configured Postgres schema {}.", this.pgSchema);
    }
    super.cleanupAll();
  }

  /** Builder for {@link CloudPostgresResourceManager}. */
  public static final class Builder extends CloudSqlResourceManager.Builder {

    private String schema;

    public Builder(String testId) {
      super(testId);
    }

    /**
     * Set the table schema for all the tables created by the resource manager. This schema will not
     * be cleaned up, but tables created by this resource manager will be.
     *
     * @param schema name of schema to user for tables.
     * @return this builder.
     */
    public Builder setSchema(String schema) {
      this.schema = schema;
      return this;
    }

    @Override
    protected String getDefaultUsername() {
      return DEFAULT_POSTGRES_USERNAME;
    }

    @Override
    protected void configurePort() {
      if (System.getProperty("cloudProxyPostgresPort") != null) {
        this.setPort(Integer.parseInt(System.getProperty("cloudProxyPostgresPort")));
      } else {
        LOG.warn("Missing -DcloudProxyPostgresPort.");
      }
    }

    @Override
    public @NonNull CloudPostgresResourceManager build() {
      return new CloudPostgresResourceManager(this);
    }
  }
}
