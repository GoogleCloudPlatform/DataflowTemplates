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

import com.google.common.annotations.VisibleForTesting;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private static final int REPLICATION_SETUP_LOCK_ID = 12345;

  private String pgSchema;
  private boolean createdSchema;
  private final Set<String> createdReplicationSlots;
  private final Set<String> createdPublications;

  CloudPostgresResourceManager(Builder builder) {
    super(builder);
    this.pgSchema = builder.schema;
    this.createdSchema = false;
    this.createdReplicationSlots = Collections.synchronizedSet(new HashSet<>());
    this.createdPublications = Collections.synchronizedSet(new HashSet<>());

    // Set schema set by builder or default to use same name as database
    if (this.pgSchema == null) {
      this.pgSchema = databaseName;
      LOG.debug("Creating Postgres schema {}.", this.pgSchema);
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
    LOG.debug("Dropping database using databaseName '{}'.", databaseName);

    this.createdDatabase = false;
    runSQLUpdate(String.format("DROP DATABASE %s WITH (force)", databaseName));

    LOG.debug("Successfully dropped database {}", databaseName);
  }

  /**
   * Creates a logical replication slot and publication for the given tables.
   *
   * @param tableNames List of table names to include in the publication. If empty or null,
   *     publication is created for all tables.
   * @return ReplicationInfo containing the names of the created slot and publication.
   */
  public ReplicationInfo createLogicalReplication(List<String> tableNames) {
    // Generate unique names
    // Postgres max identifier length is 63
    // Pattern: prefix + "_" + date + "_" + randomUUID (no hyphens)
    String dateId =
        DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(java.time.LocalDateTime.now());
    String uuid = java.util.UUID.randomUUID().toString().replace("-", "");

    String pubPrefix = "pub";
    String slotPrefix = "slot";

    String publicationName = String.format("%s_%s_%s", pubPrefix, dateId, uuid);
    String replicationSlotName = String.format("%s_%s_%s", slotPrefix, dateId, uuid);

    // Ensure they are lowercase
    publicationName = publicationName.toLowerCase();
    replicationSlotName = replicationSlotName.toLowerCase();

    try (Connection connection = getConnection()) {
      connection.setAutoCommit(false);
      try (Statement statement = connection.createStatement()) {
        // Acquire a transaction-level advisory lock to ensure that only one thread/process
        // can perform replication setup at a time. This is necessary to prevent
        // "tuple concurrently updated" errors that can occur when multiple concurrent
        // sessions try to ALTER the same user or create/drop replication slots/publications.
        // The lock is automatically released when the transaction commits or rolls back.
        LOG.debug("Acquiring lock {} for replication setup...", REPLICATION_SETUP_LOCK_ID);
        statement.execute("SELECT pg_advisory_xact_lock(" + REPLICATION_SETUP_LOCK_ID + ")");

        LOG.debug("Granting replication privilege to current user...");
        statement.execute("ALTER USER CURRENT_USER WITH REPLICATION");

        LOG.debug("Creating replication slot {}.", replicationSlotName);
        statement.execute(
            String.format(
                "SELECT pg_create_logical_replication_slot('%s', 'pgoutput')",
                replicationSlotName));
        createdReplicationSlots.add(replicationSlotName);

        if (tableNames == null || tableNames.isEmpty()) {
          LOG.debug("Creating publication {} for all tables.", publicationName);
          statement.execute(String.format("CREATE PUBLICATION %s FOR ALL TABLES", publicationName));
        } else {
          LOG.debug("Creating publication {} for tables {}.", publicationName, tableNames);
          String tables = String.join(", ", tableNames);
          statement.execute(
              String.format("CREATE PUBLICATION %s FOR TABLE %s", publicationName, tables));
        }
        createdPublications.add(publicationName);

        connection.commit();
        LOG.debug("Successfully created replication resources.");
      } catch (Exception e) {
        connection.rollback();
        throw e;
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to create logical replication resources.", e);
    }

    return new ReplicationInfo(replicationSlotName, publicationName);
  }

  /**
   * Creates a logical replication slot and publication for all tables.
   *
   * @return ReplicationInfo containing the names of the created slot and publication.
   */
  public ReplicationInfo createLogicalReplication() {
    return createLogicalReplication(Collections.emptyList());
  }

  private void dropReplicationSlot(String slotName) {
    LOG.debug("Dropping replication slot {}.", slotName);
    try (Connection connection = getConnection()) {
      connection.setAutoCommit(false);
      try (Statement statement = connection.createStatement()) {
        // Acquire a transaction-level advisory lock to serialize the dropping of replication slots.
        // This prevents concurrency issues when multiple tests or processes attempt to clean up
        // replication resources simultaneously.
        LOG.debug("Acquiring lock {} for dropping replication slot...", REPLICATION_SETUP_LOCK_ID);
        statement.execute("SELECT pg_advisory_xact_lock(" + REPLICATION_SETUP_LOCK_ID + ")");

        statement.execute(String.format("SELECT pg_drop_replication_slot('%s')", slotName));
        connection.commit();
        LOG.debug("Successfully dropped replication slot {}.", slotName);
      } catch (Exception e) {
        connection.rollback();
        throw e;
      }
    } catch (Exception e) {
      LOG.warn("Failed to drop replication slot {}.", slotName, e);
    }
  }

  private void dropPublication(String publicationName) {
    LOG.debug("Dropping publication {}.", publicationName);
    try (Connection connection = getConnection()) {
      connection.setAutoCommit(false);
      try (Statement statement = connection.createStatement()) {
        // Acquire a transaction-level advisory lock to serialize the dropping of publications.
        // This prevents concurrency issues when multiple tests or processes attempt to clean up
        // replication resources simultaneously.
        LOG.debug("Acquiring lock {} for dropping publication...", REPLICATION_SETUP_LOCK_ID);
        statement.execute("SELECT pg_advisory_xact_lock(" + REPLICATION_SETUP_LOCK_ID + ")");

        statement.execute(String.format("DROP PUBLICATION IF EXISTS %s", publicationName));
        connection.commit();
        LOG.debug("Successfully dropped publication {}.", publicationName);
      } catch (Exception e) {
        connection.rollback();
        throw e;
      }
    } catch (Exception e) {
      LOG.warn("Failed to drop publication {}.", publicationName, e);
    }
  }

  @Override
  public void cleanupAll() {
    for (String publication : createdPublications) {
      dropPublication(publication);
    }
    createdPublications.clear();

    for (String slot : createdReplicationSlots) {
      dropReplicationSlot(slot);
    }
    createdReplicationSlots.clear();
    // Cleanup table schema if using a static DB with non-static tables
    if (this.usingCustomDb && this.createdSchema) {
      LOG.debug("Attempting to drop Postgres schema {}.", this.pgSchema);
      try {
        runSQLUpdate(String.format("DROP SCHEMA %s CASCADE", this.pgSchema));
        LOG.debug("Postgres schema successfully cleaned up.");
      } catch (Exception e) {
        throw new CloudSqlResourceManagerException("Failed to drop Postgres schema.", e);
      }
    } else {
      LOG.debug("Not dropping pre-configured Postgres schema {}.", this.pgSchema);
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

  /** Record to hold replication slot and publication names. */
  public static class ReplicationInfo {
    private final String replicationSlotName;
    private final String publicationName;

    public ReplicationInfo(String replicationSlotName, String publicationName) {
      this.replicationSlotName = replicationSlotName;
      this.publicationName = publicationName;
    }

    public String getReplicationSlotName() {
      return replicationSlotName;
    }

    public String getPublicationName() {
      return publicationName;
    }
  }

  @VisibleForTesting
  Connection getConnection() throws java.sql.SQLException {
    return DriverManager.getConnection(getUri(), getUsername(), getPassword());
  }
}
