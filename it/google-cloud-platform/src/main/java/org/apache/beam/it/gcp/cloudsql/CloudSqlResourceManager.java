package org.apache.beam.it.gcp.cloudsql;

import static org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManagerUtils.generateDatabaseName;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.it.jdbc.AbstractJDBCResourceManager;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class that extends {@link AbstractJDBCResourceManager} for CloudSQL resource management.
 *
 * <p>The class supports one database, and multiple tables per database object. A database is
 * created when the resource manager first spins up, if one is not specified.
 *
 * <p>The database name is formed using testId. The database name will be "{testId}-{ISO8601 time,
 * microsecond precision}", with additional formatting.
 *
 * <p>The class is thread-safe.
 */
public abstract class CloudSqlResourceManager
    extends AbstractJDBCResourceManager<@NonNull CloudSqlContainer<?>> {
  private static final Logger LOG = LoggerFactory.getLogger(CloudSqlResourceManager.class);

  protected final List<String> createdTables;
  private boolean createdDatabase;
  private boolean usingCustomDb;

  protected CloudSqlResourceManager(@NonNull Builder builder) {
    super(CloudSqlContainer.of(), builder);

    this.createdTables = new ArrayList<>();

    this.createdDatabase = false;
    this.usingCustomDb = builder.usingCustomDb;
    if (!usingCustomDb) {
      createDatabase(builder.dbName);
    }
    this.createdDatabase = true;
  }

  @Override
  public synchronized @NonNull String getUri() {
    return String.format(
        "jdbc:%s://%s:%d/%s",
        getJDBCPrefix(),
        this.getHost(),
        this.getPort(),
        createdDatabase ? this.getDatabaseName() : "");
  }

  @Override
  protected int getJDBCPort() {
    return this.port;
  }

  @Override
  public int getPort() {
    return this.getPort(getJDBCPort());
  }

  @Override
  public boolean createTable(@NotNull String tableName, @NotNull JDBCSchema schema) {
    boolean status = super.createTable(tableName, schema);
    this.createdTables.add(tableName);
    return status;
  }

  /**
   * Drops the table with the given name from the current database.
   *
   * @param tableName The name of the table.
   * @return true, if successful.
   */
  public void dropTable(@NotNull String tableName) {
    LOG.info("Dropping table using tableName '{}'.", tableName);

    runSQLUpdate(String.format("DROP TABLE %s", tableName));

    createdTables.remove(tableName);
    LOG.info("Successfully dropped table {}.{}", databaseName, tableName);
  }

  /**
   * Creates a database in the CloudSQL instance.
   *
   * @param databaseName name of database to create.
   */
  public void createDatabase(@NotNull String databaseName) {
    LOG.info("Creating database using databaseName '{}'.", databaseName);

    runSQLUpdate(String.format("CREATE DATABASE %s", databaseName));

    LOG.info("Successfully created database {}", databaseName);
  }

  /**
   * Drops the given database in the CloudSQL instance.
   *
   * @param databaseName name of database to drop.
   */
  public void dropDatabase(@NotNull String databaseName) {
    LOG.info("Dropping database using databaseName '{}'.", databaseName);

    this.createdDatabase = false;
    runSQLUpdate(String.format("DROP DATABASE %s", databaseName));

    LOG.info("Successfully dropped database {}", databaseName);
  }

  @Override
  public void cleanupAll() {
    LOG.info("Attempting to cleanup CloudSQL manager.");
    try {
      if (this.usingCustomDb) {
        List.copyOf(createdTables).forEach(this::dropTable);
      } else {
        dropDatabase(this.databaseName);
      }
      LOG.info("CloudSQL manager successfully cleaned up.");
    } catch (Exception e) {
      throw new CloudSqlResourceManagerException("Failed to close CloudSQL resources", e);
    }
  }

  /**
   * Builder for {@link CloudSqlResourceManager}.
   *
   * <p>A class that extends {@link AbstractJDBCResourceManager.Builder} for specific CloudSQL
   * implementations.
   */
  public abstract static class Builder
      extends AbstractJDBCResourceManager.Builder<@NonNull CloudSqlContainer<?>> {

    private String dbName;
    private boolean usingCustomDb;

    public Builder(String testId) {
      super(testId, "", "");

      this.setDatabaseName(generateDatabaseName(testId));
      this.usingCustomDb = false;

      // Currently only supports static CloudSQL instance with static Cloud Auth Proxy
      this.maybeUseStaticCloudProxy();
    }

    public Builder maybeUseStaticCloudProxy() {
      if (System.getProperty("cloudProxyHost") != null) {
        this.setHost(System.getProperty("cloudProxyHost"));
      } else {
        LOG.warn("Missing -DcloudProxyHost.");
      }
      if (System.getProperty("cloudProxyPort") != null) {
        this.setPort(Integer.parseInt(System.getProperty("cloudProxyPort")));
      } else {
        LOG.warn("Missing -DcloudProxyPort.");
      }
      if (System.getProperty("cloudProxyPassword") != null) {
        this.setPassword(System.getProperty("cloudProxyPassword"));
      } else {
        LOG.warn("Missing -DcloudProxyPassword.");
      }
      if (System.getProperty("cloudProxyUsername") != null) {
        this.setUsername(System.getProperty("cloudProxyUsername"));
      } else {
        LOG.info("-DcloudProxyUsername not specified, using default: " + DEFAULT_JDBC_USERNAME);
      }
      this.useStaticContainer();

      return this;
    }

    @Override
    public @NonNull Builder setDatabaseName(@NonNull String databaseName) {
      super.setDatabaseName(databaseName);
      this.dbName = databaseName;
      this.usingCustomDb = true;
      return this;
    }
  }
}
