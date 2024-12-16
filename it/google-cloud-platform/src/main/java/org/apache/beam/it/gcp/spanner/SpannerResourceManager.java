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
package org.apache.beam.it.gcp.spanner;

import static org.apache.beam.it.common.utils.ResourceManagerUtils.checkValidProjectId;
import static org.apache.beam.it.common.utils.ResourceManagerUtils.generateNewId;
import static org.apache.beam.it.gcp.spanner.utils.SpannerResourceManagerUtils.generateDatabaseId;
import static org.apache.beam.it.gcp.spanner.utils.SpannerResourceManagerUtils.generateInstanceId;

import com.google.auth.Credentials;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.monitoring.v3.Aggregation.Aligner;
import com.google.monitoring.v3.TimeInterval;
import com.google.protobuf.Timestamp;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.it.common.utils.ExceptionUtils;
import org.apache.beam.it.gcp.monitoring.MonitoringClient;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for managing Spanner resources.
 *
 * <p>The class supports one instance, one database, and multiple tables per manager object. The
 * instance and database are created when the first table is created.
 *
 * <p>The instance and database ids are formed using testId. The database id will be {testId}, with
 * some extra formatting. The instance id will be "{testId}-{ISO8601 time, microsecond precision}",
 * with additional formatting. Note: If testId is more than 30 characters, a new testId will be
 * formed for naming: {first 21 chars of long testId} + “-” + {8 char hash of testId}.
 *
 * <p>The class is thread-safe.
 */
public final class SpannerResourceManager implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerResourceManager.class);
  private static final int MAX_BASE_ID_LENGTH = 30;

  public static final String DEFAULT_SPANNER_HOST = "https://batch-spanner.googleapis.com";
  public static final String STAGING_SPANNER_HOST =
      "https://staging-wrenchworks.sandbox.googleapis.com";

  // Retry settings for instance creation
  private static final int CREATE_MAX_RETRIES = 5;
  private static final Duration CREATE_BACKOFF_DELAY = Duration.ofSeconds(10);
  private static final Duration CREATE_BACKOFF_MAX_DELAY = Duration.ofSeconds(60);
  private static final double CREATE_BACKOFF_JITTER = 0.1;

  private boolean hasInstance = false;
  private boolean hasDatabase = false;

  private final String projectId;
  private final String instanceId;
  private final boolean usingStaticInstance;
  private final String databaseId;
  private final String region;
  private final String spannerHost;

  private final Dialect dialect;

  private final Spanner spanner;
  private final InstanceAdminClient instanceAdminClient;
  private final DatabaseAdminClient databaseAdminClient;
  private final int nodeCount;
  private Timestamp startTime;
  private MonitoringClient monitoringClient;

  private SpannerResourceManager(Builder builder) {
    this(
        builder,
        ((Supplier<Spanner>)
                () -> {
                  SpannerOptions.Builder optionsBuilder = SpannerOptions.newBuilder();
                  optionsBuilder.setProjectId(builder.projectId).setHost(builder.host);
                  if (builder.credentials != null) {
                    optionsBuilder.setCredentials(builder.credentials);
                  }
                  return optionsBuilder.build().getService();
                })
            .get());
  }

  @VisibleForTesting
  SpannerResourceManager(SpannerResourceManager.Builder builder, Spanner spanner) {
    // Check that the project ID conforms to GCP standards
    checkValidProjectId(builder.projectId);

    String testId = builder.testId;
    if (testId.length() > MAX_BASE_ID_LENGTH) {
      testId = generateNewId(testId, MAX_BASE_ID_LENGTH);
    }
    this.projectId = builder.projectId;
    this.databaseId = generateDatabaseId(testId);

    if (builder.useStaticInstance) {
      if (builder.instanceId == null) {
        throw new SpannerResourceManagerException(
            "This manager was configured to use a static resource, but the instanceId was not properly set.");
      }
      this.instanceId = builder.instanceId;
    } else {
      this.instanceId = generateInstanceId(testId);
    }
    this.usingStaticInstance = builder.useStaticInstance;

    this.region = builder.region;
    this.dialect = builder.dialect;
    this.spannerHost = builder.host;
    this.spanner = spanner;
    this.instanceAdminClient = spanner.getInstanceAdminClient();
    this.databaseAdminClient = spanner.getDatabaseAdminClient();
    this.nodeCount = builder.nodeCount;
    this.monitoringClient = builder.monitoringClient;
  }

  public static Builder builder(String testId, String projectId, String region) {
    return new Builder(testId, projectId, region, Dialect.GOOGLE_STANDARD_SQL);
  }

  public static Builder builder(String testId, String projectId, String region, Dialect dialect) {
    return new Builder(testId, projectId, region, dialect);
  }

  private synchronized void maybeCreateInstance() {
    checkIsUsable();

    if (usingStaticInstance) {
      LOG.info("Not creating Spanner instance - reusing static {}", instanceId);
      hasInstance = true;
      return;
    }

    if (hasInstance) {
      return;
    }

    LOG.info("Creating instance {} in project {}.", instanceId, projectId);
    try {
      InstanceInfo instanceInfo =
          InstanceInfo.newBuilder(InstanceId.of(projectId, instanceId))
              .setInstanceConfigId(InstanceConfigId.of(projectId, "regional-" + region))
              .setDisplayName(instanceId)
              .setNodeCount(nodeCount)
              .build();

      // Retry creation if there's a quota error
      Instance instance =
          Failsafe.with(retryOnQuotaException())
              .get(() -> instanceAdminClient.createInstance(instanceInfo).get());

      hasInstance = true;
      LOG.info("Successfully created instance {}: {}.", instanceId, instance.getState());
    } catch (Exception e) {
      cleanupAll();
      throw new SpannerResourceManagerException("Failed to create instance.", e);
    }
  }

  private synchronized void maybeCreateDatabase() {
    checkIsUsable();
    this.startTime = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();
    if (hasDatabase) {
      return;
    }
    LOG.info("Creating database {} in instance {}.", databaseId, instanceId);

    try {
      Database database =
          Failsafe.with(retryOnQuotaException())
              .get(
                  () ->
                      databaseAdminClient
                          .createDatabase(
                              databaseAdminClient
                                  .newDatabaseBuilder(
                                      DatabaseId.of(projectId, instanceId, databaseId))
                                  .setDialect(dialect)
                                  .build(),
                              ImmutableList.of())
                          .get());

      hasDatabase = true;
      LOG.info("Successfully created database {}: {}.", databaseId, database.getState());
    } catch (Exception e) {
      cleanupAll();
      throw new SpannerResourceManagerException("Failed to create database.", e);
    }
  }

  private static <T> RetryPolicy<T> retryOnQuotaException() {
    return RetryPolicy.<T>builder()
        .handleIf(
            exception -> {
              LOG.warn("Error from spanner:", exception);
              return ExceptionUtils.containsMessage(exception, "RESOURCE_EXHAUSTED");
            })
        .withMaxRetries(CREATE_MAX_RETRIES)
        .withBackoff(CREATE_BACKOFF_DELAY, CREATE_BACKOFF_MAX_DELAY)
        .withJitter(CREATE_BACKOFF_JITTER)
        .build();
  }

  private void checkIsUsable() throws IllegalStateException {
    if (spanner.isClosed()) {
      throw new IllegalStateException("Manager has cleaned up all resources and is unusable.");
    }
  }

  private void checkHasInstanceAndDatabase() throws IllegalStateException {
    if (!hasInstance) {
      throw new IllegalStateException("There is no instance for manager to perform operation on.");
    }
    if (!hasDatabase) {
      throw new IllegalStateException("There is no database for manager to perform operation on");
    }
  }

  /**
   * Return the instance ID this Resource Manager uses to create and manage tables in.
   *
   * @return the instance ID.
   */
  public String getInstanceId() {
    return this.instanceId;
  }

  /**
   * Return the dataset ID this Resource Manager uses to create and manage tables in.
   *
   * @return the dataset ID.
   */
  public String getDatabaseId() {
    return this.databaseId;
  }

  /**
   * Return the Spanner host that is servicing API requests.
   *
   * @return Spanner host.
   */
  public String getSpannerHost() {
    return this.spannerHost;
  }

  /**
   * Executes a DDL statement.
   *
   * <p>Note: Implementations may do instance creation and database creation here.
   *
   * @param statement The DDL statement.
   * @throws IllegalStateException if method is called after resources have been cleaned up.
   */
  public synchronized void executeDdlStatement(String statement) throws IllegalStateException {
    executeDdlStatements(ImmutableList.of(statement));
  }

  /**
   * Executes a list of DDL statements.
   *
   * <p>Note: Implementations may do instance creation and database creation here.
   *
   * @param statements The DDL statements.
   * @throws IllegalStateException if method is called after resources have been cleaned up.
   */
  public synchronized void executeDdlStatements(List<String> statements)
      throws IllegalStateException {
    checkIsUsable();
    maybeCreateInstance();
    maybeCreateDatabase();

    LOG.info("Executing DDL statements '{}' on database {}.", statements, databaseId);
    try {
      // executeDdlStatments can fail for spanner staging because of failfast.
      Failsafe.with(retryOnQuotaException())
          .run(
              () ->
                  databaseAdminClient
                      .updateDatabaseDdl(
                          instanceId, databaseId, statements, /* operationId= */ null)
                      .get());
      LOG.info("Successfully executed DDL statements '{}' on database {}.", statements, databaseId);
    } catch (Exception e) {
      throw new SpannerResourceManagerException("Failed to execute statement.", e);
    }
  }

  /**
   * Writes a given record into a table. This method requires {@link
   * SpannerResourceManager#executeDdlStatement(String)} to be called for the target table
   * beforehand.
   *
   * @param tableRecord A mutation object representing the table record.
   * @throws IllegalStateException if method is called after resources have been cleaned up or if
   *     the manager object has no instance or database.
   */
  public synchronized void write(Mutation tableRecord) throws IllegalStateException {
    write(ImmutableList.of(tableRecord));
  }

  /**
   * Writes a collection of table records into one or more tables. This method requires {@link
   * SpannerResourceManager#executeDdlStatement(String)} to be called for the target table
   * beforehand.
   *
   * @param tableRecords A collection of mutation objects representing table records.
   * @throws IllegalStateException if method is called after resources have been cleaned up or if
   *     the manager object has no instance or database.
   */
  public synchronized void write(Iterable<Mutation> tableRecords) throws IllegalStateException {
    checkIsUsable();
    checkHasInstanceAndDatabase();

    LOG.info("Sending {} mutations to {}.{}", Iterables.size(tableRecords), instanceId, databaseId);
    try {
      DatabaseClient databaseClient =
          spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
      databaseClient.write(tableRecords);
      LOG.info("Successfully sent mutations to {}.{}", instanceId, databaseId);
    } catch (SpannerException e) {
      throw new SpannerResourceManagerException("Failed to write mutations.", e);
    }
  }

  /**
   * Runs the specified query.
   *
   * @param query the query to execute
   */
  public ImmutableList<Struct> runQuery(String query) {
    try (ReadContext readContext =
        spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId)).singleUse()) {
      ResultSet results = readContext.executeQuery(Statement.of(query));

      ImmutableList.Builder<Struct> tableRecordsBuilder = ImmutableList.builder();
      while (results.next()) {
        tableRecordsBuilder.add(results.getCurrentRowAsStruct());
      }
      ImmutableList<Struct> tableRecords = tableRecordsBuilder.build();

      LOG.info("Loaded {} rows from {}", tableRecords.size(), query);
      return tableRecords;
    } catch (Exception e) {
      throw new SpannerResourceManagerException("Failed to read query " + query, e);
    }
  }

  /**
   * Gets the number of rows in the table.
   *
   * @param table the name of the table
   */
  public Long getRowCount(String table) {
    ImmutableList<Struct> r = runQuery(String.format("SELECT COUNT(*) FROM %s", table));
    return r.get(0).getLong(0);
  }

  /**
   * Reads all the rows in a table. This method requires {@link
   * SpannerResourceManager#executeDdlStatement(String)} to be called for the target table
   * beforehand.
   *
   * @param tableId The id of the table to read rows from.
   * @param columnNames The table's column names.
   * @return A List object containing all the rows in the table as structs.
   * @throws IllegalStateException if method is called after resources have been cleaned up or if
   *     the manager object has no instance or database.
   */
  public synchronized ImmutableList<Struct> readTableRecords(String tableId, String... columnNames)
      throws IllegalStateException {
    return readTableRecords(tableId, ImmutableList.copyOf(columnNames));
  }

  /**
   * Reads all the rows in a table.This method requires {@link
   * SpannerResourceManager#executeDdlStatement(String)} to be called for the target table
   * beforehand.
   *
   * @param tableId The id of table to read rows from.
   * @param columnNames A collection of the table's column names.
   * @return A List object containing all the rows in the table as structs.
   * @throws IllegalStateException if method is called after resources have been cleaned up or if
   *     the manager object has no instance or database.
   */
  public synchronized ImmutableList<Struct> readTableRecords(
      String tableId, Iterable<String> columnNames) throws IllegalStateException {
    checkIsUsable();
    checkHasInstanceAndDatabase();

    LOG.info(
        "Loading columns {} from {}.{}.{}",
        Iterables.toString(columnNames),
        instanceId,
        databaseId,
        tableId);
    DatabaseClient databaseClient =
        spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));

    try (ReadContext readContext = databaseClient.singleUse();
        ResultSet resultSet = readContext.read(tableId, KeySet.all(), columnNames)) {
      ImmutableList.Builder<Struct> tableRecordsBuilder = ImmutableList.builder();

      while (resultSet.next()) {
        tableRecordsBuilder.add(resultSet.getCurrentRowAsStruct());
      }
      ImmutableList<Struct> tableRecords = tableRecordsBuilder.build();
      LOG.info(
          "Loaded {} records from {}.{}.{}", tableRecords.size(), instanceId, databaseId, tableId);
      return tableRecords;
    } catch (SpannerException e) {
      throw new SpannerResourceManagerException("Error occurred while reading table records.", e);
    }
  }

  /**
   * Deletes all created resources (instance, database, and tables) and cleans up all Spanner
   * sessions, making the manager object unusable.
   */
  @Override
  public synchronized void cleanupAll() {
    try {

      if (usingStaticInstance) {
        if (databaseAdminClient != null) {
          Failsafe.with(retryOnQuotaException())
              .run(() -> databaseAdminClient.dropDatabase(instanceId, databaseId));
        }
      } else {
        LOG.info("Deleting instance {}...", instanceId);

        if (instanceAdminClient != null) {
          Failsafe.with(retryOnQuotaException())
              .run(() -> instanceAdminClient.deleteInstance(instanceId));
        }

        hasInstance = false;
      }
      hasDatabase = false;
    } catch (SpannerException e) {
      throw new SpannerResourceManagerException("Failed to delete instance.", e);
    } finally {
      if (!spanner.isClosed()) {
        spanner.close();
      }
    }
    LOG.info("Manager successfully cleaned up.");
  }

  /**
   * Collects the performance metrics for the spanner database resource like Average CPU
   * utilization.
   *
   * @param metrics The spanner metrics will be populated in this map
   */
  public void collectMetrics(@NonNull Map<String, Double> metrics) {
    hasMonitoringClient();
    checkHasInstanceAndDatabase();
    metrics.put(
        "Spanner_AverageCpuUtilization",
        getAggregateCpuUtilization(monitoringClient, Aligner.ALIGN_MEAN));
    metrics.put(
        "Spanner_MaxCpuUtilization",
        getAggregateCpuUtilization(monitoringClient, Aligner.ALIGN_MAX));
  }

  private void hasMonitoringClient() {
    if (monitoringClient == null) {
      throw new SpannerResourceManagerException(
          "SpannerResourceManager needs to be initialized with Monitoring client in order to export"
              + " metrics. Please use SpannerResourceManager.Builder(...).setMonitoringClient(...) "
              + "to initialize the monitoring client.");
    }
  }

  private Double getAggregateCpuUtilization(
      MonitoringClient monitoringClient, Aligner aggregationFunction) {
    String metricType = "spanner.googleapis.com/instance/cpu/utilization";

    TimeInterval interval =
        TimeInterval.newBuilder()
            .setEndTime(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()))
            .setStartTime(this.startTime)
            .build();

    String filter =
        "metric.type=\"%s\" AND "
            + "resource.type=\"spanner_instance\" AND "
            + "resource.label.instance_id=\"%s\" AND metric.label.database=\"%s\"";

    filter = String.format(filter, metricType, this.instanceId, this.databaseId);

    return monitoringClient.getAggregatedMetric(
        this.projectId, filter, interval, aggregationFunction);
  }

  /** Builder for {@link SpannerResourceManager}. */
  public static final class Builder {

    private final String testId;
    private final String projectId;
    private final String region;
    private final Dialect dialect;
    private @Nullable String instanceId;
    private boolean useStaticInstance;
    private Credentials credentials;
    private String host;
    private int nodeCount;
    private MonitoringClient monitoringClient;

    private Builder(String testId, String projectId, String region, Dialect dialect) {
      this.testId = testId;
      this.projectId = projectId;
      this.region = region;
      this.dialect = dialect;
      this.instanceId = null;
      this.useStaticInstance = false;
      this.host = DEFAULT_SPANNER_HOST;
      this.nodeCount = 1;
    }

    public Builder setCredentials(Credentials credentials) {
      this.credentials = credentials;
      return this;
    }

    /**
     * Configures the resource manager to use a static GCP resource instead of creating a new
     * instance of the resource.
     *
     * @return this builder object with the useStaticInstance option enabled.
     */
    public Builder useStaticInstance() {
      this.useStaticInstance = true;
      return this;
    }

    /**
     * Looks at the system properties if there's an instance id, and reuses it if configured.
     *
     * @return this builder with the instance ID set.
     */
    @SuppressWarnings("nullness")
    public Builder maybeUseStaticInstance() {
      if (System.getProperty("spannerInstanceId") != null) {
        this.useStaticInstance = true;
        this.instanceId = System.getProperty("spannerInstanceId");
      }
      return this;
    }

    /**
     * Set the instance ID of a static Spanner instance for this Resource Manager to manage.
     *
     * @return this builder with the instance ID set.
     */
    public Builder setInstanceId(String instanceId) {
      this.instanceId = instanceId;
      return this;
    }

    /**
     * Overrides spanner host, uses it for Spanner API calls.
     *
     * @param spannerHost spanner host URL
     * @return this builder with host set.
     */
    public Builder useCustomHost(String spannerHost) {
      this.host = spannerHost;
      return this;
    }

    /**
     * Configures the node count of the spanner instance if creating a new one.
     *
     * @param nodeCount
     * @return
     */
    public Builder setNodeCount(int nodeCount) {
      this.nodeCount = nodeCount;
      return this;
    }

    /**
     * Sets Monitoring Client instance to be used for getMetrics method.
     *
     * @return monitoring client
     */
    public Builder setMonitoringClient(MonitoringClient monitoringClient) {
      this.monitoringClient = monitoringClient;
      return this;
    }

    public SpannerResourceManager build() {
      return new SpannerResourceManager(this);
    }
  }
}
