/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.it.gcp.bigtable;

import static com.google.cloud.teleport.it.common.utils.ResourceManagerUtils.checkValidProjectId;
import static com.google.cloud.teleport.it.gcp.bigtable.BigtableResourceManagerUtils.checkValidTableId;
import static com.google.cloud.teleport.it.gcp.bigtable.BigtableResourceManagerUtils.generateDefaultClusters;
import static com.google.cloud.teleport.it.gcp.bigtable.BigtableResourceManagerUtils.generateInstanceId;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.AppProfile.MultiClusterRoutingPolicy;
import com.google.cloud.bigtable.admin.v2.models.AppProfile.RoutingPolicy;
import com.google.cloud.bigtable.admin.v2.models.AppProfile.SingleClusterRoutingPolicy;
import com.google.cloud.bigtable.admin.v2.models.CreateAppProfileRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateInstanceRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.GCRules;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.teleport.it.common.ResourceManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/**
 * Client for managing Bigtable resources.
 *
 * <p>The class supports one instance, and multiple tables per manager object. An instance is
 * created when the first table is created if one has not been created already.
 *
 * <p>The instance id is formed using testId. The instance id will be "{testId}-{ISO8601 time,
 * microsecond precision}", with additional formatting. Note: If testId is more than 30 characters,
 * a new testId will be formed for naming: {first 21 chars of long testId} + “-” + {8 char hash of
 * testId}.
 *
 * <p>The class is thread-safe.
 */
public class BigtableResourceManager implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableResourceManager.class);
  private static final String DEFAULT_CLUSTER_ZONE = "us-central1-a";
  private static final int DEFAULT_CLUSTER_NUM_NODES = 1;
  private static final StorageType DEFAULT_CLUSTER_STORAGE_TYPE = StorageType.SSD;

  private final String projectId;
  private final String instanceId;
  private final BigtableResourceManagerClientFactory bigtableResourceManagerClientFactory;

  private final boolean useExistingResources;
  private boolean hasInstance = false;

  private BigtableResourceManager(BigtableResourceManager.Builder builder) throws IOException {
    // Check that the project ID conforms to GCP standards
    checkValidProjectId(builder.projectId);

    // generate instance id based on given test id.
    this.instanceId = generateInstanceId(builder.testId, builder.bigtableResourceOverrides);
    this.useExistingResources = (builder.bigtableResourceOverrides != null);

    // create the bigtable admin and data client settings builders, and set the necessary id's for
    // each.
    BigtableInstanceAdminSettings.Builder bigtableInstanceAdminSettings =
        BigtableInstanceAdminSettings.newBuilder().setProjectId(builder.projectId);
    BigtableTableAdminSettings.Builder bigtableTableAdminSettings =
        BigtableTableAdminSettings.newBuilder()
            .setProjectId(builder.projectId)
            .setInstanceId(this.instanceId);
    BigtableDataSettings.Builder bigtableDataSettings =
        BigtableDataSettings.newBuilder()
            .setProjectId(builder.projectId)
            .setInstanceId(this.instanceId);

    // add the credentials to the builders, if set.
    if (builder.credentialsProvider != null) {
      bigtableInstanceAdminSettings.setCredentialsProvider(builder.credentialsProvider);
      bigtableTableAdminSettings.setCredentialsProvider(builder.credentialsProvider);
      bigtableDataSettings.setCredentialsProvider(builder.credentialsProvider);
    }

    this.projectId = builder.projectId;
    this.bigtableResourceManagerClientFactory =
        new BigtableResourceManagerClientFactory(
            bigtableInstanceAdminSettings.build(),
            bigtableTableAdminSettings.build(),
            bigtableDataSettings.build());
  }

  @VisibleForTesting
  BigtableResourceManager(
      String testId,
      String projectId,
      BigtableResourceManagerClientFactory bigtableResourceManagerClientFactory,
      BigtableResourceOverrides bigtableResourceOverrides) {
    this.projectId = projectId;
    this.instanceId = generateInstanceId(testId, bigtableResourceOverrides);
    this.bigtableResourceManagerClientFactory = bigtableResourceManagerClientFactory;
    this.useExistingResources = (bigtableResourceOverrides != null);
  }

  public static BigtableResourceManager.Builder builder(String testId, String projectId)
      throws IOException {
    return new BigtableResourceManager.Builder(testId, projectId);
  }

  /**
   * Returns the project ID this Resource Manager is configured to operate on.
   *
   * @return the project ID.
   */
  public String getProjectId() {
    return projectId;
  }

  /**
   * Return the instance ID this Resource Manager uses to create and manage tables in.
   *
   * @return the instance ID.
   */
  public String getInstanceId() {
    return instanceId;
  }

  /**
   * Creates a Bigtable instance in which all clusters, nodes and tables will exist.
   *
   * @param clusters Collection of BigtableResourceManagerCluster objects to associate with the
   *     given Bigtable instance.
   * @throws BigtableResourceManagerException if there is an error creating the instance in
   *     Bigtable.
   */
  public synchronized void createInstance(Iterable<BigtableResourceManagerCluster> clusters) {

    // Check to see if instance already exists, and throw error if it does
    if (hasInstance) {
      throw new IllegalStateException(
          "Instance " + instanceId + " already exists for project " + projectId + ".");
    }

    if (useExistingResources) {
      LOG.info(
          "Not creating instance {} in project {}, assuming test will use a pre-created one.",
          instanceId,
          projectId);
      hasInstance = true;
      return;
    } else {
      LOG.info("Creating instance {} in project {}.", instanceId, projectId);
    }

    // Create instance request object and add all the given clusters to the request
    CreateInstanceRequest request = CreateInstanceRequest.of(instanceId);
    for (BigtableResourceManagerCluster cluster : clusters) {
      request.addCluster(
          cluster.clusterId(), cluster.zone(), cluster.numNodes(), cluster.storageType());
    }

    // Send the instance request to Google Cloud
    try (BigtableInstanceAdminClient instanceAdminClient =
        bigtableResourceManagerClientFactory.bigtableInstanceAdminClient()) {
      instanceAdminClient.createInstance(request);
    } catch (Exception e) {
      throw new BigtableResourceManagerException(
          "Failed to create instance " + instanceId + ".", e);
    }
    hasInstance = true;

    LOG.info("Successfully created instance {}.", instanceId);
  }

  /**
   * Helper method for determining if an instance has been created for the ResourceManager object.
   *
   * @throws IllegalStateException if an instance has not yet been created.
   */
  private void checkHasInstance() {
    if (!hasInstance) {
      throw new IllegalStateException("There is no instance for manager to perform operation on.");
    }
  }

  /**
   * Helper method for determining if the given tableId exists in the instance.
   *
   * @param tableId The id of the table to check.
   * @throws IllegalStateException if the table does not exist in the instance.
   */
  private void checkHasTable(String tableId) {
    try (BigtableTableAdminClient tableAdminClient =
        bigtableResourceManagerClientFactory.bigtableTableAdminClient()) {
      if (!tableAdminClient.exists(tableId)) {
        throw new IllegalStateException(
            "The table " + tableId + " does not exist in instance " + instanceId + ".");
      }
    }
  }

  /**
   * Creates a table within the current instance given a table ID and a collection of column family
   * names.
   *
   * <p>The columns in this table will be automatically garbage collected once they reach the age
   * specified by {@code maxAge}.
   *
   * <p>Note: Implementations may do instance creation here, if one does not already exist.
   *
   * @param appProfileId The id of the app profile.
   * @param allowTransactionWrites Allows transactional writes when single cluster routing is
   *     enabled
   * @param clusters Clusters where traffic is going to be routed. If more than one cluster is
   *     specified, a multi-cluster routing is used. A single-cluster routing is used when a single
   *     cluster is specified.
   * @throws BigtableResourceManagerException if there is an error creating the table in Bigtable.
   */
  public synchronized void createAppProfile(
      String appProfileId, boolean allowTransactionWrites, List<String> clusters) {
    checkHasInstance();
    if (clusters == null || clusters.isEmpty()) {
      throw new IllegalArgumentException("Cluster list cannot be empty");
    }

    RoutingPolicy routingPolicy;

    if (clusters.size() == 1) {
      routingPolicy = SingleClusterRoutingPolicy.of(clusters.get(0), allowTransactionWrites);
    } else {
      routingPolicy = MultiClusterRoutingPolicy.of(new HashSet<>(clusters));
    }

    if (useExistingResources) {
      LOG.info(
          "Not creating appProfile {} for instance {}, assuming test will use a "
              + "pre-created one.",
          appProfileId,
          instanceId);
      return;
    } else {
      LOG.info("Creating appProfile {} for instance project {}.", appProfileId, instanceId);
    }

    // Send the instance request to Google Cloud
    try (BigtableInstanceAdminClient instanceAdminClient =
        bigtableResourceManagerClientFactory.bigtableInstanceAdminClient()) {
      CreateAppProfileRequest request =
          CreateAppProfileRequest.of(instanceId, appProfileId).setRoutingPolicy(routingPolicy);
      instanceAdminClient.createAppProfile(request);
      LOG.info("Successfully created appProfile {}.", appProfileId);
    } catch (Exception e) {
      throw new BigtableResourceManagerException(
          "Failed to create appProfile " + appProfileId + ".", e);
    }
  }

  /**
   * Creates a table within the current instance given a table ID and other configuration settings.
   *
   * <p>Note: Implementations may do instance creation here, if one does not already exist.
   *
   * @param tableId The id of the table.
   * @param spec Other table configurations
   */
  public synchronized void createTable(String tableId, BigtableTableSpec spec) {
    // Check table ID
    checkValidTableId(tableId);

    // Check for at least one column family
    if (!spec.getColumnFamilies().iterator().hasNext()) {
      throw new IllegalArgumentException(
          "There must be at least one column family specified when creating a table.");
    }

    // Create a default instance if this resource manager has not already created one
    if (!hasInstance) {
      createInstance(
          generateDefaultClusters(
              instanceId,
              DEFAULT_CLUSTER_ZONE,
              DEFAULT_CLUSTER_NUM_NODES,
              DEFAULT_CLUSTER_STORAGE_TYPE));
    }
    checkHasInstance();

    if (useExistingResources) {
      LOG.info("Not creating a table '{}', assuming test will use a pre-created one.", tableId);
      return;
    } else {
      LOG.info("Creating table using tableId '{}'.", tableId);
    }

    // Fetch the Bigtable Table client and create the table if it does not already exist in the
    // instance
    try (BigtableTableAdminClient tableAdminClient =
        bigtableResourceManagerClientFactory.bigtableTableAdminClient()) {
      if (!tableAdminClient.exists(tableId)) {
        CreateTableRequest createTableRequest = CreateTableRequest.of(tableId);

        for (String columnFamily : spec.getColumnFamilies()) {
          createTableRequest.addFamily(columnFamily, GCRules.GCRULES.maxAge(spec.getMaxAge()));
        }
        // TODO: Set CDC enabled
        tableAdminClient.createTable(createTableRequest);
      } else {
        throw new IllegalStateException(
            "Table " + tableId + " already exists for instance " + instanceId + ".");
      }
    } catch (Exception e) {
      throw new BigtableResourceManagerException("Failed to create table.", e);
    }

    LOG.info("Successfully created table {}.{}", instanceId, tableId);
  }

  /**
   * Creates a table within the current instance given a table ID and a collection of column family
   * names.
   *
   * <p>The columns in this table will be automatically garbage collected after one hour.
   *
   * <p>Note: Implementations may do instance creation here, if one does not already exist.
   *
   * @param tableId The id of the table.
   * @param columnFamilies A collection of column family names for the table.
   * @throws BigtableResourceManagerException if there is an error creating the table in Bigtable.
   */
  public synchronized void createTable(String tableId, Iterable<String> columnFamilies) {
    BigtableTableSpec spec = new BigtableTableSpec();
    spec.setColumnFamilies(columnFamilies);
    spec.setMaxAge(Duration.ofHours(1));
    createTable(tableId, spec);
  }

  /**
   * Creates a table within the current instance given a table ID and a collection of column family
   * names.
   *
   * <p>The columns in this table will be automatically garbage collected once they reach the age
   * specified by {@code maxAge}.
   *
   * <p>Note: Implementations may do instance creation here, if one does not already exist.
   *
   * @param tableId The id of the table.
   * @param columnFamilies A collection of column family names for the table.
   * @param maxAge Sets the maximum age the columns can persist before being garbage collected.
   * @throws BigtableResourceManagerException if there is an error creating the table in Bigtable.
   */
  public synchronized void createTable(
      String tableId, Iterable<String> columnFamilies, Duration maxAge) {
    BigtableTableSpec spec = new BigtableTableSpec();
    spec.setColumnFamilies(columnFamilies);
    spec.setMaxAge(maxAge);
    createTable(tableId, spec);
  }

  /**
   * Writes a given row into a table. This method requires {@link
   * BigtableResourceManager#createTable(String, Iterable)} to be called for the target table
   * beforehand.
   *
   * @param tableRow A mutation object representing the table row.
   * @throws BigtableResourceManagerException if method is called after resources have been cleaned
   *     up, if the manager object has no instance, if the table does not exist or if there is an
   *     IOException when attempting to retrieve the bigtable data client.
   */
  public void write(RowMutation tableRow) {
    write(ImmutableList.of(tableRow));
  }

  /**
   * Writes a collection of table rows into one or more tables. This method requires {@link
   * BigtableResourceManager#createTable(String, Iterable)} to be called for the target table
   * beforehand.
   *
   * @param tableRows A collection of mutation objects representing table rows.
   * @throws BigtableResourceManagerException if method is called after resources have been cleaned
   *     up, if the manager object has no instance, if the table does not exist or if there is an
   *     IOException when attempting to retrieve the bigtable data client.
   */
  public synchronized void write(Iterable<RowMutation> tableRows) {
    checkHasInstance();

    // Exit early if there are no mutations
    if (!tableRows.iterator().hasNext()) {
      return;
    }

    LOG.info("Sending {} mutations to instance {}.", Iterables.size(tableRows), instanceId);

    // Fetch the Bigtable data client and send row mutations to the table
    try (BigtableDataClient dataClient =
        bigtableResourceManagerClientFactory.bigtableDataClient()) {
      for (RowMutation tableRow : tableRows) {
        dataClient.mutateRow(tableRow);
      }
    } catch (Exception e) {
      throw new BigtableResourceManagerException("Failed to write mutations.", e);
    }

    LOG.info("Successfully sent mutations to instance {}.", instanceId);
  }

  /**
   * Reads all the rows in a table. This method requires {@link
   * BigtableResourceManager#createTable(String, Iterable)} to be called for the target table
   * beforehand.
   *
   * @param tableId The id of table to read rows from.
   * @return A List object containing all the rows in the table.
   * @throws BigtableResourceManagerException if method is called after resources have been cleaned
   *     up, if the manager object has no instance, if the table does not exist or if there is an
   *     IOException when attempting to retrieve the bigtable data client.
   */
  public synchronized ImmutableList<Row> readTable(String tableId) {
    checkHasInstance();
    checkHasTable(tableId);

    // List to store fetched rows
    ImmutableList.Builder<Row> tableRowsBuilder = ImmutableList.builder();

    LOG.info("Reading all rows from {}.{}", instanceId, tableId);

    // Fetch the Bigtable data client and read all the rows from the table given by tableId
    try (BigtableDataClient dataClient =
        bigtableResourceManagerClientFactory.bigtableDataClient()) {

      Query query = Query.create(tableId);
      ServerStream<Row> rowStream = dataClient.readRows(query);
      for (Row row : rowStream) {
        tableRowsBuilder.add(row);
      }

    } catch (Exception e) {
      throw new BigtableResourceManagerException("Error occurred while reading table rows.", e);
    }

    ImmutableList<Row> tableRows = tableRowsBuilder.build();
    LOG.info("Loaded {} rows from {}.{}", tableRows.size(), instanceId, tableId);

    return tableRows;
  }

  /**
   * Deletes all created resources (instance and tables) and cleans up all Bigtable clients, making
   * the manager object unusable.
   *
   * @throws BigtableResourceManagerException if there is an error deleting the instance or tables
   *     in Bigtable.
   */
  @Override
  public synchronized void cleanupAll() {
    LOG.info("Attempting to cleanup manager.");
    if (hasInstance) {
      if (useExistingResources) {
        LOG.info("Skipping instance deletion since a existing instance was used");
        return;
      }
      try (BigtableInstanceAdminClient instanceAdminClient =
          bigtableResourceManagerClientFactory.bigtableInstanceAdminClient()) {
        instanceAdminClient.deleteInstance(instanceId);
        hasInstance = false;
      } catch (Exception e) {
        throw new BigtableResourceManagerException("Failed to delete resources.", e);
      }
    }

    LOG.info("Manager successfully cleaned up.");
  }

  /** Builder for {@link BigtableResourceManager}. */
  public static final class Builder {

    private final String testId;
    private final String projectId;

    private BigtableResourceOverrides bigtableResourceOverrides;
    private CredentialsProvider credentialsProvider;

    private Builder(String testId, String projectId) {
      this.testId = testId;
      this.projectId = projectId;
    }

    public Builder setCredentialsProvider(CredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    public Builder setBigtableResourceOverrides(BigtableResourceOverrides resourceOverrides) {
      this.bigtableResourceOverrides = resourceOverrides;
      return this;
    }

    public BigtableResourceManager build() throws IOException {
      return new BigtableResourceManager(this);
    }
  }
}
