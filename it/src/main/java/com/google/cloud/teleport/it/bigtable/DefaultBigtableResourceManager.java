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
package com.google.cloud.teleport.it.bigtable;

import static com.google.cloud.teleport.it.common.ResourceManagerUtils.generateInstanceId;
import static com.google.cloud.teleport.it.common.ResourceManagerUtils.generateNewId;
import static com.google.cloud.teleport.it.bigtable.BigtableResourceManagerUtils.generateDefaultClusters;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateInstanceRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.re2j.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Default class for implementation of {@link BigtableResourceManager} interface.
 *
 * <p>The class supports one instance, and multiple tables per manager object. The
 * instance is created when the first table is created.
 *
 * <p>The instance id is formed using testId. The instance id will be "{testId}-{ISO8601 time, microsecond precision}",
 * with additional formatting. Note: If testId is more than 30 characters, a new testId will be
 * formed for naming: {first 21 chars of long testId} + “-” + {8 char hash of testId}.
 *
 * <p>The class is thread-safe.
 */
public class DefaultBigtableResourceManager implements BigtableResourceManager {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultBigtableResourceManager.class);

    private static final int MAX_BASE_ID_LENGTH = 30;
    private static final Pattern ILLEGAL_INSTANCE_CHARS = Pattern.compile("");
    
    private static final String DEFAULT_CLUSTER_ZONE = "us-central1-a";
    private static final int DEFAULT_CLUSTER_NUM_NODES = 1;
    private static final StorageType DEFAULT_CLUSTER_STORAGE_TYPE = StorageType.SSD;

    private static final String DEFAULT_CREDENTIAL_ENV_VAR = "DT_IT_ACCESS_TOKEN";

    private boolean hasInstance = false;

    private final String projectId;
    private final String instanceId;
    private final BigtableInstanceAdminClient bigtableInstanceAdminClient;
    private final BigtableTableAdminClient bigtableTableAdminClient;
    private final BigtableDataSettings bigtableDataSettings;

    private DefaultBigtableResourceManager(DefaultBigtableResourceManager.Builder builder) throws IOException {
        this(
                builder.testId,
                builder.projectId
        );
    }

    @VisibleForTesting
    DefaultBigtableResourceManager(String testId, String projectId) throws IOException {
        if (testId.length() > MAX_BASE_ID_LENGTH) {
            testId = generateNewId(testId, MAX_BASE_ID_LENGTH);
        }
        this.projectId = projectId;
        this.instanceId = generateInstanceId(testId, ILLEGAL_INSTANCE_CHARS, MAX_BASE_ID_LENGTH);

        CredentialsProvider credentialsProvider = () -> new GoogleCredentials(new AccessToken(System.getenv(DEFAULT_CREDENTIAL_ENV_VAR), null));
        this.bigtableInstanceAdminClient = BigtableInstanceAdminClient.create(BigtableInstanceAdminSettings.newBuilder().setProjectId(projectId).setCredentialsProvider(credentialsProvider).build());
        this.bigtableTableAdminClient = BigtableTableAdminClient.create(BigtableTableAdminSettings.newBuilder().setProjectId(projectId).setInstanceId(instanceId).setCredentialsProvider(credentialsProvider).build());
        this.bigtableDataSettings = BigtableDataSettings.newBuilder().setProjectId(projectId).setInstanceId(instanceId).setCredentialsProvider(credentialsProvider).build();

    }

    public static DefaultBigtableResourceManager.Builder builder(String testId, String projectId) {
        return new DefaultBigtableResourceManager.Builder(testId, projectId);
    }

    @Override
    public synchronized void createInstance(Iterable<BigtableResourceManagerCluster> clusters) throws IllegalStateException {
        checkIsUsable();
        if (bigtableInstanceAdminClient.exists(instanceId)) {
            throw new IllegalStateException("Instance " + instanceId + " already exists for project " + projectId + ".");
        }
        LOG.info("Creating instance {} in project {}.", instanceId, projectId);
        CreateInstanceRequest request = CreateInstanceRequest.of(instanceId);
        for(BigtableResourceManagerCluster cluster: clusters) {
            request.addCluster(cluster.getClusterId(), cluster.getRegion(), cluster.getNumNodes(), cluster.getStorageType());
        }
        bigtableInstanceAdminClient.createInstance(request);
        hasInstance = true;
        LOG.info("Successfully created instance {}.", instanceId);
    }

    private void checkIsUsable() throws IllegalStateException {

    }

    private void checkHasInstance() throws IllegalStateException {
        if (!hasInstance) {
            throw new IllegalStateException("There is no instance for manager to perform operation on.");
        }
    }

    private void checkHasTable(String tableId) throws IllegalStateException {
        if (!bigtableTableAdminClient.exists(tableId)) {
            throw new IllegalStateException("The table " + tableId + " does not exist.");
        }
    }

    @Override
    public synchronized void createTable(String tableId, Iterable<String> columnFamilies) {
        checkIsUsable();
        if (!hasInstance) {
            createInstance(generateDefaultClusters(instanceId, DEFAULT_CLUSTER_ZONE, DEFAULT_CLUSTER_NUM_NODES, DEFAULT_CLUSTER_STORAGE_TYPE));
        }
        checkHasInstance();

        LOG.info("Creating table using tableId '{}'.", tableId);

        if (!bigtableTableAdminClient.exists(tableId)) {
            CreateTableRequest createTableRequest =
                    CreateTableRequest.of(tableId);
            for (String columnFamily:columnFamilies) {
                createTableRequest.addFamily(columnFamily);
            }
            bigtableTableAdminClient.createTable(createTableRequest);
            System.out.printf("Table %s created successfully%n", tableId);
        }
    }

    @Override
    public synchronized void write(RowMutation tableRow) throws IllegalStateException {
        write(ImmutableList.of(tableRow));
    }

    @Override
    public synchronized void write(Iterable<RowMutation> tableRows) throws IllegalStateException {
        checkIsUsable();
        checkHasInstance();

        LOG.info("Sending {} mutations to instance {}.", Iterables.size(tableRows), instanceId);
        try (BigtableDataClient dataClient = BigtableDataClient.create(bigtableDataSettings)) {
            for(RowMutation tableRow:tableRows) {
                dataClient.mutateRow(tableRow);
            }
            LOG.info("Successfully sent mutations to instance {}.", instanceId);
        } catch (IOException e) {
            throw new BigtableResourceManagerException("Failed to write mutations.", e);
        }
    }

    @Override
    public synchronized ImmutableList<Row> readTable(String tableId) throws RuntimeException {
        checkIsUsable();
        checkHasInstance();
        checkHasTable(tableId);

        LOG.info("Reading all rows from {}.}", tableId);
        ImmutableList.Builder<Row> tableRowsBuilder = ImmutableList.builder();

        try (BigtableDataClient dataClient = BigtableDataClient.create(bigtableDataSettings)) {

            Query query = Query.create(tableId);
            ServerStream<Row> rowStream = dataClient.readRows(query);
            for (Row row : rowStream) {
                tableRowsBuilder.add(row);
            }

        } catch (IOException | NotFoundException e) {
            throw new BigtableResourceManagerException("Error occurred while reading table rows.", e);
        }

        return tableRowsBuilder.build();
    }

    @Override
    public synchronized void cleanupAll() {
        LOG.info("Attempting to cleanup manager.");
        bigtableInstanceAdminClient.deleteInstance(instanceId);
        hasInstance = false;
        bigtableInstanceAdminClient.close();
        bigtableTableAdminClient.close();
        LOG.info("Manager successfully cleaned up.");
    }

    /** Builder for {@link DefaultBigtableResourceManager}. */
    public static final class Builder {

        private final String testId;
        private final String projectId;

        private Builder(String testId, String projectId) {
            this.testId = testId;
            this.projectId = projectId;
        }

        public DefaultBigtableResourceManager build() throws IOException {
            return new DefaultBigtableResourceManager(this);
        }
    }
}
