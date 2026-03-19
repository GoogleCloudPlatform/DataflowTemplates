/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.loadtesting;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.sqladmin.SQLAdmin;
import com.google.api.services.sqladmin.model.DatabaseInstance;
import com.google.api.services.sqladmin.model.IpMapping;
import com.google.api.services.sqladmin.model.Operation;
import com.google.api.services.sqladmin.model.Settings;
import com.google.api.services.sqladmin.model.User;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.it.gcp.artifacts.GcsArtifact;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudPostgresResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates the lifecycle of Cloud SQL physical instances and logical database shards for
 * high-scale load testing.
 *
 * <p><b>Networking & Safety:</b> This orchestrator connects directly to Private IPs for maximum
 * performance during high-scale (1,024 shards) migrations. At this scale, the Cloud SQL Auth Proxy
 * would become a significant network bottleneck. Security is maintained through VPC-level
 * isolation; these instances have no public IPs and are only reachable from within the trusted VPC
 * network.
 *
 * <p><b>Credential Management:</b> To maintain portability across projects and parallel test
 * safety, the orchestrator explicitly synchronizes the 'root' password on all physical instances
 * during the provisioning stage using the provided 'cloudProxyPassword'.
 *
 * <p>The class follows an initialize-cleanup lifecycle to ensure that logical resources are purged
 * even if the initialization or the test itself fails partially.
 */
public class CloudSqlShardOrchestrator {

  private static final Logger LOG = LoggerFactory.getLogger(CloudSqlShardOrchestrator.class);

  protected final SQLDialect dbType;
  protected final int port;
  protected final String project;
  protected final String region;
  protected final String username;
  protected final String password;
  protected final GcsResourceManager gcsResourceManager;
  protected final Map<String, CloudSqlResourceManager> managers;
  protected final Map<String, String> instanceIpMap;
  protected Map<String, List<String>> requestedShardMap;
  protected final SQLAdmin sqlAdmin;

  /**
   * Constructs a new orchestrator for the specified database dialect.
   *
   * @param dbType The dialect of the source database (e.g., MYSQL, POSTGRESQL).
   * @param project The GCP project ID.
   * @param region The GCP region for Cloud SQL instances.
   * @param gcsResourceManager The GCS resource manager for uploading configuration artifacts.
   */
  public CloudSqlShardOrchestrator(
      SQLDialect dbType, String project, String region, GcsResourceManager gcsResourceManager) {
    this(
        dbType,
        project,
        region,
        gcsResourceManager,
        System.getProperty(
            "cloudProxyUsername", (dbType == SQLDialect.MYSQL) ? "root" : "postgres"),
        System.getProperty("cloudProxyPassword", ""),
        null);
  }

  /**
   * Constructs a new orchestrator with explicit credentials.
   *
   * @param dbType The dialect of the source database.
   * @param project The GCP project ID.
   * @param region The GCP region.
   * @param gcsResourceManager The GCS resource manager.
   * @param username The database username.
   * @param password The database password.
   * @param credentials The GCP credentials to use.
   */
  public CloudSqlShardOrchestrator(
      SQLDialect dbType,
      String project,
      String region,
      GcsResourceManager gcsResourceManager,
      String username,
      String password,
      GoogleCredentials credentials) {
    this.dbType = dbType;
    this.project = project;
    this.region = region;
    this.gcsResourceManager = gcsResourceManager;
    this.username = username;
    this.password = password;
    this.managers = new ConcurrentHashMap<>();
    this.instanceIpMap = new ConcurrentHashMap<>();
    this.requestedShardMap = new HashMap<>();

    try {
      this.sqlAdmin =
          new SQLAdmin.Builder(
                  GoogleNetHttpTransport.newTrustedTransport(),
                  GsonFactory.getDefaultInstance(),
                  new HttpCredentialsAdapter(
                      credentials == null
                          ? GoogleCredentials.getApplicationDefault()
                          : credentials))
              .setApplicationName("BeamIT")
              .build();
    } catch (GeneralSecurityException | IOException e) {
      LOG.error("Exception while initializing SQL Admin", e);
      throw new RuntimeException("Failed to initialize SQLAdmin client", e);
    }
    port = (dbType == SQLDialect.MYSQL) ? 3306 : 5432;
  }

  protected ExecutorService getExecutorService() {
    return Executors.newFixedThreadPool(10);
  }

  private static final int MAX_RETRIES = 10;
  private static final long INITIAL_BACKOFF_MS = 1000; // 1 second

  /**
   * Executes a Cloud SQL Admin API request with retries for 409 (Conflict) and 429 (Rate Limit)
   * errors.
   */
  protected <T> T executeWithRetries(
      com.google.api.client.googleapis.services.AbstractGoogleClientRequest<T> request)
      throws IOException, InterruptedException {
    int retries = 0;
    long backoff = INITIAL_BACKOFF_MS;

    while (true) {
      try {
        return request.execute();
      } catch (GoogleJsonResponseException e) {
        if ((e.getStatusCode() == 409 || e.getStatusCode() == 429) && retries < MAX_RETRIES) {
          LOG.warn(
              "Cloud SQL API returned {}. Retrying in {}ms... (Attempt {}/{})",
              e.getStatusCode(),
              backoff,
              retries + 1,
              MAX_RETRIES);
          Thread.sleep(backoff);
          retries++;
          backoff *= 2; // Exponential backoff
        } else {
          throw e;
        }
      }
    }
  }

  /**
   * Initializes the physical and logical sharded environment.
   *
   * @param shardMap A mapping of physical instance names to the list of logical DB names to create.
   * @param artifactName The name of the artifact file (e.g., "shards.json").
   * @return The full GCS URI to the generated bulkShardConfig.json.
   * @throws ShardOrchestrationException if provisioning or creation fails after retries.
   */
  public String initialize(Map<String, List<String>> shardMap, String artifactName)
      throws ShardOrchestrationException {
    this.requestedShardMap = new HashMap<>(shardMap);

    LOG.info("Initializing shard orchestrator for {} physical instances", shardMap.size());

    try {
      // Stage 1: Physical Provisioning (Parallel)
      provisionPhysicalInstances();

      // Stage 2: Logical Setup (Parallel)
      createLogicalDatabases();

      return generateAndUploadConfig(artifactName);
    } catch (Exception e) {
      LOG.error("Exception while initializing sharded environment", e);
      throw new ShardOrchestrationException("Failed to initialize sharded environment", e);
    }
  }

  protected void provisionPhysicalInstances() {
    LOG.info("Stage 1: Provisioning physical instances and synchronizing credentials...");
    ExecutorService executor = getExecutorService();
    List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
    for (String instanceName : requestedShardMap.keySet()) {
      futures.add(
          executor.submit(
              () -> {
                try {
                  String ip = ensureInstanceAndGetIp(instanceName);
                  instanceIpMap.put(instanceName, ip);
                  updateUserPassword(instanceName);
                } catch (Exception e) {
                  throw new RuntimeException("Failed to provision instance " + instanceName, e);
                }
              }));
    }
    awaitAndShutdownExecutor(executor, futures, "Physical provisioning");
  }

  /**
   * Synchronizes the database password for the specified user across physical shards.
   *
   * @param instanceName The name of the Cloud SQL instance to update.
   */
  protected void updateUserPassword(String instanceName) throws IOException, InterruptedException {
    LOG.info("Updating password for user {} on instance {}", username, instanceName);
    User user = new User().setName(username).setPassword(password);

    // MySQL requires a '%' host for connections from any IP within the VPC.
    if (dbType == SQLDialect.MYSQL) {
      user.setHost("%");
    }

    // MySQL requires name and host to identify the user. PostgreSQL only needs the name.
    // These are passed as query parameters in the Update request.
    SQLAdmin.Users.Update request = sqlAdmin.users().update(project, instanceName, user);
    request.setName(username);
    if (dbType == SQLDialect.MYSQL) {
      // In MySQL, host is part of the primary key for a user. '%' allows the user to connect from
      // any VPC IP.
      request.setHost("%");
    }
    Operation operation = executeWithRetries(request);
    waitForOperation(operation);
  }

  protected String ensureInstanceAndGetIp(String instanceName)
      throws IOException, InterruptedException {
    DatabaseInstance instance;
    try {
      instance = executeWithRetries(sqlAdmin.instances().get(project, instanceName));
      LOG.info("Instance {} already exists.", instanceName);
    } catch (IOException e) {
      if (e.getMessage().contains("404")) {
        LOG.info("Instance {} not found, creating...", instanceName);
        createPhysicalInstance(instanceName);
        instance = executeWithRetries(sqlAdmin.instances().get(project, instanceName));
      } else {
        throw e;
      }
    }

    waitForInstanceReady(instanceName);
    instance = executeWithRetries(sqlAdmin.instances().get(project, instanceName));

    String privateIp = null;
    if (instance.getIpAddresses() != null) {
      for (IpMapping ipMapping : instance.getIpAddresses()) {
        if ("PRIVATE".equals(ipMapping.getType())) {
          privateIp = ipMapping.getIpAddress();
          break;
        }
      }
    }

    if (privateIp == null) {
      throw new RuntimeException("Instance " + instanceName + " does not have a private IP.");
    }
    return privateIp;
  }

  protected void createPhysicalInstance(String instanceName)
      throws IOException, InterruptedException {
    String databaseVersion = dbType == SQLDialect.MYSQL ? "MYSQL_8_0" : "POSTGRES_14";
    String tier = dbType == SQLDialect.MYSQL ? "db-n1-standard-2" : "db-custom-2-7680";
    DatabaseInstance instance =
        new DatabaseInstance()
            .setName(instanceName)
            .setRegion(region)
            .setDatabaseVersion(databaseVersion)
            .setSettings(
                new Settings()
                    .setTier(tier)
                    .setIpConfiguration(
                        new com.google.api.services.sqladmin.model.IpConfiguration()
                            .setPrivateNetwork(
                                String.format("projects/%s/global/networks/default", project))
                            .setEnablePrivatePathForGoogleCloudServices(true)));

    Operation operation = executeWithRetries(sqlAdmin.instances().insert(project, instance));
    waitForOperation(operation);
  }

  protected void waitForInstanceReady(String instanceName)
      throws IOException, InterruptedException {
    LOG.info("Waiting for instance {} to be ready...", instanceName);
    for (int i = 0; i < 60; i++) {
      DatabaseInstance instance =
          executeWithRetries(sqlAdmin.instances().get(project, instanceName));
      if ("RUNNABLE".equals(instance.getState())) {
        return;
      }
      Thread.sleep(10000);
    }
    throw new RuntimeException("Timeout waiting for instance " + instanceName);
  }

  protected void waitForOperation(Operation operation) throws IOException, InterruptedException {
    String operationId = operation.getName();
    for (int i = 0; i < 120; i++) {
      Operation op = executeWithRetries(sqlAdmin.operations().get(project, operationId));
      if ("DONE".equals(op.getStatus())) {
        if (op.getError() != null) {
          throw new RuntimeException(
              "Operation failed: " + op.getError().getErrors().get(0).getMessage());
        }
        return;
      }
      Thread.sleep(10000);
    }
    throw new RuntimeException("Timeout waiting for operation " + operationId);
  }

  protected CloudSqlResourceManager createManager(String instanceName) {
    String ip = instanceIpMap.get(instanceName);
    if (dbType == SQLDialect.MYSQL) {
      return (CloudSqlResourceManager)
          CloudMySQLResourceManager.builder(instanceName)
              .maybeUseStaticInstance(ip, port, username, password)
              .build();
    } else if (dbType == SQLDialect.POSTGRESQL) {
      return (CloudSqlResourceManager)
          CloudPostgresResourceManager.builder(instanceName)
              .maybeUseStaticInstance(ip, port, username, password)
              .setDatabaseName("postgres")
              .build();
    } else {
      throw new IllegalArgumentException("Unsupported database type: " + dbType);
    }
  }

  protected void createLogicalDatabases() {
    LOG.info("Stage 2: Creating logical databases...");
    ExecutorService executor = getExecutorService();
    List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
    for (Map.Entry<String, List<String>> entry : requestedShardMap.entrySet()) {
      String instanceName = entry.getKey();
      List<String> dbNames = entry.getValue();

      futures.add(
          executor.submit(
              () -> {
                CloudSqlResourceManager manager = createManager(instanceName);
                managers.put(instanceName, manager);
                for (String dbName : dbNames) {
                  manager.createDatabase(dbName);
                }
              }));
    }
    awaitAndShutdownExecutor(executor, futures, "Logical database creation");
  }

  protected String generateAndUploadConfig(String artifactName) {
    LOG.info("Generating and uploading shard configuration...");
    JSONObject config = new JSONObject();
    config.put("configType", "dataflow");
    JSONObject shardConfigBulk = new JSONObject();
    JSONArray dataShards = new JSONArray();

    int shardIdx = 0;
    for (Map.Entry<String, List<String>> entry : requestedShardMap.entrySet()) {
      String instanceName = entry.getKey();
      String ip = instanceIpMap.get(instanceName);
      List<String> dbNames = entry.getValue();

      JSONObject dataShard = new JSONObject();
      dataShard.put("dataShardId", instanceName);
      dataShard.put("host", ip);
      dataShard.put("port", port);
      dataShard.put("user", username);
      dataShard.put("password", password);

      JSONArray databases = new JSONArray();
      for (String dbName : dbNames) {
        JSONObject db = new JSONObject();
        db.put("dbName", dbName);
        db.put("databaseId", String.format("%s%02d%s", "shard_", shardIdx, dbName));
        db.put("refDataShardId", instanceName);
        databases.put(db);
      }
      shardIdx++;
      dataShard.put("databases", databases);
      dataShards.put(dataShard);
    }

    shardConfigBulk.put("dataShards", dataShards);
    config.put("shardConfigurationBulk", shardConfigBulk);

    String configContent = config.toString();
    GcsArtifact artifact =
        (GcsArtifact) gcsResourceManager.createArtifact(artifactName, configContent.getBytes());
    BlobInfo blobInfo = artifact.getBlob().asBlobInfo();

    return String.format("gs://%s/%s", blobInfo.getBucket(), blobInfo.getName());
  }

  protected void awaitAndShutdownExecutor(
      ExecutorService executor, List<java.util.concurrent.Future<?>> futures, String phase) {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(60, TimeUnit.MINUTES)) {
        throw new ShardOrchestrationException(phase + " phase timed out");
      }
      for (java.util.concurrent.Future<?> future : futures) {
        future.get();
      }
    } catch (java.util.concurrent.ExecutionException e) {
      throw new ShardOrchestrationException(phase + " phase failed", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ShardOrchestrationException(phase + " phase interrupted", e);
    }
  }

  /**
   * Purges all logical databases created during the initialization phase.
   *
   * <p>Physical instances are preserved for reuse.
   */
  public void cleanup() {
    LOG.info("Starting cleanup of logical shards");
    if (managers.isEmpty()) {
      return;
    }

    ExecutorService executor = getExecutorService();
    List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
    for (Map.Entry<String, CloudSqlResourceManager> entry : managers.entrySet()) {
      String instanceName = entry.getKey();
      CloudSqlResourceManager manager = entry.getValue();
      List<String> shardDbs = requestedShardMap.get(instanceName);

      futures.add(
          executor.submit(
              () -> {
                if (shardDbs != null) {
                  // We must explicitly drop shard databases here because CloudSqlResourceManager
                  // only tracks the single "primary" database it was initialized with.
                  // It is unaware of additional databases created via
                  // manager.createDatabase(dbName).
                  for (String dbName : shardDbs) {
                    try {
                      manager.dropDatabase(dbName);
                    } catch (Exception e) {
                      LOG.warn(
                          "Failed to drop shard database {} on instance {}", dbName, instanceName);
                    }
                  }
                }
                manager.cleanupAll();
              }));
    }
    awaitAndShutdownExecutor(executor, futures, "Cleanup");
  }
}
