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

import static com.google.cloud.teleport.v2.templates.loadtesting.CloudSqlShardOrchestrator.MYSQL_8_0;
import static com.google.cloud.teleport.v2.templates.loadtesting.CloudSqlShardOrchestrator.POSTGRES_14;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.api.services.sqladmin.SQLAdmin;
import com.google.api.services.sqladmin.model.DatabaseInstance;
import com.google.api.services.sqladmin.model.IpMapping;
import com.google.api.services.sqladmin.model.Operation;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.beam.it.gcp.artifacts.GcsArtifact;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudPostgresResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

/** Unit tests for {@link CloudSqlShardOrchestrator}. */
@RunWith(MockitoJUnitRunner.class)
public class CloudSqlShardOrchestratorTest {

  private static final String PROJECT_ID = "test_project";
  private static final String REGION = "test_region";
  private static final String INSTANCE_NAME = "instance-1";
  private static final String PRIVATE_IP = "10.0.0.1";

  @Mock private GcsResourceManager gcsResourceManager;
  @Mock private GcsArtifact mockArtifact;
  @Mock private Blob mockBlob;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private SQLAdmin sqlAdmin;

  @Mock private CloudMySQLResourceManager mockMySqlManager;
  @Mock private CloudMySQLResourceManager.Builder mockMySqlBuilder;
  @Mock private CloudPostgresResourceManager mockPostgresManager;
  @Mock private CloudPostgresResourceManager.Builder mockPostgresBuilder;

  private Map<String, List<String>> shardMap;

  /** Helper subclass to inject mock SQLAdmin and synchronous executor. */
  private static class TestCloudSqlShardOrchestrator extends CloudSqlShardOrchestrator {
    public TestCloudSqlShardOrchestrator(
        SQLDialect sqlDialect,
        String dbVersion,
        String project,
        String region,
        GcsResourceManager gcsResourceManager,
        SQLAdmin sqlAdmin) {
      super(sqlDialect, dbVersion, project, region, gcsResourceManager);
      // Overwrite the real sqlAdmin created in super constructor
      try {
        java.lang.reflect.Field field =
            CloudSqlShardOrchestrator.class.getDeclaredField("sqlAdmin");
        field.setAccessible(true);
        field.set(this, sqlAdmin);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected ExecutorService getExecutorService() {
      return MoreExecutors.newDirectExecutorService();
    }
  }

  @Before
  public void setUp() {
    shardMap = Map.of(INSTANCE_NAME, Arrays.asList("db1", "db2"));

    // Mock GCS
    when(mockArtifact.getBlob()).thenReturn(mockBlob);
    when(gcsResourceManager.createArtifact(anyString(), any(byte[].class)))
        .thenReturn(mockArtifact);

    // Mock MySQL Builder
    when(mockMySqlBuilder.maybeUseStaticInstance(anyString(), anyInt(), anyString(), anyString()))
        .thenReturn(mockMySqlBuilder);
    when(mockMySqlBuilder.build()).thenReturn(mockMySqlManager);

    // Mock Postgres Builder
    when(mockPostgresBuilder.maybeUseStaticInstance(
            anyString(), anyInt(), anyString(), anyString()))
        .thenReturn(mockPostgresBuilder);
    when(mockPostgresBuilder.setDatabaseName(anyString())).thenReturn(mockPostgresBuilder);
    when(mockPostgresBuilder.build()).thenReturn(mockPostgresManager);
  }

  @Test
  public void testInitialize_provisionsAndSetsUpCorrectly() throws Exception {
    TestCloudSqlShardOrchestrator orchestrator =
        new TestCloudSqlShardOrchestrator(
            SQLDialect.MYSQL, MYSQL_8_0, PROJECT_ID, REGION, gcsResourceManager, sqlAdmin);

    // Mock Stage 1: Physical Provisioning
    DatabaseInstance instance =
        new DatabaseInstance()
            .setState("RUNNABLE")
            .setIpAddresses(
                Collections.singletonList(
                    new IpMapping().setType("PRIVATE").setIpAddress(PRIVATE_IP)));

    when(sqlAdmin.instances().get(PROJECT_ID, INSTANCE_NAME).execute()).thenReturn(instance);

    Operation passwordOp = new Operation().setName("pw-op").setStatus("DONE");
    when(sqlAdmin.users().update(eq(PROJECT_ID), eq(INSTANCE_NAME), any()).execute())
        .thenReturn(passwordOp);
    when(sqlAdmin.operations().get(PROJECT_ID, "pw-op").execute()).thenReturn(passwordOp);

    try (MockedStatic<CloudMySQLResourceManager> mockedMySql =
        mockStatic(CloudMySQLResourceManager.class)) {
      mockedMySql
          .when(() -> CloudMySQLResourceManager.builder(anyString()))
          .thenReturn(mockMySqlBuilder);

      when(mockBlob.asBlobInfo())
          .thenReturn(BlobInfo.newBuilder("test-bucket", "test-run/shards.json").build());

      String configPath = orchestrator.initialize(shardMap, "shards.json");

      // Verify config path
      assertThat(configPath).isEqualTo("gs://test-bucket/test-run/shards.json");

      // Verify Manager creation with discovered IP and correct MySQL port
      verify(mockMySqlBuilder)
          .maybeUseStaticInstance(eq(PRIVATE_IP), eq(3306), anyString(), anyString());

      // Verify Database creation delegation
      verify(mockMySqlManager).createDatabase("db1");
      verify(mockMySqlManager).createDatabase("db2");

      // Verify artifact content
      ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
      verify(gcsResourceManager).createArtifact(eq("shards.json"), captor.capture());
      String content = new String(captor.getValue());
      assertThat(content).contains("\"host\":\"" + PRIVATE_IP + "\"");
    }
  }

  @Test
  public void testInitialize_createsInstance_whenMissing() throws Exception {
    TestCloudSqlShardOrchestrator orchestrator =
        new TestCloudSqlShardOrchestrator(
            SQLDialect.MYSQL, MYSQL_8_0, PROJECT_ID, REGION, gcsResourceManager, sqlAdmin);

    // Mock 404 for first call, then 200 for refresh
    when(sqlAdmin.instances().get(PROJECT_ID, INSTANCE_NAME).execute())
        .thenThrow(new IOException("404 Not Found"))
        .thenReturn(
            new DatabaseInstance()
                .setState("RUNNABLE")
                .setIpAddresses(
                    Collections.singletonList(
                        new IpMapping().setType("PRIVATE").setIpAddress(PRIVATE_IP))));

    Operation op = new Operation().setName("op1").setStatus("DONE");
    when(sqlAdmin.instances().insert(eq(PROJECT_ID), any(DatabaseInstance.class)).execute())
        .thenReturn(op);
    when(sqlAdmin.operations().get(PROJECT_ID, "op1").execute()).thenReturn(op);

    Operation passwordOp = new Operation().setName("pw-op").setStatus("DONE");
    when(sqlAdmin.users().update(eq(PROJECT_ID), eq(INSTANCE_NAME), any()).execute())
        .thenReturn(passwordOp);
    when(sqlAdmin.operations().get(PROJECT_ID, "pw-op").execute()).thenReturn(passwordOp);

    try (MockedStatic<CloudMySQLResourceManager> mockedMySql =
        mockStatic(CloudMySQLResourceManager.class)) {
      mockedMySql
          .when(() -> CloudMySQLResourceManager.builder(anyString()))
          .thenReturn(mockMySqlBuilder);
      when(mockBlob.asBlobInfo()).thenReturn(BlobInfo.newBuilder("b", "r/shards.json").build());

      orchestrator.initialize(shardMap, "shards.json");

      verify(sqlAdmin.instances()).insert(eq(PROJECT_ID), any(DatabaseInstance.class));
    }
  }

  @Test
  public void testCleanup_delegatesToManagers() throws Exception {
    TestCloudSqlShardOrchestrator orchestrator =
        new TestCloudSqlShardOrchestrator(
            SQLDialect.MYSQL, MYSQL_8_0, PROJECT_ID, REGION, gcsResourceManager, sqlAdmin);

    // Mock successful initialization to populate managers map
    when(sqlAdmin.instances().get(PROJECT_ID, INSTANCE_NAME).execute())
        .thenReturn(
            new DatabaseInstance()
                .setState("RUNNABLE")
                .setIpAddresses(
                    Collections.singletonList(
                        new IpMapping().setType("PRIVATE").setIpAddress(PRIVATE_IP))));

    Operation passwordOp = new Operation().setName("pw-op").setStatus("DONE");
    when(sqlAdmin.users().update(eq(PROJECT_ID), eq(INSTANCE_NAME), any()).execute())
        .thenReturn(passwordOp);
    when(sqlAdmin.operations().get(PROJECT_ID, "pw-op").execute()).thenReturn(passwordOp);

    try (MockedStatic<CloudMySQLResourceManager> mockedMySql =
        mockStatic(CloudMySQLResourceManager.class)) {
      mockedMySql
          .when(() -> CloudMySQLResourceManager.builder(anyString()))
          .thenReturn(mockMySqlBuilder);
      when(mockBlob.asBlobInfo()).thenReturn(BlobInfo.newBuilder("b", "r/shards.json").build());

      orchestrator.initialize(shardMap, "shards.json");
      orchestrator.cleanup();

      verify(mockMySqlManager).cleanupAll();
    }
  }

  @Test
  public void testConstructor_withDefaultCredentials() throws Exception {
    try (MockedStatic<GoogleCredentials> mockedCredentials = mockStatic(GoogleCredentials.class);
        MockedStatic<GoogleNetHttpTransport> mockedTransport =
            mockStatic(GoogleNetHttpTransport.class)) {
      mockedCredentials
          .when(GoogleCredentials::getApplicationDefault)
          .thenReturn(mock(GoogleCredentials.class));
      mockedTransport
          .when(GoogleNetHttpTransport::newTrustedTransport)
          .thenReturn(mock(com.google.api.client.http.javanet.NetHttpTransport.class));

      CloudSqlShardOrchestrator orchestrator =
          new CloudSqlShardOrchestrator(
              SQLDialect.MYSQL, MYSQL_8_0, PROJECT_ID, REGION, gcsResourceManager);

      assertThat(orchestrator.project).isEqualTo(PROJECT_ID);
    }
  }

  @Test
  public void testInitialize_provisionsAndSetsUpPostgresCorrectly() throws Exception {
    TestCloudSqlShardOrchestrator orchestrator =
        new TestCloudSqlShardOrchestrator(
            SQLDialect.POSTGRESQL, POSTGRES_14, PROJECT_ID, REGION, gcsResourceManager, sqlAdmin);

    DatabaseInstance instance =
        new DatabaseInstance()
            .setState("RUNNABLE")
            .setIpAddresses(
                Collections.singletonList(
                    new IpMapping().setType("PRIVATE").setIpAddress(PRIVATE_IP)));

    when(sqlAdmin.instances().get(PROJECT_ID, INSTANCE_NAME).execute()).thenReturn(instance);

    Operation passwordOp = new Operation().setName("pw-op").setStatus("DONE");
    when(sqlAdmin.users().update(eq(PROJECT_ID), eq(INSTANCE_NAME), any()).execute())
        .thenReturn(passwordOp);
    when(sqlAdmin.operations().get(PROJECT_ID, "pw-op").execute()).thenReturn(passwordOp);

    try (MockedStatic<CloudPostgresResourceManager> mockedPostgres =
        mockStatic(CloudPostgresResourceManager.class)) {
      mockedPostgres
          .when(() -> CloudPostgresResourceManager.builder(anyString()))
          .thenReturn(mockPostgresBuilder);

      when(mockBlob.asBlobInfo()).thenReturn(BlobInfo.newBuilder("b", "r").build());

      orchestrator.initialize(shardMap, "shards.json");

      verify(mockPostgresBuilder)
          .maybeUseStaticInstance(eq(PRIVATE_IP), eq(5432), anyString(), anyString());
      verify(mockPostgresManager).createDatabase("db1");
    }
  }

  @Test
  public void testInitialize_throwsOnFailure() throws Exception {
    TestCloudSqlShardOrchestrator orchestrator =
        new TestCloudSqlShardOrchestrator(
            SQLDialect.MYSQL, MYSQL_8_0, PROJECT_ID, REGION, gcsResourceManager, sqlAdmin);

    when(sqlAdmin.instances().get(anyString(), anyString()).execute())
        .thenThrow(new IOException("API Error"));

    assertThrows(
        ShardOrchestrationException.class, () -> orchestrator.initialize(shardMap, "shards.json"));
  }

  @Test
  public void testExecuteWithRetries_retriesOn409() throws Exception {
    TestCloudSqlShardOrchestrator orchestrator =
        new TestCloudSqlShardOrchestrator(
            SQLDialect.MYSQL, MYSQL_8_0, PROJECT_ID, REGION, gcsResourceManager, sqlAdmin);

    com.google.api.client.googleapis.services.AbstractGoogleClientRequest<DatabaseInstance>
        mockRequest =
            mock(com.google.api.client.googleapis.services.AbstractGoogleClientRequest.class);

    GoogleJsonResponseException exception409 =
        new GoogleJsonResponseException(
            new HttpResponseException.Builder(409, "Conflict", new HttpHeaders()), null);

    DatabaseInstance instance = new DatabaseInstance().setName("inst1");

    when(mockRequest.execute()).thenThrow(exception409).thenReturn(instance);

    DatabaseInstance result = orchestrator.executeWithRetries(mockRequest);

    assertThat(result).isEqualTo(instance);
    verify(mockRequest, times(2)).execute();
  }

  @Test
  public void testWaitForOperation_throwsOnOpError() throws Exception {
    TestCloudSqlShardOrchestrator orchestrator =
        new TestCloudSqlShardOrchestrator(
            SQLDialect.MYSQL, MYSQL_8_0, PROJECT_ID, REGION, gcsResourceManager, sqlAdmin);

    Operation op = mock(Operation.class, Answers.RETURNS_DEEP_STUBS);
    when(op.getName()).thenReturn("op-error");
    when(op.getStatus()).thenReturn("DONE");
    when(op.getError().getErrors().get(0).getMessage()).thenReturn("Operation Failed Message");

    when(sqlAdmin.operations().get(PROJECT_ID, "op-error").execute()).thenReturn(op);

    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> orchestrator.waitForOperation(op));
    assertThat(ex.getMessage()).contains("Operation failed: Operation Failed Message");
  }

  @Test
  public void testCleanup_handlesDropDatabaseError() throws Exception {
    TestCloudSqlShardOrchestrator orchestrator =
        new TestCloudSqlShardOrchestrator(
            SQLDialect.MYSQL, MYSQL_8_0, PROJECT_ID, REGION, gcsResourceManager, sqlAdmin);

    // Mock successful initialization
    DatabaseInstance instance =
        new DatabaseInstance()
            .setState("RUNNABLE")
            .setIpAddresses(
                Collections.singletonList(
                    new IpMapping().setType("PRIVATE").setIpAddress(PRIVATE_IP)));
    when(sqlAdmin.instances().get(PROJECT_ID, INSTANCE_NAME).execute()).thenReturn(instance);
    Operation passwordOp = new Operation().setName("pw-op").setStatus("DONE");
    when(sqlAdmin.users().update(anyString(), anyString(), any()).execute()).thenReturn(passwordOp);
    when(sqlAdmin.operations().get(anyString(), anyString()).execute()).thenReturn(passwordOp);

    try (MockedStatic<CloudMySQLResourceManager> mockedMySql =
        mockStatic(CloudMySQLResourceManager.class)) {
      mockedMySql
          .when(() -> CloudMySQLResourceManager.builder(anyString()))
          .thenReturn(mockMySqlBuilder);
      when(mockBlob.asBlobInfo()).thenReturn(BlobInfo.newBuilder("b", "r").build());

      orchestrator.initialize(shardMap, "shards.json");

      // Mock error on dropDatabase
      doThrow(new RuntimeException("Drop Failed")).when(mockMySqlManager).dropDatabase("db1");

      orchestrator.cleanup();

      verify(mockMySqlManager).dropDatabase("db1");
      verify(mockMySqlManager).dropDatabase("db2");
      verify(mockMySqlManager).cleanupAll();
    }
  }

  @Test
  public void testVersionCompatability() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CloudSqlShardOrchestrator.checkVersionCompatibility(SQLDialect.MYSQL, POSTGRES_14));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            CloudSqlShardOrchestrator.checkVersionCompatibility(SQLDialect.POSTGRESQL, MYSQL_8_0));

    CloudSqlShardOrchestrator.checkVersionCompatibility(SQLDialect.MYSQL, MYSQL_8_0);
    CloudSqlShardOrchestrator.checkVersionCompatibility(SQLDialect.POSTGRESQL, POSTGRES_14);
  }
}
