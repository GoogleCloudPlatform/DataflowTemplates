/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.datastream.v1.model.MysqlSourceConfig;
import com.google.api.services.datastream.v1.model.OracleSourceConfig;
import com.google.api.services.datastream.v1.model.PostgresqlSourceConfig;
import com.google.api.services.datastream.v1.model.SourceConfig;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaOverridesParser;
import com.google.cloud.teleport.v2.spanner.migrations.schema.NoopSchemaOverridesParser;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaFileOverridesParser;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaStringOverridesParser;
import com.google.cloud.teleport.v2.spanner.source.SourceConstants;
import com.google.common.io.Resources;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DataStreamToSpannerTest {

  @Rule public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testGetSourceTypeWithDatastreamSourceType() {
    String[] args = new String[] {"--datastreamSourceType=mysql"};
    DataStreamToSpanner.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamToSpanner.Options.class);
    String result = DataStreamToSpanner.getSourceType(options);

    assertEquals("mysql", result);
  }

  @Test
  public void testGetSourceTypeWithDatastreamInputFilePattern() {
    String[] args =
        new String[] {"--inputFilePattern=gs://test-bkt/", "--directoryWatchDurationInMinutes=42"};
    DataStreamToSpanner.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamToSpanner.Options.class);
    String inputFilePattern = options.getInputFilePattern();
    Integer directoryWatchDurationInMinutes = options.getDirectoryWatchDurationInMinutes();
    Integer expectedWatchDuration = 42;

    assertEquals(inputFilePattern, "gs://test-bkt/");
    assertEquals(directoryWatchDurationInMinutes, expectedWatchDuration);
  }

  @Test
  public void testGetSourceTypeWithEmptyStreamName() {
    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage("Stream name cannot be empty.");
    String[] args = new String[] {""};
    DataStreamToSpanner.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamToSpanner.Options.class);
    String result = DataStreamToSpanner.getSourceType(options);
  }

  @Test
  public void testGetSourceTypeWithGcpCredentialsMissing() {
    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage("Unable to initialize DatastreamClient:");
    String[] args =
        new String[] {
          "--streamName=projects/sample-project/locations/sample-location/streams/sample-stream"
        };
    DataStreamToSpanner.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamToSpanner.Options.class);
    String result = DataStreamToSpanner.getSourceType(options);
  }

  @Test
  public void testConfigureSchemaOverrides_fileBased() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    when(options.getSchemaOverridesFilePath())
        .thenReturn(
            Resources.getResource("DataStreamToSpannerFileOverridesIT/override.json").getPath());
    when(options.getTableOverrides()).thenReturn("");
    when(options.getColumnOverrides()).thenReturn("");

    ISchemaOverridesParser parser = DataStreamToSpanner.configureSchemaOverrides(options);

    assertEquals(SchemaFileOverridesParser.class, parser.getClass());

    // Check the expected values in the overrides
    SchemaFileOverridesParser fileOverridesParser = (SchemaFileOverridesParser) parser;
    String tableOverride = fileOverridesParser.getTableOverride("person1");
    String columnOverride = fileOverridesParser.getColumnOverride("person1", "first_name1");
    assertEquals("human1", tableOverride);
    assertEquals("name1", columnOverride);
  }

  @Test
  public void testConfigureSchemaOverrides_stringBased() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    when(options.getSchemaOverridesFilePath()).thenReturn("");
    when(options.getTableOverrides()).thenReturn("[{person1, human1}]");
    when(options.getColumnOverrides()).thenReturn("[{person1.first_name1, person1.name1}]");

    ISchemaOverridesParser parser = DataStreamToSpanner.configureSchemaOverrides(options);

    assertEquals(SchemaStringOverridesParser.class, parser.getClass());

    // Check the expected values in the overrides
    SchemaStringOverridesParser stringParser = (SchemaStringOverridesParser) parser;
    String tableOverride = stringParser.getTableOverride("person1");
    String columnOverride = stringParser.getColumnOverride("person1", "first_name1");
    assertEquals("human1", tableOverride);
    assertEquals("name1", columnOverride);
  }

  @Test
  public void testConfigureSchemaOverrides_noOverrides() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    when(options.getSchemaOverridesFilePath()).thenReturn("");
    when(options.getTableOverrides()).thenReturn("");
    when(options.getColumnOverrides()).thenReturn("");

    ISchemaOverridesParser parser = DataStreamToSpanner.configureSchemaOverrides(options);

    assertEquals(NoopSchemaOverridesParser.class, parser.getClass());
  }

  @Test
  public void testConfigureSchemaOverrides_incorrectConfiguration() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    when(options.getSchemaOverridesFilePath()).thenReturn("/path/to/overrides.json");
    when(options.getTableOverrides()).thenReturn("table1=schema1");

    assertThrows(
        IllegalArgumentException.class,
        () -> DataStreamToSpanner.configureSchemaOverrides(options));
  }

  @Test
  public void testConfigureSchemaOverrides_onlyTableOverrides() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    when(options.getSchemaOverridesFilePath()).thenReturn("");
    when(options.getTableOverrides()).thenReturn("[{person1, human1}]");
    when(options.getColumnOverrides()).thenReturn("");

    ISchemaOverridesParser parser = DataStreamToSpanner.configureSchemaOverrides(options);

    assertEquals(SchemaStringOverridesParser.class, parser.getClass());
  }

  @Test
  public void testConfigureSchemaOverrides_onlyColumnOverrides() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    when(options.getSchemaOverridesFilePath()).thenReturn("");
    when(options.getTableOverrides()).thenReturn("");
    when(options.getColumnOverrides()).thenReturn("[{person1.first_name1, person1.name1}]");

    ISchemaOverridesParser parser = DataStreamToSpanner.configureSchemaOverrides(options);

    assertEquals(SchemaStringOverridesParser.class, parser.getClass());
  }

  @Test
  public void testConfigureSchemaOverrides_incorrectConfiguration_columnOverrides() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    when(options.getSchemaOverridesFilePath()).thenReturn("/path/to/overrides.json");
    when(options.getTableOverrides()).thenReturn("");
    when(options.getColumnOverrides()).thenReturn("col1=col2");

    assertThrows(
        IllegalArgumentException.class,
        () -> DataStreamToSpanner.configureSchemaOverrides(options));
  }

  @Test
  public void testValidateSourceType_validSource() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    when(options.getRunMode()).thenReturn("");
    when(options.getDatastreamSourceType()).thenReturn("mysql");

    DataStreamToSpanner.validateSourceType(options);

    verify(options).setDatastreamSourceType("mysql");
  }

  @Test
  public void testValidateSourceType_invalidSource() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    when(options.getRunMode()).thenReturn("");
    when(options.getDatastreamSourceType()).thenReturn("invalid_source");

    assertThrows(
        IllegalArgumentException.class, () -> DataStreamToSpanner.validateSourceType(options));
  }

  @Test
  public void testValidateSourceType_retryMode() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    when(options.getRunMode()).thenReturn(Constants.RUN_MODE_RETRY_DLQ);

    DataStreamToSpanner.validateSourceType(options);

    verify(options, times(0)).setDatastreamSourceType(anyString());
  }

  @Test
  public void testGetShadowTableSpannerConfig_validInput() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    when(options.getShadowTableSpannerInstanceId()).thenReturn("shadow-instance-id");
    when(options.getShadowTableSpannerDatabaseId()).thenReturn("shadow-database-id");
    when(options.getProjectId()).thenReturn("project-id");

    SpannerConfig spannerConfig = DataStreamToSpanner.getShadowTableSpannerConfig(options);

    assertEquals("shadow-instance-id", spannerConfig.getInstanceId().get());
    assertEquals("shadow-database-id", spannerConfig.getDatabaseId().get());
    assertEquals("project-id", spannerConfig.getProjectId().get());
  }

  @Test
  public void testGetShadowTableSpannerConfig_missingInstanceId() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    when(options.getShadowTableSpannerInstanceId()).thenReturn("");
    when(options.getShadowTableSpannerDatabaseId()).thenReturn("shadow-database-id");

    assertThrows(
        IllegalArgumentException.class,
        () -> DataStreamToSpanner.getShadowTableSpannerConfig(options));
  }

  @Test
  public void testGetShadowTableSpannerConfig_missingDatabaseId() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    when(options.getShadowTableSpannerInstanceId()).thenReturn("shadow-instance-id");
    when(options.getShadowTableSpannerDatabaseId()).thenReturn("");

    assertThrows(
        IllegalArgumentException.class,
        () -> DataStreamToSpanner.getShadowTableSpannerConfig(options));
  }

  @Test
  public void testGetShadowTableSpannerConfig_defaultValues() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    when(options.getShadowTableSpannerInstanceId()).thenReturn("");
    when(options.getShadowTableSpannerDatabaseId()).thenReturn("");
    when(options.getInstanceId()).thenReturn("main-instance-id");
    when(options.getDatabaseId()).thenReturn("main-database-id");
    when(options.getProjectId()).thenReturn("project-id");

    SpannerConfig spannerConfig = DataStreamToSpanner.getShadowTableSpannerConfig(options);

    assertEquals("main-instance-id", spannerConfig.getInstanceId().get());
    assertEquals("main-database-id", spannerConfig.getDatabaseId().get());
    assertEquals("project-id", spannerConfig.getProjectId().get());
  }

  @Test
  public void testBuildDlqManager_defaultTempLocation() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    DataflowPipelineOptions dfOptions = mock(DataflowPipelineOptions.class);
    when(options.as(DataflowPipelineOptions.class)).thenReturn(dfOptions);
    when(dfOptions.getTempLocation()).thenReturn("/tmp/test-bucket/temp");
    when(options.getDeadLetterQueueDirectory()).thenReturn("");
    when(options.getDlqMaxRetryCount()).thenReturn(500);

    DataStreamToSpanner.buildDlqManager(options);

    verify(options).setDeadLetterQueueDirectory("/tmp/test-bucket/temp/dlq/");
  }

  @Test
  public void testBuildDlqManager_tempLocationWithTrailingSlash() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    DataflowPipelineOptions dfOptions = mock(DataflowPipelineOptions.class);
    when(options.as(DataflowPipelineOptions.class)).thenReturn(dfOptions);
    when(dfOptions.getTempLocation()).thenReturn("/tmp/test-bucket/temp/");
    when(options.getDeadLetterQueueDirectory()).thenReturn("");
    when(options.getDlqMaxRetryCount()).thenReturn(500);

    DataStreamToSpanner.buildDlqManager(options);

    verify(options).setDeadLetterQueueDirectory("/tmp/test-bucket/temp/dlq/");
  }

  @Test
  public void testBuildDlqManager_withDlqDirectory() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    DataflowPipelineOptions dfOptions = mock(DataflowPipelineOptions.class);
    when(options.as(DataflowPipelineOptions.class)).thenReturn(dfOptions);
    when(dfOptions.getTempLocation()).thenReturn("/tmp/test-bucket/temp");
    when(options.getDeadLetterQueueDirectory()).thenReturn("/tmp/custom-dlq");
    when(options.getDlqMaxRetryCount()).thenReturn(500);

    DataStreamToSpanner.buildDlqManager(options);

    verify(options).setDeadLetterQueueDirectory("/tmp/custom-dlq");
  }

  @Test
  public void testBuildPipeline() throws Exception {
    String[] args =
        new String[] {
          "--sessionFilePath="
              + Resources.getResource("DataStreamToSpannerIT/mysql-session.json").getPath(),
          "--projectId=project-id",
          "--instanceId=instance-id",
          "--databaseId=database-id",
          "--spannerHost=https://batch-spanner.googleapis.com",
          "--shouldCreateShadowTables=true",
          "--shadowTablePrefix=shadow_",
          "--datastreamSourceType=mysql",
          "--runMode=" + Constants.RUN_MODE_REGULAR,
          "--dlqMaxRetryCount=500",
          "--dlqRetryMinutes=10",
          "--directoryWatchDurationInMinutes=10",
          "--inputFilePattern=gs://test-bucket/events/*",
          "--inputFileFormat=avro",
          "--tempLocation=gs://test-bucket/temp",
          "--workerMachineType=n1-standard-4"
        };

    DataStreamToSpanner.Options options =
        PipelineOptionsFactory.fromArgs(args).as(DataStreamToSpanner.Options.class);

    Pipeline pipeline = DataStreamToSpanner.buildPipeline(options);

    assertNotNull(pipeline);
  }

  @Test
  public void testBuildPipeline_retryDLQ() throws Exception {
    String[] args =
        new String[] {
          "--sessionFilePath="
              + Resources.getResource("DataStreamToSpannerIT/mysql-session.json").getPath(),
          "--projectId=project-id",
          "--instanceId=instance-id",
          "--databaseId=database-id",
          "--spannerHost=https://batch-spanner.googleapis.com",
          "--shouldCreateShadowTables=true",
          "--shadowTablePrefix=shadow_",
          "--datastreamSourceType=mysql",
          "--runMode=" + Constants.RUN_MODE_RETRY_DLQ,
          "--dlqMaxRetryCount=500",
          "--dlqRetryMinutes=10",
          "--directoryWatchDurationInMinutes=10",
          "--inputFilePattern=gs://test-bucket/events/*",
          "--inputFileFormat=avro",
          "--tempLocation=gs://test-bucket/temp",
          "--workerMachineType=n1-standard-4"
        };

    DataStreamToSpanner.Options options =
        PipelineOptionsFactory.fromArgs(args).as(DataStreamToSpanner.Options.class);

    Pipeline pipeline = DataStreamToSpanner.buildPipeline(options);

    assertNotNull(pipeline);
  }

  @Test
  public void testBuildPipeline_retryAllDLQ() throws Exception {
    String[] args =
        new String[] {
          "--sessionFilePath="
              + Resources.getResource("DataStreamToSpannerIT/mysql-session.json").getPath(),
          "--projectId=project-id",
          "--instanceId=instance-id",
          "--databaseId=database-id",
          "--spannerHost=https://batch-spanner.googleapis.com",
          "--shouldCreateShadowTables=true",
          "--shadowTablePrefix=shadow_",
          "--datastreamSourceType=mysql",
          "--runMode=" + Constants.RUN_MODE_RETRY_ALL_DLQ,
          "--dlqMaxRetryCount=500",
          "--dlqRetryMinutes=10",
          "--directoryWatchDurationInMinutes=10",
          "--inputFilePattern=gs://test-bucket/events/*",
          "--inputFileFormat=avro",
          "--tempLocation=gs://test-bucket/temp",
          "--workerMachineType=n1-standard-4"
        };

    DataStreamToSpanner.Options options =
        PipelineOptionsFactory.fromArgs(args).as(DataStreamToSpanner.Options.class);

    Pipeline pipeline = DataStreamToSpanner.buildPipeline(options);

    assertNotNull(pipeline);
  }

  @Test
  public void testBuildPipeline_withDlqGcsPubSubSubscription() throws Exception {
    String[] args =
        new String[] {
          "--sessionFilePath="
              + Resources.getResource("DataStreamToSpannerIT/mysql-session.json").getPath(),
          "--projectId=project-id",
          "--instanceId=instance-id",
          "--databaseId=database-id",
          "--spannerHost=https://batch-spanner.googleapis.com",
          "--shouldCreateShadowTables=true",
          "--shadowTablePrefix=shadow_",
          "--datastreamSourceType=mysql",
          "--runMode=" + Constants.RUN_MODE_REGULAR,
          "--dlqMaxRetryCount=500",
          "--dlqRetryMinutes=10",
          "--directoryWatchDurationInMinutes=10",
          "--inputFilePattern=gs://test-bucket/events/*",
          "--inputFileFormat=avro",
          "--tempLocation=gs://test-bucket/temp",
          "--workerMachineType=n1-standard-4",
          "--dlqGcsPubSubSubscription=projects/project-id/subscriptions/sub-id"
        };

    DataStreamToSpanner.Options options =
        PipelineOptionsFactory.fromArgs(args).as(DataStreamToSpanner.Options.class);

    Pipeline pipeline = DataStreamToSpanner.buildPipeline(options);

    assertNotNull(pipeline);
  }

  @Test
  public void testGetSourceTypeFromConfig_mysql() {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setMysqlSourceConfig(new MysqlSourceConfig());

    String sourceType = DataStreamToSpanner.getSourceTypeFromConfig(sourceConfig);

    assertEquals(SourceConstants.MYSQL_SOURCE_TYPE, sourceType);
  }

  @Test
  public void testGetSourceTypeFromConfig_oracle() {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setOracleSourceConfig(new OracleSourceConfig());

    String sourceType = DataStreamToSpanner.getSourceTypeFromConfig(sourceConfig);

    assertEquals(SourceConstants.ORACLE_SOURCE_TYPE, sourceType);
  }

  @Test
  public void testGetSourceTypeFromConfig_postgres() {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setPostgresqlSourceConfig(new PostgresqlSourceConfig());

    String sourceType = DataStreamToSpanner.getSourceTypeFromConfig(sourceConfig);

    assertEquals(SourceConstants.POSTGRES_SOURCE_TYPE, sourceType);
  }

  @Test
  public void testGetSourceTypeFromConfig_unsupported() {
    SourceConfig sourceConfig = new SourceConfig();

    assertThrows(
        IllegalArgumentException.class,
        () -> DataStreamToSpanner.getSourceTypeFromConfig(sourceConfig));
  }

  @Test
  public void testBuildPipeline_withAllOptions() throws Exception {
    String[] args =
        new String[] {
          "--sessionFilePath="
              + Resources.getResource("DataStreamToSpannerIT/mysql-session.json").getPath(),
          "--projectId=project-id",
          "--instanceId=instance-id",
          "--databaseId=database-id",
          "--spannerHost=https://batch-spanner.googleapis.com",
          "--shouldCreateShadowTables=true",
          "--shadowTablePrefix=shadow_",
          "--datastreamSourceType=mysql",
          "--runMode=" + Constants.RUN_MODE_REGULAR,
          "--dlqMaxRetryCount=500",
          "--dlqRetryMinutes=10",
          "--directoryWatchDurationInMinutes=10",
          "--inputFilePattern=gs://test-bucket/events/*",
          "--inputFileFormat=avro",
          "--tempLocation=gs://test-bucket/temp/", // Trailing slash
          "--workerMachineType=n1-standard-4",
          "--maxNumWorkers=5", // Non-zero
          "--filteredEventsDirectory=gs://test-bucket/customFilteredEvents", // Non-empty
          "--shadowTableSpannerInstanceId=shadow-instance-id", // Set shadow instance
          "--shadowTableSpannerDatabaseId=shadow-database-id" // Set shadow database
        };

    DataStreamToSpanner.Options options =
        PipelineOptionsFactory.fromArgs(args).as(DataStreamToSpanner.Options.class);

    Pipeline pipeline = DataStreamToSpanner.buildPipeline(options);

    assertNotNull(pipeline);
  }
}
