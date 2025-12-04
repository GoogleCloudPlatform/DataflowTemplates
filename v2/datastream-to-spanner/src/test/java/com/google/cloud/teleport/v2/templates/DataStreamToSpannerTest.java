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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaOverridesParser;
import com.google.cloud.teleport.v2.spanner.migrations.schema.NoopSchemaOverridesParser;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaFileOverridesParser;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaStringOverridesParser;
import com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants;
import com.google.common.io.Resources;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

@RunWith(JUnit4.class)
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
  public void testBuildDlqManager_regularMode_defaultRetryCount_tempLocationWithSlash() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    DataflowPipelineOptions dataflowOptions = mock(DataflowPipelineOptions.class);
    when(options.as(DataflowPipelineOptions.class)).thenReturn(dataflowOptions);
    when(dataflowOptions.getTempLocation()).thenReturn("gs://temp/");
    when(options.getDeadLetterQueueDirectory()).thenReturn("");
    when(options.getDlqMaxRetryCount()).thenReturn(-1);
    when(options.getRunMode()).thenReturn(DatastreamToSpannerConstants.RUN_MODE_REGULAR);

    try (MockedStatic<DeadLetterQueueManager> deadLetterQueueManagerMock =
        mockStatic(DeadLetterQueueManager.class)) {
      ArgumentCaptor<String> dlqDirectoryCaptor = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<Integer> dlqRetryCountCaptor = ArgumentCaptor.forClass(Integer.class);

      deadLetterQueueManagerMock
          .when(() -> DeadLetterQueueManager.create(any(String.class), any(Integer.class)))
          .thenReturn(null);

      DataStreamToSpanner.buildDlqManager(options);

      deadLetterQueueManagerMock.verify(
          () ->
              DeadLetterQueueManager.create(
                  dlqDirectoryCaptor.capture(), dlqRetryCountCaptor.capture()));

      assertThat(dlqDirectoryCaptor.getValue()).isEqualTo("gs://temp/dlq/");
      assertThat(dlqRetryCountCaptor.getValue())
          .isEqualTo(DatastreamToSpannerConstants.RUN_MODE_REGULAR_DEFAULT_RETRY_COUNT);
    }
  }

  @Test
  public void testBuildDlqManager_regularMode_defaultRetryCount_tempLocationWithoutSlash() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    DataflowPipelineOptions dataflowOptions = mock(DataflowPipelineOptions.class);
    when(options.as(DataflowPipelineOptions.class)).thenReturn(dataflowOptions);
    when(dataflowOptions.getTempLocation()).thenReturn("gs://temp");
    when(options.getDeadLetterQueueDirectory()).thenReturn("");
    when(options.getDlqMaxRetryCount()).thenReturn(-1);
    when(options.getRunMode()).thenReturn(DatastreamToSpannerConstants.RUN_MODE_REGULAR);

    try (MockedStatic<DeadLetterQueueManager> deadLetterQueueManagerMock =
        mockStatic(DeadLetterQueueManager.class)) {
      ArgumentCaptor<String> dlqDirectoryCaptor = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<Integer> dlqRetryCountCaptor = ArgumentCaptor.forClass(Integer.class);

      deadLetterQueueManagerMock
          .when(() -> DeadLetterQueueManager.create(any(String.class), any(Integer.class)))
          .thenReturn(null);

      DataStreamToSpanner.buildDlqManager(options);

      deadLetterQueueManagerMock.verify(
          () ->
              DeadLetterQueueManager.create(
                  dlqDirectoryCaptor.capture(), dlqRetryCountCaptor.capture()));

      assertThat(dlqDirectoryCaptor.getValue()).isEqualTo("gs://temp/dlq/");
      assertThat(dlqRetryCountCaptor.getValue())
          .isEqualTo(DatastreamToSpannerConstants.RUN_MODE_REGULAR_DEFAULT_RETRY_COUNT);
    }
  }

  @Test
  public void testBuildDlqManager_regularMode_withDlqDirectory() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    DataflowPipelineOptions dataflowOptions = mock(DataflowPipelineOptions.class);
    when(options.as(DataflowPipelineOptions.class)).thenReturn(dataflowOptions);
    when(dataflowOptions.getTempLocation()).thenReturn("gs://temp/");
    when(options.getDeadLetterQueueDirectory()).thenReturn("gs://dlq/");
    when(options.getDlqMaxRetryCount()).thenReturn(-1);
    when(options.getRunMode()).thenReturn(DatastreamToSpannerConstants.RUN_MODE_REGULAR);

    try (MockedStatic<DeadLetterQueueManager> deadLetterQueueManagerMock =
        mockStatic(DeadLetterQueueManager.class)) {
      ArgumentCaptor<String> dlqDirectoryCaptor = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<Integer> dlqRetryCountCaptor = ArgumentCaptor.forClass(Integer.class);

      deadLetterQueueManagerMock
          .when(() -> DeadLetterQueueManager.create(any(String.class), any(Integer.class)))
          .thenReturn(null);

      DataStreamToSpanner.buildDlqManager(options);

      deadLetterQueueManagerMock.verify(
          () ->
              DeadLetterQueueManager.create(
                  dlqDirectoryCaptor.capture(), dlqRetryCountCaptor.capture()));

      assertThat(dlqDirectoryCaptor.getValue()).isEqualTo("gs://dlq/");
      assertThat(dlqRetryCountCaptor.getValue())
          .isEqualTo(DatastreamToSpannerConstants.RUN_MODE_REGULAR_DEFAULT_RETRY_COUNT);
    }
  }

  @Test
  public void testBuildDlqManager_regularMode_withPositiveRetryCount() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    DataflowPipelineOptions dataflowOptions = mock(DataflowPipelineOptions.class);
    when(options.as(DataflowPipelineOptions.class)).thenReturn(dataflowOptions);
    when(dataflowOptions.getTempLocation()).thenReturn("gs://temp/");
    when(options.getDeadLetterQueueDirectory()).thenReturn("gs://dlq/");
    when(options.getDlqMaxRetryCount()).thenReturn(10);
    when(options.getRunMode()).thenReturn(DatastreamToSpannerConstants.RUN_MODE_REGULAR);

    try (MockedStatic<DeadLetterQueueManager> deadLetterQueueManagerMock =
        mockStatic(DeadLetterQueueManager.class)) {
      ArgumentCaptor<String> dlqDirectoryCaptor = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<Integer> dlqRetryCountCaptor = ArgumentCaptor.forClass(Integer.class);

      deadLetterQueueManagerMock
          .when(() -> DeadLetterQueueManager.create(any(String.class), any(Integer.class)))
          .thenReturn(null);

      DataStreamToSpanner.buildDlqManager(options);

      deadLetterQueueManagerMock.verify(
          () ->
              DeadLetterQueueManager.create(
                  dlqDirectoryCaptor.capture(), dlqRetryCountCaptor.capture()));

      assertThat(dlqDirectoryCaptor.getValue()).isEqualTo("gs://dlq/");
      assertThat(dlqRetryCountCaptor.getValue()).isEqualTo(10);
    }
  }

  @Test
  public void testBuildDlqManager_retryMode_defaultRetryCount() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    DataflowPipelineOptions dataflowOptions = mock(DataflowPipelineOptions.class);
    when(options.as(DataflowPipelineOptions.class)).thenReturn(dataflowOptions);
    when(dataflowOptions.getTempLocation()).thenReturn("gs://temp/");
    when(options.getDeadLetterQueueDirectory()).thenReturn("gs://dlq/");
    when(options.getDlqMaxRetryCount()).thenReturn(-1);
    when(options.getRunMode()).thenReturn(DatastreamToSpannerConstants.RUN_MODE_RETRY);

    ResourceId dlqDirectoryResource = mock(ResourceId.class);
    ResourceId severeDirectoryResource = mock(ResourceId.class);
    when(severeDirectoryResource.toString()).thenReturn("gs://dlq/severe/");

    try (MockedStatic<DeadLetterQueueManager> deadLetterQueueManagerMock =
            mockStatic(DeadLetterQueueManager.class);
        MockedStatic<FileSystems> fileSystemsMock = mockStatic(FileSystems.class)) {

      fileSystemsMock
          .when(() -> FileSystems.matchNewResource("gs://dlq/", true))
          .thenReturn(dlqDirectoryResource);

      when(dlqDirectoryResource.resolve("severe", StandardResolveOptions.RESOLVE_DIRECTORY))
          .thenReturn(severeDirectoryResource);

      ArgumentCaptor<String> dlqDirectoryCaptor = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<String> retryDlqUriCaptor = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<Integer> dlqRetryCountCaptor = ArgumentCaptor.forClass(Integer.class);

      deadLetterQueueManagerMock
          .when(
              () ->
                  DeadLetterQueueManager.create(
                      any(String.class), any(String.class), any(Integer.class)))
          .thenReturn(null);

      DataStreamToSpanner.buildDlqManager(options);

      deadLetterQueueManagerMock.verify(
          () ->
              DeadLetterQueueManager.create(
                  dlqDirectoryCaptor.capture(),
                  retryDlqUriCaptor.capture(),
                  dlqRetryCountCaptor.capture()));

      assertThat(dlqDirectoryCaptor.getValue()).isEqualTo("gs://dlq/");
      assertThat(retryDlqUriCaptor.getValue()).isEqualTo("gs://dlq/severe/");
      assertThat(dlqRetryCountCaptor.getValue())
          .isEqualTo(DatastreamToSpannerConstants.RUN_MODE_RETRY_DEFAULT_RETRY_COUNT);
    }
  }

  @Test
  public void testBuildDlqManager_retryMode_withPositiveRetryCount() {
    DataStreamToSpanner.Options options = mock(DataStreamToSpanner.Options.class);
    DataflowPipelineOptions dataflowOptions = mock(DataflowPipelineOptions.class);
    when(options.as(DataflowPipelineOptions.class)).thenReturn(dataflowOptions);
    when(dataflowOptions.getTempLocation()).thenReturn("gs://temp/");
    when(options.getDeadLetterQueueDirectory()).thenReturn("gs://dlq/");
    when(options.getDlqMaxRetryCount()).thenReturn(20);
    when(options.getRunMode()).thenReturn(DatastreamToSpannerConstants.RUN_MODE_RETRY);

    ResourceId dlqDirectoryResource = mock(ResourceId.class);
    ResourceId severeDirectoryResource = mock(ResourceId.class);
    when(severeDirectoryResource.toString()).thenReturn("gs://dlq/severe/");

    try (MockedStatic<DeadLetterQueueManager> deadLetterQueueManagerMock =
            mockStatic(DeadLetterQueueManager.class);
        MockedStatic<FileSystems> fileSystemsMock = mockStatic(FileSystems.class)) {

      fileSystemsMock
          .when(() -> FileSystems.matchNewResource("gs://dlq/", true))
          .thenReturn(dlqDirectoryResource);

      when(dlqDirectoryResource.resolve("severe", StandardResolveOptions.RESOLVE_DIRECTORY))
          .thenReturn(severeDirectoryResource);

      ArgumentCaptor<String> dlqDirectoryCaptor = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<String> retryDlqUriCaptor = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<Integer> dlqRetryCountCaptor = ArgumentCaptor.forClass(Integer.class);

      deadLetterQueueManagerMock
          .when(
              () ->
                  DeadLetterQueueManager.create(
                      any(String.class), any(String.class), any(Integer.class)))
          .thenReturn(null);

      DataStreamToSpanner.buildDlqManager(options);

      deadLetterQueueManagerMock.verify(
          () ->
              DeadLetterQueueManager.create(
                  dlqDirectoryCaptor.capture(),
                  retryDlqUriCaptor.capture(),
                  dlqRetryCountCaptor.capture()));

      assertThat(dlqDirectoryCaptor.getValue()).isEqualTo("gs://dlq/");
      assertThat(retryDlqUriCaptor.getValue()).isEqualTo("gs://dlq/severe/");
      assertThat(dlqRetryCountCaptor.getValue()).isEqualTo(20);
    }
  }
}
