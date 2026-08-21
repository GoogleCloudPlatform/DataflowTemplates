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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb.Options;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SpannerToSourceDbTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private Options options;
  @Mock private PCollectionView<Ddl> mockDdlView;
  @Mock private PCollectionView<Ddl> mockShadowTableDdlView;
  @Mock private SpannerConfig mockSpannerConfig;
  @Mock private SpannerConfig mockSpannerMetadataConfig;

  @BeforeClass
  public static void setupFileSystem() {
    FileSystems.setDefaultPipelineOptions(TestPipeline.testingPipelineOptions());
  }

  private List<Shard> shards;
  private Ddl dummyDdl;

  @Before
  public void setUp() {
    Shard shard = new Shard();
    shard.setLogicalShardId("shard1");
    shards = Collections.singletonList(shard);

    dummyDdl =
        Ddl.builder().createTable("shadow_T").column("c1").string().endColumn().endTable().build();

    options = PipelineOptionsFactory.as(Options.class);
    options.setRunMode("regular");
    options.setDeadLetterQueueDirectory("gs://test/dlq");
    options.setSkipDirectoryName("skip");
    options.setFiltrationMode("none");
    options.setSourceType("mysql");
    options.setMaxShardConnections(100L);
    options.setTransformationJarPath("");
    options.setTransformationClassName("");
    options.setTransformationCustomParameters("");
    options.setSessionFilePath("");
    options.setSchemaOverridesFilePath("");
    options.setTableOverrides("");
    options.setColumnOverrides("");

    DataflowPipelineDebugOptions debugOptions = options.as(DataflowPipelineDebugOptions.class);
    debugOptions.setNumberOfWorkerHarnessThreads(0);

    DataflowPipelineWorkerPoolOptions workerPoolOptions =
        options.as(DataflowPipelineWorkerPoolOptions.class);
    workerPoolOptions.setMaxNumWorkers(1);
  }

  @Test
  public void testGetReadChangeStreamDoFn() {
    options.setChangeStreamName("testStream");
    options.setMetadataInstance("testInstance");
    options.setMetadataDatabase("testDB");
    options.setSpannerPriority(RpcPriority.HIGH);
    options.setStartTimestamp("");
    options.setEndTimestamp("");

    SpannerIO.ReadChangeStream readChangeStream =
        SpannerToSourceDb.getReadChangeStreamDoFn(options, mockSpannerConfig);
    Assert.assertNotNull(readChangeStream);
  }

  @Test
  public void testBuildDlqManager() {
    DataflowPipelineOptions dfOptions = options.as(DataflowPipelineOptions.class);
    dfOptions.setTempLocation("gs://test/temp");
    options.setDeadLetterQueueDirectory("");
    options.setDlqMaxRetryCount(3);

    DeadLetterQueueManager dlqManager = SpannerToSourceDb.buildDlqManager(options);
    Assert.assertNotNull(dlqManager);
  }

  @Test
  public void testCalculateConnectionPoolSizePerWorker_Success() {
    int result = SpannerToSourceDb.calculateConnectionPoolSizePerWorker(10L, 2);
    Assert.assertEquals(5, result);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCalculateConnectionPoolSizePerWorker_Failure() {
    SpannerToSourceDb.calculateConnectionPoolSizePerWorker(2L, 10);
  }
}
