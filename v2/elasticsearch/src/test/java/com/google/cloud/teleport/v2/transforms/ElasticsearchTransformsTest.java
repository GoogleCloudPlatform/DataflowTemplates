/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.teleport.v2.transforms.ElasticsearchTransforms.WriteToElasticsearch;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test cases for the {@link ElasticsearchTransforms} class. */
public class ElasticsearchTransformsTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  /** Tests {@link WriteToElasticsearch} throws an exception if a null node address is provided. */
  @Test
  public void testNullNodeAddresses() {

    exceptionRule.expect(IllegalArgumentException.class);

    ElasticsearchTransforms.WriteToElasticsearchOptions options =
        PipelineOptionsFactory.create()
            .as(ElasticsearchTransforms.WriteToElasticsearchOptions.class);

    options.setNodeAddresses(null);
    options.setIndex("testIndex");
    options.setDocumentType("testType");

    pipeline
        .apply("CreateInput", Create.of("test"))
        .apply(WriteToElasticsearch.newBuilder().setOptions(options).build());

    pipeline.run();
  }

  /** Tests {@link WriteToElasticsearch} throws an exception if a null index is provided. */
  @Test
  public void testNullType() {

    exceptionRule.expect(IllegalArgumentException.class);

    ElasticsearchTransforms.WriteToElasticsearchOptions options =
            PipelineOptionsFactory.create()
                    .as(ElasticsearchTransforms.WriteToElasticsearchOptions.class);

    options.setNodeAddresses("http://my-node1");
    options.setIndex("testIndex");
    options.setDocumentType(null);

    pipeline
            .apply("CreateInput", Create.of("test"))
            .apply(WriteToElasticsearch.newBuilder().setOptions(options).build());

    pipeline.run();
  }

  /** Tests {@link WriteToElasticsearch} throws an exception if a null index is provided. */
  @Test
  public void testNullIndex() {

    exceptionRule.expect(IllegalArgumentException.class);

    ElasticsearchTransforms.WriteToElasticsearchOptions options =
            PipelineOptionsFactory.create()
                    .as(ElasticsearchTransforms.WriteToElasticsearchOptions.class);

    options.setNodeAddresses("http://my-node1");
    options.setIndex(null);
    options.setDocumentType("testType");

    pipeline
            .apply("CreateInput", Create.of("test"))
            .apply(WriteToElasticsearch.newBuilder().setOptions(options).build());

    pipeline.run();
  }

  /** Tests that {@link WriteToElasticsearch} throws an exception if an invalid node address is provided. */
  @Test
  public void testInvalidNodeAddresses() {

    exceptionRule.expect(IllegalArgumentException.class);

    ElasticsearchTransforms.WriteToElasticsearchOptions options =
        PipelineOptionsFactory.create()
            .as(ElasticsearchTransforms.WriteToElasticsearchOptions.class);

    options.setNodeAddresses(",");
    options.setIndex("testIndex");
    options.setDocumentType("testType");

    pipeline
        .apply("CreateInput", Create.of("test"))
        .apply(
            "TestWriteToElasticsearch",
            WriteToElasticsearch.newBuilder().setOptions(options).build());

    pipeline.run();
  }



  /** Tests that {@link WriteToElasticsearch} throws an exception if {@link org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.RetryConfiguration} are invalid. */
  @Test
  public void testWriteToElasticsearchOptionsRetryConfigMaxAttempts() {

    exceptionRule.expect(IllegalArgumentException.class);

    ElasticsearchTransforms.WriteToElasticsearchOptions options =
            PipelineOptionsFactory.create()
                    .as(ElasticsearchTransforms.WriteToElasticsearchOptions.class);

    options.setNodeAddresses("http://my-node1");
    options.setIndex("testIndex");
    options.setDocumentType("testDoc");
    options.setMaxRetryDuration(500L);
    options.setMaxRetryAttempts(null);

    pipeline
            .apply("CreateInput", Create.of("test"))
            .apply(
                    "TestWriteToElasticsearchBadMaxAttempts",
                    WriteToElasticsearch.newBuilder().setOptions(options).build());

    pipeline.run();
  }

  /** Tests that {@link WriteToElasticsearch} throws an exception if {@link org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.RetryConfiguration} are invalid. */
  @Test
  public void testWriteToElasticsearchOptionsRetryConfigMaxDuration() {

    exceptionRule.expect(IllegalArgumentException.class);

    ElasticsearchTransforms.WriteToElasticsearchOptions options =
            PipelineOptionsFactory.create()
                    .as(ElasticsearchTransforms.WriteToElasticsearchOptions.class);

    options.setNodeAddresses("http://my-node1");
    options.setIndex("testIndex");
    options.setDocumentType("testDoc");
    options.setMaxRetryDuration(null);
    options.setMaxRetryAttempts(3);

    pipeline
            .apply("CreateInput", Create.of("test"))
            .apply(
                    "TestWriteToElasticsearchBadMaxDuration",
                    WriteToElasticsearch.newBuilder().setOptions(options).build());

    pipeline.run();
  }
}
