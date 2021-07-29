/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.elasticsearch.transforms;

import com.google.cloud.teleport.v2.elasticsearch.options.ElasticsearchWriteOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test cases for the {@link WriteToElasticsearch} class. */
public class WriteToElasticsearchTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  /** Tests {@link WriteToElasticsearch} throws an exception if a null ConnectionInformation is provided. */
  @Test
  public void testNullConnectionInformation() {

    exceptionRule.expect(IllegalArgumentException.class);

    ElasticsearchWriteOptions options =
        PipelineOptionsFactory.create().as(ElasticsearchWriteOptions.class);

    options.setConnectionUrl(null);
    options.setWriteElasticsearchUsername("test");
    options.setWriteElasticsearchPassword("test");

    pipeline
        .apply("CreateInput", Create.of("test"))
        .apply(WriteToElasticsearch.newBuilder().setOptions(options).build());

    pipeline.run();
  }

  /** Tests {@link WriteToElasticsearch} throws an exception if a null index is provided. */
  @Ignore
  @Test
  public void testNullType() {

    exceptionRule.expect(IllegalArgumentException.class);

    ElasticsearchWriteOptions options =
        PipelineOptionsFactory.create().as(ElasticsearchWriteOptions.class);

    options.setConnectionUrl("https://host.domain");
    options.setWriteElasticsearchUsername("test");
    options.setWriteElasticsearchPassword("test");

    pipeline
        .apply("CreateInput", Create.of("test"))
        .apply(WriteToElasticsearch.newBuilder().setOptions(options).build());

    pipeline.run();
  }

  /**
   * Tests that {@link WriteToElasticsearch} throws an exception if an invalid ConnectionInformation is
   * provided.
   */
  @Test
  public void testInvalidConnectionInformation() {

    exceptionRule.expect(IllegalStateException.class);

    ElasticsearchWriteOptions options =
        PipelineOptionsFactory.create().as(ElasticsearchWriteOptions.class);

    options.setConnectionUrl(",");
    options.setWriteElasticsearchUsername("test");
    options.setWriteElasticsearchPassword("test");

    pipeline
        .apply("CreateInput", Create.of("test"))
        .apply(
            "TestWriteToElasticsearch",
            WriteToElasticsearch.newBuilder().setOptions(options).build());

    pipeline.run();
  }

  /**
   * Tests that {@link WriteToElasticsearch} throws an exception if {@link
   * org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.RetryConfiguration} are invalid.
   */
  @Test
  public void testElasticsearchWriteOptionsRetryConfigMaxAttempts() {

    exceptionRule.expect(IllegalArgumentException.class);

    ElasticsearchWriteOptions options =
        PipelineOptionsFactory.create().as(ElasticsearchWriteOptions.class);

    options.setConnectionUrl("https://host.domain");
    options.setWriteElasticsearchUsername("test");
    options.setWriteElasticsearchPassword("test");
    options.setMaxRetryDuration(500L);
    options.setMaxRetryAttempts(null);

    pipeline
        .apply("CreateInput", Create.of("test"))
        .apply(
            "TestWriteToElasticsearchBadMaxAttempts",
            WriteToElasticsearch.newBuilder().setOptions(options).build());

    pipeline.run();
  }

  /**
   * Tests that {@link WriteToElasticsearch} throws an exception if {@link
   * org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.RetryConfiguration} are invalid.
   */
  @Test
  public void testElasticsearchWriteOptionsRetryConfigMaxDuration() {

    exceptionRule.expect(IllegalArgumentException.class);

    ElasticsearchWriteOptions options =
        PipelineOptionsFactory.create().as(ElasticsearchWriteOptions.class);

    options.setConnectionUrl("https://host.domain");
    options.setWriteElasticsearchUsername("test");
    options.setWriteElasticsearchPassword("test");
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
