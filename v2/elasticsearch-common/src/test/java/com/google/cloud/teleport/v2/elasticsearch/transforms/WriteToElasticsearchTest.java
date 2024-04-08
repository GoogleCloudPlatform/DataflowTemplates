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

import static org.mockito.Mockito.*;

import com.google.cloud.teleport.v2.elasticsearch.options.ElasticsearchWriteOptions;
import com.google.cloud.teleport.v2.elasticsearch.utils.ElasticsearchIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test cases for the {@link WriteToElasticsearch} class. */
public class WriteToElasticsearchTest {

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  /**
   * Tests {@link WriteToElasticsearch} throws an exception if a null ConnectionInformation is
   * provided.
   */
  @Test
  public void testNullConnectionInformation() {

    exceptionRule.expect(IllegalArgumentException.class);

    ElasticsearchWriteOptions options =
        PipelineOptionsFactory.create().as(ElasticsearchWriteOptions.class);

    options.setConnectionUrl(null);
    options.setApiKey("key");

    TestPipeline pipeline = TestPipeline.create();
    pipeline
        .apply("CreateInput", Create.of("test"))
        .apply(WriteToElasticsearch.newBuilder().setOptions(options).build());

    pipeline.run();
  }

  /** Tests {@link WriteToElasticsearch} throws an exception if a null index is provided. */
  @Test
  public void testNullType() {

    exceptionRule.expect(IllegalArgumentException.class);

    ElasticsearchWriteOptions options =
        PipelineOptionsFactory.create().as(ElasticsearchWriteOptions.class);

    options.setConnectionUrl("https://host.domain");
    options.setApiKey("key");

    TestPipeline pipeline = TestPipeline.create();
    pipeline
        .apply("CreateInput", Create.of("test"))
        .apply(WriteToElasticsearch.newBuilder().setOptions(options).build());

    pipeline.run();
  }

  /**
   * Tests that {@link WriteToElasticsearch} throws an exception if an invalid ConnectionInformation
   * is provided.
   */
  @Test
  public void testInvalidConnectionInformation() {

    exceptionRule.expect(IllegalStateException.class);

    ElasticsearchWriteOptions options =
        PipelineOptionsFactory.create().as(ElasticsearchWriteOptions.class);

    options.setConnectionUrl(",");
    options.setIndex("index");
    options.setApiKey("key");

    TestPipeline pipeline = TestPipeline.create();
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
    options.setApiKey("key");
    options.setMaxRetryDuration(500L);
    options.setMaxRetryAttempts(null);

    TestPipeline pipeline = TestPipeline.create();
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
    options.setApiKey("key");
    options.setMaxRetryDuration(null);
    options.setMaxRetryAttempts(3);

    TestPipeline pipeline = TestPipeline.create();
    pipeline
        .apply("CreateInput", Create.of("test"))
        .apply(
            "TestWriteToElasticsearchBadMaxDuration",
            WriteToElasticsearch.newBuilder().setOptions(options).build());

    pipeline.run();
  }

  /**
   * Tests that {@link WriteToElasticsearch} throws an exception if {@link
   * ElasticsearchIO.ConnectionConfiguration} is invalid.
   */
  @Test
  public void testElasticsearchWriteOptionsUserAgent() {
    exceptionRule.expect(IllegalStateException.class);

    ElasticsearchWriteOptions options =
        PipelineOptionsFactory.create().as(ElasticsearchWriteOptions.class);

    options.setConnectionUrl("https://host.domain");
    options.setApiKey("key");
    options.setMaxRetryDuration(null);
    options.setMaxRetryAttempts(3);
    options.setIndex("test-index");
    options.setMaxRetryAttempts(3);
    options.setMaxRetryDuration(10L);

    TestPipeline pipeline = TestPipeline.create();

    pipeline
        .apply("CreateInput", Create.of("test"))
        .apply(
            "TestWriteToElasticsearchBadMaxDuration",
            WriteToElasticsearch.newBuilder().setOptions(options).build());
  }

  @Rule public final transient TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testElasticsearchExpandMethod() {
    ElasticsearchWriteOptions options =
        PipelineOptionsFactory.create().as(ElasticsearchWriteOptions.class);

    options.setConnectionUrl("https://localhost:9200");
    options.setApiKey("key");
    options.setMaxRetryDuration(null);
    options.setMaxRetryAttempts(3);
    options.setIndex("test-index");
    options.setMaxRetryAttempts(3);
    options.setMaxRetryDuration(10L);

    // Create some dummy input data for testing
    PCollection<String> input = testPipeline.apply(Create.of("json string 1", "json string 2"));

    // Create an instance of WriteToElasticsearch using the builder
    WriteToElasticsearch writeToElasticsearch =
        WriteToElasticsearch.newBuilder().setOptions(options).setUserAgent("testUserAgent").build();

    WriteToElasticsearch writeToElasticsearchMock = mock(WriteToElasticsearch.class);
    when(writeToElasticsearchMock.expand(any()))
        .thenReturn(mock(PDone.class)); // Mock the expand method

    testPipeline.run();
    // Call the expand method
    PDone result = writeToElasticsearchMock.expand(input);

    // Verify interactions
    verify(writeToElasticsearchMock, times(1))
        .expand(any()); // Verify that the expand method was called
  }
}
