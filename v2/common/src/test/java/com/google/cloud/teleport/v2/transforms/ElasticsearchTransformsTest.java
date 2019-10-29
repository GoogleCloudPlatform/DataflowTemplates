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

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.transforms.ElasticsearchTransforms.WriteToElasticsearch;
import com.google.common.io.Resources;
import java.io.IOException;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test cases for the {@link ElasticsearchTransforms} class. */
public class ElasticsearchTransformsTest {

  private static final String RESOURCES_DIR = "JavascriptTextTransformerTest/";

  private static final String TRANSFORM_FILE_PATH =
          Resources.getResource(RESOURCES_DIR + "transform.js").getPath();

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

  /** Tests that {@link ElasticsearchTransforms.ValueExtractorFn} returns the correct value. */
  @Test
  public void testValueExtractorFn() throws IOException {

    final String stringifiedJsonRecord = "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23}";

    final String jsonRecord = "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23,\"someProp\":\"someValue\"}";

    ObjectMapper objectMapper = new ObjectMapper();

    JsonNode jsonNode = objectMapper.readTree(stringifiedJsonRecord);

    String result = ElasticsearchTransforms.ValueExtractorFn.newBuilder()
        .setFileSystemPath(TRANSFORM_FILE_PATH)
        .setFunctionName("transform")
        .build()
        .apply(jsonNode);

    assertThat(result, is(jsonRecord));
  }

  /** Tests that {@link ElasticsearchTransforms.ValueExtractorFn} returns null when both inputs are null. */
  @Test
  public void testValueExtractorFnReturnNull() throws IOException {

    final String stringifiedJsonRecord = "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23}";

    final String jsonRecord = "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23,\"someProp\":\"someValue\"}";

    ObjectMapper objectMapper = new ObjectMapper();

    JsonNode jsonNode = objectMapper.readTree(stringifiedJsonRecord);

    String result = ElasticsearchTransforms.ValueExtractorFn.newBuilder()
            .setFileSystemPath(null)
            .setFunctionName(null)
            .build()
            .apply(jsonNode);

    assertThat(result, is(nullValue()));
  }

  /** Tests that {@link ElasticsearchTransforms.ValueExtractorFn} throws exception if only {@link ElasticsearchTransforms.ValueExtractorFn#fileSystemPath()} is supplied. */
  @Test(expected = IllegalArgumentException.class)
  public void testValueExtractorFnNullSystemPath() throws IOException {

    final String stringifiedJsonRecord = "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23}";

    final String jsonRecord = "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23,\"someProp\":\"someValue\"}";

    ObjectMapper objectMapper = new ObjectMapper();

    JsonNode jsonNode = objectMapper.readTree(stringifiedJsonRecord);

    String result = ElasticsearchTransforms.ValueExtractorFn.newBuilder()
            .setFileSystemPath(null)
            .setFunctionName("transform")
            .build()
            .apply(jsonNode);
  }

  /** Tests that {@link ElasticsearchTransforms.ValueExtractorFn} throws exception if only {@link ElasticsearchTransforms.ValueExtractorFn#functionName()} is supplied. */
  @Test(expected = IllegalArgumentException.class)
  public void testValueExtractorFnNullFunctionName() throws IOException {

    final String stringifiedJsonRecord = "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23}";

    final String jsonRecord = "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23,\"someProp\":\"someValue\"}";

    ObjectMapper objectMapper = new ObjectMapper();

    JsonNode jsonNode = objectMapper.readTree(stringifiedJsonRecord);

    String result = ElasticsearchTransforms.ValueExtractorFn.newBuilder()
            .setFileSystemPath(TRANSFORM_FILE_PATH)
            .setFunctionName(null)
            .build()
            .apply(jsonNode);
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
