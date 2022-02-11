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
package com.google.cloud.teleport.v2.elasticsearch.transforms;

import static org.junit.Assert.assertFalse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.elasticsearch.options.PubSubToElasticsearchOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test cases for {@link ProcessValidateJsonFields}. Verifies the correct behavior of the template
 * with invalid input messages.
 */
public class ProcessValidateJsonFieldsTest {

  private static final String RESOURCES_DIR = "ProcessValidateJsonFields/";
  private static final String INVALID_MESSAGE_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "invalidMessage.json").getPath();
  private static final boolean IS_WINDOWS = System.getProperty("os.name").contains("Windows");
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /**
   * Elasticsearch doesn't accept messages with only dot in field's name. Invalid message in runtime
   * will be refused by Elasticsearch and sent to Pub/Sub errorOutputTopic.
   */
  @Test
  public void testProcessValidateJsonFieldsWithInvalidInputMessage() throws IOException {
    String inputMessage = readInputMessage(INVALID_MESSAGE_FILE_PATH);
    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    PubSubToElasticsearchOptions options =
        TestPipeline.testingPipelineOptions().as(PubSubToElasticsearchOptions.class);
    PCollection<String> pc =
        pipeline
            .apply(Create.of(ImmutableList.of(inputMessage)))
            .apply("Validate JSON fields", new ProcessValidateJsonFields());

    PAssert.that(pc)
        .satisfies(
            x -> {
              String element = x.iterator().next();

              JsonNode jsonNode = null;
              try {
                jsonNode = new ObjectMapper().readTree(element);
              } catch (JsonProcessingException e) {
                e.printStackTrace();
              }
              Iterator<Map.Entry<String, JsonNode>> iter = jsonNode.fields();

              boolean hasInvalidFields = false;
              while (iter.hasNext()) {
                Map.Entry<String, JsonNode> entry = iter.next();
                String key = entry.getKey();

                if (key.equals(".")) {
                  hasInvalidFields = true;
                  break;
                }
              }

              assertFalse("Invalid message found", hasInvalidFields);
              return null;
            });

    pipeline.run(options);
  }

  private String readInputMessage(String filePath) throws IOException {
    return Files.lines(
            Paths.get(IS_WINDOWS ? filePath.substring(1) : filePath), StandardCharsets.UTF_8)
        .collect(Collectors.joining());
  }
}
