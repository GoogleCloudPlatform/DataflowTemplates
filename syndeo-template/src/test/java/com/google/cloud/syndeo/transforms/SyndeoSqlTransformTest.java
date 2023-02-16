/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.syndeo.transforms;

import static com.google.common.truth.Truth.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.syndeo.SyndeoTemplate;
import com.google.cloud.syndeo.perf.SyndeoLoadTestUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.BigtableEmulatorContainer;
import org.testcontainers.utility.DockerImageName;

@RunWith(JUnit4.class)
public class SyndeoSqlTransformTest {

  @ClassRule
  public static BigtableEmulatorContainer bigTableContainer =
      new BigtableEmulatorContainer(
          DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:367.0.0-emulators"));

  @Rule public final Timeout timeout = Timeout.seconds(40);

  static String buildPayloadWithQuery(String query) throws JsonProcessingException {
    return new ObjectMapper()
        .writeValueAsString(
            Map.of(
                "source",
                Map.of(
                    "urn",
                    "syndeo_test:schematransform:com.google.cloud:generate_data:v1",
                    "configurationParameters",
                    Map.of(
                        "numRows",
                        Long.valueOf(100L),
                        "runtimeSeconds",
                        Integer.valueOf(3),
                        "useNestedSchema",
                        true)),
                "transform",
                Map.of(
                    "urn",
                    "syndeo:schematransform:com.google.cloud:sql_transform:v1",
                    "configurationParameters",
                    Map.of("query", query)),
                "sink",
                Map.of(
                    "urn",
                    "syndeo:schematransform:com.google.cloud:bigtable_write:v1",
                    "configurationParameters",
                    Map.of(
                        "projectId",
                        "anyproject",
                        "instanceId",
                        "anyinstance",
                        "tableId",
                        "anytable-" + query.hashCode(),
                        "keyColumns",
                        Arrays.asList("lineDelta"),
                        "endpoint",
                        bigTableContainer.getEmulatorEndpoint()))));
  }

  @Test
  public void testEndToEndLocalPipeline() throws JsonProcessingException {
    String query =
        "SELECT "
            + "commit, "
            + "abs(linesAdded) - abs(linesRemoved) as lineDelta, "
            + "author.name as author, author.email as author_email "
            + "FROM input "
            + "WHERE mod(linesAdded, 2) = 1";
    String jsonPayload = buildPayloadWithQuery(query);

    Pipeline syndeoPipeline = Pipeline.create();
    SyndeoTemplate.buildPipeline(syndeoPipeline, SyndeoTemplate.buildFromJsonPayload(jsonPayload));

    TransformInputAndOutputVerifier verifier =
        new TransformInputAndOutputVerifier(
            "syndeo:schematransform:com.google.cloud:sql_transform:v1", "input", "output");
    syndeoPipeline.traverseTopologically(verifier);

    assertThat(verifier.matchedSchemas.get("input"))
        .isEqualTo(SyndeoLoadTestUtils.NESTED_TABLE_SCHEMA);
    assertThat(verifier.matchedSchemas.get("output").getFieldNames())
        .containsExactly("commit", "lineDelta", "author", "author_email");
    PipelineResult result = syndeoPipeline.run();
    result.waitUntilFinish();
    Long elementsToSink =
        StreamSupport.stream(result.metrics().allMetrics().getCounters().spliterator(), false)
            .filter(res -> res.getName().getName().equals("elementsProcessed"))
            .filter(res -> res.getKey().stepName().contains("sql_transform"))
            .map(res -> res.getAttempted())
            .findFirst()
            .get();
    Long elementsProduced =
        StreamSupport.stream(result.metrics().allMetrics().getCounters().spliterator(), false)
            .filter(res -> res.getName().getName().equals("elementsProcessed"))
            .filter(res -> res.getKey().stepName().contains("generate_data"))
            .map(res -> res.getAttempted())
            .findFirst()
            .get();

    // Verify that we filter out about 60% of elements.
    assertThat((long) (elementsProduced / 0.6)).isGreaterThan(elementsToSink);
  }

  @Test
  public void testEndToEndLocalPipelineWithErrorsInQuery() throws JsonProcessingException {
    String query =
        "SELECT "
            + "commit, commitDate, "
            + "linesAdded - linesRemoved as lineDelta, "
            + "FROM input";
    String jsonPayload = buildPayloadWithQuery(query);

    Pipeline syndeoPipeline =
        Pipeline.create(PipelineOptionsFactory.fromArgs("--streaming").create());
    SyndeoTemplate.buildPipeline(syndeoPipeline, SyndeoTemplate.buildFromJsonPayload(jsonPayload));

    TransformInputAndOutputVerifier verifier =
        new TransformInputAndOutputVerifier(
            "syndeo:schematransform:com.google.cloud:sql_transform:v1", "input", "errors");
    syndeoPipeline.traverseTopologically(verifier);

    assertThat(verifier.matchedSchemas.get("input"))
        .isEqualTo(SyndeoLoadTestUtils.NESTED_TABLE_SCHEMA);
    assertThat(verifier.matchedSchemas.get("errors").getFieldNames())
        .containsExactly("row", "error");
    assertThat(verifier.matchedSchemas.get("errors").getField("error").getType())
        .isEqualTo(Schema.FieldType.STRING);

    assertThat(
            verifier
                .matchedSchemas
                .get("errors")
                .getField("row")
                .getType()
                .getRowSchema()
                .getFieldNames())
        // Only the list of fields that are accessed, not the full list of fields in the input.
        .containsExactly("commit", "commitDate", "linesAdded", "linesRemoved");

    PipelineResult result = syndeoPipeline.run();
    result.waitUntilFinish();
    Long erroringElements =
        StreamSupport.stream(result.metrics().allMetrics().getCounters().spliterator(), false)
            .filter(res -> res.getName().getName().equals("elementsProcessed"))
            .filter(res -> res.getKey().stepName().contains("errors"))
            .map(res -> res.getAttempted())
            .findFirst()
            .orElse(-1L);

    // Verify that we have several error messages.
    assertThat(erroringElements).isGreaterThan(10);
  }

  static class TransformInputAndOutputVerifier implements Pipeline.PipelineVisitor {
    final String transformName;
    final String matchingInput;
    final String matchingOutput;
    public final Map<String, Schema> matchedSchemas = new HashMap<>();

    TransformInputAndOutputVerifier(
        String transformName, String matchingInput, String matchingOutput) {
      this.transformName = transformName;
      this.matchingInput = matchingInput;
      this.matchingOutput = matchingOutput;
    }

    @Override
    public void enterPipeline(@UnknownKeyFor @NonNull @Initialized Pipeline p) {
      // Ignore
    }

    @Override
    public Pipeline.PipelineVisitor.CompositeBehavior enterCompositeTransform(
        TransformHierarchy.@UnknownKeyFor @NonNull @Initialized Node node) {
      if (node.getFullName().equals(transformName)) {
        PCollection<?> matchedInput =
            node.getInputs().entrySet().stream()
                .filter(entry -> entry.getKey().getId().equals(matchingInput))
                .findFirst()
                .get()
                .getValue();
        PCollection<?> matchedOutput =
            node.getOutputs().entrySet().stream()
                .filter(entry -> entry.getKey().getId().equals(matchingOutput))
                .findFirst()
                .get()
                .getValue();

        matchedSchemas.put(matchingInput, matchedInput.getSchema());
        matchedSchemas.put(matchingOutput, matchedOutput.getSchema());
      }
      // We enter the root node (name: ""), which lets us look at underlying composites, but we do
      // not need
      // to look further in.
      return node.getFullName().equals("")
          ? CompositeBehavior.ENTER_TRANSFORM
          : CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
    }

    @Override
    public void leaveCompositeTransform(
        TransformHierarchy.@UnknownKeyFor @NonNull @Initialized Node node) {}

    @Override
    public void visitPrimitiveTransform(
        TransformHierarchy.@UnknownKeyFor @NonNull @Initialized Node node) {
      // Ignore
    }

    @Override
    public void visitValue(
        @UnknownKeyFor @NonNull @Initialized PValue value,
        TransformHierarchy.@UnknownKeyFor @NonNull @Initialized Node producer) {
      // Ignore
    }

    @Override
    public void leavePipeline(@UnknownKeyFor @NonNull @Initialized Pipeline pipeline) {
      // Ignore
    }
  }
}
